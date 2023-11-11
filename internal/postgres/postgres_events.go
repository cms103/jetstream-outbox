package postgres

import (
	"context"
	"log"
	"log/slog"
	"strconv"
	"time"

	// "github.com/jackc/pglogrepl"
	// "github.com/jackc/pgx/pgproto3"
	// "github.com/jackc/pgx/v5/pgconn"
	// "github.com/jackc/pgx/v5/pgtype"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/jackc/pgx/v5/pgtype"
)

type OutboxEvent struct {
	Wal   pglogrepl.LSN
	Event map[string]interface{}
}
type OutboxEvents chan OutboxEvent

func GetEventSubscription(connString string, confirmedWAL chan pglogrepl.LSN) (OutboxEvents, error) {
	ourChannel := make(OutboxEvents, 1)

	// Try connecting and setting up the subscription to the slot
	//const outputPlugin = "pgoutput"
	//const outputPlugin = "wal2json"

	conn, err := pgconn.Connect(context.Background(), connString)
	if err != nil {
		slog.Error("failed to connect to PostgreSQL server", "error", err)
		return nil, err
	}

	// streaming of large transactions is available since PG 14 (protocol version 2)
	// we also need to set 'streaming' to 'true'
	var pluginArguments = []string{
		"proto_version '2'",
		"publication_names 'jetstream_outbox'",
		"messages 'true'",
		"streaming 'true'",
	}

	sysident, err := pglogrepl.IdentifySystem(context.Background(), conn)
	if err != nil {
		slog.Error("IdentifySystem failed", "error", err)
		return nil, err
	}
	slog.Info("Startup", "SystemID", sysident.SystemID, "Timeline", strconv.Itoa(int(sysident.Timeline)), "XLogPos", sysident.XLogPos.String(), "DBName", sysident.DBName)

	slotName := "jetstream_outbox"

	// err = pglogrepl.StartReplication(context.Background(), conn, slotName, sysident.XLogPos, pglogrepl.StartReplicationOptions{PluginArgs: pluginArguments})
	err = pglogrepl.StartReplication(context.Background(), conn, slotName, pglogrepl.LSN(0), pglogrepl.StartReplicationOptions{PluginArgs: pluginArguments})
	if err != nil {
		slog.Error("StartReplication failed", "error", err)
		return nil, err
	}

	slog.Info("Started logical replication", "slot", slotName)

	// Now start looping in the background
	go replicationLoop(ourChannel, sysident, conn, confirmedWAL)
	return ourChannel, nil

}

func replicationLoop(ourChannel OutboxEvents, sysident pglogrepl.IdentifySystemResult, conn *pgconn.PgConn, confirmedWAL chan pglogrepl.LSN) {
	//	clientXLogPos := sysident.XLogPos
	lastConfirmedXLogPos := sysident.XLogPos
	lastSeenWALPos := sysident.XLogPos
	standbyMessageTimeout := time.Second * 10
	nextStandbyMessageDeadline := time.Now().Add(standbyMessageTimeout)
	//relations := map[uint32]*pglogrepl.RelationMessage{}
	relationsV2 := map[uint32]*pglogrepl.RelationMessageV2{}
	typeMap := pgtype.NewMap()

	// whenever we get StreamStartMessage we set inStream to true and then pass it to DecodeV2 function
	// on StreamStopMessage we set it back to false
	inStream := false
	var err error

	for {
		slog.Debug("Loop", "ConfirmedWAL", lastConfirmedXLogPos.String(), "SeenWAL", lastSeenWALPos.String())
		// See if we have any newly confirmed increments in the WAL
		select {
		case msg := <-confirmedWAL:
			slog.Debug("Got a confirmed WAL back", "WAL", msg.String())
			lastConfirmedXLogPos = msg
		default:
			// No updates on the last seen WAL
		}

		if time.Now().After(nextStandbyMessageDeadline) {
			err = pglogrepl.SendStandbyStatusUpdate(context.Background(), conn, pglogrepl.StandbyStatusUpdate{WALWritePosition: lastConfirmedXLogPos})
			if err != nil {
				log.Fatalln("SendStandbyStatusUpdate failed:", err)
			}
			slog.Debug("Sent Standby status message", "position", lastConfirmedXLogPos.String())
			nextStandbyMessageDeadline = time.Now().Add(standbyMessageTimeout)
		}

		ctx, cancel := context.WithDeadline(context.Background(), nextStandbyMessageDeadline)
		rawMsg, err := conn.ReceiveMessage(ctx)
		cancel()
		if err != nil {
			if pgconn.Timeout(err) {
				continue
			}
			slog.Error("ReceiveMessage failed", "error", err)
			close(ourChannel)
			return
		}

		// Another check to see if we've finished processing any more messages while waiting in ReceiveMessage
		// See if we have any newly confirmed increments in the WAL
		select {
		case msg := <-confirmedWAL:
			slog.Debug("Got a confirmed WAL back after receive message", "WAL", msg.String())
			lastConfirmedXLogPos = msg
		default:
			// No updates on the last seen WAL
		}

		if errMsg, ok := rawMsg.(*pgproto3.ErrorResponse); ok {
			slog.Error("Received Postgres WAL error", "error", errMsg)
			close(ourChannel)
			return
		}

		msg, ok := rawMsg.(*pgproto3.CopyData)
		if !ok {
			slog.Error("Casting to CopyData failed - unexpected message type", "msg", rawMsg)
			close(ourChannel)
			return
		}

		switch msg.Data[0] {
		case pglogrepl.PrimaryKeepaliveMessageByteID:
			pkm, err := pglogrepl.ParsePrimaryKeepaliveMessage(msg.Data[1:])
			if err != nil {
				slog.Error("ParsePrimaryKeepaliveMessage failed", "error", err)
				close(ourChannel)
				return
			}
			slog.Debug("Primary Keepalive Message", "ServerWALEnd:", pkm.ServerWALEnd, "ServerTime:", pkm.ServerTime, "ReplyRequested:", pkm.ReplyRequested)
			if lastConfirmedXLogPos == lastSeenWALPos {
				// We are up to date - can increment to match the server.
				slog.Debug("Moving confirmed message point on as nothing is outstanding", "lastConfirmedXLogPos", lastConfirmedXLogPos)
				lastConfirmedXLogPos = pkm.ServerWALEnd
				lastSeenWALPos = pkm.ServerWALEnd
			}

			if pkm.ReplyRequested {
				nextStandbyMessageDeadline = time.Time{}
			}

		case pglogrepl.XLogDataByteID:
			xld, err := pglogrepl.ParseXLogData(msg.Data[1:])
			if err != nil {
				slog.Error("ParseXLogData failed", "error", err)
				close(ourChannel)
				return
			}

			slog.Debug("XLogData", "WALStart", xld.WALStart, "ServerWALEnd", xld.ServerWALEnd, "ServerTime", xld.ServerTime)

			// if v2 {
			insertValues, err := processV2(xld.WALData, relationsV2, typeMap, &inStream)
			if err != nil {
				// Close the stream and quit
				slog.Error("Error during process of events - closing the channel and stopping", "error", err)
				close(ourChannel)
				return
			}

			if insertValues != nil {
				slog.Debug("Returning found insert values via channel", "values", insertValues)
				lastSeenWALPos = xld.WALStart + pglogrepl.LSN(len(xld.WALData))

				ourChannel <- OutboxEvent{
					Wal:   lastSeenWALPos,
					Event: insertValues,
				}
			}

		}
	}
}

func processV2(walData []byte, relations map[uint32]*pglogrepl.RelationMessageV2, typeMap *pgtype.Map, inStream *bool) (map[string]interface{}, error) {
	logicalMsg, err := pglogrepl.ParseV2(walData, *inStream)
	if err != nil {
		slog.Error("Failed to parse logical replication message", "error", err)
		return nil, err
	}

	slog.Debug("Receive a logical replication message", "MessageType", logicalMsg.Type().String())
	switch logicalMsg := logicalMsg.(type) {
	case *pglogrepl.RelationMessageV2:
		relations[logicalMsg.RelationID] = logicalMsg

	case *pglogrepl.BeginMessage:
		// Indicates the beginning of a group of changes in a transaction. This is only sent for committed transactions. You won't get any events from rolled back transactions.

	case *pglogrepl.CommitMessage:

	case *pglogrepl.InsertMessageV2:
		rel, ok := relations[logicalMsg.RelationID]
		if !ok {
			slog.Error("Unknown relation ID", "RelationID", logicalMsg.RelationID)
			return nil, err
		}
		values := map[string]interface{}{}
		for idx, col := range logicalMsg.Tuple.Columns {
			colName := rel.Columns[idx].Name
			switch col.DataType {
			case 'n': // null
				values[colName] = nil
			case 'u': // unchanged toast
				// This TOAST value was not changed. TOAST values are not stored in the tuple, and logical replication doesn't want to spend a disk read to fetch its value for you.
			case 't': //text
				val, err := decodeTextColumnData(typeMap, col.Data, rel.Columns[idx].DataType)
				if err != nil {
					slog.Error("Error decoding column data", "error", err)
					return nil, err
				}
				values[colName] = val
			}
		}
		return values, nil

	case *pglogrepl.UpdateMessageV2:
		// log.Printf("update for xid %d\n", logicalMsg.Xid)
		// ...
	case *pglogrepl.DeleteMessageV2:
		// log.Printf("delete for xid %d\n", logicalMsg.Xid)
		// ...
	case *pglogrepl.TruncateMessageV2:
		// log.Printf("truncate for xid %d\n", logicalMsg.Xid)
		// ...

	case *pglogrepl.TypeMessageV2:
	case *pglogrepl.OriginMessage:

	case *pglogrepl.LogicalDecodingMessageV2:
		// log.Printf("Logical decoding message: %q, %q, %d", logicalMsg.Prefix, logicalMsg.Content, logicalMsg.Xid)

	case *pglogrepl.StreamStartMessageV2:
		*inStream = true
		slog.Debug("Stream start message", "xid", logicalMsg.Xid, "firstSegment", logicalMsg.FirstSegment)
	case *pglogrepl.StreamStopMessageV2:
		*inStream = false
		slog.Debug("Stream stop message")
	case *pglogrepl.StreamCommitMessageV2:
		slog.Debug("Stream commit message", "xid", logicalMsg.Xid)
	case *pglogrepl.StreamAbortMessageV2:
		slog.Debug("Stream abort message", "xid", logicalMsg.Xid)
	default:
		slog.Warn("Unknown message type in pgoutput stream", "message", logicalMsg)
	}

	return nil, nil
}

// func processV1(walData []byte, relations map[uint32]*pglogrepl.RelationMessage, typeMap *pgtype.Map) {
// 	logicalMsg, err := pglogrepl.Parse(walData)
// 	if err != nil {
// 		log.Fatalf("Parse logical replication message: %s", err)
// 	}
// 	log.Printf("Receive a logical replication message: %s", logicalMsg.Type())
// 	switch logicalMsg := logicalMsg.(type) {
// 	case *pglogrepl.RelationMessage:
// 		relations[logicalMsg.RelationID] = logicalMsg

// 	case *pglogrepl.BeginMessage:
// 		// Indicates the beginning of a group of changes in a transaction. This is only sent for committed transactions. You won't get any events from rolled back transactions.

// 	case *pglogrepl.CommitMessage:

// 	case *pglogrepl.InsertMessage:
// 		rel, ok := relations[logicalMsg.RelationID]
// 		if !ok {
// 			log.Fatalf("unknown relation ID %d", logicalMsg.RelationID)
// 		}
// 		values := map[string]interface{}{}
// 		for idx, col := range logicalMsg.Tuple.Columns {
// 			colName := rel.Columns[idx].Name
// 			switch col.DataType {
// 			case 'n': // null
// 				values[colName] = nil
// 			case 'u': // unchanged toast
// 				// This TOAST value was not changed. TOAST values are not stored in the tuple, and logical replication doesn't want to spend a disk read to fetch its value for you.
// 			case 't': //text
// 				val, err := decodeTextColumnData(typeMap, col.Data, rel.Columns[idx].DataType)
// 				if err != nil {
// 					log.Fatalln("error decoding column data:", err)
// 				}
// 				values[colName] = val
// 			}
// 		}
// 		log.Printf("INSERT INTO %s.%s: %v", rel.Namespace, rel.RelationName, values)

// 	case *pglogrepl.UpdateMessage:
// 		// ...
// 	case *pglogrepl.DeleteMessage:
// 		// ...
// 	case *pglogrepl.TruncateMessage:
// 		// ...

// 	case *pglogrepl.TypeMessage:
// 	case *pglogrepl.OriginMessage:

// 	case *pglogrepl.LogicalDecodingMessage:
// 		log.Printf("Logical decoding message: %q, %q", logicalMsg.Prefix, logicalMsg.Content)

// 	case *pglogrepl.StreamStartMessageV2:
// 		log.Printf("Stream start message: xid %d, first segment? %d", logicalMsg.Xid, logicalMsg.FirstSegment)
// 	case *pglogrepl.StreamStopMessageV2:
// 		log.Printf("Stream stop message")
// 	case *pglogrepl.StreamCommitMessageV2:
// 		log.Printf("Stream commit message: xid %d", logicalMsg.Xid)
// 	case *pglogrepl.StreamAbortMessageV2:
// 		log.Printf("Stream abort message: xid %d", logicalMsg.Xid)
// 	default:
// 		log.Printf("Unknown message type in pgoutput stream: %T", logicalMsg)
// 	}
// }

func decodeTextColumnData(mi *pgtype.Map, data []byte, dataType uint32) (interface{}, error) {
	if dt, ok := mi.TypeForOID(dataType); ok {
		return dt.Codec.DecodeValue(mi, dataType, pgtype.TextFormatCode, data)
	}
	return string(data), nil
}
