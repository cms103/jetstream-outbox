package publisher

import (
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"

	"github.com/cms103/jetstream-outbox/internal/postgres"
	"github.com/google/uuid"
	"github.com/jackc/pglogrepl"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

type JetSteamPublisher struct {
	eventPrefix string
	nc          *nats.Conn
	workQueue   chan *SendJob
}

func New(eventPrefix string, nc *nats.Conn, maxInflight int) *JetSteamPublisher {
	return &JetSteamPublisher{
		eventPrefix: eventPrefix,
		nc:          nc,
		workQueue:   make(chan *SendJob, maxInflight),
	}
}

func (jsp *JetSteamPublisher) Run(ourEvents postgres.OutboxEvents, ourDeliveredWALChan chan pglogrepl.LSN) error {
	js, err := jetstream.New(jsp.nc)

	if err != nil {
		slog.Error("Unable to establish JetStream connection", "error", err)
		return err
	}

	sm := SubjectMapper{Prefix: jsp.eventPrefix}

	closeChannel := make(chan bool)

	// A separate go routine for handling the return of Wal following Acks
	go func() {
		jsp.WalUpdater(ourDeliveredWALChan)
		// If the WalUpdater returns then we've got error coming back from JetStream, time to stop
		closeChannel <- true
	}()

	for {
		var event postgres.OutboxEvent
		var more bool
		select {
		case <-closeChannel:
			slog.Info("JetStream publisher closing down")
			return errors.New("closing down publication")
		case event, more = <-ourEvents:
		}

		if more {
			subject := sm.Map(event.Event)
			jsonData, err := json.Marshal(event.Event["payload"])
			if err != nil {
				slog.Error("Unable to marshall DB data into event", "error", err)
				return err
			}

			msgEventID, ok := event.Event["id"].([16]uint8)
			if !ok {
				slog.Error("Error extracting message ID", "error", err)
			}

			msgID, err := uuid.FromBytes(msgEventID[:])

			if err != nil {
				slog.Error("Unable to parse UUID for event", "error", err, "id", event.Event["id"])
				return err
			}

			natsMsg := &nats.Msg{
				Data:    jsonData,
				Subject: subject,
				Header:  nats.Header{"Nats-Msg-Id": []string{msgID.String()}},
			}

			ackFuture, err := js.PublishMsgAsync(natsMsg)
			if err != nil {
				slog.Error("Error publishing event", "subject", subject, "jsonData", jsonData, "error", err)
				return err
			}

			// Once the Ack is back, update the Wal
			jsp.workQueue <- &SendJob{
				Ack:   ackFuture,
				Wal:   event.Wal,
				MsgID: msgID.String(),
			}

		} else {
			slog.Error("Event channel closed, exiting")
			return errors.New("event channel closed")

		}
	}
}

func (jsp *JetSteamPublisher) WalUpdater(walDelivery chan pglogrepl.LSN) error {
	for {
		// Get the next job to monitor
		ourJob := <-jsp.workQueue

		select {
		case <-ourJob.Ack.Ok():
			// We got the ack OK, so update the Wal
			slog.Debug("Ack recieved from JetStream", "WAL", ourJob.Wal, "MsgID", ourJob.MsgID)
			walDelivery <- ourJob.Wal
		case err := <-ourJob.Ack.Err():
			// We had a failure - need to stop everything.
			slog.Error("Error received from JetStream", "WAL", ourJob.Wal, "MsgID", ourJob.MsgID, "error", err)
			return err
		}
	}
}

type SendJob struct {
	Ack   jetstream.PubAckFuture
	Wal   pglogrepl.LSN
	MsgID string
}

type SubjectMapper struct {
	Prefix string
}

func (sm *SubjectMapper) Map(data map[string]interface{}) string {
	return fmt.Sprintf("%s.%s.%s", sm.Prefix, data["aggregatetype"], data["aggregateid"])
}
