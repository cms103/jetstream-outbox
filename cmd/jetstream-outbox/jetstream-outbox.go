package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"time"

	"github.com/cms103/jetstream-outbox/internal/postgres"
	"github.com/google/uuid"
	"github.com/jackc/pglogrepl"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

const DefaultServerAddress = "localhost:4321"
const DefaultNatsAddress = "localhost"
const DefaultNatsCredentials = ""

func main() {
	var natsAddress = flag.String("nats", DefaultNatsAddress, "NATS server address")
	var natsCredentials = flag.String("creds", DefaultNatsCredentials, "NATS credentials file location")
	var logLevel = flag.String("logging", "info", "Set the minimum logging level to debug,info,warn,error")
	var dbDetails = flag.String("db", "postgres://user:password@127.0.0.1/dbname?replication=database", "Database connection details")
	var eventPrefix = flag.String("prefix", "events", "The top level prefix for events published to NATS")

	flag.Parse()

	// Default level
	var loggingLevel slog.Leveler = slog.LevelInfo
	switch *logLevel {
	case "debug":
		loggingLevel = slog.LevelDebug
	case "info":
		loggingLevel = slog.LevelInfo
	case "warn":
		loggingLevel = slog.LevelWarn
	case "error":
		loggingLevel = slog.LevelError
	}

	slog.SetDefault(slog.New(slog.NewTextHandler(io.Writer(os.Stderr), &slog.HandlerOptions{Level: loggingLevel})))
	slog.Info("Logging level set", "level", *logLevel)

	var nc *nats.Conn

	var natsOptions []nats.Option

	if *natsCredentials != "" {
		natsOptions = append(natsOptions, nats.UserCredentials(*natsCredentials))
	}
	nc, err := nats.Connect(*natsAddress, natsOptions...)

	if err != nil {
		slog.Error("Error connecting to NATS", "error", err, "connectionAddress", *natsAddress, "credentialsFile", *natsCredentials)
		return
	}

	js, err := jetstream.New(nc)

	if err != nil {
		slog.Error("Unable to establish JetStream connection", "error", err, "connectionAddress", *natsAddress, "credentialsFile", *natsCredentials)
		return
	}

	ourDeliveredWALChan := make(chan pglogrepl.LSN, 1)

	ourEvents, err := postgres.GetEventSubscription(*dbDetails, ourDeliveredWALChan)
	if err != nil {
		slog.Error("No DB connection, closing", "error", err)
		return
	}

	sm := SubjectMapper{Prefix: *eventPrefix}

	for {
		event, more := <-ourEvents
		if more {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			subject := sm.Map(event.Event)
			jsonData, err := json.Marshal(event.Event["payload"])
			if err != nil {
				slog.Error("Unable to marshall DB data into event", "error", err)
				return
			}

			msgEventID, ok := event.Event["id"].([16]uint8)
			if !ok {
				slog.Error("Error extracting message ID", "error", err)
			}

			msgID, err := uuid.FromBytes(msgEventID[:])

			if err != nil {
				slog.Error("Unable to parse UUID for event", "error", err, "id", event.Event["id"])
				return
			}

			natsMsg := &nats.Msg{
				Data:    jsonData,
				Subject: subject,
				Header:  nats.Header{"Nats-Msg-Id": []string{msgID.String()}},
			}

			_, err = js.PublishMsg(ctx, natsMsg)
			if err != nil {
				slog.Error("Error publishing event", "subject", subject, "jsonData", jsonData)
				return
			}
			// Context is done with
			cancel()

			// Now update the DB that we've handled this WAL
			ourDeliveredWALChan <- event.Wal

		} else {
			slog.Error("Event channel closed, exiting")
			return

		}
	}
}

type SubjectMapper struct {
	Prefix string
}

func (sm *SubjectMapper) Map(data map[string]interface{}) string {
	return fmt.Sprintf("%s.%s.%s", sm.Prefix, data["aggregatetype"], data["aggregateid"])
}
