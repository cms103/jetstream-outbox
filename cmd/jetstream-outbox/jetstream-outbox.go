package main

import (
	"flag"
	"io"
	"log/slog"
	"os"

	"github.com/cms103/jetstream-outbox/internal/postgres"
	"github.com/cms103/jetstream-outbox/internal/publisher"
	"github.com/jackc/pglogrepl"
	"github.com/nats-io/nats.go"
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
	var maxInflight = flag.Int("inflight", 10, "Maximum number of outstanding JetStream acknowledgments allowed when sending a new message")

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

	ourDeliveredWALChan := make(chan pglogrepl.LSN, 1)

	ourEvents, err := postgres.GetEventSubscription(*dbDetails, ourDeliveredWALChan)
	if err != nil {
		slog.Error("No DB connection, closing", "error", err)
		return
	}

	jsp := publisher.New(*eventPrefix, nc, *maxInflight)

	err = jsp.Run(ourEvents, ourDeliveredWALChan)

}
