package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"strconv"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

const DefaultServerAddress = "localhost:4321"
const DefaultNatsAddress = "localhost"
const DefaultNatsCredentials = ""

func main() {
	var logLevel = flag.String("logging", "info", "Set the minimum logging level to debug,info,warn,error")
	var dbDetails = flag.String("db", "postgres://user:password@127.0.0.1/dbname?replication=database", "Database connection details")
	var outboxName = flag.String("table", "outbox", "The name of the table being used as an outbox")

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

	conn, err := pgx.Connect(context.Background(), *dbDetails)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Unable to connect to database: %v\n", err)
		os.Exit(1)
	}
	defer conn.Close(context.Background())

	sendThroughOutbox(conn, *outboxName)
}

func sendThroughOutbox(conn *pgx.Conn, tableName string) {
	intervalStart := time.Now()
	startTime := intervalStart
	reportInterval := time.Second * 5

	ctx := context.Background()
	aggType := "throughPutTest"
	aggId := 1
	payload := map[string]interface{}{"msg": "test message"}
	intervalCount := 0
	totalCount := 0

	insertText := fmt.Sprintf("INSERT INTO %s (id, aggregatetype, aggregateid, payload) VALUES ($1, $2, $3, $4)", tableName)

	for {
		rightNow := time.Now()
		if intervalStart.Add(reportInterval).Before(rightNow) {
			intervalDuration := rightNow.Sub(intervalStart)
			intervalPerformance := float64(intervalCount) / intervalDuration.Seconds()
			overallDuration := rightNow.Sub(startTime)
			overallPerformance := float64(totalCount) / overallDuration.Seconds()

			intervalPerformanceStr := fmt.Sprintf("%.2f", intervalPerformance)
			overallPerformanceStr := fmt.Sprintf("%.2f", overallPerformance)

			slog.Info("Performance", "IntervalRate", intervalPerformanceStr, "OverallRate", overallPerformanceStr, "IntervalCount", intervalCount, "TotalCount", totalCount)

			intervalStart = rightNow
			intervalCount = 0
		}
		txn, err := conn.Begin(ctx)
		if err != nil {
			slog.Error("Failed to start transaction", "error", err)
			return
		}

		id := uuid.New()

		_, err = txn.Exec(ctx, insertText, id, aggType, strconv.Itoa(aggId), payload)
		if err != nil {
			slog.Error("Error inserting into outbox", "error", err)
			txn.Rollback(ctx)
			return
		}

		_, err = txn.Exec(ctx, "DELETE FROM outbox WHERE id = $1", id)
		if err != nil {
			slog.Error("Error delete entry from outbox", "error", err)
			txn.Rollback(ctx)
			return
		}

		err = txn.Commit(ctx)
		if err != nil {
			slog.Error("Error performing commit", "error", err)
			return
		}

		aggId++
		totalCount++
		intervalCount++
	}
}
