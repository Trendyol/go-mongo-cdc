package main

import (
	"context"
	"log"
	"time"

	cdc "github.com/Trendyol/go-mongo-cdc"
	"github.com/Trendyol/go-mongo-cdc/config"
	"github.com/Trendyol/go-mongo-cdc/mongo/changestream"
	"go.uber.org/zap"
)

func main() {
	cfg := config.Config{
		Host:       "localhost",
		Port:       27017,
		Database:   "seller_contents_db",
		Collection: "seller_contents",
		DebugMode:  true,
		Metric: config.MetricConfig{
			Port: 8080,
		},
		Checkpoint: config.CheckpointConfig{
			Collection:   "checkpoint-SellerContents",
			SaveInterval: 30 * time.Second,
		},
	}

	connector, err := cdc.NewConnector(context.Background(), cfg, listenerFunc)
	if err != nil {
		log.Fatal("failed to create connector:", err)
	}

	defer connector.Close()

	ctx := context.Background()
	go connector.Start(ctx)

	if err := connector.WaitUntilReady(ctx); err != nil {
		log.Fatal("connector failed to start:", err)
	}

	log.Println("MongoDB CDC is running. Press Ctrl+C to stop.")

	select {}
}

func listenerFunc(lc *changestream.ListenerContext) {
	logger := zap.NewExample()
	defer logger.Sync()

	logger.Info("Change event received",
		zap.String("operation", string(lc.Message.OperationType)),
		zap.String("database", lc.Message.Database),
		zap.String("collection", lc.Message.Collection),
		zap.Any("documentId", lc.Message.DocumentID),
		zap.Time("eventTime", lc.Message.EventTime),
	)

	switch lc.Message.OperationType {
	case "insert", "update", "replace":
		if lc.Message.FullDocument != nil {
			logger.Info("Document changed",
				zap.String("operation", string(lc.Message.OperationType)),
				zap.Any("document", lc.Message.FullDocument),
			)

			logger.Info("Would convert to Elastic DTO here")

		}

	case "delete":
		logger.Info("Document deleted",
			zap.Any("documentId", lc.Message.DocumentID),
		)

	}

	if err := lc.Ack(); err != nil {
		logger.Error("Failed to acknowledge message", zap.Error(err))
	}
}
