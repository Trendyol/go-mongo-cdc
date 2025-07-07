package main

import (
	"context"
	"log"
	"time"

	cdc "github.com/Trendyol/go-mongo-cdc"
	"github.com/Trendyol/go-mongo-cdc/config"
	"github.com/Trendyol/go-mongo-cdc/mongo/changestream"
	"github.com/Trendyol/go-mongo-cdc/mongo/message"
	"go.uber.org/zap"
)

// TODO: Update example with membership & chunk configs
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
		log.Println("connector failed to start:", err)
		return
	}

	log.Println("MongoDB CDC is running. Press Ctrl+C to stop.")

	select {}
}

func listenerFunc(lc *changestream.ListenerContext) {
	logger := zap.NewExample()
	defer func() {
		if err := logger.Sync(); err != nil {
			log.Printf("Failed to sync logger: %v", err)
		}
	}()

	logger.Info("Change event received",
		zap.String("operation", string(lc.Message.OperationType)),
		zap.String("database", lc.Message.Database),
		zap.String("collection", lc.Message.Collection),
		zap.Any("documentId", lc.Message.DocumentID),
		zap.Time("eventTime", lc.Message.EventTime),
	)

	switch lc.Message.OperationType {
	case message.OperationInsert, message.OperationUpdate, message.OperationReplace:
		if lc.Message.FullDocument != nil {
			logger.Info("Document changed",
				zap.String("operation", string(lc.Message.OperationType)),
				zap.Any("document", lc.Message.FullDocument),
			)

			logger.Info("Would convert to Elastic DTO here")
		}
	case message.OperationDelete:
		logger.Info("Document deleted",
			zap.Any("documentId", lc.Message.DocumentID),
		)
	}

	if err := lc.Ack(); err != nil {
		logger.Error("Failed to acknowledge message", zap.Error(err))
	}
}
