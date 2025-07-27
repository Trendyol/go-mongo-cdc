package main

import (
	"context"
	cdc "github.com/Trendyol/go-mongo-cdc"
	"log"
	"time"

	"github.com/Trendyol/go-mongo-cdc/config"
	"github.com/Trendyol/go-mongo-cdc/mongo/changestream"
	"github.com/Trendyol/go-mongo-cdc/mongo/message"
	"go.uber.org/zap"
)

// TODO: Update example with membership & chunk configs
func main() {
	// Debug level logger
	/*	loggerConfig := zap.NewDevelopmentConfig()
		loggerConfig.Level = zap.NewAtomicLevelAt(zapcore.DebugLevel)
		logger, _ := loggerConfig.Build()
		defer logger.Sync()*/

	cfg := config.Config{
		Host:       "localhost",
		Port:       27017,
		Database:   "exampleDB",
		Collection: "exampleCollection",
		DebugMode:  true,
		Metric: config.MetricConfig{
			Port: 8080,
		},
		Checkpoint: config.CheckpointConfig{
			Collection:   "checkpoint-SellerContents",
			SaveInterval: 30 * time.Second,
		},
		Membership: config.MembershipConfig{
			HeartbeatInterval:  30 * time.Second,
			HealthCheckTimeout: 60 * time.Second,
			Enabled:            true,
			Type:               "dynamic",
			ChunkBased:         false, // Hash-based partitioning kullan
			Config: map[string]string{
				"shardKey": "sellerId", // Sadece ChunkBased: true olduğunda kullanılır
			},
		},
		/*		Logger: config.LoggerConfig{
				Logger: logger,
			},*/
	}

	connector, err := cdc.NewConnector(context.Background(), cfg, listenerFunc)
	if err != nil {
		log.Fatal("failed to create connector:", err)
	}

	defer connector.Close()

	ctx := context.Background()
	connector.Start(ctx)
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
