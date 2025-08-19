package main

import (
	"context"
	"log"
	"strings"
	"time"

	cdc "github.com/Trendyol/go-mongo-cdc"
	"github.com/Trendyol/go-mongo-cdc/config"
	"github.com/Trendyol/go-mongo-cdc/mongo/changestream"
	"github.com/Trendyol/go-mongo-cdc/mongo/message"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

type CDCListener struct {
	logger *zap.Logger
}

func main() {
	loggerConfig := zap.NewDevelopmentConfig()
	loggerConfig.Level = zap.NewAtomicLevelAt(zapcore.InfoLevel)
	logger, _ := loggerConfig.Build()

	defer func() {
		if err := logger.Sync(); err != nil {
			if !strings.Contains(err.Error(), "inappropriate ioctl for device") && !strings.Contains(err.Error(), "bad file descriptor") {
				log.Printf("Failed to sync logger: %v", err)
			}
		}
	}()

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
			SaveInterval: 60 * time.Second,
		},
		Membership: config.MembershipConfig{
			HeartbeatInterval:  30 * time.Second,
			HealthCheckTimeout: 60 * time.Second,
			ChunkBased:         false,
			Config: map[string]string{
				"shardKey": "sellerId",
			},
		},
		Logger: config.LoggerConfig{
			Logger: logger,
		},
	}

	myListener := &CDCListener{
		logger: logger,
	}

	connector, err := cdc.NewConnector(context.Background(), cfg, myListener.ProcessChangeEvent)
	if err != nil {
		log.Fatal("failed to create connector:", err)
	}

	defer connector.Close()

	ctx := context.Background()
	connector.Start(ctx)
}

func (l *CDCListener) ProcessChangeEvent(lc *changestream.ListenerContext) error {
	l.logger.Info("Change event received",
		zap.String("operation", string(lc.Message.OperationType)),
		zap.String("database", lc.Message.Database),
		zap.String("collection", lc.Message.Collection),
		zap.Any("documentId", lc.Message.DocumentID),
		zap.Time("eventTime", lc.Message.EventTime),
	)

	switch lc.Message.OperationType {
	case message.OperationInsert, message.OperationUpdate, message.OperationReplace:
		if lc.Message.FullDocument != nil {
			l.logger.Info("Document changed",
				zap.String("operation", string(lc.Message.OperationType)),
				zap.Any("document", lc.Message.FullDocument),
			)
		}
	case message.OperationDelete:
		l.logger.Info("Document deleted",
			zap.Any("documentId", lc.Message.DocumentID),
		)
	}

	if err := lc.Ack(); err != nil {
		l.logger.Error("Failed to acknowledge message", zap.Error(err))
		return err
	}
	return nil
}
