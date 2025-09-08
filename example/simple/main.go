package main

import (
	"context"

	cdc "github.com/Trendyol/go-mongo-cdc"

	"log"
	"strings"
	"time"

	"github.com/Trendyol/go-mongo-cdc/config"
	"github.com/Trendyol/go-mongo-cdc/mongo/message"
	"github.com/Trendyol/go-mongo-cdc/stream"
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
		Metric: config.MetricConfig{
			Port: 8080,
		},
		Checkpoint: config.CheckpointConfig{
			Collection:   "checkpoint",
			SaveInterval: 60 * time.Second,
		},
		Partition: config.PartitionConfig{
			HeartbeatInterval:      5 * time.Second,
			WorkerTimeout:          30 * time.Second,
			PartitionDatabase:      "exampleDB",
			RebalanceCheckInterval: 10 * time.Second,
			RuntimeFiltering:       true, // MongoDB CPU yükünü azaltmak için runtime filtreleme aktif
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

func (l *CDCListener) ProcessChangeEvent(lc *stream.ListenerContext) error {
	switch lc.Message.OperationType {
	case message.OperationInsert, message.OperationUpdate, message.OperationReplace:
		if lc.Message.FullDocument != nil {
			l.logger.Info("Document changed",
				zap.String("operation", string(lc.Message.OperationType)),
				zap.Any("document", lc.Message.FullDocument),
				zap.Int("partitionId", lc.PartitionID),
			)
		}
	case message.OperationDelete:
		l.logger.Info("Document deleted",
			zap.Any("documentId", lc.Message.DocumentID),
			zap.Int("partitionId", lc.PartitionID),
		)
	}

	if err := lc.Ack(); err != nil {
		l.logger.Error("Failed to acknowledge message", zap.Error(err))
		return err
	}
	return nil
}
