package main

import (
	"context"

	cdc "github.com/Trendyol/go-mongo-cdc"

	"fmt"
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
	loggerConfig.Level = zap.NewAtomicLevelAt(zapcore.DebugLevel)
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
			RebalanceCheckInterval: 3 * time.Second,
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
			l.logger.Info(fmt.Sprintf("Document changed - operation: %s, document: %v, partitionId: %d", string(lc.Message.OperationType), lc.Message.DocumentID, lc.PartitionID))
		}
	case message.OperationDelete:
		l.logger.Info(fmt.Sprintf("Document deleted - documentId: %v, partitionId: %d", lc.Message.DocumentID, lc.PartitionID))
	}

	if err := lc.Ack(); err != nil {
		l.logger.Error(fmt.Sprintf("Failed to acknowledge message: %v", err))
		return err
	}
	return nil
}
