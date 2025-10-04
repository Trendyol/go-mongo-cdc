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
		MongoDB: config.MongoDB{
			Connection: config.Connection{
				URI:        "localhost:27017",
				Database:   "exampleDB",
				Collection: "exampleCollection",
			},
			ConnectionPool: config.ConnectionPool{
				MaxPoolSize:   100,
				MinPoolSize:   5,
				MaxIdleTimeMS: 300000, // 5 minutes
			},
			Timeouts: config.Timeouts{
				ConnectTimeoutMS:         10000, // 10 seconds
				ServerSelectionTimeoutMS: 30000, // 30 seconds
				SocketTimeoutMS:          30000, // 30 seconds
			},
		},
		Metric: config.MetricConfig{
			Port:                      8080,
			EnableShardMetricsMapping: true, // Enable for local development with docker-compose
		},
		Checkpoint: config.CheckpointConfig{
			Collection:            "cdc_checkpoints",
			TokenSaveInterval:     10 * time.Second,
			ChangeStreamBatchSize: 1,
			BootstrapSaveCount:    1000,
			BootstrapSaveInterval: 5 * time.Second,
		},
		Partition: config.PartitionConfig{
			HeartbeatInterval:      10 * time.Second,
			WorkerTimeout:          90 * time.Second,
			PartitionDatabase:      "exampleDB",
			RebalanceCheckInterval: 15 * time.Second,
			TotalPartition:         30,
		},
		Logger: config.LoggerConfig{
			Logger: logger,
		},
	}

	myListener := &CDCListener{
		logger: logger,
	}

	connector, err := cdc.NewConnector(cfg, myListener.ProcessChangeEvent)
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
