package main

import (
	"context"

	"github.com/Trendyol/go-mongo-cdc/logger"

	cdc "github.com/Trendyol/go-mongo-cdc"

	"log"
	"time"

	"github.com/Trendyol/go-mongo-cdc/config"
	"github.com/Trendyol/go-mongo-cdc/mongo/message"
	"github.com/Trendyol/go-mongo-cdc/stream"
)

func main() {
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
				ConnectTimeoutMS:         30000,  // 30 seconds
				ServerSelectionTimeoutMS: 60000,  // 60 seconds
				SocketTimeoutMS:          120000, // 120 seconds
			},
		},
		Metric: config.MetricConfig{
			Port:                      8080,
			EnableShardMetricsMapping: true, // Enable for local development with docker-compose
		},
		Checkpoint: config.CheckpointConfig{
			TokenSaveInterval:     10 * time.Second,
			ChangeStreamBatchSize: 1,
			BootstrapSaveCount:    1000,
			BootstrapSaveInterval: 5 * time.Second,
			BootstrapBatchSize:    500,
		},
		Partition: config.PartitionConfig{
			HeartbeatInterval:      10 * time.Second,
			WorkerTimeout:          90 * time.Second,
			RebalanceCheckInterval: 15 * time.Second,
			TotalPartition:         30,
		},
	}

	connector, err := cdc.NewConnector(cfg, ProcessChangeEvent)
	if err != nil {
		log.Fatal("failed to create connector:", err)
	}

	defer connector.Close()

	ctx := context.Background()
	connector.Start(ctx)
}

func ProcessChangeEvent(lc *stream.ListenerContext) error {
	select {
	case <-lc.Context.Done():
		logger.Log.Info("Shutdown signal received, stopping event processing")
		return lc.Context.Err()
	default:
	}

	switch lc.Message.OperationType {
	case message.OperationInsert, message.OperationUpdate, message.OperationReplace:
		if lc.Message.FullDocument != nil {
			logger.Log.Info("Document changed - operation: %s, document: %v, partitionId: %d", string(lc.Message.OperationType), lc.Message.DocumentID, lc.PartitionID)
		}
	case message.OperationDelete:
		logger.Log.Info("Document deleted - documentId: %v, partitionId: %d", lc.Message.DocumentID, lc.PartitionID)
	}

	if err := lc.Ack(); err != nil {
		logger.Log.Error("Failed to acknowledge message: %v", err)
		return err
	}
	return nil
}
