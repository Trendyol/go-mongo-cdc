# Go MongoDB CDC [![Go Reference](https://pkg.go.dev/badge/github.com/Trendyol/go-mongo-cdc.svg)](https://pkg.go.dev/github.com/Trendyol/go-mongo-cdc) [![Go Report Card](https://goreportcard.com/badge/github.com/Trendyol/go-mongo-cdc)](https://goreportcard.com/report/github.com/Trendyol/go-mongo-cdc) [![OpenSSF Scorecard](https://api.scorecard.dev/projects/github.com/Trendyol/go-mongo-cdc/badge)](https://scorecard.dev/viewer/?uri=github.com/Trendyol/go-mongo-cdc)

**Go MongoDB CDC** captures and processes real-time changes from MongoDB using Change Streams.

## Features

* **Real-time Data Capture** - Instantly captures INSERT, UPDATE, DELETE and REPLACE operations using MongoDB Change Streams API
* **Scalable Partition System** - Intelligent partition management that distributes workload across multiple workers
* **Automatic Failover** - Automatic partition transfer and load balancing between workers
* **Oplog Rollover Protection** - Automatic re-snapshot mechanism when oplog history is lost, ensuring zero data loss (similar to Debezium's snapshot.mode = "when_needed")
* **Smart Resume Token Recovery** - Automatic detection and recovery from expired/invalid resume tokens with graceful fallback
* **Universal Hash Distribution** - Advanced multi-hash algorithm ensuring optimal distribution for any ID pattern
* **Bootstrap Mode** - Full collection scanning feature for processing existing data on first run
* **Advanced Checkpoint System** - Comprehensive checkpoint mechanism to prevent data loss
* **Sharded Cluster Support** - Works with both replica sets and sharded clusters
* **Comprehensive Metrics** - Detailed performance and system metrics with Prometheus
* **Easily manageable configurations** - Simple configuration with YAML and JSON files

## Example

```go
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
				MaxIdleTimeMS: 300000,
			},
			Timeouts: config.Timeouts{
				ConnectTimeoutMS:         30000,
				ServerSelectionTimeoutMS: 60000,
				SocketTimeoutMS:          120000,
			},
		},
		Metric: config.MetricConfig{
			Port: 8080,
		},
	Checkpoint: config.CheckpointConfig{
		TokenSaveInterval:       10 * time.Second,
		ChangeStreamSaveCount:   500,
		BootstrapSaveCount:      5000,
		BootstrapSaveInterval:   10 * time.Second,
		BootstrapQueryBatchSize: 5000,
	},
		Partition: config.PartitionConfig{
			HeartbeatInterval:      10 * time.Second,
			WorkerTimeout:          90 * time.Second,
			RebalanceCheckInterval: 10 * time.Second,
			TotalPartition:         30,
		},
		Logger: config.LoggerConfig{LogLevel: "debug"},
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
```

## Usage

```
$ go get github.com/Trendyol/go-mongo-cdc
```

## Configuration

### MongoDB Configuration

#### Connection Settings (`mongodb.connection`)

| Variable                        | Type   | Required | Default | Description                         |
|---------------------------------|--------|----------|---------|-------------------------------------|
| `mongodb.connection.uri`        | string | yes      |         | MongoDB connection URI              |
| `mongodb.connection.database`   | string | yes      |         | MongoDB database name               |
| `mongodb.connection.collection` | string | yes      |         | MongoDB collection                  |
| `mongodb.connection.username`   | string | no       |         | MongoDB username for authentication |
| `mongodb.connection.password`   | string | no       |         | MongoDB password for authentication |

#### Connection Pool Settings (`mongodb.connectionPool`)

| Variable                               | Type   | Required | Default | Description                                       |
|----------------------------------------|--------|----------|---------|---------------------------------------------------|
| `mongodb.connectionPool.maxPoolSize`   | uint64 | no       | 100     | Maximum number of connections in the pool         |
| `mongodb.connectionPool.minPoolSize`   | uint64 | no       | 5       | Minimum number of connections to maintain         |
| `mongodb.connectionPool.maxIdleTimeMS` | int64  | no       | 300000  | Maximum time a connection can remain idle (5 min) |

#### Timeout Settings (`mongodb.timeouts`)

| Variable                                    | Type  | Required | Default | Description                              |
|---------------------------------------------|-------|----------|---------|------------------------------------------|
| `mongodb.timeouts.connectTimeoutMS`         | int64 | no       | 30000   | Connection timeout in milliseconds       |
| `mongodb.timeouts.serverSelectionTimeoutMS` | int64 | no       | 60000   | Server selection timeout in milliseconds |
| `mongodb.timeouts.socketTimeoutMS`          | int64 | no       | 120000  | Socket timeout in milliseconds           |

### Metric Configuration

**Note:** Monitoring oplog is critical for Change Streams. If the oplog window is too small or oplog history is lost, Change Streams may fail. We recommend using MongoDB's native metrics exposed through MongoDB Exporter or MongoDB's built-in monitoring tools to track oplog health (size, used percentage, window).

| Variable      | Type | Required | Default | Description             |
|---------------|------|----------|---------|-------------------------|
| `metric.port` | int  | no       | 8080    | Prometheus metrics port |

### Checkpoint Configuration

| Variable                             | Type          | Required | Default | Description                                                                              |
|--------------------------------------|---------------|----------|---------|------------------------------------------------------------------------------------------|
| `checkpoint.type`                    | string        | no       | auto    | Checkpoint mode: "auto" (periodic) or "manual" (explicit Commit() call required)         |
| `checkpoint.tokenSaveInterval`       | time.Duration | no       | 10s     | Token save interval (only for auto mode)                                                 |
| `checkpoint.tokenSaveTimeout`        | time.Duration | no       | 10s     | Token save timeout                                                                       |
| `checkpoint.changeStreamSaveCount`   | int           | no       | 500     | Change stream batch size                                                                 |
| `checkpoint.bootstrapSaveCount`      | int           | no       | 2500    | Number of documents to process before saving                                             |
| `checkpoint.bootstrapSaveInterval`   | time.Duration | no       | 10s     | Bootstrap checkpoint save interval (only for auto mode)                                  |
| `checkpoint.bootstrapQueryBatchSize` | int           | no       | 2500    | Bootstrap batch size                                                                     |
| `checkpoint.idleHeartbeatInterval`   | time.Duration | no       | 3m      | Idle heartbeat interval                                                                  |
| `checkpoint.maxIdleTime`             | time.Duration | no       | 15m     | Maximum idle time before partition release                                               |

### Partition Configuration

| Variable                           | Type          | Required | Default               | Description                                                                                                                                                                                                                                                         |
|------------------------------------|---------------|----------|-----------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `partition.heartbeatInterval`      | time.Duration | no       | 10s                   | Worker heartbeat interval                                                                                                                                                                                                                                           |
| `partition.workerTimeout`          | time.Duration | no       | 90s                   | Worker timeout duration                                                                                                                                                                                                                                             |
| `partition.workersCollection`      | string        | no       | workers               | Workers collection name                                                                                                                                                                                                                                             |
| `partition.partitionsCollection`   | string        | no       | partition_assignments | Partition assignments collection                                                                                                                                                                                                                                    |
| `partition.rebalanceCheckInterval` | time.Duration | no       | 10s                   | Partition rebalance check interval                                                                                                                                                                                                                                  |
| `partition.totalPartition`         | int           | no       | 15                    | Total number of partitions. **Important:** This value is stored on first initialization and cannot be changed afterwards. Changing this value after initialization will cause system startup failures as it disrupts partition distribution balance across workers. |

### Logger Configuration

| Variable           | Type   | Required | Default | Description       |
|--------------------|--------|----------|---------|-------------------|
| `logger.logLevel`  | string | no       | info    | Log level         |

### Graceful Shutdown Configuration

| Variable                      | Type          | Required | Default | Description                                                       |
|-------------------------------|---------------|----------|---------|-------------------------------------------------------------------|
| `gracefulShutdownTimeout`     | time.Duration | no       | 10s     | Maximum time to wait for in-flight events to complete on shutdown |

### Configuration Example

```yaml
cdcconfig:
  mongodb:
    connection:
      uri: "localhost:27017"
      database: exampleDB
      collection: exampleCollection
    connectionPool:
      maxPoolSize: 100
      minPoolSize: 3
      maxIdleTimeMS: 300000

  metric:
    port: 8085

  checkpoint:
    bootstrapSaveCount: 5000
    bootstrapQueryBatchSize: 5000
    bootstrapSaveInterval: 10s
    tokenSaveInterval: 10s
    changeStreamSaveCount: 500

  partition:
    heartbeatInterval: 10s
    workerTimeout: 90s
    rebalanceCheckInterval: 10s
    totalPartition: 15
    workersCollection: "workers"
    partitionsCollection: "partition_assignments"

  logger:
    logLevel: "debug"

gracefulShutdownTimeout: 5s

appPort: :8080
```

## Exposed Metrics

| Metric Name                                          | Type    | Description                                                   | Labels              |
|------------------------------------------------------|---------|---------------------------------------------------------------|---------------------|
| `go_mongo_cdc_insert_total`                          | Counter | Total number of INSERT operations processed                   | N/A                 |
| `go_mongo_cdc_update_total`                          | Counter | Total number of UPDATE operations processed                   | N/A                 |
| `go_mongo_cdc_delete_total`                          | Counter | Total number of DELETE operations processed                   | N/A                 |
| `go_mongo_cdc_replace_total`                         | Counter | Total number of REPLACE operations processed                  | N/A                 |
| `go_mongo_cdc_cdc_latency_seconds`                   | Gauge   | Current CDC latency in seconds                                | N/A                 |
| `go_mongo_cdc_checkpoint_save_total`                 | Counter | Total number of successful checkpoint saves                   | N/A                 |
| `go_mongo_cdc_checkpoint_save_error_total`           | Counter | Total number of checkpoint save errors                        | N/A                 |
| `go_mongo_cdc_checkpoint_save_latency_seconds`       | Gauge   | Current checkpoint save latency in seconds                    | N/A                 |
| `go_mongo_cdc_bootstrap_document_total`              | Counter | Total number of documents processed during bootstrap          | N/A                 |
| `go_mongo_cdc_bootstrap_active`                      | Gauge   | Bootstrap active status (1=active, 0=inactive)                | N/A                 |
| `go_mongo_cdc_active_partition_count`                | Gauge   | Number of currently active partitions assigned to this worker | N/A                 |
| `go_mongo_cdc_partition_acquire_total`               | Counter | Total number of partition acquisitions                        | N/A                 |
| `go_mongo_cdc_partition_release_total`               | Counter | Total number of partition releases                            | N/A                 |
| `go_mongo_cdc_resume_token_expired_total`            | Counter | Total number of expired resume tokens                         | N/A                 |
| `go_mongo_cdc_change_stream_error_total`             | Counter | Total number of change stream errors                          | N/A                 |
| `go_mongo_cdc_change_stream_restart_total`           | Counter | Total number of change stream restarts                        | N/A                 |
| `go_mongo_cdc_last_event_time_seconds`               | Gauge   | Unix timestamp of the last processed event                    | N/A                 |
| `go_mongo_cdc_event_lag_seconds`                     | Gauge   | Duration in seconds since the last event was processed        | N/A                 |
| `go_mongo_cdc_listener_latency_seconds`              | Gauge   | Listener function execution latency in seconds                | N/A                 |
| `go_mongo_cdc_time_since_last_checkpoint_seconds`    | Gauge   | Time since last checkpoint was saved in seconds               | N/A                 |
| `go_mongo_cdc_listener_error_total`                  | Counter | Total number of listener function errors                      | N/A                 |
| `go_mongo_cdc_partition_rebalance_total`             | Counter | Total number of partition rebalance operations                | N/A                 |
| `go_mongo_cdc_build_info`                            | Gauge   | Build information                                             | version, go_version |

## Examples

- [Simple Example](example/simple/main.go)
- [Grafana Dashboard Example](example/grafana/main.go)

## Contributing

Go MongoDB CDC is always open for direct contributions. For more information please check our [Contribution Guideline document](./CONTRIBUTING.md).

## License

Released under the [MIT License](LICENSE).