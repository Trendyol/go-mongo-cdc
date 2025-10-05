# Go MongoDB CDC [![Go Reference](https://pkg.go.dev/badge/github.com/Trendyol/go-mongo-cdc.svg)](https://pkg.go.dev/github.com/Trendyol/go-mongo-cdc) [![Go Report Card](https://goreportcard.com/badge/github.com/Trendyol/go-mongo-cdc)](https://goreportcard.com/report/github.com/Trendyol/go-mongo-cdc)

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
    "log"

    cdc "github.com/Trendyol/go-mongo-cdc"
    "github.com/Trendyol/go-mongo-cdc/config"
    "github.com/Trendyol/go-mongo-cdc/mongo/message"
    "github.com/Trendyol/go-mongo-cdc/stream"
    "go.uber.org/zap"
)

func listener(ctx *stream.ListenerContext) error {
    select {
    case <-ctx.Context.Done():
        log.Printf("Shutdown signal received, stopping gracefully")
        return ctx.Context.Err()
    default:
    }

    switch ctx.Message.OperationType {
    case message.OperationInsert:
        log.Printf("New document inserted: %+v", ctx.Message.FullDocument)
    case message.OperationUpdate:
        log.Printf("Document updated: %+v", ctx.Message.FullDocument)
    case message.OperationDelete:
        log.Printf("Document deleted: %+v", ctx.Message.DocumentID)
    }

    return ctx.Ack()
}

func main() {
    cfg := config.Config{
        MongoDB: config.MongoDB{
            Connection: config.Connection{
                URI:        "localhost:27017",
                Database:   "myDB",
                Collection: "myCollection",
            },
        }
    }

    connector, err := cdc.NewConnector(cfg, listener)
    if err != nil {
        log.Fatal("Failed to create CDC connector:", err)
    }

    defer connector.Close()

    connector.Start(context.Background())
}
```

## Usage

```
$ go get github.com/Trendyol/go-mongo-cdc
```

## Configuration

### MongoDB Configuration

#### Connection Settings (`mongodb.connection`)

| Variable                      | Type   | Required | Default | Description                         |
|-------------------------------|--------|----------|---------|-------------------------------------|
| `mongodb.connection.uri`      | string | yes      |         | MongoDB connection URI              |
| `mongodb.connection.database` | string | yes      |         | MongoDB database name               |
| `mongodb.connection.collection` | string | yes    |         | MongoDB collection                  |
| `mongodb.connection.username` | string | no       |         | MongoDB username for authentication |
| `mongodb.connection.password` | string | no       |         | MongoDB password for authentication |

#### Connection Pool Settings (`mongodb.connectionPool`)

| Variable                               | Type   | Required | Default | Description                                       |
|----------------------------------------|--------|----------|---------|---------------------------------------------------|
| `mongodb.connectionPool.maxPoolSize`   | uint64 | no       | 100     | Maximum number of connections in the pool         |
| `mongodb.connectionPool.minPoolSize`   | uint64 | no       | 5       | Minimum number of connections to maintain         |
| `mongodb.connectionPool.maxIdleTimeMS` | int64  | no       | 300000  | Maximum time a connection can remain idle (5 min) |

#### Timeout Settings (`mongodb.timeouts`)

| Variable                                    | Type  | Required | Default | Description                              |
|---------------------------------------------|-------|----------|---------|------------------------------------------|
| `mongodb.timeouts.connectTimeoutMS`         | int64 | no       | 10000   | Connection timeout in milliseconds       |
| `mongodb.timeouts.serverSelectionTimeoutMS` | int64 | no       | 30000   | Server selection timeout in milliseconds |
| `mongodb.timeouts.socketTimeoutMS`          | int64 | no       | 30000   | Socket timeout in milliseconds           |

### Metric Configuration

| Variable                      | Type | Required | Default | Description                                                         |
|-------------------------------|------|----------|---------|---------------------------------------------------------------------|
| `metric.port`                 | int  | no       | 8080    | Prometheus metrics port                                             |
| `metric.enableShardMetricsMapping` | bool | no  | false   | Enable hostname-to-localhost port mapping for sharded clusters (local dev only) |
| `metric.collectionInterval`   | time.Duration | no | 30s | MongoDB metrics collection interval                          |

### Checkpoint Configuration

| Variable                          | Type          | Required | Default         | Description                                  |
|-----------------------------------|---------------|----------|-----------------|----------------------------------------------|
| `checkpoint.tokenSaveInterval`    | time.Duration | no       | 10s             | Token save interval                          |
| `checkpoint.tokenSaveTimeout`     | time.Duration | no       | 10s             | Token save timeout                           |
| `checkpoint.changeStreamBatchSize`| int           | no       | 100             | Change stream batch size                     |
| `checkpoint.bootstrapSaveCount`   | int           | no       | 1000            | Number of documents to process before saving |
| `checkpoint.bootstrapSaveInterval`| time.Duration | no       | 5s              | Bootstrap checkpoint save interval           |
| `checkpoint.idleHeartbeatInterval`| time.Duration | no       | 3m              | Idle heartbeat interval                      |
| `checkpoint.maxIdleTime`          | time.Duration | no       | 15m             | Maximum idle time before partition release   |

### Partition Configuration

| Variable                              | Type          | Required | Default              | Description                       |
|---------------------------------------|---------------|----------|----------------------|-----------------------------------|
| `partition.heartbeatInterval`         | time.Duration | no       | 10s                  | Worker heartbeat interval         |
| `partition.workerTimeout`             | time.Duration | no       | 90s                  | Worker timeout duration           |
| `partition.workersCollection`         | string        | no       | workers              | Workers collection name           |
| `partition.partitionsCollection`      | string        | no       | partition_assignments| Partition assignments collection  |
| `partition.rebalanceCheckInterval`    | time.Duration | no       | 15s                  | Partition rebalance check interval|
| `partition.totalPartition`            | int           | no       | 15                   | Total number of partitions        |

### Logger Configuration

| Variable           | Type   | Required | Default | Description       |
|--------------------|--------|----------|---------|-------------------|
| `logger.logLevel`  | string | no       | info    | Log level         |

### Graceful Shutdown Configuration

| Variable                      | Type          | Required | Default | Description                                                       |
|-------------------------------|---------------|----------|---------|-------------------------------------------------------------------|
| `gracefulShutdownTimeout`     | time.Duration | no       | 30s     | Maximum time to wait for in-flight events to complete on shutdown |

### Configuration Example

```yaml
mongodb:
  connection:
    uri: "localhost:27017"
    database: "exampleDB"
    collection: "exampleCollection"
    username: "user"
    password: "pass"
  connectionPool:
    maxPoolSize: 100
    minPoolSize: 5
    maxIdleTimeMS: 300000
  timeouts:
    connectTimeoutMS: 10000
    serverSelectionTimeoutMS: 30000
    socketTimeoutMS: 30000

metric:
  port: 8080
  enableShardMetricsMapping: false
  collectionInterval: 30s

checkpoint:
  tokenSaveInterval: 10s
  changeStreamBatchSize: 100
  bootstrapSaveCount: 1000
  bootstrapSaveInterval: 5s

partition:
  heartbeatInterval: 10s
  workerTimeout: 90s
  workersCollection: "workers"
  partitionsCollection: "partition_assignments"
  rebalanceCheckInterval: 15s
  totalPartition: 15

logger:
  logLevel: "info"

gracefulShutdownTimeout: 30s
```

## Exposed Metrics

| Metric Name                                       | Type    | Description                                                     | Labels |
|---------------------------------------------------|---------|-----------------------------------------------------------------|--------|
| `go_mongo_cdc_insert_total`                       | Counter | Total number of INSERT operations processed                     | N/A    |
| `go_mongo_cdc_update_total`                       | Counter | Total number of UPDATE operations processed                     | N/A    |
| `go_mongo_cdc_delete_total`                       | Counter | Total number of DELETE operations processed                     | N/A    |
| `go_mongo_cdc_replace_total`                      | Counter | Total number of REPLACE operations processed                    | N/A    |
| `go_mongo_cdc_process_latency_ms_current`         | Gauge   | Current processing latency in milliseconds                      | N/A    |
| `go_mongo_cdc_cdc_latency_ms_current`             | Gauge   | Current CDC latency in milliseconds                             | N/A    |
| `go_mongo_cdc_checkpoint_save_total`              | Counter | Total number of successful checkpoint saves                     | N/A    |
| `go_mongo_cdc_checkpoint_save_error_total`        | Counter | Total number of checkpoint save errors                          | N/A    |
| `go_mongo_cdc_checkpoint_save_latency_ms_current` | Gauge   | Current checkpoint save latency in milliseconds                 | N/A    |
| `go_mongo_cdc_bootstrap_document_total`           | Counter | Total number of documents processed during bootstrap            | N/A    |
| `go_mongo_cdc_bootstrap_progress_percent`         | Gauge   | Bootstrap progress percentage (0-100)                           | N/A    |
| `go_mongo_cdc_bootstrap_active`                   | Gauge   | Bootstrap active status (1=active, 0=inactive)                  | N/A    |
| `go_mongo_cdc_active_partition_count`             | Gauge   | Number of currently active partitions assigned to this worker   | N/A    |
| `go_mongo_cdc_partition_acquire_total`            | Counter | Total number of partition acquisitions                          | N/A    |
| `go_mongo_cdc_partition_release_total`            | Counter | Total number of partition releases                              | N/A    |
| `go_mongo_cdc_resume_token_expired_total`         | Counter | Total number of expired resume tokens                           | N/A    |
| `go_mongo_cdc_change_stream_error_total`          | Counter | Total number of change stream errors                            | N/A    |
| `go_mongo_cdc_change_stream_restart_total`        | Counter | Total number of change stream restarts                          | N/A    |
| `go_mongo_cdc_worker_healthy`                     | Gauge   | Worker health status (1=healthy, 0=unhealthy)                   | N/A    |
| `go_mongo_cdc_last_event_time_seconds`            | Gauge   | Unix timestamp of the last processed event                      | N/A    |
| `go_mongo_cdc_event_lag_duration_seconds`         | Gauge   | Duration in seconds since the last event was processed          | N/A    |
| `go_mongo_cdc_mongodb_oplog_size_bytes`           | Gauge   | MongoDB oplog size in bytes                                     | N/A    |
| `go_mongo_cdc_mongodb_oplog_used_bytes`           | Gauge   | MongoDB oplog used size in bytes                                | N/A    |
| `go_mongo_cdc_mongodb_oplog_used_percent`         | Gauge   | MongoDB oplog used percentage (0-100)                           | N/A    |
| `go_mongo_cdc_mongodb_oplog_window_seconds`       | Gauge   | MongoDB oplog time window in seconds (retention)                | N/A    |
| `go_mongo_cdc_mongodb_replication_lag_seconds`    | Gauge   | MongoDB replication lag in seconds                              | N/A    |
| `go_mongo_cdc_mongodb_connections_active`         | Gauge   | MongoDB active connections                                      | N/A    |
| `go_mongo_cdc_mongodb_connections_available`      | Gauge   | MongoDB available connections                                   | N/A    |

## Examples

- [Simple Example](example/simple/main.go)
- [Docker Compose Setup](example/simple/docker-compose.yml)

## Contributing

Go MongoDB CDC is always open for direct contributions. For more information please check our [Contribution Guideline document](./CONTRIBUTING.md).

## License

Released under the [MIT License](LICENSE).