# Go MongoDB Change Data Capture (CDC)

Go MongoDB CDC is a high-performance library that captures and processes real-time changes in your MongoDB database. This library monitors data changes using MongoDB Change Streams API and processes these changes in a scalable manner.

## 📋 Table of Contents

- [Features](#features)
- [Installation](#installation)
- [Quick Start](#quick-start)
- [Configuration](#configuration)
- [Architecture](#architecture)
- [Monitoring and Metrics](#monitoring-and-metrics)
- [Docker Usage](#docker-usage)
- [API Reference](#api-reference)

## ✨ Features

- **Real-time Data Capture**: Instantly captures INSERT, UPDATE, DELETE, and REPLACE operations using MongoDB Change Streams
- **Scalable Partition System**: Intelligent partition management that distributes workload across multiple workers
- **Automatic Failover**: Automatic partition transfer and load balancing between workers
- **Resume Token Support**: Ability to resume from where it left off after system interruptions
- **Smart Resume Token Recovery**: Automatic handling of expired/invalid resume tokens with graceful fallback
- **Universal Hash Distribution**: Advanced multi-hash algorithm ensuring optimal distribution for any ID pattern
- **Bootstrap Mode**: Full collection scanning feature for processing existing data for the first time
- **Prometheus Metrics**: Detailed performance and system metrics
- **Flexible Configuration**: Easy configuration with YAML and JSON files
- **Reliable Checkpoint System**: Advanced checkpoint mechanism to prevent data loss
- **Replica Set and Sharded Cluster Support**: Works with both single replica sets and sharded clusters

## 🚀 Installation

```bash
go get github.com/Trendyol/go-mongo-cdc
```

## 🔄 Recent Improvements

### Enhanced Reliability & Performance

- **Universal Hash Algorithm**: Completely redesigned partitioning system using multi-hash approach for optimal distribution across all ID patterns
- **Smart Resume Token Recovery**: Automatic detection and recovery from expired resume tokens without manual intervention
- **Zero-Downtime Error Handling**: Graceful fallback to current operation time when resume tokens become invalid
- **Improved Troubleshooting**: Enhanced error messages and automatic resolution for common issues

## ⚡ Quick Start

### 1. Basic Usage

```go
package main

import (
    "context"
    "log"
    "time"

    cdc "github.com/Trendyol/go-mongo-cdc"
    "github.com/Trendyol/go-mongo-cdc/config"
    "github.com/Trendyol/go-mongo-cdc/mongo/message"
    "github.com/Trendyol/go-mongo-cdc/stream"
    "go.uber.org/zap"
)

func main() {
    logger, _ := zap.NewDevelopment()
    defer logger.Sync()

    cfg := config.Config{
        Host:       "localhost",
        Port:       27017,
        Database:   "myDB",
        Collection: "myCollection",
        Logger: config.LoggerConfig{
            Logger: logger,
        },
    }

    connector, err := cdc.NewConnector(context.Background(), cfg, processChangeEvent)
    if err != nil {
        log.Fatal("Failed to create CDC connector:", err)
    }
    defer connector.Close()

    connector.Start(context.Background())
}

func processChangeEvent(ctx *stream.ListenerContext) error {
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
```

### 2. Configuration File Usage

**config.yaml:**
```yaml
host: localhost
port: 27017
database: myDB
collection: myCollection
username: myUser
password: myPassword
authDatabase: admin

metric:
  port: 8080

checkpoint:
  collection: "cdc_checkpoints"
  saveInterval: 30s

partition:
  heartbeatInterval: 5s
  workerTimeout: 30s
  partitionDatabase: "cdc_partitions"
  refreshInterval: 30s

logger:
  logLevel: 1
```

```go
connector, err := cdc.NewConnectorWithConfigFile(
    context.Background(),
    "config.yaml",
    processChangeEvent,
)
```

## ⚙️ Configuration

### Config Struct

| Field | Type | Description | Default |
|-------|------|-------------|---------|
| `Host` | string | MongoDB host address | - |
| `Port` | int | MongoDB port number | 27017 |
| `Username` | string | MongoDB username | - |
| `Password` | string | MongoDB password | - |
| `Database` | string | Database to monitor | - |
| `Collection` | string | Collection to monitor | - |
| `AuthDatabase` | string | Authentication database | admin |

### Metric Configuration

| Field | Type | Description | Default |
|-------|------|-------------|---------|
| `Port` | int | Prometheus metrics port | 8080 |

### Checkpoint Configuration

| Field | Type | Description | Default |
|-------|------|-------------|---------|
| `Collection` | string | Checkpoint collection name | cdc_checkpoints |
| `SaveInterval` | duration | Token save interval | 30s |

### Partition Configuration

| Field | Type | Description | Default |
|-------|------|-------------|---------|
| `HeartbeatInterval` | duration | Worker heartbeat interval | 5s |
| `WorkerTimeout` | duration | Worker timeout duration | 30s |
| `PartitionDatabase` | string | Partition database | cdc_partitions |
| `RefreshInterval` | duration | Partition refresh interval | 30s |

### Enhanced Partitioning

The system uses a universal multi-hash algorithm that ensures optimal distribution for any string pattern:
- **Triple-hash approach**: Combines DJB2, polynomial rolling, and position-weighted hashing
- **Pattern-agnostic**: Works effectively with any ID format (UUIDs, incremental IDs, custom formats)
- **Anti-collision**: Multiple hash functions minimize clustering even for similar inputs
- **High entropy**: Incorporates string length, position weights, and boundary characters

### Resume Token Management

Robust handling of resume token lifecycle with automatic recovery:
- **Automatic Expiry Detection**: Detects expired or invalid resume tokens
- **Graceful Fallback**: Automatically starts from current operation time when resume fails
- **Zero Downtime Recovery**: Seamless continuation without manual intervention
- **Smart Cleanup**: Removes invalid tokens from checkpoint storage

## 🏗️ Architecture

Go MongoDB CDC consists of the following core components:

```mermaid
graph TB
    subgraph "Go MongoDB CDC System"
        subgraph "Client Application"
            APP[Go Application]
            LISTENER[Event Listener Function]
        end
        
        subgraph "CDC Core"
            CONNECTOR[Connector]
            STREAM[Partition Stream Manager]
            PARTITION[Partition Manager]
            CHECKPOINT[Checkpoint Manager]
        end
        
        subgraph "MongoDB Cluster"
            MONGO[(MongoDB<br/>Replica Set)]
            CHANGESTREAM[Change Streams]
            CDCDB[(CDC Database<br/>Checkpoints & Partitions)]
        end
        
        subgraph "Monitoring"
            METRICS[Prometheus Metrics]
            GRAFANA[Grafana Dashboard]
        end
        
        subgraph "Multi-Worker Architecture"
            WORKER1[Worker 1<br/>Partitions: 0,1,2]
            WORKER2[Worker 2<br/>Partitions: 3,4,5]
            WORKER3[Worker 3<br/>Partitions: 6,7,8,9]
        end
    end
    
    APP --> CONNECTOR
    CONNECTOR --> STREAM
    STREAM --> PARTITION
    STREAM --> CHECKPOINT
    STREAM --> LISTENER
    
    PARTITION --> WORKER1
    PARTITION --> WORKER2
    PARTITION --> WORKER3
    
    WORKER1 --> CHANGESTREAM
    WORKER2 --> CHANGESTREAM
    WORKER3 --> CHANGESTREAM
    
    CHANGESTREAM --> MONGO
    CHECKPOINT --> CDCDB
    PARTITION --> CDCDB
    
    STREAM --> METRICS
    METRICS --> GRAFANA
    
    classDef primary fill:#e1f5fe,stroke:#01579b,stroke-width:2px
    classDef secondary fill:#f3e5f5,stroke:#4a148c,stroke-width:2px
    classDef database fill:#e8f5e8,stroke:#1b5e20,stroke-width:2px
    classDef worker fill:#fff3e0,stroke:#e65100,stroke-width:2px
    
    class CONNECTOR,STREAM primary
    class PARTITION,CHECKPOINT secondary
    class MONGO,CDCDB database
    class WORKER1,WORKER2,WORKER3 worker
```

### Components

1. **Connector**: Main component that manages the entire system
2. **Partition Manager**: Manages partition distribution between workers with automatic failover
3. **Stream Manager**: Manages MongoDB Change Streams with resume token recovery
4. **Checkpoint Manager**: Stores resume tokens and progress state with automatic cleanup
5. **Hash Engine**: Universal multi-hash algorithm for optimal data distribution
6. **Recovery System**: Automatic detection and handling of invalid resume tokens
7. **Metric System**: Provides Prometheus metrics for monitoring and alerting

### Partition System

The system divides the data load into 10 partitions using an advanced universal hash algorithm. Each partition is determined based on the multi-hash value of document IDs. This enables:

- **Optimal Distribution**: Works with any ID pattern (UUIDs, incremental, custom formats)
- **Parallel Processing**: True parallel capability across multiple workers
- **Automatic Load Balancing**: Even distribution regardless of ID patterns
- **Failover Between Workers**: Seamless worker replacement
- **Scalable Performance**: Linear performance scaling with worker count

### Bootstrap Process

When run for the first time, the system:

1. Scans all existing documents
2. Processes each document as a synthetic INSERT event
3. Starts tracking real-time changes after bootstrap completion
4. Resumes from where it left off if interrupted during bootstrap

## 📊 Monitoring and Metrics

The system provides comprehensive Prometheus metrics for monitoring and alerting:

### Operation Metrics

| Metric Name | Type | Description |
|-------------|------|-------------|
| `go_mongo_cdc_insert_total` | Counter | Total number of INSERT operations processed |
| `go_mongo_cdc_update_total` | Counter | Total number of UPDATE operations processed |
| `go_mongo_cdc_delete_total` | Counter | Total number of DELETE operations processed |
| `go_mongo_cdc_replace_total` | Counter | Total number of REPLACE operations processed |

### Latency Metrics

| Metric Name | Type | Description |
|-------------|------|-------------|
| `go_mongo_cdc_process_latency_ms_current` | Gauge | Current processing latency in milliseconds |
| `go_mongo_cdc_cdc_latency_ms_current` | Gauge | Current CDC latency (time between event creation and processing) in milliseconds |

### Checkpoint Metrics

| Metric Name | Type | Description |
|-------------|------|-------------|
| `go_mongo_cdc_checkpoint_save_total` | Counter | Total number of successful checkpoint saves |
| `go_mongo_cdc_checkpoint_save_error_total` | Counter | Total number of checkpoint save errors |
| `go_mongo_cdc_checkpoint_save_latency_ms_current` | Gauge | Current checkpoint save latency in milliseconds |

### Bootstrap Metrics

| Metric Name | Type | Description |
|-------------|------|-------------|
| `go_mongo_cdc_bootstrap_document_total` | Counter | Total number of documents processed during bootstrap |
| `go_mongo_cdc_bootstrap_progress_percent` | Gauge | Bootstrap progress percentage (0-100) |
| `go_mongo_cdc_bootstrap_active` | Gauge | Bootstrap active status (1=active, 0=inactive) |

### Partition Metrics

| Metric Name | Type | Description |
|-------------|------|-------------|
| `go_mongo_cdc_active_partition_count` | Gauge | Number of currently active partitions assigned to this worker |
| `go_mongo_cdc_partition_acquire_total` | Counter | Total number of partition acquisitions |
| `go_mongo_cdc_partition_release_total` | Counter | Total number of partition releases |

### Error & Recovery Metrics

| Metric Name | Type | Description |
|-------------|------|-------------|
| `go_mongo_cdc_resume_token_expired_total` | Counter | Total number of expired resume tokens (automatic recovery triggered) |
| `go_mongo_cdc_change_stream_error_total` | Counter | Total number of change stream errors |
| `go_mongo_cdc_change_stream_restart_total` | Counter | Total number of change stream restarts |

### Health Metrics

| Metric Name | Type | Description |
|-------------|------|-------------|
| `go_mongo_cdc_worker_healthy` | Gauge | Worker health status (1=healthy, 0=unhealthy) |
| `go_mongo_cdc_last_event_time_seconds` | Gauge | Unix timestamp of the last processed event |
| `go_mongo_cdc_event_lag_duration_seconds` | Gauge | Duration in seconds since the last event was processed |

### System Metrics

| Metric Name | Type | Description |
|-------------|------|-------------|
| `go_mongo_cdc_build_info` | Gauge | Build information (labels: version, go_version) |

### Metrics Endpoints

The following HTTP endpoints are available:

- **Metrics**: `:8080/metrics` - Prometheus metrics endpoint
- **Health**: `:8080/health` - Health check endpoint (returns 200 OK)
- **Ready**: `:8080/ready` - Readiness check endpoint (returns 200 OK)

```bash
curl http://localhost:8080/metrics
curl http://localhost:8080/health
curl http://localhost:8080/ready
```

### Alerting Examples

Example Prometheus alerting rules:

```yaml
groups:
  - name: mongodb_cdc_alerts
    interval: 30s
    rules:
      - alert: CDCHighLatency
        expr: go_mongo_cdc_cdc_latency_ms_current > 5000
        for: 5m
        labels:
          severity: warning
        annotations:
          summary: "CDC latency is high"
          description: "CDC latency is {{ $value }}ms, which exceeds 5 seconds"
      
      - alert: CDCCheckpointErrors
        expr: rate(go_mongo_cdc_checkpoint_save_error_total[5m]) > 0.1
        for: 5m
        labels:
          severity: critical
        annotations:
          summary: "High checkpoint error rate"
          description: "Checkpoint save error rate is {{ $value }} per second"
      
      - alert: CDCResumeTokenExpired
        expr: increase(go_mongo_cdc_resume_token_expired_total[15m]) > 5
        for: 5m
        labels:
          severity: warning
        annotations:
          summary: "Resume tokens expiring frequently"
          description: "{{ $value }} resume tokens expired in the last 15 minutes"
      
      - alert: CDCNoRecentEvents
        expr: time() - go_mongo_cdc_last_event_time_seconds > 600
        for: 5m
        labels:
          severity: warning
        annotations:
          summary: "No recent CDC events"
          description: "No events processed for {{ $value }} seconds"
      
      - alert: CDCWorkerUnhealthy
        expr: go_mongo_cdc_worker_healthy == 0
        for: 2m
        labels:
          severity: critical
        annotations:
          summary: "CDC worker is unhealthy"
          description: "CDC worker health check is failing"
```

## 🐳 Docker Usage

### MongoDB Replica Set Setup

```yaml
version: '3.8'
services:
  mongodb:
    image: mongo:6.0
    container_name: my-mongo
    restart: always
    ports:
      - "27017:27017"
    volumes:
      - mongo-data:/data/db
    command: --replSet rs0 --bind_ip_all --noauth

  mongodb-init:
    image: mongo:6.0
    depends_on:
      - mongodb
    command: >
      bash -c "
        echo 'Waiting for MongoDB to start...' &&
        sleep 15 &&
        mongosh mongodb://mongodb:27017/admin --eval \"
          rs.initiate({
            _id: 'rs0',
            members: [{
              _id: 0,
              host: 'localhost:27017'
            }]
          })
        \"
      "

volumes:
  mongo-data:
```

### Usage

```bash
# Start MongoDB
docker-compose up -d

# Run the application
go run main.go
```

## 🛠️ Development

### Requirements

- Go 1.24.2+
- MongoDB 4.0+ (Replica Set or Sharded Cluster)

### Build

```bash
# Install dependencies
go mod tidy

# Run tests
make test

# Run linter
make lint

# Run linter with fixes
make linter
```

### Testing

```bash
# Run all tests
go test ./...

# Run benchmark tests
go test ./... -bench . -benchmem
```

## 🔧 Troubleshooting

### Common Issues

1. **"MongoDB is not running as a replica set" Error**
   - Start MongoDB in replica set mode
   - Use `--replSet` parameter when starting

2. **"Failed to acquire partitions" Error**
   - Check MongoDB connection
   - Verify worker timeout settings

3. **"Resume token was not found" Error**
   - **Automatic Recovery**: The system now automatically handles this error
   - **No Action Required**: Invalid tokens are cleared and stream resumes from current time
   - **Monitoring**: Check logs for "Resume token expired or invalid" warnings

4. **High Memory Usage**
   - Normal during bootstrap process
   - Lower the `saveInterval` value

5. **Poor Partition Distribution**
   - **Automatic Optimization**: New universal hash algorithm provides better distribution
   - **Works with any ID pattern**: UUIDs, incremental IDs, colon-separated IDs
   - **No Configuration Needed**: Optimal distribution is automatic

## 📄 API Reference

### Connector Interface

```go
type Connector interface {
    Start(ctx context.Context)
    Close()
}
```

### ListenerFunc

```go
type ListenerFunc func(ctx *ListenerContext) error

type ListenerContext struct {
    Message     message.Message
    PartitionID int
    Ack         func() error
}
```

### Message Struct

```go
type Message struct {
    OperationType OperationType
    ClusterTime   primitive.Timestamp
    Database      string
    Collection    string
    DocumentID    interface{}
    FullDocument  bson.M
    OldDocument   bson.M
    EventTime     time.Time
}
```