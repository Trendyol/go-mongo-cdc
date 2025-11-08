package config

import (
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/Trendyol/go-mongo-cdc/logger"

	"gopkg.in/yaml.v2"
)

type Config struct {
	MongoDB                 MongoDB          `json:"mongodb" yaml:"mongodb"`
	Metric                  MetricConfig     `json:"metric" yaml:"metric"`
	Logger                  LoggerConfig     `json:"logger" yaml:"logger"`
	Checkpoint              CheckpointConfig `json:"checkpoint" yaml:"checkpoint"`
	Partition               PartitionConfig  `json:"partition" yaml:"partition"`
	GracefulShutdownTimeout time.Duration    `json:"gracefulShutdownTimeout" yaml:"gracefulShutdownTimeout"`
}

type MongoDB struct {
	Connection     Connection     `json:"connection" yaml:"connection"`
	Timeouts       Timeouts       `json:"timeouts" yaml:"timeouts"`
	ConnectionPool ConnectionPool `json:"connectionPool" yaml:"connectionPool"`
}

type Connection struct {
	URI        string `json:"uri" yaml:"uri"`
	Username   string `json:"username" yaml:"username"`
	Password   string `json:"password" yaml:"password"`
	Database   string `json:"database" yaml:"database"`
	Collection string `json:"collection" yaml:"collection"`
}

type ConnectionPool struct {
	MaxPoolSize   uint64 `json:"maxPoolSize" yaml:"maxPoolSize"`
	MinPoolSize   uint64 `json:"minPoolSize" yaml:"minPoolSize"`
	MaxIdleTimeMS int64  `json:"maxIdleTimeMS" yaml:"maxIdleTimeMS"`
}

type Timeouts struct {
	ConnectTimeoutMS         int64 `json:"connectTimeoutMS" yaml:"connectTimeoutMS"`
	ServerSelectionTimeoutMS int64 `json:"serverSelectionTimeoutMS" yaml:"serverSelectionTimeoutMS"`
	SocketTimeoutMS          int64 `json:"socketTimeoutMS" yaml:"socketTimeoutMS"`
}

type MetricConfig struct {
	Port               int           `json:"port" yaml:"port"`
	CollectionInterval time.Duration `json:"collectionInterval" yaml:"collectionInterval"`
}

type LoggerConfig struct {
	LogLevel string `json:"logLevel" yaml:"logLevel"`
}

type CheckpointConfig struct {
	Type                    string        `json:"type" yaml:"type"`
	TokenSaveTimeout        time.Duration `json:"saveTimeout" yaml:"saveTimeout"`
	TokenSaveInterval       time.Duration `json:"tokenSaveInterval" yaml:"tokenSaveInterval"`
	ChangeStreamSaveCount   int           `json:"changeStreamSaveCount" yaml:"changeStreamSaveCount"`
	BootstrapSaveCount      int           `json:"bootstrapSaveCount" yaml:"bootstrapSaveCount"`
	BootstrapSaveInterval   time.Duration `json:"bootstrapSaveInterval" yaml:"bootstrapSaveInterval"`
	BootstrapQueryBatchSize int           `json:"bootstrapQueryBatchSize" yaml:"bootstrapQueryBatchSize"`
	IdleHeartbeatInterval   time.Duration `json:"idleHeartbeatInterval" yaml:"idleHeartbeatInterval"`
	MaxIdleTime             time.Duration `json:"maxIdleTime" yaml:"maxIdleTime"`
}

type PartitionConfig struct {
	HeartbeatInterval      time.Duration `json:"heartbeatInterval" yaml:"heartbeatInterval"`
	WorkerTimeout          time.Duration `json:"workerTimeout" yaml:"workerTimeout"`
	WorkersCollection      string        `json:"workersCollection" yaml:"workersCollection"`
	PartitionsCollection   string        `json:"partitionsCollection" yaml:"partitionsCollection"`
	RebalanceCheckInterval time.Duration `json:"rebalanceCheckInterval" yaml:"rebalanceCheckInterval"`
	TotalPartition         int           `json:"totalPartition" yaml:"totalPartition"`
}

func (c *Config) SetDefault() {
	if c.MongoDB.ConnectionPool.MaxPoolSize == 0 {
		c.MongoDB.ConnectionPool.MaxPoolSize = 100
	}

	if c.MongoDB.ConnectionPool.MinPoolSize == 0 {
		c.MongoDB.ConnectionPool.MinPoolSize = 5
	}

	if c.MongoDB.ConnectionPool.MaxIdleTimeMS == 0 {
		c.MongoDB.ConnectionPool.MaxIdleTimeMS = 300000 // 5 minutes
	}

	if c.MongoDB.Timeouts.ConnectTimeoutMS == 0 {
		c.MongoDB.Timeouts.ConnectTimeoutMS = 30000 // 30 seconds
	}

	if c.MongoDB.Timeouts.ServerSelectionTimeoutMS == 0 {
		c.MongoDB.Timeouts.ServerSelectionTimeoutMS = 60000 // 60 seconds
	}

	if c.MongoDB.Timeouts.SocketTimeoutMS == 0 {
		c.MongoDB.Timeouts.SocketTimeoutMS = 120000 // 120 seconds
	}

	if c.Metric.Port == 0 {
		c.Metric.Port = 8080
	}
	if c.Metric.CollectionInterval == 0 {
		c.Metric.CollectionInterval = 20 * time.Second
	}
	if c.Logger.LogLevel == "" {
		c.Logger.LogLevel = logger.INFO
	}
	if c.Checkpoint.Type == "" {
		c.Checkpoint.Type = "auto"
	}
	if c.Checkpoint.TokenSaveInterval == 0 {
		c.Checkpoint.TokenSaveInterval = 10 * time.Second
	}
	if c.Checkpoint.BootstrapSaveCount == 0 {
		c.Checkpoint.BootstrapSaveCount = 2500
	}
	if c.Checkpoint.TokenSaveTimeout == 0 {
		c.Checkpoint.TokenSaveTimeout = 10 * time.Second
	}
	if c.Checkpoint.BootstrapSaveInterval == 0 {
		c.Checkpoint.BootstrapSaveInterval = 10 * time.Second
	}
	if c.Checkpoint.BootstrapQueryBatchSize == 0 {
		c.Checkpoint.BootstrapQueryBatchSize = 2500
	}
	if c.Checkpoint.IdleHeartbeatInterval == 0 {
		c.Checkpoint.IdleHeartbeatInterval = 3 * time.Minute
	}
	if c.Checkpoint.MaxIdleTime == 0 {
		c.Checkpoint.MaxIdleTime = 15 * time.Minute
	}

	if c.Checkpoint.ChangeStreamSaveCount == 0 {
		c.Checkpoint.ChangeStreamSaveCount = 500
	}

	if c.Partition.HeartbeatInterval == 0 {
		c.Partition.HeartbeatInterval = 10 * time.Second
	}
	if c.Partition.WorkerTimeout == 0 {
		c.Partition.WorkerTimeout = 90 * time.Second
	}
	if c.Partition.WorkersCollection == "" {
		c.Partition.WorkersCollection = "workers"
	}
	if c.Partition.PartitionsCollection == "" {
		c.Partition.PartitionsCollection = "partition_assignments"
	}
	if c.Partition.RebalanceCheckInterval == 0 {
		c.Partition.RebalanceCheckInterval = 10 * time.Second
	}
	if c.Partition.TotalPartition == 0 {
		c.Partition.TotalPartition = 15
	}

	if c.GracefulShutdownTimeout == 0 {
		c.GracefulShutdownTimeout = 10 * time.Second
	}
}

func (c *Config) Validate() error {
	if err := c.MongoDB.Validate(); err != nil {
		return fmt.Errorf("mongodb config validation failed: %w", err)
	}
	return nil
}

func (m *MongoDB) Validate() error {
	if err := m.Connection.Validate(); err != nil {
		return fmt.Errorf("connection validation failed: %w", err)
	}

	if err := m.ConnectionPool.Validate(); err != nil {
		return fmt.Errorf("connection pool validation failed: %w", err)
	}

	return nil
}

func (c *Connection) Validate() error {
	if isEmpty(c.URI) {
		return fmt.Errorf("uri is required")
	}

	if isEmpty(c.Database) {
		return fmt.Errorf("database is required")
	}

	if (!isEmpty(c.Username) && isEmpty(c.Password)) || (isEmpty(c.Username) && !isEmpty(c.Password)) {
		return fmt.Errorf("username and password must be provided together")
	}

	return nil
}

func (cp *ConnectionPool) Validate() error {
	if cp.MinPoolSize > cp.MaxPoolSize {
		return fmt.Errorf("minPoolSize (%d) cannot be greater than maxPoolSize (%d)",
			cp.MinPoolSize, cp.MaxPoolSize)
	}

	return nil
}

func isEmpty(s string) bool {
	return strings.TrimSpace(s) == ""
}

func (c *Config) Print() {
	maskedConfig := *c
	maskedConfig.MongoDB.Connection.Password = "***"
	slog.Info("MongoDB CDC Configuration loaded", "config", maskedConfig)
}

func ReadConfigJSON(filename string) (Config, error) {
	var cfg Config
	data, err := readFile(filename)
	if err != nil {
		return cfg, err
	}
	err = json.Unmarshal(data, &cfg)
	return cfg, err
}

func ReadConfigYAML(filename string) (Config, error) {
	var cfg Config
	data, err := readFile(filename)
	if err != nil {
		return cfg, err
	}
	err = yaml.Unmarshal(data, &cfg)
	return cfg, err
}

func readFile(filename string) ([]byte, error) {
	cleanPath := filepath.Clean(filename)
	file, err := os.Open(cleanPath)
	if err != nil {
		return nil, err
	}
	defer func() {
		if closeErr := file.Close(); closeErr != nil {
			return
		}
	}()
	return io.ReadAll(file)
}
