package config

import (
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"

	"go.uber.org/zap"
	"gopkg.in/yaml.v2"
)

type Config struct {
	Host         string           `json:"host" yaml:"host"`
	Port         int              `json:"port" yaml:"port"`
	Username     string           `json:"username" yaml:"username"`
	Password     string           `json:"password" yaml:"password"`
	Database     string           `json:"database" yaml:"database"`
	Collection   string           `json:"collection" yaml:"collection"`
	AuthDatabase string           `json:"authDatabase" yaml:"authDatabase"`
	Metric       MetricConfig     `json:"metric" yaml:"metric"`
	Logger       LoggerConfig     `json:"logger" yaml:"logger"`
	Checkpoint   CheckpointConfig `json:"checkpoint" yaml:"checkpoint"`
	Partition    PartitionConfig  `json:"partition" yaml:"partition"`
}

type MetricConfig struct {
	Port int `json:"port" yaml:"port"`
}

type LoggerConfig struct {
	LogLevel slog.Level  `json:"logLevel" yaml:"logLevel"`
	Logger   *zap.Logger `json:"-" yaml:"-"`
}

type CheckpointConfig struct {
	Collection   string        `json:"collection" yaml:"collection"`
	SaveInterval time.Duration `json:"saveInterval" yaml:"saveInterval"`
}

type PartitionConfig struct {
	HeartbeatInterval      time.Duration `json:"heartbeatInterval" yaml:"heartbeatInterval"`
	WorkerTimeout          time.Duration `json:"workerTimeout" yaml:"workerTimeout"`
	PartitionDatabase      string        `json:"partitionDatabase" yaml:"partitionDatabase"`
	RebalanceCheckInterval time.Duration `json:"rebalanceCheckInterval" yaml:"rebalanceCheckInterval"`
	TotalPartition         int           `json:"totalPartition" yaml:"totalPartition"`
}

func (c *Config) SetDefault() {
	if c.Port == 0 {
		c.Port = 27017
	}
	if c.AuthDatabase == "" {
		c.AuthDatabase = "admin"
	}
	if c.Metric.Port == 0 {
		c.Metric.Port = 8080
	}
	if c.Logger.LogLevel == 0 {
		c.Logger.LogLevel = slog.LevelInfo
	}
	if c.Checkpoint.Collection == "" {
		c.Checkpoint.Collection = "cdc_checkpoints"
	}
	if c.Checkpoint.SaveInterval == 0 {
		c.Checkpoint.SaveInterval = 30 * time.Second
	}

	if c.Partition.HeartbeatInterval == 0 {
		c.Partition.HeartbeatInterval = 5 * time.Second
	}
	if c.Partition.WorkerTimeout == 0 {
		c.Partition.WorkerTimeout = 30 * time.Second
	}
	if c.Partition.PartitionDatabase == "" {
		c.Partition.PartitionDatabase = "cdc_partitions"
	}
	if c.Partition.RebalanceCheckInterval == 0 {
		c.Partition.RebalanceCheckInterval = 10 * time.Second
	}
	if c.Partition.TotalPartition == 0 {
		c.Partition.TotalPartition = 10
	}
}

func (c *Config) Validate() error {
	if c.Host == "" {
		return fmt.Errorf("host is required")
	}
	if c.Database == "" {
		return fmt.Errorf("database is required")
	}
	if c.Collection == "" {
		return fmt.Errorf("collection is required")
	}

	return nil
}

func (c *Config) DSN() string {
	var dsn strings.Builder

	dsn.WriteString("mongodb://")

	// Add authentication if provided
	if c.Username != "" && c.Password != "" {
		// Use secure encoding for MongoDB credentials
		authPart := encodeMongoDBCredentials(c.Username, c.Password)
		dsn.WriteString(authPart)
	}

	// Handle IPv6 addresses
	host := c.Host
	if strings.Contains(host, ":") && !strings.HasPrefix(host, "[") {
		host = fmt.Sprintf("[%s]", host)
	}

	dsn.WriteString(fmt.Sprintf("%s:%d", host, c.Port))

	// Add database if specified
	if c.Database != "" {
		dsn.WriteString(fmt.Sprintf("/%s", c.Database))
	}

	separator := "?"

	// Add authSource if specified
	if c.AuthDatabase != "" {
		if c.Database == "" {
			dsn.WriteString("/")
		}
		dsn.WriteString(fmt.Sprintf("%sauthSource=%s", separator, c.AuthDatabase))
		separator = "&" // Bir sonraki parametre için ayırıcıyı '&' yap
	}

	// maxPoolSize parametresini statik olarak ekle
	if c.Database == "" && separator == "?" {
		dsn.WriteString("/")
	}
	dsn.WriteString(fmt.Sprintf("%smaxPoolSize=350", separator))

	return dsn.String()
}

// encodeMongoDBCredentials securely encodes username and password for MongoDB connection strings
func encodeMongoDBCredentials(username, password string) string {
	// Use multiple encoding strategies for maximum compatibility
	encodedUsername := url.QueryEscape(username)
	encodedPassword := url.QueryEscape(password)

	// Additional validation - ensure @ character is properly encoded
	if strings.Contains(username, "@") && !strings.Contains(encodedUsername, "%40") {
		encodedUsername = strings.ReplaceAll(encodedUsername, "@", "%40")
	}
	if strings.Contains(password, "@") && !strings.Contains(encodedPassword, "%40") {
		encodedPassword = strings.ReplaceAll(encodedPassword, "@", "%40")
	}

	return fmt.Sprintf("%s:%s@", encodedUsername, encodedPassword)
}

func (c *Config) Print() {
	maskedConfig := *c
	maskedConfig.Password = "***"
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
