package config

import (
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
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
	DebugMode    bool             `json:"debugMode" yaml:"debugMode"`
	Metric       MetricConfig     `json:"metric" yaml:"metric"`
	Logger       LoggerConfig     `json:"logger" yaml:"logger"`
	Checkpoint   CheckpointConfig `json:"checkpoint" yaml:"checkpoint"`
	Membership   MembershipConfig `json:"membership" yaml:"membership"`
}

type MetricConfig struct {
	Port int `json:"port" yaml:"port"`
}

type LoggerConfig struct {
	LogLevel slog.Level  `json:"logLevel" yaml:"logLevel"`
	Logger   *zap.Logger `json:"-" yaml:"-"`
}

type CheckpointConfig struct {
	Collection                 string        `json:"collection" yaml:"collection"`
	SaveInterval               time.Duration `json:"saveInterval" yaml:"saveInterval"`
	ResumeTokenRefreshInterval time.Duration `json:"resumeTokenRefreshInterval" yaml:"resumeTokenRefreshInterval"`
}

type MembershipConfig struct {
	MemberID           string            `json:"memberId" yaml:"memberId"`
	HeartbeatInterval  time.Duration     `json:"heartbeatInterval" yaml:"heartbeatInterval"`
	HealthCheckTimeout time.Duration     `json:"healthCheckTimeout" yaml:"healthCheckTimeout"`
	Config             map[string]string `json:"config" yaml:"config"`
	ChunkBased         bool              `json:"chunkBased" yaml:"chunkBased"`
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
	if c.Checkpoint.ResumeTokenRefreshInterval == 0 {
		c.Checkpoint.ResumeTokenRefreshInterval = 60 * time.Second
	}

	if c.Membership.HeartbeatInterval == 0 {
		c.Membership.HeartbeatInterval = 10 * time.Second
	}
	if c.Membership.HealthCheckTimeout == 0 {
		c.Membership.HealthCheckTimeout = 30 * time.Second
	}
	if c.Membership.Config == nil {
		c.Membership.Config = make(map[string]string)
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
	auth := ""
	if c.Username != "" && c.Password != "" {
		auth = fmt.Sprintf("%s:%s@", c.Username, c.Password)
	}

	return fmt.Sprintf("mongodb://%s%s:%d/%s?authSource=%s",
		auth, c.Host, c.Port, c.Database, c.AuthDatabase)
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
