package cdc

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/Trendyol/go-mongo-cdc/config"
	"github.com/Trendyol/go-mongo-cdc/logger"
	"github.com/Trendyol/go-mongo-cdc/metric"
	"github.com/Trendyol/go-mongo-cdc/mongo/connection"
	"github.com/Trendyol/go-mongo-cdc/stream"
	"github.com/go-playground/errors"
	"go.uber.org/zap"
)

type Connector interface {
	Start(ctx context.Context)
	Close()
}

type connector struct {
	stream             stream.PartitionStream
	prometheusRegistry metric.Registry
	mongoClient        connection.Client
	logger             *zap.Logger
	cancelCh           chan os.Signal
	workerID           string

	once   sync.Once
	closed bool
	mu     sync.Mutex
}

func NewConnectorWithConfigFile(
	ctx context.Context,
	configFilePath string,
	listenerFunc stream.ListenerFunc,
) (Connector, error) {
	var cfg config.Config
	var err error

	if strings.HasSuffix(configFilePath, ".json") {
		cfg, err = config.ReadConfigJSON(configFilePath)
	}

	if strings.HasSuffix(configFilePath, ".yml") || strings.HasSuffix(configFilePath, ".yaml") {
		cfg, err = config.ReadConfigYAML(configFilePath)
	}

	if err != nil {
		return nil, err
	}

	return NewConnector(cfg, listenerFunc)
}

func NewConnector(cfg config.Config, listenerFunc stream.ListenerFunc) (Connector, error) {
	cfg.SetDefault()
	if err := cfg.Validate(); err != nil {
		return nil, errors.Wrap(err, "config validation")
	}
	cfg.Print()

	zapLogger := logger.InitLoggerWithLevel(cfg.Logger.Logger, cfg.Logger.LogLevel)

	mongoClient, err := connection.NewMongoClient(cfg.MongoDB)
	if err != nil {
		return nil, err
	}

	m := metric.NewMetric(cfg.MongoDB.Connection.Database, cfg.MongoDB.Connection.Collection)

	workerID := generateWorkerID()

	partitionStream := stream.NewPartitionStream(mongoClient, cfg, m, listenerFunc, zapLogger, workerID)

	prometheusRegistry := metric.NewRegistry(m)

	return &connector{
		mongoClient:        mongoClient,
		stream:             partitionStream,
		prometheusRegistry: prometheusRegistry,
		logger:             zapLogger,
		workerID:           workerID,
		cancelCh:           make(chan os.Signal, 1),
	}, nil
}

func (c *connector) Start(ctx context.Context) {
	c.logger.Info(fmt.Sprintf("Starting MongoDB change stream connector - workerId: %s", c.workerID))

	g, gCtx := errgroup.WithContext(ctx)

	g.Go(func() error {
		if err := c.stream.Start(gCtx); err != nil {
			c.logger.Error(fmt.Sprintf("Failed to start partition stream: %v", err))
			return err
		}
		return nil
	})

	g.Go(func() error {
		if err := c.prometheusRegistry.StartMetricsServer(gCtx, 8080); err != nil {
			c.logger.Error(fmt.Sprintf("Failed to start metrics server: %v", err))
			return err
		}
		return nil
	})

	g.Go(func() error {
		signal.Notify(c.cancelCh, syscall.SIGTERM, syscall.SIGINT, syscall.SIGABRT, syscall.SIGQUIT)
		select {
		case <-c.cancelCh:
			c.logger.Info("Shutdown signal received")
			return nil
		case <-gCtx.Done():
			c.logger.Info(fmt.Sprintf("Context cancelled: %v", gCtx.Err()))
			return gCtx.Err()
		}
	})

	if err := g.Wait(); err != nil && err != context.Canceled {
		c.logger.Error(fmt.Sprintf("Connector shutting down due to an error: %v", err))
	} else {
		c.logger.Info("Connector shutting down gracefully")
	}

	c.Close()
}

func (c *connector) Close() {
	c.once.Do(func() {
		c.mu.Lock()
		defer c.mu.Unlock()

		c.logger.Info("Closing connections")

		if c.closed {
			c.logger.Info("Already closed, skipping cleanup")
			return
		}

		c.closed = true

		signal.Stop(c.cancelCh)

		closeCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		if err := c.stream.Stop(closeCtx); err != nil {
			c.logger.Error(fmt.Sprintf("Failed to stop stream: %v", err))
		}

		c.logger.Info("Closing mongo client")
		if err := c.mongoClient.Close(closeCtx); err != nil {
			c.logger.Error(fmt.Sprintf("Failed to close mongo client: %v", err))
		}

		c.logger.Info("Closed connections")
	})
}

func generateWorkerID() string {
	hostname, _ := os.Hostname()
	return fmt.Sprintf("%s-%d-%d", hostname, os.Getpid(), time.Now().UnixNano())
}
