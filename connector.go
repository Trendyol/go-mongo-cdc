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
)

type Connector interface {
	Start(ctx context.Context)
	Close()
	Commit()
	CommitBootstrap(partitionID int)
}

type connector struct {
	stream             stream.PartitionStream
	prometheusRegistry metric.Registry
	mongoClient        connection.Client
	metricInstance     metric.Metric
	cancelCh           chan os.Signal
	workerID           string
	cfg                config.Config

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

	logger.InitDefaultLogger(cfg.Logger.LogLevel)

	mongoClient, err := connection.NewMongoClient(cfg.MongoDB)
	if err != nil {
		return nil, err
	}

	m := metric.NewMetric(cfg.MongoDB.Connection.Database, cfg.MongoDB.Connection.Collection)

	workerID := generateWorkerID()

	partitionStream := stream.NewPartitionStream(mongoClient, cfg, m, listenerFunc, workerID)

	prometheusRegistry := metric.NewRegistry(m)

	return &connector{
		mongoClient:        mongoClient,
		stream:             partitionStream,
		prometheusRegistry: prometheusRegistry,
		metricInstance:     m,
		workerID:           workerID,
		cancelCh:           make(chan os.Signal, 1),
		cfg:                cfg,
	}, nil
}

func (c *connector) Start(ctx context.Context) {
	logger.Log.Info("Starting MongoDB change stream connector - workerId: %s", c.workerID)

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	if c.cfg.Checkpoint.Type == config.CheckpointTypeAuto {
		signal.Notify(c.cancelCh, syscall.SIGTERM, syscall.SIGINT, syscall.SIGABRT, syscall.SIGQUIT)
	}

	g, gCtx := errgroup.WithContext(ctx)

	g.Go(func() error {
		if err := c.stream.Start(gCtx); err != nil {
			logger.Log.Error("Failed to start partition stream: %v", err)
			return err
		}
		return nil
	})

	g.Go(func() error {
		if err := c.prometheusRegistry.StartMetricsServer(gCtx, c.cfg.Metric.Port); err != nil {
			logger.Log.Warn("Metrics server could not start (port %d may be in use): %v - continuing without metrics", c.cfg.Metric.Port, err)
			<-gCtx.Done()
		}
		return nil
	})

	if c.cfg.Checkpoint.Type == config.CheckpointTypeAuto {
		g.Go(func() error {
			select {
			case sig := <-c.cancelCh:
				logger.Log.Debug("Shutdown signal received: %v, cancelling context...", sig)
				cancel()
				return context.Canceled
			case <-gCtx.Done():
				logger.Log.Debug("Context cancelled: %v", gCtx.Err())
				return gCtx.Err()
			}
		})
	}

	if err := g.Wait(); err != nil && err != context.Canceled {
		logger.Log.Error("Connector shutting down due to an error: %v", err)
	} else {
		logger.Log.Info("Connector shutting down gracefully")
	}

	if c.cfg.Checkpoint.Type == config.CheckpointTypeAuto {
		c.Close()
	}
}

func (c *connector) Close() {
	c.once.Do(func() {
		c.mu.Lock()
		defer c.mu.Unlock()

		logger.Log.Info("Closing connections")

		if c.closed {
			logger.Log.Debug("Already closed, skipping cleanup")
			return
		}

		c.closed = true

		if c.cfg.Checkpoint.Type == config.CheckpointTypeAuto {
			signal.Stop(c.cancelCh)
		}

		closeCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		if err := c.stream.Stop(closeCtx); err != nil {
			logger.Log.Error("Failed to stop stream: %v", err)
		}

		logger.Log.Info("Closing mongo client")
		if err := c.mongoClient.Close(closeCtx); err != nil {
			logger.Log.Error("Failed to close mongo client: %v", err)
		}

		logger.Log.Info("Closed connections")
	})
}

func generateWorkerID() string {
	hostname, _ := os.Hostname()
	return fmt.Sprintf("%s-%d-%d", hostname, os.Getpid(), time.Now().UnixNano())
}

func (c *connector) Commit() {
	c.stream.Commit()
}

func (c *connector) CommitBootstrap(partitionID int) {
	c.stream.CommitBootstrap(partitionID)
}
