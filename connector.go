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
	"go.mongodb.org/mongo-driver/bson"
	"go.uber.org/zap"
)

type Connector interface {
	Start(ctx context.Context)
	Close()
}

type connector struct {
	stream                  stream.PartitionStream
	prometheusRegistry      metric.Registry
	mongoClient             connection.Client
	mongoMetricsCollector   metric.MongoDBMetricsCollector
	shardedMetricsCollector metric.ShardedMetricsCollector
	metricInstance          metric.Metric
	logger                  *zap.Logger
	cancelCh                chan os.Signal
	workerID                string
	isShardedCluster        bool
	cfg                     config.Config

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

	mongoMetricsCollector := metric.NewMongoDBMetricsCollector()
	shardedMetricsCollector := metric.NewShardedMetricsCollector(cfg.Metric.EnableShardMetricsMapping)

	isSharded := detectShardedCluster(mongoClient)

	return &connector{
		mongoClient:             mongoClient,
		stream:                  partitionStream,
		prometheusRegistry:      prometheusRegistry,
		mongoMetricsCollector:   mongoMetricsCollector,
		shardedMetricsCollector: shardedMetricsCollector,
		metricInstance:          m,
		logger:                  zapLogger,
		workerID:                workerID,
		isShardedCluster:        isSharded,
		cancelCh:                make(chan os.Signal, 1),
		cfg:                     cfg,
	}, nil
}

func (c *connector) Start(ctx context.Context) {
	c.logger.Info(fmt.Sprintf("Starting MongoDB change stream connector - workerId: %s", c.workerID))

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	g, gCtx := errgroup.WithContext(ctx)

	g.Go(func() error {
		if err := c.stream.Start(gCtx); err != nil {
			c.logger.Error(fmt.Sprintf("Failed to start partition stream: %v", err))
			return err
		}
		return nil
	})

	g.Go(func() error {
		if err := c.prometheusRegistry.StartMetricsServer(gCtx, c.cfg.Metric.Port); err != nil {
			c.logger.Warn(fmt.Sprintf("Metrics server could not start (port %d may be in use): %v - continuing without metrics", c.cfg.Metric.Port, err))
			<-gCtx.Done()
		}
		return nil
	})

	g.Go(func() error {
		c.collectMongoDBMetricsPeriodically(gCtx)
		c.logger.Debug("Metrics collection stopped")
		return nil
	})

	g.Go(func() error {
		signal.Notify(c.cancelCh, syscall.SIGTERM, syscall.SIGINT, syscall.SIGABRT, syscall.SIGQUIT)
		select {
		case <-c.cancelCh:
			c.logger.Info("Shutdown signal received, cancelling context...")
			cancel()
			return context.Canceled
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

func (c *connector) collectMongoDBMetricsPeriodically(ctx context.Context) {
	ticker := time.NewTicker(c.cfg.Metric.CollectionInterval)
	defer ticker.Stop()

	collectMetrics := func() {
		select {
		case <-ctx.Done():
			return
		default:
		}

		metricsCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		client, ok := c.mongoClient.(*connection.MongoClient)
		if !ok {
			return
		}

		if c.isShardedCluster {
			c.logger.Debug("Collecting shard metrics...")
			shardMetrics, err := c.shardedMetricsCollector.CollectShardedMetrics(metricsCtx, client.GetClient())
			if err != nil {
				c.logger.Debug(fmt.Sprintf("Failed to collect shard metrics: %v", err))
			} else {
				c.metricInstance.SetShardMetrics(shardMetrics)
				c.logger.Debug(fmt.Sprintf("Shard metrics collected - shards: %d", len(shardMetrics)))
			}
		} else {
			mongoMetrics, err := c.mongoMetricsCollector.CollectMongoDBMetrics(metricsCtx, client.GetClient())
			if err != nil {
				c.logger.Debug(fmt.Sprintf("Failed to collect MongoDB metrics: %v", err))
				return
			}

			c.metricInstance.SetMongoDBMetrics(mongoMetrics)

			// Log metrics status
			if mongoMetrics.OplogSize > 0 {
				c.logger.Debug(fmt.Sprintf("MongoDB metrics collected - OplogUsed: %.2f%%, ReplicationLag: %ds",
					mongoMetrics.OplogUsedPercent, mongoMetrics.ReplicationLag))
			} else {
				c.logger.Debug("MongoDB metrics collected - Oplog/Replication metrics unavailable (expected for mongos/standalone)")
			}
		}
	}

	collectMetrics()

	for {
		select {
		case <-ctx.Done():
			c.logger.Debug("Metrics collection goroutine stopping...")
			return
		case <-ticker.C:
			collectMetrics()
		}
	}
}

func detectShardedCluster(client connection.Client) bool {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	db := client.Database("admin")
	var result bson.M
	err := db.RunCommand(ctx, bson.D{{Key: "isMaster", Value: 1}}).Decode(&result)
	if err != nil {
		return false
	}

	if msg, ok := result["msg"]; ok {
		if msgStr, ok := msg.(string); ok && msgStr == "isdbgrid" {
			return true
		}
	}

	return false
}
