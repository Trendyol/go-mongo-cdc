package cdc

import (
	"context"
	goerrors "errors"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/Trendyol/go-mongo-cdc/config"
	"github.com/Trendyol/go-mongo-cdc/internal/http"
	"github.com/Trendyol/go-mongo-cdc/internal/metric"
	"github.com/Trendyol/go-mongo-cdc/logger"
	"github.com/Trendyol/go-mongo-cdc/mongo/changestream"
	"github.com/Trendyol/go-mongo-cdc/mongo/connection"
	"github.com/go-playground/errors"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"
)

type Connector interface {
	Start(ctx context.Context)
	WaitUntilReady(ctx context.Context) error
	Close()
	GetConfig() *config.Config
	SetMetricCollectors(collectors ...prometheus.Collector)
}

type connector struct {
	stream             changestream.Streamer
	prometheusRegistry metric.Registry
	server             http.Server
	cfg                *config.Config
	cancelCh           chan os.Signal
	readyCh            chan struct{}
	mongoClient        connection.Client
	logger             *zap.Logger

	once sync.Once
}

func NewConnectorWithConfigFile(ctx context.Context, configFilePath string, listenerFunc changestream.ListenerFunc) (Connector, error) {
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

	return NewConnector(ctx, cfg, listenerFunc)
}

func NewConnector(ctx context.Context, cfg config.Config, listenerFunc changestream.ListenerFunc) (Connector, error) {
	cfg.SetDefault()
	if err := cfg.Validate(); err != nil {
		return nil, errors.Wrap(err, "config validation")
	}
	cfg.Print()

	zapLogger := logger.InitLogger(cfg.Logger.Logger)

	mongoClient, err := connection.NewConnection(ctx, cfg.DSN())
	if err != nil {
		return nil, err
	}

	m := metric.NewMetric(cfg.Database, cfg.Collection)

	stream := changestream.NewStream(mongoClient, cfg, m, listenerFunc, zapLogger)

	prometheusRegistry := metric.NewRegistry(m)

	return &connector{
		cfg:                &cfg,
		mongoClient:        mongoClient,
		stream:             stream,
		prometheusRegistry: prometheusRegistry,
		server:             http.NewServer(cfg, prometheusRegistry),
		logger:             zapLogger,

		cancelCh: make(chan os.Signal, 1),
		readyCh:  make(chan struct{}, 1),
	}, nil
}

func (c *connector) Start(ctx context.Context) {
	c.once.Do(func() {
		go c.server.Listen()
	})

	c.logger.Info("Starting MongoDB Change Stream watcher...")

	err := c.stream.Open(ctx)
	if err != nil {
		if goerrors.Is(err, changestream.ErrorStreamInUse) {
			c.logger.Info("Stream capture failed, retrying...")
			time.Sleep(5 * time.Second)
			c.Start(ctx)
			return
		}
		c.logger.Error("MongoDB stream open error", zap.Error(err))
		return
	}

	c.logger.Info("MongoDB Change Stream started successfully")

	signal.Notify(c.cancelCh, syscall.SIGTERM, syscall.SIGINT, syscall.SIGABRT, syscall.SIGQUIT)

	c.readyCh <- struct{}{}

	<-c.cancelCh
	c.logger.Debug("Cancel channel triggered")
}

func (c *connector) WaitUntilReady(ctx context.Context) error {
	select {
	case <-c.readyCh:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (c *connector) Close() {
	if !isClosed(c.cancelCh) {
		close(c.cancelCh)
	}
	if !isClosed(c.readyCh) {
		close(c.readyCh)
	}

	c.stream.Close(context.TODO())
	c.mongoClient.Close(context.TODO())
	c.server.Shutdown()
}

func (c *connector) GetConfig() *config.Config {
	return c.cfg
}

func (c *connector) SetMetricCollectors(metricCollectors ...prometheus.Collector) {
	c.prometheusRegistry.AddMetricCollectors(metricCollectors...)
}

func isClosed[T any](ch <-chan T) bool {
	select {
	case <-ch:
		return true
	default:
		return false
	}
}
