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
	"github.com/Trendyol/go-mongo-cdc/internal/metric"
	"github.com/Trendyol/go-mongo-cdc/logger"
	"github.com/Trendyol/go-mongo-cdc/mongo/changestream"
	"github.com/Trendyol/go-mongo-cdc/mongo/connection"
	"github.com/go-playground/errors"
	"go.uber.org/zap"
)

type Connector interface {
	Start(ctx context.Context)
	Close()
}

type connector struct {
	stream             changestream.Streamer
	prometheusRegistry metric.Registry
	cfg                *config.Config
	mongoClient        connection.Client
	logger             *zap.Logger
	cancelCh           chan os.Signal

	once   sync.Once
	closed bool
	mu     sync.Mutex
}

func NewConnectorWithConfigFile(
	ctx context.Context,
	configFilePath string,
	listenerFunc changestream.ListenerFunc,
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
		mongoClient:        mongoClient,
		stream:             stream,
		prometheusRegistry: prometheusRegistry,
		logger:             zapLogger,
		cancelCh:           make(chan os.Signal, 1),
	}, nil
}

func (c *connector) Start(ctx context.Context) {
	go func() {
		for {
			err := c.stream.Open(ctx)
			if err == nil {
				c.logger.Info("MongoDB stream completed normally")
				return
			}

			if goerrors.Is(err, changestream.ErrorStreamInUse) {
				c.logger.Info("Stream capture failed, retrying")
				time.Sleep(5 * time.Second)
				continue
			}

			if goerrors.Is(err, context.Canceled) {
				c.mu.Lock()
				isClosed := c.closed
				c.mu.Unlock()

				if isClosed || ctx.Err() != nil {
					c.logger.Info("Stream stopped due to shutdown")
					return
				}

				c.logger.Info("Stream restarting due to membership change")
				time.Sleep(1 * time.Second)
				continue
			}

			if ctx.Err() != nil {
				c.logger.Info("Stream stopping due to context cancellation")
				return
			}

			c.logger.Error("MongoDB stream open error", zap.Error(err))
			time.Sleep(5 * time.Second)
		}
	}()

	signal.Notify(c.cancelCh, syscall.SIGTERM, syscall.SIGINT, syscall.SIGABRT, syscall.SIGQUIT)

	<-c.cancelCh
	c.logger.Info("Shutdown signal received")
}

func (c *connector) Close() {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.logger.Info("Closing connections")

	if c.closed {
		c.logger.Info("Already closed, skipping cleanup")
		return
	}

	c.closed = true

	if !isClosed(c.cancelCh) {
		close(c.cancelCh)
	}

	closeCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := c.stream.Close(closeCtx); err != nil {
		c.logger.Error("Failed to close stream", zap.Error(err))
	}

	c.logger.Info("Closing mongo client")
	if err := c.mongoClient.Close(closeCtx); err != nil {
		c.logger.Error("Failed to close mongo client", zap.Error(err))
	}

	c.logger.Info("Closed connections")
}

func isClosed[T any](ch <-chan T) bool {
	select {
	case <-ch:
		return true
	default:
		return false
	}
}
