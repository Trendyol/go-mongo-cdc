package http

import (
	"context"
	"net/http"
	"net/http/pprof"
	"strconv"
	"time"

	"github.com/Trendyol/go-mongo-cdc/config"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.uber.org/zap"
)

type Server interface {
	Listen()
	Shutdown()
}

type Registry interface {
	GetRegistry() *prometheus.Registry
}

type server struct {
	server   *http.Server
	registry Registry
	config   config.Config
	logger   *zap.Logger
}

func NewServer(cfg config.Config, registry Registry) Server {
	mux := http.NewServeMux()

	mux.HandleFunc("/status", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		if _, err := w.Write([]byte("OK")); err != nil {
			http.Error(w, "Failed to write response", http.StatusInternalServerError)
		}
	})

	mux.Handle("/metrics", promhttp.HandlerFor(registry.GetRegistry(), promhttp.HandlerOpts{}))

	if cfg.DebugMode {
		mux.HandleFunc("/debug/pprof/", pprof.Index)
		mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
		mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
		mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
		mux.HandleFunc("/debug/pprof/trace", pprof.Trace)
		mux.Handle("/debug/pprof/goroutine", pprof.Handler("goroutine"))
		mux.Handle("/debug/pprof/heap", pprof.Handler("heap"))
		mux.Handle("/debug/pprof/threadcreate", pprof.Handler("threadcreate"))
		mux.Handle("/debug/pprof/block", pprof.Handler("block"))
	}

	httpServer := &http.Server{
		Addr:         ":" + strconv.Itoa(cfg.Metric.Port),
		Handler:      mux,
		ReadTimeout:  15 * time.Second,
		WriteTimeout: 15 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	logger := zap.NewNop()

	return &server{
		server:   httpServer,
		registry: registry,
		config:   cfg,
		logger:   logger,
	}
}

func (s *server) Listen() {
	s.logger.Info("Starting HTTP server", zap.String("addr", s.server.Addr))

	if err := s.server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		s.logger.Error("HTTP server error", zap.Error(err))
	}
}

func (s *server) Shutdown() {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	s.logger.Info("Shutting down HTTP server")

	if err := s.server.Shutdown(ctx); err != nil {
		s.logger.Error("HTTP server shutdown error", zap.Error(err))
	}
}
