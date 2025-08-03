package http

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/pprof"
	"strconv"
	"time"

	"github.com/Trendyol/go-mongo-cdc/config"
	"github.com/Trendyol/go-mongo-cdc/membership"
	"github.com/Trendyol/go-mongo-cdc/mongo/connection"
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
	server     *http.Server
	registry   Registry
	config     config.Config
	logger     *zap.Logger
	client     connection.Client
	membership membership.Membership
}

type MembershipUpdateRequest struct {
	MemberNumber int `json:"memberNumber"`
	TotalMembers int `json:"totalMembers"`
}

type StatusResponse struct {
	Status    string `json:"status"`
	Timestamp string `json:"timestamp"`
}

func NewServer(cfg config.Config, registry Registry, logger *zap.Logger,
	client connection.Client, membership membership.Membership) Server {

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

	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(map[string]string{"status": "healthy"})
	})

	mux.HandleFunc("/rebalance", func(w http.ResponseWriter, r *http.Request) {
		if membership == nil {
			http.Error(w, "Membership not enabled", http.StatusBadRequest)
			return
		}

		ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
		defer cancel()

		if err := membership.TriggerRebalance(ctx); err != nil {
			logger.Error("Rebalance failed", zap.Error(err))
			http.Error(w, fmt.Sprintf("Rebalance failed: %v", err), http.StatusInternalServerError)
			return
		}

		logger.Info("Rebalance triggered via API")
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]string{
			"message": "Rebalance triggered successfully",
			"time":    time.Now().Format(time.RFC3339),
		})
	})

	mux.HandleFunc("/membership/info", func(w http.ResponseWriter, r *http.Request) {
		if membership == nil {
			http.Error(w, "Membership not enabled", http.StatusBadRequest)
			return
		}

		var req MembershipUpdateRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "Invalid JSON payload", http.StatusBadRequest)
			return
		}

		if req.MemberNumber <= 0 || req.TotalMembers <= 0 {
			http.Error(w, "Invalid member number or total members", http.StatusBadRequest)
			return
		}

		ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
		defer cancel()

		if err := membership.UpdateMembershipInfo(ctx, req.MemberNumber, req.TotalMembers); err != nil {
			logger.Error("Failed to update membership info", zap.Error(err))
			http.Error(w, fmt.Sprintf("Failed to update membership: %v", err), http.StatusInternalServerError)
			return
		}

		logger.Info("Membership info updated via API",
			zap.Int("memberNumber", req.MemberNumber),
			zap.Int("totalMembers", req.TotalMembers))

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]string{
			"message": "Membership info updated successfully",
			"time":    time.Now().Format(time.RFC3339),
		})
	})

	httpServer := &http.Server{
		Addr:         ":" + strconv.Itoa(cfg.Metric.Port),
		Handler:      mux,
		ReadTimeout:  15 * time.Second,
		WriteTimeout: 15 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	return &server{
		server:     httpServer,
		registry:   registry,
		config:     cfg,
		logger:     logger,
		client:     client,
		membership: membership,
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
