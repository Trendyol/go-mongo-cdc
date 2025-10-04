package metric

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

type Registry interface {
	StartMetricsServer(ctx context.Context, port int) error
}

type registry struct {
	collector  *Collector
	registerer *prometheus.Registry
	server     *http.Server
}

func NewRegistry(m Metric) Registry {
	collector := NewCollector(m)
	reg := prometheus.NewRegistry()
	reg.MustRegister(collector)

	return &registry{
		collector:  collector,
		registerer: reg,
	}
}

func (r *registry) StartMetricsServer(ctx context.Context, port int) error {
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.HandlerFor(r.registerer, promhttp.HandlerOpts{
		Registry: r.registerer,
	}))

	mux.HandleFunc("/health", func(w http.ResponseWriter, req *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("OK"))
	})

	mux.HandleFunc("/ready", func(w http.ResponseWriter, req *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("Ready"))
	})

	r.server = &http.Server{
		Addr:              fmt.Sprintf(":%d", port),
		Handler:           mux,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      10 * time.Second,
		IdleTimeout:       120 * time.Second,
		ReadHeaderTimeout: 5 * time.Second,
	}

	errCh := make(chan error, 1)
	go func() {
		if err := r.server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			errCh <- err
		}
	}()

	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := r.server.Shutdown(shutdownCtx); err != nil {
			fmt.Printf("Error shutting down metrics server: %v\n", err)
		}
	}()

	select {
	case err := <-errCh:
		return fmt.Errorf("metrics server failed to start: %w", err)
	case <-time.After(100 * time.Millisecond):
		return nil
	}
}
