package metric

import (
	"context"
	"fmt"
	"log"
	"net"
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
	registerer prometheus.Registerer
	server     *http.Server
}

func NewRegistry(m Metric) Registry {
	collector := NewCollector(m)
	prometheus.DefaultRegisterer.MustRegister(collector)

	return &registry{
		collector:  collector,
		registerer: prometheus.DefaultRegisterer,
	}
}

func (r *registry) StartMetricsServer(ctx context.Context, port int) error {
	listener, actualPort, err := r.createListener(port)
	if err != nil {
		return fmt.Errorf("failed to create listener: %w", err)
	}

	if actualPort != port {
		log.Printf("Warning: Requested port %d was in use, using port %d instead\n", port, actualPort)
	}

	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())

	mux.HandleFunc("/health", func(w http.ResponseWriter, req *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("OK"))
	})

	mux.HandleFunc("/ready", func(w http.ResponseWriter, req *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("Ready"))
	})

	r.server = &http.Server{
		Handler:           mux,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      10 * time.Second,
		IdleTimeout:       120 * time.Second,
		ReadHeaderTimeout: 5 * time.Second,
	}

	errCh := make(chan error, 1)
	go func() {
		log.Printf("Metrics server starting on port %d\n", actualPort)
		if err := r.server.Serve(listener); err != nil && err != http.ErrServerClosed {
			errCh <- err
		}
	}()

	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := r.server.Shutdown(shutdownCtx); err != nil {
			log.Printf("Error shutting down metrics server: %v\n", err)
		}
	}()

	select {
	case err := <-errCh:
		return fmt.Errorf("metrics server failed to start: %w", err)
	case <-time.After(100 * time.Millisecond):
		return nil
	}
}

func (r *registry) createListener(preferredPort int) (net.Listener, int, error) {
	listener, err := net.Listen("tcp", fmt.Sprintf(":%d", preferredPort))
	if err == nil {
		return listener, preferredPort, nil
	}

	for port := preferredPort + 1; port <= preferredPort+100; port++ {
		listener, err = net.Listen("tcp", fmt.Sprintf(":%d", port))
		if err == nil {
			return listener, port, nil
		}
	}

	listener, err = net.Listen("tcp", ":0")
	if err != nil {
		return nil, 0, err
	}

	actualPort := listener.Addr().(*net.TCPAddr).Port
	return listener, actualPort, nil
}
