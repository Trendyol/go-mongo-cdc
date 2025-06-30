package metric

import (
	"os"
	"runtime"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
)

type Metric interface {
	IncInsertTotal()
	IncUpdateTotal()
	IncDeleteTotal()
	SetProcessLatency(latency int64)
	SetCDCLatency(latency int64)
}

type Registry interface {
	AddMetricCollectors(collectors ...prometheus.Collector)
	GetRegistry() *prometheus.Registry
}

type metric struct {
	hostname       string
	database       string
	collection     string
	insertTotal    prometheus.Counter
	updateTotal    prometheus.Counter
	deleteTotal    prometheus.Counter
	processLatency prometheus.Gauge
	cdcLatency     prometheus.Gauge
}

type registry struct {
	registry *prometheus.Registry
}

func NewMetric(database, collection string) Metric {
	hostname, _ := os.Hostname()

	m := &metric{
		hostname:   hostname,
		database:   database,
		collection: collection,
		insertTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "go_mongo_cdc_insert_total",
			Help: "The total number of INSERT operations captured on specific collections.",
			ConstLabels: prometheus.Labels{
				"database":   database,
				"collection": collection,
				"host":       hostname,
			},
		}),
		updateTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "go_mongo_cdc_update_total",
			Help: "The total number of UPDATE operations captured on specific collections.",
			ConstLabels: prometheus.Labels{
				"database":   database,
				"collection": collection,
				"host":       hostname,
			},
		}),
		deleteTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "go_mongo_cdc_delete_total",
			Help: "The total number of DELETE operations captured on specific collections.",
			ConstLabels: prometheus.Labels{
				"database":   database,
				"collection": collection,
				"host":       hostname,
			},
		}),
		processLatency: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "go_mongo_cdc_process_latency_current",
			Help: "The current latency in processing the captured data changes.",
			ConstLabels: prometheus.Labels{
				"database":   database,
				"collection": collection,
				"host":       hostname,
			},
		}),
		cdcLatency: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "go_mongo_cdc_cdc_latency_current",
			Help: "The current latency in capturing data changes from MongoDB.",
			ConstLabels: prometheus.Labels{
				"database":   database,
				"collection": collection,
				"host":       hostname,
			},
		}),
	}

	return m
}

func (m *metric) IncInsertTotal() {
	m.insertTotal.Inc()
}

func (m *metric) IncUpdateTotal() {
	m.updateTotal.Inc()
}

func (m *metric) IncDeleteTotal() {
	m.deleteTotal.Inc()
}

func (m *metric) SetProcessLatency(latency int64) {
	m.processLatency.Set(float64(latency))
}

func (m *metric) SetCDCLatency(latency int64) {
	m.cdcLatency.Set(float64(latency))
}

func NewRegistry(metrics Metric) Registry {
	reg := prometheus.NewRegistry()

	m := metrics.(*metric)

	reg.MustRegister(m.insertTotal)
	reg.MustRegister(m.updateTotal)
	reg.MustRegister(m.deleteTotal)
	reg.MustRegister(m.processLatency)
	reg.MustRegister(m.cdcLatency)

	reg.MustRegister(collectors.NewGoCollector())
	reg.MustRegister(collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))

	buildInfo := prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "go_mongo_cdc_build_info",
			Help: "Build information for go-mongo-cdc",
		},
		[]string{"version", "go_version"},
	)
	buildInfo.WithLabelValues("dev", runtime.Version()).Set(1)
	reg.MustRegister(buildInfo)

	return &registry{registry: reg}
}

func (r *registry) AddMetricCollectors(collectors ...prometheus.Collector) {
	for _, collector := range collectors {
		r.registry.MustRegister(collector)
	}
}

func (r *registry) GetRegistry() *prometheus.Registry {
	return r.registry
}
