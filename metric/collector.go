package metric

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

type Collector struct {
	metric Metric

	insertTotal  *prometheus.Desc
	updateTotal  *prometheus.Desc
	deleteTotal  *prometheus.Desc
	replaceTotal *prometheus.Desc

	cdcLatency *prometheus.Desc

	checkpointSaveTotal      *prometheus.Desc
	checkpointSaveErrorTotal *prometheus.Desc
	checkpointSaveLatency    *prometheus.Desc

	bootstrapDocumentTotal *prometheus.Desc
	bootstrapActive        *prometheus.Desc

	activePartitionCount  *prometheus.Desc
	partitionAcquireTotal *prometheus.Desc
	partitionReleaseTotal *prometheus.Desc

	resumeTokenExpiredTotal  *prometheus.Desc
	changeStreamErrorTotal   *prometheus.Desc
	changeStreamRestartTotal *prometheus.Desc

	lastEventTime           *prometheus.Desc
	eventLagDuration        *prometheus.Desc
	listenerLatency         *prometheus.Desc
	timeSinceLastCheckpoint *prometheus.Desc
	listenerErrorTotal      *prometheus.Desc
	partitionRebalanceTotal *prometheus.Desc

	buildInfo *prometheus.Desc
}

func NewCollector(m Metric) *Collector {
	namespace := "go_mongo_cdc"

	return &Collector{
		metric:                   m,
		insertTotal:              newCounterDesc(namespace, "insert_total", "Total number of insert operations processed"),
		updateTotal:              newCounterDesc(namespace, "update_total", "Total number of update operations processed"),
		deleteTotal:              newCounterDesc(namespace, "delete_total", "Total number of delete operations processed"),
		replaceTotal:             newCounterDesc(namespace, "replace_total", "Total number of replace operations processed"),
		cdcLatency:               newGaugeDesc(namespace, "cdc_latency_seconds", "CDC latency in seconds"),
		checkpointSaveTotal:      newCounterDesc(namespace, "checkpoint_save_total", "Total number of checkpoint save operations"),
		checkpointSaveErrorTotal: newCounterDesc(namespace, "checkpoint_save_error_total", "Total number of checkpoint save errors"),
		checkpointSaveLatency:    newGaugeDesc(namespace, "checkpoint_save_latency_seconds", "Checkpoint save latency in seconds"),
		bootstrapDocumentTotal:   newCounterDesc(namespace, "bootstrap_document_total", "Total number of documents processed during bootstrap"),
		bootstrapActive:          newGaugeDesc(namespace, "bootstrap_active", "Bootstrap active status (1=active, 0=inactive)"),
		activePartitionCount:     newGaugeDesc(namespace, "active_partition_count", "Number of currently active partitions"),
		partitionAcquireTotal:    newCounterDesc(namespace, "partition_acquire_total", "Total number of partition acquisitions"),
		partitionReleaseTotal:    newCounterDesc(namespace, "partition_release_total", "Total number of partition releases"),
		resumeTokenExpiredTotal:  newCounterDesc(namespace, "resume_token_expired_total", "Total number of expired resume tokens"),
		changeStreamErrorTotal:   newCounterDesc(namespace, "change_stream_error_total", "Total number of change stream errors"),
		changeStreamRestartTotal: newCounterDesc(namespace, "change_stream_restart_total", "Total number of change stream restarts"),
		lastEventTime:            newGaugeDesc(namespace, "last_event_time_seconds", "Unix timestamp of the last processed event"),
		eventLagDuration:         newGaugeDesc(namespace, "event_lag_seconds", "Time since the last event was processed in seconds"),
		listenerLatency:          newGaugeDesc(namespace, "listener_latency_seconds", "Listener function execution latency in seconds"),
		timeSinceLastCheckpoint: newGaugeDesc(
			namespace,
			"time_since_last_checkpoint_seconds",
			"Time since last checkpoint was saved in seconds",
		),
		listenerErrorTotal:      newCounterDesc(namespace, "listener_error_total", "Total number of listener function errors"),
		partitionRebalanceTotal: newCounterDesc(namespace, "partition_rebalance_total", "Total number of partition rebalance operations"),
		buildInfo: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, "", "build_info"),
			"Build information",
			[]string{"version", "go_version"},
			nil,
		),
	}
}

func newCounterDesc(namespace, name, help string) *prometheus.Desc { //nolint:unparam
	return prometheus.NewDesc(
		prometheus.BuildFQName(namespace, "", name),
		help,
		nil,
		nil,
	)
}

func newGaugeDesc(namespace, name, help string) *prometheus.Desc { //nolint:unparam
	return prometheus.NewDesc(
		prometheus.BuildFQName(namespace, "", name),
		help,
		nil,
		nil,
	)
}

func (c *Collector) Describe(ch chan<- *prometheus.Desc) {
	prometheus.DescribeByCollect(c, ch)
}

func (c *Collector) Collect(ch chan<- prometheus.Metric) {
	c.collectOperationMetrics(ch)
	c.collectCheckpointMetrics(ch)
	c.collectBootstrapAndPartitionMetrics(ch)
	c.collectLagAndListenerMetrics(ch)
	c.collectBuildInfoMetrics(ch)
}

func (c *Collector) collectOperationMetrics(ch chan<- prometheus.Metric) {
	ch <- prometheus.MustNewConstMetric(
		c.insertTotal,
		prometheus.CounterValue,
		float64(c.metric.GetInsertTotal()),
	)

	ch <- prometheus.MustNewConstMetric(
		c.updateTotal,
		prometheus.CounterValue,
		float64(c.metric.GetUpdateTotal()),
	)

	ch <- prometheus.MustNewConstMetric(
		c.deleteTotal,
		prometheus.CounterValue,
		float64(c.metric.GetDeleteTotal()),
	)

	ch <- prometheus.MustNewConstMetric(
		c.replaceTotal,
		prometheus.CounterValue,
		float64(c.metric.GetReplaceTotal()),
	)
}

func (c *Collector) collectCheckpointMetrics(ch chan<- prometheus.Metric) {
	ch <- prometheus.MustNewConstMetric(
		c.cdcLatency,
		prometheus.GaugeValue,
		float64(c.metric.GetCDCLatency())/1000.0,
	)

	ch <- prometheus.MustNewConstMetric(
		c.checkpointSaveTotal,
		prometheus.CounterValue,
		float64(c.metric.GetCheckpointSaveTotal()),
	)

	ch <- prometheus.MustNewConstMetric(
		c.checkpointSaveErrorTotal,
		prometheus.CounterValue,
		float64(c.metric.GetCheckpointSaveErrorTotal()),
	)

	ch <- prometheus.MustNewConstMetric(
		c.checkpointSaveLatency,
		prometheus.GaugeValue,
		float64(c.metric.GetCheckpointSaveLatency())/1000.0,
	)
}

func (c *Collector) collectBootstrapAndPartitionMetrics(ch chan<- prometheus.Metric) {
	ch <- prometheus.MustNewConstMetric(
		c.bootstrapDocumentTotal,
		prometheus.CounterValue,
		float64(c.metric.GetBootstrapDocumentTotal()),
	)

	bootstrapActive := 0.0
	if c.metric.GetBootstrapStatus() {
		bootstrapActive = 1.0
	}
	ch <- prometheus.MustNewConstMetric(
		c.bootstrapActive,
		prometheus.GaugeValue,
		bootstrapActive,
	)

	ch <- prometheus.MustNewConstMetric(
		c.activePartitionCount,
		prometheus.GaugeValue,
		float64(c.metric.GetActivePartitionCount()),
	)

	ch <- prometheus.MustNewConstMetric(
		c.partitionAcquireTotal,
		prometheus.CounterValue,
		float64(c.metric.GetPartitionAcquireTotal()),
	)

	ch <- prometheus.MustNewConstMetric(
		c.partitionReleaseTotal,
		prometheus.CounterValue,
		float64(c.metric.GetPartitionReleaseTotal()),
	)

	ch <- prometheus.MustNewConstMetric(
		c.resumeTokenExpiredTotal,
		prometheus.CounterValue,
		float64(c.metric.GetResumeTokenExpiredTotal()),
	)

	ch <- prometheus.MustNewConstMetric(
		c.changeStreamErrorTotal,
		prometheus.CounterValue,
		float64(c.metric.GetChangeStreamErrorTotal()),
	)

	ch <- prometheus.MustNewConstMetric(
		c.changeStreamRestartTotal,
		prometheus.CounterValue,
		float64(c.metric.GetChangeStreamRestartTotal()),
	)
}

func (c *Collector) collectLagAndListenerMetrics(ch chan<- prometheus.Metric) {
	lastEventTime := c.metric.GetLastEventTime()
	ch <- prometheus.MustNewConstMetric(
		c.lastEventTime,
		prometheus.GaugeValue,
		float64(lastEventTime),
	)

	if lastEventTime > 0 {
		eventLag := time.Since(time.Unix(lastEventTime, 0)).Seconds()
		ch <- prometheus.MustNewConstMetric(
			c.eventLagDuration,
			prometheus.GaugeValue,
			eventLag,
		)
	}

	listenerLatencyNs := c.metric.GetListenerLatency()
	ch <- prometheus.MustNewConstMetric(
		c.listenerLatency,
		prometheus.GaugeValue,
		float64(listenerLatencyNs)/1e9,
	)

	lastCheckpointTime := c.metric.GetLastCheckpointTime()
	var timeSinceCheckpoint float64
	if lastCheckpointTime > 0 {
		timeSinceCheckpoint = time.Since(time.Unix(lastCheckpointTime, 0)).Seconds()
	} else {
		timeSinceCheckpoint = -1
	}
	ch <- prometheus.MustNewConstMetric(
		c.timeSinceLastCheckpoint,
		prometheus.GaugeValue,
		timeSinceCheckpoint,
	)

	ch <- prometheus.MustNewConstMetric(
		c.listenerErrorTotal,
		prometheus.CounterValue,
		float64(c.metric.GetListenerErrorTotal()),
	)
}

func (c *Collector) collectBuildInfoMetrics(ch chan<- prometheus.Metric) {
	ch <- prometheus.MustNewConstMetric(
		c.partitionRebalanceTotal,
		prometheus.CounterValue,
		float64(c.metric.GetPartitionRebalanceTotal()),
	)

	ch <- prometheus.MustNewConstMetric(
		c.buildInfo,
		prometheus.GaugeValue,
		1,
		"1.0.0",
		"go1.25",
	)
}
