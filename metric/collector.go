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

	processLatency *prometheus.Desc
	cdcLatency     *prometheus.Desc

	checkpointSaveTotal      *prometheus.Desc
	checkpointSaveErrorTotal *prometheus.Desc
	checkpointSaveLatency    *prometheus.Desc

	bootstrapDocumentTotal *prometheus.Desc
	bootstrapProgress      *prometheus.Desc
	bootstrapActive        *prometheus.Desc

	activePartitionCount  *prometheus.Desc
	partitionAcquireTotal *prometheus.Desc
	partitionReleaseTotal *prometheus.Desc

	resumeTokenExpiredTotal  *prometheus.Desc
	changeStreamErrorTotal   *prometheus.Desc
	changeStreamRestartTotal *prometheus.Desc

	workerHealthy           *prometheus.Desc
	lastEventTime           *prometheus.Desc
	eventLagDuration        *prometheus.Desc
	listenerLatency         *prometheus.Desc
	timeSinceLastCheckpoint *prometheus.Desc
	listenerErrorTotal      *prometheus.Desc
	partitionRebalanceTotal *prometheus.Desc

	replicationLag       *prometheus.Desc
	activeConnections    *prometheus.Desc
	availableConnections *prometheus.Desc

	shardReplicationLag *prometheus.Desc

	buildInfo *prometheus.Desc
}

type ShardMetricsHolder interface {
	GetShardMetrics() []*ShardMetrics
}

func NewCollector(m Metric) *Collector {
	namespace := "go_mongo_cdc"

	return &Collector{
		metric: m,

		insertTotal: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, "", "insert_total"),
			"Total number of insert operations processed",
			nil,
			nil,
		),
		updateTotal: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, "", "update_total"),
			"Total number of update operations processed",
			nil,
			nil,
		),
		deleteTotal: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, "", "delete_total"),
			"Total number of delete operations processed",
			nil,
			nil,
		),
		replaceTotal: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, "", "replace_total"),
			"Total number of replace operations processed",
			nil,
			nil,
		),

		processLatency: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, "", "process_latency_seconds"),
			"Processing latency in seconds",
			nil,
			nil,
		),
		cdcLatency: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, "", "cdc_latency_seconds"),
			"CDC latency in seconds",
			nil,
			nil,
		),

		checkpointSaveTotal: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, "", "checkpoint_save_total"),
			"Total number of checkpoint save operations",
			nil,
			nil,
		),
		checkpointSaveErrorTotal: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, "", "checkpoint_save_error_total"),
			"Total number of checkpoint save errors",
			nil,
			nil,
		),
		checkpointSaveLatency: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, "", "checkpoint_save_latency_seconds"),
			"Checkpoint save latency in seconds",
			nil,
			nil,
		),

		bootstrapDocumentTotal: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, "", "bootstrap_document_total"),
			"Total number of documents processed during bootstrap",
			nil,
			nil,
		),
		bootstrapProgress: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, "", "bootstrap_progress_percent"),
			"Bootstrap progress percentage (0-100)",
			nil,
			nil,
		),
		bootstrapActive: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, "", "bootstrap_active"),
			"Bootstrap active status (1=active, 0=inactive)",
			nil,
			nil,
		),

		activePartitionCount: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, "", "active_partition_count"),
			"Number of currently active partitions",
			nil,
			nil,
		),
		partitionAcquireTotal: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, "", "partition_acquire_total"),
			"Total number of partition acquisitions",
			nil,
			nil,
		),
		partitionReleaseTotal: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, "", "partition_release_total"),
			"Total number of partition releases",
			nil,
			nil,
		),

		resumeTokenExpiredTotal: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, "", "resume_token_expired_total"),
			"Total number of expired resume tokens",
			nil,
			nil,
		),
		changeStreamErrorTotal: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, "", "change_stream_error_total"),
			"Total number of change stream errors",
			nil,
			nil,
		),
		changeStreamRestartTotal: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, "", "change_stream_restart_total"),
			"Total number of change stream restarts",
			nil,
			nil,
		),

		workerHealthy: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, "", "worker_healthy"),
			"Worker health status (1=healthy, 0=unhealthy)",
			nil,
			nil,
		),
		lastEventTime: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, "", "last_event_time_seconds"),
			"Unix timestamp of the last processed event",
			nil,
			nil,
		),
		eventLagDuration: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, "", "event_lag_seconds"),
			"Time since the last event was processed in seconds",
			nil,
			nil,
		),
		listenerLatency: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, "", "listener_latency_seconds"),
			"Listener function execution latency in seconds",
			nil,
			nil,
		),
		timeSinceLastCheckpoint: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, "", "time_since_last_checkpoint_seconds"),
			"Time since last checkpoint was saved in seconds",
			nil,
			nil,
		),
		listenerErrorTotal: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, "", "listener_error_total"),
			"Total number of listener function errors",
			nil,
			nil,
		),
		partitionRebalanceTotal: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, "", "partition_rebalance_total"),
			"Total number of partition rebalance operations",
			nil,
			nil,
		),

		replicationLag: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, "mongodb", "replication_lag_seconds"),
			"MongoDB replication lag in seconds",
			nil,
			nil,
		),
		activeConnections: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, "mongodb", "connections_active"),
			"MongoDB active connections",
			nil,
			nil,
		),
		availableConnections: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, "mongodb", "connections_available"),
			"MongoDB available connections",
			nil,
			nil,
		),

		shardReplicationLag: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, "mongodb_shard", "replication_lag_seconds"),
			"MongoDB shard replication lag in seconds",
			[]string{"shard"},
			nil,
		),

		buildInfo: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, "", "build_info"),
			"Build information",
			[]string{"version", "go_version"},
			nil,
		),
	}
}

func (c *Collector) Describe(ch chan<- *prometheus.Desc) {
	prometheus.DescribeByCollect(c, ch)
}

func (c *Collector) Collect(ch chan<- prometheus.Metric) {
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

	ch <- prometheus.MustNewConstMetric(
		c.processLatency,
		prometheus.GaugeValue,
		float64(c.metric.GetProcessLatency()),
	)

	ch <- prometheus.MustNewConstMetric(
		c.cdcLatency,
		prometheus.GaugeValue,
		float64(c.metric.GetCDCLatency()),
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
		float64(c.metric.GetCheckpointSaveLatency()),
	)

	ch <- prometheus.MustNewConstMetric(
		c.bootstrapDocumentTotal,
		prometheus.CounterValue,
		float64(c.metric.GetBootstrapDocumentTotal()),
	)

	ch <- prometheus.MustNewConstMetric(
		c.bootstrapProgress,
		prometheus.GaugeValue,
		c.metric.GetBootstrapProgress(),
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

	workerHealthy := 0.0
	if c.metric.GetWorkerHealthy() {
		workerHealthy = 1.0
	}
	ch <- prometheus.MustNewConstMetric(
		c.workerHealthy,
		prometheus.GaugeValue,
		workerHealthy,
	)

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

	// Listener latency (convert nanoseconds to seconds)
	listenerLatencyNs := c.metric.GetListenerLatency()
	ch <- prometheus.MustNewConstMetric(
		c.listenerLatency,
		prometheus.GaugeValue,
		float64(listenerLatencyNs)/1e9,
	)

	// Time since last checkpoint
	lastCheckpointTime := c.metric.GetLastCheckpointTime()
	var timeSinceCheckpoint float64
	if lastCheckpointTime > 0 {
		timeSinceCheckpoint = time.Since(time.Unix(lastCheckpointTime, 0)).Seconds()
	} else {
		// If no checkpoint has been saved yet, report -1 to indicate "never"
		timeSinceCheckpoint = -1
	}
	ch <- prometheus.MustNewConstMetric(
		c.timeSinceLastCheckpoint,
		prometheus.GaugeValue,
		timeSinceCheckpoint,
	)

	// Listener errors
	ch <- prometheus.MustNewConstMetric(
		c.listenerErrorTotal,
		prometheus.CounterValue,
		float64(c.metric.GetListenerErrorTotal()),
	)

	// Partition rebalance
	ch <- prometheus.MustNewConstMetric(
		c.partitionRebalanceTotal,
		prometheus.CounterValue,
		float64(c.metric.GetPartitionRebalanceTotal()),
	)

	mongoMetrics := c.metric.GetMongoDBMetrics()
	if mongoMetrics != nil {
		ch <- prometheus.MustNewConstMetric(
			c.replicationLag,
			prometheus.GaugeValue,
			float64(mongoMetrics.ReplicationLag),
		)

		ch <- prometheus.MustNewConstMetric(
			c.activeConnections,
			prometheus.GaugeValue,
			float64(mongoMetrics.ActiveConnections),
		)

		ch <- prometheus.MustNewConstMetric(
			c.availableConnections,
			prometheus.GaugeValue,
			float64(mongoMetrics.AvailableConnections),
		)
	}

	if holder, ok := c.metric.(ShardMetricsHolder); ok {
		shardMetrics := holder.GetShardMetrics()
		for _, sm := range shardMetrics {
			if sm.Metrics == nil {
				continue
			}

			ch <- prometheus.MustNewConstMetric(
				c.shardReplicationLag,
				prometheus.GaugeValue,
				float64(sm.Metrics.ReplicationLag),
				sm.ShardName,
			)
		}
	}

	ch <- prometheus.MustNewConstMetric(
		c.buildInfo,
		prometheus.GaugeValue,
		1,
		"1.0.0",
		"go1.25",
	)
}
