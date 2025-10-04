package metric

import (
	"sync/atomic"
	"time"
)

type Metric interface {
	IncInsertTotal()
	IncUpdateTotal()
	IncDeleteTotal()
	IncReplaceTotal()

	SetProcessLatency(latencyNs int64)
	SetCDCLatency(latencyMs int64)

	IncCheckpointSaveTotal()
	IncCheckpointSaveErrorTotal()
	SetCheckpointSaveLatency(latencyMs int64)

	IncBootstrapDocumentTotal()
	SetBootstrapProgress(progress float64)
	SetBootstrapStatus(isActive bool)

	IncPartitionAcquireTotal()
	IncPartitionReleaseTotal()
	SetActivePartitionCount(count int)

	IncResumeTokenExpiredTotal()
	IncChangeStreamErrorTotal()
	IncChangeStreamRestartTotal()

	SetWorkerHealthy(isHealthy bool)
	SetLastEventTime(t time.Time)

	GetInsertTotal() int64
	GetUpdateTotal() int64
	GetDeleteTotal() int64
	GetReplaceTotal() int64
	GetProcessLatency() int64
	GetCDCLatency() int64
	GetCheckpointSaveTotal() int64
	GetCheckpointSaveErrorTotal() int64
	GetCheckpointSaveLatency() int64
	GetBootstrapDocumentTotal() int64
	GetBootstrapProgress() float64
	GetBootstrapStatus() bool
	GetActivePartitionCount() int
	GetPartitionAcquireTotal() int64
	GetPartitionReleaseTotal() int64
	GetResumeTokenExpiredTotal() int64
	GetChangeStreamErrorTotal() int64
	GetChangeStreamRestartTotal() int64
	GetWorkerHealthy() bool
	GetLastEventTime() int64
}

type metric struct {
	database   string
	collection string

	insertTotal  int64
	updateTotal  int64
	deleteTotal  int64
	replaceTotal int64

	processLatency int64
	cdcLatency     int64

	checkpointSaveTotal      int64
	checkpointSaveErrorTotal int64
	checkpointSaveLatency    int64

	bootstrapDocumentTotal int64
	bootstrapProgress      int64
	bootstrapStatus        int64

	activePartitionCount  int64
	partitionAcquireTotal int64
	partitionReleaseTotal int64

	resumeTokenExpiredTotal  int64
	changeStreamErrorTotal   int64
	changeStreamRestartTotal int64

	workerHealthy int64
	lastEventTime int64
}

func NewMetric(database, collection string) Metric {
	return &metric{
		database:      database,
		collection:    collection,
		workerHealthy: 1,
	}
}

func (m *metric) IncInsertTotal() {
	atomic.AddInt64(&m.insertTotal, 1)
}

func (m *metric) IncUpdateTotal() {
	atomic.AddInt64(&m.updateTotal, 1)
}

func (m *metric) IncDeleteTotal() {
	atomic.AddInt64(&m.deleteTotal, 1)
}

func (m *metric) IncReplaceTotal() {
	atomic.AddInt64(&m.replaceTotal, 1)
}

func (m *metric) SetProcessLatency(latencyNs int64) {
	latencyMs := latencyNs / 1000000
	atomic.StoreInt64(&m.processLatency, latencyMs)
}

func (m *metric) SetCDCLatency(latencyMs int64) {
	atomic.StoreInt64(&m.cdcLatency, latencyMs)
}

func (m *metric) IncCheckpointSaveTotal() {
	atomic.AddInt64(&m.checkpointSaveTotal, 1)
}

func (m *metric) IncCheckpointSaveErrorTotal() {
	atomic.AddInt64(&m.checkpointSaveErrorTotal, 1)
}

func (m *metric) SetCheckpointSaveLatency(latencyMs int64) {
	atomic.StoreInt64(&m.checkpointSaveLatency, latencyMs)
}

func (m *metric) IncBootstrapDocumentTotal() {
	atomic.AddInt64(&m.bootstrapDocumentTotal, 1)
}

func (m *metric) SetBootstrapProgress(progress float64) {
	atomic.StoreInt64(&m.bootstrapProgress, int64(progress*100))
}

func (m *metric) SetBootstrapStatus(isActive bool) {
	if isActive {
		atomic.StoreInt64(&m.bootstrapStatus, 1)
	} else {
		atomic.StoreInt64(&m.bootstrapStatus, 0)
	}
}

func (m *metric) IncPartitionAcquireTotal() {
	atomic.AddInt64(&m.partitionAcquireTotal, 1)
}

func (m *metric) IncPartitionReleaseTotal() {
	atomic.AddInt64(&m.partitionReleaseTotal, 1)
}

func (m *metric) SetActivePartitionCount(count int) {
	atomic.StoreInt64(&m.activePartitionCount, int64(count))
}

func (m *metric) IncResumeTokenExpiredTotal() {
	atomic.AddInt64(&m.resumeTokenExpiredTotal, 1)
}

func (m *metric) IncChangeStreamErrorTotal() {
	atomic.AddInt64(&m.changeStreamErrorTotal, 1)
}

func (m *metric) IncChangeStreamRestartTotal() {
	atomic.AddInt64(&m.changeStreamRestartTotal, 1)
}

func (m *metric) SetWorkerHealthy(isHealthy bool) {
	if isHealthy {
		atomic.StoreInt64(&m.workerHealthy, 1)
	} else {
		atomic.StoreInt64(&m.workerHealthy, 0)
	}
}

func (m *metric) SetLastEventTime(t time.Time) {
	atomic.StoreInt64(&m.lastEventTime, t.Unix())
}

func (m *metric) GetInsertTotal() int64 {
	return atomic.LoadInt64(&m.insertTotal)
}

func (m *metric) GetUpdateTotal() int64 {
	return atomic.LoadInt64(&m.updateTotal)
}

func (m *metric) GetDeleteTotal() int64 {
	return atomic.LoadInt64(&m.deleteTotal)
}

func (m *metric) GetReplaceTotal() int64 {
	return atomic.LoadInt64(&m.replaceTotal)
}

func (m *metric) GetProcessLatency() int64 {
	return atomic.LoadInt64(&m.processLatency)
}

func (m *metric) GetCDCLatency() int64 {
	return atomic.LoadInt64(&m.cdcLatency)
}

func (m *metric) GetCheckpointSaveTotal() int64 {
	return atomic.LoadInt64(&m.checkpointSaveTotal)
}

func (m *metric) GetCheckpointSaveErrorTotal() int64 {
	return atomic.LoadInt64(&m.checkpointSaveErrorTotal)
}

func (m *metric) GetCheckpointSaveLatency() int64 {
	return atomic.LoadInt64(&m.checkpointSaveLatency)
}

func (m *metric) GetBootstrapDocumentTotal() int64 {
	return atomic.LoadInt64(&m.bootstrapDocumentTotal)
}

func (m *metric) GetBootstrapProgress() float64 {
	return float64(atomic.LoadInt64(&m.bootstrapProgress)) / 100.0
}

func (m *metric) GetBootstrapStatus() bool {
	return atomic.LoadInt64(&m.bootstrapStatus) == 1
}

func (m *metric) GetActivePartitionCount() int {
	return int(atomic.LoadInt64(&m.activePartitionCount))
}

func (m *metric) GetPartitionAcquireTotal() int64 {
	return atomic.LoadInt64(&m.partitionAcquireTotal)
}

func (m *metric) GetPartitionReleaseTotal() int64 {
	return atomic.LoadInt64(&m.partitionReleaseTotal)
}

func (m *metric) GetResumeTokenExpiredTotal() int64 {
	return atomic.LoadInt64(&m.resumeTokenExpiredTotal)
}

func (m *metric) GetChangeStreamErrorTotal() int64 {
	return atomic.LoadInt64(&m.changeStreamErrorTotal)
}

func (m *metric) GetChangeStreamRestartTotal() int64 {
	return atomic.LoadInt64(&m.changeStreamRestartTotal)
}

func (m *metric) GetWorkerHealthy() bool {
	return atomic.LoadInt64(&m.workerHealthy) == 1
}

func (m *metric) GetLastEventTime() int64 {
	return atomic.LoadInt64(&m.lastEventTime)
}
