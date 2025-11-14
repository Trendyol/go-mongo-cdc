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

	SetCDCLatency(latencyMs int64)

	IncCheckpointSaveTotal()
	IncCheckpointSaveErrorTotal()
	SetCheckpointSaveLatency(latencyMs int64)

	IncBootstrapDocumentTotal()
	SetBootstrapStatus(isActive bool)

	IncPartitionAcquireTotal()
	IncPartitionReleaseTotal()
	SetActivePartitionCount(count int)

	IncResumeTokenExpiredTotal()
	IncChangeStreamErrorTotal()
	IncChangeStreamRestartTotal()

	SetLastEventTime(t time.Time)
	SetListenerLatency(latencyNs int64)
	SetLastCheckpointTime(t time.Time)
	IncListenerErrorTotal()
	IncPartitionRebalanceTotal()

	GetInsertTotal() int64
	GetUpdateTotal() int64
	GetDeleteTotal() int64
	GetReplaceTotal() int64
	GetCDCLatency() int64
	GetCheckpointSaveTotal() int64
	GetCheckpointSaveErrorTotal() int64
	GetCheckpointSaveLatency() int64
	GetBootstrapDocumentTotal() int64
	GetBootstrapStatus() bool
	GetActivePartitionCount() int
	GetPartitionAcquireTotal() int64
	GetPartitionReleaseTotal() int64
	GetResumeTokenExpiredTotal() int64
	GetChangeStreamErrorTotal() int64
	GetChangeStreamRestartTotal() int64
	GetLastEventTime() int64
	GetListenerLatency() int64
	GetLastCheckpointTime() int64
	GetListenerErrorTotal() int64
	GetPartitionRebalanceTotal() int64
}

type metric struct {
	database   string
	collection string

	insertTotal  int64
	updateTotal  int64
	deleteTotal  int64
	replaceTotal int64

	cdcLatency int64

	checkpointSaveTotal      int64
	checkpointSaveErrorTotal int64
	checkpointSaveLatency    int64

	bootstrapDocumentTotal int64
	bootstrapStatus        int64

	activePartitionCount  int64
	partitionAcquireTotal int64
	partitionReleaseTotal int64

	resumeTokenExpiredTotal  int64
	changeStreamErrorTotal   int64
	changeStreamRestartTotal int64

	lastEventTime int64

	listenerLatency         int64
	lastCheckpointTime      int64
	listenerErrorTotal      int64
	partitionRebalanceTotal int64
}

func NewMetric(database, collection string) Metric {
	return &metric{
		database:   database,
		collection: collection,
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

func (m *metric) GetLastEventTime() int64 {
	return atomic.LoadInt64(&m.lastEventTime)
}

func (m *metric) SetListenerLatency(latencyNs int64) {
	atomic.StoreInt64(&m.listenerLatency, latencyNs)
}

func (m *metric) GetListenerLatency() int64 {
	return atomic.LoadInt64(&m.listenerLatency)
}

func (m *metric) SetLastCheckpointTime(t time.Time) {
	atomic.StoreInt64(&m.lastCheckpointTime, t.Unix())
}

func (m *metric) GetLastCheckpointTime() int64 {
	return atomic.LoadInt64(&m.lastCheckpointTime)
}

func (m *metric) IncListenerErrorTotal() {
	atomic.AddInt64(&m.listenerErrorTotal, 1)
}

func (m *metric) GetListenerErrorTotal() int64 {
	return atomic.LoadInt64(&m.listenerErrorTotal)
}

func (m *metric) IncPartitionRebalanceTotal() {
	atomic.AddInt64(&m.partitionRebalanceTotal, 1)
}

func (m *metric) GetPartitionRebalanceTotal() int64 {
	return atomic.LoadInt64(&m.partitionRebalanceTotal)
}
