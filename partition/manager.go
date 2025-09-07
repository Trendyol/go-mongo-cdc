package partition

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/Trendyol/go-mongo-cdc/config"
	"github.com/Trendyol/go-mongo-cdc/mongo/connection"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.uber.org/zap"
)

const (
	TotalPartitions = 5
)

type Manager interface {
	Initialize(ctx context.Context) error
	AcquirePartitions(ctx context.Context) ([]int, error)
	ReleasePartitions(ctx context.Context) error
	RefreshPartitions(ctx context.Context) error
	SetPartitionsChangedCallback(callback func(newPartitions []int))
	Stop(ctx context.Context) error
}

type WorkerInfo struct {
	ID                 string    `bson:"_id"`
	AssignedPartitions []int     `bson:"assignedPartitions"`
	LastHeartbeat      time.Time `bson:"lastHeartbeat"`
}

type PartitionAssignment struct {
	PartitionID   int       `bson:"_id"`
	WorkerID      string    `bson:"workerId"`
	AssignedAt    time.Time `bson:"assignedAt"`
	LastHeartbeat time.Time `bson:"lastHeartbeat"`
}

type manager struct {
	workerID      string
	client        connection.Client
	workersCol    connection.Collection
	partitionsCol connection.Collection
	logger        *zap.Logger
	config        config.PartitionConfig

	mu                 sync.RWMutex
	assignedPartitions []int
	isRunning          bool

	stopCh chan struct{}
	wg     sync.WaitGroup

	// Worker change monitoring - simplified without change stream
	lastKnownWorkerCount int

	// Callback for partition changes to manage streams
	onPartitionsChanged func(newPartitions []int)
}

func NewManager(workerID string, client connection.Client, cfg config.PartitionConfig, logger *zap.Logger) Manager {
	return &manager{
		workerID:           workerID,
		client:             client,
		logger:             logger,
		config:             cfg,
		stopCh:             make(chan struct{}),
		assignedPartitions: make([]int, 0),
	}
}

func (m *manager) Initialize(ctx context.Context) error {
	db := m.client.Database(m.config.PartitionDatabase)
	m.workersCol = db.Collection("workers")
	m.partitionsCol = db.Collection("partition_assignments")

	if err := m.createIndexes(ctx); err != nil {
		return fmt.Errorf("failed to create indexes: %w", err)
	}

	if err := m.registerWorker(ctx); err != nil {
		return fmt.Errorf("failed to register worker: %w", err)
	}

	m.isRunning = true

	// Initialize with current worker count
	if activeWorkers, err := m.getActiveWorkerCount(context.Background()); err == nil {
		m.lastKnownWorkerCount = activeWorkers
	}

	m.wg.Add(1)
	go m.heartbeatLoop()

	m.wg.Add(1)
	go m.smartRebalanceMonitor()

	m.logger.Info("Partition manager initialized",
		zap.String("workerId", m.workerID),
		zap.Int("totalPartitions", TotalPartitions))

	return nil
}

func (m *manager) createIndexes(ctx context.Context) error {
	workerIndexes := []mongo.IndexModel{
		{
			Keys:    bson.D{{Key: "lastHeartbeat", Value: 1}},
			Options: options.Index().SetName("lastHeartbeat_1"),
		},
	}

	partitionIndexes := []mongo.IndexModel{
		{
			Keys:    bson.D{{Key: "workerId", Value: 1}},
			Options: options.Index().SetName("workerId_1"),
		},
		{
			Keys:    bson.D{{Key: "lastHeartbeat", Value: 1}},
			Options: options.Index().SetName("lastHeartbeat_1"),
		},
	}

	if _, err := m.workersCol.Indexes().CreateMany(ctx, workerIndexes); err != nil {
		return err
	}

	if _, err := m.partitionsCol.Indexes().CreateMany(ctx, partitionIndexes); err != nil {
		return err
	}

	return nil
}

func (m *manager) registerWorker(ctx context.Context) error {
	worker := WorkerInfo{
		ID:                 m.workerID,
		AssignedPartitions: []int{},
		LastHeartbeat:      time.Now(),
	}

	filter := bson.M{"_id": m.workerID}
	update := bson.M{"$set": worker}
	opts := options.Update().SetUpsert(true)

	_, err := m.workersCol.UpdateOne(ctx, filter, update, opts)
	return err
}

func (m *manager) heartbeatLoop() {
	defer m.wg.Done()

	ticker := time.NewTicker(m.config.HeartbeatInterval)
	defer ticker.Stop()

	for {
		select {
		case <-m.stopCh:
			return
		case <-ticker.C:
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)

			if err := m.sendHeartbeat(ctx); err != nil {
				m.logger.Error("Failed to send heartbeat", zap.Error(err))
			}

			if err := m.updatePartitionHeartbeats(ctx); err != nil {
				m.logger.Error("Failed to update partition heartbeats", zap.Error(err))
			}

			cancel()
		}
	}
}

func (m *manager) sendHeartbeat(ctx context.Context) error {
	filter := bson.M{"_id": m.workerID}
	update := bson.M{
		"$set": bson.M{
			"lastHeartbeat": time.Now(),
		},
	}

	result, err := m.workersCol.UpdateOne(ctx, filter, update)
	if err != nil {
		return err
	}

	if result.MatchedCount() == 0 {
		return m.registerWorker(ctx)
	}

	return nil
}

// TODO: worker heartbeat verebilirken partitionun veremedigi case olabilir mi?
func (m *manager) updatePartitionHeartbeats(ctx context.Context) error {
	m.mu.RLock()
	partitions := m.assignedPartitions
	m.mu.RUnlock()

	if len(partitions) == 0 {
		return nil
	}

	filter := bson.M{
		"_id":      bson.M{"$in": partitions},
		"workerId": m.workerID,
	}
	update := bson.M{
		"$set": bson.M{
			"lastHeartbeat": time.Now(),
		},
	}

	_, err := m.partitionsCol.UpdateMany(ctx, filter, update)
	return err
}

func (m *manager) smartRebalanceMonitor() {
	defer m.wg.Done()

	m.logger.Info("Starting smart rebalance monitoring")

	// Configurable rebalance check interval
	checkInterval := m.config.RebalanceCheckInterval
	ticker := time.NewTicker(checkInterval)
	defer ticker.Stop()

	for {
		select {
		case <-m.stopCh:
			m.logger.Info("Stopping smart rebalance monitoring")
			return
		case <-ticker.C:
			m.checkForWorkerChangesAndRebalance()
		}
	}
}

func (m *manager) checkForWorkerChangesAndRebalance() {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	// Mevcut aktif worker sayısını kontrol et
	currentWorkerCount, err := m.getActiveWorkerCount(ctx)
	if err != nil {
		m.logger.Error("Failed to get current worker count", zap.Error(err))
		return
	}

	// Worker sayısında değişiklik var mı kontrol et
	if currentWorkerCount != m.lastKnownWorkerCount {
		m.logger.Info("Worker count change detected",
			zap.Int("previousCount", m.lastKnownWorkerCount),
			zap.Int("currentCount", currentWorkerCount))

		// Rebalance işlemini tetikle
		if err := m.RefreshPartitions(ctx); err != nil {
			m.logger.Error("Failed to refresh partitions after worker count change",
				zap.Error(err),
				zap.Int("workerCount", currentWorkerCount))
		} else {
			m.logger.Info("Successfully rebalanced partitions after worker count change",
				zap.Int("newWorkerCount", currentWorkerCount))

			// Güncellenen worker sayısını kaydet
			m.lastKnownWorkerCount = currentWorkerCount
		}
	}
}

func (m *manager) AcquirePartitions(ctx context.Context) ([]int, error) {
	if err := m.cleanupDeadWorkers(ctx); err != nil {
		m.logger.Error("Failed to cleanup dead workers", zap.Error(err))
	}

	activeWorkers, err := m.getActiveWorkerCount(ctx)
	if err != nil {
		return nil, err
	}

	//TODO: activeWorker bulamıyorsam default 1 mi yapmalıyım panic mi atmalıyım?
	if activeWorkers == 0 {
		activeWorkers = 1
	}

	partitionsPerWorker := TotalPartitions / activeWorkers
	extraPartitions := TotalPartitions % activeWorkers

	workerIndex, err := m.getWorkerIndex(ctx)
	if err != nil {
		return nil, err
	}

	startPartition := workerIndex * partitionsPerWorker
	endPartition := startPartition + partitionsPerWorker

	if workerIndex < extraPartitions {
		startPartition += workerIndex
		endPartition += workerIndex + 1
	} else {
		startPartition += extraPartitions
		endPartition += extraPartitions
	}

	// Calculate expected partitions for this worker
	var expectedPartitions []int
	for i := startPartition; i < endPartition && i < TotalPartitions; i++ {
		expectedPartitions = append(expectedPartitions, i)
	}

	var acquiredPartitions []int
	for _, partitionID := range expectedPartitions {
		if err := m.acquirePartition(ctx, partitionID); err != nil {
			m.logger.Info("Failed to acquire partition",
				zap.Int("partition", partitionID),
				zap.Error(err))
			continue
		}
		acquiredPartitions = append(acquiredPartitions, partitionID)
	}

	m.mu.Lock()
	m.assignedPartitions = acquiredPartitions
	m.mu.Unlock()

	if err := m.updateWorkerPartitions(ctx, acquiredPartitions); err != nil {
		m.logger.Error("Failed to update worker partitions", zap.Error(err))
	}

	m.logger.Info("Acquired partitions",
		zap.Int("count", len(acquiredPartitions)),
		zap.Ints("partitions", acquiredPartitions),
		zap.Int("workerIndex", workerIndex),
		zap.Int("activeWorkers", activeWorkers),
		zap.Int("startPartition", startPartition),
		zap.Int("endPartition", endPartition),
		zap.Int("partitionsPerWorker", partitionsPerWorker),
		zap.Int("extraPartitions", extraPartitions))

	return acquiredPartitions, nil
}

func (m *manager) cleanupDeadWorkers(ctx context.Context) error {
	cutoff := time.Now().Add(-m.config.WorkerTimeout)

	//TODO: sadece workers tablosuna filter atıyor eger dead worker
	//bulursa partition ve workeri temizliyor o zaman neden 2 tablo icin de herthbeat tutuyoruz
	filter := bson.M{"lastHeartbeat": bson.M{"$lt": cutoff}}
	cursor, err := m.workersCol.Find(ctx, filter)
	if err != nil {
		return err
	}
	defer cursor.Close(ctx)

	var deadWorkerIDs []string
	for cursor.Next(ctx) {
		var worker WorkerInfo
		if err := cursor.Decode(&worker); err != nil {
			continue
		}
		deadWorkerIDs = append(deadWorkerIDs, worker.ID)
	}

	if len(deadWorkerIDs) > 0 {
		partitionFilter := bson.M{"workerId": bson.M{"$in": deadWorkerIDs}}
		_, err = m.partitionsCol.DeleteMany(ctx, partitionFilter)
		if err != nil {
			m.logger.Error("Failed to release dead worker partitions", zap.Error(err))
		}

		workerFilter := bson.M{"_id": bson.M{"$in": deadWorkerIDs}}
		_, err = m.workersCol.DeleteMany(ctx, workerFilter)
		if err != nil {
			m.logger.Error("Failed to delete dead workers", zap.Error(err))
		}

		m.logger.Info("Cleaned up dead workers", zap.Strings("workerIds", deadWorkerIDs))
	}

	return nil
}

func (m *manager) getActiveWorkerCount(ctx context.Context) (int, error) {
	cutoff := time.Now().Add(-m.config.WorkerTimeout)
	filter := bson.M{"lastHeartbeat": bson.M{"$gte": cutoff}}

	//TODO: zaten cleanupDeadWorkers de bunları temizliyorum neden tekrar filter yapıyorum bir sekilde anlık hata alırsa safe olmak icin mi?
	count, err := m.workersCol.CountDocuments(ctx, filter)
	if err != nil {
		return 0, err
	}

	return int(count), nil
}

func (m *manager) getWorkerIndex(ctx context.Context) (int, error) {
	cutoff := time.Now().Add(-m.config.WorkerTimeout)
	//TODO: zaten cleanupDeadWorkers de bunları temizliyorum neden tekrar filter yapıyorum bir sekilde anlık hata alırsa safe olmak icin mi?
	filter := bson.M{"lastHeartbeat": bson.M{"$gte": cutoff}}
	opts := options.Find().SetSort(bson.D{{Key: "_id", Value: 1}})

	cursor, err := m.workersCol.Find(ctx, filter, opts)
	if err != nil {
		return -1, err
	}
	defer cursor.Close(ctx)

	index := 0
	for cursor.Next(ctx) {
		var worker WorkerInfo
		if err := cursor.Decode(&worker); err != nil {
			continue
		}

		if worker.ID == m.workerID {
			return index, nil
		}
		index++
	}

	//TODO: worker not found ise panic?
	return -1, fmt.Errorf("worker not found in active workers list")
}

func (m *manager) acquirePartition(ctx context.Context, partitionID int) error {
	assignment := PartitionAssignment{
		PartitionID:   partitionID,
		WorkerID:      m.workerID,
		AssignedAt:    time.Now(),
		LastHeartbeat: time.Now(),
	}

	cutoff := time.Now().Add(-m.config.WorkerTimeout)

	// 1. Aşama: Mevcut document'ı conditional update et (timeout olmuş veya zaten bizim)
	filter := bson.M{
		"_id": partitionID,
		"$or": []bson.M{
			{"lastHeartbeat": bson.M{"$lt": cutoff}}, // Timeout olmuş
			{"workerId": m.workerID},                 // Zaten bizim
		},
	}

	update := bson.M{"$set": assignment}
	result, err := m.partitionsCol.UpdateOne(ctx, filter, update)
	if err != nil {
		return fmt.Errorf("failed to update existing partition: %w", err)
	}

	if result.MatchedCount() > 0 {
		return nil // Başarıyla güncellendi
	}

	// 2. Aşama: Yeni partition oluşturmaya çalış (ilk defa assign ediliyor)
	_, err = m.partitionsCol.InsertOne(ctx, assignment)
	if err != nil {
		if isDuplicateKeyError(err) {
			// Race condition: partition zaten var ve aktif owner tarafından sahiplenilmiş
			return fmt.Errorf("partition %d is already owned by another active worker", partitionID)
		}
		return fmt.Errorf("failed to insert new partition assignment: %w", err)
	}

	return nil // Başarıyla oluşturuldu
}

func (m *manager) releasePartition(ctx context.Context, partitionID int) error {
	filter := bson.M{
		"_id":      partitionID,
		"workerId": m.workerID, // Only release if we own it
	}

	result, err := m.partitionsCol.DeleteOne(ctx, filter)
	if err != nil {
		return fmt.Errorf("failed to release partition %d: %w", partitionID, err)
	}

	if result.DeletedCount() == 0 {
		m.logger.Warn("Partition was not owned by this worker during release",
			zap.Int("partition", partitionID),
			zap.String("workerId", m.workerID))
	} else {
		m.logger.Debug("Successfully released partition",
			zap.Int("partition", partitionID),
			zap.String("workerId", m.workerID))
	}

	return nil
}

func isDuplicateKeyError(err error) bool {
	return err != nil && (err.Error() == "E11000" ||
		strings.Contains(err.Error(), "E11000") ||
		strings.Contains(err.Error(), "duplicate key error"))
}

func (m *manager) updateWorkerPartitions(ctx context.Context, partitions []int) error {
	filter := bson.M{"_id": m.workerID}
	update := bson.M{
		"$set": bson.M{
			"assignedPartitions": partitions,
			"lastHeartbeat":      time.Now(),
		},
	}

	_, err := m.workersCol.UpdateOne(ctx, filter, update)
	return err
}

func (m *manager) RefreshPartitions(ctx context.Context) error {
	// 1. Mevcut partition'ları al
	m.mu.RLock()
	currentPartitions := make([]int, len(m.assignedPartitions))
	copy(currentPartitions, m.assignedPartitions)
	m.mu.RUnlock()

	// 2. Yeni partition'ları hesapla ve acquire et
	newPartitions, err := m.AcquirePartitions(ctx)
	if err != nil {
		return err
	}

	// 3. Artık sahip olmadığımız partition'ları MongoDB'den release et
	var partitionsToRelease []int
	for _, current := range currentPartitions {
		shouldKeep := false
		for _, new := range newPartitions {
			if current == new {
				shouldKeep = true
				break
			}
		}
		if !shouldKeep {
			partitionsToRelease = append(partitionsToRelease, current)
		}
	}

	if len(partitionsToRelease) > 0 {
		m.logger.Info("Releasing partitions from MongoDB",
			zap.Ints("partitionsToRelease", partitionsToRelease))

		for _, partitionID := range partitionsToRelease {
			if err := m.releasePartition(ctx, partitionID); err != nil {
				m.logger.Error("Failed to release partition from MongoDB",
					zap.Int("partition", partitionID),
					zap.Error(err))
			}
		}
	}

	// 4. Stream layer'ı bilgilendir (sadece yeni partition listesi)
	if m.onPartitionsChanged != nil {
		m.onPartitionsChanged(newPartitions)
	}

	return nil
}

func (m *manager) SetPartitionsChangedCallback(callback func(newPartitions []int)) {
	m.onPartitionsChanged = callback
}

func (m *manager) ReleasePartitions(ctx context.Context) error {
	m.mu.RLock()
	partitions := make([]int, len(m.assignedPartitions))
	copy(partitions, m.assignedPartitions)
	m.mu.RUnlock()

	if len(partitions) == 0 {
		return nil
	}

	filter := bson.M{"workerId": m.workerID}
	_, err := m.partitionsCol.DeleteMany(ctx, filter)
	if err != nil {
		return err
	}

	m.mu.Lock()
	m.assignedPartitions = []int{}
	m.mu.Unlock()

	return m.updateWorkerPartitions(ctx, []int{})
}

func (m *manager) Stop(ctx context.Context) error {
	m.mu.Lock()
	if !m.isRunning {
		m.mu.Unlock()
		return nil
	}
	m.isRunning = false
	m.mu.Unlock()

	close(m.stopCh)

	done := make(chan struct{})
	go func() {
		m.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		m.logger.Warn("Timeout waiting for heartbeat loop to stop")
	}

	if err := m.ReleasePartitions(ctx); err != nil {
		m.logger.Error("Failed to release partitions during stop", zap.Error(err))
	}

	//TODO: ReleasePartitions icerisinde worker'in partitionlarını empty slice yapıyoruz zaten hemen sonrasında siliyoruz gerek var mı?
	filter := bson.M{"_id": m.workerID}
	if _, err := m.workersCol.DeleteOne(ctx, filter); err != nil {
		m.logger.Error("Failed to unregister worker", zap.Error(err))
	}

	m.logger.Info("Partition manager stopped")
	return nil
}
