package partition

import (
	"context"
	"fmt"
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
	//GetAssignedPartitions() []int
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

	// Worker change monitoring
	workerChangeStream connection.ChangeStream

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

	m.wg.Add(1)
	go m.heartbeatLoop()

	m.wg.Add(1)
	go m.monitorWorkerChanges()

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

// TODO: burası degisiklikleri 5 saniye falan gec dinliyor bu neden arastırılacak
func (m *manager) monitorWorkerChanges() {
	defer m.wg.Done()

	m.logger.Info("Starting worker change monitoring")

	pipeline := []bson.D{
		{
			{Key: "$match", Value: bson.D{
				{Key: "operationType", Value: bson.D{
					{Key: "$in", Value: []string{"insert", "delete"}},
				}},
			}},
		},
	}

	var retryCount int
	maxRetries := 5

	for {
		select {
		case <-m.stopCh:
			m.logger.Info("Stopping worker change monitoring")
			if m.workerChangeStream != nil {
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				m.workerChangeStream.Close(ctx)
				cancel()
			}
			return
		default:
			changeStream, err := m.workersCol.Watch(context.Background(), pipeline)
			if err != nil {
				retryCount++
				m.logger.Error("Failed to create worker change stream",
					zap.Error(err),
					zap.Int("retryCount", retryCount))

				if retryCount >= maxRetries {
					m.logger.Fatal("Max retries reached for worker change stream")
					return
				}

				time.Sleep(time.Duration(retryCount) * 2 * time.Second)
				continue
			}

			m.workerChangeStream = changeStream
			retryCount = 0

			m.logger.Info("Worker change stream created successfully")

			for changeStream.Next(context.Background()) {
				var changeDoc bson.M
				if err := changeStream.Decode(&changeDoc); err != nil {
					m.logger.Error("Failed to decode worker change document", zap.Error(err))
					continue
				}

				operationType := changeDoc["operationType"].(string)
				m.logger.Debug("Worker change detected",
					zap.String("operation", operationType))

				// TODO: buna gerek var mı?
				// Wait for the change to be fully propagated and new worker to be ready
				//time.Sleep(50 * time.Millisecond)

				ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
				err := m.RefreshPartitions(ctx)
				cancel()

				if err != nil {
					m.logger.Error("Failed to refresh partitions after worker change",
						zap.Error(err),
						zap.String("operation", operationType))
				} else {
					m.logger.Info("Successfully refreshed partitions after worker change",
						zap.String("operation", operationType))
				}
			}

			if err := changeStream.Err(); err != nil {
				m.logger.Error("Worker change stream error", zap.Error(err))
			}

			changeStream.Close(context.Background())
			m.workerChangeStream = nil

			m.logger.Debug("Worker change stream closed, will retry")
			time.Sleep(5 * time.Second)
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

	var acquiredPartitions []int

	for i := startPartition; i < endPartition && i < TotalPartitions; i++ {
		if err := m.acquirePartition(ctx, i); err != nil {
			m.logger.Warn("Failed to acquire partition",
				zap.Int("partition", i),
				zap.Error(err))
			continue
		}
		acquiredPartitions = append(acquiredPartitions, i)
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
		zap.Int("activeWorkers", activeWorkers))

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

	filter := bson.M{"_id": partitionID}
	update := bson.M{"$set": assignment}
	opts := options.Update().SetUpsert(true)

	_, err := m.partitionsCol.UpdateOne(ctx, filter, update, opts)
	return err
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
	newPartitions, err := m.AcquirePartitions(ctx)
	if err != nil {
		return err
	}

	// Notify about partition changes to manage streams
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

/*func (m *manager) GetAssignedPartitions() []int {
	m.mu.RLock()
	defer m.mu.RUnlock()

	result := make([]int, len(m.assignedPartitions))
	copy(result, m.assignedPartitions)
	return result
}*/

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
	case <-time.After(10 * time.Second):
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
