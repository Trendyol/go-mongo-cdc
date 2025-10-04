package partition

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/Trendyol/go-mongo-cdc/config"
	"github.com/Trendyol/go-mongo-cdc/internal/backoff"
	"github.com/Trendyol/go-mongo-cdc/logger"
	"github.com/Trendyol/go-mongo-cdc/mongo/connection"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type Manager interface {
	Initialize(ctx context.Context) error
	AcquirePartitions(ctx context.Context) ([]int, error)
	ReleasePartitions(ctx context.Context) error
	ResyncPartitions(ctx context.Context) error
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
	config        config.PartitionConfig

	mu                 sync.RWMutex
	assignedPartitions []int
	isRunning          bool

	stopCh chan struct{}
	wg     sync.WaitGroup

	lastKnownWorkerCount int

	onPartitionsChanged func(newPartitions []int)
}

func NewManager(workerID string, client connection.Client, cfg config.PartitionConfig) Manager {
	return &manager{
		workerID:           workerID,
		client:             client,
		config:             cfg,
		stopCh:             make(chan struct{}),
		assignedPartitions: make([]int, 0),
	}
}

func (m *manager) Initialize(ctx context.Context) error {
	db := m.client.Database(m.config.PartitionDatabase)
	m.workersCol = db.Collection(m.config.WorkersCollection)
	m.partitionsCol = db.Collection(m.config.PartitionsCollection)

	if err := m.createIndexes(ctx); err != nil {
		return fmt.Errorf("failed to create indexes: %w", err)
	}

	if err := m.registerWorkerWithRetry(ctx); err != nil {
		return fmt.Errorf("failed to register worker: %w", err)
	}

	m.isRunning = true

	if activeWorkers, err := m.getActiveWorkerCount(context.Background()); err == nil {
		m.lastKnownWorkerCount = activeWorkers
	}

	m.wg.Add(1)
	go m.heartbeatLoop()

	m.wg.Add(1)
	go m.runRebalanceMonitor()

	logger.Log.Info(fmt.Sprintf("Partition manager initialized - workerId: %s, totalPartitions: %d", m.workerID, m.config.TotalPartition))

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

func (m *manager) registerWorkerWithRetry(ctx context.Context) error {
	backoffStrategy := backoff.New(backoff.Config{
		BaseDelay:  500 * time.Millisecond,
		MaxDelay:   5 * time.Second,
		Factor:     2.0,
		MaxRetries: 3,
	})

	for {
		err := m.registerWorker(ctx)
		if err == nil {
			logger.Log.Info(fmt.Sprintf("Worker registered successfully - workerId: %s, attempt: %d", m.workerID, backoffStrategy.Attempts()))
			return nil
		}

		delay, keepTrying := backoffStrategy.NextDelay()
		if !keepTrying {
			return fmt.Errorf("failed to register worker after %d attempts: %w", backoffStrategy.Attempts(), err)
		}

		logger.Log.Warn(fmt.Sprintf("Worker registration failed, retrying - attempt: %d/%d, delay: %v, error: %v",
			backoffStrategy.Attempts(), backoffStrategy.Config.MaxRetries, delay, err))

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(delay):
		}
	}
}

func (m *manager) registerWorker(ctx context.Context) error {
	serverTime, err := m.getServerTime(ctx)
	if err != nil {
		logger.Log.Warn(fmt.Sprintf("Failed to get server time, falling back to local time: %v", err))
		serverTime = time.Now()
	}

	worker := WorkerInfo{
		ID:                 m.workerID,
		AssignedPartitions: []int{},
		LastHeartbeat:      serverTime,
	}

	filter := bson.M{"_id": m.workerID}
	update := bson.M{"$set": worker}
	opts := options.Update().SetUpsert(true)

	result, err := m.workersCol.UpdateOne(ctx, filter, update, opts)
	if err != nil {
		return err
	}

	logger.Log.Debug(fmt.Sprintf("Worker registered - workerId: %s, matched: %d, modified: %d, upserted: %v",
		m.workerID, result.MatchedCount(), result.ModifiedCount(), result.UpsertedID() != nil))

	return nil
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
				logger.Log.Error(fmt.Sprintf("Failed to send heartbeat: %v", err))
			}

			if err := m.updatePartitionHeartbeats(ctx); err != nil {
				logger.Log.Error(fmt.Sprintf("Failed to update partition heartbeats: %v", err))
			}

			cancel()
		}
	}
}

func (m *manager) sendHeartbeat(ctx context.Context) error {
	filter := bson.M{"_id": m.workerID}
	update := bson.M{
		"$currentDate": bson.M{
			"lastHeartbeat": true,
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
		"$currentDate": bson.M{
			"lastHeartbeat": true,
		},
	}

	_, err := m.partitionsCol.UpdateMany(ctx, filter, update)
	return err
}

func (m *manager) runRebalanceMonitor() {
	defer m.wg.Done()

	logger.Log.Info("Starting worker changes monitoring")

	checkInterval := m.config.RebalanceCheckInterval
	ticker := time.NewTicker(checkInterval)
	defer ticker.Stop()

	for {
		select {
		case <-m.stopCh:
			logger.Log.Info("Stopping worker changes monitoring")
			return
		case <-ticker.C:
			m.triggerRebalanceIfNeeded()
		}
	}
}

func (m *manager) triggerRebalanceIfNeeded() {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	currentWorkerCount, err := m.getActiveWorkerCount(ctx)
	if err != nil {
		logger.Log.Error(fmt.Sprintf("Failed to get current worker count: %v", err))
		return
	}

	needsRebalance := currentWorkerCount != m.lastKnownWorkerCount || m.needsRebalance(ctx)

	if needsRebalance {
		if currentWorkerCount != m.lastKnownWorkerCount {
			logger.Log.Info(fmt.Sprintf("Worker count change detected - previousCount: %d, currentCount: %d", m.lastKnownWorkerCount, currentWorkerCount))
		} else {
			logger.Log.Debug(fmt.Sprintf("Retrying partition acquisition - workerCount: %d", currentWorkerCount))
		}

		if err := m.ResyncPartitions(ctx); err != nil {
			logger.Log.Error(fmt.Sprintf("Failed to refresh partitions - workerCount: %d, error: %v", currentWorkerCount, err))
		} else {
			logger.Log.Info(fmt.Sprintf("Successfully rebalanced partitions - workerCount: %d", currentWorkerCount))

			m.lastKnownWorkerCount = currentWorkerCount
		}
	}
}

func (m *manager) needsRebalance(ctx context.Context) bool {
	m.mu.RLock()
	currentPartitions := make([]int, len(m.assignedPartitions))
	copy(currentPartitions, m.assignedPartitions)
	m.mu.RUnlock()

	activeWorkers, err := m.getActiveWorkerCount(ctx)
	if err != nil {
		logger.Log.Warn(fmt.Sprintf("Failed to get active worker count for assignment check: %v", err))
		return false
	}

	workerIndex, err := m.determineWorkerIndex(ctx)
	if err != nil {
		logger.Log.Warn(fmt.Sprintf("Failed to get worker index for assignment check: %v", err))
		return false
	}

	expectedPartitions := m.calculateExpectedPartitionsForWorker(workerIndex, activeWorkers)

	if len(currentPartitions) != len(expectedPartitions) {
		logger.Log.Debug(fmt.Sprintf("Partition assignment count mismatch - workerId: %s, current: %d %v, expected: %d %v, workerIndex: %d, activeWorkers: %d",
			m.workerID, len(currentPartitions), currentPartitions, len(expectedPartitions), expectedPartitions, workerIndex, activeWorkers))
		return true
	}

	expectedPartitionsSet := make(map[int]struct{}, len(expectedPartitions))
	for _, p := range expectedPartitions {
		expectedPartitionsSet[p] = struct{}{}
	}

	for _, p := range currentPartitions {
		if _, ok := expectedPartitionsSet[p]; !ok {
			logger.Log.Debug(fmt.Sprintf("Partition assignment content mismatch - workerId: %s, has unexpected partition %d. Current: %v, Expected: %v, workerIndex: %d, activeWorkers: %d",
				m.workerID, p, currentPartitions, expectedPartitions, workerIndex, activeWorkers))
			return true
		}
	}

	return false
}

func (m *manager) AcquirePartitions(ctx context.Context) ([]int, error) {
	logger.Log.Debug(fmt.Sprintf("Starting partition acquisition - workerId: %s", m.workerID))

	if err := m.cleanupDeadWorkers(ctx); err != nil {
		logger.Log.Error(fmt.Sprintf("Failed to cleanup dead workers: %v", err))
	}

	activeWorkers, err := m.getActiveWorkerCount(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get active worker count: %w", err)
	}

	workerIndex, err := m.determineWorkerIndex(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get worker index: %w", err)
	}

	logger.Log.Debug(fmt.Sprintf("Partition acquisition context - workerId: %s, activeWorkers: %d, workerIndex: %d", m.workerID, activeWorkers, workerIndex))

	expectedPartitions := m.calculateExpectedPartitionsForWorker(workerIndex, activeWorkers)

	acquiredPartitions := make([]int, 0, len(expectedPartitions))
	failedPartitions := make([]int, 0, len(expectedPartitions))

	logger.Log.Info(fmt.Sprintf("Attempting to acquire partitions - expected: %v, activeWorkers: %d, workerIndex: %d",
		expectedPartitions, activeWorkers, workerIndex))

	for _, partitionID := range expectedPartitions {
		if err := m.tryAcquireOrTakeoverPartitionWithRetry(ctx, partitionID); err != nil {
			logger.Log.Info(fmt.Sprintf("Failed to acquire partition %d after retries: %v", partitionID, err))
			failedPartitions = append(failedPartitions, partitionID)
			continue
		}
		acquiredPartitions = append(acquiredPartitions, partitionID)
		logger.Log.Debug(fmt.Sprintf("Successfully acquired partition %d - workerId: %s", partitionID, m.workerID))
	}

	if len(failedPartitions) > 0 {
		logger.Log.Info(fmt.Sprintf("Failed to acquire some partitions - failed: %v, acquired: %v",
			failedPartitions, acquiredPartitions))
	}

	m.mu.Lock()
	m.assignedPartitions = acquiredPartitions
	m.mu.Unlock()

	if err := m.updateWorkerPartitions(ctx, acquiredPartitions); err != nil {
		logger.Log.Error(fmt.Sprintf("Failed to update worker partitions: %v", err))
	}

	logger.Log.Info(fmt.Sprintf("Acquired partitions - count: %d, partitions: %v, workerIndex: %d, activeWorkers: %d", len(acquiredPartitions), acquiredPartitions, workerIndex, activeWorkers))

	return acquiredPartitions, nil
}

func (m *manager) calculateExpectedPartitionsForWorker(workerIndex int, activeWorkers int) []int {
	if activeWorkers == 0 {
		activeWorkers = 1
	}

	totalPartitions := m.config.TotalPartition
	partitionsPerWorker := totalPartitions / activeWorkers
	extraPartitions := totalPartitions % activeWorkers

	startPartition := workerIndex * partitionsPerWorker
	endPartition := startPartition + partitionsPerWorker

	if workerIndex < extraPartitions {
		startPartition += workerIndex
		endPartition += workerIndex + 1
	} else {
		startPartition += extraPartitions
		endPartition += extraPartitions
	}

	var expectedPartitions []int
	for i := startPartition; i < endPartition && i < totalPartitions; i++ {
		expectedPartitions = append(expectedPartitions, i)
	}
	return expectedPartitions
}

func (m *manager) cleanupDeadWorkers(ctx context.Context) error {
	serverTime, err := m.getServerTime(ctx)
	if err != nil {
		logger.Log.Warn(fmt.Sprintf("Failed to get server time for cleanup, falling back to local time: %v", err))
		serverTime = time.Now()
	}

	cutoff := serverTime.Add(-m.config.WorkerTimeout)

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
		logger.Log.Info(fmt.Sprintf("Found dead workers, cleaning up - workerIds: %v, cutoff: %v", deadWorkerIDs, cutoff))

		partitionFilter := bson.M{"workerId": bson.M{"$in": deadWorkerIDs}}
		partitionResult, err := m.partitionsCol.DeleteMany(ctx, partitionFilter)
		if err != nil {
			logger.Log.Error(fmt.Sprintf("Failed to release dead worker partitions: %v", err))
		} else {
			logger.Log.Debug(fmt.Sprintf("Released %d partitions from dead workers", partitionResult.DeletedCount()))
		}

		workerFilter := bson.M{"_id": bson.M{"$in": deadWorkerIDs}}
		workerResult, err := m.workersCol.DeleteMany(ctx, workerFilter)
		if err != nil {
			logger.Log.Error(fmt.Sprintf("Failed to delete dead workers: %v", err))
		} else {
			logger.Log.Debug(fmt.Sprintf("Deleted %d dead workers", workerResult.DeletedCount()))
		}

		logger.Log.Info(fmt.Sprintf("Cleaned up dead workers - workerIds: %v", deadWorkerIDs))
	} else {
		logger.Log.Debug(fmt.Sprintf("No dead workers found - cutoff: %v", cutoff))
	}

	return nil
}

func (m *manager) getActiveWorkerCount(ctx context.Context) (int, error) {
	serverTime, err := m.getServerTime(ctx)
	if err != nil {
		logger.Log.Warn(fmt.Sprintf("Failed to get server time for active worker count, falling back to local time: %v", err))
		serverTime = time.Now()
	}

	cutoff := serverTime.Add(-m.config.WorkerTimeout)
	filter := bson.M{"lastHeartbeat": bson.M{"$gte": cutoff}}

	count, err := m.workersCol.CountDocuments(ctx, filter)
	if err != nil {
		return 0, err
	}

	return int(count), nil
}

func (m *manager) determineWorkerIndex(ctx context.Context) (int, error) {
	serverTime, err := m.getServerTime(ctx)
	if err != nil {
		logger.Log.Warn(fmt.Sprintf("Failed to get server time for worker index, falling back to local time: %v", err))
		serverTime = time.Now()
	}

	cutoff := serverTime.Add(-m.config.WorkerTimeout)
	filter := bson.M{"lastHeartbeat": bson.M{"$gte": cutoff}}
	opts := options.Find().SetSort(bson.D{{Key: "_id", Value: 1}})

	backoffStrategy := backoff.New(backoff.Config{
		BaseDelay:  1 * time.Second,
		MaxDelay:   5 * time.Second,
		Factor:     1.5,
		MaxRetries: 3,
	})

	for {
		cursor, err := m.workersCol.Find(ctx, filter, opts)
		if err != nil {
			return -1, err
		}

		var allWorkers []string
		index := 0
		found := false

		for cursor.Next(ctx) {
			var worker WorkerInfo
			if err := cursor.Decode(&worker); err != nil {
				cursor.Close(ctx)
				return -1, err
			}

			allWorkers = append(allWorkers, worker.ID)
			if worker.ID == m.workerID {
				found = true
				break
			}
			index++
		}
		cursor.Close(ctx)

		if found {
			return index, nil
		}

		logger.Log.Warn(fmt.Sprintf("Worker not found in active workers list, retrying - myWorkerId: %s, allActiveWorkers: %v, attempt: %d/%d",
			m.workerID, allWorkers, backoffStrategy.Attempts()+1, backoffStrategy.Config.MaxRetries))

		delay, keepTrying := backoffStrategy.NextDelay()
		if !keepTrying {
			logger.Log.Error(fmt.Sprintf("Worker not found in active workers list after %d attempts - myWorkerId: %s", backoffStrategy.Attempts(), m.workerID))
			return -1, fmt.Errorf("worker not found in active workers list after %d attempts", backoffStrategy.Attempts())
		}

		if heartbeatErr := m.sendHeartbeat(ctx); heartbeatErr != nil {
			logger.Log.Error(fmt.Sprintf("Failed to send heartbeat during worker index retry: %v", heartbeatErr))
		}

		select {
		case <-ctx.Done():
			return -1, ctx.Err()
		case <-time.After(delay):
		}
	}
}

func (m *manager) tryAcquireOrTakeoverPartitionWithRetry(ctx context.Context, partitionID int) error {
	backoffStrategy := backoff.New(backoff.Config{
		BaseDelay:  250 * time.Millisecond,
		MaxDelay:   5 * time.Second,
		Factor:     2.0,
		MaxRetries: 5,
	})

	for {
		err := m.tryAcquireOrTakeoverPartition(ctx, partitionID)
		if err == nil {
			return nil
		}

		delay, ok := backoffStrategy.NextDelay()
		if !ok {
			return fmt.Errorf("failed to acquire partition %d after %d attempts: %w", partitionID, backoffStrategy.Attempts(), err)
		}

		logger.Log.Debug(fmt.Sprintf("Partition acquisition failed, retrying - partition: %d, attempt: %d/%d, delay: %v, error: %v",
			partitionID, backoffStrategy.Attempts(), backoffStrategy.Config.MaxRetries, delay, err))

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(delay):
		}
	}
}

func (m *manager) tryAcquireOrTakeoverPartition(ctx context.Context, partitionID int) error {
	serverTime, err := m.getServerTime(ctx)
	if err != nil {
		logger.Log.Warn(fmt.Sprintf("Failed to get server time for partition acquisition, falling back to local time: %v", err))
		serverTime = time.Now()
	}

	cutoff := serverTime.Add(-m.config.WorkerTimeout)

	filter := bson.M{
		"_id": partitionID,
		"$or": []bson.M{
			{"lastHeartbeat": bson.M{"$lt": cutoff}},
			{"workerId": m.workerID},
		},
	}

	update := bson.M{
		"$set": bson.M{
			"partitionId": partitionID,
			"workerId":    m.workerID,
		},
		"$currentDate": bson.M{
			"assignedAt":    true,
			"lastHeartbeat": true,
		},
	}
	opts := options.FindOneAndUpdate().SetUpsert(true).SetReturnDocument(options.After)

	var result PartitionAssignment
	err = m.partitionsCol.FindOneAndUpdate(ctx, filter, update, opts).Decode(&result)

	if err != nil {
		if err == mongo.ErrNoDocuments {
			logger.Log.Debug(fmt.Sprintf("Partition %d is owned by another active worker", partitionID))
			return fmt.Errorf("partition %d is already owned by another active worker", partitionID)
		}
		return fmt.Errorf("failed to acquire partition %d: %w", partitionID, err)
	}

	if result.WorkerID == m.workerID {
		logger.Log.Debug(fmt.Sprintf("Successfully acquired partition %d", partitionID))
		return nil
	}

	return fmt.Errorf("partition %d was acquired by another worker during operation", partitionID)
}

func (m *manager) releasePartition(ctx context.Context, partitionID int) error {
	filter := bson.M{
		"_id":      partitionID,
		"workerId": m.workerID,
	}

	result, err := m.partitionsCol.DeleteOne(ctx, filter)
	if err != nil {
		return fmt.Errorf("failed to release partition %d: %w", partitionID, err)
	}

	if result.DeletedCount() == 0 {
		logger.Log.Warn(fmt.Sprintf("Partition was not owned by this worker during release - partition %d, workerId: %s", partitionID, m.workerID))
	} else {
		logger.Log.Debug(fmt.Sprintf("Successfully released partition %d - workerId: %s", partitionID, m.workerID))
	}

	return nil
}

func (m *manager) updateWorkerPartitions(ctx context.Context, partitions []int) error {
	filter := bson.M{"_id": m.workerID}
	update := bson.M{
		"$set": bson.M{
			"assignedPartitions": partitions,
		},
		"$currentDate": bson.M{
			"lastHeartbeat": true,
		},
	}

	result, err := m.workersCol.UpdateOne(ctx, filter, update)
	if err != nil {
		logger.Log.Error(fmt.Sprintf("Failed to update worker partitions - workerId: %s, partitions: %v, error: %v", m.workerID, partitions, err))
		return err
	}

	logger.Log.Debug(fmt.Sprintf("Updated worker partitions - workerId: %s, partitions: %v, matched: %d, modified: %d", m.workerID, partitions, result.MatchedCount(), result.ModifiedCount()))
	return nil
}

func (m *manager) ResyncPartitions(ctx context.Context) error {
	m.mu.RLock()
	currentPartitions := make([]int, len(m.assignedPartitions))
	copy(currentPartitions, m.assignedPartitions)
	m.mu.RUnlock()

	newPartitions, err := m.AcquirePartitions(ctx)
	if err != nil {
		return err
	}

	var partitionsToRelease []int
	for _, current := range currentPartitions {
		shouldKeep := false
		for _, newPartition := range newPartitions {
			if current == newPartition {
				shouldKeep = true
				break
			}
		}
		if !shouldKeep {
			partitionsToRelease = append(partitionsToRelease, current)
		}
	}

	if len(partitionsToRelease) > 0 {
		logger.Log.Info(fmt.Sprintf("Releasing partitions from MongoDB - partitionsToRelease: %v", partitionsToRelease))

		for _, partitionID := range partitionsToRelease {
			if err := m.releasePartition(ctx, partitionID); err != nil {
				logger.Log.Error(fmt.Sprintf("Failed to release partition from MongoDB - partition %d, error: %v", partitionID, err))
			}
		}
	}

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

	logger.Log.Info(fmt.Sprintf("Releasing all partitions - count: %d, partitions: %v", len(partitions), partitions))

	m.mu.Lock()
	m.assignedPartitions = []int{}
	m.mu.Unlock()

	filter := bson.M{"workerId": m.workerID}
	result, err := m.partitionsCol.DeleteMany(ctx, filter)
	if err != nil {
		return err
	}

	logger.Log.Info(fmt.Sprintf("Released %d partitions from MongoDB", result.DeletedCount()))

	return m.updateWorkerPartitions(ctx, []int{})
}

func (m *manager) getServerTime(ctx context.Context) (time.Time, error) {
	db := m.client.Database(m.config.PartitionDatabase)
	result := db.RunCommand(ctx, bson.D{{Key: "serverStatus", Value: 1}})

	var serverStatus struct {
		LocalTime time.Time `bson:"localTime"`
	}

	if err := result.Decode(&serverStatus); err != nil {
		return m.getServerTimeWithCurrentDate(ctx)
	}

	return serverStatus.LocalTime, nil
}

func (m *manager) getServerTimeWithCurrentDate(ctx context.Context) (time.Time, error) {
	update := bson.M{
		"$set": bson.M{
			"purpose": "server_time_check",
		},
		"$currentDate": bson.M{
			"serverTime": true,
		},
	}

	opts := options.FindOneAndUpdate().
		SetUpsert(true).
		SetReturnDocument(options.After)

	var result struct {
		ServerTime time.Time `bson:"serverTime"`
	}

	err := m.workersCol.FindOneAndUpdate(ctx,
		bson.M{"_id": "temp_time_check"},
		update,
		opts).Decode(&result)

	if err != nil {
		return time.Time{}, fmt.Errorf("failed to get server time with fallback method: %w", err)
	}

	go func() {
		deleteCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		m.workersCol.DeleteOne(deleteCtx, bson.M{"_id": "temp_time_check"})
	}()

	return result.ServerTime, nil
}

func (m *manager) Stop(ctx context.Context) error {
	m.mu.Lock()
	if !m.isRunning {
		m.mu.Unlock()
		return nil
	}
	m.isRunning = false
	m.mu.Unlock()

	if err := m.ReleasePartitions(ctx); err != nil {
		logger.Log.Error(fmt.Sprintf("Failed to release partitions during stop: %v", err))
	}

	close(m.stopCh)

	done := make(chan struct{})
	go func() {
		m.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		logger.Log.Debug("Heartbeat and monitor loops stopped gracefully")
	case <-time.After(2 * time.Second):
		logger.Log.Warn("Timeout waiting for background loops to stop")
	}

	filter := bson.M{"_id": m.workerID}
	if _, err := m.workersCol.DeleteOne(ctx, filter); err != nil {
		logger.Log.Error(fmt.Sprintf("Failed to unregister worker: %v", err))
	}

	logger.Log.Info("Partition manager stopped")
	return nil
}
