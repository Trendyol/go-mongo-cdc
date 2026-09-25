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
	LastHeartbeat      time.Time `bson:"lastHeartbeat"`
	ID                 string    `bson:"_id"`
	AssignedPartitions []int     `bson:"assignedPartitions"`
}

type PartitionAssignment struct {
	AssignedAt    time.Time `bson:"assignedAt"`
	LastHeartbeat time.Time `bson:"lastHeartbeat"`
	WorkerID      string    `bson:"workerId"`
	PartitionID   int       `bson:"_id"`
}

type manager struct {
	workersCol           connection.Collection
	partitionsCol        connection.Collection
	metadataCol          connection.Collection
	client               connection.Client
	stopCh               chan struct{}
	onPartitionsChanged  func(newPartitions []int)
	workerID             string
	database             string
	assignedPartitions   []int
	config               config.PartitionConfig
	wg                   sync.WaitGroup
	lastKnownWorkerCount int
	mu                   sync.RWMutex
	isRunning            bool
}

func NewManager(workerID string, client connection.Client, database string, cfg config.PartitionConfig) Manager {
	return &manager{
		workerID:           workerID,
		client:             client,
		database:           database,
		config:             cfg,
		stopCh:             make(chan struct{}),
		assignedPartitions: make([]int, 0),
	}
}

func (m *manager) Initialize(ctx context.Context) error {
	db := m.client.Database(m.database)
	m.workersCol = db.Collection(m.getCollectionName(m.config.WorkersCollection))
	m.partitionsCol = db.Collection(m.getCollectionName(m.config.PartitionsCollection))
	m.metadataCol = db.Collection(m.getCollectionName("partition_metadata"))

	if err := m.validateAndStoreTotalPartition(ctx); err != nil {
		return fmt.Errorf("failed to validate totalPartition: %w", err)
	}

	if err := m.createIndexes(ctx); err != nil {
		return fmt.Errorf("failed to create indexes: %w", err)
	}

	if err := m.registerWorkerWithRetry(ctx); err != nil {
		return fmt.Errorf("failed to register worker: %w", err)
	}

	m.isRunning = true

	if activeWorkers, err := m.getActiveWorkerCount(context.Background()); err == nil {
		m.setLastKnownWorkerCount(activeWorkers)
	}

	m.wg.Add(1)
	go m.heartbeatLoop()

	m.wg.Add(1)
	go m.runRebalanceMonitor()

	logger.Log.Info(
		"Partition manager initialized - workerId: %s, consumerGroup: %s, totalPartitions: %d",
		m.workerID,
		m.config.ConsumerGroup,
		m.config.TotalPartition,
	)

	return nil
}

func (m *manager) getCollectionName(baseName string) string {
	return fmt.Sprintf("%s_%s", baseName, m.config.ConsumerGroup)
}

func (m *manager) validateAndStoreTotalPartition(ctx context.Context) error {
	type Metadata struct {
		ID             string `bson:"_id"`
		ConsumerGroup  string `bson:"consumerGroup"`
		TotalPartition int    `bson:"totalPartition"`
	}

	filter := bson.M{"_id": "totalPartition"}
	var existing Metadata
	err := m.metadataCol.FindOne(ctx, filter).Decode(&existing)

	if err == mongo.ErrNoDocuments {
		metadata := Metadata{
			ID:             "totalPartition",
			TotalPartition: m.config.TotalPartition,
			ConsumerGroup:  m.config.ConsumerGroup,
		}
		_, insertErr := m.metadataCol.InsertOne(ctx, metadata)
		if insertErr != nil {
			if mongo.IsDuplicateKeyError(insertErr) {
				var recheck Metadata
				recheckErr := m.metadataCol.FindOne(ctx, filter).Decode(&recheck)
				if recheckErr != nil {
					return fmt.Errorf("failed to recheck totalPartition after duplicate key: %w", recheckErr)
				}
				if recheck.TotalPartition != m.config.TotalPartition {
					return fmt.Errorf(
						"totalPartition cannot be changed after initial setup - stored: %d, config: %d",
						recheck.TotalPartition,
						m.config.TotalPartition,
					)
				}
				logger.Log.Debug("totalPartition validation passed (concurrent insert) - totalPartition: %d, consumerGroup: %s", m.config.TotalPartition, m.config.ConsumerGroup)
				return nil
			}
			return fmt.Errorf("failed to store initial totalPartition: %w", insertErr)
		}
		logger.Log.Debug("Stored initial totalPartition: %d for consumerGroup: %s", m.config.TotalPartition, m.config.ConsumerGroup)
		return nil
	}

	if err != nil {
		return fmt.Errorf("failed to check existing totalPartition: %w", err)
	}

	if existing.TotalPartition != m.config.TotalPartition {
		return fmt.Errorf(
			"totalPartition cannot be changed after initial setup - stored: %d, config: %d",
			existing.TotalPartition,
			m.config.TotalPartition,
		)
	}

	logger.Log.Debug("totalPartition validation passed: %d for consumerGroup: %s", m.config.TotalPartition, m.config.ConsumerGroup)
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
	b := backoff.New(backoff.DefaultConfig)

	for {
		err := m.registerWorker(ctx)
		if err == nil {
			logger.Log.Info("Worker registered successfully - workerId: %s, attempt: %d", m.workerID, b.Attempts())
			return nil
		}

		attempts := b.Attempts()
		if sleepErr := b.Sleep(ctx); sleepErr != nil {
			if sleepErr == backoff.ErrMaxRetriesExceeded {
				return fmt.Errorf("failed to register worker after %d attempts: %w", attempts, err)
			}
			return sleepErr
		}

		logger.Log.Warn("Worker registration failed, retrying - attempt: %d/%d, error: %v", attempts+1, b.Config.MaxRetries, err)
	}
}

func (m *manager) registerWorker(ctx context.Context) error {
	serverTime, err := m.getServerTime(ctx)
	if err != nil {
		logger.Log.Warn("Failed to get server time, falling back to local time: %v", err)
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

	logger.Log.Debug(
		"Worker registered - workerId: %s, matched: %d, modified: %d, upserted: %v",
		m.workerID,
		result.MatchedCount(),
		result.ModifiedCount(),
		result.UpsertedID() != nil,
	)

	return nil
}

func (m *manager) heartbeatLoop() {
	defer m.wg.Done()

	ticker := time.NewTicker(m.config.HeartbeatInterval)
	defer ticker.Stop()

	for {
		select {
		case <-m.stopCh:
			logger.Log.Debug("Heartbeat loop stopped")
			return
		case <-ticker.C:
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)

			if err := m.sendHeartbeat(ctx); err != nil {
				logger.Log.Error("Failed to send heartbeat: %v", err)
			}

			if err := m.updatePartitionHeartbeats(ctx); err != nil {
				logger.Log.Error("Failed to update partition heartbeats: %v", err)
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

	logger.Log.Debug("Starting worker changes monitoring")

	checkInterval := m.config.RebalanceCheckInterval
	ticker := time.NewTicker(checkInterval)
	defer ticker.Stop()

	for {
		select {
		case <-m.stopCh:
			logger.Log.Debug("Stopping worker changes monitoring")
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
		logger.Log.Error("Failed to get current worker count: %v", err)
		return
	}

	lastKnownWorkerCount := m.getLastKnownWorkerCount()
	needsRebalance := currentWorkerCount != lastKnownWorkerCount || m.needsRebalance(ctx)

	if needsRebalance {
		if currentWorkerCount != lastKnownWorkerCount {
			logger.Log.Debug("Worker count change detected - previousCount: %d, currentCount: %d", lastKnownWorkerCount, currentWorkerCount)
		} else {
			logger.Log.Debug("Retrying partition acquisition - workerCount: %d", currentWorkerCount)
		}

		if err := m.ResyncPartitions(ctx); err != nil {
			logger.Log.Error("Failed to refresh partitions - workerCount: %d, error: %v", currentWorkerCount, err)
		} else {
			logger.Log.Info("Successfully rebalanced partitions - workerCount: %d", currentWorkerCount)

			m.setLastKnownWorkerCount(currentWorkerCount)
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
		logger.Log.Warn("Failed to get active worker count for assignment check: %v", err)
		return false
	}

	workerIndex, err := m.determineWorkerIndex(ctx)
	if err != nil {
		logger.Log.Warn("Failed to get worker index for assignment check: %v", err)
		return false
	}

	expectedPartitions := m.calculateExpectedPartitionsForWorker(workerIndex, activeWorkers)

	if len(currentPartitions) != len(expectedPartitions) {
		logger.Log.Debug(
			"Partition assignment count mismatch - workerId: %s, current: %d %v, expected: %d %v, workerIndex: %d, activeWorkers: %d",
			m.workerID,
			len(currentPartitions),
			currentPartitions,
			len(expectedPartitions),
			expectedPartitions,
			workerIndex,
			activeWorkers,
		)
		return true
	}

	expectedPartitionsSet := make(map[int]struct{}, len(expectedPartitions))
	for _, p := range expectedPartitions {
		expectedPartitionsSet[p] = struct{}{}
	}

	for _, p := range currentPartitions {
		if _, ok := expectedPartitionsSet[p]; !ok {
			logger.Log.Debug(
				"Partition assignment content mismatch - workerId: %s, has unexpected partition %d.",
				m.workerID,
				p,
			)
			logger.Log.Debug(
				"Partition assignment content details - current: %v, expected: %v, workerIndex: %d, activeWorkers: %d",
				currentPartitions,
				expectedPartitions,
				workerIndex,
				activeWorkers,
			)
			return true
		}
	}

	return false
}

func (m *manager) AcquirePartitions(ctx context.Context) ([]int, error) {
	logger.Log.Debug("Starting partition acquisition - workerId: %s", m.workerID)

	if err := m.cleanupDeadWorkers(ctx); err != nil {
		logger.Log.Error("Failed to cleanup dead workers: %v", err)
	}

	activeWorkers, err := m.getActiveWorkerCount(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get active worker count: %w", err)
	}

	workerIndex, err := m.determineWorkerIndex(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get worker index: %w", err)
	}

	logger.Log.Debug(
		"Partition acquisition context - workerId: %s, activeWorkers: %d, workerIndex: %d",
		m.workerID,
		activeWorkers,
		workerIndex,
	)

	expectedPartitions := m.calculateExpectedPartitionsForWorker(workerIndex, activeWorkers)

	acquiredPartitions := make([]int, 0, len(expectedPartitions))
	failedPartitions := make([]int, 0, len(expectedPartitions))

	logger.Log.Debug(
		"Attempting to acquire partitions - expected: %v, activeWorkers: %d, workerIndex: %d",
		expectedPartitions,
		activeWorkers,
		workerIndex,
	)

	for _, partitionID := range expectedPartitions {
		if err := m.tryAcquireOrTakeoverPartitionWithRetry(ctx, partitionID); err != nil {
			logger.Log.Debug("Failed to acquire partition %d after retries: %v", partitionID, err)
			failedPartitions = append(failedPartitions, partitionID)
			continue
		}
		acquiredPartitions = append(acquiredPartitions, partitionID)
		logger.Log.Debug("Successfully acquired partition %d - workerId: %s", partitionID, m.workerID)
	}

	if len(failedPartitions) > 0 {
		logger.Log.Debug("Failed to acquire some partitions - failed: %v, acquired: %v", failedPartitions, acquiredPartitions)
	}

	m.mu.Lock()
	m.assignedPartitions = acquiredPartitions
	m.mu.Unlock()

	if err := m.updateWorkerPartitions(ctx, acquiredPartitions); err != nil {
		logger.Log.Error("Failed to update worker partitions: %v", err)
	}

	logger.Log.Debug(
		"Acquired partitions - count: %d, partitions: %v, workerIndex: %d, activeWorkers: %d",
		len(acquiredPartitions),
		acquiredPartitions,
		workerIndex,
		activeWorkers,
	)

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
		logger.Log.Warn("Failed to get server time for cleanup, falling back to local time: %v", err)
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
		logger.Log.Debug("Found dead workers, cleaning up - workerIds: %v, cutoff: %v", deadWorkerIDs, cutoff)

		partitionFilter := bson.M{"workerId": bson.M{"$in": deadWorkerIDs}}
		partitionResult, err := m.partitionsCol.DeleteMany(ctx, partitionFilter)
		if err != nil {
			logger.Log.Error("Failed to release dead worker partitions: %v", err)
		} else {
			logger.Log.Debug("Released %d partitions from dead workers", partitionResult.DeletedCount())
		}

		workerFilter := bson.M{"_id": bson.M{"$in": deadWorkerIDs}}
		workerResult, err := m.workersCol.DeleteMany(ctx, workerFilter)
		if err != nil {
			logger.Log.Error("Failed to delete dead workers: %v", err)
		} else {
			logger.Log.Debug("Deleted %d dead workers", workerResult.DeletedCount())
		}

		logger.Log.Debug("Cleaned up dead workers - workerIds: %v", deadWorkerIDs)
	} else {
		logger.Log.Debug("No dead workers found - cutoff: %v", cutoff)
	}

	if err := m.cleanupOrphanPartitions(ctx); err != nil {
		logger.Log.Error("Failed to cleanup orphan partitions: %v", err)
	}

	return nil
}

func (m *manager) cleanupOrphanPartitions(ctx context.Context) error {
	activeWorkersCursor, err := m.workersCol.Find(ctx, bson.M{})
	if err != nil {
		return err
	}
	defer activeWorkersCursor.Close(ctx)

	activeWorkerIDs := make(map[string]bool)
	for activeWorkersCursor.Next(ctx) {
		var worker WorkerInfo
		if err := activeWorkersCursor.Decode(&worker); err != nil {
			continue
		}
		activeWorkerIDs[worker.ID] = true
	}

	partitionsCursor, err := m.partitionsCol.Find(ctx, bson.M{})
	if err != nil {
		return err
	}
	defer partitionsCursor.Close(ctx)

	var orphanPartitionIDs []int
	for partitionsCursor.Next(ctx) {
		var partition PartitionAssignment
		if err := partitionsCursor.Decode(&partition); err != nil {
			continue
		}

		if !activeWorkerIDs[partition.WorkerID] {
			orphanPartitionIDs = append(orphanPartitionIDs, partition.PartitionID)
		}
	}

	if len(orphanPartitionIDs) > 0 {
		logger.Log.Debug("Found orphan partitions (assigned to non-existent workers), cleaning up - partitions: %v", orphanPartitionIDs)

		orphanFilter := bson.M{"_id": bson.M{"$in": orphanPartitionIDs}}
		result, err := m.partitionsCol.DeleteMany(ctx, orphanFilter)
		if err != nil {
			logger.Log.Error("Failed to delete orphan partitions: %v", err)
			return err
		}

		logger.Log.Debug("Cleaned up %d orphan partitions", result.DeletedCount())
	} else {
		logger.Log.Debug("No orphan partitions found")
	}

	return nil
}

func (m *manager) getActiveWorkerCount(ctx context.Context) (int, error) {
	serverTime, err := m.getServerTime(ctx)
	if err != nil {
		logger.Log.Warn("Failed to get server time for active worker count, falling back to local time: %v", err)
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
		logger.Log.Warn("Failed to get server time for worker index, falling back to local time: %v", err)
		serverTime = time.Now()
	}

	cutoff := serverTime.Add(-m.config.WorkerTimeout)
	filter := bson.M{"lastHeartbeat": bson.M{"$gte": cutoff}}
	opts := options.Find().SetSort(bson.D{{Key: "_id", Value: 1}})

	b := backoff.New(backoff.DefaultConfig)

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

		attempts := b.Attempts()
		logger.Log.Warn(
			"Worker not found in active workers list, retrying - myWorkerId: %s, allActiveWorkers: %v, attempt: %d/%d",
			m.workerID,
			allWorkers,
			attempts+1,
			b.Config.MaxRetries,
		)

		if heartbeatErr := m.sendHeartbeat(ctx); heartbeatErr != nil {
			logger.Log.Error("Failed to send heartbeat during worker index retry: %v", heartbeatErr)
		}

		if sleepErr := b.Sleep(ctx); sleepErr != nil {
			if sleepErr == backoff.ErrMaxRetriesExceeded {
				logger.Log.Error("Worker not found in active workers list after %d attempts - myWorkerId: %s", attempts, m.workerID)
				return -1, fmt.Errorf("worker not found in active workers list after %d attempts", attempts)
			}
			return -1, sleepErr
		}
	}
}

func (m *manager) tryAcquireOrTakeoverPartitionWithRetry(ctx context.Context, partitionID int) error {
	maxRetries := 5
	retryDelay := 2 * time.Second

	for attempt := 1; attempt <= maxRetries; attempt++ {
		err := m.tryAcquireOrTakeoverPartition(ctx, partitionID)
		if err == nil {
			return nil
		}

		if attempt == maxRetries {
			return fmt.Errorf("failed to acquire partition %d after %d attempts: %w", partitionID, maxRetries, err)
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(retryDelay):
		}
	}

	return fmt.Errorf("failed to acquire partition %d after %d attempts", partitionID, maxRetries)
}

func (m *manager) tryAcquireOrTakeoverPartition(ctx context.Context, partitionID int) error {
	serverTime, err := m.getServerTime(ctx)
	if err != nil {
		logger.Log.Warn("Failed to get server time for partition acquisition, falling back to local time: %v", err)
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
			logger.Log.Debug("Partition %d is owned by another active worker", partitionID)
			return fmt.Errorf("partition %d is already owned by another active worker", partitionID)
		}
		return fmt.Errorf("failed to acquire partition %d: %w", partitionID, err)
	}

	if result.WorkerID == m.workerID {
		logger.Log.Debug("Successfully acquired partition %d", partitionID)
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
		logger.Log.Warn("Partition was not owned by this worker during release - partition %d, workerId: %s", partitionID, m.workerID)
	} else {
		logger.Log.Debug("Successfully released partition %d - workerId: %s", partitionID, m.workerID)
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
		logger.Log.Error("Failed to update worker partitions - workerId: %s, partitions: %v, error: %v", m.workerID, partitions, err)
		return err
	}

	logger.Log.Debug(
		"Updated worker partitions - workerId: %s, partitions: %v, matched: %d, modified: %d",
		m.workerID,
		partitions,
		result.MatchedCount(),
		result.ModifiedCount(),
	)
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
		logger.Log.Debug("Releasing partitions from MongoDB - partitionsToRelease: %v", partitionsToRelease)

		for _, partitionID := range partitionsToRelease {
			if err := m.releasePartition(ctx, partitionID); err != nil {
				logger.Log.Error("Failed to release partition from MongoDB - partition %d, error: %v", partitionID, err)
			}
		}
	}

	if callback := m.getPartitionsChangedCallback(); callback != nil {
		callback(newPartitions)
	}

	return nil
}

func (m *manager) SetPartitionsChangedCallback(callback func(newPartitions []int)) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.onPartitionsChanged = callback
}

func (m *manager) getPartitionsChangedCallback() func(newPartitions []int) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.onPartitionsChanged
}

func (m *manager) setLastKnownWorkerCount(count int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.lastKnownWorkerCount = count
}

func (m *manager) getLastKnownWorkerCount() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.lastKnownWorkerCount
}

func (m *manager) ReleasePartitions(ctx context.Context) error {
	m.mu.RLock()
	partitions := make([]int, len(m.assignedPartitions))
	copy(partitions, m.assignedPartitions)
	m.mu.RUnlock()

	if len(partitions) == 0 {
		return nil
	}

	logger.Log.Debug("Releasing all partitions - count: %d, partitions: %v", len(partitions), partitions)

	m.mu.Lock()
	m.assignedPartitions = []int{}
	m.mu.Unlock()

	filter := bson.M{"workerId": m.workerID}
	result, err := m.partitionsCol.DeleteMany(ctx, filter)
	if err != nil {
		return err
	}

	logger.Log.Debug("Released %d partitions from MongoDB", result.DeletedCount())

	return m.updateWorkerPartitions(ctx, []int{})
}

func (m *manager) getServerTime(ctx context.Context) (time.Time, error) {
	db := m.client.Database(m.database)
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
		_, err := m.workersCol.DeleteOne(deleteCtx, bson.M{"_id": "temp_time_check"})
		if err != nil {
			logger.Log.Debug("Failed to delete temporary server time document: %v", err)
		}
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

	close(m.stopCh)

	done := make(chan struct{})
	go func() {
		m.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		logger.Log.Debug("Heartbeat and monitor loops stopped gracefully")
	case <-time.After(15 * time.Second):
		logger.Log.Warn("Timeout waiting for background loops to stop")
	}

	maxRetries := 3
	retryDelay := 2 * time.Second
	var releaseErr error

	for attempt := 1; attempt <= maxRetries; attempt++ {
		releaseErr = m.ReleasePartitions(ctx)
		if releaseErr == nil {
			logger.Log.Debug("Successfully released all partitions on attempt %d", attempt)
			break
		}

		if attempt < maxRetries {
			logger.Log.Warn("Failed to release partitions (attempt %d/%d), retrying in %v: %v", attempt, maxRetries, retryDelay, releaseErr)
			select {
			case <-ctx.Done():
				logger.Log.Error("Context cancelled during partition release retry")
				break
			case <-time.After(retryDelay):
			}
		} else {
			logger.Log.Error("Failed to release partitions after %d attempts: %v", maxRetries, releaseErr)
		}
	}

	filter := bson.M{"_id": m.workerID}
	if _, err := m.workersCol.DeleteOne(ctx, filter); err != nil {
		logger.Log.Error("Failed to unregister worker: %v", err)
	}

	logger.Log.Info("Partition manager stopped")
	return nil
}
