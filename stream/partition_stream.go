package stream

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math"
	"strings"
	"sync"
	"time"

	"github.com/Trendyol/go-mongo-cdc/checkpoint"
	"github.com/Trendyol/go-mongo-cdc/config"
	"github.com/Trendyol/go-mongo-cdc/internal/backoff"
	"github.com/Trendyol/go-mongo-cdc/logger"
	"github.com/Trendyol/go-mongo-cdc/metric"
	"github.com/Trendyol/go-mongo-cdc/mongo/connection"
	"github.com/Trendyol/go-mongo-cdc/mongo/message"
	"github.com/Trendyol/go-mongo-cdc/partition"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

var ErrOplogHistoryLost = errors.New("oplog history lost, re-snapshot required")

type PartitionStream interface {
	Start(ctx context.Context) error
	Stop(ctx context.Context) error
}

type ListenerFunc func(ctx *ListenerContext) error

type ListenerContext struct {
	Context     context.Context
	Message     message.Message
	PartitionID int
	Ack         func() error
}

type partitionStream struct {
	client     connection.Client
	cfg        config.Config
	metric     metric.Metric
	listener   ListenerFunc
	collection connection.Collection
	database   connection.Database
	workerID   string

	partitionManager  partition.Manager
	checkpointManager checkpoint.Manager

	activeStreams map[int]*streamWorker
	streamsMutex  sync.RWMutex

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

type streamWorker struct {
	partitionID     int
	stream          connection.ChangeStream
	ctx             context.Context
	cancel          context.CancelFunc
	lastAckedToken  []byte
	lastClusterTime *primitive.Timestamp
	tokenMutex      sync.RWMutex
	lastEventTime   time.Time
	ackedEventCount int
	inFlightEvents  sync.WaitGroup
}

type streamStartInfo struct {
	resumeToken          []byte
	startAtOperationTime *primitive.Timestamp
	shouldBootstrap      bool
}

func NewPartitionStream(
	client connection.Client,
	cfg config.Config,
	metric metric.Metric,
	listener ListenerFunc,
	workerID string,
) PartitionStream {
	database := client.Database(cfg.MongoDB.Connection.Database)
	collection := database.Collection(cfg.MongoDB.Connection.Collection)

	partitionManager := partition.NewManager(workerID, client, cfg.MongoDB.Connection.Database, cfg.Partition)
	checkpointManager := checkpoint.NewManager(client, cfg.MongoDB.Connection.Database, cfg.MongoDB.Connection.Collection)

	return &partitionStream{
		client:            client,
		cfg:               cfg,
		metric:            metric,
		listener:          listener,
		collection:        collection,
		database:          database,
		workerID:          workerID,
		partitionManager:  partitionManager,
		checkpointManager: checkpointManager,
		activeStreams:     make(map[int]*streamWorker),
	}
}

func (ps *partitionStream) Start(ctx context.Context) error {
	ps.ctx, ps.cancel = context.WithCancel(ctx)

	if err := ps.ensureReplicaSetOrSharded(ps.ctx); err != nil {
		return err
	}

	if err := ps.partitionManager.Initialize(ps.ctx); err != nil {
		return fmt.Errorf("failed to initialize partition manager: %w", err)
	}

	select {
	case <-ps.ctx.Done():
		logger.Log.Debug("Event processing cancelled during initial delay")
		return ps.ctx.Err()
	case <-time.After(30 * time.Second): //TODO: sistemin reliable olması icin 30 saniye bekleme suresi iyi daha az olmaması gerekir fakat bazı kullanıcılar 30 dan yuksek vermek isteyebilir bu sebeple configurable yapılabilir
		logger.Log.Debug("Initial delay completed before acquiring partitions")
	}

	ps.partitionManager.SetPartitionsChangedCallback(ps.reconcilePartitionAssignments)

	if err := ps.acquireAndStartInitialPartitions(); err != nil {
		logger.Log.Error(fmt.Sprintf("Failed to acquire initial partitions: %v", err))
		return err
	}

	return nil
}

func (ps *partitionStream) ensureReplicaSetOrSharded(ctx context.Context) error {
	result := ps.database.RunCommand(ctx, bson.D{{Key: "isMaster", Value: 1}})

	var isMaster bson.M
	if err := result.Decode(&isMaster); err != nil {
		return err
	}

	if _, ok := isMaster["setName"]; ok {
		logger.Log.Info("Connected to MongoDB replica set")
		return nil
	}

	if msg, ok := isMaster["msg"]; ok && msg == "isdbgrid" {
		logger.Log.Debug("Connected to MongoDB sharded cluster")
		return nil
	}

	return errors.New(
		"MongoDB is not running as a replica set or sharded cluster. " +
			"Change streams require replica set or sharded cluster",
	)
}

func (ps *partitionStream) acquireAndStartInitialPartitions() error {
	partitions, err := ps.partitionManager.AcquirePartitions(ps.ctx)
	if err != nil {
		return err
	}

	ps.reconcilePartitionAssignments(partitions)
	return nil
}

func (ps *partitionStream) reconcilePartitionAssignments(newPartitions []int) {
	ps.streamsMutex.Lock()
	defer ps.streamsMutex.Unlock()

	// Track if any partition changes happened (rebalance)
	rebalanceHappened := false

	for partitionID, worker := range ps.activeStreams {
		found := false
		for _, p := range newPartitions {
			if p == partitionID {
				found = true
				break
			}
		}

		if !found {
			logger.Log.Debug(fmt.Sprintf("Stopping stream for partition %d", partitionID))
			worker.cancel()
			delete(ps.activeStreams, partitionID)
			ps.metric.IncPartitionReleaseTotal()
			rebalanceHappened = true
		}
	}

	for _, partitionID := range newPartitions {
		if _, exists := ps.activeStreams[partitionID]; !exists {
			logger.Log.Debug(fmt.Sprintf("Starting stream for partition %d", partitionID))

			worker := &streamWorker{
				partitionID:   partitionID,
				lastEventTime: time.Now(),
			}
			worker.ctx, worker.cancel = context.WithCancel(ps.ctx)

			ps.activeStreams[partitionID] = worker
			ps.metric.IncPartitionAcquireTotal()
			rebalanceHappened = true

			ps.wg.Add(1)
			go ps.managePartitionWorkerLifecycle(worker)
		}
	}

	// Track rebalance event
	if rebalanceHappened {
		ps.metric.IncPartitionRebalanceTotal()
	}

	ps.metric.SetActivePartitionCount(len(ps.activeStreams))
	logger.Log.Debug(fmt.Sprintf("Active partitions updated - count: %d, partitions: %v", len(ps.activeStreams), newPartitions))
}

func (ps *partitionStream) managePartitionWorkerLifecycle(worker *streamWorker) {
	defer ps.wg.Done()

	backoffStrategy := backoff.New(backoff.Config{
		BaseDelay:  1 * time.Second,
		MaxDelay:   30 * time.Second,
		Factor:     2.0,
		MaxRetries: 3,
	})

	for {
		select {
		case <-worker.ctx.Done():
			return
		default:
			err := ps.startOrResumePartitionStream(worker)
			if err == nil {
				logger.Log.Debug(fmt.Sprintf("Partition stream completed normally - partitionId: %d", worker.partitionID))
				return
			}

			if errors.Is(err, context.Canceled) {
				logger.Log.Info(fmt.Sprintf("Partition stream cancelled - partitionId: %d", worker.partitionID))
				return
			}

			if errors.Is(err, ErrOplogHistoryLost) {
				logger.Log.Warn(fmt.Sprintf("Oplog history lost detected, triggering automatic re-snapshot - partitionId: %d", worker.partitionID))
				ps.metric.IncResumeTokenExpiredTotal()

				if clearErr := ps.checkpointManager.ClearResumeToken(worker.ctx, worker.partitionID); clearErr != nil {
					logger.Log.Error(fmt.Sprintf("Failed to clear resume token during oplog recovery - partitionId: %d, error: %v", worker.partitionID, clearErr))
				}

				if clearErr := ps.checkpointManager.ClearBootstrapProgress(worker.ctx, worker.partitionID); clearErr != nil {
					logger.Log.Error(fmt.Sprintf("Failed to clear bootstrap progress during oplog recovery - partitionId: %d, error: %v", worker.partitionID, clearErr))
				}

				logger.Log.Info(fmt.Sprintf("Checkpoint cleared, next iteration will trigger full bootstrap - partitionId: %d", worker.partitionID))

				select {
				case <-time.After(2 * time.Second):
				case <-worker.ctx.Done():
					return
				}
				continue
			}

			logger.Log.Error(fmt.Sprintf("Partition stream error, retrying - partitionId: %d, error: %v", worker.partitionID, err))
			ps.metric.IncChangeStreamErrorTotal()
			ps.metric.IncChangeStreamRestartTotal()

			delay, _ := backoffStrategy.NextDelay()

			logger.Log.Debug(fmt.Sprintf("Backing off for %v before retry - partitionId: %d", delay, worker.partitionID))

			select {
			case <-time.After(delay):
			case <-worker.ctx.Done():
				return
			}
		}
	}
}

func (ps *partitionStream) startOrResumePartitionStream(worker *streamWorker) error {
	if !ps.verifyPartitionOwnership(worker.ctx, worker.partitionID) {
		logger.Log.Warn(fmt.Sprintf("Partition ownership verification failed at start - partitionId: %d", worker.partitionID))
		return fmt.Errorf("partition %d not owned by this worker", worker.partitionID)
	}

	startInfo, err := ps.determineStreamStartState(worker.partitionID)
	if err != nil {
		return fmt.Errorf("failed to prepare stream start: %w", err)
	}

	if startInfo.shouldBootstrap {
		logger.Log.Info(fmt.Sprintf("Starting bootstrap flow for partition %d", worker.partitionID))
		ps.metric.SetBootstrapStatus(true)
		err := ps.executeFullBootstrapFlow(worker)
		ps.metric.SetBootstrapStatus(false)
		return err
	}

	logger.Log.Info(fmt.Sprintf("Starting change stream for partition %d", worker.partitionID))
	return ps.startAndManageChangeStream(worker, startInfo.resumeToken, startInfo.startAtOperationTime)
}

func (ps *partitionStream) determineStreamStartState(partitionID int) (*streamStartInfo, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	resumeToken, clusterTime, err := ps.checkpointManager.GetResumeToken(ctx, partitionID)
	if err != nil {
		logger.Log.Error(fmt.Sprintf("Failed to get resume token - partitionId: %d, error: %v", partitionID, err))
		return nil, err
	}

	bootstrapLastID, err := ps.checkpointManager.GetBootstrapProgress(ctx, partitionID)
	if err != nil {
		logger.Log.Error(fmt.Sprintf("Failed to get bootstrap progress - partitionId: %d, error: %v", partitionID, err))
		return nil, err
	}

	shouldBootstrap := bootstrapLastID != nil || (resumeToken == nil && clusterTime == nil)

	if bootstrapLastID != nil {
		logger.Log.Info(fmt.Sprintf("Found incomplete bootstrap, will continue - partitionId: %d, lastId: %v", partitionID, bootstrapLastID))
	}

	return &streamStartInfo{
		resumeToken:          resumeToken,
		startAtOperationTime: clusterTime,
		shouldBootstrap:      shouldBootstrap,
	}, nil
}

func (ps *partitionStream) executeFullBootstrapFlow(worker *streamWorker) error {
	if !ps.verifyPartitionOwnership(worker.ctx, worker.partitionID) {
		logger.Log.Warn(fmt.Sprintf("Partition ownership verification failed before bootstrap - partitionId: %d", worker.partitionID))
		return fmt.Errorf("partition %d not owned by this worker during bootstrap verification", worker.partitionID)
	}

	opTime, err := ps.fetchCurrentDbOperationTimeWithRetry(worker.ctx, 3)
	if err != nil {
		logger.Log.Warn(fmt.Sprintf("Could not get server operation time before bootstrap - partitionId: %d, error: %v", worker.partitionID, err))
	}

	if err := ps.runBootstrapWithRetries(worker); err != nil {
		return fmt.Errorf("bootstrap failed after retries for partition %d: %w", worker.partitionID, err)
	}

	if opTime != nil {
		if err := ps.checkpointManager.SaveBootstrapClusterTime(worker.ctx, worker.partitionID, *opTime); err != nil {
			logger.Log.Warn(fmt.Sprintf("Failed to save bootstrap cluster time - partitionId: %d, error: %v", worker.partitionID, err))
		}
	}

	logger.Log.Info(fmt.Sprintf("Bootstrap completed, transitioning to change stream for partition %d", worker.partitionID))

	return ps.startAndManageChangeStream(worker, nil, opTime)
}

func (ps *partitionStream) fetchCurrentDbOperationTime(ctx context.Context) (*primitive.Timestamp, error) {
	result := ps.database.RunCommand(ctx, bson.D{{Key: "isMaster", Value: 1}})
	var isMaster bson.M
	if err := result.Decode(&isMaster); err != nil {
		return nil, err
	}

	if opTime, ok := isMaster["operationTime"].(primitive.Timestamp); ok {
		return &opTime, nil
	}
	return nil, nil
}

func (ps *partitionStream) fetchCurrentDbOperationTimeWithRetry(ctx context.Context, maxRetries int) (*primitive.Timestamp, error) {
	backoffStrategy := backoff.New(backoff.Config{
		BaseDelay:  1 * time.Second,
		MaxDelay:   5 * time.Second,
		Factor:     2.0,
		MaxRetries: maxRetries,
	})

	var lastErr error
	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}

		opTime, err := ps.fetchCurrentDbOperationTime(ctx)
		if err == nil && opTime != nil {
			logger.Log.Debug(fmt.Sprintf("Successfully obtained operation time from database after %d attempts", backoffStrategy.Attempts()))
			return opTime, nil
		}

		lastErr = err
		if err != nil {
			logger.Log.Warn(fmt.Sprintf("Failed to get operation time, attempt %d/%d: %v", backoffStrategy.Attempts()+1, maxRetries, err))
		} else {
			logger.Log.Warn(fmt.Sprintf("Operation time is nil, attempt %d/%d", backoffStrategy.Attempts()+1, maxRetries))
		}

		delay, keepTrying := backoffStrategy.NextDelay()
		if !keepTrying {
			break
		}

		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(delay):
		}
	}

	logger.Log.Warn(fmt.Sprintf("Failed to get operation time from database after %d attempts, using current time as fallback. Last error: %v", backoffStrategy.Attempts(), lastErr))

	now := time.Now().Unix()
	var t uint32
	if now < 0 {
		t = 0
	} else if now > int64(math.MaxUint32) {
		t = math.MaxUint32
	} else {
		t = uint32(now)
	}

	fallbackOpTime := &primitive.Timestamp{T: t, I: 1}
	logger.Log.Info(fmt.Sprintf("Using fallback operation time: %v", fallbackOpTime))

	return fallbackOpTime, nil
}

func (ps *partitionStream) getBootstrapProgressWithRetry(partitionID int, maxRetries int) (interface{}, error) {
	backoffStrategy := backoff.New(backoff.Config{
		BaseDelay:  1 * time.Second,
		MaxDelay:   5 * time.Second,
		Factor:     2.0,
		MaxRetries: maxRetries,
	})

	var lastErr error
	for {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)

		bootstrapLastID, err := ps.checkpointManager.GetBootstrapProgress(ctx, partitionID)
		cancel()

		if err == nil {
			logger.Log.Debug(fmt.Sprintf("Successfully obtained bootstrap progress after %d attempts - partitionId: %d", backoffStrategy.Attempts(), partitionID))
			return bootstrapLastID, nil
		}

		lastErr = err
		logger.Log.Warn(fmt.Sprintf("Failed to get bootstrap progress, attempt %d/%d - partitionId: %d, error: %v", backoffStrategy.Attempts()+1, maxRetries, partitionID, err))

		delay, keepTrying := backoffStrategy.NextDelay()
		if !keepTrying {
			return nil, fmt.Errorf("failed to get bootstrap progress after %d attempts for partition %d, last error: %v", backoffStrategy.Attempts(), partitionID, lastErr)
		}

		time.Sleep(delay)
	}
}

func (ps *partitionStream) runBootstrapWithRetries(worker *streamWorker) error {
	backoffStrategy := backoff.New(backoff.Config{
		BaseDelay:  5 * time.Second,
		MaxDelay:   30 * time.Second,
		Factor:     2.0,
		MaxRetries: 3,
	})

	for {
		select {
		case <-worker.ctx.Done():
			return worker.ctx.Err()
		default:
		}

		logger.Log.Debug(fmt.Sprintf("Bootstrap attempt %d/%d for partition %d", backoffStrategy.Attempts()+1, backoffStrategy.Config.MaxRetries, worker.partitionID))

		err := ps.bootstrapPartition(worker)
		if err == nil {
			logger.Log.Info(fmt.Sprintf("Bootstrap successful for partition %d after %d attempts", worker.partitionID, backoffStrategy.Attempts()))
			return nil
		}

		if errors.Is(err, context.Canceled) {
			logger.Log.Info(fmt.Sprintf("Bootstrap cancelled for partition %d", worker.partitionID))
			return err
		}

		logger.Log.Warn(fmt.Sprintf("Bootstrap attempt %d/%d failed for partition %d: %v", backoffStrategy.Attempts(), backoffStrategy.Config.MaxRetries, worker.partitionID, err))

		delay, keepTrying := backoffStrategy.NextDelay()
		if !keepTrying {
			return fmt.Errorf("bootstrap failed after %d attempts for partition %d", backoffStrategy.Attempts(), worker.partitionID)
		}

		select {
		case <-worker.ctx.Done():
			return worker.ctx.Err()
		case <-time.After(delay):
		}
	}
}

func (ps *partitionStream) bootstrapPartition(worker *streamWorker) error {
	logger.Log.Debug(fmt.Sprintf("Starting bootstrap for partition %d", worker.partitionID))

	bootstrapLastID, filter, err := ps.loadBootstrapStateAndCreateFilter(worker.partitionID)
	if err != nil {
		return fmt.Errorf("failed to load bootstrap state for partition %d: %w", worker.partitionID, err)
	}

	cursor, err := ps.queryDocumentsForBootstrap(worker, filter, bootstrapLastID)
	if err != nil {
		return err
	}
	defer cursor.Close(worker.ctx)

	return ps.iterateAndProcessBootstrapCursor(worker, cursor)
}

func (ps *partitionStream) loadBootstrapStateAndCreateFilter(partitionID int) (interface{}, bson.D, error) {
	bootstrapLastID, err := ps.getBootstrapProgressWithRetry(partitionID, 3)
	if err != nil {
		logger.Log.Error(fmt.Sprintf("CRITICAL: Failed to get bootstrap progress after retries - partitionId: %d, error: %v", partitionID, err))
		return nil, nil, err
	}

	filter := ps.buildBootstrapPartitionFilter(partitionID)
	if bootstrapLastID != nil {
		comparisonFilter := ps.buildResumeAfterIdFilter("_id", bootstrapLastID)
		filter = append(filter, comparisonFilter...)
		logger.Log.Info(fmt.Sprintf("Resuming bootstrap from saved progress - partitionId: %d, lastId: %v (type: %T)", partitionID, bootstrapLastID, bootstrapLastID))
	} else {
		logger.Log.Info(fmt.Sprintf("Starting fresh bootstrap - partitionId: %d", partitionID))
	}

	return bootstrapLastID, filter, nil
}

func (ps *partitionStream) queryDocumentsForBootstrap(worker *streamWorker, filter bson.D, bootstrapLastID interface{}) (connection.Cursor, error) {
	useNumericStringSorting := ps.shouldUseNumericStringSorting(worker, bootstrapLastID)

	opts := options.Find().SetSort(bson.D{{Key: "_id", Value: 1}})

	if useNumericStringSorting {
		opts.SetCollation(&options.Collation{
			Locale:          "en",
			NumericOrdering: true,
		})
		logger.Log.Debug("Using collation-based numeric string sorting")
	}

	return ps.collection.Find(worker.ctx, filter, opts)
}

func (ps *partitionStream) shouldUseNumericStringSorting(worker *streamWorker, bootstrapLastID interface{}) bool {
	if bootstrapLastID != nil {
		if lastIDStr, ok := bootstrapLastID.(string); ok && ps.isNumericString(lastIDStr) {
			logger.Log.Info(fmt.Sprintf("Resuming with numeric string ID '%s' - using mathematical sorting", lastIDStr))
			return true
		}
		return false
	}

	isNumericStringCollection, err := ps.detectNumericStringCollection(worker.ctx)
	if err != nil {
		logger.Log.Warn(fmt.Sprintf("Failed to detect ID type, using default sorting - partitionId: %d, error: %v", worker.partitionID, err))
		return false
	}

	if isNumericStringCollection {
		logger.Log.Info("Detected numeric string IDs in collection - using mathematical sorting for fresh bootstrap")
		return true
	}

	return false
}

func (ps *partitionStream) detectNumericStringCollection(ctx context.Context) (bool, error) {
	var doc struct {
		ID interface{} `bson:"_id"`
	}

	err := ps.collection.FindOne(ctx, bson.D{}).Decode(&doc)
	if err != nil {
		if err == mongo.ErrNoDocuments {
			return false, nil
		}
		return false, fmt.Errorf("failed to sample document: %w", err)
	}

	idStr, ok := doc.ID.(string)
	if !ok {
		return false, nil
	}

	return ps.isNumericString(idStr), nil
}

func (ps *partitionStream) isNumericString(s string) bool {
	if len(s) == 0 {
		return false
	}
	for _, char := range s {
		if char < '0' || char > '9' {
			return false
		}
	}
	return true
}

func (ps *partitionStream) iterateAndProcessBootstrapCursor(worker *streamWorker, cursor connection.Cursor) error {
	processState := &bootstrapProcessState{
		processedCount:     0,
		lastProcessedID:    nil,
		lastCheckpointTime: time.Now(),
		bootstrapCompleted: false,
	}

	defer ps.saveFinalBootstrapProgressOnInterruption(worker, processState)

	for cursor.Next(worker.ctx) {
		if err := ps.dispatchBootstrapDocumentToListener(worker, cursor, processState); err != nil {
			return err
		}
	}

	if err := cursor.Err(); err != nil {
		return err
	}

	return ps.finalizeBootstrapAndClearCheckpoint(worker, processState)
}

type bootstrapProcessState struct {
	processedCount     int
	lastProcessedID    interface{}
	lastCheckpointTime time.Time
	bootstrapCompleted bool
}

func (ps *partitionStream) saveFinalBootstrapProgressOnInterruption(worker *streamWorker, state *bootstrapProcessState) {
	if !state.bootstrapCompleted && state.lastProcessedID != nil {
		ctx, cancel := context.WithTimeout(context.Background(), ps.cfg.Checkpoint.TokenSaveTimeout)
		defer cancel()
		if err := ps.checkpointManager.SaveBootstrapProgress(ctx, worker.partitionID, state.lastProcessedID); err != nil {
			logger.Log.Error(fmt.Sprintf("Failed to save final bootstrap progress on interruption - partitionId: %d, lastId: %v, error: %v", worker.partitionID, state.lastProcessedID, err))
		} else {
			logger.Log.Info(fmt.Sprintf("Saved bootstrap progress on interruption - partitionId: %d, lastId: %v, processed: %d", worker.partitionID, state.lastProcessedID, state.processedCount))
		}
	}
}

func (ps *partitionStream) dispatchBootstrapDocumentToListener(worker *streamWorker, cursor connection.Cursor, state *bootstrapProcessState) error {
	var document bson.M
	if err := cursor.Decode(&document); err != nil {
		logger.Log.Error(fmt.Sprintf("Error decoding document: %v", err))
		return nil
	}

	syntheticEvent := ps.createInsertEventFromDocument(document)
	if err := ps.processEvent(worker, syntheticEvent, nil); err != nil {
		logger.Log.Error(fmt.Sprintf("Error processing synthetic event - documentId: %v, error: %v", document["_id"], err))
		return nil
	}

	state.processedCount++
	state.lastProcessedID = document["_id"]
	ps.metric.IncBootstrapDocumentTotal()

	if ps.shouldSaveBootstrapProgress(worker, state) {
		if !ps.verifyPartitionOwnership(worker.ctx, worker.partitionID) {
			logger.Log.Warn(fmt.Sprintf("Partition ownership lost before checkpoint save - partitionId: %d, processed: %d", worker.partitionID, state.processedCount))
			return fmt.Errorf("partition %d ownership lost before checkpoint", worker.partitionID)
		}

		return ps.saveBootstrapProgress(worker, document, state)
	}

	return nil
}

func (ps *partitionStream) createInsertEventFromDocument(document bson.M) message.ChangeEvent {
	now := time.Now().Unix()
	var t uint32
	if now < 0 {
		t = 0
	} else if now > int64(math.MaxUint32) {
		t = math.MaxUint32
	} else {
		t = uint32(now)
	}

	return message.ChangeEvent{
		OperationType: "insert",
		DocumentKey: message.DocumentKey{
			ID: document["_id"],
		},
		FullDocument: document,
		Namespace: message.Namespace{
			Database:   ps.cfg.MongoDB.Connection.Database,
			Collection: ps.cfg.MongoDB.Connection.Collection,
		},
		ClusterTime: primitive.Timestamp{T: t, I: 1},
	}
}

func (ps *partitionStream) shouldSaveBootstrapProgress(worker *streamWorker, state *bootstrapProcessState) bool {
	timeSinceLastCheckpoint := time.Since(state.lastCheckpointTime)

	if state.processedCount%ps.cfg.Checkpoint.BootstrapSaveCount == 0 {
		logger.Log.Debug(fmt.Sprintf("Bootstrap progress (count-based) - partitionId: %d, processed: %d", worker.partitionID, state.processedCount))
		return true
	}

	if timeSinceLastCheckpoint >= ps.cfg.Checkpoint.BootstrapSaveInterval {
		logger.Log.Debug(fmt.Sprintf("Bootstrap progress (time-based) - partitionId: %d, processed: %d, elapsed: %v", worker.partitionID, state.processedCount, timeSinceLastCheckpoint))
		return true
	}

	return false
}

func (ps *partitionStream) saveBootstrapProgress(worker *streamWorker, document bson.M, state *bootstrapProcessState) error {
	saveCtx, saveCancel := context.WithTimeout(context.Background(), ps.cfg.Checkpoint.TokenSaveTimeout)
	defer saveCancel()

	if err := ps.checkpointManager.SaveBootstrapProgress(saveCtx, worker.partitionID, document["_id"]); err != nil {
		logger.Log.Error(fmt.Sprintf("Failed to save bootstrap progress - partitionId: %d, documentId: %v, error: %v", worker.partitionID, document["_id"], err))
		return nil
	}

	logger.Log.Debug(fmt.Sprintf("Saved bootstrap progress - partitionId: %d, documentId: %v, processed: %d", worker.partitionID, document["_id"], state.processedCount))
	state.lastCheckpointTime = time.Now()

	return nil
}

func (ps *partitionStream) finalizeBootstrapAndClearCheckpoint(worker *streamWorker, state *bootstrapProcessState) error {
	logger.Log.Debug(fmt.Sprintf("Bootstrap completed - partitionId: %d, totalProcessed: %d", worker.partitionID, state.processedCount))
	state.bootstrapCompleted = true

	if err := ps.checkpointManager.ClearBootstrapProgress(worker.ctx, worker.partitionID); err != nil {
		logger.Log.Error(fmt.Sprintf("Failed to clear bootstrap progress - partitionId: %d, error: %v", worker.partitionID, err))
		return err
	}

	logger.Log.Info(fmt.Sprintf("Bootstrap progress cleared successfully - partitionId: %d", worker.partitionID))
	return nil
}

func (ps *partitionStream) startAndManageChangeStream(worker *streamWorker, resumeToken []byte, startAtOperationTime *primitive.Timestamp) error {
	if !ps.verifyPartitionOwnership(worker.ctx, worker.partitionID) {
		logger.Log.Warn(fmt.Sprintf("Partition ownership verification failed before change stream - partitionId: %d", worker.partitionID))
		return fmt.Errorf("partition %d not owned by this worker before change stream", worker.partitionID)
	}

	pipeline := ps.buildPartitionedChangeStreamPipeline(worker.partitionID)
	opts := options.ChangeStream().SetFullDocument(options.UpdateLookup)

	if resumeToken != nil {
		opts.SetResumeAfter(bson.Raw(resumeToken))
		logger.Log.Info(fmt.Sprintf("Resuming from token - partitionId: %d", worker.partitionID))
	} else if startAtOperationTime != nil {
		opts.SetStartAtOperationTime(startAtOperationTime)
		logger.Log.Debug(fmt.Sprintf("Starting from operation time - partitionId: %d, operationTime: %v", worker.partitionID, startAtOperationTime))
	}

	changeStream, err := ps.collection.Watch(worker.ctx, pipeline, opts)
	if err != nil {
		if ps.isUnrecoverableResumeError(err) && resumeToken != nil {
			logger.Log.Warn(fmt.Sprintf("Oplog history lost - resume token no longer valid - partitionId: %d, error: %v", worker.partitionID, err))
			return ErrOplogHistoryLost
		}

		return err
	}
	defer changeStream.Close(worker.ctx)

	worker.stream = changeStream

	helpersCtx, helpersCancel := context.WithCancel(worker.ctx)
	defer helpersCancel()

	tokenSaveTicker := time.NewTicker(ps.cfg.Checkpoint.TokenSaveInterval)
	defer tokenSaveTicker.Stop()
	go ps.startPeriodicTokenSaver(helpersCtx, worker, tokenSaveTicker)

	heartbeatTicker := time.NewTicker(ps.cfg.Checkpoint.IdleHeartbeatInterval)
	defer heartbeatTicker.Stop()
	go ps.startIdleHeartbeat(helpersCtx, worker, heartbeatTicker)

	defer ps.saveLatestResumeTokenOnInterruption(worker)

	logger.Log.Debug(fmt.Sprintf("Change stream is now listening for partition %d", worker.partitionID))

	for changeStream.Next(worker.ctx) {
		worker.tokenMutex.Lock()
		worker.lastEventTime = time.Now()
		worker.tokenMutex.Unlock()

		var event message.ChangeEvent
		if err := changeStream.Decode(&event); err != nil {
			logger.Log.Error(fmt.Sprintf("Error decoding change event - partitionId: %d, error: %v", worker.partitionID, err))
			continue
		}

		currentToken := changeStream.ResumeToken()
		if err := ps.processEvent(worker, event, currentToken); err != nil {
			logger.Log.Error(fmt.Sprintf("Error processing event - partitionId: %d, op: %s, error: %v", worker.partitionID, event.OperationType, err))
			continue
		}
	}

	return changeStream.Err()
}

func (ps *partitionStream) saveLatestResumeTokenOnInterruption(worker *streamWorker) {
	worker.tokenMutex.RLock()
	lastToken := worker.lastAckedToken
	lastClusterTime := worker.lastClusterTime
	worker.tokenMutex.RUnlock()

	if len(lastToken) == 0 {
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), ps.cfg.Checkpoint.TokenSaveTimeout)
	defer cancel()

	if err := ps.checkpointManager.SaveResumeToken(ctx, worker.partitionID, lastToken, lastClusterTime); err != nil {
		logger.Log.Error(fmt.Sprintf("Failed to save final resume token on interruption - partitionId: %d, error: %v", worker.partitionID, err))
	} else {
		logger.Log.Info(fmt.Sprintf("Saved final resume token on interruption - partitionId: %d", worker.partitionID))
	}
}

func (ps *partitionStream) verifyPartitionOwnership(ctx context.Context, partitionID int) bool {
	partitionsCol := ps.client.Database(ps.cfg.MongoDB.Connection.Database).Collection(ps.cfg.Partition.PartitionsCollection)

	filter := bson.M{"_id": partitionID}
	var assignment bson.M
	err := partitionsCol.FindOne(ctx, filter).Decode(&assignment)

	if err != nil {
		logger.Log.Debug(fmt.Sprintf("Partition assignment not found - partitionId: %d, error: %v", partitionID, err))
		return false
	}

	if workerID, ok := assignment["workerId"].(string); ok {
		owned := workerID == ps.workerID
		if !owned {
			logger.Log.Debug(fmt.Sprintf("Partition owned by different worker - partitionId: %d, owner: %s, current: %s",
				partitionID, workerID, ps.workerID))
		}
		return owned
	}

	logger.Log.Debug(fmt.Sprintf("Partition assignment invalid format - partitionId: %d", partitionID))
	return false
}

func (ps *partitionStream) buildPartitionedChangeStreamPipeline(partitionID int) []bson.D {
	return []bson.D{
		{
			{Key: "$match", Value: bson.D{
				{Key: "operationType", Value: bson.D{
					{Key: "$in", Value: bson.A{"insert", "update", "delete", "replace"}},
				}},
			}},
		},
		{
			{Key: "$match", Value: bson.D{
				{Key: "$expr", Value: bson.D{
					{Key: "$eq", Value: bson.A{
						bson.D{{Key: "$mod", Value: bson.A{
							ps.buildPartitioningHashExpression("$documentKey._id"),
							ps.cfg.Partition.TotalPartition,
						}}},
						partitionID,
					}},
				}},
			}},
		},
	}
}

func (ps *partitionStream) buildBootstrapPartitionFilter(partitionID int) bson.D {
	return bson.D{
		{Key: "$expr", Value: bson.D{
			{Key: "$eq", Value: bson.A{
				bson.D{{Key: "$mod", Value: bson.A{
					ps.buildPartitioningHashExpression("$_id"),
					ps.cfg.Partition.TotalPartition,
				}}},
				partitionID,
			}},
		}},
	}
}

func (ps *partitionStream) buildPartitioningHashExpression(idField string) bson.D {
	return bson.D{
		{Key: "$cond", Value: bson.D{
			{Key: "if", Value: bson.D{
				{Key: "$or", Value: bson.A{
					bson.D{{Key: "$eq", Value: bson.A{bson.D{{Key: "$type", Value: idField}}, "int"}}},
					bson.D{{Key: "$eq", Value: bson.A{bson.D{{Key: "$type", Value: idField}}, "long"}}},
				}},
			}},
			{Key: "then", Value: idField},
			{Key: "else", Value: bson.D{
				{Key: "$cond", Value: bson.D{
					{Key: "if", Value: ps.createIsNumericStringCheck(idField)},
					{Key: "then", Value: bson.D{{Key: "$toLong", Value: idField}}},
					{Key: "else", Value: ps.buildStringDistributionHash(idField)},
				}},
			}},
		}},
	}
}

func (ps *partitionStream) createIsNumericStringCheck(idField string) bson.D {
	return bson.D{
		{Key: "$and", Value: bson.A{
			bson.D{{Key: "$eq", Value: bson.A{bson.D{{Key: "$type", Value: idField}}, "string"}}},
			bson.D{{Key: "$regexMatch", Value: bson.D{
				{Key: "input", Value: idField},
				{Key: "regex", Value: "^[0-9]+$"},
			}}},
		}},
	}
}

/*
 * This function calculates a compound hash for a given _id.
 * Standard hashing methods in MongoDB can lead to poor distribution for certain
 * string patterns (e.g., sequential or timestamp-based strings).
 * This implementation combines three different hashing algorithms (DJB2, polynomial, position-weighted)
 * to ensure a more uniform distribution of documents across partitions,
 * minimizing hotspots. It's designed to be fast and produce a wide range of hash values.
 */
func (ps *partitionStream) buildStringDistributionHash(idField string) bson.D {
	return bson.D{
		{Key: "$function", Value: bson.D{
			{Key: "body", Value: `
				function(docId) {
					if (docId === null || docId === undefined) return 0;
					
					const str = docId.toString();
					let hash1 = 5381;  // DJB2 hash
					let hash2 = 0;     // Polynomial rolling hash
					let hash3 = 0;     // Position-weighted hash
					
					const len = str.length;
					
					for (let i = 0; i < len; i++) {
						const char = str.charCodeAt(i);
						
						hash1 = ((hash1 << 5) + hash1) + char;
						
						hash2 = (hash2 * 31 + char) % 2147483647;
						
						hash3 += char * (i * 37 + 1);
					}
					
					let finalHash = (hash1 * 7) + (hash2 * 3) + (hash3 * 11);
					
					finalHash += len * 17;
					
					if (len > 0) {
						finalHash += str.charCodeAt(0) * 101;
					}
					if (len > 1) {
						finalHash += str.charCodeAt(len - 1) * 103;
					}
					return Math.abs(finalHash) || 1;
				}
			`},
			{Key: "args", Value: bson.A{idField}},
			{Key: "lang", Value: "js"},
		}},
	}
}

func (ps *partitionStream) buildResumeAfterIdFilter(fieldName string, lastValue interface{}) bson.D {
	switch v := lastValue.(type) {
	case primitive.ObjectID:
		// ObjectIds have natural ordering and work well with $gt
		return bson.D{
			{Key: "$and", Value: bson.A{
				bson.M{fieldName: bson.M{"$type": "objectId"}},
				bson.M{fieldName: bson.M{"$gt": v}},
			}},
		}

	case int, int32, int64, float32, float64:
		// Numeric types work well with $gt, but ensure type consistency
		return bson.D{
			{Key: "$and", Value: bson.A{
				bson.M{fieldName: bson.M{"$type": bson.A{"int", "long", "double", "decimal"}}},
				bson.M{fieldName: bson.M{"$gt": v}},
			}},
		}

	case string:
		// For strings, use simple comparison
		// The Find() operation will apply collation if needed for numeric strings
		return bson.D{
			{Key: "$and", Value: bson.A{
				bson.M{fieldName: bson.M{"$type": "string"}},
				bson.M{fieldName: bson.M{"$gt": v}},
			}},
		}

	case primitive.Binary:
		// UUID/Binary data - use $gt with same type and subtype
		return bson.D{
			{Key: "$and", Value: bson.A{
				bson.M{fieldName: bson.M{"$type": "binData"}},
				bson.M{fieldName: bson.M{"$gt": v}},
			}},
		}

	case primitive.DateTime:
		// DateTime types should be compared as dates
		return bson.D{
			{Key: "$and", Value: bson.A{
				bson.M{fieldName: bson.M{"$type": "date"}},
				bson.M{fieldName: bson.M{"$gt": v}},
			}},
		}

	default:
		// return nil, fmt.Errorf("unsupported _id type for comparison: %T", v)

		// For any other complex type, convert to string representation for comparison
		// This handles UUID strings, complex objects, etc.
		valueStr := fmt.Sprintf("%v", v)
		logger.Log.Info(fmt.Sprintf("Using string-based comparison for complex type %T (value: %s)", v, valueStr))

		return bson.D{
			{Key: "$or", Value: bson.A{
				// Either the field is not a string and we can't compare it safely (skip it)
				bson.M{fieldName: bson.M{"$not": bson.M{"$type": "string"}}},
				// Or it's a string and greater than our string representation
				bson.M{
					"$and": bson.A{
						bson.M{fieldName: bson.M{"$type": "string"}},
						bson.M{fieldName: bson.M{"$gt": valueStr}},
					},
				},
				// Or it's the same complex type but not the exact same value
				bson.M{fieldName: bson.M{"$ne": v}},
			}},
		}
	}
}

func (ps *partitionStream) isUnrecoverableResumeError(err error) bool {
	if err == nil {
		return false
	}

	errorStr := err.Error()
	return strings.Contains(errorStr, "resume token was not found") ||
		strings.Contains(errorStr, "ChangeStreamFatalError") ||
		strings.Contains(errorStr, "cannot resume stream") ||
		strings.Contains(errorStr, "resume token") && strings.Contains(errorStr, "invalid")
}

func (ps *partitionStream) processEvent(worker *streamWorker, event message.ChangeEvent, resumeToken []byte) error {
	worker.inFlightEvents.Add(1)
	defer worker.inFlightEvents.Done()

	startTime := time.Now()

	msg, err := message.NewMessage(event)
	if err != nil {
		return err
	}

	ps.updateMetrics(msg.OperationType)

	ps.metric.SetLastEventTime(time.Now())

	cdcLatency := time.Since(msg.EventTime).Milliseconds()
	ps.metric.SetCDCLatency(cdcLatency)

	listenerCtx := &ListenerContext{
		Context:     worker.ctx,
		Message:     msg,
		PartitionID: worker.partitionID,
		Ack: func() error {
			processingLatency := time.Since(startTime)
			ps.metric.SetProcessLatency(processingLatency.Nanoseconds())

			if len(resumeToken) > 0 {
				worker.tokenMutex.Lock()
				worker.lastAckedToken = append([]byte(nil), resumeToken...)
				worker.lastClusterTime = &event.ClusterTime
				worker.ackedEventCount++
				worker.tokenMutex.Unlock()

				if worker.ackedEventCount >= ps.cfg.Checkpoint.ChangeStreamBatchSize {
					worker.tokenMutex.Lock()
					worker.ackedEventCount = 0
					worker.tokenMutex.Unlock()

					saveStart := time.Now()
					err := ps.checkpointManager.SaveResumeToken(
						ps.ctx,
						worker.partitionID,
						resumeToken,
						&event.ClusterTime,
					)

					saveLatency := time.Since(saveStart).Milliseconds()
					ps.metric.SetCheckpointSaveLatency(saveLatency)

					if err != nil {
						ps.metric.IncCheckpointSaveErrorTotal()
					} else {
						ps.metric.IncCheckpointSaveTotal()
						// Update last checkpoint time on successful save
						ps.metric.SetLastCheckpointTime(time.Now())
					}

					return err
				}
			}
			return nil
		},
	}

	// Measure listener execution time
	listenerStart := time.Now()
	listenerErr := ps.listener(listenerCtx)
	listenerDuration := time.Since(listenerStart)

	// Update listener latency metric
	ps.metric.SetListenerLatency(listenerDuration.Nanoseconds())

	// Track listener errors
	if listenerErr != nil {
		ps.metric.IncListenerErrorTotal()
	}

	return listenerErr
}

func (ps *partitionStream) updateMetrics(opType message.OperationType) {
	switch opType {
	case message.OperationInsert:
		ps.metric.IncInsertTotal()
	case message.OperationUpdate:
		ps.metric.IncUpdateTotal()
	case message.OperationDelete:
		ps.metric.IncDeleteTotal()
	case message.OperationReplace:
		ps.metric.IncInsertTotal()
	}
}

func (ps *partitionStream) startPeriodicTokenSaver(ctx context.Context, worker *streamWorker, ticker *time.Ticker) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			worker.tokenMutex.RLock()
			token := worker.lastAckedToken
			clusterTime := worker.lastClusterTime
			worker.tokenMutex.RUnlock()

			if len(token) > 0 {
				saveStart := time.Now()
				ctx, cancel := context.WithTimeout(context.Background(), ps.cfg.Checkpoint.TokenSaveTimeout)
				err := ps.checkpointManager.SaveResumeToken(
					ctx,
					worker.partitionID,
					token,
					clusterTime,
				)
				cancel()

				// Update checkpoint metrics
				saveLatency := time.Since(saveStart).Milliseconds()
				ps.metric.SetCheckpointSaveLatency(saveLatency)

				if err != nil {
					logger.Log.Error(fmt.Sprintf("Failed to save resume token periodically - partitionId: %d, error: %v", worker.partitionID, err))
					ps.metric.IncCheckpointSaveErrorTotal()
				} else {
					ps.metric.IncCheckpointSaveTotal()
					ps.metric.SetLastCheckpointTime(time.Now())
				}
			}
		}
	}
}

func (ps *partitionStream) startIdleHeartbeat(ctx context.Context, worker *streamWorker, ticker *time.Ticker) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			worker.tokenMutex.RLock()
			idleDuration := time.Since(worker.lastEventTime)
			worker.tokenMutex.RUnlock()

			if idleDuration > ps.cfg.Checkpoint.MaxIdleTime {
				logger.Log.Info(fmt.Sprintf("Stream is idle, updating highwatermark - partitionId: %d, idleDuration: %v",
					worker.partitionID, idleDuration))
				if err := ps.updateResumeTokenToHighwatermark(worker); err != nil {
					logger.Log.Warn(fmt.Sprintf("Failed to update highwatermark - partitionId: %d, error: %v",
						worker.partitionID, err))
				}
			}
		}
	}
}

func (ps *partitionStream) updateResumeTokenToHighwatermark(worker *streamWorker) error {
	worker.tokenMutex.RLock()
	lastToken := worker.lastAckedToken
	worker.tokenMutex.RUnlock()

	// Primary method: Try to get the highwatermark from the driver's internal state.
	highwatermarkToken := worker.stream.ResumeToken()

	// Fallback condition: If the driver returns no token or the same old token,
	// we actively query the server for the latest operation time.
	if highwatermarkToken == nil || bytes.Equal(lastToken, highwatermarkToken) {
		logger.Log.Debug(
			"Highwatermark token is nil or unchanged, falling back to fetching server operation time - partitionId: %d",
			worker.partitionID,
		)

		opTime, err := ps.fetchCurrentDbOperationTimeWithRetry(worker.ctx, 3)
		if err != nil {
			return fmt.Errorf("fallback failed: could not fetch server operation time: %w", err)
		}

		ctx, cancel := context.WithTimeout(context.Background(), ps.cfg.Checkpoint.TokenSaveTimeout)
		defer cancel()

		if err := ps.checkpointManager.SaveResumeToken(ctx, worker.partitionID, nil, opTime); err != nil {
			return fmt.Errorf("fallback failed: failed to save highwatermark checkpoint with opTime: %w", err)
		}

		worker.tokenMutex.Lock()
		worker.lastAckedToken = nil
		worker.lastClusterTime = opTime
		worker.lastEventTime = time.Now()
		worker.tokenMutex.Unlock()

		logger.Log.Debug("Successfully updated highwatermark via fallback method - partitionId: %d", worker.partitionID)
		return nil
	}

	// Primary method successful: We received a new, valid token from the driver.
	ctx, cancel := context.WithTimeout(context.Background(), ps.cfg.Checkpoint.TokenSaveTimeout)
	defer cancel()

	if err := ps.checkpointManager.SaveResumeToken(ctx, worker.partitionID, highwatermarkToken, nil); err != nil {
		return fmt.Errorf("failed to save highwatermark checkpoint with new token: %w", err)
	}

	worker.tokenMutex.Lock()
	worker.lastAckedToken = highwatermarkToken
	worker.lastClusterTime = nil
	worker.lastEventTime = time.Now()
	worker.tokenMutex.Unlock()

	logger.Log.Debug("Successfully updated highwatermark from driver token - partitionId: %d", worker.partitionID)
	return nil
}

func (ps *partitionStream) Stop(ctx context.Context) error {
	logger.Log.Info("Stopping partition stream")

	if err := ps.partitionManager.ReleasePartitions(ctx); err != nil {
		logger.Log.Error(fmt.Sprintf("Failed to release partitions during shutdown: %v", err))
	}

	if ps.cancel != nil {
		ps.cancel()
	}

	ps.streamsMutex.Lock()
	workers := make([]*streamWorker, 0, len(ps.activeStreams))
	for partitionID, worker := range ps.activeStreams {
		logger.Log.Info(fmt.Sprintf("Stopping stream worker %d", partitionID))
		worker.cancel()
		workers = append(workers, worker)
	}
	ps.streamsMutex.Unlock()

	timeout := ps.cfg.GracefulShutdownTimeout
	logger.Log.Info(fmt.Sprintf("Waiting for in-flight events to complete (timeout: %v)", timeout))

	inFlightDone := make(chan struct{})
	go func() {
		for _, worker := range workers {
			worker.inFlightEvents.Wait()
		}
		close(inFlightDone)
	}()

	select {
	case <-inFlightDone:
		logger.Log.Info("All in-flight events completed successfully")
	case <-time.After(timeout):
		logger.Log.Warn(fmt.Sprintf("Timeout (%v) waiting for in-flight events - some events may be reprocessed on restart", timeout))
	}

	workersDone := make(chan struct{})
	go func() {
		ps.wg.Wait()
		close(workersDone)
	}()

	select {
	case <-workersDone:
		logger.Log.Info("All stream workers stopped")
	case <-time.After(5 * time.Second):
		logger.Log.Warn("Timeout waiting for stream workers to stop")
	}

	if err := ps.partitionManager.Stop(ctx); err != nil {
		logger.Log.Error(fmt.Sprintf("Failed to stop partition manager: %v", err))
		return err
	}

	logger.Log.Info("Partition stream stopped successfully")
	return nil
}
