package stream

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math"
	"math/rand"
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
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
)

var ErrOplogHistoryLost = errors.New("oplog history lost, re-snapshot required")

type PartitionStream interface {
	Start(ctx context.Context) error
	Stop(ctx context.Context) error
	Commit()
	CommitBootstrap(partitionID int)
}

type ListenerFunc func(ctx *ListenerContext) error

type ListenerContext struct {
	Context     context.Context
	Ack         func()
	Message     message.Message
	PartitionID int
	IsBootstrap bool
}

type partitionStream struct {
	checkpointManager      checkpoint.Manager
	database               connection.Database
	metric                 metric.Metric
	ctx                    context.Context
	collection             connection.Collection
	changeStreamCollection connection.Collection
	client                 connection.Client
	partitionManager       partition.Manager
	activeStreams          map[int]*streamWorker
	listener               ListenerFunc
	cancel                 context.CancelFunc
	workerID               string
	cfg                    config.Config
	wg                     sync.WaitGroup
	streamsMutex           sync.RWMutex
}

type streamWorker struct {
	lastEventTime            time.Time
	stream                   connection.ChangeStream
	ctx                      context.Context
	cancel                   context.CancelFunc
	bootstrapState           *bootstrapProcessState
	lastClusterTime          *primitive.Timestamp
	pendingCommitClusterTime *primitive.Timestamp
	pendingCommitToken       []byte
	lastAckedToken           []byte
	inFlightEvents           sync.WaitGroup
	ackedEventCount          int
	partitionID              int
	stoppingMutex            sync.RWMutex
	tokenMutex               sync.RWMutex
	bootstrapStateMutex      sync.RWMutex
	commitMutex              sync.Mutex
	stopping                 bool
}

type streamStartInfo struct {
	startAtOperationTime *primitive.Timestamp
	resumeToken          []byte
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
	changeStreamCollection := database.CollectionWithReadPref(cfg.MongoDB.Connection.Collection, readpref.Primary())

	partitionManager := partition.NewManager(workerID, client, cfg.MongoDB.Connection.Database, cfg.Partition)
	checkpointManager := checkpoint.NewManager(
		client,
		cfg.MongoDB.Connection.Database,
		cfg.MongoDB.Connection.Collection,
		cfg.Partition.ConsumerGroup)

	return &partitionStream{
		client:                 client,
		cfg:                    cfg,
		metric:                 metric,
		listener:               listener,
		collection:             collection,
		changeStreamCollection: changeStreamCollection,
		database:               database,
		workerID:               workerID,
		partitionManager:       partitionManager,
		checkpointManager:      checkpointManager,
		activeStreams:          make(map[int]*streamWorker),
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

	randomSeconds := rand.Intn(60)
	jitter := time.Duration(randomSeconds) * time.Second
	totalDelay := 30*time.Second + jitter

	logger.Log.Info("Starting with jitter delay - Base: 30s, Jitter: %v, Total Wait: %v", jitter, totalDelay)

	select {
	case <-ps.ctx.Done():
		logger.Log.Debug("Event processing cancelled during initial delay")
		return ps.ctx.Err()
	case <-time.After(totalDelay):
		logger.Log.Debug("Initial delay completed before acquiring partitions")
	}

	ps.partitionManager.SetPartitionsChangedCallback(ps.reconcilePartitionAssignments)

	if err := ps.acquireAndStartInitialPartitions(); err != nil {
		logger.Log.Error("Failed to acquire initial partitions: %v", err)
		return err
	}

	<-ps.ctx.Done()
	logger.Log.Debug("Partition stream Start() context cancelled")
	return ps.ctx.Err()
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

	rebalanceHappened := false
	workersToStop := make([]*streamWorker, 0)

	for partitionID, worker := range ps.activeStreams {
		found := false
		for _, p := range newPartitions {
			if p == partitionID {
				found = true
				break
			}
		}

		if !found {
			logger.Log.Debug("Stopping stream for partition %d", partitionID)
			worker.stoppingMutex.Lock()
			worker.stopping = true
			worker.stoppingMutex.Unlock()
			worker.cancel()
			workersToStop = append(workersToStop, worker)
			delete(ps.activeStreams, partitionID)
			ps.metric.IncPartitionReleaseTotal()
			rebalanceHappened = true
		}
	}

	ps.streamsMutex.Unlock()

	for _, worker := range workersToStop {
		logger.Log.Debug("Waiting for in-flight events to complete for partition %d", worker.partitionID)
		worker.inFlightEvents.Wait()
		logger.Log.Debug("All in-flight events completed for partition %d", worker.partitionID)
	}

	ps.streamsMutex.Lock()
	defer ps.streamsMutex.Unlock()

	for _, partitionID := range newPartitions {
		if _, exists := ps.activeStreams[partitionID]; !exists {
			logger.Log.Debug("Starting stream for partition %d", partitionID)

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

	if rebalanceHappened {
		ps.metric.IncPartitionRebalanceTotal()
	}

	ps.metric.SetActivePartitionCount(len(ps.activeStreams))
	logger.Log.Debug("Active partitions updated - count: %d, partitions: %v", len(ps.activeStreams), newPartitions)
}

func (ps *partitionStream) managePartitionWorkerLifecycle(worker *streamWorker) {
	defer ps.wg.Done()

	b := backoff.New(backoff.StreamRetryConfig)

	for {
		select {
		case <-worker.ctx.Done():
			return
		default:
			err := ps.startOrResumePartitionStream(worker)
			if err == nil {
				logger.Log.Debug("Partition stream completed normally - partitionId: %d", worker.partitionID)
				return
			}

			if errors.Is(err, context.Canceled) {
				logger.Log.Debug("Partition stream cancelled - partitionId: %d", worker.partitionID)
				return
			}

			if errors.Is(err, ErrOplogHistoryLost) {
				logger.Log.Warn("Oplog history lost detected, triggering automatic re-snapshot - partitionId: %d", worker.partitionID)
				ps.metric.IncResumeTokenExpiredTotal()

				clearCtx, clearCancel := context.WithTimeout(context.Background(), 10*time.Second)
				if clearErr := ps.checkpointManager.ClearResumeToken(clearCtx, worker.partitionID); clearErr != nil {
					logger.Log.Error("Failed to clear resume token during oplog recovery - partitionId: %d, error: %v", worker.partitionID, clearErr)
				}
				clearCancel()

				clearCtx2, clearCancel2 := context.WithTimeout(context.Background(), 10*time.Second)
				if clearErr := ps.checkpointManager.ClearBootstrapProgress(clearCtx2, worker.partitionID); clearErr != nil {
					logger.Log.Error("Failed to clear bootstrap progress during oplog recovery - partitionId: %d, error: %v", worker.partitionID, clearErr)
				}
				clearCancel2()

				logger.Log.Debug("Checkpoint cleared, next iteration will trigger full bootstrap - partitionId: %d", worker.partitionID)

				select {
				case <-time.After(2 * time.Second):
				case <-worker.ctx.Done():
					return
				}
				b.Reset()
				continue
			}

			logger.Log.Error("Partition stream error, retrying - partitionId: %d, error: %v", worker.partitionID, err)
			ps.metric.IncChangeStreamErrorTotal()
			ps.metric.IncChangeStreamRestartTotal()

			delay, _ := b.NextDelay()
			logger.Log.Debug("Backing off for %v before retry - partitionId: %d", delay, worker.partitionID)

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
		logger.Log.Warn("Partition ownership verification failed at start - partitionId: %d", worker.partitionID)
		return fmt.Errorf("partition %d not owned by this worker", worker.partitionID)
	}

	startInfo, err := ps.determineStreamStartState(worker.partitionID)
	if err != nil {
		return fmt.Errorf("failed to prepare stream start: %w", err)
	}

	if startInfo.shouldBootstrap {
		logger.Log.Info("Starting bootstrap flow for partition %d", worker.partitionID)
		ps.metric.SetBootstrapStatus(true)
		err := ps.executeFullBootstrapFlow(worker)
		ps.metric.SetBootstrapStatus(false)
		return err
	}

	logger.Log.Info("Starting change stream for partition %d", worker.partitionID)
	return ps.startAndManageChangeStream(worker, startInfo.resumeToken, startInfo.startAtOperationTime)
}

func (ps *partitionStream) determineStreamStartState(partitionID int) (*streamStartInfo, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	resumeToken, clusterTime, err := ps.checkpointManager.GetResumeToken(ctx, partitionID)
	if err != nil {
		logger.Log.Error("Failed to get resume token - partitionId: %d, error: %v", partitionID, err)
		return nil, err
	}

	bootstrapLastID, err := ps.checkpointManager.GetBootstrapProgress(ctx, partitionID)
	if err != nil {
		logger.Log.Error("Failed to get bootstrap progress - partitionId: %d, error: %v", partitionID, err)
		return nil, err
	}

	shouldBootstrap := bootstrapLastID != nil || (resumeToken == nil && clusterTime == nil)

	if bootstrapLastID != nil {
		logger.Log.Info("Found incomplete bootstrap, will continue - partitionId: %d, lastId: %v", partitionID, bootstrapLastID)
	}

	return &streamStartInfo{
		resumeToken:          resumeToken,
		startAtOperationTime: clusterTime,
		shouldBootstrap:      shouldBootstrap,
	}, nil
}

func (ps *partitionStream) executeFullBootstrapFlow(worker *streamWorker) error {
	if !ps.verifyPartitionOwnership(worker.ctx, worker.partitionID) {
		logger.Log.Warn("Partition ownership verification failed before bootstrap - partitionId: %d", worker.partitionID)
		return fmt.Errorf("partition %d not owned by this worker during bootstrap verification", worker.partitionID)
	}

	opTime, err := ps.fetchCurrentDbOperationTimeWithRetry(worker.ctx, 3)
	if err != nil {
		logger.Log.Warn("Could not get server operation time before bootstrap - partitionId: %d, error: %v", worker.partitionID, err)
	}

	if err := ps.runBootstrapWithRetries(worker); err != nil {
		return fmt.Errorf("bootstrap failed after retries for partition %d: %w", worker.partitionID, err)
	}

	if opTime != nil {
		saveCtx, cancel := context.WithTimeout(context.Background(), ps.cfg.Checkpoint.TokenSaveTimeout)
		if err := ps.checkpointManager.SaveBootstrapClusterTime(saveCtx, worker.partitionID, *opTime); err != nil {
			logger.Log.Warn("Failed to save bootstrap cluster time - partitionId: %d, error: %v", worker.partitionID, err)
		}
		cancel()
	}

	logger.Log.Info("Bootstrap completed, transitioning to change stream for partition %d", worker.partitionID)

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
	cfg := backoff.DefaultConfig
	cfg.MaxRetries = maxRetries
	b := backoff.New(cfg)

	var lastErr error
	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}

		opTime, err := ps.fetchCurrentDbOperationTime(ctx)
		if err == nil && opTime != nil {
			logger.Log.Debug("Successfully obtained operation time from database after %d attempts", b.Attempts())
			return opTime, nil
		}

		lastErr = err
		attempts := b.Attempts()
		if err != nil {
			logger.Log.Warn("Failed to get operation time, attempt %d/%d: %v", attempts+1, maxRetries, err)
		} else {
			logger.Log.Warn("Operation time is nil, attempt %d/%d", attempts+1, maxRetries)
		}

		if sleepErr := b.Sleep(ctx); sleepErr != nil {
			if sleepErr == backoff.ErrMaxRetriesExceeded {
				break
			}
			return nil, sleepErr
		}
	}

	logger.Log.Warn(
		"Failed to get operation time from database after %d attempts, using current time as fallback. Last error: %v",
		b.Attempts(),
		lastErr,
	)

	now := time.Now().Unix()
	var t uint32
	switch {
	case now < 0:
		t = 0
	case now > int64(math.MaxUint32):
		t = math.MaxUint32
	default:
		t = uint32(now)
	}

	fallbackOpTime := &primitive.Timestamp{T: t, I: 1}
	logger.Log.Debug("Using fallback operation time: %v", fallbackOpTime)

	return fallbackOpTime, nil
}

func (ps *partitionStream) getBootstrapProgressWithRetry(partitionID int, maxRetries int) (interface{}, error) {
	cfg := backoff.DefaultConfig
	cfg.MaxRetries = maxRetries
	b := backoff.New(cfg)

	var lastErr error
	for {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)

		bootstrapLastID, err := ps.checkpointManager.GetBootstrapProgress(ctx, partitionID)
		cancel()

		if err == nil {
			logger.Log.Debug("Successfully obtained bootstrap progress after %d attempts - partitionId: %d", b.Attempts(), partitionID)
			return bootstrapLastID, nil
		}

		lastErr = err
		attempts := b.Attempts()
		logger.Log.Warn("Failed to get bootstrap progress, attempt %d/%d - partitionId: %d, error: %v", attempts+1, maxRetries, partitionID, err)

		delay, ok := b.NextDelay()
		if !ok {
			return nil, fmt.Errorf(
				"failed to get bootstrap progress after %d attempts for partition %d, last error: %v",
				attempts,
				partitionID,
				lastErr,
			)
		}

		time.Sleep(delay)
	}
}

func (ps *partitionStream) runBootstrapWithRetries(worker *streamWorker) error {
	b := backoff.New(backoff.SlowConfig)

	for {
		select {
		case <-worker.ctx.Done():
			return worker.ctx.Err()
		default:
		}

		attempts := b.Attempts()
		logger.Log.Debug("Bootstrap attempt %d/%d for partition %d", attempts+1, b.Config.MaxRetries, worker.partitionID)

		err := ps.bootstrapPartition(worker)
		if err == nil {
			logger.Log.Info("Bootstrap successful for partition %d after %d attempts", worker.partitionID, attempts)
			return nil
		}

		if errors.Is(err, context.Canceled) {
			logger.Log.Debug("Bootstrap cancelled for partition %d", worker.partitionID)
			return err
		}

		logger.Log.Warn("Bootstrap attempt %d/%d failed for partition %d: %v", attempts, b.Config.MaxRetries, worker.partitionID, err)

		if sleepErr := b.Sleep(worker.ctx); sleepErr != nil {
			if sleepErr == backoff.ErrMaxRetriesExceeded {
				return fmt.Errorf("bootstrap failed after %d attempts for partition %d", attempts, worker.partitionID)
			}
			return sleepErr
		}
	}
}

func (ps *partitionStream) bootstrapPartition(worker *streamWorker) error {
	logger.Log.Debug("Starting bootstrap for partition %d", worker.partitionID)

	filter, err := ps.loadBootstrapStateAndCreateFilter(worker.partitionID)
	if err != nil {
		return fmt.Errorf("failed to load bootstrap state for partition %d: %w", worker.partitionID, err)
	}

	cursor, err := ps.queryDocumentsForBootstrap(worker, filter)
	if err != nil {
		return err
	}
	defer cursor.Close(worker.ctx)

	return ps.iterateAndProcessBootstrapCursor(worker, cursor)
}

func (ps *partitionStream) loadBootstrapStateAndCreateFilter(partitionID int) (bson.D, error) {
	bootstrapLastID, err := ps.getBootstrapProgressWithRetry(partitionID, 3)
	if err != nil {
		logger.Log.Error("Bootstrap progress error - partitionId: %d, err: %v", partitionID, err)
		return nil, err
	}

	filter := ps.buildBootstrapPartitionFilter(partitionID)

	if bootstrapLastID == nil {
		logger.Log.Info("Starting fresh bootstrap - partitionId: %d", partitionID)
		return filter, nil
	}

	filter = append(filter, bson.E{Key: "_id", Value: bson.M{"$gt": bootstrapLastID}})

	logger.Log.Info("Resuming bootstrap - partitionId: %d, lastId: %v", partitionID, bootstrapLastID)

	return filter, nil
}

func (ps *partitionStream) queryDocumentsForBootstrap(
	worker *streamWorker,
	filter bson.D,
) (connection.Cursor, error) {
	batchSize := ps.cfg.Checkpoint.BootstrapQueryBatchSize

	opts := options.Find().
		SetSort(bson.D{{Key: "_id", Value: 1}}).
		SetHint(bson.D{{Key: "_id", Value: 1}}).
		SetBatchSize(batchSize).
		SetNoCursorTimeout(true).
		SetMaxTime(12 * time.Hour)

	return ps.collection.Find(worker.ctx, filter, opts)
}

func (ps *partitionStream) iterateAndProcessBootstrapCursor(worker *streamWorker, cursor connection.Cursor) error {
	processState := &bootstrapProcessState{
		bootstrapCompleted: false,
	}

	worker.bootstrapStateMutex.Lock()
	worker.bootstrapState = processState
	worker.bootstrapStateMutex.Unlock()

	defer func() {
		worker.bootstrapStateMutex.Lock()
		worker.bootstrapState = nil
		worker.bootstrapStateMutex.Unlock()
	}()

	if ps.cfg.Checkpoint.Type == config.CheckpointTypeAuto {
		bootstrapCtx, bootstrapCancel := context.WithCancel(worker.ctx)
		defer bootstrapCancel()
		bootstrapTicker := time.NewTicker(ps.cfg.Checkpoint.BootstrapSaveInterval)
		defer bootstrapTicker.Stop()
		go ps.startPeriodicBootstrapCommit(bootstrapCtx, worker, processState, bootstrapTicker)
		defer ps.saveFinalBootstrapProgressOnInterruption(worker, processState)
	}

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
	pendingCheckpointID    interface{}
	pendingCheckpointCount int
	totalProcessedCount    int
	pendingCheckpointMutex sync.Mutex
	bootstrapCompleted     bool
}

func (ps *partitionStream) saveFinalBootstrapProgressOnInterruption(worker *streamWorker, state *bootstrapProcessState) {
	logger.Log.Debug("Waiting for in-flight bootstrap events before saving final progress - partitionId: %d", worker.partitionID)
	worker.inFlightEvents.Wait()
	logger.Log.Debug("In-flight bootstrap events completed - partitionId: %d", worker.partitionID)

	state.pendingCheckpointMutex.Lock()
	pendingID := state.pendingCheckpointID
	totalProcessed := state.totalProcessedCount
	state.pendingCheckpointMutex.Unlock()

	if !state.bootstrapCompleted && pendingID != nil {
		ctx, cancel := context.WithTimeout(context.Background(), ps.cfg.Checkpoint.TokenSaveTimeout)
		defer cancel()
		if err := ps.checkpointManager.SaveBootstrapProgress(ctx, worker.partitionID, pendingID); err != nil {
			logger.Log.Error(
				"Failed to save final bootstrap progress on interruption - partitionId: %d, lastId: %v, error: %v",
				worker.partitionID,
				pendingID,
				err,
			)
		} else {
			logger.Log.Debug(
				"Saved bootstrap progress on interruption - partitionId: %d, lastId: %v, totalProcessed: %d",
				worker.partitionID,
				pendingID,
				totalProcessed,
			)
		}
	}
}

func (ps *partitionStream) dispatchBootstrapDocumentToListener(
	worker *streamWorker,
	cursor connection.Cursor,
	state *bootstrapProcessState,
) error {
	var document bson.M
	if err := cursor.Decode(&document); err != nil {
		logger.Log.Error("Error decoding document: %v", err)
		return nil
	}

	syntheticEvent := ps.createInsertEventFromDocument(document)

	if err := ps.processBootstrapEvent(worker, syntheticEvent, state); err != nil {
		if errors.Is(err, context.Canceled) || strings.Contains(err.Error(), "worker is stopping") {
			logger.Log.Debug("Bootstrap event skipped due to shutdown - documentId: %v", syntheticEvent.DocumentKey.ID)
		} else {
			logger.Log.Error("Error processing synthetic event - documentId: %v, error: %v", syntheticEvent.DocumentKey.ID, err)
		}
		return err
	}

	return nil
}

func (ps *partitionStream) processBootstrapEvent(worker *streamWorker, event message.ChangeEvent, state *bootstrapProcessState) error {
	if err := ps.ensureWorkerActive(worker); err != nil {
		return err
	}
	defer worker.inFlightEvents.Done()

	msg, err := message.NewMessage(event)
	if err != nil {
		return err
	}

	ps.updateMetrics(msg.OperationType)
	ps.metric.SetLastEventTime(time.Now())

	msg.IsBootstrap = true

	listenerCtx := &ListenerContext{
		Context:     worker.ctx,
		Message:     msg,
		PartitionID: worker.partitionID,
		IsBootstrap: true,
		Ack:         ps.createBootstrapAck(worker, event, state),
	}

	return ps.executeListener(listenerCtx)
}

func (ps *partitionStream) createInsertEventFromDocument(document bson.M) message.ChangeEvent {
	now := time.Now().Unix()
	var t uint32
	switch {
	case now < 0:
		t = 0
	case now > int64(math.MaxUint32):
		t = math.MaxUint32
	default:
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

func (ps *partitionStream) finalizeBootstrapAndClearCheckpoint(worker *streamWorker, state *bootstrapProcessState) error {
	state.pendingCheckpointMutex.Lock()
	totalProcessed := state.totalProcessedCount
	state.pendingCheckpointMutex.Unlock()

	logger.Log.Debug("Bootstrap completed - partitionId: %d, totalProcessed: %d", worker.partitionID, totalProcessed)
	state.bootstrapCompleted = true

	clearCtx, cancel := context.WithTimeout(context.Background(), ps.cfg.Checkpoint.TokenSaveTimeout)
	defer cancel()
	if err := ps.checkpointManager.ClearBootstrapProgress(clearCtx, worker.partitionID); err != nil {
		logger.Log.Error("Failed to clear bootstrap progress - partitionId: %d, error: %v", worker.partitionID, err)
		return err
	}

	logger.Log.Debug("Bootstrap progress cleared successfully - partitionId: %d", worker.partitionID)
	return nil
}

func (ps *partitionStream) startAndManageChangeStream(
	worker *streamWorker,
	resumeToken []byte,
	startAtOperationTime *primitive.Timestamp,
) error {
	if !ps.verifyPartitionOwnership(worker.ctx, worker.partitionID) {
		logger.Log.Warn("Partition ownership verification failed before change stream - partitionId: %d", worker.partitionID)
		return fmt.Errorf("partition %d not owned by this worker before change stream", worker.partitionID)
	}

	pipeline := ps.buildPartitionedChangeStreamPipeline(worker.partitionID)
	opts := options.ChangeStream().SetFullDocument(options.UpdateLookup)

	if resumeToken != nil {
		opts.SetResumeAfter(bson.Raw(resumeToken))
		logger.Log.Info("Resuming from token - partitionId: %d", worker.partitionID)
	} else if startAtOperationTime != nil {
		opts.SetStartAtOperationTime(startAtOperationTime)
		logger.Log.Debug("Starting from operation time - partitionId: %d, operationTime: %v", worker.partitionID, startAtOperationTime)
	}

	changeStream, err := ps.changeStreamCollection.Watch(worker.ctx, pipeline, opts)
	if err != nil {
		if ps.isUnrecoverableResumeError(err) && resumeToken != nil {
			logger.Log.Warn("Oplog history lost - resume token no longer valid - partitionId: %d, error: %v", worker.partitionID, err)
			return ErrOplogHistoryLost
		}

		return err
	}
	defer changeStream.Close(worker.ctx)

	worker.stream = changeStream

	helpersCtx, helpersCancel := context.WithCancel(worker.ctx)
	defer helpersCancel()

	if ps.cfg.Checkpoint.Type == config.CheckpointTypeAuto {
		commitTicker := time.NewTicker(ps.cfg.Checkpoint.TokenSaveInterval)
		defer commitTicker.Stop()
		go ps.startPeriodicCommit(helpersCtx, worker, commitTicker)
		defer ps.saveLatestResumeTokenOnInterruption(worker)
	}

	heartbeatTicker := time.NewTicker(ps.cfg.Checkpoint.IdleHeartbeatInterval)
	defer heartbeatTicker.Stop()
	go ps.startIdleHeartbeat(helpersCtx, worker, heartbeatTicker)

	logger.Log.Debug("Change stream is now listening for partition %d", worker.partitionID)

	for changeStream.Next(worker.ctx) {
		worker.tokenMutex.Lock()
		worker.lastEventTime = time.Now()
		worker.tokenMutex.Unlock()

		var event message.ChangeEvent
		if err := changeStream.Decode(&event); err != nil {
			logger.Log.Error("Error decoding change event - partitionId: %d, error: %v", worker.partitionID, err)
			continue
		}

		currentToken := changeStream.ResumeToken()
		if err := ps.processEvent(worker, event, currentToken); err != nil {
			logger.Log.Error("Error processing event - partitionId: %d, op: %s, error: %v", worker.partitionID, event.OperationType, err)
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
		logger.Log.Error("Failed to save final resume token on interruption - partitionId: %d, error: %v", worker.partitionID, err)
	} else {
		logger.Log.Debug("Saved final resume token on interruption - partitionId: %d", worker.partitionID)
	}
}

func (ps *partitionStream) verifyPartitionOwnership(ctx context.Context, partitionID int) bool {
	partitionsCol := ps.client.Database(ps.cfg.MongoDB.Connection.Database).Collection(ps.getPartitionsCollectionName())

	filter := bson.M{"_id": partitionID}
	var assignment bson.M
	err := partitionsCol.FindOne(ctx, filter).Decode(&assignment)
	if err != nil {
		logger.Log.Debug("Partition assignment not found - partitionId: %d, error: %v", partitionID, err)
		return false
	}

	if workerID, ok := assignment["workerId"].(string); ok {
		owned := workerID == ps.workerID
		if !owned {
			logger.Log.Debug("Partition owned by different worker - partitionId: %d, owner: %s, current: %s", partitionID, workerID, ps.workerID)
		}
		return owned
	}

	logger.Log.Debug("Partition assignment invalid format - partitionId: %d", partitionID)
	return false
}

func (ps *partitionStream) getPartitionsCollectionName() string {
	return fmt.Sprintf("%s_%s", ps.cfg.Partition.PartitionsCollection, ps.cfg.Partition.ConsumerGroup)
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
		{Key: "$abs", Value: bson.D{
			{Key: "$toHashedIndexKey", Value: idField},
		}},
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
	if err := ps.ensureWorkerActive(worker); err != nil {
		return err
	}
	defer worker.inFlightEvents.Done()

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
		IsBootstrap: false,
		Ack:         ps.createChangeStreamAck(worker, event, resumeToken),
	}

	return ps.executeListener(listenerCtx)
}

func (ps *partitionStream) ensureWorkerActive(worker *streamWorker) error {
	worker.stoppingMutex.RLock()
	if worker.stopping {
		worker.stoppingMutex.RUnlock()
		return fmt.Errorf("worker is stopping, skipping event")
	}
	worker.inFlightEvents.Add(1)
	worker.stoppingMutex.RUnlock()
	return nil
}

func (ps *partitionStream) executeListener(listenerCtx *ListenerContext) error {
	listenerStart := time.Now()
	listenerErr := ps.listener(listenerCtx)
	listenerDuration := time.Since(listenerStart)

	ps.metric.SetListenerLatency(listenerDuration.Nanoseconds())

	if listenerErr != nil {
		ps.metric.IncListenerErrorTotal()
	}

	return listenerErr
}

func (ps *partitionStream) createChangeStreamAck(worker *streamWorker, event message.ChangeEvent, resumeToken []byte) func() {
	return func() {
		if len(resumeToken) == 0 {
			return
		}

		worker.tokenMutex.Lock()
		worker.lastAckedToken = append([]byte(nil), resumeToken...)
		worker.lastClusterTime = &event.ClusterTime
		worker.ackedEventCount++
		ackedCount := worker.ackedEventCount
		worker.tokenMutex.Unlock()

		worker.commitMutex.Lock()
		worker.pendingCommitToken = append([]byte(nil), resumeToken...)
		worker.pendingCommitClusterTime = &event.ClusterTime
		worker.commitMutex.Unlock()

		if ps.cfg.Checkpoint.Type != config.CheckpointTypeAuto || ackedCount < ps.cfg.Checkpoint.ChangeStreamSaveCount {
			return
		}

		worker.tokenMutex.Lock()
		worker.ackedEventCount = 0
		worker.tokenMutex.Unlock()

		saveCtx, cancel := context.WithTimeout(context.Background(), ps.cfg.Checkpoint.TokenSaveTimeout)
		saveStart := time.Now()
		err := ps.checkpointManager.SaveResumeToken(
			saveCtx,
			worker.partitionID,
			worker.pendingCommitToken,
			worker.pendingCommitClusterTime,
		)
		cancel()

		saveLatency := time.Since(saveStart).Milliseconds()
		ps.metric.SetCheckpointSaveLatency(saveLatency)

		if err != nil {
			logger.Log.Error(
				"Failed to save checkpoint on batch size - partitionId: %d, error: %v",
				worker.partitionID,
				err,
			)
			ps.metric.IncCheckpointSaveErrorTotal()
			return
		}

		logger.Log.Debug(
			"Checkpoint saved on batch size - partitionId: %d, batchSize: %d",
			worker.partitionID,
			ps.cfg.Checkpoint.ChangeStreamSaveCount,
		)
		ps.metric.IncCheckpointSaveTotal()
		ps.metric.SetLastCheckpointTime(time.Now())

		worker.commitMutex.Lock()
		worker.pendingCommitToken = nil
		worker.pendingCommitClusterTime = nil
		worker.commitMutex.Unlock()
	}
}

func (ps *partitionStream) createBootstrapAck(worker *streamWorker, event message.ChangeEvent, state *bootstrapProcessState) func() {
	return func() {
		ps.metric.IncBootstrapDocumentTotal()

		state.pendingCheckpointMutex.Lock()
		state.pendingCheckpointID = event.DocumentKey.ID
		state.pendingCheckpointCount++
		state.totalProcessedCount++
		pendingCount := state.pendingCheckpointCount
		state.pendingCheckpointMutex.Unlock()

		if ps.cfg.Checkpoint.Type != config.CheckpointTypeAuto || pendingCount < ps.cfg.Checkpoint.BootstrapSaveCount {
			return
		}

		state.pendingCheckpointMutex.Lock()
		checkpointID := state.pendingCheckpointID
		state.pendingCheckpointCount = 0
		state.pendingCheckpointMutex.Unlock()

		if checkpointID == nil {
			return
		}

		saveCtx, cancel := context.WithTimeout(context.Background(), ps.cfg.Checkpoint.TokenSaveTimeout)
		saveStart := time.Now()
		err := ps.checkpointManager.SaveBootstrapProgress(
			saveCtx,
			worker.partitionID,
			checkpointID,
		)
		cancel()

		saveLatency := time.Since(saveStart).Milliseconds()
		ps.metric.SetCheckpointSaveLatency(saveLatency)

		if err != nil {
			logger.Log.Error(
				"Failed to save bootstrap checkpoint on batch size - partitionId: %d, error: %v",
				worker.partitionID,
				err,
			)
			ps.metric.IncCheckpointSaveErrorTotal()
			return
		}

		logger.Log.Debug(
			"Bootstrap checkpoint saved on batch size - partitionId: %d, lastID: %v, batchSize: %d",
			worker.partitionID,
			checkpointID,
			ps.cfg.Checkpoint.BootstrapSaveCount,
		)
		ps.metric.IncCheckpointSaveTotal()
		ps.metric.SetLastCheckpointTime(time.Now())

		state.pendingCheckpointMutex.Lock()
		state.pendingCheckpointID = nil
		state.pendingCheckpointMutex.Unlock()
	}
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
		ps.metric.IncReplaceTotal()
	}
}

func (ps *partitionStream) startPeriodicBootstrapCommit(
	ctx context.Context,
	worker *streamWorker,
	state *bootstrapProcessState,
	ticker *time.Ticker,
) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			state.pendingCheckpointMutex.Lock()
			checkpointID := state.pendingCheckpointID
			pendingCount := state.pendingCheckpointCount
			totalProcessed := state.totalProcessedCount
			state.pendingCheckpointMutex.Unlock()

			if checkpointID != nil {
				saveCtx, cancel := context.WithTimeout(context.Background(), ps.cfg.Checkpoint.TokenSaveTimeout)
				saveStart := time.Now()
				err := ps.checkpointManager.SaveBootstrapProgress(
					saveCtx,
					worker.partitionID,
					checkpointID,
				)
				cancel()

				saveLatency := time.Since(saveStart).Milliseconds()
				ps.metric.SetCheckpointSaveLatency(saveLatency)

				if err != nil {
					logger.Log.Error(
						"Failed to save bootstrap checkpoint periodically - partitionId: %d, error: %v",
						worker.partitionID,
						err,
					)
					ps.metric.IncCheckpointSaveErrorTotal()
				} else {
					logger.Log.Debug(
						"Bootstrap checkpoint saved periodically - partitionId: %d, lastID: %v, pendingCount: %d, totalProcessed: %d",
						worker.partitionID,
						checkpointID,
						pendingCount,
						totalProcessed,
					)
					ps.metric.IncCheckpointSaveTotal()
					ps.metric.SetLastCheckpointTime(time.Now())

					state.pendingCheckpointMutex.Lock()
					state.pendingCheckpointID = nil
					state.pendingCheckpointCount = 0
					state.pendingCheckpointMutex.Unlock()
				}
			}
		}
	}
}

func (ps *partitionStream) startPeriodicCommit(ctx context.Context, worker *streamWorker, ticker *time.Ticker) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			worker.commitMutex.Lock()
			token := worker.pendingCommitToken
			clusterTime := worker.pendingCommitClusterTime
			worker.commitMutex.Unlock()

			if len(token) > 0 || clusterTime != nil {
				saveCtx, cancel := context.WithTimeout(context.Background(), ps.cfg.Checkpoint.TokenSaveTimeout)
				saveStart := time.Now()
				err := ps.checkpointManager.SaveResumeToken(
					saveCtx,
					worker.partitionID,
					token,
					clusterTime,
				)
				cancel()

				saveLatency := time.Since(saveStart).Milliseconds()
				ps.metric.SetCheckpointSaveLatency(saveLatency)

				if err != nil {
					logger.Log.Error("Failed to save checkpoint periodically - partitionId: %d, error: %v", worker.partitionID, err)
					ps.metric.IncCheckpointSaveErrorTotal()
				} else {
					logger.Log.Debug("Checkpoint saved periodically - partitionId: %d", worker.partitionID)
					ps.metric.IncCheckpointSaveTotal()
					ps.metric.SetLastCheckpointTime(time.Now())

					worker.commitMutex.Lock()
					worker.pendingCommitToken = nil
					worker.pendingCommitClusterTime = nil
					worker.commitMutex.Unlock()
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
				logger.Log.Debug("Stream is idle, updating highwatermark - partitionId: %d, idleDuration: %v", worker.partitionID, idleDuration)
				if err := ps.updateResumeTokenToHighwatermark(worker); err != nil {
					logger.Log.Warn("Failed to update highwatermark - partitionId: %d, error: %v", worker.partitionID, err)
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

func (ps *partitionStream) Commit() {
	ps.streamsMutex.RLock()
	defer ps.streamsMutex.RUnlock()

	for _, worker := range ps.activeStreams {
		worker.commitMutex.Lock()
		token := worker.pendingCommitToken
		clusterTime := worker.pendingCommitClusterTime
		worker.commitMutex.Unlock()

		if len(token) > 0 || clusterTime != nil {
			saveCtx, cancel := context.WithTimeout(context.Background(), ps.cfg.Checkpoint.TokenSaveTimeout)
			saveStart := time.Now()
			err := ps.checkpointManager.SaveResumeToken(
				saveCtx,
				worker.partitionID,
				token,
				clusterTime,
			)
			cancel()

			saveLatency := time.Since(saveStart).Milliseconds()
			ps.metric.SetCheckpointSaveLatency(saveLatency)

			if err != nil {
				logger.Log.Error("Failed to commit checkpoint - partitionId: %d, error: %v", worker.partitionID, err)
				ps.metric.IncCheckpointSaveErrorTotal()
			} else {
				logger.Log.Debug("Checkpoint committed successfully - partitionId: %d", worker.partitionID)
				ps.metric.IncCheckpointSaveTotal()
				ps.metric.SetLastCheckpointTime(time.Now())

				worker.commitMutex.Lock()
				worker.pendingCommitToken = nil
				worker.pendingCommitClusterTime = nil
				worker.commitMutex.Unlock()
			}
		}
	}
}

func (ps *partitionStream) CommitBootstrap(partitionID int) {
	ps.streamsMutex.RLock()
	worker, exists := ps.activeStreams[partitionID]
	ps.streamsMutex.RUnlock()

	if !exists {
		logger.Log.Warn("Cannot commit bootstrap - partition not found: %d", partitionID)
		return
	}

	bootstrapState := ps.getBootstrapState(worker)
	if bootstrapState == nil || bootstrapState.bootstrapCompleted {
		return
	}

	bootstrapState.pendingCheckpointMutex.Lock()
	pendingID := bootstrapState.pendingCheckpointID
	pendingCount := bootstrapState.pendingCheckpointCount
	bootstrapState.pendingCheckpointMutex.Unlock()

	if pendingID != nil && pendingCount > 0 {
		saveCtx, cancel := context.WithTimeout(context.Background(), ps.cfg.Checkpoint.TokenSaveTimeout)
		defer cancel()

		saveStart := time.Now()
		err := ps.checkpointManager.SaveBootstrapProgress(saveCtx, partitionID, pendingID)
		saveLatency := time.Since(saveStart).Milliseconds()
		ps.metric.SetCheckpointSaveLatency(saveLatency)

		if err != nil {
			logger.Log.Error("Failed to commit bootstrap checkpoint - partitionId: %d, error: %v", partitionID, err)
			ps.metric.IncCheckpointSaveErrorTotal()
		} else {
			logger.Log.Debug("Bootstrap checkpoint committed - partitionId: %d, documentId: %v, count: %d", partitionID, pendingID, pendingCount)
			ps.metric.IncCheckpointSaveTotal()
			ps.metric.SetLastCheckpointTime(time.Now())

			bootstrapState.pendingCheckpointMutex.Lock()
			bootstrapState.pendingCheckpointID = nil
			bootstrapState.pendingCheckpointCount = 0
			bootstrapState.pendingCheckpointMutex.Unlock()
		}
	}
}

func (ps *partitionStream) getBootstrapState(worker *streamWorker) *bootstrapProcessState {
	worker.bootstrapStateMutex.RLock()
	defer worker.bootstrapStateMutex.RUnlock()
	return worker.bootstrapState
}

func (ps *partitionStream) Stop(ctx context.Context) error {
	logger.Log.Info("Stopping partition stream")

	ps.partitionManager.SetPartitionsChangedCallback(nil)

	if ps.cancel != nil {
		ps.cancel()
	}

	ps.streamsMutex.Lock()
	workers := make([]*streamWorker, 0, len(ps.activeStreams))
	for partitionID, worker := range ps.activeStreams {
		logger.Log.Debug("Stopping stream worker %d", partitionID)
		worker.stoppingMutex.Lock()
		worker.stopping = true
		worker.stoppingMutex.Unlock()
		worker.cancel()
		workers = append(workers, worker)
	}
	ps.streamsMutex.Unlock()

	timeout := ps.cfg.GracefulShutdownTimeout
	logger.Log.Info("Waiting for in-flight events to complete (timeout: %v)", timeout)

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
		logger.Log.Warn("Timeout (%v) waiting for in-flight events - some events may be reprocessed on restart", timeout)
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
	case <-ctx.Done():
		logger.Log.Warn("Context cancelled while waiting for stream workers to stop")
	}

	if err := ps.partitionManager.Stop(ctx); err != nil {
		logger.Log.Error("Failed to stop partition manager: %v", err)
		return err
	}

	logger.Log.Info("Partition stream stopped successfully")
	return nil
}
