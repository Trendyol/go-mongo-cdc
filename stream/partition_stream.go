package stream

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/Trendyol/go-mongo-cdc/checkpoint"
	"github.com/Trendyol/go-mongo-cdc/config"
	"github.com/Trendyol/go-mongo-cdc/internal/metric"
	"github.com/Trendyol/go-mongo-cdc/mongo/connection"
	"github.com/Trendyol/go-mongo-cdc/mongo/message"
	"github.com/Trendyol/go-mongo-cdc/partition"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.uber.org/zap"
)

type PartitionStream interface {
	Start(ctx context.Context) error
	Stop(ctx context.Context) error
}

type ListenerFunc func(ctx *ListenerContext) error

type ListenerContext struct {
	Message     message.Message
	PartitionID int
	Ack         func() error
}

type partitionStream struct {
	client     connection.Client
	cfg        config.Config
	metric     metric.Metric
	listener   ListenerFunc
	logger     *zap.Logger
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
	logger *zap.Logger,
	workerID string,
) PartitionStream {
	database := client.Database(cfg.Database)
	collection := database.Collection(cfg.Collection)

	partitionManager := partition.NewManager(workerID, client, cfg.Partition, logger)
	checkpointManager := checkpoint.NewManager(client, cfg.Database, cfg.Collection, logger)

	return &partitionStream{
		client:            client,
		cfg:               cfg,
		metric:            metric,
		listener:          listener,
		logger:            logger,
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

	if err := ps.checkReplicaSetStatus(ps.ctx); err != nil {
		return err
	}

	if err := ps.partitionManager.Initialize(ps.ctx); err != nil {
		return fmt.Errorf("failed to initialize partition manager: %w", err)
	}

	ps.partitionManager.SetPartitionsChangedCallback(ps.updateStreams)

	if err := ps.initializePartitions(); err != nil {
		ps.logger.Error(fmt.Sprintf("Failed to acquire initial partitions: %v", err))
		return err
	}

	return nil
}

func (ps *partitionStream) checkReplicaSetStatus(ctx context.Context) error {
	result := ps.database.RunCommand(ctx, bson.D{{Key: "isMaster", Value: 1}})

	var isMaster bson.M
	if err := result.Decode(&isMaster); err != nil {
		return err
	}

	if _, ok := isMaster["setName"]; ok {
		ps.logger.Info("Connected to MongoDB replica set")
		return nil
	}

	if msg, ok := isMaster["msg"]; ok && msg == "isdbgrid" {
		ps.logger.Debug("Connected to MongoDB sharded cluster")
		return nil
	}

	return errors.New(
		"MongoDB is not running as a replica set or sharded cluster. " +
			"Change streams require replica set or sharded cluster",
	)
}

func (ps *partitionStream) initializePartitions() error {
	// Wait a bit to allow other workers to register
	select {
	case <-ps.ctx.Done():
		return ps.ctx.Err()
	case <-time.After(3 * time.Second):
		ps.logger.Debug("Initial partition acquisition delay completed")
	}

	partitions, err := ps.partitionManager.AcquirePartitions(ps.ctx)
	if err != nil {
		return err
	}

	ps.updateStreams(partitions)
	return nil
}

func (ps *partitionStream) updateStreams(newPartitions []int) {
	ps.streamsMutex.Lock()
	defer ps.streamsMutex.Unlock()

	for partitionID, worker := range ps.activeStreams {
		found := false
		for _, p := range newPartitions {
			if p == partitionID {
				found = true
				break
			}
		}

		if !found {
			ps.logger.Debug(fmt.Sprintf("Stopping stream for partition %d", partitionID))
			worker.cancel()
			delete(ps.activeStreams, partitionID)
		}
	}

	for _, partitionID := range newPartitions {
		if _, exists := ps.activeStreams[partitionID]; !exists {
			ps.logger.Debug(fmt.Sprintf("Starting stream for partition %d", partitionID))

			worker := &streamWorker{
				partitionID: partitionID,
			}
			worker.ctx, worker.cancel = context.WithCancel(ps.ctx)

			ps.activeStreams[partitionID] = worker

			ps.wg.Add(1)
			go ps.runPartitionStream(worker)
		}
	}

	ps.logger.Debug(fmt.Sprintf("Active partitions updated - count: %d, partitions: %v", len(ps.activeStreams), newPartitions))
}

func (ps *partitionStream) runPartitionStream(worker *streamWorker) {
	defer ps.wg.Done()

	select {
	case <-worker.ctx.Done():
		ps.logger.Debug(fmt.Sprintf("Event processing cancelled during delay - partitionId: %d", worker.partitionID))
		return
	case <-time.After(30 * time.Second): //TODO: configden alınabilir
		ps.logger.Debug(fmt.Sprintf("Event processing delay completed - partitionId: %d, workerId: %s",
			worker.partitionID, ps.workerID))
	}

	for {
		select {
		case <-worker.ctx.Done():
			return
		default:
			err := ps.processPartitionStream(worker)
			if err == nil {
				ps.logger.Debug(fmt.Sprintf("Partition stream completed normally - partitionId: %d", worker.partitionID))
				return
			}

			if errors.Is(err, context.Canceled) {
				ps.logger.Info(fmt.Sprintf("Partition stream cancelled - partitionId: %d", worker.partitionID))
				return
			}

			//TODO: Exponential Backoff with Jitter yapılabilir
			// sonsuza dek denemeli mi?
			ps.logger.Error(fmt.Sprintf("Partition stream error, retrying - partitionId: %d, error: %v", worker.partitionID, err))

			time.Sleep(5 * time.Second)
		}
	}
}

func (ps *partitionStream) processPartitionStream(worker *streamWorker) error {
	if !ps.verifyPartitionOwnership(worker.ctx, worker.partitionID) {
		ps.logger.Warn(fmt.Sprintf("Partition ownership verification failed at start - partitionId: %d", worker.partitionID))
		return fmt.Errorf("partition %d not owned by this worker", worker.partitionID)
	}

	startInfo, err := ps.getStreamStartInfo(worker.partitionID)
	if err != nil {
		return fmt.Errorf("failed to prepare stream start: %w", err)
	}

	if startInfo.shouldBootstrap {
		ps.logger.Info(fmt.Sprintf("Starting bootstrap flow for partition %d", worker.partitionID))
		return ps.runBootstrapFlow(worker)
	}

	ps.logger.Info(fmt.Sprintf("Starting change stream for partition %d", worker.partitionID))
	return ps.runChangeStream(worker, startInfo.resumeToken, startInfo.startAtOperationTime)
}

func (ps *partitionStream) getStreamStartInfo(partitionID int) (*streamStartInfo, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	resumeToken, clusterTime, err := ps.checkpointManager.GetResumeToken(ctx, partitionID)
	if err != nil {
		ps.logger.Error(fmt.Sprintf("Failed to get resume token - partitionId: %d, error: %v", partitionID, err))
		return nil, err
	}

	bootstrapLastID, err := ps.checkpointManager.GetBootstrapProgress(ctx, partitionID)
	if err != nil {
		ps.logger.Error(fmt.Sprintf("Failed to get bootstrap progress - partitionId: %d, error: %v", partitionID, err))
		return nil, err
	}

	shouldBootstrap := bootstrapLastID != nil || (resumeToken == nil && clusterTime == nil)

	if bootstrapLastID != nil {
		ps.logger.Info(fmt.Sprintf("Found incomplete bootstrap, will continue - partitionId: %d, lastId: %v", partitionID, bootstrapLastID))
	}

	return &streamStartInfo{
		resumeToken:          resumeToken,
		startAtOperationTime: clusterTime,
		shouldBootstrap:      shouldBootstrap,
	}, nil
}

func (ps *partitionStream) runBootstrapFlow(worker *streamWorker) error {
	if !ps.verifyPartitionOwnership(worker.ctx, worker.partitionID) {
		ps.logger.Warn(fmt.Sprintf("Partition ownership verification failed before bootstrap - partitionId: %d", worker.partitionID))
		return fmt.Errorf("partition %d not owned by this worker during bootstrap verification", worker.partitionID)
	}

	opTime, err := ps.getServerOperationTime(worker.ctx)
	if err != nil {
		ps.logger.Warn(fmt.Sprintf("Could not get server operation time before bootstrap - partitionId: %d, error: %v", worker.partitionID, err))
		//TODO: opTime şart burayı duzelt
	}

	if err := ps.bootstrapPartitionWithRetry(worker); err != nil {
		return fmt.Errorf("bootstrap failed after retries for partition %d: %w", worker.partitionID, err)
	}

	if opTime != nil {
		if err := ps.checkpointManager.SaveBootstrapClusterTime(worker.ctx, worker.partitionID, *opTime); err != nil {
			ps.logger.Warn(fmt.Sprintf("Failed to save bootstrap cluster time - partitionId: %d, error: %v", worker.partitionID, err))
		}
	}

	ps.logger.Info(fmt.Sprintf("Bootstrap completed, transitioning to change stream for partition %d", worker.partitionID))

	return ps.runChangeStream(worker, nil, opTime)
}

func (ps *partitionStream) getServerOperationTime(ctx context.Context) (*primitive.Timestamp, error) {
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

func (ps *partitionStream) bootstrapPartitionWithRetry(worker *streamWorker) error {
	maxRetries := 3
	retryDelay := 5 * time.Second

	for attempt := 1; attempt <= maxRetries; attempt++ {
		select {
		case <-worker.ctx.Done():
			return worker.ctx.Err()
		default:
		}

		ps.logger.Debug(fmt.Sprintf("Bootstrap attempt %d/%d for partition %d", attempt, maxRetries, worker.partitionID))

		err := ps.bootstrapPartition(worker)
		if err == nil {
			ps.logger.Info(fmt.Sprintf("Bootstrap successful for partition %d after %d attempts", worker.partitionID, attempt))
			return nil
		}

		if errors.Is(err, context.Canceled) {
			ps.logger.Info(fmt.Sprintf("Bootstrap cancelled for partition %d", worker.partitionID))
			return err
		}

		ps.logger.Warn(fmt.Sprintf("Bootstrap attempt %d/%d failed for partition %d: %v", attempt, maxRetries, worker.partitionID, err))

		if attempt < maxRetries {
			select {
			case <-worker.ctx.Done():
				return worker.ctx.Err()
			case <-time.After(retryDelay):
			}
		}
	}

	return fmt.Errorf("bootstrap failed after %d attempts for partition %d", maxRetries, worker.partitionID)
}

func (ps *partitionStream) bootstrapPartition(worker *streamWorker) error {
	ps.logger.Debug(fmt.Sprintf("Starting bootstrap for partition %d", worker.partitionID))

	bootstrapLastID, filter := ps.prepareBootstrapProgress(worker.partitionID)

	cursor, err := ps.createBootstrapCursor(worker, filter, bootstrapLastID)
	if err != nil {
		return err
	}
	defer cursor.Close(worker.ctx)

	return ps.processBootstrapDocuments(worker, cursor)
}

func (ps *partitionStream) prepareBootstrapProgress(partitionID int) (interface{}, bson.D) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	bootstrapLastID, err := ps.checkpointManager.GetBootstrapProgress(ctx, partitionID)
	if err != nil {
		ps.logger.Error(fmt.Sprintf("Failed to get bootstrap progress - partitionId: %d, error: %v", partitionID, err))
		//TODO: bu durumda devam mı etmeli?
	}

	filter := ps.createBootstrapFilter(partitionID)
	if bootstrapLastID != nil {
		comparisonFilter := ps.createTypeSafeGreaterThanFilter("_id", bootstrapLastID)
		filter = append(filter, comparisonFilter...)
		ps.logger.Info(fmt.Sprintf("Resuming bootstrap from saved progress - partitionId: %d, lastId: %v (type: %T)", partitionID, bootstrapLastID, bootstrapLastID))
	} else {
		ps.logger.Info(fmt.Sprintf("Starting fresh bootstrap - partitionId: %d", partitionID))
	}

	return bootstrapLastID, filter
}

func (ps *partitionStream) createBootstrapCursor(worker *streamWorker, filter bson.D, bootstrapLastID interface{}) (connection.Cursor, error) {
	useNumericStringSorting := ps.shouldUseNumericStringSorting(worker, filter, bootstrapLastID)

	opts := options.Find().SetSort(bson.D{{Key: "_id", Value: 1}})

	if useNumericStringSorting {
		opts.SetCollation(&options.Collation{
			Locale:          "en",
			NumericOrdering: true,
		})
		ps.logger.Debug("Using collation-based numeric string sorting")
	}

	return ps.collection.Find(worker.ctx, filter, opts)
}

func (ps *partitionStream) shouldUseNumericStringSorting(worker *streamWorker, filter bson.D, bootstrapLastID interface{}) bool {
	if bootstrapLastID != nil {
		if lastIDStr, ok := bootstrapLastID.(string); ok && ps.isNumericString(lastIDStr) {
			ps.logger.Info(fmt.Sprintf("Resuming with numeric string ID '%s' - using mathematical sorting", lastIDStr))
			return true
		}
		return false
	}

	isNumericStringCollection, err := ps.detectNumericStringCollection(worker.ctx, filter)
	if err != nil {
		ps.logger.Warn(fmt.Sprintf("Failed to detect ID type, using default sorting - partitionId: %d, error: %v", worker.partitionID, err))
		return false
	}

	if isNumericStringCollection {
		ps.logger.Info("Detected numeric string IDs in collection - using mathematical sorting for fresh bootstrap")
		return true
	}

	return false
}

func (ps *partitionStream) processBootstrapDocuments(worker *streamWorker, cursor connection.Cursor) error {
	processState := &bootstrapProcessState{
		processedCount:     0,
		lastProcessedID:    nil,
		lastCheckpointTime: time.Now(),
		bootstrapCompleted: false,
	}

	defer ps.handleBootstrapInterruption(worker, processState)

	for cursor.Next(worker.ctx) {
		if err := ps.processBootstrapDocument(worker, cursor, processState); err != nil {
			return err
		}
	}

	if err := cursor.Err(); err != nil {
		return err
	}

	return ps.completeBootstrap(worker, processState)
}

type bootstrapProcessState struct {
	processedCount     int
	lastProcessedID    interface{}
	lastCheckpointTime time.Time
	bootstrapCompleted bool
}

func (ps *partitionStream) handleBootstrapInterruption(worker *streamWorker, state *bootstrapProcessState) {
	if !state.bootstrapCompleted && state.lastProcessedID != nil {
		ctx, cancel := context.WithTimeout(context.Background(), ps.cfg.Checkpoint.SaveTimeout)
		defer cancel()
		if err := ps.checkpointManager.SaveBootstrapProgress(ctx, worker.partitionID, state.lastProcessedID); err != nil {
			ps.logger.Error(fmt.Sprintf("Failed to save final bootstrap progress on interruption - partitionId: %d, lastId: %v, error: %v", worker.partitionID, state.lastProcessedID, err))
		} else {
			ps.logger.Info(fmt.Sprintf("Saved bootstrap progress on interruption - partitionId: %d, lastId: %v, processed: %d", worker.partitionID, state.lastProcessedID, state.processedCount))
		}
	}
}

func (ps *partitionStream) processBootstrapDocument(worker *streamWorker, cursor connection.Cursor, state *bootstrapProcessState) error {
	if state.processedCount > 0 && state.processedCount%1000 == 0 {
		if !ps.verifyPartitionOwnership(worker.ctx, worker.partitionID) {
			ps.logger.Warn(fmt.Sprintf("Partition ownership lost during bootstrap - partitionId: %d, processed: %d", worker.partitionID, state.processedCount))
			return fmt.Errorf("partition %d ownership lost during bootstrap", worker.partitionID)
		}
	}

	var document bson.M
	if err := cursor.Decode(&document); err != nil {
		ps.logger.Error(fmt.Sprintf("Error decoding document: %v", err))
		return nil
	}

	syntheticEvent := ps.createSyntheticEvent(document)
	if err := ps.processEvent(worker, syntheticEvent, nil); err != nil {
		ps.logger.Error(fmt.Sprintf("Error processing synthetic event - documentId: %v, error: %v", document["_id"], err))
		return nil
	}

	state.processedCount++
	state.lastProcessedID = document["_id"]

	return ps.handleBootstrapCheckpoint(worker, document, state)
}

func (ps *partitionStream) createSyntheticEvent(document bson.M) message.ChangeEvent {
	return message.ChangeEvent{
		OperationType: "insert",
		DocumentKey: message.DocumentKey{
			ID: document["_id"],
		},
		FullDocument: document,
		Namespace: message.Namespace{
			Database:   ps.cfg.Database,
			Collection: ps.cfg.Collection,
		},
		ClusterTime: primitive.Timestamp{T: uint32(time.Now().Unix()), I: 1},
	}
}

func (ps *partitionStream) handleBootstrapCheckpoint(worker *streamWorker, document bson.M, state *bootstrapProcessState) error {
	shouldSaveCheckpoint := ps.shouldSaveBootstrapCheckpoint(worker, state)
	if !shouldSaveCheckpoint {
		return nil
	}

	if !ps.verifyPartitionOwnership(worker.ctx, worker.partitionID) {
		ps.logger.Warn(fmt.Sprintf("Partition ownership lost before checkpoint save - partitionId: %d, processed: %d", worker.partitionID, state.processedCount))
		return fmt.Errorf("partition %d ownership lost before checkpoint", worker.partitionID)
	}

	return ps.saveBootstrapCheckpoint(worker, document, state)
}

func (ps *partitionStream) shouldSaveBootstrapCheckpoint(worker *streamWorker, state *bootstrapProcessState) bool {
	timeSinceLastCheckpoint := time.Since(state.lastCheckpointTime)

	if state.processedCount%ps.cfg.Checkpoint.BootstrapSaveCount == 0 {
		ps.logger.Debug(fmt.Sprintf("Bootstrap progress (count-based) - partitionId: %d, processed: %d", worker.partitionID, state.processedCount))
		return true
	}

	if timeSinceLastCheckpoint >= ps.cfg.Checkpoint.BootstrapSaveInterval {
		ps.logger.Debug(fmt.Sprintf("Bootstrap progress (time-based) - partitionId: %d, processed: %d, elapsed: %v", worker.partitionID, state.processedCount, timeSinceLastCheckpoint))
		return true
	}

	return false
}

func (ps *partitionStream) saveBootstrapCheckpoint(worker *streamWorker, document bson.M, state *bootstrapProcessState) error {
	saveCtx, saveCancel := context.WithTimeout(context.Background(), ps.cfg.Checkpoint.SaveTimeout)
	defer saveCancel()

	if err := ps.checkpointManager.SaveBootstrapProgress(saveCtx, worker.partitionID, document["_id"]); err != nil {
		ps.logger.Error(fmt.Sprintf("Failed to save bootstrap progress - partitionId: %d, documentId: %v, error: %v", worker.partitionID, document["_id"], err))
		return nil
	}

	ps.logger.Debug(fmt.Sprintf("Saved bootstrap progress - partitionId: %d, documentId: %v, processed: %d", worker.partitionID, document["_id"], state.processedCount))
	state.lastCheckpointTime = time.Now()
	return nil
}

func (ps *partitionStream) completeBootstrap(worker *streamWorker, state *bootstrapProcessState) error {
	ps.logger.Debug(fmt.Sprintf("Bootstrap completed - partitionId: %d, totalProcessed: %d", worker.partitionID, state.processedCount))
	state.bootstrapCompleted = true

	if err := ps.checkpointManager.ClearBootstrapProgress(worker.ctx, worker.partitionID); err != nil {
		ps.logger.Error(fmt.Sprintf("Failed to clear bootstrap progress - partitionId: %d, error: %v", worker.partitionID, err))
		return err
	}

	ps.logger.Info(fmt.Sprintf("Bootstrap progress cleared successfully - partitionId: %d", worker.partitionID))
	return nil
}

func (ps *partitionStream) runChangeStream(worker *streamWorker, resumeToken []byte, startAtOperationTime *primitive.Timestamp) error {
	if !ps.verifyPartitionOwnership(worker.ctx, worker.partitionID) {
		ps.logger.Warn(fmt.Sprintf("Partition ownership verification failed before change stream - partitionId: %d", worker.partitionID))
		return fmt.Errorf("partition %d not owned by this worker before change stream", worker.partitionID)
	}

	pipeline := ps.createChangeStreamPipeline(worker.partitionID)
	opts := options.ChangeStream().SetFullDocument(options.UpdateLookup)

	if resumeToken != nil {
		opts.SetResumeAfter(bson.Raw(resumeToken))
		ps.logger.Info(fmt.Sprintf("Resuming from token - partitionId: %d", worker.partitionID))
	} else if startAtOperationTime != nil {
		opts.SetStartAtOperationTime(startAtOperationTime)
		ps.logger.Debug(fmt.Sprintf("Starting from operation time - partitionId: %d, operationTime: %v", worker.partitionID, startAtOperationTime))
	}

	changeStream, err := ps.collection.Watch(worker.ctx, pipeline, opts)
	if err != nil {
		if ps.isResumeTokenError(err) && resumeToken != nil {
			ps.logger.Warn("Resume token expired or invalid, clearing and starting from current time",
				zap.Int("partitionId", worker.partitionID),
				zap.Error(err))

			if clearErr := ps.checkpointManager.ClearResumeToken(worker.ctx, worker.partitionID); clearErr != nil {
				ps.logger.Error("Failed to clear invalid resume token", zap.Error(clearErr))
			}

			return ps.runChangeStream(worker, nil, startAtOperationTime)
		}

		return err
	}
	defer changeStream.Close(worker.ctx)

	worker.stream = changeStream

	tokenSaveTicker := time.NewTicker(ps.cfg.Checkpoint.SaveInterval)
	defer tokenSaveTicker.Stop()
	go ps.periodicTokenSave(worker, tokenSaveTicker)

	//TODO: her eventi isler islemez kaydediyoruz su an yapı degisince ise yarayacak
	//defer ps.saveFinalTokenOnExit(worker)

	ps.logger.Debug(fmt.Sprintf("Change stream is now listening for partition %d", worker.partitionID))

	for changeStream.Next(worker.ctx) {
		var event message.ChangeEvent
		if err := changeStream.Decode(&event); err != nil {
			ps.logger.Error(fmt.Sprintf("Error decoding change event - partitionId: %d, error: %v", worker.partitionID, err))
			continue
		}

		currentToken := changeStream.ResumeToken()
		if err := ps.processEvent(worker, event, currentToken); err != nil {
			ps.logger.Error(fmt.Sprintf("Error processing event - partitionId: %d, op: %s, error: %v", worker.partitionID, event.OperationType, err))
			continue
		}
	}

	return changeStream.Err()
}

func (ps *partitionStream) saveFinalTokenOnExit(worker *streamWorker) {
	worker.tokenMutex.RLock()
	lastToken := worker.lastAckedToken
	lastClusterTime := worker.lastClusterTime
	worker.tokenMutex.RUnlock()

	if len(lastToken) == 0 {
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), ps.cfg.Checkpoint.SaveTimeout)
	defer cancel()

	if err := ps.checkpointManager.SaveResumeToken(ctx, worker.partitionID, lastToken, lastClusterTime); err != nil {
		ps.logger.Error(fmt.Sprintf("Failed to save final resume token on interruption - partitionId: %d, error: %v", worker.partitionID, err))
	} else {
		ps.logger.Info(fmt.Sprintf("Saved final resume token on interruption - partitionId: %d", worker.partitionID))
	}
}

func (ps *partitionStream) verifyPartitionOwnership(ctx context.Context, partitionID int) bool {
	partitionsCol := ps.client.Database(ps.cfg.Partition.PartitionDatabase).Collection("partition_assignments")

	filter := bson.M{"_id": partitionID}
	var assignment bson.M
	err := partitionsCol.FindOne(ctx, filter).Decode(&assignment)

	if err != nil {
		ps.logger.Debug(fmt.Sprintf("Partition assignment not found - partitionId: %d, error: %v", partitionID, err))
		return false
	}

	if workerID, ok := assignment["workerId"].(string); ok {
		owned := workerID == ps.workerID
		if !owned {
			ps.logger.Debug(fmt.Sprintf("Partition owned by different worker - partitionId: %d, owner: %s, current: %s",
				partitionID, workerID, ps.workerID))
		}
		return owned
	}

	ps.logger.Debug(fmt.Sprintf("Partition assignment invalid format - partitionId: %d", partitionID))
	return false
}

func (ps *partitionStream) createChangeStreamPipeline(partitionID int) []bson.D {
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
							ps.createHashExpression("$documentKey._id"),
							ps.cfg.Partition.TotalPartition,
						}}},
						partitionID,
					}},
				}},
			}},
		},
	}
}

func (ps *partitionStream) createBootstrapFilter(partitionID int) bson.D {
	return bson.D{
		{Key: "$expr", Value: bson.D{
			{Key: "$eq", Value: bson.A{
				bson.D{{Key: "$mod", Value: bson.A{
					ps.createHashExpression("$_id"),
					ps.cfg.Partition.TotalPartition,
				}}},
				partitionID,
			}},
		}},
	}
}

func (ps *partitionStream) createHashExpression(idField string) bson.D {
	return bson.D{
		{Key: "$cond", Value: bson.D{
			{Key: "if", Value: bson.D{
				{Key: "$or", Value: bson.A{
					bson.D{{Key: "$eq", Value: bson.A{bson.D{{Key: "$type", Value: idField}}, "int"}}},
					bson.D{{Key: "$eq", Value: bson.A{bson.D{{Key: "$type", Value: idField}}, "long"}}},
					bson.D{{Key: "$eq", Value: bson.A{bson.D{{Key: "$type", Value: idField}}, "double"}}},
				}},
			}},
			{Key: "then", Value: idField},
			{Key: "else", Value: bson.D{
				{Key: "$cond", Value: bson.D{
					{Key: "if", Value: ps.createIsNumericStringCheck(idField)},
					{Key: "then", Value: bson.D{{Key: "$toDouble", Value: idField}}},
					{Key: "else", Value: ps.createOptimizedStringHash(idField)},
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

func (ps *partitionStream) createOptimizedStringHash(idField string) bson.D {
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

func (ps *partitionStream) createTypeSafeGreaterThanFilter(fieldName string, lastValue interface{}) bson.D {
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
		ps.logger.Info(fmt.Sprintf("Using string-based comparison for complex type %T (value: %s)", v, valueStr))

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

func (ps *partitionStream) stringToFloat(s string) float64 {
	result := 0.0
	for _, char := range s {
		if char >= '0' && char <= '9' {
			result = result*10 + float64(char-'0')
		}
	}
	return result
}

func (ps *partitionStream) detectNumericStringCollection(ctx context.Context, filter bson.D) (bool, error) {
	const sampleSize = 10
	const minSamplesForConfidence = 3

	ps.logger.Debug(fmt.Sprintf("Detecting collection ID type with sample size %d for partition...", sampleSize))

	opts := options.Find().SetLimit(sampleSize)
	cursor, err := ps.collection.Find(ctx, filter, opts)
	if err != nil {
		return false, fmt.Errorf("koleksiyonu örneklemek için sorgu başarısız oldu: %w", err)
	}
	defer cursor.Close(ctx)

	var docsChecked int
	for cursor.Next(ctx) {
		var doc struct {
			ID interface{} `bson:"_id"`
		}

		if err := cursor.Decode(&doc); err != nil {
			ps.logger.Warn(fmt.Sprintf("failed to decode, r: %v", err))
			continue
		}
		docsChecked++

		idStr, ok := doc.ID.(string)
		if !ok {
			ps.logger.Info(fmt.Sprintf("found different type than a string (tip: %T)", doc.ID))
			return false, nil
		}

		if !ps.isNumericString(idStr) {
			ps.logger.Info(fmt.Sprintf("found different type than a numeric string ('%s')", idStr))
			return false, nil
		}
	}

	if err := cursor.Err(); err != nil {
		return false, fmt.Errorf("failed at cursor: %w", err)
	}

	if docsChecked < minSamplesForConfidence {
		ps.logger.Info(fmt.Sprintf("%d documents were found in the sample, which is below the safe decision threshold of %d. The default sorting will be used.", docsChecked, minSamplesForConfidence))
		return false, nil
	}

	ps.logger.Info(fmt.Sprintf("All %d documents in the sample were verified as numeric strings. Mathematical sorting will be used.", docsChecked))

	return true, nil
}

func (ps *partitionStream) isResumeTokenError(err error) bool {
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
	startTime := time.Now()

	msg, err := message.NewMessage(event)
	if err != nil {
		return err
	}

	ps.updateMetrics(msg.OperationType)

	listenerCtx := &ListenerContext{
		Message:     msg,
		PartitionID: worker.partitionID,
		Ack: func() error {
			processingLatency := time.Since(startTime)
			ps.metric.SetProcessLatency(processingLatency.Nanoseconds())

			if len(resumeToken) > 0 {
				worker.tokenMutex.Lock()
				worker.lastAckedToken = append([]byte(nil), resumeToken...)
				worker.lastClusterTime = &event.ClusterTime
				worker.tokenMutex.Unlock()

				return ps.checkpointManager.SaveResumeToken(
					ps.ctx,
					worker.partitionID,
					resumeToken,
					&event.ClusterTime,
				)
			}
			return nil
		},
	}

	return ps.listener(listenerCtx)
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

func (ps *partitionStream) periodicTokenSave(worker *streamWorker, ticker *time.Ticker) {
	for {
		select {
		case <-worker.ctx.Done():
			return
		case <-ticker.C:
			worker.tokenMutex.RLock()
			token := worker.lastAckedToken
			clusterTime := worker.lastClusterTime
			worker.tokenMutex.RUnlock()

			if len(token) > 0 {
				ctx, cancel := context.WithTimeout(context.Background(), ps.cfg.Checkpoint.SaveTimeout)
				if err := ps.checkpointManager.SaveResumeToken(
					ctx,
					worker.partitionID,
					token,
					clusterTime,
				); err != nil {
					ps.logger.Error(fmt.Sprintf("Failed to save resume token periodically - partitionId: %d, error: %v", worker.partitionID, err))
				}
				cancel()
			}
		}
	}
}

func (ps *partitionStream) Stop(ctx context.Context) error {
	ps.logger.Info("Stopping partition stream")

	if err := ps.partitionManager.ReleasePartitions(ctx); err != nil {
		ps.logger.Error(fmt.Sprintf("Failed to release partitions during shutdown: %v", err))
	}

	if ps.cancel != nil {
		ps.cancel()
	}

	ps.streamsMutex.Lock()
	for partitionID, worker := range ps.activeStreams {
		ps.logger.Info(fmt.Sprintf("Stopping stream worker %d", partitionID))
		worker.cancel()
	}
	ps.streamsMutex.Unlock()

	done := make(chan struct{})
	go func() {
		ps.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		ps.logger.Info("All stream workers stopped")
	case <-time.After(30 * time.Second):
		ps.logger.Warn("Timeout waiting for stream workers to stop")
	}

	if err := ps.partitionManager.Stop(ctx); err != nil {
		ps.logger.Error(fmt.Sprintf("Failed to stop partition manager: %v", err))
		return err
	}

	ps.logger.Info("Partition stream stopped successfully")
	return nil
}
