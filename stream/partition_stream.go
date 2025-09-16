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

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	bootstrapLastID, err := ps.checkpointManager.GetBootstrapProgress(ctx, worker.partitionID)
	if err != nil {
		ps.logger.Error(fmt.Sprintf("Failed to get bootstrap progress - partitionId: %d, error: %v", worker.partitionID, err))
		bootstrapLastID = nil
	}

	filter := ps.createDocumentFilter(worker.partitionID)

	if bootstrapLastID != nil {
		filter = append(filter, bson.E{Key: "_id", Value: bson.M{"$gt": bootstrapLastID}})
		ps.logger.Info(fmt.Sprintf("Resuming bootstrap from saved progress - partitionId: %d, lastId: %v", worker.partitionID, bootstrapLastID))
	} else {
		ps.logger.Info(fmt.Sprintf("Starting fresh bootstrap - partitionId: %d", worker.partitionID))
	}

	opts := options.Find().SetSort(bson.D{{Key: "_id", Value: 1}})
	cursor, err := ps.collection.Find(worker.ctx, filter, opts)
	if err != nil {
		return err
	}
	defer cursor.Close(worker.ctx)

	processedCount := 0
	var lastProcessedID interface{}
	lastCheckpointTime := time.Now()

	bootstrapCompleted := false
	defer func() {
		if !bootstrapCompleted && lastProcessedID != nil {
			ctx, cancel := context.WithTimeout(context.Background(), ps.cfg.Checkpoint.SaveTimeout)
			defer cancel()
			if err := ps.checkpointManager.SaveBootstrapProgress(ctx, worker.partitionID, lastProcessedID); err != nil {
				ps.logger.Error(fmt.Sprintf("Failed to save final bootstrap progress on interruption - partitionId: %d, lastId: %v, error: %v", worker.partitionID, lastProcessedID, err))
			} else {
				ps.logger.Info(fmt.Sprintf("Saved bootstrap progress on interruption - partitionId: %d, lastId: %v, processed: %d", worker.partitionID, lastProcessedID, processedCount))
			}
		}
	}()

	for cursor.Next(worker.ctx) {
		if processedCount > 0 && processedCount%1000 == 0 {
			if !ps.verifyPartitionOwnership(worker.ctx, worker.partitionID) {
				ps.logger.Warn(fmt.Sprintf("Partition ownership lost during bootstrap - partitionId: %d, processed: %d", worker.partitionID, processedCount))
				return fmt.Errorf("partition %d ownership lost during bootstrap", worker.partitionID)
			}
		}

		var document bson.M
		if err := cursor.Decode(&document); err != nil {
			ps.logger.Error(fmt.Sprintf("Error decoding document: %v", err))
			continue
		}

		syntheticEvent := message.ChangeEvent{
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

		if err := ps.processEvent(worker, syntheticEvent, nil); err != nil {
			ps.logger.Error(fmt.Sprintf("Error processing synthetic event - documentId: %v, error: %v", document["_id"], err))
			continue
		}

		processedCount++
		lastProcessedID = document["_id"]

		shouldSaveCheckpoint := false
		timeSinceLastCheckpoint := time.Since(lastCheckpointTime)

		if processedCount%ps.cfg.Checkpoint.BootstrapSaveCount == 0 {
			shouldSaveCheckpoint = true
			ps.logger.Debug(fmt.Sprintf("Bootstrap progress (count-based) - partitionId: %d, processed: %d", worker.partitionID, processedCount))
		} else if timeSinceLastCheckpoint >= ps.cfg.Checkpoint.BootstrapSaveInterval {
			shouldSaveCheckpoint = true
			ps.logger.Debug(fmt.Sprintf("Bootstrap progress (time-based) - partitionId: %d, processed: %d, elapsed: %v", worker.partitionID, processedCount, timeSinceLastCheckpoint))
		}

		if shouldSaveCheckpoint {
			if !ps.verifyPartitionOwnership(worker.ctx, worker.partitionID) {
				ps.logger.Warn(fmt.Sprintf("Partition ownership lost before checkpoint save - partitionId: %d, processed: %d", worker.partitionID, processedCount))
				return fmt.Errorf("partition %d ownership lost before checkpoint", worker.partitionID)
			}

			saveCtx, saveCancel := context.WithTimeout(context.Background(), ps.cfg.Checkpoint.SaveTimeout)
			if err := ps.checkpointManager.SaveBootstrapProgress(saveCtx, worker.partitionID, document["_id"]); err != nil {
				ps.logger.Error(fmt.Sprintf("Failed to save bootstrap progress - partitionId: %d, documentId: %v, error: %v", worker.partitionID, document["_id"], err))
			} else {
				ps.logger.Debug(fmt.Sprintf("Saved bootstrap progress - partitionId: %d, documentId: %v, processed: %d", worker.partitionID, document["_id"], processedCount))
				lastCheckpointTime = time.Now()
			}
			saveCancel()
		}
	}

	if err := cursor.Err(); err != nil {
		return err
	}

	ps.logger.Debug(fmt.Sprintf("Bootstrap completed - partitionId: %d, totalProcessed: %d", worker.partitionID, processedCount))

	bootstrapCompleted = true

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

	pipeline := ps.createPipeline(worker.partitionID)
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

func (ps *partitionStream) createPipeline(partitionID int) []bson.D {
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

func (ps *partitionStream) createDocumentFilter(partitionID int) bson.D {
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
