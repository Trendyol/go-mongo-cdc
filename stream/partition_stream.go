package stream

import (
	"context"
	"errors"
	"fmt"
	"hash/fnv"
	"strconv"
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

	// Manager'dan partition değişikliklerini dinle ve stream'leri güncelle
	ps.partitionManager.SetPartitionsChangedCallback(ps.updateStreams)

	// Initial partition assignment
	if err := ps.refreshPartitions(); err != nil {
		ps.logger.Error("Failed to acquire initial partitions", zap.Error(err))
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

func (ps *partitionStream) refreshPartitions() error {
	newPartitions, err := ps.partitionManager.AcquirePartitions(ps.ctx)
	if err != nil {
		return err
	}

	// Manager'dan gelen partition listesine göre stream'leri güncelle
	ps.updateStreams(newPartitions)
	return nil
}

func (ps *partitionStream) updateStreams(newPartitions []int) {
	ps.streamsMutex.Lock()
	defer ps.streamsMutex.Unlock()

	// Stop streams for partitions we no longer own
	for partitionID, worker := range ps.activeStreams {
		found := false
		for _, p := range newPartitions {
			if p == partitionID {
				found = true
				break
			}
		}

		if !found {
			ps.logger.Info("Stopping stream for partition", zap.Int("partitionId", partitionID))
			worker.cancel()
			delete(ps.activeStreams, partitionID)
		}
	}

	// Start streams for new partitions
	for _, partitionID := range newPartitions {
		if _, exists := ps.activeStreams[partitionID]; !exists {
			ps.logger.Info("Starting stream for partition", zap.Int("partitionId", partitionID))

			worker := &streamWorker{
				partitionID: partitionID,
			}
			worker.ctx, worker.cancel = context.WithCancel(ps.ctx)

			ps.activeStreams[partitionID] = worker

			ps.wg.Add(1)
			go ps.runPartitionStream(worker)
		}
	}

	ps.logger.Debug("Active partitions updated",
		zap.Int("count", len(ps.activeStreams)),
		zap.Ints("partitions", newPartitions))
}

func (ps *partitionStream) runPartitionStream(worker *streamWorker) {
	defer ps.wg.Done()

	for {
		select {
		case <-worker.ctx.Done():
			return
		default:
			err := ps.processPartitionStream(worker)
			if err == nil {
				ps.logger.Info("Partition stream completed normally",
					zap.Int("partitionId", worker.partitionID))
				return
			}

			if errors.Is(err, context.Canceled) {
				ps.logger.Info("Partition stream cancelled",
					zap.Int("partitionId", worker.partitionID))
				return
			}

			ps.logger.Error("Partition stream error, retrying",
				zap.Int("partitionId", worker.partitionID),
				zap.Error(err))

			time.Sleep(5 * time.Second)
		}
	}
}

func (ps *partitionStream) processPartitionStream(worker *streamWorker) error {
	resumeToken, startAtOperationTime, err := ps.prepareStreamStart(worker.partitionID)
	if err != nil {
		return err
	}

	shouldBootstrap := false
	if resumeToken == nil && startAtOperationTime == nil {
		ps.logger.Debug("No resume token or cluster time found, starting bootstrap",
			zap.Int("partitionId", worker.partitionID))
		shouldBootstrap = true

		opTime, err := ps.getServerOperationTime(worker.ctx)
		if err == nil && opTime != nil {
			startAtOperationTime = opTime
		}
	}

	if shouldBootstrap {
		if err := ps.bootstrapPartition(worker); err != nil {
			return fmt.Errorf("bootstrap failed: %w", err)
		}

		if startAtOperationTime != nil {
			if err := ps.checkpointManager.SaveBootstrapClusterTime(worker.ctx, worker.partitionID, *startAtOperationTime); err != nil {
				ps.logger.Warn("Failed to save bootstrap cluster time",
					zap.Int("partitionId", worker.partitionID),
					zap.Error(err))
			}
		}
	}

	pipeline := ps.createPipeline(worker.partitionID)
	opts := options.ChangeStream().SetFullDocument(options.UpdateLookup)

	if resumeToken != nil {
		opts.SetResumeAfter(bson.Raw(resumeToken))
		ps.logger.Info("Resuming from token", zap.Int("partitionId", worker.partitionID))
	} else if startAtOperationTime != nil {
		opts.SetStartAtOperationTime(startAtOperationTime)
		ps.logger.Debug("Starting from operation time",
			zap.Int("partitionId", worker.partitionID),
			zap.Any("operationTime", startAtOperationTime))
	}

	changeStream, err := ps.collection.Watch(worker.ctx, pipeline, opts)
	if err != nil {
		return err
	}
	defer changeStream.Close(worker.ctx)

	worker.stream = changeStream

	tokenSaveTicker := time.NewTicker(ps.cfg.Checkpoint.SaveInterval)
	defer tokenSaveTicker.Stop()

	go ps.periodicTokenSave(worker, tokenSaveTicker)

	for changeStream.Next(worker.ctx) {
		var event message.ChangeEvent
		if err := changeStream.Decode(&event); err != nil {
			ps.logger.Error("Error decoding change event",
				zap.Int("partitionId", worker.partitionID),
				zap.Error(err))
			continue
		}

		currentToken := changeStream.ResumeToken()
		if err := ps.processEvent(worker, event, currentToken); err != nil {
			ps.logger.Error("Error processing event",
				zap.Int("partitionId", worker.partitionID),
				zap.String("operationType", event.OperationType),
				zap.Error(err))
			continue
		}
	}

	if err := changeStream.Err(); err != nil {
		return err
	}

	return nil
}

func (ps *partitionStream) prepareStreamStart(partitionID int) ([]byte, *primitive.Timestamp, error) {
	resumeToken, clusterTime, err := ps.checkpointManager.GetResumeToken(ps.ctx, partitionID)
	if err != nil {
		return nil, nil, err
	}

	// Check if we have incomplete bootstrap
	bootstrapLastID, err := ps.checkpointManager.GetBootstrapProgress(ps.ctx, partitionID)
	if err != nil {
		return nil, nil, err
	}

	if bootstrapLastID != nil {
		ps.logger.Info("Found incomplete bootstrap, will continue",
			zap.Int("partitionId", partitionID),
			zap.Any("lastId", bootstrapLastID))
		return nil, nil, nil
	}

	return resumeToken, clusterTime, nil
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

func (ps *partitionStream) bootstrapPartition(worker *streamWorker) error {
	ps.logger.Debug("Starting bootstrap for partition", zap.Int("partitionId", worker.partitionID))

	bootstrapLastID, _ := ps.checkpointManager.GetBootstrapProgress(ps.ctx, worker.partitionID)

	var filter bson.D
	if ps.cfg.Partition.RuntimeFiltering {
		filter = bson.D{}
	} else {
		filter = ps.createDocumentFilter(worker.partitionID)
	}

	if bootstrapLastID != nil {
		filter = append(filter, bson.E{Key: "_id", Value: bson.M{"$gt": bootstrapLastID}})
	}

	cursor, err := ps.collection.Find(worker.ctx, filter)
	if err != nil {
		return err
	}
	defer cursor.Close(worker.ctx)

	processedCount := 0
	for cursor.Next(worker.ctx) {
		var document bson.M
		if err := cursor.Decode(&document); err != nil {
			ps.logger.Error("Error decoding document", zap.Error(err))
			continue
		}

		if ps.cfg.Partition.RuntimeFiltering {
			calculatedPartition := ps.calculateRuntimePartition(document["_id"])
			if calculatedPartition != worker.partitionID {
				continue
			}
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
			ps.logger.Error("Error processing synthetic event",
				zap.Any("documentId", document["_id"]),
				zap.Error(err))
			continue
		}

		processedCount++

		if processedCount%1000 == 0 {
			ps.logger.Info("Bootstrap progress",
				zap.Int("partitionId", worker.partitionID),
				zap.Int("processed", processedCount))

			if err := ps.checkpointManager.SaveBootstrapProgress(worker.ctx, worker.partitionID, document["_id"]); err != nil {
				ps.logger.Error("Failed to save bootstrap progress", zap.Error(err))
			}
		}
	}

	if err := cursor.Err(); err != nil {
		return err
	}

	ps.logger.Debug("Bootstrap completed",
		zap.Int("partitionId", worker.partitionID),
		zap.Int("totalProcessed", processedCount))

	return ps.checkpointManager.ClearBootstrapProgress(worker.ctx, worker.partitionID)
}

func (ps *partitionStream) createPipeline(partitionID int) []bson.D {
	pipeline := []bson.D{
		{
			{Key: "$match", Value: bson.D{
				{Key: "operationType", Value: bson.D{
					{Key: "$in", Value: bson.A{"insert", "update", "delete", "replace"}},
				}},
			}},
		},
	}

	if !ps.cfg.Partition.RuntimeFiltering {
		pipeline = append(pipeline, bson.D{
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
		})
	}

	return pipeline
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
					{Key: "else", Value: ps.createStringHashExpression(idField)},
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

func (ps *partitionStream) createStringHashExpression(idField string) bson.D {
	// Hash using last 3 characters for better distribution with 1000 partitions
	return bson.D{
		{Key: "$add", Value: bson.A{
			bson.D{{Key: "$multiply", Value: bson.A{
				ps.createSingleHexCharToInt(bson.D{
					{Key: "$substr", Value: bson.A{
						bson.D{{Key: "$toString", Value: idField}},
						bson.D{{Key: "$max", Value: bson.A{
							0,
							bson.D{{Key: "$subtract", Value: bson.A{
								bson.D{{Key: "$strLenCP", Value: bson.D{{Key: "$toString", Value: idField}}}},
								3,
							}}},
						}}},
						1,
					}},
				}),
				256, // 16^2
			}}},
			bson.D{{Key: "$multiply", Value: bson.A{
				ps.createSingleHexCharToInt(bson.D{
					{Key: "$substr", Value: bson.A{
						bson.D{{Key: "$toString", Value: idField}},
						bson.D{{Key: "$max", Value: bson.A{
							0,
							bson.D{{Key: "$subtract", Value: bson.A{
								bson.D{{Key: "$strLenCP", Value: bson.D{{Key: "$toString", Value: idField}}}},
								2,
							}}},
						}}},
						1,
					}},
				}),
				16,
			}}},
			ps.createSingleHexCharToInt(bson.D{
				{Key: "$substr", Value: bson.A{
					bson.D{{Key: "$toString", Value: idField}},
					bson.D{{Key: "$max", Value: bson.A{
						0,
						bson.D{{Key: "$subtract", Value: bson.A{
							bson.D{{Key: "$strLenCP", Value: bson.D{{Key: "$toString", Value: idField}}}},
							1,
						}}},
					}}},
					1,
				}},
			}),
		}},
	}
}

func (ps *partitionStream) createSingleHexCharToInt(charExpr bson.D) bson.D {
	return bson.D{
		{Key: "$switch", Value: bson.D{
			{Key: "branches", Value: bson.A{
				bson.D{{Key: "case", Value: bson.D{{Key: "$eq", Value: bson.A{charExpr, "0"}}}}, {Key: "then", Value: 0}},
				bson.D{{Key: "case", Value: bson.D{{Key: "$eq", Value: bson.A{charExpr, "1"}}}}, {Key: "then", Value: 1}},
				bson.D{{Key: "case", Value: bson.D{{Key: "$eq", Value: bson.A{charExpr, "2"}}}}, {Key: "then", Value: 2}},
				bson.D{{Key: "case", Value: bson.D{{Key: "$eq", Value: bson.A{charExpr, "3"}}}}, {Key: "then", Value: 3}},
				bson.D{{Key: "case", Value: bson.D{{Key: "$eq", Value: bson.A{charExpr, "4"}}}}, {Key: "then", Value: 4}},
				bson.D{{Key: "case", Value: bson.D{{Key: "$eq", Value: bson.A{charExpr, "5"}}}}, {Key: "then", Value: 5}},
				bson.D{{Key: "case", Value: bson.D{{Key: "$eq", Value: bson.A{charExpr, "6"}}}}, {Key: "then", Value: 6}},
				bson.D{{Key: "case", Value: bson.D{{Key: "$eq", Value: bson.A{charExpr, "7"}}}}, {Key: "then", Value: 7}},
				bson.D{{Key: "case", Value: bson.D{{Key: "$eq", Value: bson.A{charExpr, "8"}}}}, {Key: "then", Value: 8}},
				bson.D{{Key: "case", Value: bson.D{{Key: "$eq", Value: bson.A{charExpr, "9"}}}}, {Key: "then", Value: 9}},
				bson.D{{Key: "case", Value: bson.D{{Key: "$eq", Value: bson.A{charExpr, "a"}}}}, {Key: "then", Value: 10}},
				bson.D{{Key: "case", Value: bson.D{{Key: "$eq", Value: bson.A{charExpr, "b"}}}}, {Key: "then", Value: 11}},
				bson.D{{Key: "case", Value: bson.D{{Key: "$eq", Value: bson.A{charExpr, "c"}}}}, {Key: "then", Value: 12}},
				bson.D{{Key: "case", Value: bson.D{{Key: "$eq", Value: bson.A{charExpr, "d"}}}}, {Key: "then", Value: 13}},
				bson.D{{Key: "case", Value: bson.D{{Key: "$eq", Value: bson.A{charExpr, "e"}}}}, {Key: "then", Value: 14}},
				bson.D{{Key: "case", Value: bson.D{{Key: "$eq", Value: bson.A{charExpr, "f"}}}}, {Key: "then", Value: 15}},
				bson.D{{Key: "case", Value: bson.D{{Key: "$eq", Value: bson.A{charExpr, "A"}}}}, {Key: "then", Value: 10}},
				bson.D{{Key: "case", Value: bson.D{{Key: "$eq", Value: bson.A{charExpr, "B"}}}}, {Key: "then", Value: 11}},
				bson.D{{Key: "case", Value: bson.D{{Key: "$eq", Value: bson.A{charExpr, "C"}}}}, {Key: "then", Value: 12}},
				bson.D{{Key: "case", Value: bson.D{{Key: "$eq", Value: bson.A{charExpr, "D"}}}}, {Key: "then", Value: 13}},
				bson.D{{Key: "case", Value: bson.D{{Key: "$eq", Value: bson.A{charExpr, "E"}}}}, {Key: "then", Value: 14}},
				bson.D{{Key: "case", Value: bson.D{{Key: "$eq", Value: bson.A{charExpr, "F"}}}}, {Key: "then", Value: 15}},
			}},
			{Key: "default", Value: 0},
		}},
	}
}

func (ps *partitionStream) calculateRuntimePartition(documentID interface{}) int {
	var hashValue uint64

	switch id := documentID.(type) {
	case int:
		hashValue = uint64(id)
	case int32:
		hashValue = uint64(id)
	case int64:
		hashValue = uint64(id)
	case float64:
		hashValue = uint64(id)
	case string:
		if numVal, err := strconv.ParseFloat(id, 64); err == nil {
			hashValue = uint64(numVal)
		} else {
			hashValue = ps.calculateStringHash(id)
		}
	case primitive.ObjectID:
		hashValue = ps.calculateStringHash(id.Hex())
	default:
		idStr := fmt.Sprintf("%v", id)
		hashValue = ps.calculateStringHash(idStr)
	}

	return int(hashValue % uint64(ps.cfg.Partition.TotalPartition))
}

func (ps *partitionStream) calculateStringHash(s string) uint64 {
	if len(s) < 3 {
		h := fnv.New64a()
		h.Write([]byte(s))
		return h.Sum64()
	}

	lastThreeChars := s[len(s)-3:]
	var hashValue uint64

	for i, char := range lastThreeChars {
		charValue := ps.hexCharToInt(byte(char))
		multiplier := uint64(1)
		for j := 0; j < 2-i; j++ {
			multiplier *= 16
		}
		hashValue += uint64(charValue) * multiplier
	}

	return hashValue
}

func (ps *partitionStream) hexCharToInt(char byte) int {
	switch {
	case char >= '0' && char <= '9':
		return int(char - '0')
	case char >= 'a' && char <= 'f':
		return int(char - 'a' + 10)
	case char >= 'A' && char <= 'F':
		return int(char - 'A' + 10)
	default:
		return 0
	}
}

func (ps *partitionStream) processEvent(worker *streamWorker, event message.ChangeEvent, resumeToken []byte) error {
	startTime := time.Now()

	if !ps.shouldProcessEvent(event, worker.partitionID) {
		return nil
	}

	msg, err := message.NewMessage(event)
	if err != nil {
		return err
	}

	ps.updateMetrics(msg.OperationType)

	/*// Double-check partition assignment during event processing
	ps.streamsMutex.RLock()
	isStillAssigned := false
	for partitionID := range ps.activeStreams {
		if partitionID == worker.partitionID {
			isStillAssigned = true
			break
		}
	}
	ps.streamsMutex.RUnlock()

	if !isStillAssigned {
		ps.logger.Warn("Received event for unassigned partition - this should not happen!",
			zap.Int("partitionId", worker.partitionID),
			zap.String("operation", string(msg.OperationType)),
			zap.Any("documentId", msg.DocumentID),
			zap.Ints("activePartitions", func() []int {
				ps.streamsMutex.RLock()
				defer ps.streamsMutex.RUnlock()
				partitions := make([]int, 0, len(ps.activeStreams))
				for pid := range ps.activeStreams {
					partitions = append(partitions, pid)
				}
				return partitions
			}()))
		return nil
	}*/

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

func (ps *partitionStream) shouldProcessEvent(event message.ChangeEvent, partitionID int) bool {
	if !ps.cfg.Partition.RuntimeFiltering {
		return true
	}

	calculatedPartition := ps.calculateRuntimePartition(event.DocumentKey.ID)
	return calculatedPartition == partitionID
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
				if err := ps.checkpointManager.SaveResumeToken(
					ps.ctx,
					worker.partitionID,
					token,
					clusterTime,
				); err != nil {
					ps.logger.Error("Failed to save resume token periodically",
						zap.Int("partitionId", worker.partitionID),
						zap.Error(err))
				}
			}
		}
	}
}

func (ps *partitionStream) Stop(ctx context.Context) error {
	ps.logger.Info("Stopping partition stream")

	if ps.cancel != nil {
		ps.cancel()
	}

	ps.streamsMutex.Lock()
	for partitionID, worker := range ps.activeStreams {
		ps.logger.Info("Stopping stream worker", zap.Int("partitionId", partitionID))
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
		ps.logger.Error("Failed to stop partition manager", zap.Error(err))
		return err
	}

	ps.logger.Info("Partition stream stopped successfully")
	return nil
}
