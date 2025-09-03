package changestream

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Trendyol/go-mongo-cdc/membership"
	"github.com/Trendyol/go-mongo-cdc/mongo/connection"

	"github.com/Trendyol/go-mongo-cdc/config"
	"github.com/Trendyol/go-mongo-cdc/internal/metric"
	"github.com/Trendyol/go-mongo-cdc/mongo/message"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.uber.org/zap"
)

var ErrorStreamInUse = errors.New("change stream is already in use")

type Streamer interface {
	Open(ctx context.Context) error
	Close(ctx context.Context) error
}

type ListenerFunc func(ctx *ListenerContext) error

type ListenerContext struct {
	Message message.Message
	Ack     func() error
}

type stream struct {
	client     connection.Client
	cfg        config.Config
	metric     metric.Metric
	listener   ListenerFunc
	logger     *zap.Logger
	collection connection.Collection
	database   connection.Database
	checkpoint connection.Collection
	isActive   bool

	membership      membership.Membership
	partitionIndex  int
	totalPartitions int

	// Partition version control for race condition prevention
	partitionVersion int64

	streamContext context.Context
	streamCancel  context.CancelFunc

	membershipCollection   connection.Collection
	membershipChangeStream interface{}
	membershipContext      context.Context
	membershipCancel       context.CancelFunc

	partitionMutex sync.RWMutex

	membershipWG sync.WaitGroup

	tokenMutex           sync.RWMutex
	lastAckedToken       []byte
	lastAckedClusterTime *primitive.Timestamp
}

type checkpointInfo struct {
	ID              string              `bson:"_id"`
	ResumeToken     []byte              `bson:"resumeToken"`
	LastRun         primitive.DateTime  `bson:"lastRun"`
	LastClusterTime primitive.Timestamp `bson:"lastClusterTime"`
	PartitionIndex  int                 `bson:"partitionIndex"`
	TotalPartitions int                 `bson:"totalPartitions"`
	ScanLastID      interface{}         `bson:"scanLastId"`
}

func NewStream(
	client connection.Client,
	cfg config.Config,
	metric metric.Metric,
	listener ListenerFunc,
	logger *zap.Logger,
) Streamer {
	database := client.Database(cfg.Database)
	collection := database.Collection(cfg.Collection)
	checkpoint := database.Collection(cfg.Checkpoint.Collection)

	var membershipCollection connection.Collection
	membershipDatabaseName := cfg.Membership.Config["database"]
	membershipCollectionName := cfg.Membership.Config["collection"]
	if !(membershipDatabaseName != "" && membershipCollectionName != "") {
		membershipCollectionName = "cdc_membership"
		membershipDatabaseName = "cdc_cluster"
	}

	membershipDatabase := client.Database(membershipDatabaseName)
	membershipCollection = membershipDatabase.Collection(membershipCollectionName)

	s := &stream{
		client:               client,
		cfg:                  cfg,
		metric:               metric,
		listener:             listener,
		logger:               logger,
		collection:           collection,
		database:             database,
		checkpoint:           checkpoint,
		membershipCollection: membershipCollection,
		isActive:             false,
		partitionIndex:       0,
		totalPartitions:      1,
	}

	membershipConfig := membership.MembershipConfig{
		HeartbeatInterval:  cfg.Membership.HeartbeatInterval,
		HealthCheckTimeout: cfg.Membership.HealthCheckTimeout,
		Config:             cfg.Membership.Config,
	}

	membershipInstance := membership.NewMembership(membershipConfig, client, logger)

	s.membership = membershipInstance

	return s
}

//nolint:funlen
func (s *stream) Open(ctx context.Context) error {
	if s.isActive {
		return ErrorStreamInUse
	}

	s.isActive = true
	defer func() {
		s.isActive = false
	}()

	if err := s.checkReplicaSetStatus(ctx); err != nil {
		return err
	}

	s.streamContext, s.streamCancel = context.WithCancel(ctx)

	s.logger.Info("Starting MongoDB Change Stream",
		zap.String("database", s.cfg.Database),
		zap.String("collection", s.cfg.Collection))

	if s.membership != nil {
		if err := s.membership.Initialize(ctx); err != nil {
			s.logger.Error("Failed to initialize membership", zap.Error(err))
			return err
		}

		if err := s.membership.Start(ctx); err != nil {
			s.logger.Error("Failed to start membership", zap.Error(err))
			return err
		}

		s.logger.Info("Initial delay to allow other members to register...")
		time.Sleep(time.Second * 5)

		if err := s.membership.UpdateMembershipInfo(ctx); err != nil {
			s.logger.Error("Failed to perform post-delay membership update", zap.Error(err))
			return err
		}

		// Wait for membership stability before starting stream
		stabilityDuration := 2 * time.Second
		s.logger.Info("Waiting for membership stability before starting stream",
			zap.Duration("stabilityDuration", stabilityDuration))

		if err := s.membership.WaitForMembershipStability(ctx, stabilityDuration); err != nil {
			s.logger.Error("Failed to wait for membership stability", zap.Error(err))
			return err
		}

		// Get stable partition info with version control
		partitionIndex, totalPartitions, version := s.membership.GetPartitionInfo()

		// Initialize partition state atomically
		s.partitionMutex.Lock()
		s.partitionIndex = partitionIndex
		s.totalPartitions = totalPartitions
		atomic.StoreInt64(&s.partitionVersion, version)
		s.partitionMutex.Unlock()

		s.startMembershipMonitoring(ctx)

		s.logger.Info("membership initialized with stability control",
			zap.Int("partition_index", partitionIndex),
			zap.Int("total_partitions", totalPartitions),
			zap.Int64("partition_version", version),
			zap.String("actual_member_id", s.membership.GetMemberInfo().ID),
		)
	}

	var resumeToken []byte
	var startAtOperationTime *primitive.Timestamp

	cp, err := s.loadCheckpointInfo(ctx)
	if err != nil {
		s.logger.Error("Failed to load checkpoint info", zap.Error(err))
		panic("Failed to load checkpoint info")
	}

	if cp != nil {
		if cp.PartitionIndex == s.partitionIndex && cp.TotalPartitions == s.totalPartitions {
			if len(cp.ResumeToken) > 0 {
				resumeToken = cp.ResumeToken
			}
		} else {
			s.logger.Warn("Checkpoint partition info mismatched; will use lastClusterTime if available",
				zap.Int("saved_partition_index", cp.PartitionIndex),
				zap.Int("saved_total_partitions", cp.TotalPartitions),
				zap.Int("current_partition_index", s.partitionIndex),
				zap.Int("current_total_partitions", s.totalPartitions))
		}

		isTimestampSet := cp.LastClusterTime != primitive.Timestamp{}

		if isTimestampSet {
			ts := cp.LastClusterTime
			ts.I++
			startAtOperationTime = &ts
		}
	}

	// Bootstrap durumunu kontrol et
	shouldProcessDocuments := false

	if cp != nil && cp.ScanLastID != nil {
		s.logger.Info("Continuing incomplete bootstrap from last scan position", zap.Any("lastScanId", cp.ScanLastID))
		shouldProcessDocuments = true
	} else if len(resumeToken) == 0 && startAtOperationTime == nil {
		s.logger.Warn("No usable resume token or lastClusterTime, starting bootstrap process")
		shouldProcessDocuments = true
	}

	if shouldProcessDocuments {
		opTime, opErr := s.getServerOperationTime(ctx)
		if opErr != nil {
			s.logger.Warn("Failed to get server operationTime; starting stream without startAtOperationTime", zap.Error(opErr))
		} else {
			startAtOperationTime = opTime
		}

		if err := s.processAllDocuments(ctx); err != nil {
			s.logger.Error("Failed to process existing documents", zap.Error(err))
			return err
		}

		if startAtOperationTime != nil {
			if err := s.saveBootstrapLastClusterTime(ctx, *startAtOperationTime); err != nil {
				s.logger.Warn("Failed to save bootstrap lastClusterTime", zap.Error(err))
			}
		}
	}

	pipeline := s.createPipeline()

	opts := options.ChangeStream().SetFullDocument(options.UpdateLookup)
	if resumeToken != nil {
		resumeTokenRaw := bson.Raw(resumeToken)
		opts.SetResumeAfter(resumeTokenRaw)
		s.logger.Info("Resuming change stream from stored token")
	} else if startAtOperationTime != nil {
		opts.SetStartAtOperationTime(startAtOperationTime)
		s.logger.Info("Starting change stream from operationTime", zap.Any("operationTime", startAtOperationTime))
	}

	changeStream := s.collection.Watch(s.streamContext, pipeline, opts)
	defer func() {
		if err := changeStream.Close(ctx); err != nil {
			s.logger.Error("Failed to close change stream", zap.Error(err))
		}
	}()

	s.logger.Info("MongoDB Change Stream started successfully")

	tokenSaveTicker := time.NewTicker(s.cfg.Checkpoint.SaveInterval)
	defer tokenSaveTicker.Stop()

	go func() {
		for {
			select {
			case <-tokenSaveTicker.C:
				s.tokenMutex.RLock()
				acked := s.lastAckedToken
				ct := s.lastAckedClusterTime
				s.tokenMutex.RUnlock()
				if len(acked) > 0 {
					if err := s.saveResumeToken(s.streamContext, acked, ct); err != nil {
						s.logger.Error("Failed to periodically save resume token", zap.Error(err))
					}
				}
			case <-s.streamContext.Done():
				return
			}
		}
	}()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-s.streamContext.Done():
			return context.Canceled

		default:
			if !changeStream.Next(s.streamContext) {
				if err := changeStream.Err(); err != nil {
					if !(errors.Is(err, context.Canceled) || strings.Contains(err.Error(), "context canceled")) {
						s.logger.Error("Change stream error", zap.Error(err))
					}

					if s.isResumeTokenError(err) {
						if recErr := s.recoverFromResumeError(ctx); recErr == nil {
							return context.Canceled
						}
					}
					return err
				}
				s.logger.Info("Change stream ended")
				return nil
			}

			var event message.ChangeEvent
			if err := changeStream.Decode(&event); err != nil {
				s.logger.Error("Error decoding change event", zap.Error(err))
				continue
			}

			currentToken := changeStream.ResumeToken()
			if err := s.processEvent(ctx, event, currentToken); err != nil {
				s.logger.Error("Error processing change event",
					zap.String("operationType", event.OperationType),
					zap.Error(err))
				continue
			}

		}
	}
}

func (s *stream) checkReplicaSetStatus(ctx context.Context) error {
	result := s.database.RunCommand(ctx, bson.D{{Key: "isMaster", Value: 1}})

	var isMaster bson.M
	if err := result.Decode(&isMaster); err != nil {
		return err
	}

	if _, ok := isMaster["setName"]; ok {
		s.logger.Info("Connected to MongoDB replica set")
		return nil
	}

	if msg, ok := isMaster["msg"]; ok && msg == "isdbgrid" {
		s.logger.Debug("Connected to MongoDB sharded cluster")
		return nil
	}

	return errors.New(
		"MongoDB is not running as a replica set or sharded cluster. " +
			"Change streams require replica set or sharded cluster",
	)
}

func (s *stream) startMembershipMonitoring(ctx context.Context) {
	if s.membershipCollection == nil {
		s.logger.Debug("Membership collection not configured, skipping membership monitoring")
		return
	}

	if s.membershipCancel != nil {
		s.membershipCancel()
		done := make(chan struct{})
		go func() {
			s.membershipWG.Wait()
			close(done)
		}()
		select {
		case <-done:
		case <-time.After(2 * time.Second):
			s.logger.Warn("Previous membership monitor did not stop in time; proceeding to start new one")
		}
	}

	s.membershipContext, s.membershipCancel = context.WithCancel(ctx)

	s.membershipWG.Add(1)
	go func() {
		defer s.membershipWG.Done()
		s.logger.Info("Starting membership change stream monitoring")

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
		maxRetries := 3

		for {
			select {
			case <-s.membershipContext.Done():
				s.logger.Info("Membership change stream context cancelled")
				return
			default:
				changeStream := s.membershipCollection.Watch(s.membershipContext, pipeline)
				if changeStream == nil {
					s.logger.Error("Failed to create membership change stream")
					retryCount++
					if retryCount >= maxRetries {
						s.logger.Error("Max retries reached for membership change stream")
						return
					}
					time.Sleep(time.Duration(retryCount) * 5 * time.Second)
					continue
				}

				s.membershipChangeStream = changeStream
				retryCount = 0

				for changeStream.Next(s.membershipContext) {
					var changeDoc bson.M
					if err := changeStream.Decode(&changeDoc); err != nil {
						s.logger.Error("Failed to decode membership change stream document", zap.Error(err))
						continue
					}

					operationType := changeDoc["operationType"].(string)
					s.logger.Debug("Membership change detected",
						zap.String("operation", operationType))

					if err := s.membership.UpdateMembershipInfo(s.membershipContext); err != nil {
						s.logger.Error("Failed to update membership info from database", zap.Error(err))
						continue
					}

					partitionChanged, err := s.updatePartitionInfo()
					if err != nil {
						s.logger.Error("Failed to update partition info after membership change", zap.Error(err))
						continue
					}

					if partitionChanged {
						s.logger.Info("Partition changed after membership update, restarting stream")
						if s.streamCancel != nil {
							s.streamCancel()
						}
					}
				}

				// Close change stream with a short timeout to avoid hangs on canceled contexts
				{
					closeCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
					_ = changeStream.Close(closeCtx)
					cancel()
				}

				if s.membershipContext.Err() != nil {
					s.logger.Debug("Membership change stream closed due to shutdown")
					return
				}

				s.logger.Debug("Membership change stream closed, will retry")

				retryCount++
				backoffDuration := time.Duration(retryCount) * 2 * time.Second
				if backoffDuration > 30*time.Second {
					backoffDuration = 30 * time.Second
				}

				select {
				case <-s.membershipContext.Done():
					return
				case <-time.After(backoffDuration):
					continue
				}
			}
		}
	}()
}

func (s *stream) updatePartitionInfo() (bool, error) {
	if s.membership == nil {
		return false, nil
	}

	// Get partition info with version from membership (race-safe)
	newPartitionIndex, newTotalPartitions, membershipVersion := s.membership.GetPartitionInfo()

	// Check if partition assignment is invalid
	if newPartitionIndex < 0 || newTotalPartitions <= 0 {
		s.logger.Error("Invalid partition assignment received",
			zap.Int("partitionIndex", newPartitionIndex),
			zap.Int("totalPartitions", newTotalPartitions),
			zap.Int64("membershipVersion", membershipVersion))
		return false, fmt.Errorf("invalid partition assignment: index=%d, total=%d",
			newPartitionIndex, newTotalPartitions)
	}

	s.partitionMutex.Lock()
	defer s.partitionMutex.Unlock()

	oldIndex := s.partitionIndex
	oldTotal := s.totalPartitions
	oldVersion := atomic.LoadInt64(&s.partitionVersion)

	// Update partition info atomically
	s.partitionIndex = newPartitionIndex
	s.totalPartitions = newTotalPartitions
	atomic.StoreInt64(&s.partitionVersion, membershipVersion)

	// Check if partition assignment changed
	hasChanged := oldIndex != newPartitionIndex ||
		oldTotal != newTotalPartitions ||
		oldVersion != membershipVersion

	if hasChanged {
		s.logger.Info("partition info updated",
			zap.Int("old_index", oldIndex),
			zap.Int("new_index", newPartitionIndex),
			zap.Int("old_total", oldTotal),
			zap.Int("new_total", newTotalPartitions),
			zap.Int64("old_version", oldVersion),
			zap.Int64("new_version", membershipVersion))
		return true, nil
	}

	s.logger.Debug("partition info checked, no changes needed",
		zap.Int("partition_index", newPartitionIndex),
		zap.Int("total_partitions", newTotalPartitions),
		zap.Int64("version", membershipVersion))

	return false, nil
}

func (s *stream) loadCheckpointInfo(ctx context.Context) (*checkpointInfo, error) {
	checkpointID := s.getCheckpointID()
	filter := bson.M{"_id": checkpointID}

	result := s.checkpoint.FindOne(ctx, filter)
	if err := result.Err(); err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			return nil, nil
		}
		return nil, err
	}

	var cp checkpointInfo
	if err := result.Decode(&cp); err != nil {
		return nil, err
	}
	return &cp, nil
}

func (s *stream) getServerOperationTime(ctx context.Context) (*primitive.Timestamp, error) {
	result := s.database.RunCommand(ctx, bson.D{{Key: "isMaster", Value: 1}})
	var isMaster bson.M
	if err := result.Decode(&isMaster); err != nil {
		return nil, err
	}

	if opTime, ok := isMaster["operationTime"].(primitive.Timestamp); ok {
		return &opTime, nil
	}
	return nil, nil
}

func (s *stream) saveBootstrapLastClusterTime(ctx context.Context, ts primitive.Timestamp) error {
	checkpointID := s.getCheckpointID()
	filter := bson.M{"_id": checkpointID}
	update := bson.M{
		"$set": bson.M{
			"lastClusterTime": ts,
			"updatedAt":       time.Now(),
			"database":        s.cfg.Database,
			"collection":      s.cfg.Collection,
			"partitionIndex":  s.partitionIndex,
			"totalPartitions": s.totalPartitions,
		},
	}
	opts := options.Update().SetUpsert(true)
	_, err := s.checkpoint.UpdateOne(ctx, filter, update, opts)
	return err
}

func (s *stream) initBootstrapState(ctx context.Context) error {
	checkpointID := s.getCheckpointID()
	filter := bson.M{"_id": checkpointID}
	update := bson.M{"$set": bson.M{
		"updatedAt":       time.Now(),
		"database":        s.cfg.Database,
		"collection":      s.cfg.Collection,
		"partitionIndex":  s.partitionIndex,
		"totalPartitions": s.totalPartitions,
	}}
	opts := options.Update().SetUpsert(true)
	_, err := s.checkpoint.UpdateOne(ctx, filter, update, opts)
	return err
}

func (s *stream) saveScanProgress(ctx context.Context, lastID interface{}) error {
	checkpointID := s.getCheckpointID()
	filter := bson.M{"_id": checkpointID}
	update := bson.M{"$set": bson.M{
		"scanLastId":      lastID,
		"updatedAt":       time.Now(),
		"database":        s.cfg.Database,
		"collection":      s.cfg.Collection,
		"partitionIndex":  s.partitionIndex,
		"totalPartitions": s.totalPartitions,
	}}
	opts := options.Update().SetUpsert(true)
	_, err := s.checkpoint.UpdateOne(ctx, filter, update, opts)
	return err
}

func (s *stream) clearBootstrapState(ctx context.Context) error {
	checkpointID := s.getCheckpointID()
	filter := bson.M{"_id": checkpointID}
	update := bson.M{
		"$unset": bson.M{
			"scanLastId": "",
		},
		"$set": bson.M{
			"updatedAt": time.Now(),
		},
	}
	_, err := s.checkpoint.UpdateOne(ctx, filter, update, options.Update())
	return err
}

//nolint:funlen
func (s *stream) processAllDocuments(ctx context.Context) error {
	s.logger.Info("Starting to process all existing documents as insert events")

	// Bootstrap başlangıcını kaydet
	if err := s.initBootstrapState(ctx); err != nil {
		s.logger.Error("Failed to init bootstrap state", zap.Error(err))
		return err
	}

	cp, _ := s.loadCheckpointInfo(ctx)

	var filter bson.D
	if s.membership != nil && s.totalPartitions > 1 {
		filter = s.createDocumentFilter()
	} else {
		filter = bson.D{}
	}

	//TODO: random string veya uuid v4 icin dogru calısmıyor
	if cp != nil && cp.ScanLastID != nil {
		filter = append(filter, bson.E{Key: "_id", Value: bson.M{"$gt": cp.ScanLastID}})
	}

	cursor, err := s.collection.Find(ctx, filter)
	if err != nil {
		s.logger.Error("Failed to create cursor for bootstrap scan", zap.Error(err))
		return err
	}
	defer func() {
		if err := cursor.Close(ctx); err != nil {
			s.logger.Error("Failed to close cursor", zap.Error(err))
		}
	}()

	processedCount := 0
	for cursor.Next(ctx) {
		var document bson.M
		if err := cursor.Decode(&document); err != nil {
			s.logger.Error("Error decoding document", zap.Error(err))
			continue
		}

		syntheticEvent := message.ChangeEvent{
			OperationType: "insert",
			DocumentKey: message.DocumentKey{
				ID: document["_id"],
			},
			FullDocument: document,
			Namespace: message.Namespace{
				Database:   s.cfg.Database,
				Collection: s.cfg.Collection,
			},
			ClusterTime: primitive.Timestamp{T: uint32(time.Now().Unix()), I: 1}, // #nosec G115
		}

		if err := s.processEvent(ctx, syntheticEvent, nil); err != nil {
			s.logger.Error("Error processing synthetic insert event",
				zap.Any("documentId", document["_id"]),
				zap.Error(err))
			continue
		}

		processedCount++

		if processedCount%1000 == 0 {
			s.logger.Info("Processed documents", zap.Int("count", processedCount))
			if err := s.saveScanProgress(ctx, document["_id"]); err != nil {
				s.logger.Error("Failed to save scan progress", zap.Error(err))
			}
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
	}

	if err := cursor.Err(); err != nil {
		return err
	}

	s.logger.Info("Completed processing all existing documents",
		zap.Int("totalProcessed", processedCount))

	if err := s.clearBootstrapState(ctx); err != nil {
		s.logger.Error("Failed to clear bootstrap state", zap.Error(err))
		return err
	}

	return nil
}

func (s *stream) createPipeline() []bson.D {
	basePipeline := s.createBasePipeline()

	if s.membership != nil && s.totalPartitions > 1 {
		return s.createVersionAwarePartitionPipeline(basePipeline)
	}

	return basePipeline
}

func (s *stream) createBasePipeline() []bson.D {
	return []bson.D{
		{
			{Key: "$match", Value: bson.D{
				{Key: "operationType", Value: bson.D{
					{Key: "$in", Value: bson.A{"insert", "update", "delete", "replace"}},
				}},
			}},
		},
	}
}

func (s *stream) createVersionAwarePartitionPipeline(basePipeline []bson.D) []bson.D {
	// Capture current partition state atomically
	s.partitionMutex.RLock()
	currentIndex := s.partitionIndex
	currentTotal := s.totalPartitions
	currentVersion := atomic.LoadInt64(&s.partitionVersion)
	s.partitionMutex.RUnlock()

	// Validate partition assignment before creating pipeline
	if !s.membership.IsPartitionValid(currentVersion, currentIndex, currentTotal) {
		s.logger.Warn("Partition assignment became invalid during pipeline creation",
			zap.Int("currentIndex", currentIndex),
			zap.Int("currentTotal", currentTotal),
			zap.Int64("currentVersion", currentVersion))

		// Retry with fresh partition info
		newIndex, newTotal, newVersion := s.membership.GetPartitionInfo()

		s.partitionMutex.Lock()
		s.partitionIndex = newIndex
		s.totalPartitions = newTotal
		atomic.StoreInt64(&s.partitionVersion, newVersion)
		s.partitionMutex.Unlock()

		currentIndex = newIndex
		currentTotal = newTotal
		currentVersion = newVersion

		s.logger.Info("Updated partition info during pipeline creation",
			zap.Int("newIndex", newIndex),
			zap.Int("newTotal", newTotal),
			zap.Int64("newVersion", newVersion))
	}

	return s.createSimplePartitionPipelineWithState(basePipeline, currentIndex, currentTotal)
}

func (s *stream) createSimplePartitionPipelineWithState(basePipeline []bson.D, partitionIndex, totalPartitions int) []bson.D {
	if s.membership == nil || totalPartitions <= 1 {
		return basePipeline
	}

	if s.cfg.Membership.ChunkBased {
		shardKey := s.cfg.Membership.Config["shardKey"]
		if shardKey == "" {
			s.logger.Warn("shard key not configured for chunk-based partitioning")
			return s.createHashBasedPartitionPipelineWithState(basePipeline, partitionIndex, totalPartitions)
		}

		chunkRanges := s.getChunkRangesForPartition(partitionIndex, totalPartitions)
		if len(chunkRanges) == 0 {
			s.logger.Warn("no chunk ranges found for this partition, falling back to hash-based partitioning")
			return s.createHashBasedPartitionPipelineWithState(basePipeline, partitionIndex, totalPartitions)
		}

		return s.createChunkBasedPartitionPipeline(basePipeline, shardKey, chunkRanges)
	}

	s.logger.Info("using hash-based partitioning",
		zap.Int("partitionIndex", partitionIndex),
		zap.Int("totalPartitions", totalPartitions))
	return s.createHashBasedPartitionPipelineWithState(basePipeline, partitionIndex, totalPartitions)
}

// createHashBasedPartitionPipelineWithState creates hash-based pipeline with explicit state
func (s *stream) createHashBasedPartitionPipelineWithState(basePipeline []bson.D, partitionIndex, totalPartitions int) []bson.D {
	// Document ID'sine göre hash-based partitioning
	// ObjectID string'inin uzunluğunu hesaplayıp son 2 karakteri alıyoruz
	hashBasedFilter := bson.D{
		{Key: "$match", Value: bson.D{
			{Key: "$expr", Value: bson.D{
				{Key: "$eq", Value: bson.A{
					bson.D{{Key: "$mod", Value: bson.A{
						s.createHexToIntExpression("$documentKey._id"),
						totalPartitions,
					}}},
					partitionIndex,
				}},
			}},
		}},
	}

	result := append(basePipeline, hashBasedFilter)

	return result
}

func (s *stream) createHexToIntExpression(idField string) bson.D {
	// Improved algorithm that works with any string, not just hexadecimal
	// Try different approaches based on configuration or fallback strategy

	// First, try MongoDB's native $toHashedIndexKey (MongoDB 4.4+)
	if s.cfg.Membership.Config["useToHashedIndexKey"] == "true" {
		return s.createToHashedIndexKeyExpression(idField)
	}

	// Try $abs with $toHashedIndexKey approach (as suggested by teammate)
	if s.cfg.Membership.Config["useAbsHashedIndexKey"] == "true" {
		return s.createAbsToHashedIndexKeyExpression(idField)
	}

	// Try the simple CRC32-like approach
	if s.cfg.Membership.Config["useCRC32Hash"] == "true" {
		return s.createCRC32LikeExpression(idField)
	}

	// Fallback to enhanced string hashing for any characters
	return s.createEnhancedStringHashExpression(idField)
}

// createToHashedIndexKeyExpression uses MongoDB's native $toHashedIndexKey operator
func (s *stream) createToHashedIndexKeyExpression(idField string) bson.D {
	return bson.D{
		{Key: "$toHashedIndexKey", Value: bson.D{
			{Key: "$toString", Value: idField},
		}},
	}
}

// createAbsToHashedIndexKeyExpression uses $abs with $toHashedIndexKey (as suggested by teammate)
func (s *stream) createAbsToHashedIndexKeyExpression(idField string) bson.D {
	return bson.D{
		{Key: "$abs", Value: bson.D{
			{Key: "$toHashedIndexKey", Value: bson.D{
				{Key: "$toString", Value: idField},
			}},
		}},
	}
}

// createCRC32LikeExpression creates a simple CRC32-like hash for any string
func (s *stream) createCRC32LikeExpression(idField string) bson.D {
	// Simple polynomial hash that works with any characters
	// This approach is more reliable than the complex character mapping
	idStr := bson.D{{Key: "$toString", Value: idField}}

	return bson.D{
		{Key: "$add", Value: bson.A{
			// Hash based on string length (prime multiplier)
			bson.D{{Key: "$multiply", Value: bson.A{
				bson.D{{Key: "$strLenCP", Value: idStr}},
				31,
			}}},
			// Hash based on first character's position in alphabet (if any)
			bson.D{{Key: "$cond", Value: bson.D{
				{Key: "if", Value: bson.D{{Key: "$gt", Value: bson.A{
					bson.D{{Key: "$strLenCP", Value: idStr}},
					0,
				}}}},
				{Key: "then", Value: bson.D{{Key: "$multiply", Value: bson.A{
					bson.D{{Key: "$indexOfCP", Value: bson.A{
						"0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ-_:.",
						bson.D{{Key: "$substr", Value: bson.A{idStr, 0, 1}}},
					}}},
					37,
				}}}},
				{Key: "else", Value: 0},
			}}},
			// Hash based on last character's position (if different from first)
			bson.D{{Key: "$cond", Value: bson.D{
				{Key: "if", Value: bson.D{{Key: "$gt", Value: bson.A{
					bson.D{{Key: "$strLenCP", Value: idStr}},
					1,
				}}}},
				{Key: "then", Value: bson.D{{Key: "$multiply", Value: bson.A{
					bson.D{{Key: "$indexOfCP", Value: bson.A{
						"0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ-_:.",
						bson.D{{Key: "$substr", Value: bson.A{
							idStr,
							bson.D{{Key: "$subtract", Value: bson.A{
								bson.D{{Key: "$strLenCP", Value: idStr}},
								1,
							}}},
							1,
						}}},
					}}},
					41,
				}}}},
				{Key: "else", Value: 0},
			}}},
		}},
	}
}

// createEnhancedStringHashExpression creates a hash that works with any string characters
func (s *stream) createEnhancedStringHashExpression(idField string) bson.D {
	// Use a combination of string operations to create a better distributed hash
	// This approach works with any characters, not just hexadecimal

	return bson.D{
		{Key: "$add", Value: bson.A{
			// Hash based on last 4 characters for better distribution
			bson.D{{Key: "$multiply", Value: bson.A{
				s.createCharCodeSum(bson.D{
					{Key: "$substr", Value: bson.A{
						bson.D{{Key: "$toString", Value: idField}},
						bson.D{{Key: "$max", Value: bson.A{
							0,
							bson.D{{Key: "$subtract", Value: bson.A{
								bson.D{{Key: "$strLenCP", Value: bson.D{{Key: "$toString", Value: idField}}}},
								4,
							}}},
						}}},
						4,
					}},
				}),
				31, // Prime number for better distribution
			}}},
			// Add string length as additional entropy
			bson.D{{Key: "$strLenCP", Value: bson.D{{Key: "$toString", Value: idField}}}},
		}},
	}
}

// createCharCodeSum calculates a simple hash based on string content
func (s *stream) createCharCodeSum(strExpr bson.D) bson.D {
	// Simplified approach: use string length and first/last character positions
	// This avoids complex operations that might not work well in aggregation pipeline

	strLen := bson.D{{Key: "$strLenCP", Value: strExpr}}

	return bson.D{
		{Key: "$add", Value: bson.A{
			// Base hash from string length
			bson.D{{Key: "$multiply", Value: bson.A{strLen, 17}}}, // 17 is a prime
			// Add hash from first character (if exists)
			bson.D{{Key: "$cond", Value: bson.D{
				{Key: "if", Value: bson.D{{Key: "$gt", Value: bson.A{strLen, 0}}}},
				{Key: "then", Value: bson.D{{Key: "$multiply", Value: bson.A{
					s.createSimpleCharHash(bson.D{
						{Key: "$substr", Value: bson.A{strExpr, 0, 1}},
					}),
					23, // Another prime
				}}}},
				{Key: "else", Value: 0},
			}}},
			// Add hash from last character (if exists and different from first)
			bson.D{{Key: "$cond", Value: bson.D{
				{Key: "if", Value: bson.D{{Key: "$gt", Value: bson.A{strLen, 1}}}},
				{Key: "then", Value: bson.D{{Key: "$multiply", Value: bson.A{
					s.createSimpleCharHash(bson.D{
						{Key: "$substr", Value: bson.A{
							strExpr,
							bson.D{{Key: "$subtract", Value: bson.A{strLen, 1}}},
							1,
						}},
					}),
					29, // Another prime
				}}}},
				{Key: "else", Value: 0},
			}}},
		}},
	}
}

// createSimpleCharHash creates a simple hash for a single character
func (s *stream) createSimpleCharHash(charExpr bson.D) bson.D {
	// Use a simplified character mapping that works for any character
	// Map common characters to different values for better distribution
	return bson.D{
		{Key: "$switch", Value: bson.D{
			{Key: "branches", Value: bson.A{
				// Numbers
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
				// Hex letters
				bson.D{{Key: "case", Value: bson.D{{Key: "$eq", Value: bson.A{charExpr, "a"}}}}, {Key: "then", Value: 10}},
				bson.D{{Key: "case", Value: bson.D{{Key: "$eq", Value: bson.A{charExpr, "b"}}}}, {Key: "then", Value: 11}},
				bson.D{{Key: "case", Value: bson.D{{Key: "$eq", Value: bson.A{charExpr, "c"}}}}, {Key: "then", Value: 12}},
				bson.D{{Key: "case", Value: bson.D{{Key: "$eq", Value: bson.A{charExpr, "d"}}}}, {Key: "then", Value: 13}},
				bson.D{{Key: "case", Value: bson.D{{Key: "$eq", Value: bson.A{charExpr, "e"}}}}, {Key: "then", Value: 14}},
				bson.D{{Key: "case", Value: bson.D{{Key: "$eq", Value: bson.A{charExpr, "f"}}}}, {Key: "then", Value: 15}},
				bson.D{{Key: "case", Value: bson.D{{Key: "$eq", Value: bson.A{charExpr, "A"}}}}, {Key: "then", Value: 16}},
				bson.D{{Key: "case", Value: bson.D{{Key: "$eq", Value: bson.A{charExpr, "B"}}}}, {Key: "then", Value: 17}},
				bson.D{{Key: "case", Value: bson.D{{Key: "$eq", Value: bson.A{charExpr, "C"}}}}, {Key: "then", Value: 18}},
				bson.D{{Key: "case", Value: bson.D{{Key: "$eq", Value: bson.A{charExpr, "D"}}}}, {Key: "then", Value: 19}},
				bson.D{{Key: "case", Value: bson.D{{Key: "$eq", Value: bson.A{charExpr, "E"}}}}, {Key: "then", Value: 20}},
				bson.D{{Key: "case", Value: bson.D{{Key: "$eq", Value: bson.A{charExpr, "F"}}}}, {Key: "then", Value: 21}},
				// Common characters in IDs
				bson.D{{Key: "case", Value: bson.D{{Key: "$eq", Value: bson.A{charExpr, "-"}}}}, {Key: "then", Value: 22}},
				bson.D{{Key: "case", Value: bson.D{{Key: "$eq", Value: bson.A{charExpr, "_"}}}}, {Key: "then", Value: 23}},
				bson.D{{Key: "case", Value: bson.D{{Key: "$eq", Value: bson.A{charExpr, ":"}}}}, {Key: "then", Value: 24}},
				bson.D{{Key: "case", Value: bson.D{{Key: "$eq", Value: bson.A{charExpr, "."}}}}, {Key: "then", Value: 25}},
				// Letters for language codes like "tr-TR"
				bson.D{{Key: "case", Value: bson.D{{Key: "$eq", Value: bson.A{charExpr, "T"}}}}, {Key: "then", Value: 26}},
				bson.D{{Key: "case", Value: bson.D{{Key: "$eq", Value: bson.A{charExpr, "R"}}}}, {Key: "then", Value: 27}},
				bson.D{{Key: "case", Value: bson.D{{Key: "$eq", Value: bson.A{charExpr, "E"}}}}, {Key: "then", Value: 28}},
				bson.D{{Key: "case", Value: bson.D{{Key: "$eq", Value: bson.A{charExpr, "N"}}}}, {Key: "then", Value: 29}},
				bson.D{{Key: "case", Value: bson.D{{Key: "$eq", Value: bson.A{charExpr, "U"}}}}, {Key: "then", Value: 30}},
				bson.D{{Key: "case", Value: bson.D{{Key: "$eq", Value: bson.A{charExpr, "S"}}}}, {Key: "then", Value: 31}},
			}},
			// For any other character, use a hash based on string comparison
			{Key: "default", Value: bson.D{{Key: "$mod", Value: bson.A{
				bson.D{{Key: "$strLenCP", Value: charExpr}},
				37, // Prime number for better distribution
			}}}},
		}},
	}
}

// Legacy hex-based algorithm (kept for backward compatibility)
func (s *stream) createLegacyHexToIntExpression(idField string) bson.D {
	// ObjectID'nin son 2 karakterini hexadecimal'den integer'a çevir
	// Hex karakterler: 0-9, a-f, A-F
	// Her karakteri ayrı ayrı değerlendirip 16 tabanında hesaplama yap

	// örnek: objectId'sinin son 2 karakteri c7 olan bir id icin 16lik tabana ceviriyor (12 * 16) + 7 = 199
	// sonrasında toplam pod sayısıyla modluyor ornegin 10 pod olsun 199 % 10 = 9 bu index degerine sahip pod işleyecek sadece

	//Örnek bir ID'nin sonu ...c7 olsun:
	//$toString -> ID'yi "....c7" string'ine çevirir.
	//$strLenCP -> String'in uzunluğunu bulur (diyelim ki 24).
	//$subtract -> 24 - 2 = 22 sonucunu bulur.
	//$max -> max(0, 22) işlemini yapar, sonuç 22 olur.
	//$substr -> String'in 22. indeksinden başlayarak 1 karakter alır. Bu karakter c'dir.
	//createSingleHexCharToInt -> Aldığı "c" karakterini sayısal değeri olan 12'ye çevirir.
	//$multiply -> Sonuç olarak 12 sayısını 16 ile çarpar ve 192 değerini üretir.
	return bson.D{
		{Key: "$add", Value: bson.A{
			bson.D{{Key: "$multiply", Value: bson.A{
				s.createSingleHexCharToInt(bson.D{
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
			s.createSingleHexCharToInt(bson.D{
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

func (s *stream) createSingleHexCharToInt(charExpr bson.D) bson.D {
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

func (s *stream) createChunkBasedPartitionPipeline(basePipeline []bson.D, shardKey string, chunkRanges []ChunkRange) []bson.D {
	var orConditions []bson.D
	for _, chunkRange := range chunkRanges {
		var minVal, maxVal interface{}

		if minDoc, ok := chunkRange.Min.(bson.M); ok {
			minVal = minDoc[shardKey]
		} else {
			if primitiveMin, ok := chunkRange.Min.(primitive.M); ok {
				minVal = primitiveMin[shardKey]
			} else {
				s.logger.Error("unsupported chunk range min type",
					zap.Any("min", chunkRange.Min),
					zap.String("type", fmt.Sprintf("%T", chunkRange.Min)))
				continue
			}
		}

		if maxDoc, ok := chunkRange.Max.(bson.M); ok {
			maxVal = maxDoc[shardKey]
		} else {
			if primitiveMax, ok := chunkRange.Max.(primitive.M); ok {
				maxVal = primitiveMax[shardKey]
			} else {
				s.logger.Error("unsupported chunk range max type",
					zap.Any("max", chunkRange.Max),
					zap.String("type", fmt.Sprintf("%T", chunkRange.Max)))
				continue
			}
		}

		condition := bson.D{
			{Key: "$and", Value: bson.A{
				bson.D{{Key: "fullDocument." + shardKey, Value: bson.D{{Key: "$gte", Value: minVal}}}},
				bson.D{{Key: "fullDocument." + shardKey, Value: bson.D{{Key: "$lt", Value: maxVal}}}},
			}},
		}
		orConditions = append(orConditions, condition)
	}

	if len(orConditions) > 0 {
		chunkFilter := bson.D{
			{Key: "$match", Value: bson.D{
				{Key: "$or", Value: orConditions},
			}},
		}
		basePipeline = append(basePipeline, chunkFilter)
	}

	s.logger.Info("chunk-based partitioning enabled",
		zap.String("shard_key", shardKey),
		zap.Int("partition_index", s.partitionIndex),
		zap.Int("total_partitions", s.totalPartitions),
		zap.Int("chunk_ranges", len(chunkRanges)),
		zap.Any("generated_pipeline", basePipeline),
		zap.Any("chunk_ranges_detail", chunkRanges),
	)

	return basePipeline
}

//nolint:funlen
func (s *stream) processEvent(ctx context.Context, event message.ChangeEvent, resumeTokenAtEvent []byte) error {
	startTime := time.Now()

	// Check partition validity before processing event
	if s.membership != nil {
		s.partitionMutex.RLock()
		currentIndex := s.partitionIndex
		currentTotal := s.totalPartitions
		currentVersion := atomic.LoadInt64(&s.partitionVersion)
		s.partitionMutex.RUnlock()

		// Validate current partition assignment
		if !s.membership.IsPartitionValid(currentVersion, currentIndex, currentTotal) {
			s.logger.Warn("Partition assignment became invalid during event processing",
				zap.Int("currentIndex", currentIndex),
				zap.Int("currentTotal", currentTotal),
				zap.Int64("currentVersion", currentVersion),
				zap.String("eventType", event.OperationType))

			// This will trigger stream restart via membership change detection
			panic("partition assignment invalid during event processing")
		}
	}

	msg, err := message.NewMessage(event)
	if err != nil {
		return err
	}

	s.updateMetrics(msg.OperationType)

	listenerCtx := &ListenerContext{
		Message: msg,
		Ack: func() error {
			processingLatency := time.Since(startTime)
			s.metric.SetProcessLatency(processingLatency.Nanoseconds())
			if len(resumeTokenAtEvent) > 0 {
				s.tokenMutex.Lock()
				s.lastAckedToken = append([]byte(nil), resumeTokenAtEvent...)
				ct := event.ClusterTime
				s.lastAckedClusterTime = &ct
				s.tokenMutex.Unlock()
				if err := s.saveResumeToken(ctx, resumeTokenAtEvent, &ct); err != nil {
					s.logger.Error("Failed to save resume token on ack", zap.Error(err))
				}
			}
			return nil
		},
	}

	if err := s.listener(listenerCtx); err != nil {
		return err
	}

	return nil
}

func (s *stream) updateMetrics(opType message.OperationType) {
	switch opType {
	case message.OperationInsert:
		s.metric.IncInsertTotal()
	case message.OperationUpdate:
		s.metric.IncUpdateTotal()
	case message.OperationDelete:
		s.metric.IncDeleteTotal()
	case message.OperationReplace:
		s.metric.IncInsertTotal()
	}
}

func (s *stream) saveResumeToken(ctx context.Context, token []byte, clusterTime *primitive.Timestamp) error {
	if len(token) == 0 {
		s.logger.Debug("Resume token is empty, skipping save")
		return nil
	}

	checkpointID := s.getCheckpointID()

	filter := bson.M{"_id": checkpointID}
	set := bson.M{
		"resumeToken":     token,
		"lastRun":         time.Now(),
		"updatedAt":       time.Now(),
		"database":        s.cfg.Database,
		"collection":      s.cfg.Collection,
		"partitionIndex":  s.partitionIndex,
		"totalPartitions": s.totalPartitions,
	}
	if clusterTime != nil {
		set["lastClusterTime"] = *clusterTime
	}
	update := bson.M{"$set": set}

	opts := options.Update().SetUpsert(true)
	_, err := s.checkpoint.UpdateOne(ctx, filter, update, opts)

	if err != nil {
		s.logger.Error("Failed to save resume token",
			zap.String("checkpoint_id", checkpointID),
			zap.Error(err))
		return err
	}

	s.logger.Debug("Resume token saved successfully",
		zap.String("checkpoint_id", checkpointID))

	return nil
}

func (s *stream) getCheckpointID() string {
	return s.cfg.Database + "_" + s.cfg.Collection + "_checkpoint"
}

func (s *stream) createDocumentFilter() bson.D {
	if s.cfg.Membership.ChunkBased {
		shardKey := s.cfg.Membership.Config["shardKey"]
		if shardKey == "" {
			return s.createHashBasedDocumentFilter()
		}

		chunkRanges := s.getChunkRanges()
		if len(chunkRanges) == 0 {
			return s.createHashBasedDocumentFilter()
		}

		return s.createChunkBasedDocumentFilter(shardKey, chunkRanges)
	}

	return s.createHashBasedDocumentFilter()
}

func (s *stream) createHashBasedDocumentFilter() bson.D {
	// Document query'si için optimize edilmiş hash-based filtering
	// ID'nin son 2 karakterini hex'den int'e çevirerek partition'lara dağıtıyoruz
	return bson.D{
		{Key: "$expr", Value: bson.D{
			{Key: "$eq", Value: bson.A{
				bson.D{{Key: "$mod", Value: bson.A{
					s.createHexToIntExpression("$_id"),
					s.totalPartitions,
				}}},
				s.partitionIndex,
			}},
		}},
	}
}

type ChunkRange struct {
	Min interface{} `bson:"min"`
	Max interface{} `bson:"max"`
}

func (s *stream) createChunkBasedDocumentFilter(shardKey string, chunkRanges []ChunkRange) bson.D {
	var orConditions []bson.D
	for _, chunkRange := range chunkRanges {
		var minVal, maxVal interface{}

		if minDoc, ok := chunkRange.Min.(bson.M); ok {
			minVal = minDoc[shardKey]
		} else if primitiveMin, ok := chunkRange.Min.(primitive.M); ok {
			minVal = primitiveMin[shardKey]
		} else {
			continue
		}

		if maxDoc, ok := chunkRange.Max.(bson.M); ok {
			maxVal = maxDoc[shardKey]
		} else if primitiveMax, ok := chunkRange.Max.(primitive.M); ok {
			maxVal = primitiveMax[shardKey]
		} else {
			continue
		}

		condition := bson.D{
			{Key: "$and", Value: bson.A{
				bson.D{{Key: shardKey, Value: bson.D{{Key: "$gte", Value: minVal}}}},
				bson.D{{Key: shardKey, Value: bson.D{{Key: "$lt", Value: maxVal}}}},
			}},
		}
		orConditions = append(orConditions, condition)
	}

	if len(orConditions) > 0 {
		return bson.D{
			{Key: "$or", Value: orConditions},
		}
	}

	return bson.D{}
}

func (s *stream) getChunkRanges() []ChunkRange {
	s.partitionMutex.RLock()
	currentIndex := s.partitionIndex
	currentTotal := s.totalPartitions
	s.partitionMutex.RUnlock()

	return s.getChunkRangesForPartition(currentIndex, currentTotal)
}

func (s *stream) getChunkRangesForPartition(partitionIndex, totalPartitions int) []ChunkRange {
	chunks, err := s.getAllChunks()
	if err != nil {
		s.logger.Error("failed to get chunks", zap.Error(err))
		return nil
	}

	var assignedChunks []ChunkRange
	for i, chunk := range chunks {
		if i%totalPartitions == partitionIndex {
			assignedChunks = append(assignedChunks, chunk)
		}
	}

	// Final summary with chunk ranges
	var rangeSummary []string
	for _, chunk := range assignedChunks {
		if minDoc, ok := chunk.Min.(bson.M); ok {
			if maxDoc, ok := chunk.Max.(bson.M); ok {
				rangeSummary = append(rangeSummary, fmt.Sprintf("sellerId: %v → %v",
					minDoc["sellerId"], maxDoc["sellerId"]))
			}
		}
	}

	s.logger.Debug("Chunk ranges assigned for partition",
		zap.Int("partitionIndex", partitionIndex),
		zap.Int("totalPartitions", totalPartitions),
		zap.Int("assignedChunks", len(assignedChunks)),
		zap.Strings("rangeSummary", rangeSummary))

	return assignedChunks
}

func (s *stream) getAllChunks() ([]ChunkRange, error) {
	configDB := s.client.Database("config")
	chunksCollection := configDB.Collection("chunks")

	collectionUUID, err := s.getCollectionUUID()
	if err != nil {
		s.logger.Warn("failed to get collection UUID, trying ns filter", zap.Error(err))
	}

	var filter bson.D
	if collectionUUID != nil {
		filter = bson.D{
			{Key: "uuid", Value: collectionUUID},
		}
		s.logger.Info("querying chunks collection with UUID",
			zap.String("namespace", s.cfg.Database+"."+s.cfg.Collection),
			zap.Any("uuid", collectionUUID))
	} else {
		filter = bson.D{
			{Key: "ns", Value: s.cfg.Database + "." + s.cfg.Collection},
		}
		s.logger.Info("querying chunks collection with namespace",
			zap.String("namespace", s.cfg.Database+"."+s.cfg.Collection),
			zap.Any("filter", filter))
	}

	cursor, err := chunksCollection.Find(context.Background(), filter)
	if err != nil {
		s.logger.Error("failed to query chunks collection", zap.Error(err))
		return nil, err
	}
	defer cursor.Close(context.Background())

	var chunks []ChunkRange
	for cursor.Next(context.Background()) {
		var chunkDoc bson.M
		if err := cursor.Decode(&chunkDoc); err != nil {
			continue
		}

		if minVal, ok := chunkDoc["min"]; ok {
			if maxVal, ok := chunkDoc["max"]; ok {
				chunks = append(chunks, ChunkRange{
					Min: minVal,
					Max: maxVal,
				})
			}
		}
	}

	return chunks, nil
}

func (s *stream) getCollectionUUID() (interface{}, error) {
	configDB := s.client.Database("config")
	collectionsCol := configDB.Collection("collections")

	filter := bson.D{
		{Key: "_id", Value: s.cfg.Database + "." + s.cfg.Collection},
	}

	var result bson.M
	err := collectionsCol.FindOne(context.Background(), filter).Decode(&result)
	if err != nil {
		return nil, err
	}

	uuid, exists := result["uuid"]
	if !exists {
		return nil, fmt.Errorf("uuid field not found in collection metadata")
	}

	s.logger.Info("found collection UUID",
		zap.String("namespace", s.cfg.Database+"."+s.cfg.Collection),
		zap.Any("uuid", uuid))

	return uuid, nil
}

func (s *stream) Close(ctx context.Context) error {
	s.logger.Info("Closing MongoDB Change Stream")

	if s.streamCancel != nil {
		s.streamCancel()
	}

	s.stopMembershipMonitoring()
	done := make(chan struct{})
	go func() {
		s.membershipWG.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-ctx.Done():
		s.logger.Warn("Timeout waiting for membership monitoring to stop")
	}

	s.isActive = false

	if s.membership != nil {
		if err := s.membership.Stop(ctx); err != nil {
			s.logger.Error("Failed to stop membership", zap.Error(err))
		}
	}

	s.logger.Info("MongoDB Change Stream closed")
	return nil
}

func (s *stream) stopMembershipMonitoring() {
	if s.membershipCancel != nil {
		s.membershipCancel()
	}
	s.membershipChangeStream = nil
}

func (s *stream) isResumeTokenError(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	if strings.Contains(msg, "resume token") ||
		strings.Contains(msg, "ChangeStreamFatalError") ||
		strings.Contains(msg, "PlanExecutor") {
		return true
	}
	return false
}

func (s *stream) recoverFromResumeError(ctx context.Context) error {
	s.logger.Warn("Attempting to recover from resume token error")

	checkpointID := s.cfg.Database + "_" + s.cfg.Collection + "_checkpoint"
	filter := bson.M{"_id": checkpointID}

	var cp struct {
		ResumeToken     []byte               `bson:"resumeToken"`
		LastClusterTime *primitive.Timestamp `bson:"lastClusterTime"`
	}

	res := s.checkpoint.FindOne(ctx, filter)
	if res != nil && res.Err() == nil {
		_ = res.Decode(&cp)
	}

	if len(cp.ResumeToken) > 0 {
		_, _ = s.checkpoint.UpdateOne(ctx, filter, bson.M{"$unset": bson.M{"resumeToken": ""}}, options.Update())
	}

	if cp.LastClusterTime != nil {
		s.logger.Info("Will try to restart from lastClusterTime", zap.Any("lastClusterTime", cp.LastClusterTime))
		s.tokenMutex.Lock()
		s.lastAckedClusterTime = cp.LastClusterTime
		s.tokenMutex.Unlock()
		return nil
	}

	s.logger.Info("No lastClusterTime found; fallback to server operationTime or full scan on next start")
	return nil
}
