package changestream

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/Trendyol/go-mongo-cdc/membership"
	"github.com/Trendyol/go-mongo-cdc/mongo/connection"

	"github.com/Trendyol/go-mongo-cdc/config"
	"github.com/Trendyol/go-mongo-cdc/internal/metric"
	"github.com/Trendyol/go-mongo-cdc/mongo/message"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.uber.org/zap"
)

var ErrorStreamInUse = errors.New("change stream is already in use")

type Streamer interface {
	Open(ctx context.Context) error
	Close(ctx context.Context) error
	Rebalance() error
}

type ListenerFunc func(ctx *ListenerContext)

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

	// Rebalance için gerekli
	rebalanceMutex sync.Mutex
	rebalanceTimer *time.Timer
	rebalanceDelay time.Duration
	isRebalancing  bool

	// Change stream management
	currentChangeStream interface{}
	streamContext       context.Context
	streamCancel        context.CancelFunc
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

	s := &stream{
		client:          client,
		cfg:             cfg,
		metric:          metric,
		listener:        listener,
		logger:          logger,
		collection:      collection,
		database:        database,
		checkpoint:      checkpoint,
		isActive:        false,
		partitionIndex:  0,
		totalPartitions: 1,
		rebalanceDelay:  5 * time.Second, // Configurable yapılabilir
		isRebalancing:   false,
	}

	logger.Info("Checking membership configuration",
		zap.Bool("enabled", cfg.Membership.Enabled),
		zap.String("type", cfg.Membership.Type),
		zap.Int("member_number", cfg.Membership.MemberNumber),
		zap.Int("total_members", cfg.Membership.TotalMembers))

	if cfg.Membership.Enabled {
		membershipConfig := membership.MembershipConfig{
			Type:               membership.MembershipType(cfg.Membership.Type),
			MemberID:           cfg.Membership.MemberID,
			MemberNumber:       cfg.Membership.MemberNumber,
			TotalMembers:       cfg.Membership.TotalMembers,
			HeartbeatInterval:  cfg.Membership.HeartbeatInterval,
			HealthCheckTimeout: cfg.Membership.HealthCheckTimeout,
			Config:             cfg.Membership.Config,
		}

		membershipInstance, err := membership.NewMembership(membershipConfig, client, logger)
		if err != nil {
			logger.Error("Failed to create membership instance", zap.Error(err))
			return s
		}

		s.membership = membershipInstance
	}

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
		s.stopRebalanceTimer()
	}()

	// Stream context'ini ayarla
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

		s.membership.SetChangeCallback(s.onMembershipChange)

		activeMembers := s.membership.GetMembershipInfo().Members
		s.totalPartitions = len(activeMembers)

		actualMemberID := s.membership.GetMemberInfo().ID

		for i, member := range activeMembers {
			if member.ID == actualMemberID {
				s.partitionIndex = i
				break
			}
		}

		s.logger.Info("membership initialized",
			zap.Int("partition_index", s.partitionIndex),
			zap.Int("total_partitions", s.totalPartitions),
			zap.String("actual_member_id", actualMemberID),
		)
	}

	if err := s.checkReplicaSetStatus(ctx); err != nil {
		return err
	}

	resumeToken, err := s.loadResumeToken(ctx)
	if err != nil || resumeToken == nil {
		s.logger.Warn("Resume token not found or failed to load, processing all existing documents", zap.Error(err))

		if err := s.processAllDocuments(ctx); err != nil {
			s.logger.Error("Failed to process existing documents", zap.Error(err))
			return err
		}
	}

	pipeline := s.createPipeline()

	opts := options.ChangeStream().SetFullDocument(options.UpdateLookup)
	if resumeToken != nil {
		resumeTokenRaw := bson.Raw(resumeToken)
		opts.SetResumeAfter(resumeTokenRaw)
		s.logger.Info("Resuming change stream from stored token")
	}

	changeStream := s.collection.Watch(s.streamContext, pipeline, opts)
	s.currentChangeStream = changeStream
	defer func() {
		if err := changeStream.Close(ctx); err != nil {
			s.logger.Error("Failed to close change stream", zap.Error(err))
		}
	}()

	s.logger.Info("MongoDB Change Stream started successfully")

	tokenSaveTicker := time.NewTicker(s.cfg.Checkpoint.SaveInterval)
	defer tokenSaveTicker.Stop()

	saveTokenChan := make(chan struct{})
	go func() {
		for {
			select {
			case <-tokenSaveTicker.C:
				saveTokenChan <- struct{}{}
			case <-ctx.Done():
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
		case <-saveTokenChan:
			if changeStream.ResumeToken() != nil {
				if err := s.saveResumeToken(ctx, changeStream.ResumeToken()); err != nil {
					s.logger.Error("Failed to periodically save resume token", zap.Error(err))
				}
			}
		default:
			if !changeStream.Next(s.streamContext) {
				if err := changeStream.Err(); err != nil {
					s.logger.Error("Change stream error", zap.Error(err))
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

			if err := s.processEvent(ctx, event); err != nil {
				s.logger.Error("Error processing change event",
					zap.String("operationType", event.OperationType),
					zap.Error(err))
				continue
			}

			if err := s.saveResumeToken(ctx, changeStream.ResumeToken()); err != nil {
				s.logger.Error("Failed to save resume token after event", zap.Error(err))
			}
		}
	}
}

func (s *stream) Close(ctx context.Context) error {
	s.logger.Info("Closing MongoDB Change Stream")

	// Rebalance timer'ını durdur
	s.stopRebalanceTimer()

	// Stream context'ini iptal et
	if s.streamCancel != nil {
		s.streamCancel()
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

func (s *stream) createPipeline() []bson.D {
	basePipeline := s.createBasePipeline()

	if s.membership != nil && s.totalPartitions > 1 {
		return s.createSimplePartitionPipeline(basePipeline)
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
		s.logger.Info("Connected to MongoDB sharded cluster")
		return nil
	}

	return errors.New(
		"MongoDB is not running as a replica set or sharded cluster. " +
			"Change streams require replica set or sharded cluster",
	)
}

//nolint:funlen
func (s *stream) processEvent(_ context.Context, event message.ChangeEvent) error {
	startTime := time.Now()

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
			return nil
		},
	}

	s.listener(listenerCtx)
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

func (s *stream) saveResumeToken(ctx context.Context, token []byte) error {
	if len(token) == 0 {
		s.logger.Debug("Resume token is empty, skipping save")
		return nil
	}

	checkpointID := s.cfg.Database + "_" + s.cfg.Collection + "_checkpoint"

	filter := bson.M{"_id": checkpointID}
	update := bson.M{
		"$set": bson.M{
			"resumeToken": token,
			"lastRun":     time.Now(),
			"updatedAt":   time.Now(),
			"database":    s.cfg.Database,
			"collection":  s.cfg.Collection,
		},
	}

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

func (s *stream) loadResumeToken(ctx context.Context) ([]byte, error) {
	checkpointID := s.cfg.Database + "_" + s.cfg.Collection + "_checkpoint"

	filter := bson.M{"_id": checkpointID}
	result := s.checkpoint.FindOne(ctx, filter)

	if result.Err() != nil {
		if result.Err().Error() == "mongo: no documents in result" {
			s.logger.Info("No resume token found, starting from scratch")
			return nil, nil
		}
		return nil, result.Err()
	}

	var checkpoint struct {
		ID          string             `bson:"_id"`
		ResumeToken []byte             `bson:"resumeToken"`
		LastRun     primitive.DateTime `bson:"lastRun"`
	}

	if err := result.Decode(&checkpoint); err != nil {
		return nil, err
	}

	s.logger.Info("Resume token loaded successfully",
		zap.String("checkpoint_id", checkpoint.ID),
		zap.Time("lastRun", checkpoint.LastRun.Time()))

	return checkpoint.ResumeToken, nil
}

//nolint:funlen
func (s *stream) processAllDocuments(ctx context.Context) error {
	s.logger.Info("Starting to process all existing documents as insert events")

	var filter bson.D
	if s.membership != nil && s.totalPartitions > 1 {
		filter = s.createDocumentFilter()
	} else {
		filter = bson.D{}
	}

	cursor, err := s.collection.Find(ctx, filter)
	if err != nil {
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

		if err := s.processEvent(ctx, syntheticEvent); err != nil {
			s.logger.Error("Error processing synthetic insert event",
				zap.Any("documentId", document["_id"]),
				zap.Error(err))
			continue
		}

		processedCount++

		if processedCount%1000 == 0 {
			s.logger.Info("Processed documents", zap.Int("count", processedCount))
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

	return nil
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
	// Document query için hash-based filtering
	// ObjectID'nin son karakterlerini kullanarak document'ları partition'lara dağıtıyoruz
	return bson.D{
		{Key: "$expr", Value: bson.D{
			{Key: "$eq", Value: bson.A{
				bson.D{{Key: "$mod", Value: bson.A{
					bson.D{{Key: "$toInt", Value: bson.D{
						{Key: "$substr", Value: bson.A{
							bson.D{{Key: "$toString", Value: "$_id"}},
							bson.D{{Key: "$subtract", Value: bson.A{
								bson.D{{Key: "$strLenCP", Value: bson.D{{Key: "$toString", Value: "$_id"}}}},
								2,
							}}},
							2,
						}},
					}}},
					s.totalPartitions,
				}}},
				s.partitionIndex,
			}},
		}},
	}
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

func (s *stream) onMembershipChange(oldInfo, newInfo membership.MembershipInfo) {
	// Actual member ID'yi membership'ten al
	actualMemberID := s.membership.GetMemberInfo().ID

	s.logger.Info("membership change detected",
		zap.Any("active_members", newInfo.Members),
		zap.String("actual_member_id", actualMemberID),
	)

	oldPartitionIndex := s.partitionIndex
	oldTotalPartitions := s.totalPartitions

	s.totalPartitions = len(newInfo.Members)

	// Kendimizi bul ve partition index'i ayarla
	for i, member := range newInfo.Members {
		if member.ID == actualMemberID {
			s.partitionIndex = i
			break
		}
	}

	if s.isActive && (oldPartitionIndex != s.partitionIndex || oldTotalPartitions != s.totalPartitions) {
		s.logger.Info("partition assignment changed, triggering rebalance",
			zap.Int("old_partition", oldPartitionIndex),
			zap.Int("new_partition", s.partitionIndex),
			zap.Int("old_total", oldTotalPartitions),
			zap.Int("new_total", s.totalPartitions),
		)

		// Rebalance trigger - context olarak arka planda çalışan context'i kullan
		go func() {
			if err := s.Rebalance(); err != nil {
				s.logger.Error("Failed to trigger rebalance", zap.Error(err))
			}
		}()
	}
}

func (s *stream) createSimplePartitionPipeline(basePipeline []bson.D) []bson.D {
	if s.membership == nil || s.totalPartitions <= 1 {
		return basePipeline
	}

	if s.cfg.Membership.ChunkBased {
		shardKey := s.cfg.Membership.Config["shardKey"]
		if shardKey == "" {
			s.logger.Warn("shard key not configured for chunk-based partitioning")
			return s.createHashBasedPartitionPipeline(basePipeline)
		}

		chunkRanges := s.getChunkRanges()
		if len(chunkRanges) == 0 {
			s.logger.Warn("no chunk ranges found for this partition, falling back to hash-based partitioning")
			return s.createHashBasedPartitionPipeline(basePipeline)
		}

		return s.createChunkBasedPartitionPipeline(basePipeline, shardKey, chunkRanges)
	}

	s.logger.Info("using hash-based partitioning")
	return s.createHashBasedPartitionPipeline(basePipeline)
}

func (s *stream) createHashBasedPartitionPipeline(basePipeline []bson.D) []bson.D {
	// Document ID'sine göre hash-based partitioning
	// ObjectID string'inin uzunluğunu hesaplayıp son 2 karakteri alıyoruz
	hashBasedFilter := bson.D{
		{Key: "$match", Value: bson.D{
			{Key: "$expr", Value: bson.D{
				{Key: "$eq", Value: bson.A{
					bson.D{{Key: "$mod", Value: bson.A{
						bson.D{{Key: "$toInt", Value: bson.D{
							{Key: "$substr", Value: bson.A{
								bson.D{{Key: "$toString", Value: "$documentKey._id"}},
								bson.D{{Key: "$subtract", Value: bson.A{
									bson.D{{Key: "$strLenCP", Value: bson.D{{Key: "$toString", Value: "$documentKey._id"}}}},
									2,
								}}},
								2,
							}},
						}}},
						s.totalPartitions,
					}}},
					s.partitionIndex,
				}},
			}},
		}},
	}

	result := append(basePipeline, hashBasedFilter)

	return result
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

type ChunkRange struct {
	Min interface{} `bson:"min"`
	Max interface{} `bson:"max"`
}

func (s *stream) getChunkRanges() []ChunkRange {
	shardKey := s.cfg.Membership.Config["shardKey"]
	if shardKey == "" {
		return nil
	}

	chunks, err := s.getAllChunks()
	if err != nil {
		s.logger.Error("failed to get chunks", zap.Error(err))
		return nil
	}

	var assignedChunks []ChunkRange
	for i, chunk := range chunks {
		if i%s.totalPartitions == s.partitionIndex {
			assignedChunks = append(assignedChunks, chunk)
		}
	}

	return assignedChunks
}

func (s *stream) getAllChunks() ([]ChunkRange, error) {
	configDB := s.client.Database("config")
	chunksCollection := configDB.Collection("chunks")

	filter := bson.D{
		{Key: "ns", Value: s.cfg.Database + "." + s.cfg.Collection},
	}

	cursor, err := chunksCollection.Find(context.Background(), filter)
	if err != nil {
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

func (s *stream) GetMembership() membership.Membership {
	return s.membership
}

// Rebalance performs a smooth rebalancing operation
func (s *stream) Rebalance() error {
	s.rebalanceMutex.Lock()
	defer s.rebalanceMutex.Unlock()

	// Eğer zaten rebalancing yapılıyorsa, timer'ı reset et
	if s.isRebalancing && s.rebalanceTimer != nil {
		if s.rebalanceTimer.Stop() {
			s.rebalanceTimer.Reset(s.rebalanceDelay)
			s.logger.Info("rebalance timer reset due to another rebalance request")
		} else {
			s.rebalanceTimer = time.AfterFunc(s.rebalanceDelay, func() {
				s.performRebalance()
			})
			s.logger.Info("new rebalance timer scheduled")
		}
		return nil
	}

	s.logger.Info("starting rebalance operation")
	s.isRebalancing = true

	// Dynamic membership için delay yok, diğerleri için delay var
	delay := s.rebalanceDelay
	if s.membership != nil && s.cfg.Membership.Type == "dynamic" {
		delay = 0
		s.logger.Info("dynamic membership detected, skipping rebalance delay")
	}

	s.rebalanceTimer = time.AfterFunc(delay, func() {
		s.performRebalance()
	})

	if delay > 0 {
		s.logger.Info("rebalance will start after delay", zap.Duration("delay", delay))
	}

	return nil
}

// performRebalance does the actual rebalancing work
func (s *stream) performRebalance() {
	s.logger.Info("performing rebalance operation",
		zap.Int("old_partition_index", s.partitionIndex),
		zap.Int("new_total_partitions", s.totalPartitions))

	// Önce mevcut stream'i gracefully kapat
	if s.streamCancel != nil {
		s.logger.Info("closing current change stream for rebalance")
		s.streamCancel()

		// Biraz bekle ki stream düzgün kapansın
		time.Sleep(100 * time.Millisecond)
	}

	s.isRebalancing = false

	// Pipeline yeniden oluşturulacak, stream restart gerekiyor
	// Bu sadece change stream'in sonlanmasına neden olur
	// Open() loop'unda context.Canceled dönünce stream yeniden başlatılır
	s.logger.Info("rebalance completed, change stream will restart with new pipeline",
		zap.Int("partition_index", s.partitionIndex),
		zap.Int("total_partitions", s.totalPartitions))
}

// stopRebalanceTimer rebalance timer'ını durdurur
func (s *stream) stopRebalanceTimer() {
	if s.rebalanceTimer != nil {
		s.rebalanceTimer.Stop()
		s.rebalanceTimer = nil
	}
}
