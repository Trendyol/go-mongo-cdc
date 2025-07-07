package changestream

import (
	"context"
	"errors"
	"fmt"
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
	defer func() { s.isActive = false }()

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
		s.logger.Info("Membership change callback registered")

		s.logger.Info("waiting for membership discovery to complete")
		time.Sleep(3 * time.Second) // TODO: How to wait them?

		membershipInfo := s.membership.GetMembershipInfo()
		activeMembers := s.getActiveMembers(membershipInfo.Members)
		s.totalPartitions = len(activeMembers)

		for i, member := range activeMembers {
			if member.ID == s.cfg.Membership.MemberID {
				s.partitionIndex = i
				break
			}
		}
		s.logger.Info("membership initialized",
			zap.Int("partition_index", s.partitionIndex),
			zap.Int("total_partitions", s.totalPartitions),
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

	changeStream := s.collection.Watch(ctx, pipeline, opts)
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
		case <-saveTokenChan:
			if changeStream.ResumeToken() != nil {
				if err := s.saveResumeToken(ctx, changeStream.ResumeToken()); err != nil {
					s.logger.Error("Failed to periodically save resume token", zap.Error(err))
				}
			}
		default:
			if !changeStream.Next(ctx) {
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
	if s.membership != nil && s.totalPartitions > 1 {
		return s.createSimplePartitionPipeline()
	}

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
		filter = s.createChunkBasedFilter()
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

func (s *stream) createChunkBasedFilter() bson.D {
	if !s.cfg.Membership.ChunkBased {
		return bson.D{}
	}

	shardKey := s.cfg.Membership.Config["shardKey"]
	if shardKey == "" {
		return bson.D{}
	}

	chunkRanges := s.getChunkRanges()
	if len(chunkRanges) == 0 {
		return bson.D{}
	}

	var orConditions []bson.D
	for _, chunkRange := range chunkRanges {
		condition := bson.D{
			{Key: "$and", Value: bson.A{
				bson.D{{Key: shardKey, Value: bson.D{{Key: "$gte", Value: chunkRange.Min}}}},
				bson.D{{Key: shardKey, Value: bson.D{{Key: "$lt", Value: chunkRange.Max}}}},
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
	activeMembers := s.getActiveMembers(newInfo.Members)

	s.logger.Info("membership change detected",
		zap.Any("membership_info", newInfo),
		zap.Int("new_total_members", len(activeMembers)),
	)

	oldPartitionIndex := s.partitionIndex
	oldTotalPartitions := s.totalPartitions

	s.totalPartitions = len(activeMembers)

	for i, member := range activeMembers {
		if member.ID == s.cfg.Membership.MemberID {
			s.partitionIndex = i
			break
		}
	}
	if s.isActive && (oldPartitionIndex != s.partitionIndex || oldTotalPartitions != s.totalPartitions) {
		s.logger.Info("partition assignment changed, restarting stream",
			zap.Int("old_partition", oldPartitionIndex),
			zap.Int("new_partition", s.partitionIndex),
			zap.Int("old_total", oldTotalPartitions),
			zap.Int("new_total", s.totalPartitions),
		)

		// TODO: Implement graceful stream restart
		// This could involve stopping current stream and starting new one
		// with updated partition filter
	}
}

func (s *stream) getActiveMembers(members []membership.MemberInfo) []membership.MemberInfo {
	var activeMembers []membership.MemberInfo
	s.logger.Debug("filtering active members",
		zap.Int("total_members", len(members)),
	)

	for _, member := range members {
		s.logger.Debug("checking member",
			zap.String("member_id", member.ID),
			zap.String("status", string(member.Status)),
		)

		if member.Status == membership.MemberStatusActive || member.Status == membership.MemberStatusLeader {
			activeMembers = append(activeMembers, member)
			s.logger.Debug("member is active",
				zap.String("member_id", member.ID),
			)
		}
	}

	s.logger.Debug("active members filtered",
		zap.Int("active_count", len(activeMembers)),
	)

	return activeMembers
}

func (s *stream) createSimplePartitionPipeline() []bson.D {
	basePipeline := []bson.D{
		{
			{Key: "$match", Value: bson.D{
				{Key: "operationType", Value: bson.D{
					{Key: "$in", Value: bson.A{"insert", "update", "delete", "replace"}},
				}},
			}},
		},
	}

	if s.membership == nil || s.totalPartitions <= 1 {
		return basePipeline
	}

	if !s.cfg.Membership.ChunkBased {
		s.logger.Info("chunk-based partitioning is disabled, using basic partitioning")
		return basePipeline
	}

	shardKey := s.cfg.Membership.Config["shardKey"]
	if shardKey == "" {
		s.logger.Warn("shard key not configured for chunk-based partitioning")
		return basePipeline
	}

	chunkRanges := s.getChunkRanges()
	if len(chunkRanges) == 0 {
		s.logger.Warn("no chunk ranges found for this partition")
		return basePipeline
	}

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
