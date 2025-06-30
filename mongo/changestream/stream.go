package changestream

import (
	"context"
	"errors"
	"time"

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
}

func NewStream(client connection.Client, cfg config.Config, metric metric.Metric, listener ListenerFunc, logger *zap.Logger) Streamer {
	database := client.Database(cfg.Database)
	collection := database.Collection(cfg.Collection)
	checkpoint := database.Collection(cfg.Checkpoint.Collection)

	return &stream{
		client:     client,
		cfg:        cfg,
		metric:     metric,
		listener:   listener,
		logger:     logger,
		collection: collection,
		database:   database,
		checkpoint: checkpoint,
		isActive:   false,
	}
}

func (s *stream) Open(ctx context.Context) error {
	if s.isActive {
		return ErrorStreamInUse
	}

	s.isActive = true
	defer func() { s.isActive = false }()

	s.logger.Info("Starting MongoDB Change Stream",
		zap.String("database", s.cfg.Database),
		zap.String("collection", s.cfg.Collection))

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
	defer changeStream.Close(ctx)

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
	s.logger.Info("MongoDB Change Stream closed")
	return nil
}

func (s *stream) createPipeline() []bson.D {
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

	return errors.New("MongoDB is not running as a replica set or sharded cluster. Change streams require replica set or sharded cluster")
}

func (s *stream) processEvent(ctx context.Context, event message.ChangeEvent) error {
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
	}
}

func (s *stream) saveResumeToken(ctx context.Context, token []byte) error {
	if token == nil || len(token) == 0 {
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

func (s *stream) processAllDocuments(ctx context.Context) error {
	s.logger.Info("Starting to process all existing documents as insert events")

	cursor, err := s.collection.Find(ctx, bson.D{})
	if err != nil {
		return err
	}
	defer cursor.Close(ctx)

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
			ClusterTime: primitive.Timestamp{T: uint32(time.Now().Unix()), I: 1},
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
