package checkpoint

import (
	"context"
	"fmt"
	"time"

	"github.com/Trendyol/go-mongo-cdc/internal/backoff"
	"github.com/Trendyol/go-mongo-cdc/logger"
	"github.com/Trendyol/go-mongo-cdc/mongo/connection"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type Manager interface {
	SaveResumeToken(ctx context.Context, partitionID int, token []byte, clusterTime *primitive.Timestamp) error
	GetResumeToken(ctx context.Context, partitionID int) ([]byte, *primitive.Timestamp, error)
	ClearResumeToken(ctx context.Context, partitionID int) error
	SaveBootstrapProgress(ctx context.Context, partitionID int, lastID interface{}) error
	GetBootstrapProgress(ctx context.Context, partitionID int) (interface{}, error)
	ClearBootstrapProgress(ctx context.Context, partitionID int) error
	SaveBootstrapClusterTime(ctx context.Context, partitionID int, clusterTime primitive.Timestamp) error
}

type CheckpointInfo struct {
	ID              string              `bson:"_id"`
	PartitionID     int                 `bson:"partitionId"`
	ResumeToken     []byte              `bson:"resumeToken,omitempty"`
	LastClusterTime primitive.Timestamp `bson:"lastClusterTime,omitempty"`
	UpdatedAt       time.Time           `bson:"updatedAt"`
	BootstrapLastID interface{}         `bson:"bootstrapLastId,omitempty"`
	IsBootstrapping bool                `bson:"isBootstrapping"`
}

type manager struct {
	collection    connection.Collection
	database      string
	collName      string
	consumerGroup string
}

func NewManager(client connection.Client, database, collection, consumerGroup string) Manager {
	db := client.Database(database)
	checkpointCol := db.Collection(fmt.Sprintf("%s_checkpoints_%s", collection, consumerGroup))

	return &manager{
		collection:    checkpointCol,
		database:      database,
		collName:      collection,
		consumerGroup: consumerGroup,
	}
}

func (m *manager) getCheckpointID(partitionID int) string {
	return fmt.Sprintf("%s_%s_partition_%d", m.database, m.collName, partitionID)
}

func (m *manager) SaveResumeToken(ctx context.Context, partitionID int, token []byte, clusterTime *primitive.Timestamp) error {
	if len(token) == 0 && clusterTime == nil {
		logger.Log.Debug("SaveResumeToken skipped: both token and clusterTime are nil - partitionId: %d", partitionID)
		return nil
	}

	checkpointID := m.getCheckpointID(partitionID)
	filter := bson.M{"_id": checkpointID}

	setFields := bson.M{
		"partitionId":     partitionID,
		"updatedAt":       time.Now(),
		"isBootstrapping": false,
	}

	if len(token) > 0 {
		setFields["resumeToken"] = token
	}

	if clusterTime != nil {
		setFields["lastClusterTime"] = *clusterTime
	}

	update := bson.M{"$set": setFields}

	opts := options.Update().SetUpsert(true)

	b := backoff.New(backoff.FastConfig)

	for {
		_, err := m.collection.UpdateOne(ctx, filter, update, opts)
		if err == nil {
			logger.Log.Debug("Resume token saved - partitionId: %d, checkpointId: %s", partitionID, checkpointID)
			return nil
		}

		attempts := b.Attempts()
		if sleepErr := b.Sleep(ctx); sleepErr != nil {
			if sleepErr == backoff.ErrMaxRetriesExceeded {
				logger.Log.Error(
					"Failed to save resume token after all retries - partitionId: %d, checkpointId: %s, error: %v",
					partitionID, checkpointID, err,
				)
				return err
			}
			return sleepErr
		}

		logger.Log.Warn(
			"Failed to save resume token, retrying... - partitionId: %d, attempt: %d, maxRetries: %d, error: %v",
			partitionID, attempts+1, b.Config.MaxRetries, err,
		)
	}
}

func (m *manager) GetResumeToken(ctx context.Context, partitionID int) ([]byte, *primitive.Timestamp, error) {
	checkpointID := m.getCheckpointID(partitionID)
	filter := bson.M{"_id": checkpointID}

	var checkpoint CheckpointInfo
	err := m.collection.FindOne(ctx, filter).Decode(&checkpoint)
	if err != nil {
		if err == mongo.ErrNoDocuments {
			return nil, nil, nil
		}
		return nil, nil, err
	}

	var clusterTime *primitive.Timestamp
	if checkpoint.LastClusterTime != (primitive.Timestamp{}) {
		clusterTime = &checkpoint.LastClusterTime
	}

	return checkpoint.ResumeToken, clusterTime, nil
}

func (m *manager) SaveBootstrapProgress(ctx context.Context, partitionID int, lastID interface{}) error {
	checkpointID := m.getCheckpointID(partitionID)
	filter := bson.M{"_id": checkpointID}

	update := bson.M{
		"$set": bson.M{
			"partitionId":     partitionID,
			"bootstrapLastId": lastID,
			"isBootstrapping": true,
			"updatedAt":       time.Now(),
		},
	}

	opts := options.Update().SetUpsert(true)

	b := backoff.New(backoff.FastConfig)

	for {
		_, err := m.collection.UpdateOne(ctx, filter, update, opts)
		if err == nil {
			return nil
		}

		if sleepErr := b.Sleep(ctx); sleepErr != nil {
			if sleepErr == backoff.ErrMaxRetriesExceeded {
				logger.Log.Error("Failed to save bootstrap progress after retries - partitionId: %d, lastId: %v, error: %v", partitionID, lastID, err)
				return err
			}
			return sleepErr
		}
	}
}

func (m *manager) GetBootstrapProgress(ctx context.Context, partitionID int) (interface{}, error) {
	checkpointID := m.getCheckpointID(partitionID)
	filter := bson.M{"_id": checkpointID}

	var checkpoint CheckpointInfo
	err := m.collection.FindOne(ctx, filter).Decode(&checkpoint)
	if err != nil {
		if err == mongo.ErrNoDocuments {
			return nil, nil
		}
		return nil, err
	}

	if !checkpoint.IsBootstrapping {
		return nil, nil
	}

	return checkpoint.BootstrapLastID, nil
}

func (m *manager) ClearBootstrapProgress(ctx context.Context, partitionID int) error {
	checkpointID := m.getCheckpointID(partitionID)
	filter := bson.M{"_id": checkpointID}

	update := bson.M{
		"$set": bson.M{
			"isBootstrapping": false,
			"updatedAt":       time.Now(),
		},
		"$unset": bson.M{
			"bootstrapLastId": "",
		},
	}

	_, err := m.collection.UpdateOne(ctx, filter, update)
	return err
}

func (m *manager) ClearResumeToken(ctx context.Context, partitionID int) error {
	checkpointID := m.getCheckpointID(partitionID)
	filter := bson.M{"_id": checkpointID}

	update := bson.M{
		"$unset": bson.M{
			"resumeToken":     "",
			"lastClusterTime": "",
		},
		"$set": bson.M{
			"updatedAt": time.Now(),
		},
	}

	result, err := m.collection.UpdateOne(ctx, filter, update)
	if err != nil {
		logger.Log.Error("Failed to clear resume token - partitionId: %d, checkpointId: %s, error: %v", partitionID, checkpointID, err)
		return err
	}

	if result.MatchedCount() > 0 {
		logger.Log.Debug("Resume token cleared successfully - partitionId: %d, checkpointId: %s", partitionID, checkpointID)
	}

	return nil
}

func (m *manager) SaveBootstrapClusterTime(ctx context.Context, partitionID int, clusterTime primitive.Timestamp) error {
	checkpointID := m.getCheckpointID(partitionID)
	filter := bson.M{"_id": checkpointID}

	update := bson.M{
		"$set": bson.M{
			"partitionId":     partitionID,
			"lastClusterTime": clusterTime,
			"updatedAt":       time.Now(),
		},
	}

	opts := options.Update().SetUpsert(true)
	_, err := m.collection.UpdateOne(ctx, filter, update, opts)

	return err
}
