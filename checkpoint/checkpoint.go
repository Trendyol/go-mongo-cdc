package checkpoint

import (
	"context"
	"fmt"
	"time"

	"github.com/Trendyol/go-mongo-cdc/mongo/connection"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.uber.org/zap"
)

type Manager interface {
	SaveResumeToken(ctx context.Context, partitionID int, token []byte, clusterTime *primitive.Timestamp) error
	GetResumeToken(ctx context.Context, partitionID int) ([]byte, *primitive.Timestamp, error)
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
	LastRun         time.Time           `bson:"lastRun"`
	UpdatedAt       time.Time           `bson:"updatedAt"`

	// Bootstrap fields
	BootstrapLastID interface{} `bson:"bootstrapLastId,omitempty"`
	IsBootstrapping bool        `bson:"isBootstrapping"`
}

type manager struct {
	collection connection.Collection
	database   string
	collName   string
	logger     *zap.Logger
}

func NewManager(client connection.Client, database, collection string, logger *zap.Logger) Manager {
	db := client.Database(database)
	checkpointCol := db.Collection(collection + "_checkpoints")

	return &manager{
		collection: checkpointCol,
		database:   database,
		collName:   collection,
		logger:     logger,
	}
}

func (m *manager) getCheckpointID(partitionID int) string {
	return fmt.Sprintf("%s_%s_partition_%d", m.database, m.collName, partitionID)
}

func (m *manager) SaveResumeToken(ctx context.Context, partitionID int, token []byte, clusterTime *primitive.Timestamp) error {
	if len(token) == 0 {
		return nil
	}

	checkpointID := m.getCheckpointID(partitionID)
	filter := bson.M{"_id": checkpointID}

	update := bson.M{
		"$set": bson.M{
			"partitionId":     partitionID,
			"resumeToken":     token,
			"lastRun":         time.Now(),
			"updatedAt":       time.Now(),
			"isBootstrapping": false,
		},
	}

	if clusterTime != nil {
		update["$set"].(bson.M)["lastClusterTime"] = *clusterTime
	}

	opts := options.Update().SetUpsert(true)
	_, err := m.collection.UpdateOne(ctx, filter, update, opts)

	if err != nil {
		m.logger.Error(fmt.Sprintf("Failed to save resume token - partitionId: %d, checkpointId: %s, error: %v", partitionID, checkpointID, err))
		return err
	}

	m.logger.Debug(fmt.Sprintf("Resume token saved - partitionId: %d, checkpointId: %s", partitionID, checkpointID))

	return nil
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
	_, err := m.collection.UpdateOne(ctx, filter, update, opts)

	if err != nil {
		m.logger.Error(fmt.Sprintf("Failed to save bootstrap progress - partitionId: %d, lastId: %v, error: %v", partitionID, lastID, err))
	}

	return err
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
