package metric

import (
	"context"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
)

type MongoDBMetrics struct {
	OplogSize              int64
	OplogUsedSize          int64
	OplogUsedPercent       float64
	OplogTimeDiff          int64
	OplogMinRetentionHours float64

	ReplicationLag         int64
	ReplicationOplogWindow int64

	ActiveConnections    int32
	AvailableConnections int32

	CollectionCount int64
	DataSize        int64
	IndexSize       int64
}

type MongoDBMetricsCollector interface {
	CollectMongoDBMetrics(ctx context.Context, client *mongo.Client) (*MongoDBMetrics, error)
}

type mongoDBMetricsCollector struct {
	database string
}

func NewMongoDBMetricsCollector(database string) MongoDBMetricsCollector {
	if database == "" {
		database = "local" // Fallback to 'local' if not provided
	}
	return &mongoDBMetricsCollector{
		database: database,
	}
}

func (c *mongoDBMetricsCollector) CollectMongoDBMetrics(ctx context.Context, client *mongo.Client) (*MongoDBMetrics, error) {
	metrics := &MongoDBMetrics{}

	// Oplog metrics are optional (only available in replica sets)
	// Silently skip if not available (single node, mongos, etc.)
	_ = c.collectOplogMetrics(ctx, client, metrics)

	// Replication metrics are optional (only available in replica sets)
	// Silently skip if not available
	_ = c.collectReplicationMetrics(ctx, client, metrics)

	// Connection metrics should always be available
	if err := c.collectConnectionMetrics(ctx, client, metrics); err != nil {
		return nil, err
	}

	return metrics, nil
}

func (c *mongoDBMetricsCollector) collectOplogMetrics(ctx context.Context, client *mongo.Client, metrics *MongoDBMetrics) error {
	db := client.Database(c.database)

	var result bson.M
	err := db.RunCommand(ctx, bson.D{{Key: "collStats", Value: "oplog.rs"}}).Decode(&result)
	if err != nil {
		return err
	}

	if size, ok := result["size"].(int64); ok {
		metrics.OplogUsedSize = size
	} else if size, ok := result["size"].(int32); ok {
		metrics.OplogUsedSize = int64(size)
	}

	if maxSize, ok := result["maxSize"].(int64); ok {
		metrics.OplogSize = maxSize
	} else if maxSize, ok := result["maxSize"].(int32); ok {
		metrics.OplogSize = int64(maxSize)
	}

	if metrics.OplogSize > 0 {
		metrics.OplogUsedPercent = float64(metrics.OplogUsedSize) / float64(metrics.OplogSize) * 100
	}

	oplogColl := db.Collection("oplog.rs")

	var firstEntry bson.M
	cursor, err := oplogColl.Find(ctx, bson.M{}, nil)
	if err == nil {
		defer cursor.Close(ctx)
		if cursor.Next(ctx) {
			cursor.Decode(&firstEntry)
		}
	}

	var lastEntry bson.M
	cursor, err = oplogColl.Find(ctx, bson.M{}, nil)
	if err == nil {
		defer cursor.Close(ctx)
		for cursor.Next(ctx) {
			cursor.Decode(&lastEntry)
		}
	}

	if firstTS, ok := firstEntry["ts"].(int64); ok {
		if lastTS, ok := lastEntry["ts"].(int64); ok {
			metrics.OplogTimeDiff = (lastTS - firstTS) / 1000
		}
	}

	return nil
}

func (c *mongoDBMetricsCollector) collectReplicationMetrics(ctx context.Context, client *mongo.Client, metrics *MongoDBMetrics) error {
	var result bson.M
	err := client.Database("admin").RunCommand(ctx, bson.D{{Key: "replSetGetStatus", Value: 1}}).Decode(&result)
	if err != nil {
		return err
	}

	if members, ok := result["members"].(bson.A); ok {
		var primaryOptime, secondaryOptime time.Time

		for _, member := range members {
			if m, ok := member.(bson.M); ok {
				if stateStr, ok := m["stateStr"].(string); ok {
					if optimeDate, ok := m["optimeDate"].(time.Time); ok {
						if stateStr == "PRIMARY" {
							primaryOptime = optimeDate
						} else if stateStr == "SECONDARY" {
							secondaryOptime = optimeDate
						}
					}
				}
			}
		}

		if !primaryOptime.IsZero() && !secondaryOptime.IsZero() {
			metrics.ReplicationLag = int64(primaryOptime.Sub(secondaryOptime).Seconds())
		}
	}

	if optimes, ok := result["optimes"].(bson.M); ok {
		if readConcernMajority, ok := optimes["readConcernMajorityOpTime"].(bson.M); ok {
			if lastCommitted, ok := optimes["lastCommittedOpTime"].(bson.M); ok {
				if rcmTS, ok := readConcernMajority["ts"].(int64); ok {
					if lcTS, ok := lastCommitted["ts"].(int64); ok {
						metrics.ReplicationOplogWindow = (lcTS - rcmTS) / 1000
					}
				}
			}
		}
	}

	return nil
}

func (c *mongoDBMetricsCollector) collectConnectionMetrics(ctx context.Context, client *mongo.Client, metrics *MongoDBMetrics) error {
	var result bson.M
	err := client.Database("admin").RunCommand(ctx, bson.D{{Key: "serverStatus", Value: 1}}).Decode(&result)
	if err != nil {
		return err
	}

	if connections, ok := result["connections"].(bson.M); ok {
		if current, ok := connections["current"].(int32); ok {
			metrics.ActiveConnections = current
		}
		if available, ok := connections["available"].(int32); ok {
			metrics.AvailableConnections = available
		}
	}

	return nil
}
