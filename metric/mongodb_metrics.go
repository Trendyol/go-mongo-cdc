package metric

import (
	"context"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
)

type MongoDBMetrics struct {
	ReplicationLag int64

	ActiveConnections    int32
	AvailableConnections int32

	CollectionCount int64
	DataSize        int64
	IndexSize       int64
}

type MongoDBMetricsCollector interface {
	CollectMongoDBMetrics(ctx context.Context, client *mongo.Client) (*MongoDBMetrics, error)
}

type mongoDBMetricsCollector struct{}

func NewMongoDBMetricsCollector() MongoDBMetricsCollector {
	return &mongoDBMetricsCollector{}
}

func (c *mongoDBMetricsCollector) CollectMongoDBMetrics(ctx context.Context, client *mongo.Client) (*MongoDBMetrics, error) {
	metrics := &MongoDBMetrics{}

	_ = c.collectReplicationMetrics(ctx, client, metrics)

	if err := c.collectConnectionMetrics(ctx, client, metrics); err != nil {
		return nil, err
	}

	return metrics, nil
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
