package metric

import (
	"context"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
)

type MongoDBMetrics struct {
	ActiveConnections    int32
	AvailableConnections int32
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

	if err := c.collectConnectionMetrics(ctx, client, metrics); err != nil {
		return nil, err
	}

	return metrics, nil
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
