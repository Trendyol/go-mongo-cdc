package metric

import (
	"context"
	"fmt"
	"strings"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type ShardMetrics struct {
	ShardName string
	Metrics   *MongoDBMetrics
}

type ShardedMetricsCollector interface {
	CollectShardedMetrics(ctx context.Context, mongosClient *mongo.Client) ([]*ShardMetrics, error)
}

type shardedMetricsCollector struct {
	baseCollector     MongoDBMetricsCollector
	enableHostMapping bool
}

func NewShardedMetricsCollector(enableHostMapping bool, database string) ShardedMetricsCollector {
	return &shardedMetricsCollector{
		baseCollector:     NewMongoDBMetricsCollector(database),
		enableHostMapping: enableHostMapping,
	}
}

func (c *shardedMetricsCollector) CollectShardedMetrics(ctx context.Context, mongosClient *mongo.Client) ([]*ShardMetrics, error) {
	shards, err := c.getShardList(ctx, mongosClient)
	if err != nil {
		return nil, fmt.Errorf("failed to get shard list: %w", err)
	}

	if len(shards) == 0 {
		return nil, fmt.Errorf("no shards found in config.shards")
	}

	results := make([]*ShardMetrics, 0, len(shards))

	for _, shard := range shards {
		select {
		case <-ctx.Done():
			return results, nil
		default:
		}

		shardClient, err := c.connectToShard(ctx, shard)
		if err != nil {
			continue
		}

		metrics, err := c.baseCollector.CollectMongoDBMetrics(ctx, shardClient)
		disconnectCtx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
		shardClient.Disconnect(disconnectCtx)
		cancel()

		if err != nil {
			continue
		}

		results = append(results, &ShardMetrics{
			ShardName: shard.Name,
			Metrics:   metrics,
		})
	}

	if len(results) == 0 {
		return nil, fmt.Errorf("failed to collect metrics from any shard")
	}

	return results, nil
}

type shardInfo struct {
	Name string
	Host string
}

func (c *shardedMetricsCollector) getShardList(ctx context.Context, client *mongo.Client) ([]shardInfo, error) {
	configDB := client.Database("config")
	shardsCol := configDB.Collection("shards")

	cursor, err := shardsCol.Find(ctx, bson.M{})
	if err != nil {
		return nil, err
	}
	defer cursor.Close(ctx)

	var shards []shardInfo
	for cursor.Next(ctx) {
		var doc bson.M
		if err := cursor.Decode(&doc); err != nil {
			continue
		}

		name, ok := doc["_id"].(string)
		if !ok {
			continue
		}

		host, ok := doc["host"].(string)
		if !ok {
			continue
		}

		shards = append(shards, shardInfo{
			Name: name,
			Host: host,
		})
	}

	return shards, nil
}

func (c *shardedMetricsCollector) connectToShard(ctx context.Context, shard shardInfo) (*mongo.Client, error) {
	replicaSet, hosts := c.parseShardHost(shard.Host)

	// Convert container hostnames to localhost ports if enabled (for local development)
	if c.enableHostMapping {
		hosts = c.normalizeHosts(hosts)
	}

	uri := fmt.Sprintf("mongodb://%s", strings.Join(hosts, ","))
	if replicaSet != "" {
		uri += fmt.Sprintf("/?replicaSet=%s", replicaSet)
	}

	clientOpts := options.Client().
		ApplyURI(uri).
		SetConnectTimeout(2 * time.Second).
		SetServerSelectionTimeout(2 * time.Second).
		SetSocketTimeout(2 * time.Second).
		SetDirect(true)

	connectCtx, connectCancel := context.WithTimeout(ctx, 3*time.Second)
	defer connectCancel()

	client, err := mongo.Connect(connectCtx, clientOpts)
	if err != nil {
		return nil, err
	}

	pingCtx, pingCancel := context.WithTimeout(ctx, 1*time.Second)
	defer pingCancel()

	if err := client.Ping(pingCtx, nil); err != nil {
		client.Disconnect(context.Background())
		return nil, err
	}

	return client, nil
}

func (c *shardedMetricsCollector) parseShardHost(host string) (string, []string) {
	if strings.Contains(host, "/") {
		parts := strings.SplitN(host, "/", 2)
		replicaSet := parts[0]
		hosts := strings.Split(parts[1], ",")
		return replicaSet, hosts
	}

	hosts := strings.Split(host, ",")
	return "", hosts
}

// normalizeHosts converts container hostnames to localhost ports for local development
// Based on docker-compose-sharded.yml port mappings:
// - shard1-server:27018 -> localhost:27018
// - shard2-server:27018 -> localhost:27028 (mapped to different port)
// - config-server:27019 -> localhost:27019
func (c *shardedMetricsCollector) normalizeHosts(hosts []string) []string {
	normalized := make([]string, 0, len(hosts))

	for _, host := range hosts {
		// Extract hostname and port
		parts := strings.Split(host, ":")
		hostname := parts[0]

		// Map common container/internal names to localhost ports
		switch {
		case strings.Contains(hostname, "shard1"):
			normalized = append(normalized, "localhost:27018")
		case strings.Contains(hostname, "shard2"):
			normalized = append(normalized, "localhost:27028")
		case strings.Contains(hostname, "config"):
			normalized = append(normalized, "localhost:27019")
		case hostname == "localhost" || hostname == "127.0.0.1":
			normalized = append(normalized, host)
		default:
			// Keep original for production environments
			normalized = append(normalized, host)
		}
	}

	return normalized
}
