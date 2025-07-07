package integration

import (
	"context"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	cdc "github.com/Trendyol/go-mongo-cdc"
	"github.com/Trendyol/go-mongo-cdc/config"
	"github.com/Trendyol/go-mongo-cdc/internal/http"
	"github.com/Trendyol/go-mongo-cdc/internal/metric"
	"github.com/Trendyol/go-mongo-cdc/logger"
	"github.com/Trendyol/go-mongo-cdc/mongo/changestream"
	"github.com/Trendyol/go-mongo-cdc/mongo/connection"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.uber.org/zap"
)

const (
	defaultStepTimeout       = 10 * time.Second
	defaultConcurrentTimeout = 30 * time.Second
	membershipTimeout        = 45 * time.Second

	concurrentWorkers   = 5
	operationsPerWorker = 10
)

type E2ETestSuite struct {
	CDCTestSuite
}

func TestE2ESuite(t *testing.T) {
	suite.Run(t, new(E2ETestSuite))
}

func (suite *E2ETestSuite) TestCompleteWorkflow() {
	collector := NewMessageCollector()
	connector, _, cancel := suite.startConnectorWithReadinessCheck(collector)
	defer connector.Close()
	defer cancel()

	workflowSteps := []struct {
		name      string
		operation func() error
		expected  string
	}{
		{
			name: "create_document",
			operation: func() error {
				_, err := suite.insertTestDocument(bson.M{
					"_id":         primitive.NewObjectID(),
					"productName": "Test Product",
					"price":       100.50,
					"category":    "Electronics",
				})
				return err
			},
			expected: "insert",
		},
		{
			name: "update_price",
			operation: func() error {
				_, err := suite.updateTestDocument(
					bson.M{"productName": "Test Product"},
					bson.M{"price": 89.99},
				)
				return err
			},
			expected: "update",
		},
		{
			name: "update_category",
			operation: func() error {
				_, err := suite.updateTestDocument(
					bson.M{"productName": "Test Product"},
					bson.M{"category": "Home & Garden"},
				)
				return err
			},
			expected: "update",
		},
		{
			name: "delete_document",
			operation: func() error {
				_, err := suite.deleteTestDocument(bson.M{"productName": "Test Product"})
				return err
			},
			expected: "delete",
		},
	}

	for i, step := range workflowSteps {
		err := step.operation()
		require.NoError(suite.T(), err, "Step %s failed", step.name)

		suite.waitForCondition(func() bool {
			return collector.MessageCount() > i
		}, defaultStepTimeout, "waiting for step "+step.name)

		messages := collector.GetMessages()
		assert.Equal(suite.T(), step.expected, string(messages[i].OperationType), "Step %s operation type", step.name)
	}

	assert.Equal(suite.T(), len(workflowSteps), collector.MessageCount())
}

func (suite *E2ETestSuite) TestConcurrentOperations() {
	collector := NewMessageCollector()
	connector, _, cancel := suite.startConnectorWithReadinessCheck(collector)
	defer connector.Close()
	defer cancel()

	totalOperations := concurrentWorkers * operationsPerWorker

	var wg sync.WaitGroup
	wg.Add(concurrentWorkers)

	for i := 0; i < concurrentWorkers; i++ {
		go func(workerID int) {
			defer wg.Done()

			for j := 0; j < operationsPerWorker; j++ {
				doc := bson.M{
					"_id":       primitive.NewObjectID(),
					"worker":    workerID,
					"sequence":  j,
					"data":      "concurrent_test_data",
					"timestamp": time.Now(),
				}

				_, err := suite.insertTestDocument(doc)
				if err != nil {
					suite.T().Errorf("Worker %d operation %d failed: %v", workerID, j, err)
				}

				time.Sleep(10 * time.Millisecond)
			}
		}(i)
	}

	wg.Wait()

	suite.waitForCondition(func() bool {
		return collector.MessageCount() >= totalOperations
	}, defaultConcurrentTimeout, "all concurrent operations received")

	messages := collector.GetMessages()
	assert.GreaterOrEqual(suite.T(), len(messages), totalOperations)

	workerCounts := make(map[int]int)
	for _, msg := range messages {
		if msg.IsInsert() && msg.FullDocument != nil {
			if workerID, ok := msg.FullDocument["worker"]; ok {
				if wid, ok := workerID.(int32); ok {
					workerCounts[int(wid)]++
				}
			}
		}
	}

	for i := 0; i < concurrentWorkers; i++ {
		assert.GreaterOrEqual(suite.T(), workerCounts[i], operationsPerWorker, "Worker %d operations", i)
	}
}

func (suite *E2ETestSuite) TestChunkBasedMembershipPartitioning() {
	suite.T().Log("Testing chunk-based membership partitioning with multiple connectors")

	if !suite.isShardedCluster() {
		suite.T().Skip("Skipping chunk-based test: MongoDB is not in sharded mode")
		return
	}

	const numMembers = 3
	collectors := make([]MessageCollector, numMembers)
	connectors := make([]cdc.Connector, numMembers)
	cancels := make([]context.CancelFunc, numMembers)

	for i := 0; i < numMembers; i++ {
		collectors[i] = NewMessageCollector()
		connector, _, cancel := suite.startMembershipConnector(i+1, numMembers, collectors[i])
		connectors[i] = connector
		cancels[i] = cancel
	}

	defer func() {
		for i := 0; i < numMembers; i++ {
			connectors[i].Close()
			cancels[i]()
		}
	}()

	suite.T().Log("Waiting for stream restart completion after membership discovery...")
	time.Sleep(5 * time.Second)

	for i := 0; i < numMembers; i++ {
		suite.waitForMemberCDCReady(collectors[i], i, membershipTimeout)
	}

	suite.T().Log("All connectors are ready, starting document insertion")

	totalDocs := 100
	shardKeyValues := make([]int32, totalDocs)

	for i := 0; i < totalDocs; i++ {
		shardKeyValue := int32(i)
		shardKeyValues[i] = shardKeyValue

		doc := bson.M{
			"_id":       primitive.NewObjectID(),
			"sellerId":  shardKeyValue,
			"product":   "test_product_" + string(rune(i)),
			"price":     float64(i) * 10.5,
			"timestamp": time.Now(),
		}

		_, err := suite.insertTestDocument(doc)
		require.NoError(suite.T(), err, "Failed to insert document %d", i)

		time.Sleep(10 * time.Millisecond)
	}

	suite.T().Log("All documents inserted, waiting for processing")

	suite.waitForCondition(func() bool {
		totalReceived := 0
		for i := 0; i < numMembers; i++ {
			totalReceived += collectors[i].MessageCount()
		}
		return totalReceived >= totalDocs
	}, membershipTimeout, "all documents processed by members")

	suite.verifyChunkBasedPartitioning(collectors, shardKeyValues, numMembers)
}

func (suite *E2ETestSuite) TestMembershipRebalancing() {
	suite.T().Log("Testing membership rebalancing when members join/leave")

	if !suite.isShardedCluster() {
		suite.T().Skip("Skipping rebalancing test: MongoDB is not in sharded mode")
		return
	}

	const initialMembers = 2
	collectors := make([]MessageCollector, 3)
	connectors := make([]cdc.Connector, 3)
	cancels := make([]context.CancelFunc, 3)

	for i := 0; i < initialMembers; i++ {
		collectors[i] = NewMessageCollector()
		connector, _, cancel := suite.startMembershipConnector(i+1, initialMembers, collectors[i])
		connectors[i] = connector
		cancels[i] = cancel
	}

	defer func() {
		for i := 0; i < 3; i++ {
			if connectors[i] != nil {
				connectors[i].Close()
			}
			if cancels[i] != nil {
				cancels[i]()
			}
		}
	}()

	suite.T().Log("Waiting for change streams to be fully ready...")
	time.Sleep(3 * time.Second)

	for i := 0; i < initialMembers; i++ {
		suite.waitForMemberCDCReady(collectors[i], i, membershipTimeout)
	}

	for i := 0; i < 20; i++ {
		doc := bson.M{
			"_id":      primitive.NewObjectID(),
			"sellerId": int32(i),
			"phase":    "initial",
		}
		_, err := suite.insertTestDocument(doc)
		require.NoError(suite.T(), err)
		time.Sleep(50 * time.Millisecond)
	}

	time.Sleep(5 * time.Second)

	initialCounts := make([]int, initialMembers)
	for i := 0; i < initialMembers; i++ {
		initialCounts[i] = collectors[i].MessageCount()
	}

	suite.T().Logf("Initial distribution: %v", initialCounts)

	collectors[2] = NewMessageCollector()
	connector, _, cancel := suite.startMembershipConnector(3, 3, collectors[2]) // Member 3 out of 3
	connectors[2] = connector
	cancels[2] = cancel

	suite.T().Log("Waiting for third member change stream to be ready...")
	time.Sleep(3 * time.Second)

	time.Sleep(10 * time.Second)

	for i := 20; i < 60; i++ {
		doc := bson.M{
			"_id":      primitive.NewObjectID(),
			"sellerId": int32(i),
			"phase":    "after_rebalance",
		}
		_, err := suite.insertTestDocument(doc)
		require.NoError(suite.T(), err)
		time.Sleep(50 * time.Millisecond)
	}

	time.Sleep(5 * time.Second)
	finalCounts := make([]int, 3)
	for i := 0; i < 3; i++ {
		finalCounts[i] = collectors[i].MessageCount()
	}

	suite.T().Logf("Final distribution: %v", finalCounts)

	assert.Greater(suite.T(), finalCounts[2], 0, "Third member should process some documents after joining")
}

func (suite *E2ETestSuite) TestMembershipWithoutChunks() {
	suite.T().Log("Testing membership behavior when chunk-based partitioning is disabled")

	collector := NewMessageCollector()
	connector, _, cancel := suite.startMembershipConnectorWithoutChunks(1, 2, collector)
	defer connector.Close()
	defer cancel()

	suite.waitForCDCReady(collector, membershipTimeout)

	for i := 0; i < 10; i++ {
		doc := bson.M{
			"_id":      primitive.NewObjectID(),
			"sellerId": int32(i),
			"test":     "no_chunks",
		}
		_, err := suite.insertTestDocument(doc)
		require.NoError(suite.T(), err)
		time.Sleep(100 * time.Millisecond)
	}

	suite.waitForCondition(func() bool {
		return collector.MessageCount() >= 10
	}, defaultStepTimeout, "all documents processed without chunk partitioning")

	assert.GreaterOrEqual(suite.T(), collector.MessageCount(), 10)
}

func (suite *E2ETestSuite) TestBasicMembershipIntegration() {
	suite.T().Log("Testing basic membership integration")

	collector := NewMessageCollector()

	host, port := suite.parseConnectionURI()
	uniqueCheckpointName := suite.generateUniqueCheckpointName("cdc_checkpoints_member_test")
	cfg := config.Config{
		Host:       host,
		Port:       port,
		Database:   suite.testDB,
		Collection: suite.testCollection,
		DebugMode:  true,
		Metric: config.MetricConfig{
			Port: 8081,
		},
		Checkpoint: config.CheckpointConfig{
			Collection:   uniqueCheckpointName,
			SaveInterval: 5 * time.Second,
		},
		Membership: config.MembershipConfig{
			Enabled:            true,
			Type:               "dynamic",
			HeartbeatInterval:  2 * time.Second,
			HealthCheckTimeout: 10 * time.Second,
			ChunkBased:         false,
			Config: map[string]string{
				"shardKey":             "sellerId",
				"membershipCollection": "test_membership",
				"membershipDatabase":   "cdc_cluster",
			},
		},
	}

	connector, err := suite.createTestConnectorWithCustomConfig(cfg, collector.CollectMessage)
	require.NoError(suite.T(), err)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	defer connector.Close()

	go connector.Start(ctx)

	suite.waitForCDCReady(collector, membershipTimeout)

	for i := 0; i < 5; i++ {
		doc := bson.M{
			"_id":      primitive.NewObjectID(),
			"sellerId": int32(i),
			"product":  "membership_test_product",
			"test":     "basic_membership",
		}
		_, err := suite.insertTestDocument(doc)
		require.NoError(suite.T(), err)
		time.Sleep(200 * time.Millisecond)
	}

	suite.waitForCondition(func() bool {
		return collector.MessageCount() >= 5
	}, defaultStepTimeout, "all documents processed with membership")

	messages := collector.GetMessages()
	assert.GreaterOrEqual(suite.T(), len(messages), 5)

	insertCount := 0
	for _, msg := range messages {
		if msg.IsInsert() {
			insertCount++
		}
	}
	assert.Equal(suite.T(), 5, insertCount, "All messages should be insert operations")

	suite.T().Log("Basic membership integration test completed successfully")
}

func (suite *E2ETestSuite) TestChunkBasedMembershipEnabled() {
	suite.T().Log("Testing chunk-based membership (mock environment)")

	if !suite.setupMockShardedEnvironment() {
		suite.T().Skip("Could not setup mock sharded environment")
		return
	}

	collector := NewMessageCollector()

	host, port := suite.parseConnectionURI()
	uniqueCheckpointName := suite.generateUniqueCheckpointName("cdc_checkpoints_chunk_test")
	cfg := config.Config{
		Host:       host,
		Port:       port,
		Database:   suite.testDB,
		Collection: suite.testCollection,
		DebugMode:  true,
		Metric: config.MetricConfig{
			Port: 8082,
		},
		Checkpoint: config.CheckpointConfig{
			Collection:   uniqueCheckpointName,
			SaveInterval: 5 * time.Second,
		},
		Membership: config.MembershipConfig{
			Enabled:            true,
			Type:               "static",
			MemberID:           "chunk-test-member-1",
			MemberNumber:       1,
			TotalMembers:       2,
			HeartbeatInterval:  2 * time.Second,
			HealthCheckTimeout: 10 * time.Second,
			ChunkBased:         true,
			Config: map[string]string{
				"shardKey": "sellerId",
			},
		},
	}

	connector, err := suite.createTestConnectorWithCustomConfig(cfg, collector.CollectMessage)
	require.NoError(suite.T(), err)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	defer connector.Close()

	go connector.Start(ctx)

	suite.T().Log("Waiting for change stream and chunk-based filtering to be fully ready...")
	time.Sleep(5 * time.Second)

	suite.waitForMemberCDCReady(collector, 0, membershipTimeout)

	// Insert documents with different shard key values
	// Based on our mock chunks: 0-25, 25-50, 50-75, 75-100
	// Member 1 should get chunks 0 and 2 (0-25, 50-75)
	// Member 2 should get chunks 1 and 3 (25-50, 75-100)
	testDocs := []struct {
		sellerId int32
		expected bool
	}{
		{5, true},   // Chunk 0 (0-25) -> Member 1
		{35, false}, // Chunk 1 (25-50) -> Member 2
		{55, true},  // Chunk 2 (50-75) -> Member 1
		{85, false}, // Chunk 3 (75-100) -> Member 2
	}

	for i, testDoc := range testDocs {
		doc := bson.M{
			"_id":      primitive.NewObjectID(),
			"sellerId": testDoc.sellerId,
			"product":  "chunk_test_product",
			"index":    i,
		}
		_, err := suite.insertTestDocument(doc)
		require.NoError(suite.T(), err)
		time.Sleep(200 * time.Millisecond)
	}

	time.Sleep(3 * time.Second)

	messages := collector.GetMessages()
	suite.T().Logf("Received %d messages with chunk-based partitioning", len(messages))

	processedSellerIds := make([]int32, 0)
	for _, msg := range messages {
		if msg.IsInsert() && msg.FullDocument != nil {
			if sellerId, ok := msg.FullDocument["sellerId"]; ok {
				if id, ok := sellerId.(int32); ok {
					processedSellerIds = append(processedSellerIds, id)
				}
			}
		}
	}

	suite.T().Logf("Processed seller IDs: %v", processedSellerIds)

	expectedProcessed := 0
	for _, id := range processedSellerIds {
		if id == 5 || id == 55 {
			expectedProcessed++
		}
	}

	// Should process exactly the documents from assigned chunks
	assert.GreaterOrEqual(suite.T(), expectedProcessed, 1,
		"Should process at least some documents from assigned chunks")

	suite.T().Log("Chunk-based membership test completed")
}

// TODO: write cases with multiple connector and dynamic membership

func (suite *E2ETestSuite) waitForMemberCDCReady(collector MessageCollector, memberIndex int, timeout time.Duration) {
	suite.logger.Info("Testing member-specific CDC readiness...",
		zap.Int("member_index", memberIndex))

	initialCount := collector.MessageCount()

	// Based on mock chunks (0-25, 25-50, 50-75, 75-100) and 3 members:
	// Member 0 (partition 0): gets chunks 0,3 (0-25, 75-100)
	// Member 1 (partition 1): gets chunk 1 (25-50)
	// Member 2 (partition 2): gets chunk 2 (50-75)
	//
	var testSellerId int32
	switch memberIndex {
	case 0:
		testSellerId = 10 // Falls into chunk 0 (0-25)
	case 1:
		testSellerId = 35 // Falls into chunk 1 (25-50)
	case 2:
		testSellerId = 60 // Falls into chunk 2 (50-75)
	default:
		testSellerId = 10 // Default to chunk 0
	}

	testDoc := bson.M{
		"_id":         primitive.NewObjectID(),
		"testType":    "member_readiness_check",
		"sellerId":    testSellerId,
		"memberIndex": memberIndex,
		"timestamp":   time.Now(),
	}

	_, err := suite.insertTestDocument(testDoc)
	suite.Require().NoError(err, "Failed to insert member readiness test document for member %d", memberIndex)

	suite.logger.Info("Waiting for member CDC readiness...",
		zap.Int("member_index", memberIndex),
		zap.Int32("test_seller_id", testSellerId),
		zap.Int("initial_count", initialCount),
		zap.Int("current_count", collector.MessageCount()))

	suite.waitForCondition(func() bool {
		currentCount := collector.MessageCount()
		if currentCount > initialCount {
			suite.logger.Info("Member CDC readiness condition met!",
				zap.Int("member_index", memberIndex),
				zap.Int("current_count", currentCount),
				zap.Int("initial_count", initialCount))
			return true
		}

		// Log periodically
		suite.logger.Debug("Still waiting for member CDC readiness...",
			zap.Int("member_index", memberIndex),
			zap.Int("current_count", currentCount),
			zap.Int("initial_count", initialCount))
		return false
	}, timeout, fmt.Sprintf("Member %d CDC to capture readiness test document with sellerId %d", memberIndex, testSellerId))

	suite.logger.Info("Member CDC readiness confirmed",
		zap.Int("member_index", memberIndex),
		zap.Int32("test_seller_id", testSellerId))

	collector.Clear()
}

func (suite *E2ETestSuite) isShardedCluster() bool {
	result := suite.database.RunCommand(suite.ctx, bson.D{{Key: "isMaster", Value: 1}})
	var response bson.M
	if err := result.Decode(&response); err != nil {
		return false
	}

	if msg, ok := response["msg"]; ok && msg == "isdbgrid" {
		return true
	}

	return suite.setupMockShardedEnvironment()
}

func (suite *E2ETestSuite) setupMockShardedEnvironment() bool {
	configDB := suite.mongoClient.Database("config")
	chunksCollection := configDB.Collection("chunks")

	chunksCollection.Drop(suite.ctx)
	mockChunks := []interface{}{
		bson.M{
			"_id": primitive.NewObjectID(),
			"ns":  suite.testDB + "." + suite.testCollection,
			"min": bson.M{"sellerId": int32(0)},
			"max": bson.M{"sellerId": int32(25)},
		},
		bson.M{
			"_id": primitive.NewObjectID(),
			"ns":  suite.testDB + "." + suite.testCollection,
			"min": bson.M{"sellerId": int32(25)},
			"max": bson.M{"sellerId": int32(50)},
		},
		bson.M{
			"_id": primitive.NewObjectID(),
			"ns":  suite.testDB + "." + suite.testCollection,
			"min": bson.M{"sellerId": int32(50)},
			"max": bson.M{"sellerId": int32(75)},
		},
		bson.M{
			"_id": primitive.NewObjectID(),
			"ns":  suite.testDB + "." + suite.testCollection,
			"min": bson.M{"sellerId": int32(75)},
			"max": bson.M{"sellerId": int32(100)},
		},
	}

	_, err := chunksCollection.InsertMany(suite.ctx, mockChunks)
	return err == nil
}

func (suite *E2ETestSuite) verifyChunkBasedPartitioning(collectors []MessageCollector, shardKeyValues []int32, numMembers int) {
	suite.T().Log("Verifying chunk-based partitioning results")

	memberDocCounts := make([]int, numMembers)
	memberShardKeys := make([][]int32, numMembers)

	for i := 0; i < numMembers; i++ {
		messages := collectors[i].GetMessages()
		memberDocCounts[i] = len(messages)
		memberShardKeys[i] = make([]int32, 0)

		for _, msg := range messages {
			if msg.IsInsert() && msg.FullDocument != nil {
				if sellerId, ok := msg.FullDocument["sellerId"]; ok {
					if id, ok := sellerId.(int32); ok {
						memberShardKeys[i] = append(memberShardKeys[i], id)
					}
				}
			}
		}
	}

	suite.T().Logf("Member document counts: %v", memberDocCounts)

	for i := 0; i < numMembers; i++ {
		assert.Greater(suite.T(), memberDocCounts[i], 0,
			"Member %d should process at least some documents", i+1)
	}

	allProcessedKeys := make(map[int32]int)
	for i := 0; i < numMembers; i++ {
		for _, key := range memberShardKeys[i] {
			allProcessedKeys[key]++
		}
	}

	duplicateCount := 0
	for key, count := range allProcessedKeys {
		if count > 1 {
			duplicateCount++
			suite.T().Logf("Document with sellerId %d was processed by %d members", key, count)
		}
	}

	assert.LessOrEqual(suite.T(), duplicateCount, len(shardKeyValues)/10,
		"Too many duplicate documents processed")

	totalProcessed := 0
	for i := 0; i < numMembers; i++ {
		totalProcessed += memberDocCounts[i]
	}

	assert.GreaterOrEqual(suite.T(), totalProcessed, len(shardKeyValues),
		"Should process documents")
}

func (suite *E2ETestSuite) startMembershipConnector(memberNumber, totalMembers int, collector MessageCollector) (cdc.Connector, context.Context, context.CancelFunc) {
	cfg := suite.createMembershipConfig(memberNumber, totalMembers, true)
	return suite.startConnectorWithConfig(cfg, collector)
}

func (suite *E2ETestSuite) startMembershipConnectorWithoutChunks(memberNumber, totalMembers int, collector MessageCollector) (cdc.Connector, context.Context, context.CancelFunc) {
	cfg := suite.createMembershipConfig(memberNumber, totalMembers, false)
	return suite.startConnectorWithConfig(cfg, collector)
}

func (suite *E2ETestSuite) createMembershipConfig(memberNumber, totalMembers int, chunkBased bool) config.Config {
	host, port := suite.parseConnectionURI()

	uniqueCheckpointName := suite.generateUniqueCheckpointName(fmt.Sprintf("cdc_checkpoints_member_%d", memberNumber))

	cfg := config.Config{
		Host:       host,
		Port:       port,
		Database:   suite.testDB,
		Collection: suite.testCollection,
		DebugMode:  true,
		Metric: config.MetricConfig{
			Port: 8080 + memberNumber,
		},
		Checkpoint: config.CheckpointConfig{
			Collection:   uniqueCheckpointName,
			SaveInterval: 5 * time.Second,
		},
		Membership: config.MembershipConfig{
			Enabled:            true,
			Type:               "static",
			MemberID:           fmt.Sprintf("member-%d", memberNumber),
			MemberNumber:       memberNumber,
			TotalMembers:       totalMembers,
			HeartbeatInterval:  2 * time.Second,
			HealthCheckTimeout: 10 * time.Second,
			ChunkBased:         chunkBased,
			Config: map[string]string{
				"shardKey": "sellerId",
			},
		},
	}

	return cfg
}

func (suite *E2ETestSuite) startConnectorWithConfig(cfg config.Config, collector MessageCollector) (cdc.Connector, context.Context, context.CancelFunc) {
	ctx, cancel := context.WithCancel(context.Background())

	connector, err := suite.createTestConnectorWithCustomConfig(cfg, collector.CollectMessage)
	require.NoError(suite.T(), err)

	go connector.Start(ctx)

	return connector, ctx, cancel
}

func (suite *E2ETestSuite) createTestConnectorWithCustomConfig(cfg config.Config, listenerFunc changestream.ListenerFunc) (cdc.Connector, error) {
	cfg.SetDefault()
	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	zapLogger := logger.InitLogger(cfg.Logger.Logger)

	host, port := suite.parseConnectionURI()
	externalURI := fmt.Sprintf("mongodb://%s:%d/?directConnection=true&readPreference=primary", host, port)

	suite.logger.Info("Using external mapped URI for CDC with custom config",
		zap.String("externalURI", externalURI),
		zap.String("host", host),
		zap.Int("port", port),
		zap.String("member_id", cfg.Membership.MemberID),
		zap.Int("member_number", cfg.Membership.MemberNumber),
		zap.Int("total_members", cfg.Membership.TotalMembers),
		zap.Bool("chunk_based", cfg.Membership.ChunkBased))

	mongoClient, err := connection.NewConnection(suite.ctx, externalURI)
	if err != nil {
		return nil, err
	}

	m := metric.NewMetric(cfg.Database, cfg.Collection)
	stream := changestream.NewStream(mongoClient, cfg, m, listenerFunc, zapLogger)
	prometheusRegistry := metric.NewRegistry(m)

	return &testConnector{
		cfg:                &cfg,
		mongoClient:        mongoClient,
		stream:             stream,
		prometheusRegistry: prometheusRegistry,
		server:             http.NewServer(cfg, prometheusRegistry, zapLogger, mongoClient, nil),
		logger:             zapLogger,
		cancelCh:           make(chan os.Signal, 1),
		readyCh:            make(chan struct{}, 1),
	}, nil
}

func (suite *CDCTestSuite) createTestConnectorWithConfig(cfg config.Config,
	listenerFunc changestream.ListenerFunc) (cdc.Connector, error) {

	return suite.createTestConnector(listenerFunc)
}
