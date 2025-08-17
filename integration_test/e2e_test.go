package integration

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	cdc "github.com/Trendyol/go-mongo-cdc"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
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

func (suite *E2ETestSuite) TestBasicMembershipIntegration() {
	suite.T().Log("Testing basic membership integration")

	collector := NewMessageCollector()

	connector, _, cancel := suite.startConnectorWithReadinessCheck(collector)
	defer connector.Close()
	defer cancel()

	for i := 0; i < 5; i++ {
		doc := bson.M{
			"_id":      primitive.NewObjectID(),
			"sellerId": int32(i),
			"product":  "membership_test_product",
			"test":     "basic_membership",
		}
		_, err := suite.insertTestDocument(doc)
		require.NoError(suite.T(), err)
		time.Sleep(50 * time.Millisecond)
	}

	suite.waitForCondition(func() bool {
		return collector.MessageCount() >= 5
	}, defaultStepTimeout, "all documents processed with membership")

	messages := collector.GetMessages()
	assert.Equal(suite.T(), len(messages), 5)

	insertCount := 0
	for _, msg := range messages {
		if msg.IsInsert() {
			insertCount++
		}
	}
	assert.Equal(suite.T(), 5, insertCount, "All messages should be insert operations")

	suite.T().Log("Basic membership integration test completed successfully")
}

func (suite *E2ETestSuite) TestMembershipRebalancing() {
	suite.T().Log("Testing membership rebalancing when members join/leave")

	const connectorCount = 2
	const maxConnectorCount = 3
	collectors := make([]MessageCollector, maxConnectorCount)
	connectors := make([]cdc.Connector, maxConnectorCount)
	cancels := make([]context.CancelFunc, maxConnectorCount)

	for i := 0; i < connectorCount; i++ {
		collectors[i] = NewMessageCollector()
		connector, _, cancel := suite.startConnectorWithReadinessCheck(collectors[i])
		connectors[i] = connector
		cancels[i] = cancel
	}

	defer func() {
		for i := 0; i < maxConnectorCount; i++ {
			if connectors[i] != nil {
				connectors[i].Close()
			}
			if cancels[i] != nil {
				cancels[i]()
			}
		}
	}()

	suite.T().Log("Phase 1: Testing basic functionality with 2 members")

	initialDocuments := 20
	for i := 0; i < initialDocuments; i++ {
		doc := bson.M{
			"_id":   i + 1,
			"phase": "initial",
		}
		_, err := suite.insertTestDocument(doc)
		require.NoError(suite.T(), err)
		time.Sleep(50 * time.Millisecond)
	}

	suite.waitForMessageProcessing(collectors[:connectorCount], initialDocuments/connectorCount)

	initialCounts := make([]int, connectorCount)
	totalInitial := 0
	for i := 0; i < connectorCount; i++ {
		initialCounts[i] = collectors[i].MessageCount()
		assert.Equal(suite.T(), initialDocuments/connectorCount, collectors[i].MessageCount())

		totalInitial += initialCounts[i]
	}

	assert.Equal(suite.T(), totalInitial, initialDocuments)

	suite.T().Logf("Initial distribution (2 members): %v, total: %d", initialCounts, totalInitial)
	suite.T().Logf("Inserted %d documents, processed %d messages", initialDocuments, totalInitial)

	suite.T().Log("Phase 2: Adding third member for rebalancing test")

	collectors[2] = NewMessageCollector()
	connector, _, cancel := suite.startConnectorWithReadinessCheck(collectors[2])
	connectors[2] = connector
	cancels[2] = cancel

	for i := 0; i < maxConnectorCount; i++ {
		collectors[i].Clear()
	}

	rebalanceDocuments := 30
	for i := 0; i < rebalanceDocuments; i++ {
		doc := bson.M{
			"_id":   i + 21,
			"phase": "after_rebalance",
		}
		_, err := suite.insertTestDocument(doc)
		require.NoError(suite.T(), err)
		time.Sleep(50 * time.Millisecond)
	}

	suite.waitForMessageProcessing(collectors[:maxConnectorCount], rebalanceDocuments/maxConnectorCount)

	rebalanceCounts := make([]int, maxConnectorCount)
	totalNewMessages := 0
	for i := 0; i < maxConnectorCount; i++ {
		rebalanceCounts[i] = collectors[i].MessageCount()
		assert.Equal(suite.T(), rebalanceDocuments/maxConnectorCount, collectors[i].MessageCount())

		totalNewMessages += rebalanceCounts[i]
	}

	suite.T().Logf("Rebalance phase NEW messages distribution: %v", rebalanceCounts)
	suite.T().Logf("Total NEW messages processed: %d out of %d sent", totalNewMessages, rebalanceDocuments)
	suite.T().Logf("Third member processed NEW: %d messages", rebalanceCounts[2])

	suite.T().Log("Phase 3: Testing member leave scenario")
	suite.testMemberLeave(connectors, cancels, collectors, maxConnectorCount)
}

func (suite *E2ETestSuite) waitForMessageProcessing(collectors []MessageCollector, expectedTotal int) {
	suite.T().Logf("Waiting for %d messages to be processed...", expectedTotal)

	suite.waitForCondition(func() bool {
		total := 0
		for _, collector := range collectors {
			total += collector.MessageCount()
		}
		return total >= expectedTotal
	}, membershipTimeout, fmt.Sprintf("expected %d messages to be processed", expectedTotal))
}

func (suite *E2ETestSuite) testMemberLeave(connectors []cdc.Connector, cancels []context.CancelFunc, collectors []MessageCollector, maxMembers int) {
	suite.T().Log("Starting member leave test...")

	memberToRemove := maxMembers - 1
	suite.T().Logf("Removing member %d", memberToRemove)

	removedMemberPreviousCount := collectors[memberToRemove].MessageCount()

	if connectors[memberToRemove] != nil {
		connectors[memberToRemove].Close()
		connectors[memberToRemove] = nil
	}
	if cancels[memberToRemove] != nil {
		cancels[memberToRemove]()
		cancels[memberToRemove] = nil
	}

	suite.T().Log("Waiting for stream restart to complete...")
	time.Sleep(1 * time.Second)

	// Clear remaining collectors for member leave test
	for i := 0; i < maxMembers-1; i++ {
		collectors[i].Clear()
	}

	leaveTestDocuments := 20
	for i := 0; i < leaveTestDocuments; i++ {
		doc := bson.M{
			"_id":   i + 51,
			"phase": "after_member_leave",
		}
		_, err := suite.insertTestDocument(doc)
		require.NoError(suite.T(), err)
		time.Sleep(50 * time.Millisecond)
	}

	suite.waitForCondition(func() bool {
		total := 0
		for i := 0; i < maxMembers-1; i++ {
			total += suite.countMessagesByPhase(collectors[i], "after_member_leave")
		}
		return total >= leaveTestDocuments
	}, membershipTimeout, "remaining members should process all after_member_leave messages")

	postLeaveCounts := make([]int, maxMembers-1)
	totalAfterLeave := 0
	for i := 0; i < maxMembers-1; i++ {
		postLeaveCounts[i] = suite.countMessagesByPhase(collectors[i], "after_member_leave")
		totalAfterLeave += postLeaveCounts[i]
	}

	suite.T().Logf("Post-leave distribution: %v, total: %d", postLeaveCounts, totalAfterLeave)

	assert.Equal(suite.T(), leaveTestDocuments, totalAfterLeave,
		"All after_member_leave documents should be processed by remaining members")

	assert.Equal(suite.T(), removedMemberPreviousCount, collectors[memberToRemove].MessageCount(),
		"Removed member should not process any new messages")

	suite.T().Log("Member leave test completed successfully")
}

func (suite *E2ETestSuite) countMessagesByPhase(collector MessageCollector, phase string) int {
	messages := collector.GetMessages()
	count := 0
	for _, msg := range messages {
		if msg.IsInsert() && msg.FullDocument != nil {
			if v, ok := msg.FullDocument["phase"]; ok {
				if s, ok := v.(string); ok && s == phase {
					count++
				}
			}
		}
	}
	return count
}
