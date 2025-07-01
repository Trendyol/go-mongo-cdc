package integration

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

const (
	defaultStepTimeout       = 10 * time.Second
	defaultConcurrentTimeout = 30 * time.Second

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
