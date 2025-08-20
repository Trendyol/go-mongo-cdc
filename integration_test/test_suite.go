package integration

import (
	"context"
	goerrors "errors"
	"fmt"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	cdc "github.com/Trendyol/go-mongo-cdc"
	"github.com/Trendyol/go-mongo-cdc/config"
	"github.com/Trendyol/go-mongo-cdc/internal/metric"
	"github.com/Trendyol/go-mongo-cdc/logger"
	"github.com/Trendyol/go-mongo-cdc/mongo/changestream"
	"github.com/Trendyol/go-mongo-cdc/mongo/connection"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/suite"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/mongodb"
	"github.com/testcontainers/testcontainers-go/wait"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
	"go.uber.org/zap"
)

type CDCTestSuite struct {
	suite.Suite
	mongoContainer testcontainers.Container
	mongoClient    *mongo.Client
	ctx            context.Context
	logger         *zap.Logger
	database       *mongo.Database
	collection     *mongo.Collection
	testDB         string
	testCollection string
	connectionURI  string
}

func (suite *CDCTestSuite) SetupSuite() {
	suite.ctx = context.Background()
	suite.testDB = "test_cdc_db"
	suite.testCollection = "test_collection"

	var err error
	suite.logger, err = zap.NewDevelopment()
	suite.Require().NoError(err)

	suite.setupMongoContainer()
	suite.setupMongoClient()
	suite.initializeReplicaSet()
}

func (suite *CDCTestSuite) TearDownSuite() {
	if suite.mongoClient != nil {
		suite.mongoClient.Disconnect(suite.ctx)
	}
	if suite.mongoContainer != nil {
		suite.mongoContainer.Terminate(suite.ctx)
	}
	if suite.logger != nil {
		suite.logger.Sync()
	}
}

func (suite *CDCTestSuite) SetupTest() {
	err := suite.collection.Drop(suite.ctx)
	suite.Require().NoError(err)

	checkpointCollections := []string{
		"cdc_checkpoints",
		"cdc_checkpoints_chunk_test",
		"cdc_checkpoints_member_test",
		"cdc_checkpoints_member_1",
		"cdc_checkpoints_member_2",
		"cdc_checkpoints_member_3",
	}

	allCollections, err := suite.database.ListCollectionNames(suite.ctx, bson.D{})
	if err == nil {
		for _, collName := range allCollections {
			if strings.HasPrefix(collName, "cdc_checkpoints") {
				checkpointCollection := suite.database.Collection(collName)
				err = checkpointCollection.Drop(suite.ctx)
				if err != nil {
					suite.logger.Debug("Failed to drop pattern-matched checkpoint collection",
						zap.String("collection", collName),
						zap.Error(err))
				} else {
					suite.logger.Debug("Dropped pattern-matched checkpoint collection",
						zap.String("collection", collName))
				}
			}
		}
	}

	for _, collectionName := range checkpointCollections {
		checkpointCollection := suite.database.Collection(collectionName)
		err = checkpointCollection.Drop(suite.ctx)
		if err != nil {
			suite.logger.Debug("Failed to drop checkpoint collection (may not exist)",
				zap.String("collection", collectionName),
				zap.Error(err))
		}
	}

	membershipCollections := []string{
		"test_membership",
		"cdc_membership",
	}

	for _, collectionName := range membershipCollections {
		membershipCollection := suite.database.Collection(collectionName)
		err = membershipCollection.Drop(suite.ctx)
		if err != nil {
			suite.logger.Debug("Failed to drop membership collection (may not exist)",
				zap.String("collection", collectionName),
				zap.Error(err))
		}
	}

	configDB := suite.mongoClient.Database("config")
	configCollections := []string{
		"chunks",
		"collections",
		"databases",
	}

	for _, collectionName := range configCollections {
		configCollection := configDB.Collection(collectionName)
		err = configCollection.Drop(suite.ctx)
		if err != nil {
			suite.logger.Debug("Failed to drop config collection (may not exist)",
				zap.String("collection", collectionName),
				zap.Error(err))
		}
	}

	clusterDB := suite.mongoClient.Database("cdc_cluster")
	clusterCollections := []string{
		"cdc_membership",
	}

	for _, collectionName := range clusterCollections {
		clusterCollection := clusterDB.Collection(collectionName)
		err = clusterCollection.Drop(suite.ctx)
		if err != nil {
			suite.logger.Debug("Failed to drop cluster collection (may not exist)",
				zap.String("collection", collectionName),
				zap.Error(err))
		}
	}
}

func (suite *CDCTestSuite) setupMongoContainer() {
	ctx := context.Background()

	mongoContainer, err := mongodb.Run(ctx,
		"mongo:7.0",
		mongodb.WithReplicaSet("rs0"),
		testcontainers.WithWaitStrategy(
			wait.ForAll(
				wait.ForListeningPort("27017/tcp"),
				wait.ForLog("Waiting for connections"),
				wait.ForExec([]string{"mongosh", "--quiet", "--eval", "db.adminCommand('ping')"}).
					WithExitCodeMatcher(func(exitCode int) bool {
						return exitCode == 0
					}).
					WithStartupTimeout(60*time.Second),
				wait.ForExec([]string{"mongosh", "--quiet", "--eval",
					"let status = rs.status(); if (status.myState === 1) { print('PRIMARY_READY'); } else { throw new Error('Not primary yet'); }"}).
					WithExitCodeMatcher(func(exitCode int) bool {
						return exitCode == 0
					}).
					WithStartupTimeout(120*time.Second),
			).WithStartupTimeout(180*time.Second),
		),
	)
	suite.Require().NoError(err)

	suite.mongoContainer = mongoContainer

	connectionString, err := mongoContainer.ConnectionString(ctx)
	suite.Require().NoError(err)
	suite.connectionURI = connectionString

	suite.logger.Info("MongoDB container started", zap.String("connectionURI", suite.connectionURI))
}

func (suite *CDCTestSuite) setupMongoClient() {
	ctx, cancel := context.WithTimeout(suite.ctx, 60*time.Second)
	defer cancel()

	modifiedURI := suite.buildDirectConnectionURI()

	clientOptions := options.Client().ApplyURI(modifiedURI)
	clientOptions.SetConnectTimeout(20 * time.Second)
	clientOptions.SetServerSelectionTimeout(30 * time.Second)
	clientOptions.SetRetryWrites(false)
	clientOptions.SetDirect(true)
	clientOptions.SetReadPreference(readpref.Primary())

	suite.logger.Info("Attempting to connect to MongoDB",
		zap.String("originalURI", suite.connectionURI),
		zap.String("modifiedURI", modifiedURI))

	mongoClient, err := mongo.Connect(ctx, clientOptions)
	suite.Require().NoError(err, "Failed to create MongoDB client")

	suite.logger.Info("Connected to MongoDB, attempting ping...")
	err = mongoClient.Ping(ctx, nil)
	suite.Require().NoError(err, "Failed to ping MongoDB")

	suite.logger.Info("MongoDB ping successful")
	suite.mongoClient = mongoClient
	suite.database = mongoClient.Database(suite.testDB)
	suite.collection = suite.database.Collection(suite.testCollection)
}

func (suite *CDCTestSuite) buildDirectConnectionURI() string {
	host, port := suite.parseConnectionURI()

	directURI := fmt.Sprintf("mongodb://%s:%d/?directConnection=true&readPreference=primary", host, port)

	suite.logger.Info("Built direct connection URI",
		zap.String("directURI", directURI))

	return directURI
}

func (suite *CDCTestSuite) initializeReplicaSet() {
	ctx, cancel := context.WithTimeout(suite.ctx, 60*time.Second)
	defer cancel()

	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	suite.logger.Info("Waiting for replica set to be ready...")

	for {
		select {
		case <-ctx.Done():
			suite.FailNow("Timeout waiting for replica set to be ready")
		case <-ticker.C:
			if suite.isReplicaSetReady() {
				suite.logger.Info("Replica set is ready")

				_, err := suite.database.RunCommand(suite.ctx, bson.D{{Key: "isMaster", Value: 1}}).Raw()
				if err == nil {
					suite.logger.Info("Replica set verification successful")
					return
				}
				suite.logger.Warn("Replica set verification failed, retrying...", zap.Error(err))
			}
		}
	}
}

func (suite *CDCTestSuite) isReplicaSetReady() bool {
	ctx, cancel := context.WithTimeout(suite.ctx, 5*time.Second)
	defer cancel()

	result := suite.database.RunCommand(ctx, bson.D{{Key: "isMaster", Value: 1}})

	var response bson.M
	if err := result.Decode(&response); err != nil {
		suite.logger.Debug("isMaster command failed", zap.Error(err))
		return false
	}

	setName, hasSetName := response["setName"]
	if !hasSetName || setName == nil {
		suite.logger.Debug("No replica set found")
		return false
	}

	ismaster, hasIsMaster := response["ismaster"]
	if !hasIsMaster {
		suite.logger.Debug("ismaster field not found")
		return false
	}

	isPrimary := ismaster.(bool)
	suite.logger.Debug("Replica set status",
		zap.String("setName", setName.(string)),
		zap.Bool("isPrimary", isPrimary))

	return isPrimary
}

func (suite *CDCTestSuite) createTestConfig() config.Config {
	host, port := suite.parseConnectionURI()

	cfg := config.Config{
		Host:       host,
		Port:       port,
		Database:   suite.testDB,
		Collection: suite.testCollection,
		DebugMode:  true,
		Metric: config.MetricConfig{
			Port: 8080,
		},
		Logger: config.LoggerConfig{
			Logger: suite.logger,
		},
		Checkpoint: config.CheckpointConfig{
			Collection:   "cdc_checkpoints",
			SaveInterval: 1 * time.Second,
		},
	}

	return cfg
}

func (suite *CDCTestSuite) parseConnectionURI() (string, int) {
	host := "localhost"
	port := 27017

	// Parse connection URI: mongodb://localhost:51335/?replicaSet=rs0
	if suite.connectionURI != "" {
		uri := strings.TrimPrefix(suite.connectionURI, "mongodb://")

		parts := strings.Split(uri, "/")
		if len(parts) > 0 {
			hostPort := parts[0]
			hostPortParts := strings.Split(hostPort, ":")
			if len(hostPortParts) == 2 {
				host = hostPortParts[0]
				if parsedPort, err := strconv.Atoi(hostPortParts[1]); err == nil {
					port = parsedPort
				}
			}
		}
	}

	suite.logger.Info("Parsed connection details",
		zap.String("host", host),
		zap.Int("port", port),
		zap.String("originalURI", suite.connectionURI))

	return host, port
}

func (suite *CDCTestSuite) createTestConnector(listenerFunc changestream.ListenerFunc) (cdc.Connector, error) {
	cfg := suite.createTestConfig()

	cfg.SetDefault()
	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	zapLogger := logger.InitLogger(cfg.Logger.Logger)

	host, port := suite.parseConnectionURI()
	externalURI := fmt.Sprintf("mongodb://%s:%d/?directConnection=true&readPreference=primary", host, port)

	suite.logger.Info("Using external mapped URI for CDC (same as test client)",
		zap.String("externalURI", externalURI),
		zap.String("host", host),
		zap.Int("port", port))

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
		logger:             zapLogger,
		cancelCh:           make(chan os.Signal, 1),
		readyCh:            make(chan struct{}, 1),
	}, nil
}

type testConnector struct {
	stream             changestream.Streamer
	prometheusRegistry metric.Registry
	cfg                *config.Config
	cancelCh           chan os.Signal
	readyCh            chan struct{}
	mongoClient        connection.Client
	logger             *zap.Logger
	once               sync.Once
	mu                 sync.RWMutex
	closed             bool
}

func (c *testConnector) Start(ctx context.Context) {
	c.logger.Info("Starting MongoDB Change Stream watcher...")

	go func() {
		retryCount := 0
		maxRetries := 5

		for {
			err := c.stream.Open(ctx)
			if err == nil {
				c.logger.Info("MongoDB stream completed normally")
				return
			}

			if goerrors.Is(err, changestream.ErrorStreamInUse) {
				c.logger.Info("Stream capture failed, retrying...")
				time.Sleep(5 * time.Second)
				continue
			}

			if goerrors.Is(err, context.Canceled) {
				c.once.Do(func() {
					if !isClosed(c.readyCh) {
						c.readyCh <- struct{}{}
					}
				})

				c.mu.Lock()
				isClosed := c.closed
				c.mu.Unlock()

				if isClosed || ctx.Err() != nil {
					c.logger.Info("Stream stopped due to shutdown")
					return
				}

				c.logger.Info("Stream restarting due to membership change")
				time.Sleep(1 * time.Second)
				retryCount = 0 // Reset retry count for membership changes
				continue
			}

			// Resume token hatası için sınırlı retry
			if err != nil && (strings.Contains(err.Error(), "resume token") ||
				strings.Contains(err.Error(), "ChangeStreamFatalError") ||
				strings.Contains(err.Error(), "PlanExecutor")) {
				retryCount++
				if retryCount > maxRetries {
					c.logger.Error("Max retries reached for resume token errors, stopping", zap.Int("retries", retryCount))
					return
				}
				c.logger.Warn("Resume token error, retrying", zap.Error(err), zap.Int("attempt", retryCount))
				time.Sleep(2 * time.Second)
				continue
			}

			if ctx.Err() != nil {
				c.logger.Info("Stream stopping due to context cancellation")
				return
			}

			c.logger.Error("MongoDB stream open error", zap.Error(err))
			time.Sleep(5 * time.Second)
		}
	}()

	// İlk ready sinyali için
	c.once.Do(func() {
		go func() {
			time.Sleep(2 * time.Second) // Stream'in başlaması için kısa bekleme
			if !isClosed(c.readyCh) {
				c.readyCh <- struct{}{}
			}
		}()
	})

	signal.Notify(c.cancelCh, syscall.SIGTERM, syscall.SIGINT, syscall.SIGABRT, syscall.SIGQUIT)

	<-c.cancelCh
	c.logger.Debug("Cancel channel triggered")
}

func (c *testConnector) WaitUntilReady(ctx context.Context) error {
	select {
	case <-c.readyCh:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (c *testConnector) Close() {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.logger.Info("Closing test connector")

	if c.closed {
		c.logger.Info("Already closed, skipping cleanup")
		return
	}

	c.closed = true

	if !isClosed(c.cancelCh) {
		close(c.cancelCh)
	}
	if !isClosed(c.readyCh) {
		close(c.readyCh)
	}

	closeCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := c.stream.Close(closeCtx); err != nil {
		c.logger.Error("Failed to close stream", zap.Error(err))
	}

	c.logger.Info("Closing mongo client")
	if err := c.mongoClient.Close(closeCtx); err != nil {
		c.logger.Error("Failed to close mongo client", zap.Error(err))
	}

	c.logger.Info("Test connector closed")
}

func (c *testConnector) GetConfig() *config.Config {
	return c.cfg
}

func (c *testConnector) SetMetricCollectors(metricCollectors ...prometheus.Collector) {
	c.prometheusRegistry.AddMetricCollectors(metricCollectors...)
}

func isClosed[T any](ch <-chan T) bool {
	select {
	case <-ch:
		return true
	default:
		return false
	}
}

func (suite *CDCTestSuite) insertTestDocument(doc bson.M) (*mongo.InsertOneResult, error) {
	return suite.collection.InsertOne(suite.ctx, doc)
}

func (suite *CDCTestSuite) updateTestDocument(filter, update bson.M) (*mongo.UpdateResult, error) {
	return suite.collection.UpdateOne(suite.ctx, filter, bson.M{"$set": update})
}

func (suite *CDCTestSuite) deleteTestDocument(filter bson.M) (*mongo.DeleteResult, error) {
	return suite.collection.DeleteOne(suite.ctx, filter)
}

func (suite *CDCTestSuite) waitForCondition(condition func() bool, timeout time.Duration, message string) {
	timer := time.After(timeout)
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-timer:
			suite.FailNow(fmt.Sprintf("Timeout waiting for condition: %s", message))
		case <-ticker.C:
			if condition() {
				return
			}
		}
	}
}

func (suite *CDCTestSuite) startConnectorWithReadinessCheck(collector MessageCollector) (cdc.Connector, context.Context, context.CancelFunc) {
	connector, err := suite.createTestConnector(collector.CollectMessage)
	suite.Require().NoError(err)

	ctx, cancel := context.WithCancel(suite.ctx)
	go connector.Start(ctx)

	suite.logger.Info("Starting CDC connector...")
	time.Sleep(3 * time.Second)

	suite.logger.Info("CDC connector ready for testing")
	return connector, ctx, cancel
}

func (suite *CDCTestSuite) generateUniqueCheckpointName(prefix string) string {
	timestamp := time.Now().UnixNano()
	return fmt.Sprintf("%s_%d", prefix, timestamp)
}
