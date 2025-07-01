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
	"github.com/Trendyol/go-mongo-cdc/internal/http"
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
	"go.mongodb.org/mongo-driver/bson/primitive"
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

	checkpointCollection := suite.database.Collection("cdc_checkpoints")
	err = checkpointCollection.Drop(suite.ctx)
	suite.Require().NoError(err)
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
			Collection:                 "cdc_checkpoints",
			SaveInterval:               1 * time.Second,
			ResumeTokenRefreshInterval: 2 * time.Second,
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
		server:             http.NewServer(cfg, prometheusRegistry),
		logger:             zapLogger,
		cancelCh:           make(chan os.Signal, 1),
		readyCh:            make(chan struct{}, 1),
	}, nil
}

type testConnector struct {
	stream             changestream.Streamer
	prometheusRegistry metric.Registry
	server             http.Server
	cfg                *config.Config
	cancelCh           chan os.Signal
	readyCh            chan struct{}
	mongoClient        connection.Client
	logger             *zap.Logger
	once               sync.Once
}

func (c *testConnector) Start(ctx context.Context) {
	c.once.Do(func() {
		go c.server.Listen()
	})

	c.logger.Info("Starting MongoDB Change Stream watcher...")

	err := c.stream.Open(ctx)
	if err != nil {
		if goerrors.Is(err, changestream.ErrorStreamInUse) {
			c.logger.Info("Stream capture failed, retrying...")
			time.Sleep(5 * time.Second)
			c.Start(ctx)
			return
		}
		c.logger.Error("MongoDB stream open error", zap.Error(err))
		return
	}

	c.logger.Info("MongoDB Change Stream started successfully")

	signal.Notify(c.cancelCh, syscall.SIGTERM, syscall.SIGINT, syscall.SIGABRT, syscall.SIGQUIT)

	c.readyCh <- struct{}{}

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
	if !isClosed(c.cancelCh) {
		close(c.cancelCh)
	}
	if !isClosed(c.readyCh) {
		close(c.readyCh)
	}

	c.stream.Close(context.TODO())
	c.mongoClient.Close(context.TODO())
	c.server.Shutdown()
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

func (suite *CDCTestSuite) waitForCDCReady(collector MessageCollector, timeout time.Duration) {
	suite.logger.Info("Testing CDC readiness...")

	initialCount := collector.MessageCount()

	testDoc := bson.M{
		"_id":       primitive.NewObjectID(),
		"testType":  "cdc_readiness_check",
		"timestamp": time.Now(),
	}

	_, err := suite.insertTestDocument(testDoc)
	suite.Require().NoError(err, "Failed to insert CDC readiness test document")

	suite.waitForCondition(func() bool {
		return collector.MessageCount() > initialCount
	}, timeout, "CDC to capture readiness test document")

	suite.logger.Info("CDC readiness confirmed")

	collector.Clear()
}

func (suite *CDCTestSuite) startConnectorWithReadinessCheck(collector MessageCollector) (cdc.Connector, context.Context, context.CancelFunc) {
	connector, err := suite.createTestConnector(collector.CollectMessage)
	suite.Require().NoError(err)

	ctx, cancel := context.WithCancel(suite.ctx)
	go connector.Start(ctx)

	suite.logger.Info("Starting CDC connector...")
	time.Sleep(3 * time.Second)

	suite.waitForCDCReady(collector, 15*time.Second)
	suite.logger.Info("CDC connector ready for testing")
	return connector, ctx, cancel
}
