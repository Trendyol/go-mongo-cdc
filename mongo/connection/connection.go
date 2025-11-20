package connection

import (
	"context"
	"time"

	"github.com/Trendyol/go-mongo-cdc/config"

	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
)

type Client interface {
	Database(name string) Database
	Close(ctx context.Context) error
	Ping(ctx context.Context) error
}

type MongoClient struct {
	client *mongo.Client
}

func (m *MongoClient) GetClient() *mongo.Client {
	return m.client
}

func (m *MongoClient) Database(name string) Database {
	return &mongoDatabaseImpl{db: m.client.Database(name)}
}

func (m *MongoClient) Close(ctx context.Context) error {
	return m.client.Disconnect(ctx)
}

func (m *MongoClient) Ping(ctx context.Context) error {
	return m.client.Ping(ctx, readpref.Primary())
}

type Database interface {
	Collection(name string) Collection
	RunCommand(ctx context.Context, runCommand interface{}) SingleResult
}

type Collection interface {
	Watch(ctx context.Context, pipeline interface{}, opts ...*options.ChangeStreamOptions) (ChangeStream, error)
	FindOne(ctx context.Context, filter interface{}, opts ...*options.FindOneOptions) SingleResult
	Find(ctx context.Context, filter interface{}, opts ...*options.FindOptions) (Cursor, error)
	InsertOne(ctx context.Context, document interface{}, opts ...*options.InsertOneOptions) (InsertResult, error)
	UpdateOne(
		ctx context.Context,
		filter interface{},
		update interface{},
		opts ...*options.UpdateOptions,
	) (UpdateResult, error)
	UpdateMany(
		ctx context.Context,
		filter interface{},
		update interface{},
		opts ...*options.UpdateOptions,
	) (UpdateResult, error)
	FindOneAndUpdate(
		ctx context.Context,
		filter interface{},
		update interface{},
		opts ...*options.FindOneAndUpdateOptions,
	) SingleResult
	DeleteOne(ctx context.Context, filter interface{}, opts ...*options.DeleteOptions) (DeleteResult, error)
	DeleteMany(ctx context.Context, filter interface{}, opts ...*options.DeleteOptions) (DeleteResult, error)
	CountDocuments(ctx context.Context, filter interface{}, opts ...*options.CountOptions) (int64, error)
	Indexes() IndexView
}

type ChangeStream interface {
	Next(ctx context.Context) bool
	Decode(val interface{}) error
	Err() error
	Close(ctx context.Context) error
	ResumeToken() []byte
}

type SingleResult interface {
	Decode(v interface{}) error
	Err() error
}

type Cursor interface {
	Next(ctx context.Context) bool
	Decode(val interface{}) error
	Close(ctx context.Context) error
	Err() error
}

type InsertResult interface {
	InsertedID() interface{}
}

type UpdateResult interface {
	MatchedCount() int64
	ModifiedCount() int64
	UpsertedCount() int64
	UpsertedID() interface{}
}

type DeleteResult interface {
	DeletedCount() int64
}

type IndexView interface {
	CreateMany(ctx context.Context, models []mongo.IndexModel, opts ...*options.CreateIndexesOptions) ([]string, error)
}

type mongoDatabaseImpl struct {
	db *mongo.Database
}

type mongoCollectionImpl struct {
	coll *mongo.Collection
}

type mongoChangeStreamImpl struct {
	cs *mongo.ChangeStream
}

type mongoSingleResultImpl struct {
	sr *mongo.SingleResult
}

type mongoCursorImpl struct {
	cursor *mongo.Cursor
}

type mongoInsertResultImpl struct {
	ir *mongo.InsertOneResult
}

type mongoUpdateResultImpl struct {
	ur *mongo.UpdateResult
}

type mongoDeleteResultImpl struct {
	dr *mongo.DeleteResult
}

type mongoIndexViewImpl struct {
	iv mongo.IndexView
}

func NewMongoClient(cfg config.MongoDB) (Client, error) {
	ctx := context.Background()

	clientOpts := options.Client().ApplyURI("mongodb://" + cfg.Connection.URI)
	clientOpts.SetRetryWrites(true)
	clientOpts.SetRetryReads(true)

	if cfg.Connection.Username != "" && cfg.Connection.Password != "" {
		clientOpts.SetAuth(options.Credential{
			Username:   cfg.Connection.Username,
			Password:   cfg.Connection.Password,
			AuthSource: "admin",
		})
	}

	clientOpts.SetMaxPoolSize(cfg.ConnectionPool.MaxPoolSize)
	clientOpts.SetMinPoolSize(cfg.ConnectionPool.MinPoolSize)
	clientOpts.SetMaxConnIdleTime(time.Duration(cfg.ConnectionPool.MaxIdleTimeMS) * time.Millisecond)

	clientOpts.SetConnectTimeout(time.Duration(cfg.Timeouts.ConnectTimeoutMS) * time.Millisecond)
	clientOpts.SetServerSelectionTimeout(time.Duration(cfg.Timeouts.ServerSelectionTimeoutMS) * time.Millisecond)
	clientOpts.SetSocketTimeout(time.Duration(cfg.Timeouts.SocketTimeoutMS) * time.Millisecond)

	client, err := mongo.Connect(ctx, clientOpts)
	if err != nil {
		return nil, err
	}

	pingCtx, pingCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer pingCancel()

	if err = client.Ping(pingCtx, readpref.Primary()); err != nil {
		errDisc := client.Disconnect(ctx)
		if errDisc != nil {
			return nil, errDisc
		}

		return nil, err
	}

	return &MongoClient{client: client}, nil
}

func (d *mongoDatabaseImpl) Collection(name string) Collection {
	return &mongoCollectionImpl{coll: d.db.Collection(name)}
}

func (d *mongoDatabaseImpl) RunCommand(ctx context.Context, runCommand interface{}) SingleResult {
	return &mongoSingleResultImpl{sr: d.db.RunCommand(ctx, runCommand)}
}

func (c *mongoCollectionImpl) Watch(
	ctx context.Context,
	pipeline interface{},
	opts ...*options.ChangeStreamOptions,
) (ChangeStream, error) {
	cs, err := c.coll.Watch(ctx, pipeline, opts...)
	if err != nil {
		return nil, err
	}
	return &mongoChangeStreamImpl{cs: cs}, nil
}

func (c *mongoCollectionImpl) FindOne(
	ctx context.Context,
	filter interface{},
	opts ...*options.FindOneOptions,
) SingleResult {
	return &mongoSingleResultImpl{sr: c.coll.FindOne(ctx, filter, opts...)}
}

func (c *mongoCollectionImpl) Find(
	ctx context.Context,
	filter interface{},
	opts ...*options.FindOptions,
) (Cursor, error) {
	cursor, err := c.coll.Find(ctx, filter, opts...)
	if err != nil {
		return nil, err
	}
	return &mongoCursorImpl{cursor: cursor}, nil
}

func (c *mongoCollectionImpl) InsertOne(
	ctx context.Context,
	document interface{},
	opts ...*options.InsertOneOptions,
) (InsertResult, error) {
	result, err := c.coll.InsertOne(ctx, document, opts...)
	if err != nil {
		return nil, err
	}
	return &mongoInsertResultImpl{ir: result}, nil
}

func (c *mongoCollectionImpl) UpdateOne(
	ctx context.Context,
	filter interface{},
	update interface{},
	opts ...*options.UpdateOptions,
) (UpdateResult, error) {
	result, err := c.coll.UpdateOne(ctx, filter, update, opts...)
	if err != nil {
		return nil, err
	}
	return &mongoUpdateResultImpl{ur: result}, nil
}

func (c *mongoCollectionImpl) DeleteOne(
	ctx context.Context,
	filter interface{},
	opts ...*options.DeleteOptions,
) (DeleteResult, error) {
	result, err := c.coll.DeleteOne(ctx, filter, opts...)
	if err != nil {
		return nil, err
	}
	return &mongoDeleteResultImpl{dr: result}, nil
}

func (c *mongoCollectionImpl) DeleteMany(
	ctx context.Context,
	filter interface{},
	opts ...*options.DeleteOptions,
) (DeleteResult, error) {
	result, err := c.coll.DeleteMany(ctx, filter, opts...)
	if err != nil {
		return nil, err
	}
	return &mongoDeleteResultImpl{dr: result}, nil
}

func (c *mongoCollectionImpl) UpdateMany(
	ctx context.Context,
	filter interface{},
	update interface{},
	opts ...*options.UpdateOptions,
) (UpdateResult, error) {
	result, err := c.coll.UpdateMany(ctx, filter, update, opts...)
	if err != nil {
		return nil, err
	}
	return &mongoUpdateResultImpl{ur: result}, nil
}

func (c *mongoCollectionImpl) FindOneAndUpdate(
	ctx context.Context,
	filter interface{},
	update interface{},
	opts ...*options.FindOneAndUpdateOptions,
) SingleResult {
	return &mongoSingleResultImpl{sr: c.coll.FindOneAndUpdate(ctx, filter, update, opts...)}
}

func (c *mongoCollectionImpl) CountDocuments(
	ctx context.Context,
	filter interface{},
	opts ...*options.CountOptions,
) (int64, error) {
	return c.coll.CountDocuments(ctx, filter, opts...)
}

func (c *mongoCollectionImpl) Indexes() IndexView {
	return &mongoIndexViewImpl{iv: c.coll.Indexes()}
}

func (cs *mongoChangeStreamImpl) Next(ctx context.Context) bool {
	return cs.cs.Next(ctx)
}

func (cs *mongoChangeStreamImpl) Decode(val interface{}) error {
	return cs.cs.Decode(val)
}

func (cs *mongoChangeStreamImpl) Err() error {
	return cs.cs.Err()
}

func (cs *mongoChangeStreamImpl) Close(ctx context.Context) error {
	return cs.cs.Close(ctx)
}

func (cs *mongoChangeStreamImpl) ResumeToken() []byte {
	if cs.cs.ResumeToken() == nil {
		return nil
	}
	return cs.cs.ResumeToken()
}

func (sr *mongoSingleResultImpl) Decode(v interface{}) error {
	return sr.sr.Decode(v)
}

func (sr *mongoSingleResultImpl) Err() error {
	return sr.sr.Err()
}

func (c *mongoCursorImpl) Next(ctx context.Context) bool {
	return c.cursor.Next(ctx)
}

func (c *mongoCursorImpl) Decode(val interface{}) error {
	return c.cursor.Decode(val)
}

func (c *mongoCursorImpl) Close(ctx context.Context) error {
	return c.cursor.Close(ctx)
}

func (c *mongoCursorImpl) Err() error {
	return c.cursor.Err()
}

func (ur *mongoUpdateResultImpl) MatchedCount() int64 {
	return ur.ur.MatchedCount
}

func (ur *mongoUpdateResultImpl) ModifiedCount() int64 {
	return ur.ur.ModifiedCount
}

func (ur *mongoUpdateResultImpl) UpsertedCount() int64 {
	return ur.ur.UpsertedCount
}

func (ur *mongoUpdateResultImpl) UpsertedID() interface{} {
	return ur.ur.UpsertedID
}

func (ir *mongoInsertResultImpl) InsertedID() interface{} {
	return ir.ir.InsertedID
}

func (dr *mongoDeleteResultImpl) DeletedCount() int64 {
	return dr.dr.DeletedCount
}

func (iv *mongoIndexViewImpl) CreateMany(
	ctx context.Context,
	models []mongo.IndexModel,
	opts ...*options.CreateIndexesOptions,
) ([]string, error) {
	return iv.iv.CreateMany(ctx, models, opts...)
}
