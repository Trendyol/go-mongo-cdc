package connection

import (
	"context"
	"fmt"
	"time"

	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
)

type Client interface {
	Database(name string) Database
	Close(ctx context.Context) error
	Ping(ctx context.Context) error
}

type Database interface {
	Collection(name string) Collection
	RunCommand(ctx context.Context, runCommand interface{}) SingleResult
}

type Collection interface {
	Watch(ctx context.Context, pipeline interface{}, opts ...*options.ChangeStreamOptions) ChangeStream
	FindOne(ctx context.Context, filter interface{}, opts ...*options.FindOneOptions) SingleResult
	Find(ctx context.Context, filter interface{}, opts ...*options.FindOptions) (Cursor, error)
	UpdateOne(
		ctx context.Context,
		filter interface{},
		update interface{},
		opts ...*options.UpdateOptions,
	) (UpdateResult, error)
	DeleteOne(ctx context.Context, filter interface{}, opts ...*options.DeleteOptions) (DeleteResult, error)
	DeleteMany(ctx context.Context, filter interface{}, opts ...*options.DeleteOptions) (DeleteResult, error)
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

type UpdateResult interface {
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

type mongoClientImpl struct {
	client *mongo.Client
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

type mongoUpdateResultImpl struct {
	ur *mongo.UpdateResult
}

type mongoDeleteResultImpl struct {
	dr *mongo.DeleteResult
}

type mongoIndexViewImpl struct {
	iv mongo.IndexView
}

func NewConnection(ctx context.Context, uri string) (Client, error) {
	clientOptions := options.Client().ApplyURI(uri)

	clientOptions.SetConnectTimeout(10 * time.Second)
	clientOptions.SetServerSelectionTimeout(5 * time.Second)

	client, err := mongo.Connect(ctx, clientOptions)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to MongoDB: %w", err)
	}

	if err := client.Ping(ctx, readpref.Primary()); err != nil {
		return nil, fmt.Errorf("failed to ping MongoDB: %w", err)
	}

	return &mongoClientImpl{client: client}, nil
}

func (c *mongoClientImpl) Database(name string) Database {
	return &mongoDatabaseImpl{db: c.client.Database(name)}
}

func (c *mongoClientImpl) Close(ctx context.Context) error {
	return c.client.Disconnect(ctx)
}

func (c *mongoClientImpl) Ping(ctx context.Context) error {
	return c.client.Ping(ctx, readpref.Primary())
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
) ChangeStream {
	cs, err := c.coll.Watch(ctx, pipeline, opts...)
	if err != nil {
		panic(err) // TODO For now, we'll panic, but this should be handled better
	}
	return &mongoChangeStreamImpl{cs: cs}
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

func (ur *mongoUpdateResultImpl) ModifiedCount() int64 {
	return ur.ur.ModifiedCount
}

func (ur *mongoUpdateResultImpl) UpsertedCount() int64 {
	return ur.ur.UpsertedCount
}

func (ur *mongoUpdateResultImpl) UpsertedID() interface{} {
	return ur.ur.UpsertedID
}

func (dr *mongoDeleteResultImpl) DeletedCount() int64 {
	return dr.dr.DeletedCount
}

func (iv *mongoIndexViewImpl) CreateMany(ctx context.Context, models []mongo.IndexModel, opts ...*options.CreateIndexesOptions) ([]string, error) {
	return iv.iv.CreateMany(ctx, models, opts...)
}
