package main

import (
	"context"
	"fmt"
	cdc "github.com/Trendyol/go-mongo-cdc"
	"github.com/Trendyol/go-mongo-cdc/config"
	"github.com/Trendyol/go-mongo-cdc/logger"
	"github.com/Trendyol/go-mongo-cdc/mongo/message"
	"github.com/Trendyol/go-mongo-cdc/stream"
	"log"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

func main() {
	time.Sleep(15 * time.Second)

	// Seed initial data BEFORE starting CDC connector
	// This ensures bootstrap will process all existing documents
	log.Println("Seeding initial data before CDC starts...")
	seedInitialData()
	log.Println("Initial data seeding completed, starting CDC connector...")

	// Start continuous seeding in background
	go continuousSeedMongoDB()

	cfg := config.Config{
		MongoDB: config.MongoDB{
			Connection: config.Connection{
				URI:        "mongodb:27017",
				Database:   "exampleDB",
				Collection: "exampleCollection",
			},
			ConnectionPool: config.ConnectionPool{
				MaxPoolSize:   100,
				MinPoolSize:   5,
				MaxIdleTimeMS: 300000,
			},
			Timeouts: config.Timeouts{
				ConnectTimeoutMS:         30000,
				ServerSelectionTimeoutMS: 60000,
				SocketTimeoutMS:          120000,
			},
		},
		Metric: config.MetricConfig{
			Port:               8080,
			CollectionInterval: 2 * time.Second,
		},
		Checkpoint: config.CheckpointConfig{
			TokenSaveInterval:     5 * time.Second,
			ChangeStreamBatchSize: 100,
			BootstrapSaveCount:    1000,
			BootstrapSaveInterval: 10 * time.Second,
			BootstrapBatchSize:    1000,
		},
		Partition: config.PartitionConfig{
			HeartbeatInterval:      10 * time.Second,
			WorkerTimeout:          90 * time.Second,
			RebalanceCheckInterval: 10 * time.Second,
			TotalPartition:         10,
		},
		Logger: config.LoggerConfig{LogLevel: "debug"},
	}

	connector, err := cdc.NewConnector(cfg, ProcessChangeEvent)
	if err != nil {
		log.Fatal("failed to create connector:", err)
	}

	defer connector.Close()

	ctx := context.Background()
	connector.Start(ctx)
}

func seedInitialData() {
	ctx := context.Background()

	clientOptions := options.Client().ApplyURI("mongodb://mongodb:27017")
	client, err := mongo.Connect(ctx, clientOptions)
	if err != nil {
		log.Fatal("failed to connect to MongoDB:", err)
	}
	defer client.Disconnect(ctx)

	err = client.Ping(ctx, nil)
	if err != nil {
		log.Fatal("failed to ping MongoDB:", err)
	}

	collection := client.Database("exampleDB").Collection("exampleCollection")

	// Check if data already exists
	count, err := collection.CountDocuments(ctx, bson.M{})
	if err != nil {
		log.Printf("Error counting documents: %v", err)
	}

	if count > 0 {
		log.Printf("Collection already has %d documents, skipping initial seeding", count)
		return
	}

	// Initial bootstrap data - 20,000 documents
	const initialDocCount = 20000
	log.Printf("Starting initial data seeding: %d documents...", initialDocCount)

	// Batch insert for better performance
	batchSize := 1000
	for i := 0; i < initialDocCount; i += batchSize {
		var docs []interface{}
		for j := 0; j < batchSize && (i+j) < initialDocCount; j++ {
			docNum := i + j + 1
			docs = append(docs, bson.M{
				"_id":       fmt.Sprintf("doc-%d", docNum),
				"counter":   docNum,
				"message":   "Initial Bootstrap Data",
				"category":  fmt.Sprintf("category-%d", docNum%10),
				"status":    "active",
				"timestamp": time.Now(),
			})
		}

		_, err := collection.InsertMany(ctx, docs, options.InsertMany().SetOrdered(false))
		if err != nil {
			log.Printf("Error inserting batch starting at %d: %v", i, err)
		} else {
			log.Printf("Inserted batch: %d-%d documents", i+1, i+len(docs))
		}
	}

	log.Printf("Initial data seeding completed: %d documents inserted", initialDocCount)
}

func continuousSeedMongoDB() {
	// Wait for CDC to start and bootstrap to complete
	time.Sleep(5 * time.Second)

	ctx := context.Background()

	clientOptions := options.Client().ApplyURI("mongodb://mongodb:27017")
	client, err := mongo.Connect(ctx, clientOptions)
	if err != nil {
		log.Fatal("failed to connect to MongoDB:", err)
	}
	defer client.Disconnect(ctx)

	err = client.Ping(ctx, nil)
	if err != nil {
		log.Fatal("failed to ping MongoDB:", err)
	}

	collection := client.Database("exampleDB").Collection("exampleCollection")

	log.Println("Starting continuous data operations...")

	// Continuous operations starting from 20001
	counter := 20000
	for {
		counter++
		documentID := fmt.Sprintf("doc-%d", counter)

		document := bson.M{
			"_id":       documentID,
			"counter":   counter,
			"message":   "Continuous Operation",
			"category":  fmt.Sprintf("category-%d", counter%10),
			"status":    "active",
			"timestamp": time.Now(),
		}

		opts := options.Update().SetUpsert(true)
		filter := bson.M{"_id": documentID}
		update := bson.M{"$set": document}

		_, err := collection.UpdateOne(ctx, filter, update, opts)
		if err != nil {
			log.Printf("Error upserting document: %v", err)
		} else {
			log.Printf("Upserted document: %s", documentID)
		}

		// Every 10 operations, delete an old document
		if counter%10 == 0 {
			deleteID := fmt.Sprintf("doc-%d", counter-5)
			_, err := collection.DeleteOne(ctx, bson.M{"_id": deleteID})
			if err != nil {
				log.Printf("Error deleting document: %v", err)
			} else {
				log.Printf("Deleted document: %s", deleteID)
			}
		}

		// Every 20 operations, update a random existing document
		if counter%20 == 0 {
			updateID := fmt.Sprintf("doc-%d", counter-15)
			updateDoc := bson.M{
				"$set": bson.M{
					"message":      "Updated Document",
					"status":       "updated",
					"last_updated": time.Now(),
				},
			}
			_, err := collection.UpdateOne(ctx, bson.M{"_id": updateID}, updateDoc)
			if err != nil {
				log.Printf("Error updating document: %v", err)
			} else {
				log.Printf("Updated document: %s", updateID)
			}
		}

		time.Sleep(25 * time.Millisecond)
	}
}

func ProcessChangeEvent(lc *stream.ListenerContext) error {
	select {
	case <-lc.Context.Done():
		logger.Log.Info("Shutdown signal received, stopping event processing")
		return lc.Context.Err()
	default:
	}

	switch lc.Message.OperationType {
	case message.OperationInsert, message.OperationUpdate, message.OperationReplace:
		if lc.Message.FullDocument != nil {
			logger.Log.Info("Document changed - operation: %s, document: %v, partitionId: %d", string(lc.Message.OperationType), lc.Message.DocumentID, lc.PartitionID)
		}
	case message.OperationDelete:
		logger.Log.Info("Document deleted - documentId: %v, partitionId: %d", lc.Message.DocumentID, lc.PartitionID)
	}

	if err := lc.Ack(); err != nil {
		logger.Log.Error("Failed to acknowledge message: %v", err)
		return err
	}
	return nil
}
