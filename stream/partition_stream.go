package stream

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/Trendyol/go-mongo-cdc/checkpoint"
	"github.com/Trendyol/go-mongo-cdc/config"
	"github.com/Trendyol/go-mongo-cdc/internal/metric"
	"github.com/Trendyol/go-mongo-cdc/mongo/connection"
	"github.com/Trendyol/go-mongo-cdc/mongo/message"
	"github.com/Trendyol/go-mongo-cdc/partition"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.uber.org/zap"
)

var ErrStreamRestart = errors.New("stream restart requested")

type PartitionStream interface {
	Start(ctx context.Context) error
	Stop(ctx context.Context) error
}

type ListenerFunc func(ctx *ListenerContext) error

type ListenerContext struct {
	Message     message.Message
	PartitionID int
	Ack         func() error
}

type partitionStream struct {
	client     connection.Client
	cfg        config.Config
	metric     metric.Metric
	listener   ListenerFunc
	logger     *zap.Logger
	collection connection.Collection
	database   connection.Database

	partitionManager  partition.Manager
	checkpointManager checkpoint.Manager

	// Tek stream için yapılar
	globalStream connection.ChangeStream
	streamMutex  sync.RWMutex

	// Partition takibi
	assignedPartitions map[int]bool
	partitionsMutex    sync.RWMutex

	// Per-partition checkpoint takibi
	partitionTokens map[int][]byte
	tokenMutex      sync.RWMutex

	// Stream restart kontrolü
	restartChan chan struct{}

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

func NewPartitionStream(
	client connection.Client,
	cfg config.Config,
	metric metric.Metric,
	listener ListenerFunc,
	logger *zap.Logger,
	workerID string,
) PartitionStream {
	database := client.Database(cfg.Database)
	collection := database.Collection(cfg.Collection)

	partitionManager := partition.NewManager(workerID, client, cfg.Partition, logger)
	checkpointManager := checkpoint.NewManager(client, cfg.Database, cfg.Collection, logger)

	return &partitionStream{
		client:             client,
		cfg:                cfg,
		metric:             metric,
		listener:           listener,
		logger:             logger,
		collection:         collection,
		database:           database,
		partitionManager:   partitionManager,
		checkpointManager:  checkpointManager,
		assignedPartitions: make(map[int]bool),
		partitionTokens:    make(map[int][]byte),
		restartChan:        make(chan struct{}, 1),
	}
}

func (ps *partitionStream) Start(ctx context.Context) error {
	ps.ctx, ps.cancel = context.WithCancel(ctx)

	if err := ps.checkReplicaSetStatus(ps.ctx); err != nil {
		return err
	}

	if err := ps.partitionManager.Initialize(ps.ctx); err != nil {
		return fmt.Errorf("failed to initialize partition manager: %w", err)
	}

	ps.partitionManager.SetPartitionsChangedCallback(func(newPartitions []int) {
		ps.partitionsMutex.Lock()
		oldPartitions := make(map[int]bool)
		for k, v := range ps.assignedPartitions {
			oldPartitions[k] = v
		}

		// Assigned partitionları güncelle
		ps.assignedPartitions = make(map[int]bool)
		for _, partitionID := range newPartitions {
			ps.assignedPartitions[partitionID] = true
		}
		ps.partitionsMutex.Unlock()

		// Eski partition'ların token'larını temizle
		ps.tokenMutex.Lock()
		newTokens := make(map[int][]byte)
		for _, partitionID := range newPartitions {
			if token, exists := ps.partitionTokens[partitionID]; exists {
				newTokens[partitionID] = token
			}
		}
		ps.partitionTokens = newTokens
		ps.tokenMutex.Unlock()

		// Eğer partition'lar değişti ise stream'i yeniden başlat
		partitionsChanged := len(oldPartitions) != len(ps.assignedPartitions)
		if !partitionsChanged {
			for partitionID := range ps.assignedPartitions {
				if !oldPartitions[partitionID] {
					partitionsChanged = true
					break
				}
			}
		}

		if partitionsChanged {
			ps.logger.Info("Partitions changed, sending restart signal",
				zap.Ints("newPartitions", newPartitions))
			ps.restartStream()
		}

		ps.logger.Debug("Assigned partitions updated",
			zap.Int("count", len(ps.assignedPartitions)),
			zap.Ints("partitions", newPartitions))
	})

	// Initial partition assignment
	if err := ps.refreshPartitions(); err != nil {
		ps.logger.Error("Failed to acquire initial partitions", zap.Error(err))
		return err
	}

	// Tek global stream başlat
	ps.wg.Add(1)
	go ps.runGlobalStream()

	ps.wg.Add(1)
	go ps.partitionMonitor()

	return nil
}

func (ps *partitionStream) checkReplicaSetStatus(ctx context.Context) error {
	result := ps.database.RunCommand(ctx, bson.D{{Key: "isMaster", Value: 1}})

	var isMaster bson.M
	if err := result.Decode(&isMaster); err != nil {
		return err
	}

	if _, ok := isMaster["setName"]; ok {
		ps.logger.Info("Connected to MongoDB replica set")
		return nil
	}

	if msg, ok := isMaster["msg"]; ok && msg == "isdbgrid" {
		ps.logger.Debug("Connected to MongoDB sharded cluster")
		return nil
	}

	return errors.New(
		"MongoDB is not running as a replica set or sharded cluster. " +
			"Change streams require replica set or sharded cluster",
	)
}

func (ps *partitionStream) refreshPartitions() error {
	newPartitions, err := ps.partitionManager.AcquirePartitions(ps.ctx)
	if err != nil {
		return err
	}

	ps.partitionsMutex.Lock()
	defer ps.partitionsMutex.Unlock()

	// Assigned partitionları güncelle
	ps.assignedPartitions = make(map[int]bool)
	for _, partitionID := range newPartitions {
		ps.assignedPartitions[partitionID] = true
	}

	ps.logger.Debug("Assigned partitions updated",
		zap.Int("count", len(ps.assignedPartitions)),
		zap.Ints("partitions", newPartitions))

	return nil
}

func (ps *partitionStream) runGlobalStream() {
	defer ps.wg.Done()

	for {
		select {
		case <-ps.ctx.Done():
			return
		default:
			err := ps.processGlobalStream()
			if err == nil {
				ps.logger.Info("Global stream completed normally")
				return
			}

			if errors.Is(err, context.Canceled) {
				ps.logger.Info("Global stream cancelled")
				return
			}

			if errors.Is(err, ErrStreamRestart) {
				ps.logger.Info("Stream restart requested, restarting immediately")
				continue
			}

			ps.logger.Error("Global stream error, retrying", zap.Error(err))
			time.Sleep(5 * time.Second)
		}
	}
}

func (ps *partitionStream) processGlobalStream() error {
	resumeToken, startAtOperationTime, err := ps.prepareGlobalStreamStart()
	if err != nil {
		return err
	}

	shouldBootstrap := false
	if resumeToken == nil && startAtOperationTime == nil {
		ps.logger.Debug("No resume token or cluster time found, starting bootstrap")
		shouldBootstrap = true

		opTime, err := ps.getServerOperationTime(ps.ctx)
		if err == nil && opTime != nil {
			startAtOperationTime = opTime
		}
	}

	if shouldBootstrap {
		if err := ps.bootstrapGlobal(); err != nil {
			return fmt.Errorf("bootstrap failed: %w", err)
		}

		if startAtOperationTime != nil {
			ps.partitionsMutex.RLock()
			for partitionID := range ps.assignedPartitions {
				if err := ps.checkpointManager.SaveBootstrapClusterTime(ps.ctx, partitionID, *startAtOperationTime); err != nil {
					ps.logger.Warn("Failed to save bootstrap cluster time",
						zap.Int("partitionId", partitionID),
						zap.Error(err))
				}
			}
			ps.partitionsMutex.RUnlock()
		}
	}

	pipeline := ps.createGlobalPipeline()
	opts := options.ChangeStream().SetFullDocument(options.UpdateLookup)

	if resumeToken != nil {
		opts.SetResumeAfter(bson.Raw(resumeToken))
		ps.logger.Info("Resuming from token")
	} else if startAtOperationTime != nil {
		opts.SetStartAtOperationTime(startAtOperationTime)
		ps.logger.Debug("Starting from operation time", zap.Any("operationTime", startAtOperationTime))
	}

	changeStream, err := ps.collection.Watch(ps.ctx, pipeline, opts)
	if err != nil {
		return err
	}
	defer changeStream.Close(ps.ctx)

	ps.streamMutex.Lock()
	ps.globalStream = changeStream
	ps.streamMutex.Unlock()

	// Context with restart capability
	streamCtx, streamCancel := context.WithCancel(ps.ctx)
	defer streamCancel()

	// Periodic token save
	tokenSaveTicker := time.NewTicker(ps.cfg.Checkpoint.SaveInterval)
	defer tokenSaveTicker.Stop()
	go ps.periodicTokenSave(tokenSaveTicker, streamCtx)

	// Restart signal listener
	restartReceived := false
	go func() {
		select {
		case <-ps.restartChan:
			ps.logger.Info("Restart signal received, closing stream")
			restartReceived = true
			streamCancel()
		case <-streamCtx.Done():
			return
		case <-ps.ctx.Done():
			return
		}
	}()

	for changeStream.Next(streamCtx) {
		// Stream kapatıldıysa çık
		select {
		case <-streamCtx.Done():
			ps.logger.Info("Stream context cancelled, stopping processing")
			return context.Canceled
		default:
		}

		var event message.ChangeEvent
		if err := changeStream.Decode(&event); err != nil {
			ps.logger.Error("Error decoding change event", zap.Error(err))
			continue
		}

		currentToken := changeStream.ResumeToken()
		if err := ps.processGlobalEvent(event, currentToken); err != nil {
			ps.logger.Error("Error processing event",
				zap.String("operationType", event.OperationType),
				zap.Error(err))
			continue
		}
	}

	// Context iptal edildiyse restart olup olmadığını kontrol et
	select {
	case <-streamCtx.Done():
		if restartReceived {
			ps.logger.Info("Stream cancelled due to restart request")
			return ErrStreamRestart
		}
		return context.Canceled
	default:
	}

	if err := changeStream.Err(); err != nil {
		return err
	}

	return nil
}

func (ps *partitionStream) prepareGlobalStreamStart() ([]byte, *primitive.Timestamp, error) {
	ps.partitionsMutex.RLock()
	assignedPartitions := make([]int, 0, len(ps.assignedPartitions))
	for partitionID := range ps.assignedPartitions {
		assignedPartitions = append(assignedPartitions, partitionID)
	}
	ps.partitionsMutex.RUnlock()

	if len(assignedPartitions) == 0 {
		return nil, nil, nil
	}

	// Her partition'ın checkpoint'ini yükle
	ps.tokenMutex.Lock()
	for _, partitionID := range assignedPartitions {
		resumeToken, _, err := ps.checkpointManager.GetResumeToken(ps.ctx, partitionID)
		if err == nil && len(resumeToken) > 0 {
			ps.partitionTokens[partitionID] = resumeToken
		}
	}
	ps.tokenMutex.Unlock()

	// En eski cluster time'a sahip token'ı bul (stream start için)
	var oldestToken []byte
	var oldestTime *primitive.Timestamp

	for _, partitionID := range assignedPartitions {
		resumeToken, clusterTime, err := ps.checkpointManager.GetResumeToken(ps.ctx, partitionID)
		if err != nil {
			continue
		}

		if oldestTime == nil || (clusterTime != nil && clusterTime.T < oldestTime.T) {
			oldestToken = resumeToken
			oldestTime = clusterTime
		}
	}

	// Bootstrap kontrolü - herhangi bir partition'da incomplete bootstrap var mı?
	for _, partitionID := range assignedPartitions {
		bootstrapLastID, err := ps.checkpointManager.GetBootstrapProgress(ps.ctx, partitionID)
		if err != nil {
			continue
		}

		if bootstrapLastID != nil {
			ps.logger.Info("Found incomplete bootstrap, will continue",
				zap.Int("partitionId", partitionID),
				zap.Any("lastId", bootstrapLastID))
			return nil, nil, nil
		}
	}

	return oldestToken, oldestTime, nil
}

func (ps *partitionStream) getServerOperationTime(ctx context.Context) (*primitive.Timestamp, error) {
	result := ps.database.RunCommand(ctx, bson.D{{Key: "isMaster", Value: 1}})
	var isMaster bson.M
	if err := result.Decode(&isMaster); err != nil {
		return nil, err
	}

	if opTime, ok := isMaster["operationTime"].(primitive.Timestamp); ok {
		return &opTime, nil
	}
	return nil, nil
}

func (ps *partitionStream) bootstrapGlobal() error {
	ps.logger.Debug("Starting bootstrap for assigned partitions")

	ps.partitionsMutex.RLock()
	assignedPartitions := make([]int, 0, len(ps.assignedPartitions))
	for partitionID := range ps.assignedPartitions {
		assignedPartitions = append(assignedPartitions, partitionID)
	}
	ps.partitionsMutex.RUnlock()

	if len(assignedPartitions) == 0 {
		return nil
	}

	// Her partition için bootstrap yap
	for _, partitionID := range assignedPartitions {
		if err := ps.bootstrapPartition(partitionID); err != nil {
			ps.logger.Error("Failed to bootstrap partition",
				zap.Int("partitionId", partitionID),
				zap.Error(err))
			return err
		}
	}

	ps.logger.Debug("Bootstrap completed for all assigned partitions")
	return nil
}

func (ps *partitionStream) bootstrapPartition(partitionID int) error {
	ps.logger.Debug("Starting bootstrap for partition", zap.Int("partitionId", partitionID))

	bootstrapLastID, _ := ps.checkpointManager.GetBootstrapProgress(ps.ctx, partitionID)

	filter := ps.createDocumentFilter(partitionID)
	if bootstrapLastID != nil {
		filter = append(filter, bson.E{Key: "_id", Value: bson.M{"$gt": bootstrapLastID}})
	}

	cursor, err := ps.collection.Find(ps.ctx, filter)
	if err != nil {
		return err
	}
	defer cursor.Close(ps.ctx)

	processedCount := 0
	for cursor.Next(ps.ctx) {
		var document bson.M
		if err := cursor.Decode(&document); err != nil {
			ps.logger.Error("Error decoding document", zap.Error(err))
			continue
		}

		syntheticEvent := message.ChangeEvent{
			OperationType: "insert",
			DocumentKey: message.DocumentKey{
				ID: document["_id"],
			},
			FullDocument: document,
			Namespace: message.Namespace{
				Database:   ps.cfg.Database,
				Collection: ps.cfg.Collection,
			},
			ClusterTime: primitive.Timestamp{T: uint32(time.Now().Unix()), I: 1},
		}

		if err := ps.processGlobalEvent(syntheticEvent, nil); err != nil {
			ps.logger.Error("Error processing synthetic event",
				zap.Int("partitionId", partitionID),
				zap.Any("documentId", document["_id"]),
				zap.Error(err))
			continue
		}

		processedCount++

		if processedCount%1000 == 0 {
			ps.logger.Info("Bootstrap progress",
				zap.Int("partitionId", partitionID),
				zap.Int("processed", processedCount))

			if err := ps.checkpointManager.SaveBootstrapProgress(ps.ctx, partitionID, document["_id"]); err != nil {
				ps.logger.Error("Failed to save bootstrap progress", zap.Error(err))
			}
		}
	}

	if err := cursor.Err(); err != nil {
		return err
	}

	ps.logger.Debug("Bootstrap completed",
		zap.Int("partitionId", partitionID),
		zap.Int("totalProcessed", processedCount))

	return ps.checkpointManager.ClearBootstrapProgress(ps.ctx, partitionID)
}

func (ps *partitionStream) createGlobalPipeline() []bson.D {
	ps.partitionsMutex.RLock()
	assignedPartitions := make([]int, 0, len(ps.assignedPartitions))
	for partitionID := range ps.assignedPartitions {
		assignedPartitions = append(assignedPartitions, partitionID)
	}
	ps.partitionsMutex.RUnlock()

	ps.logger.Info("Creating pipeline for assigned partitions",
		zap.Ints("assignedPartitions", assignedPartitions))

	if len(assignedPartitions) == 0 {
		// Eğer henüz partition assign edilmemişse, hiçbir şey dönmeyen filter
		return []bson.D{
			{
				{Key: "$match", Value: bson.D{
					{Key: "_id", Value: bson.D{{Key: "$exists", Value: false}}},
				}},
			},
		}
	}

	return []bson.D{
		{
			{Key: "$match", Value: bson.D{
				{Key: "operationType", Value: bson.D{
					{Key: "$in", Value: bson.A{"insert", "update", "delete", "replace"}},
				}},
			}},
		},
		{
			{Key: "$match", Value: bson.D{
				{Key: "$expr", Value: bson.D{
					{Key: "$in", Value: bson.A{
						bson.D{{Key: "$mod", Value: bson.A{
							ps.createHashExpression("$documentKey._id"),
							partition.TotalPartitions,
						}}},
						assignedPartitions,
					}},
				}},
			}},
		},
	}
}

func (ps *partitionStream) calculatePartition(documentID interface{}) int {
	hashValue := ps.calculateHashValue(documentID)
	partitionID := hashValue % partition.TotalPartitions
	ps.logger.Debug("Partition calculation",
		zap.Any("documentId", documentID),
		zap.Int("hashValue", hashValue),
		zap.Int("partitionId", partitionID),
		zap.Int("totalPartitions", partition.TotalPartitions))
	return partitionID
}

func (ps *partitionStream) calculateHashValue(documentID interface{}) int {
	idStr := fmt.Sprintf("%v", documentID)

	// En fazla son 3 karakteri al, daha az varsa hepsini al
	startIndex := len(idStr) - 3
	if startIndex < 0 {
		startIndex = 0
	}
	lastChars := idStr[startIndex:]

	hashValue := 0
	for i, char := range lastChars {
		charValue := ps.hexCharToInt(char)
		multiplier := 1
		for j := 0; j < len(lastChars)-1-i; j++ {
			multiplier *= 16
		}
		hashValue += charValue * multiplier
	}
	return hashValue
}

func (ps *partitionStream) hexCharToInt(char rune) int {
	switch char {
	case '0':
		return 0
	case '1':
		return 1
	case '2':
		return 2
	case '3':
		return 3
	case '4':
		return 4
	case '5':
		return 5
	case '6':
		return 6
	case '7':
		return 7
	case '8':
		return 8
	case '9':
		return 9
	case 'a', 'A':
		return 10
	case 'b', 'B':
		return 11
	case 'c', 'C':
		return 12
	case 'd', 'D':
		return 13
	case 'e', 'E':
		return 14
	case 'f', 'F':
		return 15
	default:
		return 0
	}
}

func (ps *partitionStream) createHashExpression(idField string) bson.D {
	// Hash using last 3 characters for better distribution with partitions
	return bson.D{
		{Key: "$add", Value: bson.A{
			bson.D{{Key: "$multiply", Value: bson.A{
				ps.createSingleHexCharToInt(bson.D{
					{Key: "$substr", Value: bson.A{
						bson.D{{Key: "$toString", Value: idField}},
						bson.D{{Key: "$max", Value: bson.A{
							0,
							bson.D{{Key: "$subtract", Value: bson.A{
								bson.D{{Key: "$strLenCP", Value: bson.D{{Key: "$toString", Value: idField}}}},
								3,
							}}},
						}}},
						1,
					}},
				}),
				256, // 16^2
			}}},
			bson.D{{Key: "$multiply", Value: bson.A{
				ps.createSingleHexCharToInt(bson.D{
					{Key: "$substr", Value: bson.A{
						bson.D{{Key: "$toString", Value: idField}},
						bson.D{{Key: "$max", Value: bson.A{
							0,
							bson.D{{Key: "$subtract", Value: bson.A{
								bson.D{{Key: "$strLenCP", Value: bson.D{{Key: "$toString", Value: idField}}}},
								2,
							}}},
						}}},
						1,
					}},
				}),
				16,
			}}},
			ps.createSingleHexCharToInt(bson.D{
				{Key: "$substr", Value: bson.A{
					bson.D{{Key: "$toString", Value: idField}},
					bson.D{{Key: "$max", Value: bson.A{
						0,
						bson.D{{Key: "$subtract", Value: bson.A{
							bson.D{{Key: "$strLenCP", Value: bson.D{{Key: "$toString", Value: idField}}}},
							1,
						}}},
					}}},
					1,
				}},
			}),
		}},
	}
}

func (ps *partitionStream) createSingleHexCharToInt(charExpr bson.D) bson.D {
	return bson.D{
		{Key: "$switch", Value: bson.D{
			{Key: "branches", Value: bson.A{
				bson.D{{Key: "case", Value: bson.D{{Key: "$eq", Value: bson.A{charExpr, "0"}}}}, {Key: "then", Value: 0}},
				bson.D{{Key: "case", Value: bson.D{{Key: "$eq", Value: bson.A{charExpr, "1"}}}}, {Key: "then", Value: 1}},
				bson.D{{Key: "case", Value: bson.D{{Key: "$eq", Value: bson.A{charExpr, "2"}}}}, {Key: "then", Value: 2}},
				bson.D{{Key: "case", Value: bson.D{{Key: "$eq", Value: bson.A{charExpr, "3"}}}}, {Key: "then", Value: 3}},
				bson.D{{Key: "case", Value: bson.D{{Key: "$eq", Value: bson.A{charExpr, "4"}}}}, {Key: "then", Value: 4}},
				bson.D{{Key: "case", Value: bson.D{{Key: "$eq", Value: bson.A{charExpr, "5"}}}}, {Key: "then", Value: 5}},
				bson.D{{Key: "case", Value: bson.D{{Key: "$eq", Value: bson.A{charExpr, "6"}}}}, {Key: "then", Value: 6}},
				bson.D{{Key: "case", Value: bson.D{{Key: "$eq", Value: bson.A{charExpr, "7"}}}}, {Key: "then", Value: 7}},
				bson.D{{Key: "case", Value: bson.D{{Key: "$eq", Value: bson.A{charExpr, "8"}}}}, {Key: "then", Value: 8}},
				bson.D{{Key: "case", Value: bson.D{{Key: "$eq", Value: bson.A{charExpr, "9"}}}}, {Key: "then", Value: 9}},
				bson.D{{Key: "case", Value: bson.D{{Key: "$eq", Value: bson.A{charExpr, "a"}}}}, {Key: "then", Value: 10}},
				bson.D{{Key: "case", Value: bson.D{{Key: "$eq", Value: bson.A{charExpr, "b"}}}}, {Key: "then", Value: 11}},
				bson.D{{Key: "case", Value: bson.D{{Key: "$eq", Value: bson.A{charExpr, "c"}}}}, {Key: "then", Value: 12}},
				bson.D{{Key: "case", Value: bson.D{{Key: "$eq", Value: bson.A{charExpr, "d"}}}}, {Key: "then", Value: 13}},
				bson.D{{Key: "case", Value: bson.D{{Key: "$eq", Value: bson.A{charExpr, "e"}}}}, {Key: "then", Value: 14}},
				bson.D{{Key: "case", Value: bson.D{{Key: "$eq", Value: bson.A{charExpr, "f"}}}}, {Key: "then", Value: 15}},
				bson.D{{Key: "case", Value: bson.D{{Key: "$eq", Value: bson.A{charExpr, "A"}}}}, {Key: "then", Value: 10}},
				bson.D{{Key: "case", Value: bson.D{{Key: "$eq", Value: bson.A{charExpr, "B"}}}}, {Key: "then", Value: 11}},
				bson.D{{Key: "case", Value: bson.D{{Key: "$eq", Value: bson.A{charExpr, "C"}}}}, {Key: "then", Value: 12}},
				bson.D{{Key: "case", Value: bson.D{{Key: "$eq", Value: bson.A{charExpr, "D"}}}}, {Key: "then", Value: 13}},
				bson.D{{Key: "case", Value: bson.D{{Key: "$eq", Value: bson.A{charExpr, "E"}}}}, {Key: "then", Value: 14}},
				bson.D{{Key: "case", Value: bson.D{{Key: "$eq", Value: bson.A{charExpr, "F"}}}}, {Key: "then", Value: 15}},
			}},
			{Key: "default", Value: 0},
		}},
	}
}

func (ps *partitionStream) createDocumentFilter(partitionID int) bson.D {
	return bson.D{
		{Key: "$expr", Value: bson.D{
			{Key: "$eq", Value: bson.A{
				bson.D{{Key: "$mod", Value: bson.A{
					ps.createHashExpression("$_id"),
					partition.TotalPartitions,
				}}},
				partitionID,
			}},
		}},
	}
}

func (ps *partitionStream) periodicTokenSave(ticker *time.Ticker, ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-ps.ctx.Done():
			return
		case <-ticker.C:
			ps.tokenMutex.RLock()
			tokens := make(map[int][]byte)
			for partitionID, token := range ps.partitionTokens {
				if len(token) > 0 {
					tokens[partitionID] = append([]byte(nil), token...)
				}
			}
			ps.tokenMutex.RUnlock()

			for partitionID, token := range tokens {
				if err := ps.checkpointManager.SaveResumeToken(
					ps.ctx,
					partitionID,
					token,
					nil,
				); err != nil {
					ps.logger.Error("Failed to save partition resume token periodically",
						zap.Int("partitionId", partitionID),
						zap.Error(err))
				}
			}
		}
	}
}

func (ps *partitionStream) restartStream() {
	// Non-blocking restart signal gönder
	select {
	case ps.restartChan <- struct{}{}:
		ps.logger.Info("Stream restart signal sent")
	default:
		ps.logger.Debug("Stream restart signal already pending")
	}
}

func (ps *partitionStream) processGlobalEvent(event message.ChangeEvent, resumeToken []byte) error {
	startTime := time.Now()

	// Partition ID hesapla ve debug log
	partitionID := ps.calculatePartition(event.DocumentKey.ID)
	ps.logger.Debug("Processing event",
		zap.Any("documentId", event.DocumentKey.ID),
		zap.Int("calculatedPartition", partitionID),
		zap.String("operationType", event.OperationType))

	// MongoDB filter zaten doğru partition'ları getirdiği için tekrar kontrol yapmaya gerek yok

	msg, err := message.NewMessage(event)
	if err != nil {
		return err
	}

	ps.updateMetrics(msg.OperationType)

	listenerCtx := &ListenerContext{
		Message:     msg,
		PartitionID: partitionID,
		Ack: func() error {
			processingLatency := time.Since(startTime)
			ps.metric.SetProcessLatency(processingLatency.Nanoseconds())

			if len(resumeToken) > 0 {
				// Bu partition'ın checkpoint'ini güncelle
				ps.tokenMutex.Lock()
				ps.partitionTokens[partitionID] = append([]byte(nil), resumeToken...)
				ps.tokenMutex.Unlock()

				ps.logger.Debug("Saving checkpoint for partition",
					zap.Int("partitionId", partitionID),
					zap.Any("documentId", event.DocumentKey.ID))

				return ps.checkpointManager.SaveResumeToken(
					ps.ctx,
					partitionID,
					resumeToken,
					&event.ClusterTime,
				)
			}
			return nil
		},
	}

	return ps.listener(listenerCtx)
}

func (ps *partitionStream) updateMetrics(opType message.OperationType) {
	switch opType {
	case message.OperationInsert:
		ps.metric.IncInsertTotal()
	case message.OperationUpdate:
		ps.metric.IncUpdateTotal()
	case message.OperationDelete:
		ps.metric.IncDeleteTotal()
	case message.OperationReplace:
		ps.metric.IncInsertTotal()
	}
}

func (ps *partitionStream) partitionMonitor() {
	defer ps.wg.Done()

	ticker := time.NewTicker(ps.cfg.Partition.RefreshInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ps.ctx.Done():
			return
		case <-ticker.C:
			if err := ps.refreshPartitions(); err != nil {
				ps.logger.Error("Failed to refresh partitions", zap.Error(err))
			}
		}
	}
}

func (ps *partitionStream) Stop(ctx context.Context) error {
	ps.logger.Info("Stopping partition stream")

	if ps.cancel != nil {
		ps.cancel()
	}

	// Restart channel'ı kapat
	close(ps.restartChan)

	ps.streamMutex.Lock()
	if ps.globalStream != nil {
		ps.logger.Info("Closing global stream")
		ps.globalStream.Close(ctx)
	}
	ps.streamMutex.Unlock()

	done := make(chan struct{})
	go func() {
		ps.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		ps.logger.Info("Global stream stopped")
	case <-time.After(30 * time.Second):
		ps.logger.Warn("Timeout waiting for global stream to stop")
	}

	if err := ps.partitionManager.Stop(ctx); err != nil {
		ps.logger.Error("Failed to stop partition manager", zap.Error(err))
		return err
	}

	ps.logger.Info("Partition stream stopped successfully")
	return nil
}
