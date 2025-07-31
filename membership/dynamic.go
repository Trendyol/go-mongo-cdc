package membership

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/Trendyol/go-mongo-cdc/mongo/connection"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.uber.org/zap"
)

type DynamicMembership struct {
	config     MembershipConfig
	logger     *zap.Logger
	client     connection.Client
	collection connection.Collection

	mu             sync.RWMutex
	membershipInfo MembershipInfo
	memberInfo     MemberInfo
	isRunning      bool

	heartbeatTicker *time.Ticker
	stopChan        chan struct{}
	wg              sync.WaitGroup

	changeCallback MembershipChangeCallback

	// Change stream için
	membershipStreamCtx    context.Context
	membershipStreamCancel context.CancelFunc
}

type MemberDocument struct {
	ID        string    `bson:"_id"`
	LastSeen  time.Time `bson:"lastSeen"`
	CreatedAt time.Time `bson:"createdAt"`
}

func NewDynamicMembership(config MembershipConfig, client connection.Client, logger *zap.Logger) *DynamicMembership {
	memberID := config.MemberID
	if memberID == "" {
		memberID = generateMemberID()
	}

	return &DynamicMembership{
		config:   config,
		logger:   logger,
		client:   client,
		stopChan: make(chan struct{}),
		memberInfo: MemberInfo{
			ID:       memberID,
			LastSeen: time.Now(),
		},
	}
}

func (d *DynamicMembership) Initialize(ctx context.Context) error {
	d.logger.Info("Initializing dynamic membership")

	membershipCollection := d.config.Config["membershipCollection"]
	if membershipCollection == "" {
		membershipCollection = "cdc_membership"
	}

	membershipDB := d.config.Config["membershipDatabase"]
	if membershipDB == "" {
		membershipDB = "cdc_cluster"
	}

	database := d.client.Database(membershipDB)
	d.collection = database.Collection(membershipCollection)

	if err := d.createIndexes(ctx); err != nil {
		return fmt.Errorf("failed to create indexes: %w", err)
	}

	if err := d.registerMember(ctx); err != nil {
		return fmt.Errorf("failed to register member: %w", err)
	}

	d.logger.Info("Dynamic membership initialized successfully",
		zap.String("memberId", d.memberInfo.ID),
		zap.String("membershipCollection", membershipCollection))

	return nil
}

func (d *DynamicMembership) Start(ctx context.Context) error {
	d.mu.Lock()
	if d.isRunning {
		d.mu.Unlock()
		return nil
	}
	d.isRunning = true
	d.mu.Unlock()

	// İlk başta membership info'yu yükle
	if err := d.updateMembershipInfo(ctx); err != nil {
		d.logger.Error("Failed to load initial membership info", zap.Error(err))
		return err
	}

	d.heartbeatTicker = time.NewTicker(d.config.HeartbeatInterval)

	// Change stream için context oluştur
	d.membershipStreamCtx, d.membershipStreamCancel = context.WithCancel(ctx)

	d.wg.Add(2)
	go d.heartbeatLoop(ctx)
	go d.membershipChangeStreamLoop(d.membershipStreamCtx)

	d.logger.Info("Dynamic membership started with change stream monitoring")
	return nil
}

func (d *DynamicMembership) Stop(ctx context.Context) error {
	d.mu.Lock()
	if !d.isRunning {
		d.mu.Unlock()
		return nil
	}
	d.isRunning = false
	d.mu.Unlock()

	close(d.stopChan)

	if d.heartbeatTicker != nil {
		d.heartbeatTicker.Stop()
	}

	// Change stream'i durdur
	if d.membershipStreamCancel != nil {
		d.membershipStreamCancel()
	}

	d.wg.Wait()

	if err := d.unregisterMember(ctx); err != nil {
		d.logger.Error("Failed to unregister member", zap.String("memberId", d.memberInfo.ID), zap.Error(err))
	}

	d.logger.Info("Dynamic membership stopped")
	return nil
}

func (d *DynamicMembership) GetMembershipInfo() MembershipInfo {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return d.membershipInfo
}

func (d *DynamicMembership) GetMemberInfo() MemberInfo {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return d.memberInfo
}

func (d *DynamicMembership) TriggerRebalance(ctx context.Context) error {
	d.logger.Info("Triggering rebalance operation")
	return d.performRebalance(ctx)
}

func (d *DynamicMembership) UpdateMembershipInfo(ctx context.Context, memberNumber, totalMembers int) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	d.memberInfo.LastSeen = time.Now()

	d.logger.Info("Dynamic membership info updated")

	return nil
}

func (d *DynamicMembership) SetChangeCallback(callback MembershipChangeCallback) {
	d.mu.Lock()
	d.changeCallback = callback
	d.mu.Unlock()
}

func (d *DynamicMembership) createIndexes(ctx context.Context) error {
	indexes := []mongo.IndexModel{
		{
			Keys:    bson.D{{Key: "lastSeen", Value: 1}},
			Options: options.Index().SetName("lastSeen_1"),
		},
	}

	_, err := d.collection.Indexes().CreateMany(ctx, indexes)
	return err
}

func (d *DynamicMembership) registerMember(ctx context.Context) error {
	doc := MemberDocument{
		ID:        d.memberInfo.ID,
		LastSeen:  time.Now(),
		CreatedAt: time.Now(),
	}

	opts := options.Update().SetUpsert(true)
	filter := bson.M{"_id": d.memberInfo.ID}
	update := bson.M{"$set": doc}

	_, err := d.collection.UpdateOne(ctx, filter, update, opts)
	return err
}

func (d *DynamicMembership) unregisterMember(ctx context.Context) error {
	filter := bson.M{"_id": d.memberInfo.ID}
	_, err := d.collection.DeleteOne(ctx, filter)
	return err
}

func (d *DynamicMembership) heartbeatLoop(ctx context.Context) {
	defer d.wg.Done()

	for {
		select {
		case <-ctx.Done():
			return
		case <-d.stopChan:
			return
		case <-d.heartbeatTicker.C:
			if err := d.sendHeartbeat(ctx); err != nil {
				d.logger.Error("Failed to send heartbeat", zap.Error(err))
			}
		}
	}
}

func (d *DynamicMembership) sendHeartbeat(ctx context.Context) error {
	filter := bson.M{"_id": d.memberInfo.ID}
	update := bson.M{
		"$set": bson.M{
			"lastSeen": time.Now(),
		},
	}

	_, err := d.collection.UpdateOne(ctx, filter, update)
	if err != nil {
		return err
	}

	return nil
}

func (d *DynamicMembership) performRebalance(ctx context.Context) error {

	members, err := d.getActiveMembers(ctx)
	if err != nil {
		return err
	}

	if len(members) == 0 {
		return nil
	}

	return d.updateMembershipInfoInternal(members)
}

func (d *DynamicMembership) getActiveMembers(ctx context.Context) ([]MemberInfo, error) {

	cutoff := time.Now().Add(-d.config.HealthCheckTimeout)

	// İlk önce expired member'ları temizle
	expiredFilter := bson.M{"lastSeen": bson.M{"$lt": cutoff}}
	_, err := d.collection.DeleteMany(ctx, expiredFilter)
	if err != nil {
		d.logger.Error("Failed to cleanup expired members", zap.Error(err))
	}

	// Sonra active member'ları getir (sadece lastSeen'e göre, status'e bakma)
	activeFilter := bson.M{"lastSeen": bson.M{"$gte": cutoff}}
	d.logger.Debug("Searching for active members", zap.Any("filter", activeFilter))

	cursor, err := d.collection.Find(ctx, activeFilter)
	if err != nil {
		d.logger.Error("Failed to find active members", zap.Error(err))
		return nil, err
	}
	defer cursor.Close(ctx)

	var members []MemberInfo
	for cursor.Next(ctx) {
		var doc MemberDocument
		if err := cursor.Decode(&doc); err != nil {
			d.logger.Error("Failed to decode member document", zap.Error(err))
			continue
		}

		d.logger.Debug("Found active member",
			zap.String("id", doc.ID),
			zap.Time("lastSeen", doc.LastSeen))

		members = append(members, MemberInfo{
			ID:       doc.ID,
			LastSeen: doc.LastSeen,
		})
	}

	d.logger.Debug("getActiveMembers completed",
		zap.Int("count", len(members)),
		zap.Any("members", members))
	return members, nil
}

func (d *DynamicMembership) updateMembershipInfoInternal(members []MemberInfo) error {
	// Members'ı ID'ye göre sırala - consistent ordering için
	sort.Slice(members, func(i, j int) bool {
		return members[i].ID < members[j].ID
	})

	d.mu.Lock()
	oldInfo := d.membershipInfo

	d.membershipInfo = MembershipInfo{
		TotalMembers: len(members),
		Members:      members,
		LastUpdated:  time.Now(),
	}

	for _, member := range members {
		if member.ID == d.memberInfo.ID {
			d.memberInfo = member
			break
		}
	}

	newInfo := d.membershipInfo
	callback := d.changeCallback
	d.mu.Unlock()

	// Sadece gerçek değişiklik olduğunda callback çağır
	if callback != nil && (oldInfo.TotalMembers != newInfo.TotalMembers || d.memberListChanged(oldInfo.Members, newInfo.Members)) {
		callback(newInfo)
	}

	return nil
}

func (d *DynamicMembership) updateMembershipInfo(ctx context.Context) error {
	members, err := d.getActiveMembers(ctx)
	if err != nil {
		return err
	}

	//TODO: kendini oldurse daha iyi olur gibi konusalım (hepsi aynı seyi yapsın kendini de gormuyorsa bende yokum deyip panic yapabilir)
	if len(members) == 0 {
		d.logger.Warn("No active members found, treating self as only member")
		// Hiç member bulunamazsa kendini tek member olarak kabul et
		members = []MemberInfo{
			{
				ID:       d.memberInfo.ID,
				LastSeen: time.Now(),
			},
		}
	}

	return d.updateMembershipInfoInternal(members)
}

// memberListChanged member listesinin değişip değişmediğini kontrol eder
func (d *DynamicMembership) memberListChanged(oldMembers, newMembers []MemberInfo) bool {
	if len(oldMembers) != len(newMembers) {
		return true
	}

	oldIDs := make(map[string]bool)
	for _, member := range oldMembers {
		oldIDs[member.ID] = true
	}

	for _, member := range newMembers {
		if !oldIDs[member.ID] {
			return true
		}
	}

	return false
}

// getMemberIDs member ID listesini döndürür
func (d *DynamicMembership) getMemberIDs(members []MemberInfo) []string {
	var ids []string
	for _, member := range members {
		ids = append(ids, member.ID)
	}
	return ids
}

// membershipChangeStreamLoop membership collection'ındaki değişiklikleri dinler
func (d *DynamicMembership) membershipChangeStreamLoop(ctx context.Context) {
	defer d.wg.Done()

	d.logger.Info("Starting membership change stream monitoring")

	// Change stream için pipeline - sadece critical değişiklikleri dinle
	pipeline := []bson.D{
		{
			{Key: "$match", Value: bson.D{
				{Key: "operationType", Value: bson.D{
					{Key: "$in", Value: []string{"insert", "delete"}}, // Sadece insert/delete dinle
				}},
			}},
		},
	}

	var retryCount int
	maxRetries := 3

	for {
		select {
		case <-ctx.Done():
			d.logger.Info("Membership change stream context cancelled")
			return
		default:
			// Change stream oluştur
			changeStream := d.collection.Watch(ctx, pipeline)
			if changeStream == nil {
				d.logger.Error("Failed to create membership change stream")
				retryCount++
				if retryCount >= maxRetries {
					d.logger.Error("Max retries reached for change stream, falling back to polling")
					// Polling fallback başlat
					d.startPollingFallback(ctx)
					return
				}
				time.Sleep(time.Duration(retryCount) * 5 * time.Second)
				continue
			}

			retryCount = 0 // Reset retry counter on successful connection

			// Change stream'i dinle
			for changeStream.Next(ctx) {
				var changeDoc bson.M
				if err := changeStream.Decode(&changeDoc); err != nil {
					d.logger.Error("Failed to decode change stream document", zap.Error(err))
					continue
				}

				operationType := changeDoc["operationType"].(string)
				d.logger.Debug("Membership change detected",
					zap.String("operation", operationType))

				// Insert/Delete her zaman önemli, hemen güncelle
				if err := d.updateMembershipInfo(ctx); err != nil {
					d.logger.Error("Failed to update membership info after change", zap.Error(err))
				}
			}

			// Change stream kapandı, hata kontrolü
			if err := changeStream.Err(); err != nil {
				d.logger.Error("Membership change stream error", zap.Error(err))
			}

			changeStream.Close(ctx)
			d.logger.Debug("Membership change stream closed, will retry")

			// Exponential backoff
			retryCount++
			backoffDuration := time.Duration(retryCount) * 2 * time.Second
			if backoffDuration > 30*time.Second {
				backoffDuration = 30 * time.Second
			}

			select {
			case <-ctx.Done():
				return
			case <-time.After(backoffDuration):
				continue
			}
		}
	}
}

// startPollingFallback change stream başarısız olduğunda polling fallback başlatır
func (d *DynamicMembership) startPollingFallback(ctx context.Context) {
	d.logger.Info("Starting polling fallback for membership monitoring")

	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			d.logger.Info("Polling fallback stopped")
			return
		case <-ticker.C:
			if err := d.updateMembershipInfo(ctx); err != nil {
				d.logger.Error("Failed to update membership info in polling fallback", zap.Error(err))
			}
		}
	}
}
