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

	d.wg.Add(1)
	go d.heartbeatLoop(ctx)

	d.logger.Info("Dynamic membership started (change stream monitoring moved to stream level)")
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
	sort.Slice(members, func(i, j int) bool {
		return members[i].ID < members[j].ID
	})

	d.mu.Lock()

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

	d.mu.Unlock()

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

func (d *DynamicMembership) UpdateMembershipInfoFromDatabase(ctx context.Context) error {
	return d.updateMembershipInfo(ctx)
}
