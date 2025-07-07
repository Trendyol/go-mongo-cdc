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
}

type MemberDocument struct {
	ID        string            `bson:"_id"`
	Status    MemberStatus      `bson:"status"`
	LastSeen  time.Time         `bson:"lastSeen"`
	Metadata  map[string]string `bson:"metadata"`
	CreatedAt time.Time         `bson:"createdAt"`
	UpdatedAt time.Time         `bson:"updatedAt"`
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
			Status:   MemberStatusActive,
			LastSeen: time.Now(),
			Metadata: make(map[string]string),
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

	d.heartbeatTicker = time.NewTicker(d.config.HeartbeatInterval)

	d.wg.Add(1)
	go d.heartbeatLoop(ctx)

	d.logger.Info("Dynamic membership started")
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
		d.logger.Error("Failed to unregister member", zap.Error(err))
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

func (d *DynamicMembership) IsLeader() bool {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return d.memberInfo.Status == MemberStatusLeader
}

func (d *DynamicMembership) TriggerRebalance(ctx context.Context) error {
	d.logger.Info("Triggering rebalance operation")
	return d.performRebalance(ctx)
}

func (d *DynamicMembership) UpdateMembershipInfo(ctx context.Context, memberNumber, totalMembers int) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	d.memberInfo.MemberNumber = memberNumber
	d.config.TotalMembers = totalMembers
	d.memberInfo.LastSeen = time.Now()

	d.logger.Info("Dynamic membership info updated",
		zap.Int("member_number", memberNumber),
		zap.Int("total_members", totalMembers))

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
		{
			Keys:    bson.D{{Key: "status", Value: 1}},
			Options: options.Index().SetName("status_1"),
		},
	}

	_, err := d.collection.Indexes().CreateMany(ctx, indexes)
	return err
}

func (d *DynamicMembership) registerMember(ctx context.Context) error {
	doc := MemberDocument{
		ID:        d.memberInfo.ID,
		Status:    d.memberInfo.Status,
		LastSeen:  time.Now(),
		Metadata:  d.memberInfo.Metadata,
		CreatedAt: time.Now(),
		UpdatedAt: time.Now(),
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
			"lastSeen":  time.Now(),
			"updatedAt": time.Now(),
		},
	}

	_, err := d.collection.UpdateOne(ctx, filter, update)
	if err != nil {
		return err
	}

	d.logger.Debug("Heartbeat sent successfully", zap.String("memberId", d.memberInfo.ID))
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

	leader := d.electLeader(members)
	return d.updateMembershipInfoInternal(ctx, members, leader)
}

func (d *DynamicMembership) getActiveMembers(ctx context.Context) ([]MemberInfo, error) {
	cutoff := time.Now().Add(-d.config.HealthCheckTimeout)
	filter := bson.M{"lastSeen": bson.M{"$lt": cutoff}}
	_, err := d.collection.DeleteMany(ctx, filter)
	if err != nil {
		d.logger.Error("Failed to cleanup expired members", zap.Error(err))
	}

	filter = bson.M{"status": bson.M{"$in": []MemberStatus{MemberStatusActive, MemberStatusLeader}}}
	cursor, err := d.collection.Find(ctx, filter)
	if err != nil {
		return nil, err
	}
	defer cursor.Close(ctx)

	var members []MemberInfo
	memberIndex := 1
	for cursor.Next(ctx) {
		var doc MemberDocument
		if err := cursor.Decode(&doc); err != nil {
			continue
		}

		members = append(members, MemberInfo{
			ID:           doc.ID,
			MemberNumber: memberIndex,
			Status:       doc.Status,
			LastSeen:     doc.LastSeen,
			Metadata:     doc.Metadata,
		})
		memberIndex++
	}

	return members, nil
}

func (d *DynamicMembership) electLeader(members []MemberInfo) MemberInfo {
	sort.Slice(members, func(i, j int) bool {
		return members[i].ID < members[j].ID
	})

	leader := members[0] // TODO: Update this
	leader.Status = MemberStatusLeader

	return leader
}

func (d *DynamicMembership) updateMembershipInfoInternal(ctx context.Context, members []MemberInfo, leader MemberInfo) error {
	d.mu.Lock()
	oldInfo := d.membershipInfo

	d.membershipInfo = MembershipInfo{
		MemberNumber: d.findMemberNumber(members),
		TotalMembers: len(members),
		Members:      members,
		Leader:       &leader,
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

	if callback != nil {
		d.logger.Debug("Calling membership change callback",
			zap.Int("old_total_members", oldInfo.TotalMembers),
			zap.Int("new_total_members", newInfo.TotalMembers))
		callback(oldInfo, newInfo)
	}

	return nil
}

func (d *DynamicMembership) findMemberNumber(members []MemberInfo) int {
	for _, member := range members {
		if member.ID == d.memberInfo.ID {
			return member.MemberNumber
		}
	}
	return 1
}

func (d *DynamicMembership) updateMembershipInfo(ctx context.Context) error {
	members, err := d.getActiveMembers(ctx)
	if err != nil {
		return err
	}

	if len(members) == 0 {
		return nil
	}

	leader := d.electLeader(members)
	return d.updateMembershipInfoInternal(ctx, members, leader)
}
