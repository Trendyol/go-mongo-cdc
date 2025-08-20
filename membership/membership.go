package membership

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Trendyol/go-mongo-cdc/mongo/connection"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.uber.org/zap"
)

type Membership interface {
	Initialize(ctx context.Context) error
	Start(ctx context.Context) error
	Stop(ctx context.Context) error
	GetMemberInfo() MemberInfo
	UpdateMembershipInfo(ctx context.Context) error

	// Partition assignment race prevention
	GetPartitionInfo() (partitionIndex int, totalPartitions int, version int64)
	IsPartitionValid(expectedVersion int64, expectedIndex int, expectedTotal int) bool
	WaitForMembershipStability(ctx context.Context, stabilityDuration time.Duration) error
}

type MembershipInfo struct {
	TotalMembers int          `json:"totalMembers"`
	Members      []MemberInfo `json:"members"`
	LastUpdated  time.Time    `json:"lastUpdated"`
}

type MemberInfo struct {
	ID       string    `json:"id"`
	LastSeen time.Time `json:"lastSeen"`
}

type MembershipConfig struct {
	HeartbeatInterval  time.Duration     `json:"heartbeatInterval" yaml:"heartbeatInterval"`
	HealthCheckTimeout time.Duration     `json:"healthCheckTimeout" yaml:"healthCheckTimeout"`
	Config             map[string]string `json:"config" yaml:"config"`
}

type MemberDocument struct {
	ID        string    `bson:"_id"`
	LastSeen  time.Time `bson:"lastSeen"`
	CreatedAt time.Time `bson:"createdAt"`
}

type membership struct {
	config     MembershipConfig
	logger     *zap.Logger
	client     connection.Client
	collection connection.Collection

	mu             sync.RWMutex
	membershipInfo MembershipInfo
	memberInfo     MemberInfo
	isRunning      bool

	// Version control for race condition prevention
	membershipVersion int64

	heartbeatTicker *time.Ticker
	stopChan        chan struct{}
	wg              sync.WaitGroup
}

func NewMembership(config MembershipConfig, client connection.Client, logger *zap.Logger) Membership {
	return &membership{
		config:   config,
		logger:   logger,
		client:   client,
		stopChan: make(chan struct{}),
		memberInfo: MemberInfo{
			ID:       generateMemberID(),
			LastSeen: time.Now(),
		},
	}
}

func (d *membership) Initialize(ctx context.Context) error {
	d.logger.Debug("Initializing membership")

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

	d.logger.Debug("Membership initialized successfully",
		zap.String("memberId", d.memberInfo.ID),
		zap.String("membershipCollection", membershipCollection))

	return nil
}

func (d *membership) createIndexes(ctx context.Context) error {
	indexes := []mongo.IndexModel{
		{
			Keys:    bson.D{{Key: "lastSeen", Value: 1}},
			Options: options.Index().SetName("lastSeen_1"),
		},
	}

	_, err := d.collection.Indexes().CreateMany(ctx, indexes)
	return err
}

func (d *membership) registerMember(ctx context.Context) error {
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

func (d *membership) Start(ctx context.Context) error {
	d.mu.Lock()
	if d.isRunning {
		d.mu.Unlock()
		return nil
	}
	d.isRunning = true
	d.mu.Unlock()

	d.logger.Debug("Starting membership")

	if err := d.updateMembershipInfo(ctx); err != nil {
		d.logger.Error("Failed to load initial membership info", zap.Error(err))
		return err
	}

	d.heartbeatTicker = time.NewTicker(d.config.HeartbeatInterval)

	d.wg.Add(1)
	go d.heartbeatLoop(ctx)

	d.logger.Debug("Membership started")
	return nil
}

func (d *membership) updateMembershipInfo(ctx context.Context) error {
	selfID := d.getSelfID()

	members, err := d.getActiveMembers(ctx)
	if err != nil {
		return err
	}

	d.validateMembershipState(members, selfID)

	return d.updateMembershipInfoInternal(members)
}

func (d *membership) getActiveMembers(ctx context.Context) ([]MemberInfo, error) {

	cutoff := time.Now().Add(-d.config.HealthCheckTimeout)

	expiredFilter := bson.M{"lastSeen": bson.M{"$lt": cutoff}}
	_, err := d.collection.DeleteMany(ctx, expiredFilter)
	if err != nil {
		d.logger.Error("Failed to cleanup expired members", zap.Error(err))
	}

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

func (d *membership) validateMembershipState(members []MemberInfo, selfID string) {
	if len(members) == 0 {
		d.logger.Fatal("No active members found in cluster, including self. This indicates a critical membership issue.",
			zap.String("memberID", selfID),
			zap.Time("lastSeen", d.memberInfo.LastSeen),
			zap.Duration("healthCheckTimeout", d.config.HealthCheckTimeout))
	}

	selfFound := false
	for _, member := range members {
		if member.ID == selfID {
			selfFound = true
			break
		}
	}

	if !selfFound {
		d.logger.Fatal("Self not found in active members list",
			zap.String("selfID", selfID),
			zap.Int("activeMembersCount", len(members)),
			zap.Any("activeMembers", members))
	}
}

func (d *membership) updateMembershipInfoInternal(members []MemberInfo) error {
	sort.Slice(members, func(i, j int) bool {
		return members[i].ID < members[j].ID
	})

	var selfMember *MemberInfo
	selfID := d.getSelfID()
	for i := range members {
		if members[i].ID == selfID {
			selfMember = &members[i]
			break
		}
	}

	d.mu.Lock()

	newVersion := atomic.AddInt64(&d.membershipVersion, 1)

	d.membershipInfo = MembershipInfo{
		TotalMembers: len(members),
		Members:      members,
		LastUpdated:  time.Now(),
	}

	if selfMember != nil {
		d.memberInfo = *selfMember
	}

	d.mu.Unlock()

	d.logger.Debug("Membership info updated",
		zap.Int64("version", newVersion),
		zap.Int("totalMembers", len(members)),
		zap.String("selfID", selfID),
		zap.Bool("selfFound", selfMember != nil))

	return nil
}

func (d *membership) heartbeatLoop(ctx context.Context) {
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

func (d *membership) sendHeartbeat(ctx context.Context) error {
	filter := bson.M{"_id": d.memberInfo.ID}
	update := bson.M{
		"$set": bson.M{
			"lastSeen": time.Now(),
		},
	}

	result, err := d.collection.UpdateOne(ctx, filter, update)
	if err != nil {
		return err
	}

	if result.MatchedCount() == 0 {
		d.logger.Error("Failed to update own heartbeat - document not found",
			zap.String("memberID", d.memberInfo.ID))

		if regErr := d.registerMember(ctx); regErr != nil {
			d.logger.Fatal("Failed to re-register member after heartbeat failure",
				zap.String("memberID", d.memberInfo.ID),
				zap.Error(regErr))
		}

		d.logger.Info("Successfully re-registered member after heartbeat failure",
			zap.String("memberID", d.memberInfo.ID))
	}

	return nil
}

func (d *membership) UpdateMembershipInfo(ctx context.Context) error {
	return d.updateMembershipInfo(ctx)
}

func (d *membership) Stop(ctx context.Context) error {
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

	d.logger.Info("Membership stopped")
	return nil
}

func (d *membership) unregisterMember(ctx context.Context) error {
	filter := bson.M{"_id": d.memberInfo.ID}
	_, err := d.collection.DeleteOne(ctx, filter)
	return err
}

func (d *membership) GetMemberInfo() MemberInfo {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return d.memberInfo
}

func (d *membership) getSelfID() string {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return d.memberInfo.ID
}

func (d *membership) GetPartitionInfo() (partitionIndex int, totalPartitions int, version int64) {
	d.mu.RLock()
	defer d.mu.RUnlock()

	selfID := d.memberInfo.ID
	totalMembers := len(d.membershipInfo.Members)
	partitionIdx := -1

	for i, member := range d.membershipInfo.Members {
		if member.ID == selfID {
			partitionIdx = i
			break
		}
	}

	return partitionIdx, totalMembers, atomic.LoadInt64(&d.membershipVersion)
}

func (d *membership) IsPartitionValid(expectedVersion int64, expectedIndex int, expectedTotal int) bool {
	currentIndex, currentTotal, currentVersion := d.GetPartitionInfo()

	return currentVersion == expectedVersion &&
		currentIndex == expectedIndex &&
		currentTotal == expectedTotal
}

func (d *membership) WaitForMembershipStability(ctx context.Context, stabilityDuration time.Duration) error {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	var lastVersion int64 = -1
	var stableStart time.Time

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			currentVersion := atomic.LoadInt64(&d.membershipVersion)

			if lastVersion != currentVersion {
				lastVersion = currentVersion
				stableStart = time.Now()
				d.logger.Debug("Membership version changed, resetting stability timer",
					zap.Int64("version", currentVersion))
				continue
			}

			if time.Since(stableStart) >= stabilityDuration {
				d.logger.Debug("Membership stable",
					zap.Int64("version", currentVersion),
					zap.Duration("stableDuration", time.Since(stableStart)))
				return nil
			}
		}
	}
}
