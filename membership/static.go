package membership

import (
	"context"
	"fmt"
	"sync"
	"time"

	"go.uber.org/zap"
)

type StaticMembership struct {
	config     MembershipConfig
	logger     *zap.Logger
	memberInfo MemberInfo
	mu         sync.RWMutex

	changeCallback MembershipChangeCallback
}

func NewStaticMembership(config MembershipConfig, logger *zap.Logger) *StaticMembership {
	memberID := config.MemberID
	if memberID == "" {
		memberID = generateMemberID()
	}

	return &StaticMembership{
		config: config,
		logger: logger,
		memberInfo: MemberInfo{
			ID:           memberID,
			MemberNumber: config.MemberNumber,
			Status:       MemberStatusActive,
			LastSeen:     time.Now(),
			Metadata:     make(map[string]string),
		},
	}
}

func (s *StaticMembership) Initialize(ctx context.Context) error {
	s.logger.Info("Static membership initialized",
		zap.String("member_id", s.memberInfo.ID),
		zap.Int("member_number", s.memberInfo.MemberNumber),
		zap.Int("total_members", s.config.TotalMembers))
	return nil
}

func (s *StaticMembership) Start(ctx context.Context) error {
	s.logger.Info("Static membership started",
		zap.String("member_id", s.memberInfo.ID),
		zap.Int("member_number", s.memberInfo.MemberNumber),
		zap.Int("total_members", s.config.TotalMembers))
	return nil
}

func (s *StaticMembership) Stop(ctx context.Context) error {
	s.logger.Info("Static membership stopped")
	return nil
}

func (s *StaticMembership) GetMembershipInfo() MembershipInfo {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var allMembers []MemberInfo
	for i := 1; i <= s.config.TotalMembers; i++ {
		memberID := s.memberInfo.ID
		if i != s.memberInfo.MemberNumber {
			memberID = fmt.Sprintf("static-member-%d", i)
		}

		allMembers = append(allMembers, MemberInfo{
			ID:           memberID,
			MemberNumber: i,
			Status:       MemberStatusActive,
			LastSeen:     time.Now(),
			Metadata:     make(map[string]string),
		})
	}

	return MembershipInfo{
		MemberNumber: s.memberInfo.MemberNumber,
		TotalMembers: s.config.TotalMembers,
		Members:      allMembers,
		LastUpdated:  time.Now(),
	}
}

func (s *StaticMembership) GetMemberInfo() MemberInfo {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.memberInfo
}

func (s *StaticMembership) IsLeader() bool {
	return s.memberInfo.MemberNumber == 1 // TODO: First member is leader in static. Update this
}

func (s *StaticMembership) TriggerRebalance(ctx context.Context) error {
	s.logger.Info("Rebalance triggered for static membership",
		zap.String("member_id", s.memberInfo.ID))
	return nil
}

func (s *StaticMembership) UpdateMembershipInfo(ctx context.Context, memberNumber, totalMembers int) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.memberInfo.MemberNumber = memberNumber
	s.config.TotalMembers = totalMembers
	s.memberInfo.LastSeen = time.Now()

	s.logger.Info("Static membership info updated",
		zap.Int("member_number", memberNumber),
		zap.Int("total_members", totalMembers))

	return nil
}

func (s *StaticMembership) SetChangeCallback(callback MembershipChangeCallback) {
	s.mu.Lock()
	s.changeCallback = callback
	s.mu.Unlock()
}
