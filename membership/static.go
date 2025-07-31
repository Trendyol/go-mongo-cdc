package membership

import (
	"context"
	"time"

	"go.uber.org/zap"
)

type StaticMembership struct {
	config         MembershipConfig
	logger         *zap.Logger
	membershipInfo MembershipInfo
	memberInfo     MemberInfo
	changeCallback MembershipChangeCallback
}

func NewStaticMembership(config MembershipConfig, logger *zap.Logger) *StaticMembership {
	memberID := config.MemberID
	if memberID == "" {
		memberID = generateMemberID()
	}

	memberInfo := MemberInfo{
		ID:           memberID,
		MemberNumber: config.MemberNumber,
		LastSeen:     time.Now(),
		Metadata:     make(map[string]string),
	}

	// Static membership'te sadece kendi member bilgimiz var
	members := []MemberInfo{memberInfo}

	return &StaticMembership{
		config:     config,
		logger:     logger,
		memberInfo: memberInfo,
		membershipInfo: MembershipInfo{
			MemberNumber: config.MemberNumber,
			TotalMembers: config.TotalMembers,
			Members:      members,
			LastUpdated:  time.Now(),
		},
	}
}

func (s *StaticMembership) Initialize(ctx context.Context) error {
	s.logger.Info("Static membership initialized",
		zap.Int("member_number", s.membershipInfo.MemberNumber),
		zap.Int("total_members", s.membershipInfo.TotalMembers),
		zap.String("member_id", s.memberInfo.ID))
	return nil
}

func (s *StaticMembership) Start(ctx context.Context) error {
	s.logger.Info("Static membership started")
	return nil
}

func (s *StaticMembership) Stop(ctx context.Context) error {
	s.logger.Info("Static membership stopped")
	return nil
}

func (s *StaticMembership) GetMembershipInfo() MembershipInfo {
	return s.membershipInfo
}

func (s *StaticMembership) GetMemberInfo() MemberInfo {
	return s.memberInfo
}

func (s *StaticMembership) TriggerRebalance(ctx context.Context) error {
	// Static membership'te rebalance gerekmez
	s.logger.Debug("Rebalance triggered but ignored in static membership")
	return nil
}

func (s *StaticMembership) UpdateMembershipInfo(ctx context.Context, memberNumber, totalMembers int) error {
	oldInfo := s.membershipInfo

	s.memberInfo.MemberNumber = memberNumber
	s.membershipInfo.MemberNumber = memberNumber
	s.membershipInfo.TotalMembers = totalMembers
	s.membershipInfo.LastUpdated = time.Now()

	// Callback varsa çağır
	if s.changeCallback != nil && (oldInfo.MemberNumber != memberNumber || oldInfo.TotalMembers != totalMembers) {
		s.changeCallback(s.membershipInfo)
	}

	s.logger.Info("Static membership info updated",
		zap.Int("member_number", memberNumber),
		zap.Int("total_members", totalMembers))

	return nil
}

func (s *StaticMembership) SetChangeCallback(callback MembershipChangeCallback) {
	s.changeCallback = callback
}
