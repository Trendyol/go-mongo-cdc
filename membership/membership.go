package membership

import (
	"context"
	"fmt"
	"time"

	"github.com/Trendyol/go-mongo-cdc/mongo/connection"
	"go.uber.org/zap"
)

type MembershipType string

const (
	MembershipTypeStatic  MembershipType = "static"  // Static membership with fixed member count
	MembershipTypeDynamic MembershipType = "dynamic" // Dynamic membership with automatic discovery
)

type MemberInfo struct {
	ID           string            `json:"id"`
	MemberNumber int               `json:"memberNumber"`
	Status       MemberStatus      `json:"status"`
	LastSeen     time.Time         `json:"lastSeen"`
	Metadata     map[string]string `json:"metadata"`
}

type MemberStatus string

const (
	MemberStatusActive   MemberStatus = "active"
	MemberStatusInactive MemberStatus = "inactive"
	MemberStatusLeader   MemberStatus = "leader"
)

type MembershipInfo struct {
	MemberNumber int          `json:"memberNumber"`
	TotalMembers int          `json:"totalMembers"`
	Members      []MemberInfo `json:"members"`
	Leader       *MemberInfo  `json:"leader"`
	LastUpdated  time.Time    `json:"lastUpdated"`
}

type MembershipChangeCallback func(oldInfo, newInfo MembershipInfo)

type Membership interface {
	Initialize(ctx context.Context) error
	Start(ctx context.Context) error
	Stop(ctx context.Context) error
	GetMembershipInfo() MembershipInfo
	GetMemberInfo() MemberInfo
	IsLeader() bool
	TriggerRebalance(ctx context.Context) error
	UpdateMembershipInfo(ctx context.Context, memberNumber, totalMembers int) error
	SetChangeCallback(callback MembershipChangeCallback)
}

type MembershipConfig struct {
	Type               MembershipType    `json:"type" yaml:"type"`
	MemberID           string            `json:"memberId" yaml:"memberId"`
	MemberNumber       int               `json:"memberNumber" yaml:"memberNumber"`
	TotalMembers       int               `json:"totalMembers" yaml:"totalMembers"`
	HeartbeatInterval  time.Duration     `json:"heartbeatInterval" yaml:"heartbeatInterval"`
	HealthCheckTimeout time.Duration     `json:"healthCheckTimeout" yaml:"healthCheckTimeout"`
	Config             map[string]string `json:"config" yaml:"config"`
}

func NewMembership(config MembershipConfig, client interface{}, logger interface{}) (Membership, error) {
	switch config.Type {
	case MembershipTypeDynamic:
		if mongoClient, ok := client.(connection.Client); ok {
			if zapLogger, ok := logger.(*zap.Logger); ok {
				return NewDynamicMembership(config, mongoClient, zapLogger), nil
			}
		}
		return nil, fmt.Errorf("invalid client or logger type for dynamic membership")
	case MembershipTypeStatic:
		if zapLogger, ok := logger.(*zap.Logger); ok {
			return NewStaticMembership(config, zapLogger), nil
		}
		return nil, fmt.Errorf("invalid logger type for static membership")
	default:
		if zapLogger, ok := logger.(*zap.Logger); ok {
			return NewStaticMembership(config, zapLogger), nil
		}
		return nil, fmt.Errorf("invalid logger type for default membership")
	}
}
