package membership

import (
	"context"
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
	LastSeen     time.Time         `json:"lastSeen"`
	Metadata     map[string]string `json:"metadata"`
}

type MembershipInfo struct {
	MemberNumber int          `json:"memberNumber"`
	TotalMembers int          `json:"totalMembers"`
	Members      []MemberInfo `json:"members"`
	LastUpdated  time.Time    `json:"lastUpdated"`
}

type Membership interface {
	Initialize(ctx context.Context) error
	Start(ctx context.Context) error
	Stop(ctx context.Context) error
	GetMembershipInfo() MembershipInfo
	GetMemberInfo() MemberInfo
	TriggerRebalance(ctx context.Context) error
	UpdateMembershipInfo(ctx context.Context, memberNumber, totalMembers int) error
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

func NewMembership(config MembershipConfig, client connection.Client, logger *zap.Logger) (Membership, error) {
	switch config.Type {
	case MembershipTypeStatic:
		return NewStaticMembership(config, logger), nil
	case MembershipTypeDynamic:
		return NewDynamicMembership(config, client, logger), nil
	default:
		return NewDynamicMembership(config, client, logger), nil
	}
}
