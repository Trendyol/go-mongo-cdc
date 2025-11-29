package message

import (
	"fmt"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

type OperationType string

const (
	OperationInsert  OperationType = "insert"
	OperationUpdate  OperationType = "update"
	OperationDelete  OperationType = "delete"
	OperationReplace OperationType = "replace"
)

type Message struct {
	EventTime     time.Time           `json:"eventTime"`
	DocumentID    interface{}         `json:"documentId"`
	FullDocument  bson.M              `json:"fullDocument,omitempty"`
	OperationType OperationType       `json:"operationType"`
	Database      string              `json:"database"`
	Collection    string              `json:"collection"`
	ClusterTime   primitive.Timestamp `json:"clusterTime"`
	IsBootstrap   bool                `json:"isBootstrap"`
}

type ChangeEvent struct {
	OperationType string              `bson:"operationType"`
	DocumentKey   DocumentKey         `bson:"documentKey"`
	FullDocument  bson.M              `bson:"fullDocument,omitempty"`
	Namespace     Namespace           `bson:"ns"`
	ClusterTime   primitive.Timestamp `bson:"clusterTime"`
}

type DocumentKey struct {
	ID interface{} `bson:"_id"`
}

type Namespace struct {
	Database   string `bson:"db"`
	Collection string `bson:"coll"`
}

func NewMessage(event ChangeEvent) (Message, error) {
	msg := Message{
		OperationType: OperationType(event.OperationType),
		ClusterTime:   event.ClusterTime,
		Database:      event.Namespace.Database,
		Collection:    event.Namespace.Collection,
		DocumentID:    event.DocumentKey.ID,
		EventTime:     time.Unix(int64(event.ClusterTime.T), 0),
	}

	switch msg.OperationType {
	case OperationInsert, OperationReplace, OperationUpdate:
		msg.FullDocument = event.FullDocument
	case OperationDelete:
	default:
		return msg, fmt.Errorf("unsupported operation type: %s", event.OperationType)
	}

	return msg, nil
}

func (m Message) IsInsert() bool {
	return m.OperationType == OperationInsert
}

func (m Message) IsUpdate() bool {
	return m.OperationType == OperationUpdate
}

func (m Message) IsDelete() bool {
	return m.OperationType == OperationDelete
}

func (m Message) IsReplace() bool {
	return m.OperationType == OperationReplace
}

func (m Message) GetDocumentID() interface{} {
	return m.DocumentID
}

func (m Message) GetFullDocument() bson.M {
	return m.FullDocument
}
