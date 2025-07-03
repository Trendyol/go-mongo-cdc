package integration

import (
	"github.com/Trendyol/go-mongo-cdc/mongo/changestream"
	"github.com/Trendyol/go-mongo-cdc/mongo/message"
)

type messageCollector struct {
	messages []message.Message
	errors   []error
}

type MessageCollector interface {
	CollectMessage(lc *changestream.ListenerContext)
	GetMessages() []message.Message
	GetErrors() []error
	Clear()
	HasMessages() bool
	MessageCount() int
}

func NewMessageCollector() MessageCollector {
	return &messageCollector{
		messages: make([]message.Message, 0),
		errors:   make([]error, 0),
	}
}

func (mc *messageCollector) CollectMessage(lc *changestream.ListenerContext) {
	mc.messages = append(mc.messages, lc.Message)
	if err := lc.Ack(); err != nil {
		mc.errors = append(mc.errors, err)
	}
}

func (mc *messageCollector) GetMessages() []message.Message {
	return mc.messages
}

func (mc *messageCollector) GetErrors() []error {
	return mc.errors
}

func (mc *messageCollector) Clear() {
	mc.messages = mc.messages[:0]
	mc.errors = mc.errors[:0]
}

func (mc *messageCollector) HasMessages() bool {
	return len(mc.messages) > 0
}

func (mc *messageCollector) MessageCount() int {
	return len(mc.messages)
}
