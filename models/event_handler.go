package models

type EventHandler interface {
	BeforePartitionStop(partitionID int)
	AfterPartitionStop(partitionID int)
	BeforePartitionStart(partitionID int)
	AfterPartitionStart(partitionID int)
}

type EmptyEventHandler struct{}

func (h *EmptyEventHandler) BeforePartitionStop(partitionID int) {
}

func (h *EmptyEventHandler) AfterPartitionStop(partitionID int) {
}

func (h *EmptyEventHandler) BeforePartitionStart(partitionID int) {
}

func (h *EmptyEventHandler) AfterPartitionStart(partitionID int) {
}

var DefaultEventHandler EventHandler = &EmptyEventHandler{}





