package stream

import "context"

type EventHandler interface {
	BeforeRebalanceStart()
	AfterRebalanceStart()
	BeforeRebalanceEnd()
	AfterRebalanceEnd()
	BeforeStreamStart()
	AfterStreamStart()
	BeforeStreamStop()
	AfterStreamStop()
}

type RebalanceCoordinator interface {
	WaitForRebalanceReady(ctx context.Context) error
	NotifyRebalanceComplete()
}

type DefaultEventHandler struct{}

func (h *DefaultEventHandler) BeforeRebalanceStart() {
}

func (h *DefaultEventHandler) AfterRebalanceStart() {
}

func (h *DefaultEventHandler) BeforeRebalanceEnd() {
}

func (h *DefaultEventHandler) AfterRebalanceEnd() {
}

func (h *DefaultEventHandler) BeforeStreamStart() {
}

func (h *DefaultEventHandler) AfterStreamStart() {
}

func (h *DefaultEventHandler) BeforeStreamStop() {
}

func (h *DefaultEventHandler) AfterStreamStop() {
}

func (h *DefaultEventHandler) WaitForRebalanceReady(ctx context.Context) error {
	return nil
}

func (h *DefaultEventHandler) NotifyRebalanceComplete() {
}
