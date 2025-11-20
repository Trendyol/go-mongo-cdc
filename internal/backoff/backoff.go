package backoff

import (
	"context"
	"crypto/rand"
	"math/big"
	"time"
)

type Config struct {
	BaseDelay  time.Duration
	MaxDelay   time.Duration
	Factor     float64
	MaxRetries int
}

type Backoff struct {
	Config       Config
	currentDelay time.Duration
	attempts     int
}

func New(config Config) *Backoff {
	return &Backoff{
		Config:       config,
		currentDelay: config.BaseDelay,
	}
}

func (b *Backoff) NextDelay() (time.Duration, bool) {
	if b.Config.MaxRetries > 0 && b.attempts >= b.Config.MaxRetries {
		return 0, false
	}

	delay := b.currentDelay

	maxJitter := int64(delay / 5)
	if maxJitter > 0 {
		n, err := rand.Int(rand.Reader, big.NewInt(maxJitter))
		if err == nil {
			delay += time.Duration(n.Int64())
		}
	}

	b.currentDelay = time.Duration(float64(b.currentDelay) * b.Config.Factor)
	if b.currentDelay > b.Config.MaxDelay {
		b.currentDelay = b.Config.MaxDelay
	}

	b.attempts++

	return delay, true
}

func (b *Backoff) Sleep(ctx context.Context) error {
	delay, ok := b.NextDelay()
	if !ok {
		return ErrMaxRetriesExceeded
	}

	timer := time.NewTimer(delay)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func (b *Backoff) Attempts() int {
	return b.attempts
}

func (b *Backoff) Reset() {
	b.currentDelay = b.Config.BaseDelay
	b.attempts = 0
}

var ErrMaxRetriesExceeded = &MaxRetriesExceededError{}

type MaxRetriesExceededError struct{}

func (e *MaxRetriesExceededError) Error() string {
	return "max retries exceeded"
}

var (
	DefaultConfig = Config{
		BaseDelay:  1 * time.Second,
		MaxDelay:   5 * time.Second,
		Factor:     2.0,
		MaxRetries: 3,
	}

	FastConfig = Config{
		BaseDelay:  100 * time.Millisecond,
		MaxDelay:   2 * time.Second,
		Factor:     2.0,
		MaxRetries: 3,
	}

	SlowConfig = Config{
		BaseDelay:  5 * time.Second,
		MaxDelay:   30 * time.Second,
		Factor:     2.0,
		MaxRetries: 3,
	}

	StreamRetryConfig = Config{
		BaseDelay:  1 * time.Second,
		MaxDelay:   30 * time.Second,
		Factor:     2.0,
		MaxRetries: 3,
	}
)
