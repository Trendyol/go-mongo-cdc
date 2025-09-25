package backoff

import (
	crand "crypto/rand"
	"math/big"
	"time"
)

// Config holds the configuration for the backoff strategy.
type Config struct {
	BaseDelay  time.Duration
	MaxDelay   time.Duration
	Factor     float64
	MaxRetries int // 0 for infinite retries
}

// Backoff manages the state of an exponential backoff operation.
type Backoff struct {
	Config       Config
	currentDelay time.Duration
	attempts     int
}

// New creates a new Backoff instance with the given configuration.
func New(config Config) *Backoff {
	return &Backoff{
		Config:       config,
		currentDelay: config.BaseDelay,
	}
}

// NextDelay calculates and returns the next backoff duration.
// It returns false if the max number of retries has been exceeded.
func (b *Backoff) NextDelay() (time.Duration, bool) {
	if b.Config.MaxRetries > 0 && b.attempts >= b.Config.MaxRetries {
		return 0, false
	}

	delay := b.currentDelay

	// Add jitter (up to 20% of the current delay)
	maxJitter := delay / 5
	jitter := time.Duration(0)
	if maxJitter > 0 {
		n, err := crand.Int(crand.Reader, big.NewInt(int64(maxJitter)))
		if err == nil {
			jitter = time.Duration(n.Int64())
		}
	}
	delay += jitter

	// Increase delay for the next attempt
	b.currentDelay = time.Duration(float64(b.currentDelay) * b.Config.Factor)
	if b.currentDelay > b.Config.MaxDelay {
		b.currentDelay = b.Config.MaxDelay
	}

	b.attempts++

	return delay, true
}

// Attempts returns the current number of attempts.
func (b *Backoff) Attempts() int {
	return b.attempts
}

// Reset resets the backoff state.
func (b *Backoff) Reset() {
	b.currentDelay = b.Config.BaseDelay
	b.attempts = 0
}
