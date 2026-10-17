package circuitbreaker

import (
	"context"
	"errors"
	"sync"
	"time"
)

type State string

const (
	StateOpen     State = "open"
	StateClosed   State = "closed"
	StateHalfOpen State = "half_open"
)

var (
	ErrCircuitBreakerOpen = errors.New("circuit breaker open")
)

type Counter struct {
	Total          int
	TotalSuccess   int
	TotalFailure   int
	CurrentSuccess int
	CurrentFailure int
}

func (c *Counter) reset() {
	c.Total = 0
	c.TotalSuccess = 0
	c.TotalFailure = 0
	c.CurrentSuccess = 0
	c.CurrentFailure = 0
}

type CircuitBreaker struct {
	mu sync.RWMutex

	state State

	counter *Counter

	threshold    int
	tripTime     time.Time
	callTimeout  time.Duration
	resetTimeout time.Duration
}

func New(threshold int, callTimeout, resetTimeout time.Duration) *CircuitBreaker {
	return &CircuitBreaker{
		counter:      &Counter{},
		threshold:    threshold,
		state:        StateClosed,
		callTimeout:  callTimeout,
		resetTimeout: resetTimeout,
	}
}

func (cb *CircuitBreaker) State() State {
	cb.mu.RLock()
	defer cb.mu.RUnlock()
	return cb.state
}

func (cb *CircuitBreaker) Count() Counter {
	cb.mu.RLock()
	defer cb.mu.RUnlock()
	return Counter{
		Total:          cb.counter.Total,
		TotalSuccess:   cb.counter.TotalSuccess,
		TotalFailure:   cb.counter.TotalFailure,
		CurrentSuccess: cb.counter.CurrentSuccess,
		CurrentFailure: cb.counter.CurrentFailure,
	}
}

func (cb *CircuitBreaker) Do(f func(ctx context.Context) error) error {
	cb.mu.Lock()

	switch cb.state {
	case StateOpen:
		if time.Since(cb.tripTime) <= cb.resetTimeout {
			cb.mu.Unlock()
			return ErrCircuitBreakerOpen
		}
		cb.state = StateHalfOpen
	case StateClosed, StateHalfOpen:
	}

	cb.mu.Unlock()
	return cb.do(f)
}

func (cb *CircuitBreaker) do(f func(ctx context.Context) error) error {
	ctx, cancel := context.WithTimeout(context.Background(), cb.callTimeout)
	defer cancel()

	cb.mu.Lock()
	cb.counter.Total++
	cb.mu.Unlock()

	err := f(ctx)

	cb.mu.Lock()
	if err != nil {
		cb.counter.TotalFailure++
		cb.counter.CurrentFailure++
		cb.counter.CurrentSuccess = 0
		cb.tripTime = time.Now()
		if cb.counter.CurrentFailure >= cb.threshold {
			cb.state = StateOpen
		}
		cb.mu.Unlock()
		return err
	}

	if cb.state == StateHalfOpen {
		cb.state = StateClosed
		cb.counter.reset()
		cb.mu.Unlock()
		return nil
	}

	cb.counter.TotalSuccess++
	cb.counter.CurrentSuccess++
	cb.counter.CurrentFailure = 0

	cb.mu.Unlock()
	return nil
}
