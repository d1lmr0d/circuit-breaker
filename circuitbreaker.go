package circuitbreaker

import (
	"context"
	"errors"
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

type CircuitBreaker struct {
	threshold    int
	state        State
	counter      Counter
	tripTime     time.Time
	callTimeout  time.Duration
	resetTimeout time.Duration
}

func New(threshold int, callTimeout, resetTimeout time.Duration) *CircuitBreaker {
	return &CircuitBreaker{
		threshold:    threshold,
		state:        StateClosed,
		callTimeout:  callTimeout,
		resetTimeout: resetTimeout,
	}
}

func (cb *CircuitBreaker) State() State { return cb.state }

func (cb *CircuitBreaker) Count() Counter { return cb.counter }

func (cb *CircuitBreaker) reset() {
	cb.counter.Total = 0
	cb.counter.TotalSuccess = 0
	cb.counter.TotalFailure = 0
	cb.counter.CurrentSuccess = 0
	cb.counter.CurrentFailure = 0
}

func (cb *CircuitBreaker) Do(f func(ctx context.Context) error) error {
	switch cb.State() {
	case StateOpen:
		if time.Since(cb.tripTime) > cb.resetTimeout {
			cb.state = StateHalfOpen
			return cb.do(f)
		}
		return ErrCircuitBreakerOpen
	case StateClosed, StateHalfOpen:
	}
	return cb.do(f)
}

func (cb *CircuitBreaker) do(f func(ctx context.Context) error) error {
	ctx, cancel := context.WithTimeout(context.Background(), cb.callTimeout)
	defer cancel()

	cb.counter.Total++

	err := f(ctx)
	if err != nil {
		cb.counter.TotalFailure++
		cb.counter.CurrentFailure++
		cb.counter.CurrentSuccess = 0
		cb.tripTime = time.Now()
		if cb.counter.CurrentFailure >= cb.threshold {
			cb.state = StateOpen
		}
		return err
	}

	if cb.State() == StateHalfOpen {
		cb.reset()
		cb.state = StateClosed
		return nil
	}

	cb.counter.TotalSuccess++
	cb.counter.CurrentSuccess++
	cb.counter.CurrentFailure = 0

	return nil
}
