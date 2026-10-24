package circuitbreaker

import (
	"context"
	"errors"
	"testing"
	"time"
)

var errTestFailure = errors.New("test failure")

type doAction struct {
	ctx                context.Context
	result             error
	waitForReset       bool
	waitForCallTimeout bool
}

type doCase struct {
	name             string
	threshold        int
	callTimeout      time.Duration
	resetTimeout     time.Duration
	actions          []doAction
	wantErrs         []error
	wantState        State
	wantCount        Counter
	wantCalls        int
	checkCallbackCtx func(*testing.T, context.Context)
}

func TestDo(t *testing.T) {
	cases := []doCase{
		{
			name:         "closed success",
			threshold:    3,
			callTimeout:  time.Second,
			resetTimeout: time.Minute,
			actions:      []doAction{{ctx: context.Background()}},
			wantErrs:     []error{nil},
			wantState:    StateClosed,
			wantCount:    Counter{Total: 1, TotalSuccess: 1, CurrentSuccess: 1},
			wantCalls:    1,
		},
		{
			name:         "closed failure",
			threshold:    3,
			callTimeout:  time.Second,
			resetTimeout: time.Minute,
			actions:      []doAction{{ctx: context.Background(), result: errTestFailure}},
			wantErrs:     []error{errTestFailure},
			wantState:    StateClosed,
			wantCount:    Counter{Total: 1, TotalFailure: 1, CurrentFailure: 1},
			wantCalls:    1,
		},
		{
			name:         "success resets consecutive failures",
			threshold:    3,
			callTimeout:  time.Second,
			resetTimeout: time.Minute,
			actions: []doAction{
				{ctx: context.Background(), result: errTestFailure},
				{ctx: context.Background(), result: errTestFailure},
				{ctx: context.Background()},
			},
			wantErrs:  []error{errTestFailure, errTestFailure, nil},
			wantState: StateClosed,
			wantCount: Counter{Total: 3, TotalSuccess: 1, TotalFailure: 2, CurrentSuccess: 1},
			wantCalls: 3,
		},
		{
			name:         "failure below threshold stays closed",
			threshold:    3,
			callTimeout:  time.Second,
			resetTimeout: time.Minute,
			actions: []doAction{
				{ctx: context.Background(), result: errTestFailure},
				{ctx: context.Background(), result: errTestFailure},
			},
			wantErrs:  []error{errTestFailure, errTestFailure},
			wantState: StateClosed,
			wantCount: Counter{Total: 2, TotalFailure: 2, CurrentFailure: 2},
			wantCalls: 2,
		},
		{
			name:         "threshold failures open breaker",
			threshold:    2,
			callTimeout:  time.Second,
			resetTimeout: time.Minute,
			actions: []doAction{
				{ctx: context.Background(), result: errTestFailure},
				{ctx: context.Background(), result: errTestFailure},
			},
			wantErrs:  []error{errTestFailure, errTestFailure},
			wantState: StateOpen,
			wantCount: Counter{Total: 2, TotalFailure: 2, CurrentFailure: 2},
			wantCalls: 2,
		},
		{
			name:         "open breaker rejects call without changing counters",
			threshold:    1,
			callTimeout:  time.Second,
			resetTimeout: time.Minute,
			actions: []doAction{
				{ctx: context.Background(), result: errTestFailure},
				{ctx: context.Background()},
			},
			wantErrs:  []error{errTestFailure, ErrCircuitBreakerOpen},
			wantState: StateOpen,
			wantCount: Counter{Total: 1, TotalFailure: 1, CurrentFailure: 1},
			wantCalls: 1,
		},
		{
			name:         "successful probe closes and resets breaker",
			threshold:    1,
			callTimeout:  time.Second,
			resetTimeout: time.Millisecond,
			actions: []doAction{
				{ctx: context.Background(), result: errTestFailure},
				{ctx: context.Background(), waitForReset: true},
			},
			wantErrs:  []error{errTestFailure, nil},
			wantState: StateClosed,
			wantCount: Counter{},
			wantCalls: 2,
		},
		{
			name:         "failed probe reopens breaker",
			threshold:    1,
			callTimeout:  time.Second,
			resetTimeout: time.Millisecond,
			actions: []doAction{
				{ctx: context.Background(), result: errTestFailure},
				{ctx: context.Background(), waitForReset: true, result: errTestFailure},
				{ctx: context.Background()},
			},
			wantErrs:  []error{errTestFailure, errTestFailure, ErrCircuitBreakerOpen},
			wantState: StateOpen,
			wantCount: Counter{Total: 2, TotalFailure: 2, CurrentFailure: 2},
			wantCalls: 2,
		},
		{
			name:         "caller cancellation is passed through and not counted",
			threshold:    1,
			callTimeout:  time.Second,
			resetTimeout: time.Minute,
			actions: []doAction{{
				ctx: func() context.Context {
					ctx, cancel := context.WithCancel(context.Background())
					cancel()
					return ctx
				}(),
				result: context.Canceled,
			}},
			wantErrs:  []error{context.Canceled},
			wantState: StateClosed,
			wantCount: Counter{Total: 1},
			wantCalls: 1,
			checkCallbackCtx: func(t *testing.T, ctx context.Context) {
				t.Helper()
				if !errors.Is(ctx.Err(), context.Canceled) {
					t.Errorf("callback context error: want %v, got %v", context.Canceled, ctx.Err())
				}
			},
		},
		{
			name:         "call timeout is counted as failure",
			threshold:    2,
			callTimeout:  time.Millisecond,
			resetTimeout: time.Minute,
			actions: []doAction{{
				ctx:                context.Background(),
				waitForCallTimeout: true,
			}},
			wantErrs:  []error{context.DeadlineExceeded},
			wantState: StateClosed,
			wantCount: Counter{Total: 1, TotalFailure: 1, CurrentFailure: 1},
			wantCalls: 1,
			checkCallbackCtx: func(t *testing.T, ctx context.Context) {
				t.Helper()
				if !errors.Is(ctx.Err(), context.DeadlineExceeded) {
					t.Errorf("callback context error: want %v, got %v", context.DeadlineExceeded, ctx.Err())
				}
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var calls int
			cb := New(tc.threshold, tc.callTimeout, tc.resetTimeout)

			for i, action := range tc.actions {
				if action.waitForReset {
					time.Sleep(tc.resetTimeout + time.Millisecond)
				}

				var callbackCtx context.Context
				err := cb.Do(action.ctx, func(ctx context.Context) error {
					calls++
					callbackCtx = ctx
					if action.waitForCallTimeout {
						<-ctx.Done()
						return ctx.Err()
					}
					return action.result
				})
				if !errors.Is(err, tc.wantErrs[i]) {
					t.Errorf("call %d error: want %v, got %v", i+1, tc.wantErrs[i], err)
				}

				if tc.checkCallbackCtx != nil && callbackCtx != nil {
					tc.checkCallbackCtx(t, callbackCtx)
				}
			}

			if got := cb.State(); got != tc.wantState {
				t.Errorf("state: want %s, got %s", tc.wantState, got)
			}
			if got := cb.Count(); got != tc.wantCount {
				t.Errorf("counts: want %+v, got %+v", tc.wantCount, got)
			}
			if calls != tc.wantCalls {
				t.Errorf("callback calls: want %d, got %d", tc.wantCalls, calls)
			}
		})
	}
}
