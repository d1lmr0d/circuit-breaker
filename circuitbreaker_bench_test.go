package circuitbreaker

import (
	"context"
	"errors"
	"math"
	"testing"
	"time"
)

var errBenchmarkFailure = errors.New("benchmark failure")

func BenchmarkDoSuccess(b *testing.B) {
	cb := New(math.MaxInt, time.Hour, time.Hour)
	ctx := context.Background()
	fn := func(context.Context) error { return nil }

	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		_ = cb.Do(ctx, fn)
	}
}

func BenchmarkDoFailure(b *testing.B) {
	cb := New(math.MaxInt, time.Hour, time.Hour)
	ctx := context.Background()
	fn := func(context.Context) error { return errBenchmarkFailure }

	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		_ = cb.Do(ctx, fn)
	}
}

func BenchmarkDoParallelSuccess(b *testing.B) {
	cb := New(math.MaxInt, time.Hour, time.Hour)
	ctx := context.Background()
	fn := func(context.Context) error { return nil }

	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_ = cb.Do(ctx, fn)
		}
	})
}
