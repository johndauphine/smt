package orchestrator

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"smt/internal/config"
)

// makeConfig builds the minimal config needed by tests that exercise
// just the AIConcurrency accessor.
func makeConfig(aiConcurrency int) *config.Config {
	c := &config.Config{}
	c.Migration.AIConcurrency = aiConcurrency
	return c
}

// TestRunParallel_ActuallyRunsInParallel proves the helper does what it
// claims: 8 work items each sleeping 50ms should finish in ~50-80ms with
// concurrency=8, not in ~400ms (which is what serial would do). This
// guards against future refactors that accidentally serialize the calls.
func TestRunParallel_ActuallyRunsInParallel(t *testing.T) {
	const items = 8
	const sleep = 50 * time.Millisecond

	work := make([]int, items)
	for i := range work {
		work[i] = i
	}

	start := time.Now()
	err := runParallel(context.Background(), work, items, func(_ context.Context, _ int, _ int) error {
		time.Sleep(sleep)
		return nil
	})
	elapsed := time.Since(start)

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// Generous upper bound: 3× the sleep gives plenty of slack for goroutine
	// startup, but is well under the ~400ms a serial run would take.
	if elapsed > 3*sleep {
		t.Errorf("expected ~%s with concurrency=%d, got %s (likely serial)", sleep, items, elapsed)
	}
}

// TestRunParallel_RespectsConcurrencyBound asserts the SetLimit cap
// actually limits in-flight goroutines. With 100 items and limit=4,
// peak concurrency must never exceed 4.
func TestRunParallel_RespectsConcurrencyBound(t *testing.T) {
	const items = 100
	const limit = 4

	work := make([]int, items)

	var inFlight atomic.Int32
	var maxInFlight atomic.Int32
	var mu sync.Mutex

	err := runParallel(context.Background(), work, limit, func(_ context.Context, _ int, _ int) error {
		cur := inFlight.Add(1)
		// Track max under a mutex to make the read+update atomic
		mu.Lock()
		if cur > maxInFlight.Load() {
			maxInFlight.Store(cur)
		}
		mu.Unlock()
		// Brief work so multiple goroutines really overlap
		time.Sleep(2 * time.Millisecond)
		inFlight.Add(-1)
		return nil
	})

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got := maxInFlight.Load(); got > limit {
		t.Errorf("max concurrent in-flight = %d, want <= %d", got, limit)
	}
	if got := maxInFlight.Load(); got < 2 {
		t.Errorf("max concurrent in-flight = %d, expected at least 2 (parallelism not happening)", got)
	}
}

// TestRunParallel_FirstErrorCancelsRest mirrors the sequential
// semantics: as soon as one item fails, the rest get a cancelled
// context. Items that started before the failure can finish; items
// that haven't started should observe the cancellation and bail.
func TestRunParallel_FirstErrorCancelsRest(t *testing.T) {
	const items = 50
	work := make([]int, items)
	for i := range work {
		work[i] = i
	}

	target := errors.New("intentional failure")
	var completed atomic.Int32

	err := runParallel(context.Background(), work, 4, func(ctx context.Context, _ int, item int) error {
		if item == 3 {
			return target
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(10 * time.Millisecond):
			completed.Add(1)
			return nil
		}
	})

	if !errors.Is(err, target) {
		t.Fatalf("expected target error to propagate, got %v", err)
	}
	// With 50 items and limit=4, after failure we expect most items to
	// observe the cancel. A handful may have already been in flight when
	// item 3 failed; assert "not all 49 completed", which would mean the
	// cancel never fired.
	if completed.Load() == int32(items-1) {
		t.Errorf("all %d non-failing items completed; cancellation never fired", items-1)
	}
}

// TestRunParallel_DefaultsWhenZero documents that passing n=0 yields
// the package default, not zero parallelism (which would deadlock).
func TestRunParallel_DefaultsWhenZero(t *testing.T) {
	called := atomic.Int32{}
	err := runParallel(context.Background(), []int{1, 2, 3}, 0, func(_ context.Context, _, _ int) error {
		called.Add(1)
		return nil
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got := called.Load(); got != 3 {
		t.Errorf("expected 3 calls, got %d", got)
	}
}

// TestOrchestrator_AIConcurrency exercises the accessor: returns the
// config value when set, defaultAIConcurrency when unset.
func TestOrchestrator_AIConcurrency(t *testing.T) {
	o := &Orchestrator{config: makeConfig(0)}
	if got := o.aiConcurrency(); got != defaultAIConcurrency {
		t.Errorf("zero config: got %d, want %d", got, defaultAIConcurrency)
	}

	o = &Orchestrator{config: makeConfig(16)}
	if got := o.aiConcurrency(); got != 16 {
		t.Errorf("explicit 16: got %d, want 16", got)
	}

	o = &Orchestrator{config: makeConfig(1)}
	if got := o.aiConcurrency(); got != 1 {
		t.Errorf("explicit 1 (local-model setting): got %d, want 1", got)
	}
}
