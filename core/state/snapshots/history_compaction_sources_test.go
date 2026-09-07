package snapshots

import (
	"context"
	"errors"
	"reflect"
	"sync/atomic"
	"testing"
	"time"
)

func TestHistorySourceWorkersBoundAndOrder(t *testing.T) {
	candidates := make([]historyCompactionCandidate, 12)
	for i := range candidates {
		candidates[i].history.FromTxNum = uint64(i)
	}
	entered, release := make(chan struct{}, len(candidates)), make(chan struct{})
	var active, peak atomic.Int32
	var progress []uint64
	var sources []stateDomainChangeBinaryCompactionSource
	var resultErr error
	done := make(chan struct{})
	go func() {
		defer close(done)
		sources, resultErr = collectHistorySourcesOrdered(context.Background(), candidates, 2, func(_ context.Context, c historyCompactionCandidate) (stateDomainChangeBinaryCompactionSource, error) {
			n := active.Add(1)
			defer active.Add(-1)
			for old := peak.Load(); n > old && !peak.CompareAndSwap(old, n); old = peak.Load() {
			}
			entered <- struct{}{}
			<-release
			return stateDomainChangeBinaryCompactionSource{history: c.history}, nil
		}, func(n uint64) { progress = append(progress, n) })
	}()
	for range 2 {
		select {
		case <-entered:
		case <-time.After(5 * time.Second):
			close(release)
			t.Fatal("two source readers did not start")
		}
	}
	if active.Load() != 2 {
		t.Errorf("active readers = %d, want 2", active.Load())
	}
	close(release)
	<-done
	if resultErr != nil || len(sources) != len(candidates) || peak.Load() != 2 || active.Load() != 0 {
		t.Fatalf("sources=%d peak=%d active=%d err=%v", len(sources), peak.Load(), active.Load(), resultErr)
	}
	var expected []uint64
	for i := range candidates {
		if !reflect.DeepEqual(sources[i].history, candidates[i].history) {
			t.Fatalf("source %d reordered", i)
		}
		expected = append(expected, uint64(i+1))
	}
	if !reflect.DeepEqual(progress, expected) {
		t.Fatalf("progress %v, want %v", progress, expected)
	}
}

func TestHistorySourceWorkersPreserveFirstError(t *testing.T) {
	firstErr, laterErr := errors.New("first source"), errors.New("later source")
	laterFinished := make(chan struct{})
	candidates := []historyCompactionCandidate{{}, {history: SegmentRef{FromTxNum: 1}}}
	sources, err := collectHistorySourcesOrdered(context.Background(), candidates, 2, func(_ context.Context, c historyCompactionCandidate) (stateDomainChangeBinaryCompactionSource, error) {
		if c.history.FromTxNum == 0 {
			<-laterFinished
			return stateDomainChangeBinaryCompactionSource{}, firstErr
		}
		close(laterFinished)
		return stateDomainChangeBinaryCompactionSource{}, laterErr
	}, func(uint64) { t.Error("failed source advanced progress") })
	if !errors.Is(err, firstErr) || sources != nil {
		t.Fatalf("sources=%v error=%v, want first source error", sources, err)
	}
}

func TestHistorySourceWorkersCancelAndJoin(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	entered := make(chan struct{}, 2)
	var active atomic.Int32
	done := make(chan error, 1)
	go func() {
		_, err := collectHistorySourcesOrdered(ctx, make([]historyCompactionCandidate, 8), 2, func(ctx context.Context, _ historyCompactionCandidate) (stateDomainChangeBinaryCompactionSource, error) {
			active.Add(1)
			defer active.Add(-1)
			entered <- struct{}{}
			<-ctx.Done()
			return stateDomainChangeBinaryCompactionSource{}, ctx.Err()
		}, func(uint64) {})
		done <- err
	}()
	for range 2 {
		select {
		case <-entered:
		case <-time.After(5 * time.Second):
			t.Fatal("readers did not start")
		}
	}
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) || active.Load() != 0 {
			t.Fatalf("returned before readers joined: active=%d err=%v", active.Load(), err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("canceled readers were not joined")
	}
}

func TestHistorySourceWorkerConfig(t *testing.T) {
	for _, value := range []string{"", "1", "2", "0", "3", "garbage"} {
		t.Setenv("GTRON_HISTORY_COMPACTION_SOURCE_WORKERS", value)
		n, err := historyCompactionSourceWorkers()
		valid := value == "" || value == "1" || value == "2"
		if (err == nil) != valid || valid && n != 1 && n != 2 {
			t.Fatalf("%q: workers=%d err=%v", value, n, err)
		}
	}
}
