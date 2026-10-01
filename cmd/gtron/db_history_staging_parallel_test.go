package main

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/tronprotocol/go-tron/core/rawdb"
)

func TestHistoryStagingParallelPlannerBoundedAndOrdered(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	started := make(chan uint64, 16)
	release := make(chan struct{})
	var active, peak atomic.Int64
	var written []uint64
	done := make(chan error, 1)
	go func() {
		done <- writeHistoryStagingPlanBuckets(ctx, 16, 4,
			func(ctx context.Context, _ int, bucket uint64) (historyStagingPlanBucket, error) {
				current := active.Add(1)
				defer active.Add(-1)
				for old := peak.Load(); current > old; old = peak.Load() {
					if peak.CompareAndSwap(old, current) {
						break
					}
				}
				started <- bucket
				select {
				case <-release:
				case <-ctx.Done():
					return historyStagingPlanBucket{}, ctx.Err()
				}
				return historyStagingPlanBucket{Proof: rawdb.HistoryStagingProof{Bucket: bucket}}, nil
			}, func(row historyStagingPlanBucket) error {
				written = append(written, row.Proof.Bucket)
				return nil
			}, nil)
	}()
	for i := 0; i < 4; i++ {
		select {
		case <-started:
		case <-ctx.Done():
			t.Fatal("four bounded workers did not start")
		}
	}
	select {
	case bucket := <-started:
		t.Fatalf("dispatched bucket %d while all four slots were blocked", bucket)
	case <-time.After(20 * time.Millisecond):
	}
	close(release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if peak.Load() > 4 || active.Load() != 0 || len(written) != 16 {
		t.Fatalf("workers peak=%d active=%d written=%d", peak.Load(), active.Load(), len(written))
	}
	for i, bucket := range written {
		if bucket != uint64(i+1) {
			t.Fatalf("plan row %d is bucket %d", i, bucket)
		}
	}
}

func TestHistoryStagingParallelPlannerLowestErrorAndJoin(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	err2, err3 := errors.New("bucket two"), errors.New("bucket three")
	thirdStarted := make(chan struct{})
	releaseSecond := make(chan struct{})
	var active atomic.Int64
	var written []uint64
	done := make(chan error, 1)
	go func() {
		done <- writeHistoryStagingPlanBuckets(ctx, 8, 3,
			func(ctx context.Context, _ int, bucket uint64) (historyStagingPlanBucket, error) {
				active.Add(1)
				defer active.Add(-1)
				switch bucket {
				case 2:
					select {
					case <-releaseSecond:
						return historyStagingPlanBucket{}, err2
					case <-ctx.Done():
						return historyStagingPlanBucket{}, ctx.Err()
					}
				case 3:
					close(thirdStarted)
					return historyStagingPlanBucket{}, err3
				default:
					return historyStagingPlanBucket{Proof: rawdb.HistoryStagingProof{Bucket: bucket}}, nil
				}
			}, func(row historyStagingPlanBucket) error {
				written = append(written, row.Proof.Bucket)
				return nil
			}, nil)
	}()
	select {
	case <-thirdStarted:
	case <-ctx.Done():
		t.Fatal("third bucket did not start")
	}
	close(releaseSecond)
	if err := <-done; !errors.Is(err, err2) {
		t.Fatalf("planner did not return lowest bucket error: %v", err)
	}
	if active.Load() != 0 || len(written) != 1 || written[0] != 1 {
		t.Fatalf("workers not joined or failed rows published: active=%d written=%v", active.Load(), written)
	}
}

func TestHistoryStagingParallelPlannerCancellationJoinsWorkers(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	started := make(chan struct{}, 4)
	var active atomic.Int64
	done := make(chan error, 1)
	go func() {
		done <- writeHistoryStagingPlanBuckets(ctx, 16, 4,
			func(ctx context.Context, _ int, _ uint64) (historyStagingPlanBucket, error) {
				active.Add(1)
				defer active.Add(-1)
				started <- struct{}{}
				<-ctx.Done()
				return historyStagingPlanBucket{}, ctx.Err()
			}, func(historyStagingPlanBucket) error { return nil }, nil)
	}()
	for i := 0; i < 4; i++ {
		select {
		case <-started:
		case <-time.After(3 * time.Second):
			cancel()
			t.Fatal("workers did not start")
		}
	}
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) || active.Load() != 0 {
			t.Fatalf("cancel did not join workers: err=%v active=%d", err, active.Load())
		}
	case <-time.After(3 * time.Second):
		t.Fatal("canceled planner did not join")
	}
}
