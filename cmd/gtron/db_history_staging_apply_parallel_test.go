package main

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/tronprotocol/go-tron/core/rawdb"
)

func stagingTestNext(n uint64) func(context.Context) (historyStagingPlanBucket, bool, error) {
	var bucket uint64
	return func(ctx context.Context) (historyStagingPlanBucket, bool, error) {
		if err := ctx.Err(); err != nil {
			return historyStagingPlanBucket{}, false, err
		}
		bucket++
		return historyStagingPlanBucket{Proof: rawdb.HistoryStagingProof{Bucket: bucket}}, bucket <= n, nil
	}
}

func TestHistoryStagingApplyPipelineWindowOrderAndJoin(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	started := make(chan uint64, 32)
	release := make(chan struct{})
	var active, peak atomic.Int64
	var written []uint64
	done := make(chan error, 1)
	go func() {
		done <- pipelineHistoryStagingRows(ctx, 8, stagingTestNext(32),
			func(ctx context.Context, _ int, row historyStagingPlanBucket) (func(context.Context) error, error) {
				now := active.Add(1)
				defer active.Add(-1)
				for old := peak.Load(); now > old; old = peak.Load() {
					if peak.CompareAndSwap(old, now) {
						break
					}
				}
				started <- row.Proof.Bucket
				select {
				case <-release:
				case <-ctx.Done():
					return nil, ctx.Err()
				}
				return func(c context.Context) error { return c.Err() }, nil
			}, func(row historyStagingPlanBucket, check func(context.Context) error) error {
				if err := check(ctx); err != nil {
					return err
				}
				written = append(written, row.Proof.Bucket)
				return nil
			})
	}()
	for i := 0; i < 8; i++ {
		select {
		case <-started:
		case <-ctx.Done():
			t.Fatal("workers did not all start")
		}
	}
	select {
	case b := <-started:
		t.Fatalf("window overflow at %d", b)
	case <-time.After(20 * time.Millisecond):
	}
	close(release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if active.Load() != 0 || peak.Load() != 8 || len(written) != 32 {
		t.Fatalf("active=%d peak=%d written=%d", active.Load(), peak.Load(), len(written))
	}
	for i, b := range written {
		if b != uint64(i+1) {
			t.Fatalf("unordered %v", written)
		}
	}
}

func TestHistoryStagingApplyPipelineCancellationAndFailuresJoin(t *testing.T) {
	for _, mode := range []string{"cancel", "prepare", "consume", "next"} {
		t.Run(mode, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			var active atomic.Int64
			sentinel := errors.New(mode)
			started := make(chan struct{}, 8)
			next := stagingTestNext(16)
			var read int
			done := make(chan error, 1)
			go func() {
				done <- pipelineHistoryStagingRows(ctx, 8,
					func(ctx context.Context) (historyStagingPlanBucket, bool, error) {
						read++
						if mode == "next" && read == 9 {
							return historyStagingPlanBucket{}, false, sentinel
						}
						return next(ctx)
					}, func(ctx context.Context, _ int, row historyStagingPlanBucket) (func(context.Context) error, error) {
						active.Add(1)
						defer active.Add(-1)
						if mode == "cancel" {
							started <- struct{}{}
							<-ctx.Done()
							return nil, ctx.Err()
						}
						if mode == "prepare" && row.Proof.Bucket == 3 {
							return nil, sentinel
						}
						return nil, nil
					}, func(row historyStagingPlanBucket, _ func(context.Context) error) error {
						if mode == "consume" && row.Proof.Bucket == 3 {
							return sentinel
						}
						return nil
					})
			}()
			if mode == "cancel" {
				for i := 0; i < 8; i++ {
					<-started
				}
				cancel()
			}
			select {
			case err := <-done:
				if mode == "cancel" {
					if !errors.Is(err, context.Canceled) {
						t.Fatal(err)
					}
				} else if !errors.Is(err, sentinel) {
					t.Fatal(err)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("pipeline did not stop")
			}
			if active.Load() != 0 {
				t.Fatal("worker leak")
			}
		})
	}
}

func TestHistoryStagingApplyPipelineReportsLowestFailure(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	third := make(chan struct{})
	secondErr, thirdErr := errors.New("second"), errors.New("third")
	var consumed []uint64
	err := pipelineHistoryStagingRows(ctx, 8, stagingTestNext(16),
		func(ctx context.Context, _ int, row historyStagingPlanBucket) (func(context.Context) error, error) {
			switch row.Proof.Bucket {
			case 2:
				select {
				case <-third:
					return nil, secondErr
				case <-ctx.Done():
					return nil, ctx.Err()
				}
			case 3:
				close(third)
				return nil, thirdErr
			}
			return nil, nil
		}, func(row historyStagingPlanBucket, _ func(context.Context) error) error {
			consumed = append(consumed, row.Proof.Bucket)
			return nil
		})
	if !errors.Is(err, secondErr) || len(consumed) != 1 || consumed[0] != 1 {
		t.Fatalf("err=%v consumed=%v", err, consumed)
	}
}
