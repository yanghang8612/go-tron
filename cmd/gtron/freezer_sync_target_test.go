package main

import (
	"fmt"
	"sync"
	"testing"
	"time"

	rawdbfreezer "github.com/tronprotocol/go-tron/core/rawdb/freezer"
)

func TestSyncEventLogTargetDoesNotWaitForDirectMigration(t *testing.T) {
	f, err := rawdbfreezer.NewFreezer(t.TempDir(), "", false, 2049, map[string]rawdbfreezer.TableConfig{
		"bodies": {Prunable: true}, "tx_infos": {Prunable: true}, "state_roots": {Prunable: true, NoSnappy: true},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	probe := makeSyncEventLogTargetBlock(f, 64, nil)
	if target, valid, deferred := probe(); target != 63 || !valid || deferred {
		t.Fatalf("initial target: %d %v %v", target, valid, deferred)
	}
	entered, resume := make(chan struct{}), make(chan struct{})
	var enterOnce, resumeOnce sync.Once
	unblock := func() { resumeOnce.Do(func() { close(resume) }) }
	done := make(chan error, 1)
	go func() {
		_, err := f.MigrateV2(rawdbfreezer.V2MigrationOptions{
			Tables: []string{"bodies", "tx_infos", "state_roots"}, SegmentBlocks: 64, FrameBlocks: 8,
			MaxSegments: 1, Online: true, SourceHead: 64,
			Source: func(kind string, number uint64) ([]byte, error) {
				enterOnce.Do(func() { close(entered) })
				<-resume
				return []byte(fmt.Sprintf("%s-%d", kind, number)), nil
			},
		})
		done <- err
	}()
	defer func() {
		unblock()
		select {
		case err := <-done:
			if err != nil {
				t.Error(err)
			}
		case <-time.After(10 * time.Second):
			t.Error("direct migration did not finish")
		}
	}()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("direct migration did not acquire its write lock")
	}
	type result struct {
		target          uint64
		valid, deferred bool
	}
	observed := make(chan result, 1)
	go func() {
		target, valid, deferred := probe()
		observed <- result{target, valid, deferred}
	}()
	select {
	case got := <-observed:
		if got.valid || !got.deferred {
			t.Fatalf("busy layout treated as a valid negative: %+v", got)
		}
	case <-time.After(time.Second):
		t.Fatal("event target callback waited for the full direct segment")
	}
	unblock()
	select {
	case err := <-done:
		// Preserve the result for the cleanup path, including after a failure.
		done <- err
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("direct migration did not complete")
	}
	if target, valid, deferred := probe(); target != 127 || !valid || deferred {
		t.Fatalf("next target did not resume after publication: %d %v %v", target, valid, deferred)
	}
}
