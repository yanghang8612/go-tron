package freezer

import (
	"bytes"
	"context"
	"fmt"
	"sync"
	"testing"
	"time"
)

type freezerSyncReadiness struct {
	coverage  uint64
	canAppend bool
	observed  bool
}

// The lock holder stays blocked until the assertion has observed a result. A
// timeout also releases that holder so a regression to RLock fails rather than
// leaving the test's cleanup or probe goroutine waiting forever.
func readFreezerSyncReadinessPromptly(t *testing.T, f *Freezer, releaseHolder func()) freezerSyncReadiness {
	t.Helper()
	done := make(chan freezerSyncReadiness, 1)
	go func() {
		coverage, canAppend, observed := f.TryDirectV2AppendStatus()
		done <- freezerSyncReadiness{coverage, canAppend, observed}
	}()
	select {
	case got := <-done:
		return got
	case <-time.After(time.Second):
		releaseHolder()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Error("readiness probe did not exit after the lock holder was released")
		}
		t.Fatal("readiness probe waited for an optional-work lock")
		return freezerSyncReadiness{}
	}
}

func TestFreezerSyncReadinessDuringRealDirectV2Migration(t *testing.T) {
	dir := t.TempDir()
	tables := map[string]TableConfig{
		"bodies":      {Prunable: true},
		"tx_infos":    {Prunable: true},
		"state_roots": {NoSnappy: true, Prunable: true},
	}
	f, err := NewFreezer(dir, "", false, 2049, tables)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	entered, release, migrated := make(chan struct{}), make(chan struct{}), make(chan struct{})
	var enterOnce, releaseOnce sync.Once
	unblock := func() { releaseOnce.Do(func() { close(release) }) }
	var result V2MigrationResult
	var migrateErr error
	want := func(kind string, number uint64) []byte {
		return []byte(fmt.Sprintf("readiness-%s-canonical-block-%d", kind, number))
	}
	t.Cleanup(func() {
		cancel()
		unblock()
		select {
		case <-migrated:
			if err := f.Close(); err != nil {
				t.Errorf("close freezer: %v", err)
			}
		case <-time.After(5 * time.Second):
			// Do not call Close while a broken migration still owns writeLock.
			// That would turn a useful assertion failure into a test-suite hang.
			t.Error("migration did not exit after cancellation and source release")
		}
	})
	go func() {
		result, migrateErr = f.MigrateV2(V2MigrationOptions{
			Tables:        []string{"bodies", "tx_infos", "state_roots"},
			SegmentBlocks: 64, FrameBlocks: 8, MaxSegments: 1,
			Online: true, SourceHead: 64, Context: ctx,
			Source: func(kind string, number uint64) ([]byte, error) {
				enterOnce.Do(func() {
					close(entered)
					<-release
				})
				if err := ctx.Err(); err != nil {
					return nil, err
				}
				return want(kind, number), nil
			},
		})
		close(migrated)
	}()
	select {
	case <-entered:
	case <-migrated:
		t.Fatalf("migration exited before entering the source: %v", migrateErr)
	case <-time.After(5 * time.Second):
		t.Fatal("migration did not reach its direct source")
	}
	if f.writeLock.TryRLock() {
		f.writeLock.RUnlock()
		t.Fatal("fixture did not hold the real migration write lock")
	}
	got := readFreezerSyncReadinessPromptly(t, f, unblock)
	if got.observed || got.canAppend {
		t.Fatalf("busy migration was reported as a valid readiness result: %+v", got)
	}
	select {
	case <-migrated:
		t.Fatal("migration unexpectedly completed while its source was blocked")
	default:
	}
	unblock()
	select {
	case <-migrated:
	case <-time.After(5 * time.Second):
		t.Fatal("migration did not finish after source release")
	}
	if migrateErr != nil || result.End != 64 || result.Segments != 1 {
		t.Fatalf("migration result=%+v err=%v", result, migrateErr)
	}
	got = readFreezerSyncReadinessPromptly(t, f, func() {})
	if !got.observed || !got.canAppend || got.coverage != 64 {
		t.Fatalf("completed V2 migration is not ready at its durable prefix: %+v", got)
	}
	for kind := range tables {
		for number := uint64(0); number < 64; number++ {
			got, err := f.Ancient(kind, number)
			if err != nil || !bytes.Equal(got, want(kind, number)) {
				t.Fatalf("canonical %s[%d]=%q err=%v", kind, number, got, err)
			}
		}
	}
}

func TestFreezerSyncReadinessUnknownDuringV2StoreSwap(t *testing.T) {
	f, err := NewFreezer(t.TempDir(), "", false, 2049, map[string]TableConfig{"bodies": {Prunable: true}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = f.Close() })
	f.v2Mu.Lock()
	var once sync.Once
	unblock := func() { once.Do(func() { f.v2Mu.Unlock() }) }
	t.Cleanup(unblock)
	got := readFreezerSyncReadinessPromptly(t, f, unblock)
	if got.observed || got.canAppend {
		t.Fatalf("locked V2 store was treated as an observed negative/positive: %+v", got)
	}
	unblock()
	got = readFreezerSyncReadinessPromptly(t, f, func() {})
	if !got.observed || !got.canAppend || got.coverage != 0 {
		t.Fatalf("empty unlocked freezer should permit direct genesis append: %+v", got)
	}
}

func TestFreezerSyncReadinessObservesV1Suffix(t *testing.T) {
	f, err := NewFreezer(t.TempDir(), "", false, 2049, map[string]TableConfig{"bodies": {Prunable: true}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = f.Close() })
	if _, err := f.ModifyAncients(func(op AncientWriteOp) error {
		return op.AppendRaw("bodies", 0, []byte("visible-v1-suffix"))
	}); err != nil {
		t.Fatal(err)
	}
	got := readFreezerSyncReadinessPromptly(t, f, func() {})
	if !got.observed || got.canAppend || got.coverage != 0 {
		t.Fatalf("V1 suffix must be a valid observed negative: %+v", got)
	}
	if data, err := f.Ancient("bodies", 0); err != nil || string(data) != "visible-v1-suffix" {
		t.Fatalf("readiness probe changed the V1 suffix: %q err=%v", data, err)
	}
}
