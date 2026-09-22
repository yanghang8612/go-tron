package freezer

import (
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
)

func buildPublishTestRun(tb testing.TB, dir string, start, end uint64, prefixBits uint32) (TransactionIndexBuildResult, TransactionIndexEntry) {
	tb.Helper()
	var entry TransactionIndexEntry
	entry.Hash[0] = byte(start + 1)
	binary.BigEndian.PutUint64(entry.Hash[24:], start+1)
	entry.Location = transactionLocationMarker | start<<transactionLocationOrdinalBits
	result, err := BuildTransactionIndexRun(TransactionIndexRunPath(dir, start, end), TransactionIndexBuildOptions{
		PrefixBits: prefixBits,
		StartBlock: start,
		EndBlock:   end,
		Iterate:    transactionIndexTestIterator([]TransactionIndexEntry{entry}),
	})
	if err != nil {
		tb.Fatal(err)
	}
	return result, entry
}

func testPublishFreezer(tb testing.TB, dir string, coverage uint64) *Freezer {
	tb.Helper()
	store, err := OpenTransactionIndexStore(dir)
	if err != nil {
		tb.Fatal(err)
	}
	return &Freezer{datadir: dir, v2: &v2Store{coverage: coverage}, txIndex: store}
}

func TestFreezerPublishTransactionIndexRunReusesVerifiedRuns(t *testing.T) {
	dir := t.TempDir()
	first, firstEntry := buildPublishTestRun(t, dir, 0, 10, 8)
	if err := PublishTransactionIndexRun(dir, first); err != nil {
		t.Fatal(err)
	}
	f := testPublishFreezer(t, dir, 30)
	defer f.txIndex.Close()
	selected, oldRun := f.txIndex, f.txIndex.runs[0]
	second, secondEntry := buildPublishTestRun(t, dir, 10, 20, 8)
	if err := f.PublishTransactionIndexRun(second); err != nil {
		t.Fatal(err)
	}
	if f.txIndex != selected || f.txIndex.runs[0] != oldRun || f.TransactionIndexCoverage() != 20 {
		t.Fatal("healthy publish replaced the validated old run or failed to extend coverage")
	}
	for _, entry := range []TransactionIndexEntry{firstEntry, secondEntry} {
		locations, err := f.TransactionIndexCandidates(entry.Hash)
		if err != nil || len(locations) != 1 || locations[0] != entry.Location {
			t.Fatalf("candidates for %x = %v, %v", entry.Hash, locations, err)
		}
	}
	opened, err := OpenTransactionIndexStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer opened.Close()
	if opened.Coverage() != 20 || len(opened.runs) != 2 {
		t.Fatalf("persisted coverage/runs = %d/%d", opened.Coverage(), len(opened.runs))
	}
	// A replay of the same durable publication validates the run but must not
	// reopen/replace already installed readers.
	if err := f.PublishTransactionIndexRun(second); err != nil || f.txIndex != selected {
		t.Fatalf("idempotent replay replaced live store: %v", err)
	}
}

func TestFreezerPublishTransactionIndexRunRejectsCorruptionAndRecoversStaleManifest(t *testing.T) {
	dir := t.TempDir()
	first, firstEntry := buildPublishTestRun(t, dir, 0, 10, 8)
	if err := PublishTransactionIndexRun(dir, first); err != nil {
		t.Fatal(err)
	}
	f := testPublishFreezer(t, dir, 30)
	defer func() { _ = f.txIndex.Close() }()
	selected := f.txIndex
	oldRun := selected.runs[0]
	second, entry := buildPublishTestRun(t, dir, 10, 20, 8)
	file, err := os.OpenFile(second.Path, os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	var corrupt [1]byte
	if _, err := file.ReadAt(corrupt[:], int64(second.FileBytes-1)); err != nil {
		t.Fatal(err)
	}
	corrupt[0] ^= 0xff
	if _, err := file.WriteAt(corrupt[:], int64(second.FileBytes-1)); err != nil {
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	if err := f.PublishTransactionIndexRun(second); err == nil {
		t.Fatal("corrupt new run was published")
	}
	if f.txIndex != selected || f.TransactionIndexCoverage() != 10 {
		t.Fatal("failed verification changed live coverage")
	}
	manifest, err := readTransactionIndexManifest(filepath.Join(dir, transactionIndexDirectoryName))
	if err != nil || len(manifest.Runs) != 1 {
		t.Fatalf("failed verification changed durable manifest: %+v %v", manifest, err)
	}
	if err := os.Remove(second.Path); err != nil {
		t.Fatal(err)
	}
	second, entry = buildPublishTestRun(t, dir, 10, 20, 8)
	// Simulate a crash after durable manifest publication but before the live
	// store update. The next Freezer publish must reconcile from disk.
	if err := PublishTransactionIndexRun(dir, second); err != nil {
		t.Fatal(err)
	}
	if err := f.PublishTransactionIndexRun(second); err != nil {
		t.Fatal(err)
	}
	if f.txIndex == selected || f.TransactionIndexCoverage() != 20 {
		t.Fatal("stale live store was not reconciled")
	}
	if _, err := oldRun.Candidates(firstEntry.Hash); err == nil {
		t.Fatal("retired stale run was not closed after reader handoff")
	}
	locations, err := f.TransactionIndexCandidates(entry.Hash)
	if err != nil || len(locations) != 1 || locations[0] != entry.Location {
		t.Fatalf("recovered candidates = %v, %v", locations, err)
	}
}

func TestFreezerPublishTransactionIndexRunConcurrentReaders(t *testing.T) {
	dir := t.TempDir()
	first, entry := buildPublishTestRun(t, dir, 0, 10, 8)
	if err := PublishTransactionIndexRun(dir, first); err != nil {
		t.Fatal(err)
	}
	f := testPublishFreezer(t, dir, 100)
	defer f.txIndex.Close()
	second, _ := buildPublishTestRun(t, dir, 10, 20, 8)
	var wg sync.WaitGroup
	errCh := make(chan error, 1)
	started := make(chan struct{})
	var startOnce sync.Once
	signalStarted := func() { startOnce.Do(func() { close(started) }) }
	wg.Add(1)
	go func() {
		defer wg.Done()
		defer signalStarted()
		for i := 0; i < 10_000; i++ {
			f.v2Mu.RLock()
			coverage, runs := f.txIndex.coverage, len(f.txIndex.runs)
			f.v2Mu.RUnlock()
			if (coverage != 10 || runs != 1) && (coverage != 20 || runs != 2) {
				errCh <- os.ErrInvalid
				return
			}
			locations, err := f.TransactionIndexCandidates(entry.Hash)
			if err != nil {
				errCh <- err
				return
			}
			if len(locations) != 1 || locations[0] != entry.Location {
				errCh <- os.ErrInvalid
				return
			}
			if i == 0 {
				signalStarted()
			}
		}
	}()
	<-started
	if err := f.PublishTransactionIndexRun(second); err != nil {
		t.Fatal(err)
	}
	wg.Wait()
	select {
	case err := <-errCh:
		t.Fatal(err)
	default:
	}
}

func TestFreezerPublishTransactionIndexRunRejectsManifestBeyondV2(t *testing.T) {
	dir := t.TempDir()
	first, entry := buildPublishTestRun(t, dir, 0, 10, 8)
	if err := PublishTransactionIndexRun(dir, first); err != nil {
		t.Fatal(err)
	}
	f := testPublishFreezer(t, dir, 20)
	defer func() { _ = f.txIndex.Close() }()
	selected := f.txIndex
	future, _ := buildPublishTestRun(t, dir, 10, 30, 8)
	if err := PublishTransactionIndexRun(dir, future); err != nil {
		t.Fatal(err)
	}
	if err := f.PublishTransactionIndexRun(first); err == nil {
		t.Fatal("manifest coverage beyond V2 was installed")
	}
	if f.txIndex != selected || f.TransactionIndexCoverage() != 10 {
		t.Fatal("rejected manifest replaced the live reader")
	}
	locations, err := f.TransactionIndexCandidates(entry.Hash)
	if err != nil || len(locations) != 1 || locations[0] != entry.Location {
		t.Fatalf("old reader no longer works: %v, %v", locations, err)
	}
}

func TestFreezerPublishTransactionIndexRunRefreshesChangedManifestMetadata(t *testing.T) {
	dir := t.TempDir()
	first, _ := buildPublishTestRun(t, dir, 0, 10, 8)
	if err := PublishTransactionIndexRun(dir, first); err != nil {
		t.Fatal(err)
	}
	f := testPublishFreezer(t, dir, 20)
	defer func() { _ = f.txIndex.Close() }()
	selected := f.txIndex
	base := filepath.Join(dir, transactionIndexDirectoryName)
	manifest, err := readTransactionIndexManifest(base)
	if err != nil {
		t.Fatal(err)
	}
	manifest.Runs[0].CompactionLevel++
	if err := writeTransactionIndexManifest(base, manifest); err != nil {
		t.Fatal(err)
	}
	second, _ := buildPublishTestRun(t, dir, 10, 20, 8)
	if err := f.PublishTransactionIndexRun(second); err != nil {
		t.Fatal(err)
	}
	if f.txIndex == selected || f.txIndex.manifestRuns[0] != manifest.Runs[0] {
		t.Fatal("changed durable manifest declaration was not loaded")
	}
}

func BenchmarkFreezerPublishTransactionIndexRunWithExistingRuns(b *testing.B) {
	for _, variant := range []struct {
		name    string
		publish func(*Freezer, TransactionIndexBuildResult) error
	}{
		{"full_reopen_reference", publishTransactionIndexRunFullReopenForBenchmark},
		{"incremental", (*Freezer).PublishTransactionIndexRun},
	} {
		b.Run(variant.name, func(b *testing.B) {
			b.StopTimer()
			dir := b.TempDir()
			// Four existing 20-bit directories (8 MiB apiece) and one new
			// run. Both paths include full new-run Verify and durable manifest
			// publication; only reset for the next iteration is excluded.
			for i := uint64(0); i < 4; i++ {
				result, _ := buildPublishTestRun(b, dir, i*10, (i+1)*10, 20)
				if err := PublishTransactionIndexRun(dir, result); err != nil {
					b.Fatal(err)
				}
			}
			f := testPublishFreezer(b, dir, 50)
			defer func() { _ = f.txIndex.Close() }()
			result, _ := buildPublishTestRun(b, dir, 40, 50, 20)
			base := filepath.Join(dir, transactionIndexDirectoryName)
			manifest, err := readTransactionIndexManifest(base)
			if err != nil {
				b.Fatal(err)
			}
			b.ReportAllocs()
			b.ResetTimer()
			b.StartTimer()
			for i := 0; i < b.N; i++ {
				if err := variant.publish(f, result); err != nil {
					b.Fatal(err)
				}
				b.StopTimer()
				f.v2Mu.Lock()
				last := f.txIndex.runs[len(f.txIndex.runs)-1]
				f.txIndex.runs = f.txIndex.runs[:len(f.txIndex.runs)-1]
				f.txIndex.manifestRuns = f.txIndex.manifestRuns[:len(f.txIndex.manifestRuns)-1]
				f.txIndex.coverage = 40
				f.v2Mu.Unlock()
				if err := last.Close(); err != nil {
					b.Fatal(err)
				}
				if err := writeTransactionIndexManifest(base, manifest); err != nil {
					b.Fatal(err)
				}
				if i+1 < b.N {
					b.StartTimer()
				}
			}
		})
	}
}

// This reference copies the prior Freezer publish sequence solely for a
// same-input before/after benchmark. It is not used by production code.
func publishTransactionIndexRunFullReopenForBenchmark(f *Freezer, result TransactionIndexBuildResult) error {
	f.v2Migrate.Lock()
	defer f.v2Migrate.Unlock()
	if f.readonly {
		return errReadOnly
	}
	if result.EndBlock > f.V2Coverage() {
		return fmt.Errorf("transaction index end %d exceeds V2 coverage %d", result.EndBlock, f.V2Coverage())
	}
	current, err := OpenTransactionIndexStore(f.datadir)
	if err != nil {
		return err
	}
	if current.Coverage() == result.EndBlock {
		if err := verifyTransactionIndexBuildResult(f.datadir, result); err != nil {
			_ = current.Close()
			return err
		}
		f.replaceTransactionIndexStore(current)
		return nil
	}
	if current.Coverage() != result.StartBlock {
		_ = current.Close()
		return fmt.Errorf("transaction index publish: live coverage %d does not match run start %d", current.Coverage(), result.StartBlock)
	}
	if err := current.Close(); err != nil {
		return err
	}
	if err := PublishTransactionIndexRun(f.datadir, result); err != nil {
		return err
	}
	store, err := OpenTransactionIndexStore(f.datadir)
	if err != nil {
		return err
	}
	f.replaceTransactionIndexStore(store)
	return nil
}
