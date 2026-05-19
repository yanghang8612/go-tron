// Copyright 2019 The go-ethereum Authors
// Copyright 2026 The go-tron Authors
//
// Ported from go-ethereum/core/rawdb/freezer_table_test.go. The test set is
// trimmed to the cases that exercise behaviour gtron's slice-1 cares about:
//   - append + retrieve at item N
//   - read past end returns ErrOutOfBounds
//   - close + reopen still reads
//   - index/data crash recovery (dangling head, dangling indexes)
//   - truncateHead
//   - snappy compression vs raw
//   - sharding across the maxFileSize boundary
//
// SPDX-License-Identifier: LGPL-3.0-or-later

package freezer

import (
	"bytes"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"testing"

	"github.com/ethereum/go-ethereum/metrics"
)

// newTestTable opens a fresh freezerTable inside a t.TempDir() with inactive
// meters (so tests don't leak goroutines). The returned table is closed by
// t.Cleanup automatically.
func newTestTable(t *testing.T, name string, maxSize uint32, cfg freezerTableConfig) (*freezerTable, string) {
	t.Helper()
	dir := t.TempDir()
	tab, err := newTable(dir,
		name,
		metrics.NewInactiveMeter(),
		metrics.NewInactiveMeter(),
		metrics.NewGauge(),
		maxSize,
		cfg,
		false)
	if err != nil {
		t.Fatalf("can't open freezer table: %v", err)
	}
	t.Cleanup(func() {
		// Close may already have been called in the test body; ignore the error.
		_ = tab.Close()
	})
	return tab, dir
}

// reopenTable closes the given table and reopens it in the same directory.
func reopenTable(t *testing.T, tab *freezerTable, dir string, maxSize uint32) *freezerTable {
	t.Helper()
	name := tab.name
	cfg := tab.config
	if err := tab.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	got, err := newTable(dir,
		name,
		metrics.NewInactiveMeter(),
		metrics.NewInactiveMeter(),
		metrics.NewGauge(),
		maxSize,
		cfg,
		false)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	return got
}

func getChunk(size, b int) []byte {
	data := make([]byte, size)
	for i := range data {
		data[i] = byte(b)
	}
	return data
}

func writeChunks(t *testing.T, ft *freezerTable, n, length int) {
	t.Helper()

	batch := ft.newBatch()
	for i := 0; i < n; i++ {
		if err := batch.AppendRaw(uint64(i), getChunk(length, i)); err != nil {
			t.Fatalf("AppendRaw(%d, ...) returned error: %v", i, err)
		}
	}
	if err := batch.commit(); err != nil {
		t.Fatalf("Commit returned error: %v", err)
	}
}

// TestFreezerTableBasics writes 255 short rows that force the head file to
// roll over multiple times, then reads each row back.
func TestFreezerTableBasics(t *testing.T) {
	t.Parallel()
	tab, _ := newTestTable(t, fmt.Sprintf("basics-%d", rand.Uint64()), 50, freezerTableConfig{noSnappy: true})

	writeChunks(t, tab, 255, 15)

	for y := 0; y < 255; y++ {
		exp := getChunk(15, y)
		got, err := tab.Retrieve(uint64(y))
		if err != nil {
			t.Fatalf("reading item %d: %v", y, err)
		}
		if !bytes.Equal(got, exp) {
			t.Fatalf("test %d, got \n%x != \n%x", y, got, exp)
		}
	}
	// Can't read past end.
	if _, err := tab.Retrieve(255); err != errOutOfBounds {
		t.Fatalf("expected errOutOfBounds, got %v", err)
	}
}

// TestFreezerTableReopen exercises the close+reopen path: data must survive
// the round-trip.
func TestFreezerTableReopen(t *testing.T) {
	t.Parallel()
	name := fmt.Sprintf("reopen-%d", rand.Uint64())
	tab, dir := newTestTable(t, name, 50, freezerTableConfig{noSnappy: true})

	writeChunks(t, tab, 100, 15)

	tab = reopenTable(t, tab, dir, 50)
	t.Cleanup(func() { _ = tab.Close() })

	for y := 0; y < 100; y++ {
		exp := getChunk(15, y)
		got, err := tab.Retrieve(uint64(y))
		if err != nil {
			t.Fatalf("post-reopen read %d: %v", y, err)
		}
		if !bytes.Equal(got, exp) {
			t.Fatalf("post-reopen item %d: %x != %x", y, got, exp)
		}
	}
}

// TestFreezerTableSnappyVsRaw confirms a compressed table and an uncompressed
// table can both store and retrieve the same payload (independently — they
// use different file extensions, so a mismatch fails noisily).
func TestFreezerTableSnappyVsRaw(t *testing.T) {
	t.Parallel()
	payload := []byte("the quick brown fox jumps over the lazy dog")

	raw, _ := newTestTable(t, fmt.Sprintf("raw-%d", rand.Uint64()), 1024, freezerTableConfig{noSnappy: true})
	compressed, _ := newTestTable(t, fmt.Sprintf("cmp-%d", rand.Uint64()), 1024, freezerTableConfig{noSnappy: false})

	for _, tab := range []*freezerTable{raw, compressed} {
		batch := tab.newBatch()
		for i := 0; i < 16; i++ {
			if err := batch.AppendRaw(uint64(i), payload); err != nil {
				t.Fatalf("append on %s: %v", tab.name, err)
			}
		}
		if err := batch.commit(); err != nil {
			t.Fatalf("commit on %s: %v", tab.name, err)
		}
	}

	for _, tab := range []*freezerTable{raw, compressed} {
		for i := 0; i < 16; i++ {
			got, err := tab.Retrieve(uint64(i))
			if err != nil {
				t.Fatalf("retrieve %d on %s: %v", i, tab.name, err)
			}
			if !bytes.Equal(got, payload) {
				t.Fatalf("payload mismatch on %s[%d]: %x", tab.name, i, got)
			}
		}
	}
}

// TestFreezerTableShardBoundary writes items totalling more than maxFileSize
// to force file-shard rollover, then verifies sequential reads still work.
func TestFreezerTableShardBoundary(t *testing.T) {
	t.Parallel()
	// 50-byte files; 10 items of 15 bytes each => spread across at least 3 files.
	tab, _ := newTestTable(t, fmt.Sprintf("shard-%d", rand.Uint64()), 50, freezerTableConfig{noSnappy: true})

	writeChunks(t, tab, 10, 15)

	// File 0 should hit 45 bytes (3 items × 15) then roll to 0001.rdat.
	// File 1 likewise; the tenth item ends up in 0003.rdat.
	for i := 0; i < 10; i++ {
		if _, err := tab.Retrieve(uint64(i)); err != nil {
			t.Fatalf("item %d unreadable across shard boundary: %v", i, err)
		}
	}
	// Confirm we actually rolled — head must be on file > 0.
	if tab.headId == 0 {
		t.Fatalf("head still on file 0 after 150 bytes written into 50-byte shards")
	}
}

// TestFreezerTableTruncateHead writes 30 items then truncates to 10.
func TestFreezerTableTruncateHead(t *testing.T) {
	t.Parallel()
	tab, _ := newTestTable(t, fmt.Sprintf("trunc-%d", rand.Uint64()), 50, freezerTableConfig{noSnappy: true})
	writeChunks(t, tab, 30, 15)

	if err := tab.truncateHead(10); err != nil {
		t.Fatalf("truncateHead: %v", err)
	}
	if got := tab.items.Load(); got != 10 {
		t.Fatalf("items after truncate: want 10, got %d", got)
	}
	// Items 0..9 still readable.
	for i := 0; i < 10; i++ {
		if _, err := tab.Retrieve(uint64(i)); err != nil {
			t.Fatalf("post-truncate read %d: %v", i, err)
		}
	}
	// Item 10 must be gone.
	if _, err := tab.Retrieve(10); err == nil {
		t.Fatalf("expected errOutOfBounds at 10 post-truncate, got nil")
	}
}

// TestFreezerTableDanglingHead truncates the index file by 4 bytes after a
// clean shutdown. On reopen, the corruption-detection code must drop the
// torn entry and leave the rest intact.
func TestFreezerTableDanglingHead(t *testing.T) {
	t.Parallel()
	name := fmt.Sprintf("dangling-head-%d", rand.Uint64())
	tab, dir := newTestTable(t, name, 50, freezerTableConfig{noSnappy: true})
	writeChunks(t, tab, 255, 15)
	if _, err := tab.Retrieve(0xfe); err != nil {
		t.Fatalf("pre-corrupt read: %v", err)
	}
	if err := tab.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	// Lop 4 bytes off the index file — that's less than one indexEntry (6
	// bytes) so the repair path will pad-truncate back to a multiple of 6,
	// dropping the last full entry along with the partial one.
	idxPath := filepath.Join(dir, fmt.Sprintf("%s.ridx", name))
	idxFile, err := os.OpenFile(idxPath, os.O_RDWR, 0644)
	if err != nil {
		t.Fatalf("open index: %v", err)
	}
	stat, err := idxFile.Stat()
	if err != nil {
		idxFile.Close()
		t.Fatalf("stat: %v", err)
	}
	if err := idxFile.Truncate(stat.Size() - 4); err != nil {
		idxFile.Close()
		t.Fatalf("truncate: %v", err)
	}
	idxFile.Close()

	tab, err = newTable(dir,
		name,
		metrics.NewInactiveMeter(),
		metrics.NewInactiveMeter(),
		metrics.NewGauge(),
		50,
		freezerTableConfig{noSnappy: true},
		false)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	t.Cleanup(func() { _ = tab.Close() })

	if _, err := tab.Retrieve(0xff); err == nil {
		t.Errorf("expected errOutOfBounds for torn index entry, got nil")
	}
	if _, err := tab.Retrieve(0xfd); err != nil {
		t.Errorf("entry below torn index unreadable: %v", err)
	}
}

// TestFreezerTableDanglingData crops a non-head .rdat file mid-write. The
// reopen path should truncate the index back to a consistent length.
func TestFreezerTableDanglingData(t *testing.T) {
	t.Parallel()
	name := fmt.Sprintf("dangling-data-%d", rand.Uint64())
	tab, dir := newTestTable(t, name, 50, freezerTableConfig{noSnappy: true})
	writeChunks(t, tab, 9, 15) // 3 files of 45 bytes, exactly
	if err := tab.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	// Crop the third data file from 45 to 20 bytes — mid-item.
	cropPath := filepath.Join(dir, fmt.Sprintf("%s.0002.rdat", name))
	stat, err := os.Stat(cropPath)
	if err != nil {
		t.Fatalf("stat %s: %v", cropPath, err)
	}
	if stat.Size() != 45 {
		t.Fatalf("expected 45 bytes in %s, got %d", cropPath, stat.Size())
	}
	cropFile, err := os.OpenFile(cropPath, os.O_RDWR, 0644)
	if err != nil {
		t.Fatalf("open crop: %v", err)
	}
	if err := cropFile.Truncate(20); err != nil {
		cropFile.Close()
		t.Fatalf("truncate crop: %v", err)
	}
	cropFile.Close()

	// Reopen — should rewind to 7 items (45+45+15 bytes) and resize the data
	// file to 15 bytes.
	tab, err = newTable(dir,
		name,
		metrics.NewInactiveMeter(),
		metrics.NewInactiveMeter(),
		metrics.NewGauge(),
		50,
		freezerTableConfig{noSnappy: true},
		false)
	if err != nil {
		t.Fatalf("reopen after data crop: %v", err)
	}
	t.Cleanup(func() { _ = tab.Close() })

	if got := tab.items.Load(); got != 7 {
		t.Errorf("items after crop repair: want 7, got %d", got)
	}
	stat, err = os.Stat(cropPath)
	if err != nil {
		t.Fatalf("stat post-repair: %v", err)
	}
	if stat.Size() != 15 {
		t.Errorf("crop file size after repair: want 15, got %d", stat.Size())
	}
}

// TestFreezerTableSequentialRead exercises RetrieveItems across a range that
// crosses file-shard boundaries.
func TestFreezerTableSequentialRead(t *testing.T) {
	t.Parallel()
	tab, _ := newTestTable(t, fmt.Sprintf("seqread-%d", rand.Uint64()), 50, freezerTableConfig{noSnappy: true})
	writeChunks(t, tab, 30, 15)

	items, err := tab.RetrieveItems(0, 10000, 100000)
	if err != nil {
		t.Fatalf("RetrieveItems: %v", err)
	}
	if len(items) != 30 {
		t.Fatalf("expected 30 items, got %d", len(items))
	}
	for i, have := range items {
		want := getChunk(15, i)
		if !bytes.Equal(want, have) {
			t.Fatalf("RetrieveItems[%d]: %x != %x", i, have, want)
		}
	}
}

// TestFreezerTableSnappyMismatch checks that a snappy-configured reader can't
// read a raw-data table (file extensions differ; the index won't be found).
func TestFreezerTableSnappyMismatch(t *testing.T) {
	t.Parallel()
	name := fmt.Sprintf("mismatch-%d", rand.Uint64())
	tab, dir := newTestTable(t, name, 50, freezerTableConfig{noSnappy: true})
	writeChunks(t, tab, 5, 15)
	if err := tab.Close(); err != nil {
		t.Fatalf("close raw: %v", err)
	}
	// Reopen with noSnappy=false. The configuration looks for .cidx not
	// .ridx, so it sees an empty table.
	tab, err := newTable(dir,
		name,
		metrics.NewInactiveMeter(),
		metrics.NewInactiveMeter(),
		metrics.NewGauge(),
		50,
		freezerTableConfig{noSnappy: false},
		false)
	if err != nil {
		t.Fatalf("reopen as compressed: %v", err)
	}
	defer tab.Close()
	if _, err := tab.Retrieve(0); err == nil {
		t.Fatalf("expected empty-table read failure when extension/config disagree")
	}
}
