// Copyright 2021 The go-ethereum Authors
// Copyright 2026 The go-tron Authors
//
// Ported from go-ethereum/core/rawdb/freezer_test.go, narrowed to slice-1
// behaviour: ModifyAncients + Ancient retrieval, ModifyAncients rollback,
// TruncateHead, and AncientCount / HasAncient.
//
// SPDX-License-Identifier: LGPL-3.0-or-later

package freezer

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

var testTables = map[string]TableConfig{
	"raw": {NoSnappy: true},
	"cmp": {NoSnappy: false},
}

var prunableTestTables = map[string]TableConfig{
	"raw": {NoSnappy: true, Prunable: true},
	"cmp": {NoSnappy: false, Prunable: true},
}

func newTestFreezer(t *testing.T) *Freezer {
	t.Helper()
	dir := t.TempDir()
	// Tiny per-shard size so even small tests touch the rollover path.
	f, err := NewFreezer(dir, "", false, 2049, testTables)
	if err != nil {
		t.Fatalf("NewFreezer: %v", err)
	}
	t.Cleanup(func() { _ = f.Close() })
	return f
}

func TestFreezerModifyAndRead(t *testing.T) {
	t.Parallel()
	f := newTestFreezer(t)

	var (
		rawVals [][]byte
		cmpVals [][]byte
	)
	for i := 0; i < 50; i++ {
		rawVals = append(rawVals, getChunk(256, i))
		cmpVals = append(cmpVals, []byte("compressible payload payload payload payload payload payload"))
	}

	written, err := f.ModifyAncients(func(op AncientWriteOp) error {
		for i := range rawVals {
			if err := op.AppendRaw("raw", uint64(i), rawVals[i]); err != nil {
				return err
			}
			if err := op.AppendRaw("cmp", uint64(i), cmpVals[i]); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("ModifyAncients: %v", err)
	}
	if written <= 0 {
		t.Fatalf("non-positive written bytes: %d", written)
	}

	for kind, want := range map[string][][]byte{"raw": rawVals, "cmp": cmpVals} {
		count, err := f.AncientCount(kind)
		if err != nil {
			t.Fatalf("AncientCount(%s): %v", kind, err)
		}
		if count != uint64(len(want)) {
			t.Fatalf("%s count: want %d, got %d", kind, len(want), count)
		}
		for i, exp := range want {
			got, err := f.Ancient(kind, uint64(i))
			if err != nil {
				t.Fatalf("%s[%d]: %v", kind, i, err)
			}
			if !bytes.Equal(got, exp) {
				t.Fatalf("%s[%d]: %x != %x", kind, i, got, exp)
			}
		}
		// HasAncient at head returns false.
		ok, err := f.HasAncient(kind, uint64(len(want)))
		if err != nil {
			t.Fatalf("HasAncient(%s, head): %v", kind, err)
		}
		if ok {
			t.Fatalf("HasAncient(%s, %d) at head returned true", kind, len(want))
		}
		// HasAncient at head-1 returns true.
		ok, err = f.HasAncient(kind, uint64(len(want)-1))
		if err != nil {
			t.Fatalf("HasAncient(%s, head-1): %v", kind, err)
		}
		if !ok {
			t.Fatalf("HasAncient(%s, %d) at head-1 returned false", kind, len(want)-1)
		}
	}

	// Out-of-bounds read.
	if _, err := f.Ancient("raw", uint64(len(rawVals))); !errors.Is(err, ErrOutOfBounds) {
		t.Fatalf("expected ErrOutOfBounds, got %v", err)
	}
	// Unknown table.
	if _, err := f.Ancient("nope", 0); !errors.Is(err, ErrUnknownTable) {
		t.Fatalf("expected ErrUnknownTable, got %v", err)
	}
}

// TestFreezerModifyRollback confirms ModifyAncients rolls every table back
// to its pre-call head when the callback returns an error — and that the
// rollback survives a close + reopen.
func TestFreezerModifyRollback(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	tables := map[string]TableConfig{"raw": {NoSnappy: true}}
	f, err := NewFreezer(dir, "", false, 2049, tables)
	if err != nil {
		t.Fatalf("NewFreezer: %v", err)
	}

	theErr := errors.New("intentional rollback")
	_, err = f.ModifyAncients(func(op AncientWriteOp) error {
		// Force enough writes to cross a shard boundary, then abort.
		if err := op.AppendRaw("raw", 0, make([]byte, 2048)); err != nil {
			return err
		}
		if err := op.AppendRaw("raw", 1, make([]byte, 2048)); err != nil {
			return err
		}
		if err := op.AppendRaw("raw", 2, make([]byte, 2048)); err != nil {
			return err
		}
		return theErr
	})
	if !errors.Is(err, theErr) {
		t.Fatalf("ModifyAncients returned %v, want %v", err, theErr)
	}
	if got, _ := f.AncientCount("raw"); got != 0 {
		t.Fatalf("count after rollback: want 0, got %d", got)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	// Reopening must still see zero items — rollback was durable.
	f2, err := NewFreezer(dir, "", false, 2049, tables)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	t.Cleanup(func() { _ = f2.Close() })
	if got, _ := f2.AncientCount("raw"); got != 0 {
		t.Fatalf("reopen count after rollback: want 0, got %d", got)
	}
}

// TestFreezerTruncateHead reverts a freezer to a smaller item count.
func TestFreezerTruncateHead(t *testing.T) {
	t.Parallel()
	f := newTestFreezer(t)

	const N = 64
	_, err := f.ModifyAncients(func(op AncientWriteOp) error {
		for i := uint64(0); i < N; i++ {
			if err := op.AppendRaw("raw", i, getChunk(128, int(i))); err != nil {
				return err
			}
			if err := op.AppendRaw("cmp", i, getChunk(128, int(i))); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("modify: %v", err)
	}

	old, err := f.TruncateHead(10)
	if err != nil {
		t.Fatalf("TruncateHead: %v", err)
	}
	if old != N {
		t.Fatalf("TruncateHead returned old=%d, want %d", old, N)
	}
	for _, kind := range []string{"raw", "cmp"} {
		got, _ := f.AncientCount(kind)
		if got != 10 {
			t.Fatalf("%s count after truncate: want 10, got %d", kind, got)
		}
		if _, err := f.Ancient(kind, 10); !errors.Is(err, ErrOutOfBounds) {
			t.Fatalf("read past new head on %s: %v", kind, err)
		}
		if _, err := f.Ancient(kind, 9); err != nil {
			t.Fatalf("read at new head-1 on %s: %v", kind, err)
		}
	}
}

func TestFreezerTruncateTail(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	f, err := NewFreezer(dir, "", false, 2049, prunableTestTables)
	if err != nil {
		t.Fatalf("NewFreezer: %v", err)
	}

	const N = 64
	_, err = f.ModifyAncients(func(op AncientWriteOp) error {
		for i := uint64(0); i < N; i++ {
			if err := op.AppendRaw("raw", i, getChunk(128, int(i))); err != nil {
				return err
			}
			if err := op.AppendRaw("cmp", i, getChunk(128, int(i))); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("modify: %v", err)
	}

	old, err := f.TruncateTail(10)
	if err != nil {
		t.Fatalf("TruncateTail: %v", err)
	}
	if old != 0 {
		t.Fatalf("TruncateTail returned old=%d, want 0", old)
	}
	tail, err := f.Tail()
	if err != nil || tail != 10 {
		t.Fatalf("tail after truncate = %d/%v, want 10", tail, err)
	}
	for _, kind := range []string{"raw", "cmp"} {
		got, _ := f.AncientCount(kind)
		if got != N {
			t.Fatalf("%s count after tail truncate: want %d, got %d", kind, N, got)
		}
		if ok, err := f.HasAncient(kind, 9); err != nil || ok {
			t.Fatalf("%s HasAncient(9) = %v/%v, want false", kind, ok, err)
		}
		if _, err := f.Ancient(kind, 9); !errors.Is(err, ErrOutOfBounds) {
			t.Fatalf("read before tail on %s: %v", kind, err)
		}
		if ok, err := f.HasAncient(kind, 10); err != nil || !ok {
			t.Fatalf("%s HasAncient(10) = %v/%v, want true", kind, ok, err)
		}
		if _, err := f.Ancient(kind, 10); err != nil {
			t.Fatalf("read at tail on %s: %v", kind, err)
		}
	}
	if err := f.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	reopened, err := NewFreezer(dir, "", false, 2049, prunableTestTables)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	t.Cleanup(func() { _ = reopened.Close() })
	tail, err = reopened.Tail()
	if err != nil || tail != 10 {
		t.Fatalf("reopened tail = %d/%v, want 10", tail, err)
	}
	if _, err := reopened.Ancient("raw", 9); !errors.Is(err, ErrOutOfBounds) {
		t.Fatalf("reopened read before tail: %v", err)
	}
	if _, err := reopened.Ancient("raw", 10); err != nil {
		t.Fatalf("reopened read at tail: %v", err)
	}
}

func TestFreezerStatsExposeTailBounds(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	f, err := NewFreezer(dir, "", false, 50, prunableTestTables)
	if err != nil {
		t.Fatalf("NewFreezer: %v", err)
	}
	t.Cleanup(func() { _ = f.Close() })

	const N = 10
	_, err = f.ModifyAncients(func(op AncientWriteOp) error {
		for i := uint64(0); i < N; i++ {
			if err := op.AppendRaw("raw", i, getChunk(15, int(i))); err != nil {
				return err
			}
			if err := op.AppendRaw("cmp", i, getChunk(15, int(i))); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("modify: %v", err)
	}

	stats, err := f.Stats()
	if err != nil {
		t.Fatalf("Stats before prune: %v", err)
	}
	if stats.Datadir != dir || stats.ReadOnly || stats.Head != N || stats.Tail != 0 {
		t.Fatalf("stats before prune = %+v, want datadir/head/tail", stats)
	}
	if len(stats.Tables) != 2 || stats.Tables[0].Name != "cmp" || stats.Tables[1].Name != "raw" {
		t.Fatalf("table order = %+v, want cmp/raw", stats.Tables)
	}
	if !stats.Tables[0].Prunable || stats.Tables[0].NoSnappy || !stats.Tables[1].Prunable || !stats.Tables[1].NoSnappy {
		t.Fatalf("table flags = %+v", stats.Tables)
	}
	for _, table := range stats.Tables {
		if table.Head != N || table.PhysicalTail != 0 || table.HiddenTail != 0 {
			t.Fatalf("table before prune = %+v, want head=%d tails=0", table, N)
		}
		if table.VisibleSize == 0 {
			t.Fatalf("table before prune visible size is zero: %+v", table)
		}
	}

	if _, err := f.TruncateTail(7); err != nil {
		t.Fatalf("TruncateTail: %v", err)
	}
	stats, err = f.Stats()
	if err != nil {
		t.Fatalf("Stats after truncate tail: %v", err)
	}
	if stats.Tail != 7 {
		t.Fatalf("stats tail after truncate = %d, want 7", stats.Tail)
	}
	for _, table := range stats.Tables {
		if table.PhysicalTail != 0 || table.HiddenTail != 7 {
			t.Fatalf("table after virtual tail = %+v, want physical=0 hidden=7", table)
		}
		if table.HiddenSize == 0 {
			t.Fatalf("table hidden size after virtual tail is zero: %+v", table)
		}
	}

	if removed, err := f.PruneTailFiles(); err != nil {
		t.Fatalf("PruneTailFiles: %v", err)
	} else if removed == 0 {
		t.Fatal("PruneTailFiles removed no files, want physical tail movement")
	}
	stats, err = f.Stats()
	if err != nil {
		t.Fatalf("Stats after physical prune: %v", err)
	}
	for _, table := range stats.Tables {
		if table.HiddenTail != 7 {
			t.Fatalf("table hidden tail after physical prune = %+v, want 7", table)
		}
		if table.PhysicalTail == 0 || table.PhysicalTail >= table.HiddenTail {
			t.Fatalf("table physical tail after physical prune = %+v, want 0 < physical < hidden", table)
		}
		if table.TailFile == 0 {
			t.Fatalf("table tail file after physical prune = %+v, want advanced tail file", table)
		}
	}
}

func TestFreezerPruneTailFilesDeletesCompleteTailShards(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	tables := map[string]TableConfig{"raw": {NoSnappy: true, Prunable: true}}
	f, err := NewFreezer(dir, "", false, 50, tables)
	if err != nil {
		t.Fatalf("NewFreezer: %v", err)
	}

	const N = 10
	_, err = f.ModifyAncients(func(op AncientWriteOp) error {
		for i := uint64(0); i < N; i++ {
			if err := op.AppendRaw("raw", i, getChunk(15, int(i))); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("modify: %v", err)
	}
	if err := f.Sync(); err != nil {
		t.Fatalf("sync: %v", err)
	}
	for _, file := range []string{"raw.0000.rdat", "raw.0001.rdat", "raw.0002.rdat"} {
		if _, err := os.Stat(filepath.Join(dir, file)); err != nil {
			t.Fatalf("expected %s before prune: %v", file, err)
		}
	}

	if _, err := f.TruncateTail(7); err != nil {
		t.Fatalf("TruncateTail: %v", err)
	}
	removed, err := f.PruneTailFiles()
	if err != nil {
		t.Fatalf("PruneTailFiles: %v", err)
	}
	if removed != 2 {
		t.Fatalf("PruneTailFiles removed %d files, want 2", removed)
	}
	for _, file := range []string{"raw.0000.rdat", "raw.0001.rdat"} {
		if _, err := os.Stat(filepath.Join(dir, file)); !os.IsNotExist(err) {
			t.Fatalf("%s after prune err = %v, want not exist", file, err)
		}
	}
	if _, err := os.Stat(filepath.Join(dir, "raw.0002.rdat")); err != nil {
		t.Fatalf("retained shard missing after prune: %v", err)
	}
	if _, err := f.Ancient("raw", 6); !errors.Is(err, ErrOutOfBounds) {
		t.Fatalf("read hidden item 6: %v, want ErrOutOfBounds", err)
	}
	got, err := f.Ancient("raw", 7)
	if err != nil {
		t.Fatalf("read visible item 7: %v", err)
	}
	if want := getChunk(15, 7); !bytes.Equal(got, want) {
		t.Fatalf("item 7 = %x, want %x", got, want)
	}
	if tail, err := f.Tail(); err != nil || tail != 7 {
		t.Fatalf("tail after prune = %d/%v, want 7/nil", tail, err)
	}
	if count, err := f.AncientCount("raw"); err != nil || count != N {
		t.Fatalf("count after prune = %d/%v, want %d/nil", count, err, N)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	reopened, err := NewFreezer(dir, "", false, 50, tables)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	t.Cleanup(func() { _ = reopened.Close() })
	if _, err := reopened.Ancient("raw", 6); !errors.Is(err, ErrOutOfBounds) {
		t.Fatalf("reopened read hidden item 6: %v, want ErrOutOfBounds", err)
	}
	got, err = reopened.Ancient("raw", 7)
	if err != nil {
		t.Fatalf("reopened read visible item 7: %v", err)
	}
	if want := getChunk(15, 7); !bytes.Equal(got, want) {
		t.Fatalf("reopened item 7 = %x, want %x", got, want)
	}
	if tail, err := reopened.Tail(); err != nil || tail != 7 {
		t.Fatalf("reopened tail = %d/%v, want 7/nil", tail, err)
	}
}

func TestFreezerPruneTailFilesAllHiddenTruncatesHead(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	tables := map[string]TableConfig{"raw": {NoSnappy: true, Prunable: true}}
	f, err := NewFreezer(dir, "", false, 50, tables)
	if err != nil {
		t.Fatalf("NewFreezer: %v", err)
	}

	_, err = f.ModifyAncients(func(op AncientWriteOp) error {
		for i := uint64(0); i < 3; i++ {
			if err := op.AppendRaw("raw", i, getChunk(15, int(i))); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("modify: %v", err)
	}
	if err := f.Sync(); err != nil {
		t.Fatalf("sync: %v", err)
	}
	dataPath := filepath.Join(dir, "raw.0000.rdat")
	stat, err := os.Stat(dataPath)
	if err != nil {
		t.Fatalf("head shard before prune stat: %v", err)
	}
	if stat.Size() != 45 {
		t.Fatalf("head shard before prune size = %d, want 45", stat.Size())
	}
	if _, err := f.TruncateTail(3); err != nil {
		t.Fatalf("TruncateTail: %v", err)
	}
	removed, err := f.PruneTailFiles()
	if err != nil {
		t.Fatalf("PruneTailFiles: %v", err)
	}
	if removed != 0 {
		t.Fatalf("PruneTailFiles removed %d files, want 0", removed)
	}
	stat, err = os.Stat(dataPath)
	if err != nil {
		t.Fatalf("head shard after prune stat: %v", err)
	}
	if stat.Size() != 0 {
		t.Fatalf("head shard after prune size = %d, want 0", stat.Size())
	}
	if _, err := f.Ancient("raw", 2); !errors.Is(err, ErrOutOfBounds) {
		t.Fatalf("read hidden item 2: %v, want ErrOutOfBounds", err)
	}
	if _, err := f.ModifyAncients(func(op AncientWriteOp) error {
		return op.AppendRaw("raw", 3, getChunk(15, 3))
	}); err != nil {
		t.Fatalf("append after full-tail prune: %v", err)
	}
	got, err := f.Ancient("raw", 3)
	if err != nil {
		t.Fatalf("read appended item 3: %v", err)
	}
	if want := getChunk(15, 3); !bytes.Equal(got, want) {
		t.Fatalf("item 3 = %x, want %x", got, want)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	reopened, err := NewFreezer(dir, "", false, 50, tables)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	t.Cleanup(func() { _ = reopened.Close() })
	if tail, err := reopened.Tail(); err != nil || tail != 3 {
		t.Fatalf("reopened tail = %d/%v, want 3/nil", tail, err)
	}
	if count, err := reopened.AncientCount("raw"); err != nil || count != 4 {
		t.Fatalf("reopened count = %d/%v, want 4/nil", count, err)
	}
	if _, err := reopened.Ancient("raw", 2); !errors.Is(err, ErrOutOfBounds) {
		t.Fatalf("reopened read hidden item 2: %v, want ErrOutOfBounds", err)
	}
	got, err = reopened.Ancient("raw", 3)
	if err != nil {
		t.Fatalf("reopened read appended item 3: %v", err)
	}
	if want := getChunk(15, 3); !bytes.Equal(got, want) {
		t.Fatalf("reopened item 3 = %x, want %x", got, want)
	}
}

func TestFreezerTruncateTailRejectsNonPrunableTables(t *testing.T) {
	t.Parallel()
	f := newTestFreezer(t)

	_, err := f.ModifyAncients(func(op AncientWriteOp) error {
		if err := op.AppendRaw("raw", 0, getChunk(128, 0)); err != nil {
			return err
		}
		return op.AppendRaw("cmp", 0, getChunk(128, 0))
	})
	if err != nil {
		t.Fatalf("modify: %v", err)
	}
	if _, err := f.TruncateTail(1); err == nil || !strings.Contains(err.Error(), "not prunable") {
		t.Fatalf("TruncateTail on non-prunable tables err = %v, want not prunable", err)
	}
}

func TestFreezerTruncateTailPreflightsAllTables(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	tables := map[string]TableConfig{
		"raw": {NoSnappy: true, Prunable: true},
		"cmp": {NoSnappy: true},
	}
	f, err := NewFreezer(dir, "", false, 2049, tables)
	if err != nil {
		t.Fatalf("NewFreezer: %v", err)
	}
	t.Cleanup(func() { _ = f.Close() })

	_, err = f.ModifyAncients(func(op AncientWriteOp) error {
		if err := op.AppendRaw("raw", 0, getChunk(128, 0)); err != nil {
			return err
		}
		return op.AppendRaw("cmp", 0, getChunk(128, 0))
	})
	if err != nil {
		t.Fatalf("modify: %v", err)
	}
	if _, err := f.TruncateTail(1); err == nil || !strings.Contains(err.Error(), "not prunable") {
		t.Fatalf("TruncateTail on mixed tables err = %v, want not prunable", err)
	}
	if tail, err := f.Tail(); err != nil || tail != 0 {
		t.Fatalf("tail after failed mixed truncate = %d/%v, want 0", tail, err)
	}
	if ok, err := f.HasAncient("raw", 0); err != nil || !ok {
		t.Fatalf("raw HasAncient(0) after failed mixed truncate = %v/%v, want true", ok, err)
	}
}

func TestFreezerOpenRepairsTableCardinalityMismatch(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	rawOnly := map[string]TableConfig{"raw": {NoSnappy: true}}
	allTables := map[string]TableConfig{"raw": {NoSnappy: true}, "cmp": {NoSnappy: true}}

	empty, err := NewFreezer(dir, "", false, 2049, allTables)
	if err != nil {
		t.Fatalf("NewFreezer empty all-tables: %v", err)
	}
	if err := empty.Close(); err != nil {
		t.Fatalf("close empty all-tables: %v", err)
	}

	f, err := NewFreezer(dir, "", false, 2049, rawOnly)
	if err != nil {
		t.Fatalf("NewFreezer raw-only: %v", err)
	}
	if _, err := f.ModifyAncients(func(op AncientWriteOp) error {
		for i := uint64(0); i < 5; i++ {
			if err := op.AppendRaw("raw", i, getChunk(32, int(i))); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatalf("modify raw-only: %v", err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("close raw-only: %v", err)
	}

	reopened, err := NewFreezer(dir, "", false, 2049, allTables)
	if err != nil {
		t.Fatalf("reopen with all tables: %v", err)
	}
	t.Cleanup(func() { _ = reopened.Close() })
	stats, err := reopened.Stats()
	if err != nil {
		t.Fatalf("Stats after repair: %v", err)
	}
	if !stats.Repair.Applied || stats.Repair.TargetHead != 0 || stats.Repair.TargetTail != 0 || len(stats.Repair.Tables) != 1 {
		t.Fatalf("repair stats = %+v, want one table repaired to head/tail 0", stats.Repair)
	}
	repair := stats.Repair.Tables[0]
	if repair.Name != "raw" || repair.HeadBefore != 5 || repair.HeadAfter != 0 || repair.HiddenTailBefore != 0 || repair.HiddenTailAfter != 0 {
		t.Fatalf("repair table = %+v, want raw head 5->0 tail 0->0", repair)
	}
	for _, kind := range []string{"raw", "cmp"} {
		got, err := reopened.AncientCount(kind)
		if err != nil {
			t.Fatalf("AncientCount(%s): %v", kind, err)
		}
		if got != 0 {
			t.Fatalf("%s count after repair = %d, want 0", kind, got)
		}
	}
}

func TestFreezerReadonlyRejectsTableCardinalityMismatch(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	rawOnly := map[string]TableConfig{"raw": {NoSnappy: true}}
	allTables := map[string]TableConfig{"raw": {NoSnappy: true}, "cmp": {NoSnappy: true}}

	empty, err := NewFreezer(dir, "", false, 2049, allTables)
	if err != nil {
		t.Fatalf("NewFreezer empty all-tables: %v", err)
	}
	if err := empty.Close(); err != nil {
		t.Fatalf("close empty all-tables: %v", err)
	}

	f, err := NewFreezer(dir, "", false, 2049, rawOnly)
	if err != nil {
		t.Fatalf("NewFreezer raw-only: %v", err)
	}
	if _, err := f.ModifyAncients(func(op AncientWriteOp) error {
		for i := uint64(0); i < 3; i++ {
			if err := op.AppendRaw("raw", i, getChunk(32, int(i))); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatalf("modify raw-only: %v", err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("close raw-only: %v", err)
	}

	if _, err := NewFreezer(dir, "", true, 2049, allTables); err == nil {
		t.Fatal("readonly reopen with mismatched tables succeeded, want error")
	} else if !strings.Contains(err.Error(), "differing head") {
		t.Fatalf("readonly mismatch error = %v, want differing head", err)
	}
}

// TestFreezerRange exercises AncientRange across multiple shards.
func TestFreezerRange(t *testing.T) {
	t.Parallel()
	f := newTestFreezer(t)

	const N = 100
	_, err := f.ModifyAncients(func(op AncientWriteOp) error {
		for i := uint64(0); i < N; i++ {
			if err := op.AppendRaw("raw", i, getChunk(64, int(i))); err != nil {
				return err
			}
			if err := op.AppendRaw("cmp", i, getChunk(64, int(i))); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("modify: %v", err)
	}

	for _, kind := range []string{"raw", "cmp"} {
		got, err := f.AncientRange(kind, 10, 20, 0)
		if err != nil {
			t.Fatalf("AncientRange(%s): %v", kind, err)
		}
		if len(got) != 20 {
			t.Fatalf("%s range len: want 20, got %d", kind, len(got))
		}
		for i, blob := range got {
			want := getChunk(64, 10+i)
			if !bytes.Equal(blob, want) {
				t.Fatalf("%s range[%d]: %x != %x", kind, i, blob, want)
			}
		}
	}
}
