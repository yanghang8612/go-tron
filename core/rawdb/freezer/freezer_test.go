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
	"testing"
)

var testTables = map[string]TableConfig{
	"raw": {NoSnappy: true},
	"cmp": {NoSnappy: false},
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
