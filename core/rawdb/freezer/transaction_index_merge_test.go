package freezer

import (
	"encoding/binary"
	"fmt"
	"path/filepath"
	"testing"
)

func TestCompactTransactionIndexTailGeometricMerge(t *testing.T) {
	dir := t.TempDir()
	type expectedEntry struct {
		hash     [32]byte
		location uint64
	}
	var expected []expectedEntry
	for segment := uint64(0); segment < 4; segment++ {
		start := segment * 10
		end := start + 10
		entries := make([]TransactionIndexEntry, 0, 10)
		for block := start; block < end; block++ {
			var hash [32]byte
			binary.BigEndian.PutUint64(hash[:8], block*17+1)
			binary.BigEndian.PutUint64(hash[24:], block+1000)
			entries = append(entries, TransactionIndexEntry{Hash: hash, Location: block})
			expected = append(expected, expectedEntry{hash: hash, location: block})
		}
		path := TransactionIndexRunPath(dir, start, end)
		result, err := BuildTransactionIndexRun(path, TransactionIndexBuildOptions{
			PrefixBits: 8,
			StartBlock: start,
			EndBlock:   end,
			Iterate:    transactionIndexTestIterator(entries),
		})
		if err != nil {
			t.Fatalf("build segment %d: %v", segment, err)
		}
		if err := PublishTransactionIndexRun(dir, result); err != nil {
			t.Fatalf("publish segment %d: %v", segment, err)
		}
		for {
			_, _, merged, err := CompactTransactionIndexTail(dir)
			if err != nil {
				t.Fatalf("merge after segment %d: %v", segment, err)
			}
			if !merged {
				break
			}
		}
	}

	store, err := OpenTransactionIndexStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	if store.Coverage() != 40 || len(store.runs) != 1 {
		t.Fatalf("merged store coverage=%d runs=%d, want 40/1", store.Coverage(), len(store.runs))
	}
	if got := filepath.Base(store.runs[0].Path()); got != "00000000000000000000-00000000000000000040.gtxi" {
		t.Fatalf("merged filename=%q", got)
	}
	for _, want := range expected {
		locations, err := store.Candidates(want.hash)
		if err != nil {
			t.Fatal(err)
		}
		if len(locations) != 1 || locations[0] != want.location {
			t.Fatalf("candidate %x=%v, want [%d]", want.hash[:8], locations, want.location)
		}
	}
}

func TestMergeTransactionIndexRunsPreservesFingerprintCollisions(t *testing.T) {
	dir := t.TempDir()
	makeHash := func(tail byte) [32]byte {
		var hash [32]byte
		copy(hash[:16], []byte("same-prefix-fp!!"))
		hash[31] = tail
		return hash
	}
	leftEntry := TransactionIndexEntry{Hash: makeHash(1), Location: 1}
	rightEntry := TransactionIndexEntry{Hash: makeHash(2), Location: 2}
	for i, run := range []struct {
		start, end uint64
		entry      TransactionIndexEntry
	}{{0, 2, leftEntry}, {2, 4, rightEntry}} {
		path := TransactionIndexRunPath(dir, run.start, run.end)
		result, err := BuildTransactionIndexRun(path, TransactionIndexBuildOptions{
			PrefixBits: 8,
			StartBlock: run.start,
			EndBlock:   run.end,
			Iterate:    transactionIndexTestIterator([]TransactionIndexEntry{run.entry}),
		})
		if err != nil {
			t.Fatalf("build %d: %v", i, err)
		}
		if err := PublishTransactionIndexRun(dir, result); err != nil {
			t.Fatalf("publish %d: %v", i, err)
		}
	}
	if _, _, merged, err := CompactTransactionIndexTail(dir); err != nil || !merged {
		t.Fatalf("merge=%v err=%v", merged, err)
	}
	store, err := OpenTransactionIndexStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	for i, entry := range []TransactionIndexEntry{leftEntry, rightEntry} {
		locations, err := store.Candidates(entry.Hash)
		if err != nil {
			t.Fatal(err)
		}
		if len(locations) != 2 {
			t.Fatalf("collision candidate %d=%v, want both locations", i, locations)
		}
	}
	if err := store.runs[0].Verify(); err != nil {
		t.Fatal(fmt.Errorf("verify merged collision run: %w", err))
	}
}
