package freezer

import (
	"encoding/binary"
	"fmt"
	"os"
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

func TestCompactTransactionIndexTailRepairsDeferredBacklog(t *testing.T) {
	dir := t.TempDir()
	for segment := uint64(0); segment < 4; segment++ {
		start, end := segment*10, segment*10+10
		var hash [32]byte
		binary.BigEndian.PutUint64(hash[:8], segment+1)
		result, err := BuildTransactionIndexRun(TransactionIndexRunPath(dir, start, end), TransactionIndexBuildOptions{
			PrefixBits: 8,
			StartBlock: start,
			EndBlock:   end,
			Iterate: transactionIndexTestIterator([]TransactionIndexEntry{{
				Hash: hash, Location: start,
			}}),
		})
		if err != nil {
			t.Fatal(err)
		}
		if err := PublishTransactionIndexRun(dir, result); err != nil {
			t.Fatal(err)
		}
	}
	merges := 0
	for {
		_, _, merged, err := CompactTransactionIndexTail(dir)
		if err != nil {
			t.Fatal(err)
		}
		if !merged {
			break
		}
		merges++
	}
	store, err := OpenTransactionIndexStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	if merges != 3 || len(store.runs) != 1 || store.Coverage() != 40 {
		t.Fatalf("deferred compaction merges=%d runs=%d coverage=%d, want 3/1/40", merges, len(store.runs), store.Coverage())
	}
}

func TestCompactTransactionIndexTailMergesDifferentPrefixWidths(t *testing.T) {
	dir := t.TempDir()
	entries := make([]TransactionIndexEntry, 0, 64)
	for segment, prefixBits := range []uint32{8, 12} {
		start, end := uint64(segment*10), uint64(segment*10+10)
		var runEntries []TransactionIndexEntry
		for i := 0; i < 32; i++ {
			var hash [32]byte
			binary.BigEndian.PutUint64(hash[:8], uint64(segment*10_000+i*17+1))
			binary.BigEndian.PutUint64(hash[8:16], uint64(i)*0x9e3779b97f4a7c15)
			entry := TransactionIndexEntry{Hash: hash, Location: start + uint64(i%10)}
			runEntries = append(runEntries, entry)
			entries = append(entries, entry)
		}
		path := TransactionIndexRunPath(dir, start, end)
		result, err := BuildTransactionIndexRun(path, TransactionIndexBuildOptions{
			PrefixBits: prefixBits,
			StartBlock: start,
			EndBlock:   end,
			Iterate:    transactionIndexTestIterator(runEntries),
		})
		if err != nil {
			t.Fatal(err)
		}
		if err := PublishTransactionIndexRun(dir, result); err != nil {
			t.Fatal(err)
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
	if len(store.runs) != 1 || store.runs[0].PrefixBits() != 8 {
		t.Fatalf("merged runs=%d prefix=%d, want 1/8", len(store.runs), store.runs[0].PrefixBits())
	}
	for _, entry := range entries {
		locations, err := store.Candidates(entry.Hash)
		if err != nil {
			t.Fatal(err)
		}
		if len(locations) != 1 || locations[0] != entry.Location {
			t.Fatalf("candidate %x = %v, want [%d]", entry.Hash[:8], locations, entry.Location)
		}
	}
}

func TestTransactionIndexOrphanCleanupRecoversPublishedMergeCrash(t *testing.T) {
	dir := t.TempDir()
	var obsolete []string
	for segment := uint64(0); segment < 2; segment++ {
		start, end := segment*10, segment*10+10
		path := TransactionIndexRunPath(dir, start, end)
		result, err := BuildTransactionIndexRun(path, TransactionIndexBuildOptions{
			PrefixBits: 8,
			StartBlock: start,
			EndBlock:   end,
			Iterate: transactionIndexTestIterator([]TransactionIndexEntry{{
				Hash:     [32]byte{byte(segment + 1)},
				Location: start,
			}}),
		})
		if err != nil {
			t.Fatalf("build segment %d: %v", segment, err)
		}
		if err := PublishTransactionIndexRun(dir, result); err != nil {
			t.Fatalf("publish segment %d: %v", segment, err)
		}
		obsolete = append(obsolete, path)
	}
	stale, err := OpenTransactionIndexStore(dir)
	if err != nil {
		t.Fatalf("open pre-merge store: %v", err)
	}
	defer stale.Close()
	if _, returnedObsolete, merged, err := CompactTransactionIndexTail(dir); err != nil || !merged {
		t.Fatalf("publish merged manifest: merged=%v err=%v", merged, err)
	} else if len(returnedObsolete) != len(obsolete) {
		t.Fatalf("obsolete paths=%v, want %v", returnedObsolete, obsolete)
	}
	if _, err := cleanupUnreferencedTransactionIndexRuns(dir, stale); err == nil {
		t.Fatal("orphan cleanup accepted a pre-merge selected store")
	}
	// Model an abrupt exit after manifest publication: the caller never gets a
	// chance to unlink returnedObsolete. Reopening selects only the merged run.
	for _, path := range obsolete {
		if _, err := os.Stat(path); err != nil {
			t.Fatalf("crash orphan %q missing before recovery: %v", path, err)
		}
	}
	selected, err := OpenTransactionIndexStore(dir)
	if err != nil {
		t.Fatalf("reopen selected store: %v", err)
	}
	defer selected.Close()
	if selected.Coverage() != 20 || len(selected.runs) != 1 {
		t.Fatalf("selected coverage/runs=%d/%d, want 20/1", selected.Coverage(), len(selected.runs))
	}

	// A complete run beyond manifest coverage may be an interrupted future
	// build. It must survive cleanup so a later publication can recover it.
	futurePath := TransactionIndexRunPath(dir, 20, 30)
	if _, err := BuildTransactionIndexRun(futurePath, TransactionIndexBuildOptions{
		PrefixBits: 8,
		StartBlock: 20,
		EndBlock:   30,
		Iterate: transactionIndexTestIterator([]TransactionIndexEntry{{
			Hash:     [32]byte{3},
			Location: 20,
		}}),
	}); err != nil {
		t.Fatalf("build future recovery run: %v", err)
	}
	unknownPath := filepath.Join(filepath.Dir(futurePath), "operator-note.gtxi")
	if err := os.WriteFile(unknownPath, []byte("preserve"), 0o600); err != nil {
		t.Fatal(err)
	}

	cleanup, err := cleanupUnreferencedTransactionIndexRuns(dir, selected)
	if err != nil {
		t.Fatalf("clean crash orphans: %v", err)
	}
	if cleanup.Files != 2 || cleanup.Bytes == 0 {
		t.Fatalf("cleanup=%+v, want two non-empty obsolete runs", cleanup)
	}
	for _, path := range obsolete {
		if _, err := os.Stat(path); !os.IsNotExist(err) {
			t.Fatalf("obsolete run %q survived cleanup: %v", path, err)
		}
	}
	for _, path := range []string{selected.runs[0].Path(), futurePath, unknownPath} {
		if _, err := os.Stat(path); err != nil {
			t.Fatalf("protected run %q removed: %v", path, err)
		}
	}
}

func TestParseTransactionIndexRunFilenameStrict(t *testing.T) {
	valid := "00000000000000000001-00000000000000000002.gtxi"
	if start, end, ok := parseTransactionIndexRunFilename(valid); !ok || start != 1 || end != 2 {
		t.Fatalf("parse valid run=%d/%d/%v", start, end, ok)
	}
	for _, name := range []string{
		"1-2.gtxi",
		"00000000000000000002-00000000000000000001.gtxi",
		"00000000000000000001-00000000000000000002.gtxi.bak",
		"0000000000000000000x-00000000000000000002.gtxi",
	} {
		if _, _, ok := parseTransactionIndexRunFilename(name); ok {
			t.Fatalf("accepted non-canonical run filename %q", name)
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

func TestMergeTransactionIndexRunsPreservesCollisionsAcrossPrefixWidths(t *testing.T) {
	dir := t.TempDir()
	makeHash := func(tail byte) [32]byte {
		var hash [32]byte
		copy(hash[:16], []byte("same-route-bits!"))
		hash[31] = tail
		return hash
	}
	entries := []TransactionIndexEntry{
		{Hash: makeHash(1), Location: 1},
		{Hash: makeHash(2), Location: 2},
	}
	for i, prefixBits := range []uint32{8, 12} {
		start := uint64(i * 2)
		result, err := BuildTransactionIndexRun(TransactionIndexRunPath(dir, start, start+2), TransactionIndexBuildOptions{
			PrefixBits: prefixBits,
			StartBlock: start,
			EndBlock:   start + 2,
			Iterate:    transactionIndexTestIterator(entries[i : i+1]),
		})
		if err != nil {
			t.Fatal(err)
		}
		if err := PublishTransactionIndexRun(dir, result); err != nil {
			t.Fatal(err)
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
	for _, entry := range entries {
		locations, err := store.Candidates(entry.Hash)
		if err != nil {
			t.Fatal(err)
		}
		if len(locations) != 2 {
			t.Fatalf("collision candidates for %x = %v, want both locations", entry.Hash[:16], locations)
		}
	}
}
