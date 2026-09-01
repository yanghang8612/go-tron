package freezer

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"
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

func TestTransactionIndexRunGenerationAdmitsBalancedPeers(t *testing.T) {
	for _, tc := range []struct {
		left, right transactionIndexManifestRun
		want        bool
	}{
		{
			left:  transactionIndexManifestRun{StartBlock: 0, EndBlock: 4_097, CompactionLevelSet: true},
			right: transactionIndexManifestRun{StartBlock: 4_097, EndBlock: 8_193, CompactionLevelSet: true},
			want:  true,
		},
		{
			left:  transactionIndexManifestRun{StartBlock: 0, EndBlock: 8_193, CompactionLevel: 1, CompactionLevelSet: true},
			right: transactionIndexManifestRun{StartBlock: 8_193, EndBlock: 12_290, CompactionLevelSet: true},
			want:  false,
		},
		{
			left:  transactionIndexManifestRun{StartBlock: 0, EndBlock: 5_001},
			right: transactionIndexManifestRun{StartBlock: 5_001, EndBlock: 10_001},
			want:  true,
		},
		{
			left:  transactionIndexManifestRun{StartBlock: 0, EndBlock: 8_192},
			right: transactionIndexManifestRun{StartBlock: 8_192, EndBlock: 12_288, CompactionLevelSet: true},
			want:  false,
		},
	} {
		if got := transactionIndexRunsCanMerge(tc.left, tc.right); got != tc.want {
			t.Fatalf("merge eligibility left=%+v right=%+v got=%t, want %t", tc.left, tc.right, got, tc.want)
		}
	}
}

func TestTransactionIndexBaseCompactionLevelMatchesMaintenanceLeaves(t *testing.T) {
	for _, tc := range []struct {
		blocks uint64
		want   uint32
	}{
		{blocks: 1, want: 0},
		{blocks: 8_192, want: 0},
		{blocks: 8_193, want: 1},
		{blocks: 65_536, want: 3},
		{blocks: 1_000_003, want: 7},
	} {
		if got := transactionIndexBaseCompactionLevel(tc.blocks); got != tc.want {
			t.Fatalf("base compaction level blocks=%d got=%d, want %d", tc.blocks, got, tc.want)
		}
	}
}

func TestCompactTransactionIndexTailMergesPrimeSegmentBalancedChunks(t *testing.T) {
	dir := t.TempDir()
	start := uint64(0)
	for leaf := 0; leaf < 8; leaf++ {
		blocks := uint64(4_096)
		if leaf%2 == 0 {
			blocks++
		}
		end := start + blocks
		var hash [32]byte
		binary.BigEndian.PutUint64(hash[24:], uint64(leaf+1))
		result, err := BuildTransactionIndexRun(TransactionIndexRunPath(dir, start, end), TransactionIndexBuildOptions{
			PrefixBits: 8,
			StartBlock: start,
			EndBlock:   end,
			Iterate: transactionIndexTestIterator([]TransactionIndexEntry{{
				Hash: hash, Location: start,
			}}),
		})
		if err != nil {
			t.Fatalf("build balanced leaf %d: %v", leaf, err)
		}
		if err := PublishTransactionIndexRun(dir, result); err != nil {
			t.Fatalf("publish balanced leaf %d: %v", leaf, err)
		}
		for {
			_, _, merged, err := CompactTransactionIndexTail(dir)
			if err != nil {
				t.Fatalf("compact balanced leaf %d: %v", leaf, err)
			}
			if !merged {
				break
			}
		}
		start = end
	}
	store, err := OpenTransactionIndexStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	if len(store.runs) != 1 || store.Coverage() != 4*8_193 {
		t.Fatalf("balanced prime compaction runs=%d coverage=%d, want 1/%d", len(store.runs), store.Coverage(), 4*8_193)
	}
}

func TestCompactTransactionIndexTailContextCancelKeepsManifest(t *testing.T) {
	dir := t.TempDir()
	const rowsPerRun = 50_000
	for segment := uint64(0); segment < 2; segment++ {
		entries := make([]TransactionIndexEntry, rowsPerRun)
		for i := range entries {
			binary.BigEndian.PutUint64(entries[i].Hash[:8], uint64(i)+1)
			entries[i].Location = segment * 10
		}
		result, err := BuildTransactionIndexRun(TransactionIndexRunPath(dir, segment*10, segment*10+10), TransactionIndexBuildOptions{
			PrefixBits: 8,
			StartBlock: segment * 10,
			EndBlock:   segment*10 + 10,
			Iterate:    transactionIndexTestIterator(entries),
		})
		if err != nil {
			t.Fatalf("build run %d: %v", segment, err)
		}
		if err := PublishTransactionIndexRun(dir, result); err != nil {
			t.Fatalf("publish run %d: %v", segment, err)
		}
	}
	manifestPath := filepath.Join(dir, transactionIndexDirectoryName, transactionIndexManifestName)
	before, err := os.ReadFile(manifestPath)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		_, _, merged, err := CompactTransactionIndexTailContext(ctx, dir)
		if merged && err == nil {
			err = errors.New("merge unexpectedly published before cancellation")
		}
		done <- err
	}()
	runsDir := filepath.Join(dir, transactionIndexDirectoryName, transactionIndexRunsDirectory)
	deadline := time.After(2 * time.Second)
	for {
		matches, err := filepath.Glob(filepath.Join(runsDir, ".tx-index-*.tmp"))
		if err != nil {
			t.Fatal(err)
		}
		if len(matches) > 0 {
			cancel()
			break
		}
		select {
		case err := <-done:
			t.Fatalf("merge completed before cancellation hook: %v", err)
		case <-deadline:
			t.Fatal("timed out waiting for merge build")
		case <-time.After(time.Millisecond):
		}
	}
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled merge error=%v, want context cancellation", err)
	}
	after, err := os.ReadFile(manifestPath)
	if err != nil {
		t.Fatal(err)
	}
	if string(after) != string(before) {
		t.Fatal("canceled merge changed transaction-index manifest")
	}
	if _, err := os.Stat(TransactionIndexRunPath(dir, 0, 20)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("canceled merge published merged run: %v", err)
	}
}

func TestVerifyTransactionIndexBuildResultContextPreservesLegacyWrapper(t *testing.T) {
	dir := t.TempDir()
	result, err := BuildTransactionIndexRun(TransactionIndexRunPath(dir, 0, 1), TransactionIndexBuildOptions{
		PrefixBits: 8,
		StartBlock: 0,
		EndBlock:   1,
		Iterate: transactionIndexTestIterator([]TransactionIndexEntry{{
			Hash: [32]byte{1}, Location: 0,
		}}),
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := verifyTransactionIndexBuildResultContext(ctx, dir, result); !errors.Is(err, context.Canceled) {
		t.Fatalf("context verification error=%v, want cancellation", err)
	}
	if err := verifyTransactionIndexBuildResult(dir, result); err != nil {
		t.Fatalf("legacy background verification: %v", err)
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
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := cleanupUnreferencedTransactionIndexRunsContext(canceled, dir, selected); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled orphan cleanup error=%v, want context cancellation", err)
	}
	for _, path := range obsolete {
		if _, err := os.Stat(path); err != nil {
			t.Fatalf("canceled cleanup removed %q: %v", path, err)
		}
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
