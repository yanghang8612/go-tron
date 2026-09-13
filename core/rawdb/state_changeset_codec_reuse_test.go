package rawdb

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"reflect"
	"testing"
	"unsafe"

	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/tronprotocol/go-tron/internal/historychunk"
)

func TestStateHistoryChunkWorkMatchesIndependentCodec(t *testing.T) {
	for _, workers := range []int{1, 2, 4} {
		for _, size := range []int{historychunk.MinSize - 1, historychunk.MaxSize, 512 << 10} {
			rows := chunkHistoryRows(size, 3)
			raw := encodeBorrowedStateDomainChangeTestBlock(t, rows)
			independent, useful := encodeStateChangeChunksWithWorkers(raw, workers)
			var work stateChangeChunkWork
			got, gotUseful := encodeStateChangeChunksWithChunkWork(raw, workers, &work)
			if useful != gotUseful || !bytes.Equal(got, independent) || !work.matches(raw) {
				t.Fatalf("size=%d workers=%d codec or layout changed", size, workers)
			}
			start := 0
			for i, end := range work.cuts {
				if work.hashes[i] != sha256.Sum256(raw[start:end]) {
					t.Fatalf("hash differs at chunk %d", i)
				}
				start = end
			}
			if work.matches(bytes.Clone(raw)) || work.matches(raw[:len(raw)-1]) {
				t.Fatal("work accepted a different raw allocation or length")
			}
			// The existing splitter grows only its bounded offset vector. No
			// chunk values are retained by the additional hash metadata.
			maxEntries := len(raw)/historychunk.MinSize + 1
			bytesUsed := cap(work.cuts)*int(unsafe.Sizeof(int(0))) + cap(work.hashes)*32
			if len(work.hashes) > maxEntries || bytesUsed > maxEntries*64+1024 {
				t.Fatalf("metadata not bounded by decoded input: bytes=%d entries=%d", bytesUsed, maxEntries)
			}
		}
	}
}

func TestStateHistoryChunkWorkGateAndMalformedLayout(t *testing.T) {
	rows := chunkHistoryRows(512<<10, 3)
	raw := encodeBorrowedStateDomainChangeTestBlock(t, rows)
	var work stateChangeChunkWork
	encodeStateDomainChangeBlockStorageWithChunkWork(raw, rows, true, &work)
	if !work.matches(raw) {
		t.Fatal("qualifying v2 encoder did not supply reuse metadata")
	}
	for _, mutate := range []func(*stateChangeChunkWork){
		func(w *stateChangeChunkWork) { w.cuts[0] = 0 },
		func(w *stateChangeChunkWork) { w.cuts[0] = historychunk.MinSize - 1 },
		func(w *stateChangeChunkWork) { w.cuts[0] = historychunk.MaxSize + 1 },
		func(w *stateChangeChunkWork) { w.cuts[1] = w.cuts[0] },
		func(w *stateChangeChunkWork) { w.cuts[len(w.cuts)-1]-- },
		func(w *stateChangeChunkWork) { w.cuts[len(w.cuts)-1]++ },
		func(w *stateChangeChunkWork) { w.hashes = w.hashes[:len(w.hashes)-1] },
	} {
		bad := work
		bad.cuts = append([]int(nil), work.cuts...)
		mutate(&bad)
		if bad.matches(raw) {
			t.Fatal("invalid reuse layout accepted")
		}
	}
	for _, enabled := range []bool{false, true} {
		// Disabled CDC, or a single large Prev, cannot provide a v2 plan.
		// Even a caller reusing this variable cannot carry prior metadata.
		input := rows
		if enabled {
			input = rows[:1]
		}
		got, compressed := encodeStateDomainChangeBlockStorageWithChunkWork(raw, input, enabled, &work)
		want, wantCompressed := encodeStateDomainChangeBlockStorageWithDedup(raw, input, enabled)
		if !bytes.Equal(got, want) || compressed != wantCompressed || work.rawStart != nil || work.cuts != nil || work.hashes != nil {
			t.Fatal("old codec gate changed or stale metadata retained")
		}
	}
	// v2 can fail its savings policy while its valid chunk metadata remains
	// useful to the independent cross-block policy.
	random := chunkHistoryRows(512<<10, 1)[0].Prev
	if _, useful := encodeStateChangeChunksWithChunkWork(random, 2, &work); useful || !work.matches(random) {
		t.Fatal("non-saving v2 encoding lost its valid reuse metadata")
	}
}

func stateHistoryChunkWorkTestPlan(t *testing.T, db ethdb.KeyValueStore, rows []*StateDomainChange, reuse bool, setup string) (*sharedHistoryTestBatch, []byte, *stateHistorySharedWriteStats, error) {
	t.Helper()
	raw := encodeBorrowedStateDomainChangeTestBlock(t, rows)
	var work stateChangeChunkWork
	var request *stateChangeChunkWork
	if reuse {
		request = &work
	}
	baseline, _ := encodeStateDomainChangeBlockStorageWithChunkWork(raw, rows, true, request)
	if reuse && !work.matches(raw) {
		t.Fatal("reuse fixture did not execute v2 and supply same-RLP metadata")
	}
	scope := newSharedHistoryTestBatch(db)
	var writer ethdb.KeyValueWriter = scope
	if setup == "chunk_put" {
		scope.failAt = 1
	}
	if setup == "metadata_put" || setup == "pack_put" {
		key := stateHistoryChunkBucketKey(stateHistoryChunkBucket(rows[0].BlockNum))
		if setup == "pack_put" {
			key = stateChangeSetKey(rows[0].BlockNum, 0)
		}
		writer = &sharedHistoryFaultScope{sharedHistoryTestBatch: scope, failKey: key, failure: errors.New(setup)}
	}
	if setup == "cost_fallback" {
		baseline = []byte{0} // Deliberately unaffordable seed; no writes allowed.
	}
	pack, stats, err := planAndWriteSharedStateHistoryWithChunkWork(writer, rows[0].BlockNum, raw, baseline, rows, true, request)
	if err == nil && setup != "cost_fallback" {
		err = writer.Put(stateChangeSetKey(rows[0].BlockNum, 0), pack)
	}
	return scope, pack, stats, err
}

func TestStateHistoryChunkWorkPublicationAndFailuresMatchIndependentPlanning(t *testing.T) {
	for _, setup := range []string{"seed", "existing", "bucket_boundary", "retired", "invalid_meta", "corrupt_chunk", "cost_fallback", "chunk_put", "metadata_put", "pack_put"} {
		t.Run(setup, func(t *testing.T) {
			rows := chunkHistoryRows(512<<10, 3)
			for _, row := range rows {
				row.BlockNum = 1023
			}
			var priorScope *sharedHistoryTestBatch
			var priorPack []byte
			var priorStats *stateHistorySharedWriteStats
			var priorErr error
			for _, reuse := range []bool{false, true} {
				db := sharedHistoryTestDB(t)
				if setup == "existing" || setup == "bucket_boundary" || setup == "corrupt_chunk" {
					seed, _, _, err := stateHistoryChunkWorkTestPlan(t, db, rows, false, "seed")
					if err != nil {
						t.Fatal(err)
					}
					if err := seed.batch.Write(); err != nil {
						t.Fatal(err)
					}
				}
				if setup == "retired" || setup == "invalid_meta" {
					value := []byte{1, 1}
					if setup == "invalid_meta" {
						value = []byte{2, 0}
					}
					if err := db.Put(stateHistoryChunkBucketKey(0), value); err != nil {
						t.Fatal(err)
					}
				}
				if setup == "corrupt_chunk" {
					it := db.NewIterator(stateHistoryChunkBucketPrefix(0), nil)
					if !it.Next() {
						it.Release()
						t.Fatal("missing seed chunk")
					}
					key, value := bytes.Clone(it.Key()), bytes.Clone(it.Value())
					it.Release()
					value[len(value)-1] ^= 1
					if err := db.Put(key, value); err != nil {
						t.Fatal(err)
					}
				}
				if setup == "bucket_boundary" {
					for _, row := range rows {
						row.BlockNum = 1024
					}
				}
				scope, pack, stats, err := stateHistoryChunkWorkTestPlan(t, db, rows, reuse, setup)
				if stats != nil {
					stats.work = 0 // Planning wall time is the only intentionally changed field.
				}
				if reuse {
					if !bytes.Equal(pack, priorPack) || !reflect.DeepEqual(stats, priorStats) || !reflect.DeepEqual(scope.pending, priorScope.pending) || scope.puts != priorScope.puts {
						t.Fatal("reuse changed bytes, cost, admission or publication count")
					}
					if (err == nil) != (priorErr == nil) || err != nil && err.Error() != priorErr.Error() {
						t.Fatalf("reuse changed error: got=%v want=%v", err, priorErr)
					}
				} else {
					priorScope, priorPack, priorStats, priorErr = scope, bytes.Clone(pack), stats, err
				}
				scope.batch.Reset()
				// Restore the input height before the independent next run.
				for _, row := range rows {
					row.BlockNum = 1023
				}
			}
		})
	}
}

func TestStateHistoryChunkWorkCanonicalWriterBytes(t *testing.T) {
	enableSharedHistoryTest(t)
	prior := stateChangeBlockChunkEncoding.Swap(true)
	t.Cleanup(func() { stateChangeBlockChunkEncoding.Store(prior) })
	rows := chunkHistoryRows(512<<10, 3)
	oldScope, _, _, err := stateHistoryChunkWorkTestPlan(t, sharedHistoryTestDB(t), rows, false, "seed")
	if err != nil {
		t.Fatal(err)
	}
	db := sharedHistoryTestDB(t)
	scope := newSharedHistoryTestBatch(db)
	if err := WriteStateDomainChangeBlockRows(scope, rows); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(scope.pending, oldScope.pending) {
		t.Fatal("canonical writer differs from independent old planning")
	}
	if err := scope.batch.Write(); err != nil {
		t.Fatal(err)
	}
	for _, want := range rows {
		got, present, err := ReadStateDomainChange(db, want.BlockNum, want.Seq)
		if err != nil || !present || !reflect.DeepEqual(got, want) {
			t.Fatalf("canonical writer history differs: present=%v err=%v", present, err)
		}
	}
}
