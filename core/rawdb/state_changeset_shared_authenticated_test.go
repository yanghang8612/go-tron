package rawdb

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"math/rand"
	"reflect"
	"testing"

	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/golang/snappy"
	"github.com/tronprotocol/go-tron/internal/historychunk"
)

type sharedAuthenticatedTrace struct {
	op, key, value string
}

type sharedAuthenticatedScope struct {
	*sharedHistoryTestBatch
	trace           []sharedAuthenticatedTrace
	reads, failRead int
	failWriteKey    []byte
	atomic          bool
}

func (s *sharedAuthenticatedScope) StateHistoryChunkWritesAtomic() bool { return s.atomic }
func (s *sharedAuthenticatedScope) Get(key []byte) ([]byte, error) {
	s.trace = append(s.trace, sharedAuthenticatedTrace{op: "Get", key: string(key)})
	s.reads++
	if s.failRead != 0 && s.reads == s.failRead {
		return nil, errors.New("injected authenticated chunk read")
	}
	return s.sharedHistoryTestBatch.Get(key)
}
func (s *sharedAuthenticatedScope) Has(key []byte) (bool, error) {
	s.trace = append(s.trace, sharedAuthenticatedTrace{op: "Has", key: string(key)})
	return s.sharedHistoryTestBatch.Has(key)
}
func (s *sharedAuthenticatedScope) Put(key, value []byte) error {
	s.trace = append(s.trace, sharedAuthenticatedTrace{op: "Put", key: string(key), value: string(value)})
	if bytes.Equal(key, s.failWriteKey) {
		return errors.New("injected authenticated chunk write")
	}
	return s.sharedHistoryTestBatch.Put(key, value)
}

type sharedAuthenticatedPlanner func(ethdb.KeyValueWriter, uint64, []byte, []byte, []*StateDomainChange, bool, *stateChangeChunkWork) ([]byte, *stateHistorySharedWriteStats, error)

type sharedAuthenticatedResult struct {
	pack, readBack []byte
	stats          *stateHistorySharedWriteStats
	err            error
	trace          []sharedAuthenticatedTrace
	pending        map[string][]byte
	puts           int
	counters       [7]int64
}

func sharedAuthenticatedCounters() [7]int64 {
	return [7]int64{sharedHistoryAttempts.Snapshot().Count(), sharedHistoryFallback.Snapshot().Count(),
		sharedHistoryRetired.Snapshot().Count(), sharedHistorySelected.Snapshot().Count(),
		sharedHistoryBaselineBytes.Snapshot().Count(), sharedHistoryChunkBytes.Snapshot().Count(),
		sharedHistoryPackBytes.Snapshot().Count()}
}

func sharedAuthenticatedRun(t *testing.T, scenario string, reuse bool, planner sharedAuthenticatedPlanner) sharedAuthenticatedResult {
	t.Helper()
	rows := chunkHistoryRows(512<<10, 3)
	if scenario == "snappy_existing" {
		for _, row := range rows {
			row.Prev = bytes.Repeat([]byte("same-old-value-contents"), 25000)
		}
	}
	raw := encodeBorrowedStateDomainChangeTestBlock(t, rows)
	originalRaw := bytes.Clone(raw)
	var work stateChangeChunkWork
	var request *stateChangeChunkWork
	if reuse {
		request = &work
	}
	baseline, _ := encodeStateDomainChangeBlockStorageWithChunkWork(raw, rows, true, request)
	if scenario == "snappy_existing" {
		baseline = raw // Exercise compressed stored chunks without the seed-cost gate masking it.
	}
	base := NewMemoryDatabase()
	defer func() { _ = base.Close() }()
	existing := scenario == "existing" || scenario == "snappy_existing" || scenario == "corrupt_chunk" ||
		scenario == "corrupt_envelope" || scenario == "missing_chunk" || scenario == "bucket_boundary" || scenario == "read_error"
	if existing {
		seed := newSharedHistoryTestBatch(base)
		_, stats, err := legacySharedAuthenticatedChunkPlanner(seed, rows[0].BlockNum, raw, baseline, rows, true, request)
		if err != nil || stats == nil {
			t.Fatalf("seed not admitted: stats=%+v err=%v", stats, err)
		}
		if err := seed.batch.Write(); err != nil {
			t.Fatal(err)
		}
	}
	if scenario == "corrupt_chunk" || scenario == "corrupt_envelope" || scenario == "missing_chunk" {
		it := base.NewIterator(stateHistoryChunkBucketPrefix(0), nil)
		if !it.Next() {
			t.Fatal("seed has no chunk")
		}
		key, value := bytes.Clone(it.Key()), bytes.Clone(it.Value())
		it.Release()
		if scenario == "missing_chunk" {
			if err := base.Delete(key); err != nil {
				t.Fatal(err)
			}
		} else {
			if scenario == "corrupt_envelope" {
				value[0] = 2
			} else {
				value[len(value)-1] ^= 1
			}
			if err := base.Put(key, value); err != nil {
				t.Fatal(err)
			}
		}
	}
	if scenario == "bucket_boundary" {
		for _, row := range rows {
			row.BlockNum = 1024
		}
	}
	metaKey := stateHistoryChunkBucketKey(stateHistoryChunkBucket(rows[0].BlockNum))
	packKey := stateChangeSetKey(rows[0].BlockNum, 0)
	if scenario == "retired" || scenario == "invalid_meta" {
		meta := []byte{1, 1}
		if scenario == "invalid_meta" {
			meta[0] = 2
		}
		if err := base.Put(metaKey, meta); err != nil {
			t.Fatal(err)
		}
	}
	if scenario == "cost_fallback" {
		baseline = []byte{0}
	}
	scope := &sharedAuthenticatedScope{sharedHistoryTestBatch: newSharedHistoryTestBatch(base), atomic: scenario != "not_atomic"}
	if scenario == "read_error" {
		scope.failRead = 3
	}
	if scenario == "chunk_put_error" {
		scope.failAt = 2
	}
	if scenario == "metadata_put_error" {
		scope.failWriteKey = metaKey
	}
	if scenario == "pack_put_error" {
		scope.failWriteKey = packKey
	}
	before := sharedAuthenticatedCounters()
	pack, stats, err := planner(scope, rows[0].BlockNum, raw, baseline, rows, scenario != "disabled", request)
	if err == nil {
		err = scope.Put(packKey, pack)
		if err == nil {
			stats.observe()
		}
	}
	after := sharedAuthenticatedCounters()
	for i := range after {
		after[i] -= before[i]
	}
	result := sharedAuthenticatedResult{pack: pack, stats: stats, err: err, trace: scope.trace, pending: scope.pending, puts: scope.puts, counters: after}
	if stats != nil {
		stats.work = 0
	}
	if err == nil && isStateHistorySharedPack(pack) {
		result.readBack, err = decodeStateHistorySharedPack(scope.sharedHistoryTestBatch, pack, rows[0].BlockNum)
		if err != nil || !bytes.Equal(result.readBack, raw) {
			t.Fatalf("reader rejected writer output: %v", err)
		}
	}
	if !bytes.Equal(raw, originalRaw) {
		t.Fatal("planner mutated immutable RLP input")
	}
	scope.batch.Reset()
	return result
}

func TestSharedHistoryAuthenticatedCandidateFullPlannerOracle(t *testing.T) {
	for _, scenario := range []string{"seed", "existing", "snappy_existing", "bucket_boundary", "retired", "invalid_meta", "missing_chunk", "corrupt_chunk", "corrupt_envelope", "cost_fallback", "read_error", "chunk_put_error", "metadata_put_error", "pack_put_error", "disabled", "not_atomic"} {
		for _, reuse := range []bool{false, true} {
			name := scenario + "/fresh-hash"
			if reuse {
				name = scenario + "/v2-hash"
			}
			t.Run(name, func(t *testing.T) {
				want := sharedAuthenticatedRun(t, scenario, reuse, legacySharedAuthenticatedChunkPlanner)
				expectError := scenario == "invalid_meta" || scenario == "corrupt_chunk" || scenario == "corrupt_envelope" ||
					scenario == "read_error" || scenario == "chunk_put_error" || scenario == "metadata_put_error" || scenario == "pack_put_error"
				if (want.err != nil) != expectError {
					t.Fatalf("oracle fixture missed expected error path: %v", want.err)
				}
				got := sharedAuthenticatedRun(t, scenario, reuse, planAndWriteSharedStateHistoryWithChunkWork)
				if !reflect.DeepEqual(got, want) {
					t.Fatalf("full operation differs: error got=%v want=%v, trace %d/%d, puts %d/%d, stats got=%+v want=%+v",
						got.err, want.err, len(got.trace), len(want.trace), got.puts, want.puts, got.stats, want.stats)
				}
			})
		}
	}
}

func TestSharedHistoryAuthenticatedReaderDecoderOracle(t *testing.T) {
	rng := rand.New(rand.NewSource(20260914))
	check := func(data []byte, want int, hash [32]byte) {
		t.Helper()
		before := bytes.Clone(data)
		expected, wantErr := legacySharedAuthenticatedChunkDecoder(data, want, hash)
		got, gotErr := decodeStateHistorySharedChunk(data, want, hash)
		if !bytes.Equal(expected, got) || !reflect.DeepEqual(gotErr, wantErr) || !bytes.Equal(data, before) {
			t.Fatalf("reader auth/error changed: want=%d got=%v expected=%v", want, gotErr, wantErr)
		}
	}
	for _, size := range []int{1, historychunk.MinSize - 1, historychunk.MinSize, historychunk.MaxSize} {
		for _, compressible := range []bool{false, true} {
			raw := make([]byte, size)
			if !compressible {
				_, _ = rng.Read(raw)
			}
			hash := sha256.Sum256(raw)
			encoded := encodeStateHistorySharedChunk(raw)
			for _, want := range []int{size, 0, -1, size - 1, size + 1, historychunk.MaxSize + 1} {
				check(encoded, want, hash)
			}
			badHash := hash
			badHash[0] ^= 1
			check(encoded, size, badHash) // Public reader must still reject a valid payload at the wrong digest.
			for i := range encoded {
				if i > 20 && i != len(encoded)-1 {
					continue
				}
				bad := bytes.Clone(encoded)
				bad[i] ^= 0x80
				check(bad, size, hash)
			}
			for _, bad := range [][]byte{nil, {1, 2, 1, 0}, {1, 0, 0}, append(bytes.Clone(encoded), 0)} {
				check(bad, size, hash)
			}
			// Force a valid Snappy length followed by an invalid literal body.
			bad := []byte{1, 1}
			bad = binary.AppendUvarint(bad, uint64(size))
			payload := snappy.Encode(nil, raw)
			bad = append(bad, payload[:len(payload)-1]...)
			check(bad, size, hash)
		}
	}
	for i := 0; i < 1000; i++ {
		data := make([]byte, rng.Intn(256))
		_, _ = rng.Read(data)
		check(data, rng.Intn(historychunk.MaxSize+3)-1, sha256.Sum256(data))
	}
}
