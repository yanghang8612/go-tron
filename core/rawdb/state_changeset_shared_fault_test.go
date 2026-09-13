package rawdb

import (
	"bytes"
	"encoding/binary"
	"errors"
	"reflect"
	"testing"

	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/tronprotocol/go-tron/internal/historychunk"
)

type sharedHistoryFaultScope struct {
	*sharedHistoryTestBatch
	failKey    []byte
	failure    error
	failed     bool
	chunkReads int
}

func (s *sharedHistoryFaultScope) Put(key, value []byte) error {
	if bytes.Equal(key, s.failKey) {
		s.failed = true
		return s.failure
	}
	return s.sharedHistoryTestBatch.Put(key, value)
}

func (s *sharedHistoryFaultScope) Get(key []byte) ([]byte, error) {
	if bytes.HasPrefix(key, stateHistorySharedChunkPrefix) {
		s.chunkReads++
	}
	return s.sharedHistoryTestBatch.Get(key)
}

func (s *sharedHistoryFaultScope) Has(key []byte) (bool, error) {
	if bytes.HasPrefix(key, stateHistorySharedChunkPrefix) {
		s.chunkReads++
	}
	return s.sharedHistoryTestBatch.Has(key)
}

func TestSharedHistoryFaultFinalPublicationDiscardAndRetry(t *testing.T) {
	enableSharedHistoryTest(t)
	for _, phase := range []string{"metadata", "final_pack"} {
		t.Run(phase, func(t *testing.T) {
			db := sharedHistoryTestDB(t)
			rows := chunkHistoryRows(512<<10, 1)
			block := rows[0].BlockNum
			metaKey := stateHistoryChunkBucketKey(stateHistoryChunkBucket(block))
			packKey := stateChangeSetKey(block, 0)
			failKey := metaKey
			if phase == "final_pack" {
				failKey = packKey
			}
			want := errors.New("injected " + phase + " publication failure")
			scope := &sharedHistoryFaultScope{sharedHistoryTestBatch: newSharedHistoryTestBatch(db), failKey: failKey, failure: want}
			if err := WriteStateDomainChangeBlockRows(scope, rows); !errors.Is(err, want) || !scope.failed {
				t.Fatalf("specific publication failure was not reached: %v", err)
			}
			chunks := 0
			for key := range scope.pending {
				if bytes.HasPrefix([]byte(key), stateHistorySharedChunkPrefix) {
					chunks++
				}
			}
			if chunks == 0 {
				t.Fatal("fixture failed before staging any chunks")
			}
			if _, present := scope.pending[string(packKey)]; present {
				t.Fatal("referencing pack was staged despite publication failure")
			}
			if _, present := scope.pending[string(metaKey)]; present != (phase == "final_pack") {
				t.Fatalf("metadata staging does not distinguish failure phases: present=%v", present)
			}
			if got := sharedHistoryKVBytes(t, db, nil); got != 0 {
				t.Fatalf("failed scope escaped its atomic batch: %d KV bytes", got)
			}
			// Canonical failure discards the complete scope, including chunks
			// already staged before the failed metadata or final pack Put.
			scope.batch.Reset()
			clear(scope.pending)
			if err := scope.batch.Write(); err != nil {
				t.Fatal(err)
			}
			if got := sharedHistoryKVBytes(t, db, nil); got != 0 {
				t.Fatalf("discard left references, metadata or orphan chunks: %d KV bytes", got)
			}
			retry := newSharedHistoryTestBatch(db)
			if err := WriteStateDomainChangeBlockRows(retry, rows); err != nil {
				t.Fatal(err)
			}
			if err := retry.batch.Write(); err != nil {
				t.Fatal(err)
			}
			pack, err := db.Get(packKey)
			if err != nil || !isStateHistorySharedPack(pack) {
				t.Fatalf("retry did not publish the shared format: %v", err)
			}
			raw, err := decodeStateHistorySharedPack(db, pack, block)
			if err != nil || !bytes.Equal(raw, encodeBorrowedStateDomainChangeTestBlock(t, rows)) {
				t.Fatalf("retry did not restore the complete original RLP: %v", err)
			}
			got, present, err := ReadStateDomainChange(db, block, rows[0].Seq)
			if err != nil || !present || !reflect.DeepEqual(got, rows[0]) {
				t.Fatalf("public retry read differs: present=%v err=%v", present, err)
			}
		})
	}
}

type sharedHistoryFaultReader struct {
	ethdb.KeyValueReader
	reads int
}

func (r *sharedHistoryFaultReader) Get(key []byte) ([]byte, error) {
	r.reads++
	return r.KeyValueReader.Get(key)
}

func (r *sharedHistoryFaultReader) Has(key []byte) (bool, error) {
	r.reads++
	return r.KeyValueReader.Has(key)
}

// The hashes need not resolve: every table below must be rejected before the
// decoder attempts any point lookup, even for an earlier well-formed entry.
func sharedHistoryFaultPack(block, decoded, count uint64, lengths ...uint64) []byte {
	pack := append([]byte(nil), stateDomainChangeBlockEnvelopeMagic[:]...)
	pack = append(pack, stateDomainChangeBlockSharedVersion)
	pack = binary.AppendUvarint(pack, block)
	pack = binary.AppendUvarint(pack, decoded)
	pack = append(pack, make([]byte, 32)...)
	pack = binary.AppendUvarint(pack, count)
	for _, length := range lengths {
		pack = binary.AppendUvarint(pack, length)
		pack = append(pack, make([]byte, 32)...)
	}
	return pack
}

func TestSharedHistoryFaultMalformedHeaderDoesNotReadChunks(t *testing.T) {
	db := sharedHistoryTestDB(t)
	const block = uint64(42)
	for _, tc := range []struct {
		name string
		pack []byte
	}{
		{"wrong_physical_block", sharedHistoryFaultPack(block+1, historychunk.MinSize, 1, historychunk.MinSize)},
		{"decoded_over_limit", sharedHistoryFaultPack(block, stateDomainChangeBlockMaxDecodedBytes+1, 1, historychunk.MinSize)},
		{"decoded_zero", sharedHistoryFaultPack(block, 0, 1, historychunk.MinSize)},
		{"count_zero", sharedHistoryFaultPack(block, historychunk.MinSize, 0)},
		{"count_over_limit", sharedHistoryFaultPack(block, historychunk.MinSize, stateDomainChangeBlockMaxDecodedBytes/historychunk.MinSize+2)},
		{"truncated_table", sharedHistoryFaultPack(block, 2*historychunk.MinSize, 2, historychunk.MinSize)},
		{"zero_length", sharedHistoryFaultPack(block, historychunk.MinSize, 1, 0)},
		{"short_nonfinal_chunk", sharedHistoryFaultPack(block, 2*historychunk.MinSize, 2, historychunk.MinSize-1, historychunk.MinSize+1)},
		{"oversized_chunk", sharedHistoryFaultPack(block, historychunk.MaxSize+1, 1, historychunk.MaxSize+1)},
		{"invalid_tail_after_valid_chunk", sharedHistoryFaultPack(block, 2*historychunk.MinSize, 2, historychunk.MinSize, 0)},
		{"total_exceeds_decoded", sharedHistoryFaultPack(block, historychunk.MinSize, 2, historychunk.MinSize, historychunk.MinSize)},
		{"total_below_decoded", sharedHistoryFaultPack(block, 2*historychunk.MinSize, 1, historychunk.MinSize)},
		{"trailing_bytes", append(sharedHistoryFaultPack(block, historychunk.MinSize, 1, historychunk.MinSize), 0)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			reader := &sharedHistoryFaultReader{KeyValueReader: db}
			if data, err := decodeStateHistorySharedPack(reader, tc.pack, block); err == nil || data != nil {
				t.Fatalf("malformed pack returned data=%d err=%v", len(data), err)
			}
			if reader.reads != 0 {
				t.Fatalf("malformed reference table performed %d point lookups", reader.reads)
			}
		})
	}
}

func TestSharedHistoryFaultInvalidBucketSchemaDoesNotReadChunksOrWrite(t *testing.T) {
	enableSharedHistoryTest(t)
	db := sharedHistoryTestDB(t)
	rows := chunkHistoryRows(256<<10, 1)
	key := stateHistoryChunkBucketKey(stateHistoryChunkBucket(rows[0].BlockNum))
	for _, invalid := range [][]byte{{}, {1}, {2, 0}, {1, 2}, {1, 0, 0}} {
		if err := db.Put(key, invalid); err != nil {
			t.Fatal(err)
		}
		scope := &sharedHistoryFaultScope{sharedHistoryTestBatch: newSharedHistoryTestBatch(db)}
		if err := WriteStateDomainChangeBlockRows(scope, rows); err == nil {
			t.Fatalf("invalid bucket schema accepted: %x", invalid)
		}
		if scope.puts != 0 || len(scope.pending) != 0 || scope.chunkReads != 0 {
			t.Fatalf("invalid bucket schema caused chunk work: puts=%d pending=%d reads=%d", scope.puts, len(scope.pending), scope.chunkReads)
		}
	}
}

func TestSharedHistoryFaultCorruptExistingChunkDoesNotWrite(t *testing.T) {
	enableSharedHistoryTest(t)
	db := sharedHistoryTestDB(t)
	rows := chunkHistoryRows(512<<10, 1)
	seed := newSharedHistoryTestBatch(db)
	if err := WriteStateDomainChangeBlockRows(seed, rows); err != nil {
		t.Fatal(err)
	}
	if err := seed.batch.Write(); err != nil {
		t.Fatal(err)
	}
	it := db.NewIterator(stateHistoryChunkBucketPrefix(stateHistoryChunkBucket(rows[0].BlockNum)), nil)
	if !it.Next() {
		it.Release()
		t.Fatal("fixture has no shared chunks")
	}
	key, healthy := bytes.Clone(it.Key()), bytes.Clone(it.Value())
	it.Release()
	corrupt := bytes.Clone(healthy)
	corrupt[len(corrupt)-1] ^= 1
	if err := db.Put(key, corrupt); err != nil {
		t.Fatal(err)
	}
	before := sharedHistoryKVBytes(t, db, nil)
	// Physical height is the only changed field; the original RLP is equal,
	// so the next pack necessarily encounters every existing seed chunk.
	rows[0].BlockNum++
	scope := newSharedHistoryTestBatch(db)
	if err := WriteStateDomainChangeBlockRows(scope, rows); err == nil {
		t.Fatal("writer accepted a corrupt existing chunk")
	}
	if scope.puts != 0 || len(scope.pending) != 0 {
		t.Fatalf("corruption was discovered after publishing: puts=%d pending=%d", scope.puts, len(scope.pending))
	}
	if got := sharedHistoryKVBytes(t, db, nil); got != before {
		t.Fatalf("corrupt input changed durable KV bytes: got=%d want=%d", got, before)
	}
	if present, err := db.Has(stateChangeSetKey(rows[0].BlockNum, 0)); err != nil || present {
		t.Fatalf("failed referencing pack reached disk: present=%v err=%v", present, err)
	}
	if got, err := db.Get(key); err != nil || !bytes.Equal(got, corrupt) {
		t.Fatalf("writer silently replaced corrupt evidence: %v", err)
	}
	// Explicitly restore the fixture's known original bytes, then a fresh
	// publication can reuse them and the public reader must recover the row.
	if err := db.Put(key, healthy); err != nil {
		t.Fatal(err)
	}
	retry := newSharedHistoryTestBatch(db)
	if err := WriteStateDomainChangeBlockRows(retry, rows); err != nil {
		t.Fatal(err)
	}
	if err := retry.batch.Write(); err != nil {
		t.Fatal(err)
	}
	got, present, err := ReadStateDomainChange(db, rows[0].BlockNum, rows[0].Seq)
	if err != nil || !present || !reflect.DeepEqual(got, rows[0]) {
		t.Fatalf("repaired fixture retry differs: present=%v err=%v", present, err)
	}
}
