package rawdb

// The replay database is always a newly created private temporary directory.
// Exported packs are diagnostic inputs, not a canonical blockchain backup.
import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"sync"

	"github.com/ethereum/go-ethereum/ethdb"
)

type HistorySharingSample struct {
	Block              uint64                      `json:"block"`
	Bucket             uint64                      `json:"bucket"`
	Rows               uint64                      `json:"rows"`
	DecodedBytes       uint64                      `json:"decoded_bytes"`
	RawSHA256          string                      `json:"raw_sha256"`
	ExportBytes        uint64                      `json:"export_bytes"`
	BaselinePackBytes  uint64                      `json:"baseline_pack_bytes"`
	BaselineKVBytes    uint64                      `json:"baseline_kv_bytes"`
	PackBytes          uint64                      `json:"pack_bytes"`
	NewChunkValueBytes uint64                      `json:"new_chunk_value_bytes"`
	NewChunkKeyBytes   uint64                      `json:"new_chunk_key_bytes"`
	NewMetadataKVBytes uint64                      `json:"new_metadata_kv_bytes"`
	NewKVBytes         uint64                      `json:"new_kv_bytes"`
	NewChunks          uint64                      `json:"new_chunks"`
	Shared             bool                        `json:"shared"`
	ByteExact          bool                        `json:"byte_exact"`
	ReopenExact        bool                        `json:"reopen_exact"`
	BaselineEncode     HistoryCodecBenchmarkTiming `json:"baseline_encode"`
	Write              HistoryCodecBenchmarkTiming `json:"write"`
	Read               HistoryCodecBenchmarkTiming `json:"read"`
	ReopenRead         HistoryCodecBenchmarkTiming `json:"reopen_read"`
}

// HistorySharingBenchmark is serialized and holds only hashes of completed
// inputs. It uses production packing, explicit atomic batches and real Pebble,
// keeping original heights so bucket boundaries cannot be hidden by replay.
type HistorySharingBenchmark struct {
	mu               sync.Mutex
	db               ethdb.KeyValueStore
	directory        string
	samples          []HistorySharingSample
	encoded, decoded uint64
	sealed, failed   bool
}

func NewHistorySharingBenchmark() (*HistorySharingBenchmark, error) {
	dir, err := os.MkdirTemp("", "gtron-history-sharing-")
	if err != nil {
		return nil, err
	}
	db, err := NewPebbleDB(dir, 32, 16)
	if err != nil {
		return nil, errors.Join(err, os.RemoveAll(dir))
	}
	return &HistorySharingBenchmark{db: db, directory: dir}, nil
}

func (b *HistorySharingBenchmark) Close() error {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.sealed = true
	var err error
	if b.db != nil {
		err = b.db.Close()
		b.db = nil
	}
	return errors.Join(err, os.RemoveAll(b.directory))
}

// AddPack accepts only self-contained formats, with hard 128MiB/pack,
// 1GiB cumulative encoded/decoded and 256-pack ceilings. Failed attempts
// poison this diagnostic replay, which can only be closed thereafter.
func (b *HistorySharingBenchmark) AddPack(ctx context.Context, block uint64, encoded []byte) (sample HistorySharingSample, resultErr error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	defer func() {
		if resultErr != nil {
			b.failed = true
		}
	}()
	if ctx == nil {
		return sample, errors.New("nil sharing replay context")
	}
	if b.db == nil || b.sealed || b.failed {
		return sample, errors.New("sharing replay closed, failed or sealed")
	}
	if err := ctx.Err(); err != nil {
		return sample, err
	}
	if len(b.samples) >= 256 || len(encoded) == 0 || len(encoded) > 128<<20 || uint64(len(encoded)) > (1<<30)-b.encoded {
		return sample, ErrHistoryCodecBenchmarkLimit
	}
	if n := len(b.samples); n > 0 && block <= b.samples[n-1].Block {
		return sample, errors.New("sharing replay heights must increase")
	}
	b.encoded += uint64(len(encoded))
	_, size, compressed, err := stateDomainChangeBlockCompressionPayload(encoded)
	if err != nil {
		return sample, err
	}
	if !compressed {
		size = len(encoded)
	}
	if uint64(size) > (1<<30)-b.decoded {
		return sample, ErrHistoryCodecBenchmarkLimit
	}
	b.decoded += uint64(size)
	raw, err := decodeStateDomainChangeBlockStorage(encoded)
	if err != nil {
		return sample, err
	}
	var validation HistoryCodecBenchmarkSample
	if err := inspectHistoryBenchmarkPack(ctx, raw, block, 100000, &validation); err != nil {
		return sample, err
	}
	changes, err := decodePersistedStateDomainChangeBlock(raw, block)
	if err != nil {
		return sample, err
	}
	sample = HistorySharingSample{Block: block, Bucket: stateHistoryChunkBucket(block), Rows: validation.Rows, DecodedBytes: uint64(len(raw)), RawSHA256: historyBenchmarkDigest(raw), ExportBytes: uint64(len(encoded))}
	started := startHistoryBenchmarkTiming()
	baseline, _ := encodeStateDomainChangeBlockStorageWithDedup(raw, changes, true)
	sample.BaselineEncode = started.finish()
	sample.BaselinePackBytes = uint64(len(baseline))
	sample.BaselineKVBytes = uint64(len(baseline) + len(stateChangeSetKey(block, 0)))
	if err := ctx.Err(); err != nil {
		return sample, err
	}
	tx := &historySharingReplayBatch{KeyValueStore: b.db, batch: b.db.NewBatch(), pending: make(map[string][]byte)}
	started = startHistoryBenchmarkTiming()
	if err := writeStateDomainChangeBlockRows(tx, changes, true, true); err != nil {
		return sample, err
	}
	if err := ctx.Err(); err != nil {
		return sample, err
	}
	if err := tx.batch.Write(); err != nil {
		return sample, err
	}
	sample.Write = started.finish()
	for key, value := range tx.pending {
		sample.NewKVBytes += uint64(len(key) + len(value))
		switch {
		case bytes.HasPrefix([]byte(key), stateHistorySharedChunkPrefix):
			sample.NewChunks++
			sample.NewChunkKeyBytes += uint64(len(key))
			sample.NewChunkValueBytes += uint64(len(value))
		case bytes.HasPrefix([]byte(key), stateHistorySharedBucketPrefix):
			sample.NewMetadataKVBytes += uint64(len(key) + len(value))
		case bytes.Equal([]byte(key), stateChangeSetKey(block, 0)):
			sample.PackBytes = uint64(len(value))
			sample.Shared = isStateHistorySharedPack(value)
		default:
			return sample, fmt.Errorf("unexpected sharing replay key")
		}
	}
	started = startHistoryBenchmarkTiming()
	if err := b.verify(ctx, sample); err != nil {
		return sample, err
	}
	sample.Read, sample.ByteExact = started.finish(), true
	b.samples = append(b.samples, sample)
	return sample, nil
}

func (b *HistorySharingBenchmark) verify(ctx context.Context, sample HistorySharingSample) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	view, release, err := AcquireStateHistoryReadView(b.db)
	if err != nil {
		return err
	}
	defer release()
	pack, err := view.Get(stateChangeSetKey(sample.Block, 0))
	if err != nil {
		return err
	}
	materialized, err := materializeStateHistorySharedPack(pack, sample.Block, []ethdb.KeyValueReader{view})
	if err != nil {
		return err
	}
	raw, err := decodeStateDomainChangeBlockStorage(materialized)
	if err != nil {
		return err
	}
	if uint64(len(raw)) != sample.DecodedBytes || historyBenchmarkDigest(raw) != sample.RawSHA256 {
		return errors.New("sharing replay original RLP differs")
	}
	// Also validate the production public iterator after the restart, rather
	// than considering raw decoder equality alone sufficient reader coverage.
	var rows uint64
	if err := IterateStateDomainChangesContext(ctx, view, sample.Block, func(change *StateDomainChange) (bool, error) { rows++; return true, ctx.Err() }); err != nil {
		return err
	}
	if rows != sample.Rows {
		return errors.New("sharing replay public reader row count differs")
	}
	return ctx.Err()
}

// ReopenAndVerify seals writes, closes Pebble and checks every completed pack
// in a fresh reader. Logical KV size is measured from the actual reopened DB;
// it excludes WAL/SST amplification and must not be called physical disk size.
func (b *HistorySharingBenchmark) ReopenAndVerify(ctx context.Context) ([]HistorySharingSample, uint64, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.db == nil || b.sealed || b.failed {
		return nil, 0, errors.New("sharing replay is not open for verification")
	}
	b.sealed = true
	if err := b.db.Close(); err != nil {
		b.db = nil
		return nil, 0, err
	}
	b.db = nil
	db, err := NewPebbleDB(b.directory, 32, 16)
	if err != nil {
		return nil, 0, err
	}
	b.db = db
	for i := range b.samples {
		started := startHistoryBenchmarkTiming()
		if err := b.verify(ctx, b.samples[i]); err != nil {
			return nil, 0, err
		}
		b.samples[i].ReopenRead, b.samples[i].ReopenExact = started.finish(), true
	}
	var total uint64
	it := b.db.NewIterator(nil, nil)
	defer it.Release()
	for it.Next() {
		if err := ctx.Err(); err != nil {
			return nil, 0, err
		}
		total += uint64(len(it.Key()) + len(it.Value()))
	}
	if err := it.Error(); err != nil {
		return nil, 0, err
	}
	var written uint64
	for _, s := range b.samples {
		written += s.NewKVBytes
	}
	if total != written {
		return nil, 0, errors.New("reopened logical KV bytes differ from atomic publication accounting")
	}
	return append([]HistorySharingSample(nil), b.samples...), total, nil
}

// This capability belongs only to the benchmark's owned single-use batch.
// Discarding it abandons all puts; its private DB has no concurrent writers.
type historySharingReplayBatch struct {
	ethdb.KeyValueStore
	batch   ethdb.Batch
	pending map[string][]byte
}

func (b *historySharingReplayBatch) StateHistoryChunkWritesAtomic() bool { return true }
func (b *historySharingReplayBatch) Get(key []byte) ([]byte, error) {
	if v, ok := b.pending[string(key)]; ok {
		return bytes.Clone(v), nil
	}
	return b.KeyValueStore.Get(key)
}
func (b *historySharingReplayBatch) Has(key []byte) (bool, error) {
	if _, ok := b.pending[string(key)]; ok {
		return true, nil
	}
	return b.KeyValueStore.Has(key)
}
func (b *historySharingReplayBatch) Put(key, value []byte) error {
	if err := b.batch.Put(key, value); err != nil {
		return err
	}
	b.pending[string(key)] = bytes.Clone(value)
	return nil
}
func (b *historySharingReplayBatch) Delete([]byte) error {
	return errors.New("sharing replay does not delete")
}
