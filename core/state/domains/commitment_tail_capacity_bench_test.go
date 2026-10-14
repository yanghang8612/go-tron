package domains

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/blockbuffer"
	"github.com/tronprotocol/go-tron/core/pointread"
	"github.com/tronprotocol/go-tron/core/rawdb"
)

// Count actual cursor calls after overlay/cache resolution, including prefetch.
// Instrumentation and the Pebble/cache/fold configuration are identical for
// both capacities. No per-hit counter is inserted into production code.
type tailCountedDB struct {
	ethdb.KeyValueStore
	reads [10]atomic.Uint64
}

func (d *tailCountedDB) NewPointReadSnapshot() (pointread.Snapshot, error) {
	return d.NewPointReadSnapshotWithCapacity(0)
}

func (d *tailCountedDB) NewPointReadSnapshotWithCapacity(n int) (pointread.Snapshot, error) {
	s, err := d.KeyValueStore.(pointread.CapacitySnapshotter).NewPointReadSnapshotWithCapacity(n)
	if err != nil {
		return nil, err
	}
	return &tailCountedSnapshot{Snapshot: s, db: d}, nil
}

func (d *tailCountedDB) IsKeyNotFound(err error) bool {
	return d.KeyValueStore.(interface{ IsKeyNotFound(error) bool }).IsKeyNotFound(err)
}

type tailCountedSnapshot struct {
	pointread.Snapshot
	db *tailCountedDB
}

func (s *tailCountedSnapshot) NewCursor(prefix []byte) (pointread.Cursor, error) {
	c, err := s.Snapshot.NewCursor(prefix)
	if err != nil {
		return nil, err
	}
	return &tailCountedCursor{Cursor: c, db: s.db, prefixLen: len(prefix)}, nil
}

type tailCountedCursor struct {
	pointread.Cursor
	db        *tailCountedDB
	prefixLen int
}

func (c *tailCountedCursor) View(key []byte, fn func([]byte) error) (bool, error) {
	depth := len(key) - c.prefixLen
	if depth < 0 || depth > 8 {
		depth = 9
	}
	c.db.reads[depth].Add(1)
	return c.Cursor.View(key, fn)
}

type tailPipelineFixture struct {
	rows    [][2][]byte
	updates [8][]rawdb.StateCommitmentUpdate
	roots   [8]common.Hash
}

var tailPipelineSeed struct {
	sync.Once
	keys [][]byte
}

func commitmentTailPipelineKeys() [][]byte {
	tailPipelineSeed.Do(func() {
		// A deliberately clustered but valid keyspace moves the sparse branches
		// to depths 6/7 without importing a mainnet database. Filtering happens
		// once outside measurement and is identical for every workload/policy.
		for candidate := uint64(1); len(tailPipelineSeed.keys) < 1536; candidate++ {
			var key [8]byte
			binary.BigEndian.PutUint64(key[:], candidate)
			path := keyPath(key[:])
			if path[0] == 0 && path[1]>>4 == 0 {
				tailPipelineSeed.keys = append(tailPipelineSeed.keys, bytes.Clone(key[:]))
			}
		}
	})
	return tailPipelineSeed.keys
}

func newTailPipelineFixture(t testing.TB, mode string) *tailPipelineFixture {
	t.Helper()
	keys := commitmentTailPipelineKeys()
	db := rawdb.NewMemoryDatabase()
	seed := make([]rawdb.StateCommitmentUpdate, len(keys))
	for i, key := range keys {
		seed[i] = rawdb.NewStateCommitmentPut(key, []byte{0, byte(i), byte(i >> 8)})
	}
	if _, err := ApplyLatestCommitmentWithStore(NewStagedCommitmentStore(db), seed); err != nil {
		t.Fatal(err)
	}
	f := new(tailPipelineFixture)
	it := db.NewIterator(nil, nil)
	for it.Next() {
		f.rows = append(f.rows, [2][]byte{bytes.Clone(it.Key()), bytes.Clone(it.Value())})
	}
	if err := it.Error(); err != nil {
		t.Fatal(err)
	}
	it.Release()
	for block := range f.updates {
		add := func(id int) {
			f.updates[block] = append(f.updates[block], rawdb.NewStateCommitmentPut(keys[id], []byte{byte(block + 1), byte(id), byte(id >> 8)}))
		}
		if mode != "cold_scan" {
			for i := 0; i < 128; i++ {
				add((i*53 + block*7) % 256)
			}
		}
		if mode != "hot" {
			for i := 0; i < 128; i++ {
				add(256 + (block*128+i)%1280)
			}
		}
		if mode == "delayed_read" && block > 1 {
			for i := 0; i < 64; i++ {
				add(256 + ((block-2)*128+i)%1280)
			}
		}
		var err error
		f.roots[block], err = ApplyLatestCommitmentWithStore(NewStagedCommitmentStore(db), f.updates[block])
		if err != nil {
			t.Fatal(err)
		}
	}
	return f
}

func (f *tailPipelineFixture) run(t testing.TB, db *tailCountedDB, tail int) (uint64, uint64) {
	t.Helper()
	buf := blockbuffer.New(db)
	buf.SetMaxInflight(3)
	buf.SetBaseReadCacheSizeWithTrunkAndTail(32<<10, tail, 4, rawdb.CommitmentBranchKeyPrefix)
	p, err := NewOrderedCommitmentPipeline(buf)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	for block, updates := range f.updates {
		number := uint64(block + 1)
		buf.BeginBlock(common.Hash{byte(number)}, number)
		h, _ := buf.NewestInflight()
		result := <-p.Submit(buf.ViewLayer(h), updates)
		if result.Err != nil || result.Root != f.roots[block] {
			t.Fatalf("block %d: root=%x err=%v want=%x", number, result.Root, result.Err, f.roots[block])
		}
		if err := buf.CommitInflight(h); err != nil {
			t.Fatal(err)
		}
		if err := buf.FlushUpTo(number, db); err != nil {
			t.Fatal(err)
		}
	}
	var total uint64
	for i := range db.reads {
		total += db.reads[i].Load()
	}
	return total, db.reads[6].Load() + db.reads[7].Load()
}

func (f *tailPipelineFixture) database(t testing.TB) *tailCountedDB {
	t.Helper()
	db, err := rawdb.NewPebbleDB(t.TempDir(), 16, 32)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	batch := db.NewBatch()
	for _, row := range f.rows {
		if err := batch.Put(row[0], row[1]); err != nil {
			t.Fatal(err)
		}
	}
	if err := batch.Write(); err != nil {
		t.Fatal(err)
	}
	return &tailCountedDB{KeyValueStore: db}
}

func TestCommitmentTailCapacityPipelineRootParity(t *testing.T) {
	t.Setenv("GTRON_COMMITMENT_PARTITIONS", "64")
	t.Setenv("GTRON_COMMITMENT_PREFETCH_OVERLAP", "0")
	for _, mode := range []string{"hot", "cold_scan", "scan", "delayed_read"} {
		t.Run(mode, func(t *testing.T) {
			f := newTailPipelineFixture(t, mode)
			for _, tail := range []int{0, 32 << 10, 64 << 10, 256 << 10} {
				total, deep := f.run(t, f.database(t), tail)
				t.Logf("tail=%d durable=%d depth6_7=%d", tail, total, deep)
			}
		})
	}
}

// Each operation starts from the same durable keyspace, folds eight blocks,
// publishes each layer and flushes it. Fresh Pebble setup and seed generation
// are outside the timer; pipeline/cache construction and Close are inside.
// This is a small clustered-key corpus, not a mainnet throughput forecast.
func BenchmarkCommitmentTailCapacityPipeline(b *testing.B) {
	b.Setenv("GTRON_COMMITMENT_PARTITIONS", "64")
	b.Setenv("GTRON_COMMITMENT_PREFETCH_OVERLAP", "0")
	for _, mode := range []string{"hot", "cold_scan", "scan", "delayed_read"} {
		f := newTailPipelineFixture(b, mode)
		for _, tail := range []int{0, 32 << 10, 64 << 10, 256 << 10} {
			b.Run(fmt.Sprintf("%s/tail_%d", mode, tail), func(b *testing.B) {
				b.ReportAllocs()
				var total, deep uint64
				for i := 0; i < b.N; i++ {
					b.StopTimer()
					db := f.database(b)
					b.StartTimer()
					n, d := f.run(b, db, tail)
					total += n
					deep += d
					b.StopTimer()
					if err := db.Close(); err != nil {
						b.Fatal(err)
					}
					b.StartTimer()
				}
				b.ReportMetric(float64(total)/float64(b.N), "durable_reads/op")
				b.ReportMetric(float64(deep)/float64(b.N), "depth6_7_reads/op")
				b.ReportMetric(8, "blocks/op")
			})
		}
	}
}
