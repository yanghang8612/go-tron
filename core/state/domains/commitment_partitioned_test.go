package domains

import (
	"bytes"
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/blockbuffer"
	"github.com/tronprotocol/go-tron/core/pointread"
	"github.com/tronprotocol/go-tron/core/rawdb"
)

func TestPartitionedCommitmentCompatibility(t *testing.T) {
	t.Setenv("GTRON_COMMITMENT_PARTITIONS", "64")
	for _, tc := range []struct {
		name string
		run  func(*testing.T)
	}{
		{"inflight", TestOrderedCommitmentPipelineMatchesSequentialAcrossInflightBlocks},
		{"empty-singleton", TestOrderedCommitmentPipelineEmptySingletonNoopAndDelete},
		{"immutable-base", TestOrderedCommitmentPipelineUsesImmutableBaseDelta},
		{"frozen-delta", TestOrderedCommitmentPipelineUsesNewDeltaOverFrozenDeltaAndBase},
		{"pebble-prefetch", TestOrderedCommitmentPipelinePrefetchPreservesPebbleRoot},
		{"pebble-overlap", TestOrderedCommitmentPrefetchOverlapPebbleInflightParity},
		{"session-lifetime", TestOrderedCommitmentPrefetchOverlapPreservesSessionLifetime},
	} {
		t.Run(tc.name, tc.run)
	}
}

func TestPartitionedCommitmentCollapseRestart(t *testing.T) {
	t.Setenv("GTRON_COMMITMENT_PARTITIONS", "64")
	db, err := rawdb.NewPebbleDB(t.TempDir(), 16, 32)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	reference := rawdb.NewMemoryDatabase()
	buf := blockbuffer.New(db)
	buf.SetMaxInflight(4)
	// Many leaves per partition, followed by deletion down to one leaf across
	// all four owners of a first-level branch, then emptiness and reinsertion.
	seed := buildRandomPuts(rand.New(rand.NewSource(871331)), 512)
	var height uint64
	for cycle := 0; cycle < 5; cycle++ {
		p, err := NewOrderedCommitmentPipeline(buf)
		if err != nil {
			t.Fatal(err)
		}
		blocks := make([][]rawdb.StateCommitmentUpdate, 4)
		blocks[0] = seed
		for _, item := range seed[1:] {
			blocks[1] = append(blocks[1], rawdb.NewStateCommitmentDelete(item.Key))
		}
		blocks[2] = []rawdb.StateCommitmentUpdate{rawdb.NewStateCommitmentDelete(seed[0].Key)}
		blocks[3] = seed[:1] // restart with a virtual singleton at depth one
		var results [4]<-chan OrderedCommitmentResult
		var handles [4]blockbuffer.InflightHandle
		var roots [4]common.Hash
		for i, updates := range blocks {
			roots[i], err = ApplyLatestCommitmentWithStore(NewStagedCommitmentStore(reference), updates)
			if err != nil {
				t.Fatal(err)
			}
			height++
			buf.BeginBlock(common.Hash{byte(height)}, height)
			handles[i], _ = buf.NewestInflight()
			results[i] = p.Submit(buf.ViewLayer(handles[i]), updates)
		}
		for i, result := range results {
			got := <-result
			if got.Err != nil || got.Root != roots[i] {
				t.Fatalf("cycle %d block %d: root %x err %v want %x", cycle, i, got.Root, got.Err, roots[i])
			}
			if err := buf.CommitInflight(handles[i]); err != nil {
				t.Fatal(err)
			}
		}
		p.Close()
		if err := buf.Flush(db); err != nil {
			t.Fatal(err)
		}
		want, got := collectCommitmentRows(t, reference), collectCommitmentRows(t, db)
		if len(want) != len(got) {
			t.Fatalf("row counts %d != %d", len(want), len(got))
		}
		for key, value := range want {
			if !bytes.Equal(got[key], value) {
				t.Fatalf("branch %x differs", key)
			}
		}
	}
}

func TestPartitionedCommitmentRejectsCorruptChild(t *testing.T) {
	t.Setenv("GTRON_COMMITMENT_PARTITIONS", "64")
	db := rawdb.NewMemoryDatabase()
	if _, err := ApplyLatestCommitmentWithStore(NewStagedCommitmentStore(db), buildRandomPuts(rand.New(rand.NewSource(77)), 512)); err != nil {
		t.Fatal(err)
	}
	store := newRawdbBranchStore(db)
	if err := store.PutBranch([]byte{0}, BranchData{}); err != nil {
		t.Fatal(err)
	}
	if p, err := NewOrderedCommitmentPipeline(db); err == nil {
		p.Close()
		t.Fatal("accepted corrupt first-level branch")
	}
}

// Delay only durable cursor reads, below overlay/cache resolution. This is a
// controlled I/O-latency model over real Pebble SSTs, not an EBS benchmark.
type partitionLatencyDB struct {
	ethdb.KeyValueStore
	delay time.Duration
}

func (d *partitionLatencyDB) NewPointReadSnapshot() (pointread.Snapshot, error) {
	s, err := d.KeyValueStore.(pointread.Snapshotter).NewPointReadSnapshot()
	if err != nil {
		return nil, err
	}
	return &partitionLatencySnapshot{Snapshot: s, delay: d.delay}, nil
}

type partitionLatencySnapshot struct {
	pointread.Snapshot
	delay time.Duration
}

func (s *partitionLatencySnapshot) NewCursor(prefix []byte) (pointread.Cursor, error) {
	c, err := s.Snapshot.NewCursor(prefix)
	if err != nil {
		return nil, err
	}
	return &partitionLatencyCursor{Cursor: c, delay: s.delay}, nil
}

type partitionLatencyCursor struct {
	pointread.Cursor
	delay time.Duration
}

func (c *partitionLatencyCursor) View(key []byte, fn func([]byte) error) (bool, error) {
	if c.delay > 0 {
		time.Sleep(c.delay)
	}
	return c.Cursor.View(key, fn)
}

func BenchmarkCommitmentPartitionsPebble(b *testing.B) {
	for _, delay := range []time.Duration{0, 250 * time.Microsecond} {
		for _, batch := range []int{0, 16, 512} {
			for _, mode := range []string{"16", "64"} {
				b.Run(fmt.Sprintf("delay=%s/batch=%d/lanes=%s", delay, batch, mode), func(b *testing.B) {
					b.Setenv("GTRON_COMMITMENT_PARTITIONS", mode)
					old := CommitmentParentPrefetchDepth
					CommitmentParentPrefetchDepth = 0
					defer func() { CommitmentParentPrefetchDepth = old }()
					db, err := rawdb.NewPebbleDB(b.TempDir(), 16, 64)
					if err != nil {
						b.Fatal(err)
					}
					defer func() { _ = db.Close() }()
					seed := buildRandomPuts(rand.New(rand.NewSource(986)), 32768)
					if _, err := ApplyLatestCommitmentWithStore(NewStagedCommitmentStore(db), seed); err != nil {
						b.Fatal(err)
					}
					if err := db.Compact(nil, nil); err != nil {
						b.Fatal(err)
					}
					buf := blockbuffer.New(&partitionLatencyDB{KeyValueStore: db, delay: delay})
					p, err := NewOrderedCommitmentPipeline(buf)
					if err != nil {
						b.Fatal(err)
					}
					defer p.Close()
					updates := make([]rawdb.StateCommitmentUpdate, batch)
					b.ReportAllocs()
					b.ResetTimer()
					for i := 0; i < b.N; i++ {
						for j := range updates {
							updates[j] = rawdb.NewStateCommitmentPut(seed[(i*277+j*41)%len(seed)].Key, []byte{byte(i), byte(i >> 8), byte(j)})
						}
						buf.BeginBlock(common.Hash{byte(i + 1)}, uint64(i+1))
						h, _ := buf.NewestInflight()
						if result := <-p.Submit(buf.ViewLayer(h), updates); result.Err != nil {
							b.Fatal(result.Err)
						}
						if err := buf.CommitInflight(h); err != nil {
							b.Fatal(err)
						}
						if err := buf.Flush(db); err != nil {
							b.Fatal(err)
						}
					}
					b.StopTimer()
				})
			}
		}
	}
}
