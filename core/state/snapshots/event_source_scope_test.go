package snapshots

import (
	"encoding/binary"
	"fmt"
	"testing"

	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
)

type eventCanonicalCountingChain struct {
	db                              AggregatorDB
	finish                          uint64
	authority, terminal, solidReads uint64
	bad                             map[uint64]bool
}

func (c *eventCanonicalCountingChain) DB() AggregatorDB { return c.db }
func (c *eventCanonicalCountingChain) LatestSolidifiedBlockNum() int64 {
	c.solidReads++
	return int64(c.finish)
}
func (c *eventCanonicalCountingChain) CanonicalBlockHashStrict(n uint64) (common.Hash, bool, error) {
	if n == c.finish {
		c.authority++
	} else {
		c.terminal++
	}
	if c.bad[n] {
		return common.Hash{0xfe}, true, nil
	}
	return eventCanonicalTestHash(n), true, nil
}
func eventCanonicalTestHash(n uint64) common.Hash {
	var h common.Hash
	binary.BigEndian.PutUint64(h[len(h)-8:], n+1)
	return h
}
func eventCanonicalScopeFixture(t testing.TB, segments int) (*Manager, *Manifest, *eventCanonicalCountingChain) {
	t.Helper()
	db := rawdb.NewMemoryChainDB()
	finish := uint64(segments)*10 + 1
	if err := rawdb.WriteStageProgressWithHash(db, rawdb.StageFinish, finish, eventCanonicalTestHash(finish)); err != nil {
		t.Fatal(err)
	}
	chain := &eventCanonicalCountingChain{db: db, finish: finish, bad: make(map[uint64]bool)}
	refs := make([]SegmentRef, segments)
	for i := range refs {
		end := uint64(i+1) * 10
		refs[i] = SegmentRef{Dataset: SegmentDatasetEventLog, Kind: SegmentEventLog, FromTxNum: uint64(i)*10 + 1, ToTxNum: end, SourceBlockHash: eventCanonicalTestHash(end).Hex()}
	}
	manifest := manifestLookupView(&Manifest{Segments: refs})
	manager := &Manager{}
	manager.SetEventLogCanonicalSource(chain)
	return manager, manifest, chain
}

func TestEventCanonicalScopeReadsAuthorityOnceAndNeverCachesFork(t *testing.T) {
	manager, manifest, chain := eventCanonicalScopeFixture(t, 16384)
	from := uint64(8192*10 + 1)
	to := from + 30
	if ok, err := manager.eventSourcesCanonical(manifest, from, to); err != nil || !ok {
		t.Fatal(ok, err)
	}
	if chain.authority != 1 || chain.solidReads != 1 || chain.terminal != 4 {
		t.Fatal("authority must be once per operation; terminals only intersecting refs", chain.authority, chain.solidReads, chain.terminal)
	}
	chain.bad[from+9] = true
	if ok, err := manager.eventSourcesCanonical(manifest, from, to); err == nil || ok {
		t.Fatal("cross-request fork proof cached", ok, err)
	}
	if chain.authority != 2 || chain.solidReads != 2 {
		t.Fatal("new request did not re-observe authority", chain.authority, chain.solidReads)
	}
}

// This isolates the newly added canonical-proof layer from the existing file
// decoder/cache paths. After one immutable catalog index build, a one-ref query
// does no full-catalog copy/sort and one fresh Finish/solid observation.
func BenchmarkEventCanonicalNarrowQuery(b *testing.B) {
	for _, segments := range []int{16, 1024, 16384} {
		b.Run(fmt.Sprintf("segments_%d", segments), func(b *testing.B) {
			manager, manifest, chain := eventCanonicalScopeFixture(b, segments)
			n := uint64(segments/2)*10 + 1
			if ok, err := manager.eventSourcesCanonical(manifest, n, n); err != nil || !ok {
				b.Fatal(ok, err)
			}
			chain.authority, chain.terminal, chain.solidReads = 0, 0, 0
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if ok, err := manager.eventSourcesCanonical(manifest, n, n); err != nil || !ok {
					b.Fatal(ok, err)
				}
			}
			b.StopTimer()
			b.ReportMetric(float64(chain.authority)/float64(b.N), "Finish/op")
			b.ReportMetric(float64(chain.terminal)/float64(b.N), "terminal/op")
			b.ReportMetric(float64(chain.solidReads)/float64(b.N), "solid/op")
		})
	}
}
