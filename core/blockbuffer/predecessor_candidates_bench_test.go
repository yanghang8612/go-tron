package blockbuffer

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math/bits"

	"github.com/cespare/xxhash/v2"
	"testing"

	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/pointread"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/state/kvdomains"
)

// This exact harness is copied to actual parent 9030ce7. It does not call any
// candidate-only function. Each op is one shielded root predecessor, never an
// independent-root batch. Shapes are frozen before collecting performance data. This measures only
// the local overlay plus 512 unrelated-write maintenance, not the full Pebble
// or SaveCurrentAsBest stack.
type predecessorBenchBase struct {
	ethdb.KeyValueStore
	gets, iterators, seeks int
}

func (b *predecessorBenchBase) Get(key []byte) ([]byte, error) {
	b.gets++
	return b.KeyValueStore.Get(key)
}
func (b *predecessorBenchBase) NewIterator(prefix, start []byte) ethdb.Iterator {
	b.iterators++
	return b.KeyValueStore.NewIterator(prefix, start)
}
func (b *predecessorBenchBase) SeekPrefixBefore(prefix, upper []byte) ([]byte, []byte, bool, error) {
	b.seeks++
	return pointread.SeekBefore(struct{ ethdb.Iteratee }{b}, prefix, upper)
}

func predecessorBenchBuilds(b *Buffer, reset bool) [64]*string {
	var pointers [64]*string
	at := 0
	v := b.loadReadView()
	for _, list := range [][]*layer{v.layers, v.inflight} {
		for _, l := range list {
			for i := range l.shards {
				s := &l.shards[i]
				s.mu.Lock()
				if s.prefixBucketIndex != nil && s.prefixBucketIndex.iteratorKeysBuilt {
					if len(s.prefixBucketIndex.iteratorKeys) > 0 {
						pointers[at] = &s.prefixBucketIndex.iteratorKeys[0]
					}
				}
				if reset {
					s.prefixBucketIndex = nil
				}
				s.mu.Unlock()
				at++
			}
		}
	}
	return pointers
}

// The commutative two-word checksum verifies the entire active layer's key and
// value input after every churn restoration. Map iteration order is irrelevant.
// Both revisions execute exactly this fixture code outside the timer.
type predecessorBenchDigest struct {
	writes, deletes int
	sum, mixed      uint64
}

func predecessorBenchLayerDigest(l *layer) (d predecessorBenchDigest) {
	for n := range l.shards {
		s := &l.shards[n]
		s.mu.RLock()
		for key, value := range s.writes {
			h := xxhash.Sum64String(key)
			v := xxhash.Sum64(value)
			d.writes++
			d.sum += h
			d.mixed ^= bits.RotateLeft64(h, int(v&63)) ^ v
		}
		for key := range s.deletes {
			h := xxhash.Sum64String(key)
			d.deletes++
			d.sum += h
			d.mixed ^= bits.RotateLeft64(h, 31) ^ 0xf00df00d
		}
		s.mu.RUnlock()
	}
	return
}
func predecessorBenchRestoreChurn(l *layer, keys [][]byte) {
	for _, key := range keys {
		s := l.shardForBytes(key)
		s.mu.Lock()
		delete(s.writes, string(key))
		delete(s.deletes, string(key))
		delete(s.durableWrites, string(key))
		s.mu.Unlock()
	}
	// Remove the entire index holder so no built/sorted state can leak between
	// samples. The next untimed warm read restores each revision's ready state.
	for n := range l.shards {
		s := &l.shards[n]
		s.mu.Lock()
		s.prefixBucketIndex = nil
		s.mu.Unlock()
	}
}

func BenchmarkPredecessorCandidates(b *testing.B) {
	for _, mib := range []int{8, 128} {
		for _, mode := range []string{"cold", "churn", "hotstable", "smallprefix", "tombstones", "generic"} {
			b.Run(fmt.Sprintf("%dMiB/%s", mib, mode), func(b *testing.B) {
				const valueBytes = 256
				base := &predecessorBenchBase{KeyValueStore: rawdb.NewMemoryDatabase()}
				defer base.Close()
				buf := New(base)
				buf.SetMaxInflight(4)
				prefix := rawdb.StateKVLatestCommitmentKey(common.SystemAccountAddress, 0, kvdomains.SystemShielded, rawdb.ShieldedRootChangePrefix())
				upper := rawdb.StateKVLatestCommitmentKey(common.SystemAccountAddress, 0, kvdomains.SystemShielded, rawdb.ShieldedRootChangeStateKey(1000000))
				rootKey := rawdb.StateKVLatestCommitmentKey(common.SystemAccountAddress, 0, kvdomains.SystemShielded, rawdb.ShieldedRootChangeStateKey(1))
				if mode == "generic" {
					prefix = []byte("generic/root/")
					upper = []byte("generic/root/999")
					rootKey = []byte("generic/root/001")
				}
				rootValue := rawdb.EncodeStateKVLatestValue(bytes.Repeat([]byte{0x45}, 32))
				if err := base.Put(rootKey, rootValue); err != nil {
					b.Fatal(err)
				}
				if mode == "tombstones" {
					for n := uint64(2); n <= 97; n++ {
						if err := base.Put(rawdb.StateKVLatestCommitmentKey(common.SystemAccountAddress, 0, kvdomains.SystemShielded, rawdb.ShieldedRootChangeStateKey(n)), rootValue); err != nil {
							b.Fatal(err)
						}
					}
				}
				payload := mib << 20
				if mode == "smallprefix" {
					payload = 64 * valueBytes
				}
				rows := payload / valueBytes
				value := bytes.Repeat([]byte{0x62}, valueBytes)
				for ln := 0; ln < 4; ln++ {
					buf.BeginBlock(bufHash(byte(ln+1)), uint64(ln+1))
					for n := ln; n < rows; n += 4 {
						var owner common.Address
						owner[0] = 0x41
						owner[1] = byte(n)
						binary.BigEndian.PutUint64(owner[13:], uint64(n))
						var logical [8]byte
						binary.BigEndian.PutUint64(logical[:], uint64(n))
						key := rawdb.StateKVLatestCommitmentKey(owner, 0, kvdomains.ContractStorage, logical[:])
						if err := buf.Put(key, value); err != nil {
							b.Fatal(err)
						}
					}
					if mode == "tombstones" && ln == 3 {
						for n := uint64(2); n <= 97; n++ {
							buf.Delete(rawdb.StateKVLatestCommitmentKey(common.SystemAccountAddress, 0, kvdomains.SystemShielded, rawdb.ShieldedRootChangeStateKey(n)))
						}
					}
					if ln < 3 {
						buf.CommitBlock()
					}
				}
				if mode != "cold" {
					if _, _, ok, err := buf.SeekPrefixBefore(prefix, upper); err != nil || !ok {
						b.Fatalf("warmup %t/%v", ok, err)
					}
				}
				active := buf.loadReadView().inflight[0]
				var freshKeys [][]byte
				if mode == "churn" {
					for j := 0; j < 512; j++ {
						var owner common.Address
						owner[0] = 0x41
						owner[1] = byte(j)
						binary.BigEndian.PutUint64(owner[13:], uint64(rows+j))
						var logical [8]byte
						binary.BigEndian.PutUint64(logical[:], uint64(rows+j))
						freshKeys = append(freshKeys, rawdb.StateKVLatestCommitmentKey(owner, 0, kvdomains.ContractStorage, logical[:]))
					}
				}
				initialDigest := predecessorBenchLayerDigest(active)
				gets, iterators, seeks := 0, 0, 0
				builds := 0
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					b.StopTimer()
					if mode == "cold" {
						predecessorBenchBuilds(buf, true)
					}
					before := predecessorBenchBuilds(buf, false)
					base.gets, base.iterators, base.seeks = 0, 0, 0
					b.StartTimer()
					if mode == "churn" {
						// The same actual public mutation API in both arms includes
						// all new bucket-sorted invalidation maintenance costs.
						for _, key := range freshKeys {
							if err := buf.Put(key, value); err != nil {
								b.Fatal(err)
							}
						}
					}
					k, v, ok, err := buf.SeekPrefixBefore(prefix, upper)
					if err != nil || !ok || !bytes.Equal(k, rootKey) || !bytes.Equal(v, rootValue) {
						b.Fatalf("got %x/%x/%t/%v", k, v, ok, err)
					}
					b.StopTimer()
					gets += base.gets
					iterators += base.iterators
					seeks += base.seeks
					after := predecessorBenchBuilds(buf, false)
					for j, ptr := range after {
						if ptr != nil && ptr != before[j] {
							builds++
						}
					}
					if mode == "churn" {
						predecessorBenchRestoreChurn(active, freshKeys)
						if got := predecessorBenchLayerDigest(active); got != initialDigest {
							b.Fatalf("churn restore changed input: got %+v want %+v", got, initialDigest)
						}
						if _, _, ok, err := buf.SeekPrefixBefore(prefix, upper); err != nil || !ok {
							b.Fatalf("restore warmup %t/%v", ok, err)
						}
					}
					b.StartTimer()
				}
				b.StopTimer()
				b.ReportMetric(float64(builds)/float64(b.N), "fullsorts/op")
				b.ReportMetric(float64(gets)/float64(b.N), "baseGets/op")
				b.ReportMetric(float64(iterators)/float64(b.N), "baseIters/op")
				b.ReportMetric(float64(seeks)/float64(b.N), "baseSeeks/op")
			})
		}
	}
}
