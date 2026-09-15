package rawdb

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"math/rand"
	"testing"

	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/state/kvdomains"
	"github.com/tronprotocol/go-tron/internal/historychunk"
)

// Exact-format fixtures retain physical heights. These deliberately controlled
// patterns are not a replay of production CDC boundaries or a measured hit rate.
func ownedReadBenchmarkPack(raw []byte, block uint64, compress bool) ([]byte, map[string][]byte) {
	pack := append([]byte(nil), stateDomainChangeBlockEnvelopeMagic[:]...)
	pack = append(pack, stateDomainChangeBlockSharedVersion)
	pack = binary.AppendUvarint(pack, block)
	pack = binary.AppendUvarint(pack, uint64(len(raw)))
	digest := sha256.Sum256(raw)
	pack = append(pack, digest[:]...)
	count := (len(raw) + historychunk.MaxSize - 1) / historychunk.MaxSize
	pack = binary.AppendUvarint(pack, uint64(count))
	values := make(map[string][]byte)
	for offset := 0; offset < len(raw); offset += historychunk.MaxSize {
		end := min(offset+historychunk.MaxSize, len(raw))
		chunk := raw[offset:end]
		hash := sha256.Sum256(chunk)
		pack = binary.AppendUvarint(pack, uint64(len(chunk)))
		pack = append(pack, hash[:]...)
		var value []byte
		if compress {
			value = encodeStateHistorySharedChunk(chunk)
		} else {
			value = binary.AppendUvarint([]byte{1, 0}, uint64(len(chunk)))
			value = append(value, chunk...)
		}
		values[string(stateHistoryChunkKey(stateHistoryChunkBucket(block), hash))] = value
	}
	return pack, values
}

func ownedReadBenchmarkFixture(t testing.TB, shape string) (ethdb.KeyValueStore, uint64, uint64, uint64) {
	t.Helper()
	db, err := NewPebbleDB(t.TempDir(), 32, 16)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	blocks := 16
	size := 512 << 10
	start := uint64(2050)
	compress := false
	if shape == "OverBudget" {
		size = 2 << 20
	}
	if shape == "BucketBoundary" {
		start = 2040
	}
	if shape == "HighReuseSnappy" {
		compress = true
	}
	rng := rand.New(rand.NewSource(20260915))
	base := make([]byte, size)
	_, _ = rng.Read(base)
	if compress {
		for i := range base {
			base[i] = byte(i % 251)
		}
	}
	total := uint64(0)
	for i := 0; i < blocks; i++ {
		prev := bytes.Clone(base)
		if shape == "Unique" || shape == "OverBudget" || shape == "LowReuse" && i%4 != 0 {
			_, _ = rng.Read(prev)
		}
		block := start + uint64(i)
		row := &StateDomainChange{BlockNum: block, Seq: 1, TxNum: block, FlatDomain: StateFlatDomainKVLatest, Owner: common.Address{0x41, 9}, Generation: 7, Domain: kvdomains.SystemDelegation, Key: []byte("memo-replay"), PrevExists: true, Prev: prev}
		raw := encodeBorrowedStateDomainChangeTestBlock(t, []*StateDomainChange{row})
		pack, values := ownedReadBenchmarkPack(raw, block, compress)
		total += uint64(len(raw))
		batch := db.NewBatch()
		for key, value := range values {
			if err = batch.Put([]byte(key), value); err != nil {
				t.Fatal(err)
			}
		}
		if err = batch.Put(stateChangeSetKey(block, 0), pack); err != nil {
			t.Fatal(err)
		}
		if err = WriteStateTxRange(batch, block, common.Hash{byte(i + 1)}, block, block); err != nil {
			t.Fatal(err)
		}
		if err = batch.Write(); err != nil {
			t.Fatal(err)
		}
	}
	return db, start, start + uint64(blocks) - 1, total
}

// OwnedReadBenchmarkFixtureForTest exposes only the deterministic test fixture
// to the external-package complete cold builder benchmark.
func OwnedReadBenchmarkFixtureForTest(t testing.TB, shape string) (ethdb.KeyValueStore, uint64, uint64, uint64) {
	return ownedReadBenchmarkFixture(t, shape)
}
