package blockbuffer

import (
	"bytes"
	"errors"
	"fmt"
	"math"
	"strings"
	"sync"
	"testing"

	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/types"
	corepb "github.com/tronprotocol/go-tron/proto/core"
)

func strictHashBlock(t testing.TB, number uint64, txs int, marker byte) ([]byte, common.Hash) {
	t.Helper()
	pb := &corepb.Block{BlockHeader: &corepb.BlockHeader{RawData: &corepb.BlockHeaderRaw{Number: int64(number), Timestamp: 123456, Version: 34, ParentHash: bytes.Repeat([]byte{marker}, 32)}}}
	for i := 0; i < txs; i++ {
		pb.Transactions = append(pb.Transactions, &corepb.Transaction{RawData: &corepb.TransactionRaw{Data: bytes.Repeat([]byte{byte(i)}, 132), Timestamp: int64(i + 1)}, Signature: [][]byte{bytes.Repeat([]byte{0xab}, 65)}})
	}
	block := types.NewBlockFromPB(pb)
	data, err := block.Marshal()
	if err != nil {
		t.Fatal(err)
	}
	return data, block.Hash()
}

func TestStrictBlockHashMemoReplacementAndRetirement(t *testing.T) {
	disk := rawdb.NewMemoryDatabase()
	buf := New(disk)
	buf.BeginBlock(bufHash(1), 1)
	raw, want := strictHashBlock(t, 100, 100, 1)
	key := rawdb.BlockStorageKey(100)
	if err := buf.Put(key, raw); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		got, found, err := buf.BlockHashByNumberStrict(100)
		if got != want || !found || err != nil {
			t.Fatalf("lookup = %x,%v,%v", got, found, err)
		}
	}
	cached := buf.strictBlockHashes.Load()
	if cached == nil {
		t.Fatal("missing successful memo")
	}
	// Cache owns its input even when the caller drops or mutates original bytes.
	raw[0] ^= 0xff
	if got, _, err := buf.BlockHashByNumberStrict(100); got != want || err != nil {
		t.Fatalf("caller mutation = %x,%v", got, err)
	}
	buf.CommitBlock()
	if err := buf.Flush(disk); err != nil {
		t.Fatal(err)
	}
	if got, _, err := buf.BlockHashByNumberStrict(100); got != want || err != nil {
		t.Fatalf("flushed memo = %x,%v", got, err)
	}
	buf.BeginBlock(bufHash(2), 2)
	replacement, newHash := strictHashBlock(t, 100, 100, 2)
	if err := buf.Put(key, replacement); err != nil {
		t.Fatal(err)
	}
	if got, _, err := buf.BlockHashByNumberStrict(100); got != newHash || err != nil {
		t.Fatalf("replacement = %x,%v", got, err)
	}
	// Same header + valid changed body refreshes the successful memo; malformed
	// transaction suffix must still be rejected despite the unchanged header.
	differentBody, _ := strictHashBlock(t, 100, 101, 2)
	if err := buf.Put(key, differentBody); err != nil {
		t.Fatal(err)
	}
	if got, _, err := buf.BlockHashByNumberStrict(100); got != newHash || err != nil {
		t.Fatalf("different body = %x,%v", got, err)
	}
	if !bytes.Equal(cached.entries[100].raw, differentBody) {
		t.Fatal("memo did not authenticate complete changed body")
	}
	malformed := append(append([]byte(nil), differentBody...), 0x0a, 0x01, 0xff)
	if err := buf.Put(key, malformed); err != nil {
		t.Fatal(err)
	}
	if got, found, err := buf.BlockHashByNumberStrict(100); err == nil || !found || got != (common.Hash{}) || !strings.Contains(err.Error(), "decode") {
		t.Fatalf("malformed tx = %x,%v,%v", got, found, err)
	}
	wrong, _ := strictHashBlock(t, 101, 0, 2)
	if err := buf.Put(key, wrong); err != nil {
		t.Fatal(err)
	}
	if _, found, err := buf.BlockHashByNumberStrict(100); err == nil || !found || !strings.Contains(err.Error(), "contains block number 101") {
		t.Fatalf("wrong number = %v,%v", found, err)
	}
	if err := buf.Delete(key); err != nil {
		t.Fatal(err)
	}
	if _, found, err := buf.BlockHashByNumberStrict(100); found || err != nil {
		t.Fatalf("tombstone = %v,%v", found, err)
	}
	buf.DiscardActive()
	if got, _, err := buf.BlockHashByNumberStrict(100); got != want || err != nil {
		t.Fatalf("rewind = %x,%v", got, err)
	}
	buf.BeginBlock(bufHash(3), 3)
	if err := buf.Put(key, replacement); err != nil {
		t.Fatal(err)
	}
	buf.CommitBlock()
	if got, _, err := buf.BlockHashByNumberStrict(100); got != newHash || err != nil {
		t.Fatalf("committed replacement = %x,%v", got, err)
	}
	buf.DiscardBlock(bufHash(3))
	if got, _, err := buf.BlockHashByNumberStrict(100); got != want || err != nil {
		t.Fatalf("committed rewind = %x,%v", got, err)
	}
}

type strictHashErrorDB struct {
	err      error
	hasError bool
}

func (d strictHashErrorDB) Has([]byte) (bool, error) {
	if d.hasError {
		return false, d.err
	}
	return true, nil
}
func (d strictHashErrorDB) Get([]byte) ([]byte, error) { return nil, d.err }

func TestStrictBlockHashMemoPreservesReadErrorsAndReaderReplacement(t *testing.T) {
	for _, hasError := range []bool{false, true} {
		sentinel := errors.New("block store error")
		b := New(strictHashErrorDB{sentinel, hasError})
		cache := new(strictBlockHashCache)
		cache.remember(100, []byte("previous"), common.Hash{1})
		b.strictBlockHashes.Store(cache)
		if _, _, err := b.BlockHashByNumberStrict(100); !errors.Is(err, sentinel) {
			t.Fatalf("hasError %v: %v", hasError, err)
		}
	}
	b := New(rawdb.NewMemoryDatabase())
	for _, want := range []common.Hash{{1}, {2}} {
		b.SetBlockHashReader(BlockHashReaderFunc(func(uint64) (common.Hash, bool) { return want, true }))
		if got, found, err := b.BlockHashByNumberStrict(100); got != want || !found || err != nil {
			t.Fatalf("reader = %x,%v,%v", got, found, err)
		}
	}
}

func TestStrictBlockHashMemoBudgetAndCollision(t *testing.T) {
	c := new(strictBlockHashCache)
	raw := bytes.Repeat([]byte{0xab}, strictBlockHashMaxRowBytes)
	for i := uint64(0); i < 512; i++ {
		c.remember(i, raw, common.Hash{byte(i)})
		if c.bytes > strictBlockHashMaxBytes {
			t.Fatalf("budget exceeded: %d", c.bytes)
		}
		actual := 0
		for _, e := range c.entries {
			actual += len(e.raw)
			if e.valid && len(e.raw) > strictBlockHashMaxRowBytes {
				t.Fatal("oversized entry")
			}
		}
		if actual != c.bytes {
			t.Fatalf("charge %d != actual %d", c.bytes, actual)
		}
	}
	for _, n := range []uint64{0, 256, math.MaxUint64, 255} {
		c.remember(n, []byte("valid"), common.Hash{byte(n)})
		if h, ok := c.lookup(n, []byte("valid")); !ok || h != (common.Hash{byte(n)}) {
			t.Fatalf("number %d = %x,%v", n, h, ok)
		}
		if _, ok := c.lookup(n^256, []byte("valid")); ok {
			t.Fatalf("slot collision returned number %d", n)
		}
	}
	c.remember(7, bytes.Repeat([]byte{1}, strictBlockHashMaxRowBytes+1), common.Hash{1})
	if _, ok := c.lookup(7, bytes.Repeat([]byte{1}, strictBlockHashMaxRowBytes+1)); ok {
		t.Fatal("oversized row cached")
	}
	input := []byte("input")
	c.remember(8, input, common.Hash{8})
	input[0] = 'X'
	if _, ok := c.lookup(8, []byte("input")); !ok {
		t.Fatal("memo retained caller bytes")
	}
}

func TestStrictBlockHashMemoConcurrentReplacement(t *testing.T) {
	b := New(rawdb.NewMemoryDatabase())
	b.BeginBlock(bufHash(1), 1)
	rows := make([][]byte, 2)
	allowed := make(map[common.Hash]bool)
	for i := range rows {
		var hash common.Hash
		rows[i], hash = strictHashBlock(t, 100, 100, byte(i))
		allowed[hash] = true
	}
	key := rawdb.BlockStorageKey(100)
	if err := b.Put(key, rows[0]); err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	for worker := 0; worker < 8; worker++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			for i := 0; i < 100; i++ {
				if worker == 0 {
					if err := b.Put(key, rows[i%2]); err != nil {
						t.Error(err)
					}
				} else {
					hash, found, err := b.BlockHashByNumberStrict(100)
					if err != nil || !found || !allowed[hash] {
						t.Errorf("concurrent lookup = %x,%v,%v", hash, found, err)
						return
					}
				}
			}
		}(worker)
	}
	wg.Wait()
}

var benchmarkStrictBlockHash common.Hash

func BenchmarkBufferStrictBlockHash(b *testing.B) {
	for _, location := range []string{"layer", "base"} {
		for _, mode := range []string{"full-decode", "memo-hit", "memo-cold"} {
			b.Run(location+"/"+mode, func(b *testing.B) {
				raw, _ := strictHashBlock(b, 100, 100, 1)
				disk := rawdb.NewMemoryDatabase()
				buf := New(disk)
				key := rawdb.BlockStorageKey(100)
				if location == "layer" {
					buf.BeginBlock(bufHash(1), 1)
					if err := buf.Put(key, raw); err != nil {
						b.Fatal(err)
					}
				} else {
					if err := disk.Put(key, raw); err != nil {
						b.Fatal(err)
					}
				}
				if mode != "full-decode" {
					if _, _, err := buf.BlockHashByNumberStrict(100); err != nil {
						b.Fatal(err)
					}
				}
				b.ReportAllocs()
				b.SetBytes(int64(len(raw)))
				b.ResetTimer()
				for b.Loop() {
					if mode == "full-decode" {
						data, ok, err := rawdb.ReadBlockRawStrict(buf, 100)
						if err != nil || !ok {
							b.Fatal(err)
						}
						block, err := types.UnmarshalBlock(data)
						if err != nil {
							b.Fatal(err)
						}
						benchmarkStrictBlockHash = block.Hash()
					} else {
						if mode == "memo-cold" {
							cache := buf.strictBlockHashes.Load()
							cache.mu.Lock()
							cache.entries[100] = strictBlockHashEntry{}
							cache.bytes = 0
							cache.mu.Unlock()
						}
						hash, found, err := buf.BlockHashByNumberStrict(100)
						if err != nil || !found {
							b.Fatal(fmt.Sprint(found, err))
						}
						benchmarkStrictBlockHash = hash
					}
				}
				b.StopTimer()
				if cache := buf.strictBlockHashes.Load(); cache != nil {
					b.ReportMetric(float64(cache.bytes), "memo-retained-B")
				} else {
					b.ReportMetric(0, "memo-retained-B")
				}
			})
		}
	}
}
