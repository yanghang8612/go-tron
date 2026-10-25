package snapshots

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"hash/crc32"
	"math"
	"math/rand"
	"os"
	"path/filepath"
	"sort"
	"testing"
)

func txTableFixture(t *testing.T, txs []uint64) ([]byte, *historyTxTable) {
	t.Helper()
	b, err := newHistoryTxBuilder(t.TempDir(), "test")
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	for i, tx := range txs {
		if err := b.Add(tx, uint32(i)); err != nil {
			t.Fatal(err)
		}
	}
	var out bytes.Buffer
	out.Write(make([]byte, 152))
	if err := b.AppendTo(&out, 152, false); err != nil {
		t.Fatal(err)
	}
	h := stateDomainChangeBinaryAccessorV6Header{fromTxNum: 0, toTxNum: math.MaxUint64, recordCount: uint64(len(txs))}
	table, err := decodeHistoryTxFooter(bytes.NewReader(out.Bytes()), uint64(out.Len()), h)
	if err != nil {
		t.Fatal(err)
	}
	return out.Bytes(), table
}
func TestHistoryTxRunsBoundariesAndOracle(t *testing.T) {
	for _, runs := range []int{0, 1, 255, 256, 257, 600} {
		rng := rand.New(rand.NewSource(int64(runs)))
		var txs []uint64
		tx := uint64(0)
		for i := 0; i < runs; i++ {
			tx += uint64(rng.Intn(9) + 1)
			for j := 0; j < 1+rng.Intn(7); j++ {
				txs = append(txs, tx)
			}
		}
		_, table := txTableFixture(t, txs)
		if err := table.Check(); err != nil {
			t.Fatal(err)
		}
		for i, want := range txs {
			got, err := table.TxNum(uint32(i))
			if err != nil || got != want {
				t.Fatalf("ordinal %d: %d/%d %v", i, got, want, err)
			}
		}
		for from := uint64(0); from <= tx+1; from++ {
			to := min(tx+2, from+11)
			a, b, err := table.Bounds(from, to)
			wa := sort.Search(len(txs), func(i int) bool { return txs[i] >= from })
			wb := sort.Search(len(txs), func(i int) bool { return txs[i] > to })
			if err != nil || a != uint64(wa) || b != uint64(wb) {
				t.Fatalf("runs=%d range=%d..%d: %d/%d want%d/%d %v", runs, from, to, a, b, wa, wb, err)
			}
		}
		if _, err := table.TxNum(uint32(len(txs))); err == nil {
			t.Fatal("accepted out-of-range ordinal")
		}
	}
	_, table := txTableFixture(t, []uint64{math.MaxUint64, math.MaxUint64})
	a, b, err := table.Bounds(math.MaxUint64, math.MaxUint64)
	if err != nil || a != 0 || b != 2 {
		t.Fatalf("MaxUint64 range %d %d %v", a, b, err)
	}
}
func TestHistoryTxRunsCorruptionAndRouting(t *testing.T) {
	txs := make([]uint64, 600)
	for i := range txs {
		txs[i] = uint64(i * 3)
	}
	raw, table := txTableFixture(t, txs)
	h := stateDomainChangeBinaryAccessorV6Header{toTxNum: math.MaxUint64, recordCount: 600}
	for _, at := range []int{len(raw) - 64, len(raw) - 4, int(table.prefix + table.dataLen), len(raw) - 12} {
		bad := append([]byte(nil), raw...)
		bad[at] ^= 1
		if _, err := decodeHistoryTxFooter(bytes.NewReader(bad), uint64(len(bad)), h); err == nil {
			t.Fatalf("accepted corrupt directory/footer at%d", at)
		}
	}
	bad := append([]byte(nil), raw...)
	bad[table.prefix] ^= 1
	v, err := decodeHistoryTxFooter(bytes.NewReader(bad), uint64(len(bad)), h)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = v.TxNum(0); err == nil {
		t.Fatal("accepted corrupt page")
	}
	// Even with refreshed root CRC, invalid ordinal coverage cannot route reads.
	bad = append([]byte(nil), raw...)
	base := table.prefix + table.dataLen
	binary.BigEndian.PutUint32(bad[base+8:base+12], 1)
	footer := bad[len(bad)-64:]
	binary.BigEndian.PutUint32(footer[52:56], crc32.ChecksumIEEE(bad[base:uint64(len(bad))-64]))
	binary.BigEndian.PutUint32(footer[60:], crc32.ChecksumIEEE(footer[:60]))
	if _, err := decodeHistoryTxFooter(bytes.NewReader(bad), uint64(len(bad)), h); err == nil {
		t.Fatal("accepted invalid authenticated ordinal directory")
	}
	for _, n := range []int{0, 63, len(raw) - 1} {
		if _, err := decodeHistoryTxFooter(bytes.NewReader(raw[:n]), uint64(n), h); err == nil {
			t.Fatal("accepted truncation")
		}
	}
}
func TestHistoryTxRunsCacheAndOrderBounds(t *testing.T) {
	txs := make([]uint64, 180000)
	for i := range txs {
		txs[i] = uint64(i + 1)
	}
	_, table := txTableFixture(t, txs)
	for i := range table.pages {
		if _, err := table.page(i); err != nil {
			t.Fatal(err)
		}
		if table.cacheBytes > historyTxDecodedCacheLimit {
			t.Fatal("decoded cache exceeded bound")
		}
	}
	for _, i := range []uint32{0, 256, 179999} {
		got, err := table.TxNum(i)
		if err != nil || got != uint64(i+1) {
			t.Fatalf("eviction altered result %d %v", got, err)
		}
	}
	b, err := newHistoryTxBuilder(t.TempDir(), "order")
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	if err := b.Add(2, 0); err != nil {
		t.Fatal(err)
	}
	if b.Add(1, 1) == nil || b.Add(3, 2) == nil {
		t.Fatal("accepted invalid chronology")
	}
}

func TestHistoryTxRunsStrictCoverageRejectsForgedTransactions(t *testing.T) {
	dir := t.TempDir()
	db := rawdb.NewMemoryDatabase()
	for block := uint64(1); block <= 140; block++ {
		var hash common.Hash
		hash[31] = byte(block)
		if err := rawdb.WriteStateTxRange(db, block, hash, 3*block, 3*block); err != nil {
			t.Fatal(err)
		}
		if err := rawdb.WriteStateDomainChange(db, &rawdb.StateDomainChange{BlockNum: block, BlockHash: hash, TxNum: 3 * block, Seq: 1, FlatDomain: rawdb.StateFlatDomainAccountLatest, Owner: common.Address{0: common.AddressPrefixMainnet, 20: byte(min(block, 139))}, PrevExists: true, Prev: []byte{byte(block)}}); err != nil {
			t.Fatal(err)
		}
	}
	refs, err := BuildStateDomainChangeHistorySegmentsFromDB(db, dir, 3, 420, "history/state-domain-change-v11-forged.seg")
	if err != nil {
		t.Fatal(err)
	}
	var history, index, accessor SegmentRef
	for _, r := range refs {
		switch r.Kind {
		case SegmentHistory:
			history = r
		case SegmentInverted:
			index = r
		case SegmentAccessor:
			accessor = r
		}
	}
	verify := func() error {
		return verifyStateDomainChangeBinaryCompanionsAgainstSegmentContext(context.Background(), dir, history, index, accessor)
	}
	if err := verify(); err != nil {
		t.Fatal("valid V11", err)
	}
	path := filepath.Join(dir, accessor.Path)
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	h, err := decodeStateDomainChangeBinaryAccessorKeyHeader(bytes.NewReader(data), uint64(len(data)))
	if err != nil || h.txRuns == nil {
		t.Fatal(h, err)
	}
	tx := h.txRuns
	page := tx.pages[0]
	payload := data[tx.prefix+page.off : tx.prefix+page.off+uint64(page.length)]
	// Three-byte TxNum gaps allow a single run to move while preserving all
	// monotone routing and row counts. Both page and root CRCs are regenerated.
	if payload[0] != 0 || payload[2] != 3 || payload[4] != 3 {
		t.Fatal("unexpected fixture")
	}
	payload[2] = 4
	payload[4] = 2
	entry := data[tx.prefix+tx.dataLen : tx.prefix+tx.dataLen+32]
	binary.BigEndian.PutUint32(entry[28:], historyTxPageCRC(0, entry, payload))
	footer := data[len(data)-64:]
	binary.BigEndian.PutUint32(footer[52:56], crc32.ChecksumIEEE(data[tx.prefix+tx.dataLen:uint64(len(data))-64]))
	binary.BigEndian.PutUint32(footer[60:], crc32.ChecksumIEEE(footer[:60]))
	if err := checkStateDomainChangeBinaryAccessorV7(bytes.NewReader(data), uint64(len(data))); err != nil {
		t.Fatal("structurally valid forgery", err)
	}
	if err := os.WriteFile(path, data, 0600); err != nil {
		t.Fatal(err)
	}
	accessor.Checksum = fmt.Sprintf("sha256:%x", sha256.Sum256(data))
	accessor.Size = uint64(len(data))
	if err := verify(); err == nil {
		t.Fatal("forged transaction table accepted by joint history proof")
	}
}

func TestHistoryTxRunsSequentialIncludesSingletons(t *testing.T) {
	txs := make([]uint64, 4000)
	for i := range txs {
		txs[i] = uint64(i/3)*5 + 2
	}
	_, table := txTableFixture(t, txs)
	c := historyTxSequentialCursor{table: table}
	for i, want := range txs {
		got, err := c.Next(uint64(i))
		if err != nil || got != want {
			t.Fatal(i, got, want, err)
		}
	}
	if _, err := c.Next(uint64(len(txs))); err == nil {
		t.Fatal("accepted after end")
	}
	c = historyTxSequentialCursor{table: table}
	if _, err := c.Next(1); err == nil {
		t.Fatal("accepted skipped ordinal")
	}
}
