package rawdb

import (
	"bytes"
	"encoding/binary"
	"testing"

	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/types"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	"google.golang.org/protobuf/proto"
)

// fakeAncient is a deterministic, in-memory AncientReader that lets the
// slice-2 fall-through tests assert "the ancient table was consulted"
// without spinning up a real freezer on disk. Slice 3 (which writes to
// the freezer) is the canonical write-side test; here we only need the
// read side and a way to seed canned bytes per kind/number.
type fakeAncient struct {
	rows map[string]map[uint64][]byte
}

func newFakeAncient() *fakeAncient {
	return &fakeAncient{rows: make(map[string]map[uint64][]byte)}
}

func (f *fakeAncient) put(kind string, num uint64, data []byte) {
	tbl, ok := f.rows[kind]
	if !ok {
		tbl = make(map[uint64][]byte)
		f.rows[kind] = tbl
	}
	tbl[num] = data
}

func (f *fakeAncient) Ancient(kind string, number uint64) ([]byte, error) {
	tbl, ok := f.rows[kind]
	if !ok {
		return nil, ErrNotInAncient
	}
	data, ok := tbl[number]
	if !ok {
		return nil, ErrNotInAncient
	}
	return data, nil
}

func (f *fakeAncient) AncientRange(kind string, start, count, maxBytes uint64) ([][]byte, error) {
	tbl, ok := f.rows[kind]
	if !ok {
		return nil, ErrNotInAncient
	}
	if _, ok := tbl[start]; !ok {
		return nil, ErrNotInAncient
	}
	var out [][]byte
	var total uint64
	for i := uint64(0); i < count; i++ {
		row, ok := tbl[start+i]
		if !ok {
			break
		}
		if maxBytes > 0 && total+uint64(len(row)) > maxBytes && len(out) > 0 {
			break
		}
		out = append(out, row)
		total += uint64(len(row))
	}
	return out, nil
}

func (f *fakeAncient) AncientCount(kind string) (uint64, error) {
	tbl, ok := f.rows[kind]
	if !ok {
		return 0, nil
	}
	// Count is "first gap" for contiguous fakes; this is enough for the
	// fall-through tests, which only ever seed a single row.
	var n uint64
	for {
		if _, ok := tbl[n]; !ok {
			return n, nil
		}
		n++
	}
}

func (f *fakeAncient) HasAncient(kind string, number uint64) (bool, error) {
	tbl, ok := f.rows[kind]
	if !ok {
		return false, nil
	}
	_, ok = tbl[number]
	return ok, nil
}

// newBlockProto builds a minimal *corepb.Block at the given number whose
// hash is deterministic. The slice-2 tests don't care about transaction
// content; they need the proto to round-trip through ReadBlock and to
// have a stable Hash() so the bsr-<hash> ↔ state-root path can be
// exercised.
func newBlockProto(num uint64, ts int64) *corepb.Block {
	return &corepb.Block{
		BlockHeader: &corepb.BlockHeader{
			RawData: &corepb.BlockHeaderRaw{
				Number:    int64(num),
				Timestamp: ts,
			},
		},
	}
}

func TestReadBlockHashRawMatchesCanonicalBlockHash(t *testing.T) {
	pb := newBlockProto(77, 123456)
	pb.Transactions = []*corepb.Transaction{{
		RawData: &corepb.TransactionRaw{Data: bytes.Repeat([]byte{0xab}, 1024)},
	}}
	block := types.NewBlockFromPB(pb)
	data, err := block.Marshal()
	if err != nil {
		t.Fatal(err)
	}
	if got, want := ReadBlockHashRaw(data), block.Hash(); got != want {
		t.Fatalf("ReadBlockHashRaw = %x, want %x", got, want)
	}
}

// TestReadBlock_AncientFallthrough verifies that ReadBlock prefers the
// freezer when an entry exists at the requested number, even when the
// KV side is empty.
func TestReadBlock_AncientFallthrough(t *testing.T) {
	t.Parallel()

	pb := newBlockProto(7, 12345)
	block := types.NewBlockFromPB(pb)
	data, err := block.Marshal()
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	anc := newFakeAncient()
	anc.put(ancientBlocks, 7, data)

	cdb := NewChainDB(NewMemoryDatabase(), anc)
	got := ReadBlock(cdb, 7)
	if got == nil {
		t.Fatal("ReadBlock returned nil; expected ancient hit")
	}
	if got.Number() != 7 {
		t.Fatalf("number: got %d, want 7", got.Number())
	}
	if got.Hash() != block.Hash() {
		t.Fatalf("hash: got %x, want %x", got.Hash(), block.Hash())
	}
}

func TestReadBlockHash_AncientLegacyFallthrough(t *testing.T) {
	t.Parallel()

	block := types.NewBlockFromPB(newBlockProto(7, 12345))
	data, err := block.Marshal()
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	anc := newFakeAncient()
	anc.put(ancientBlocks, 7, data)
	cdb := NewChainDB(NewMemoryDatabase(), anc)

	got, ok := ReadBlockHash(cdb, 7)
	if !ok || got != block.Hash() {
		t.Fatalf("ReadBlockHash ancient = %x,%v want %x,true", got, ok, block.Hash())
	}
}

// TestReadBlock_KVPath verifies that ReadBlock reads from the hot KV
// store when no ancient entry exists (the slice-2 default with
// NoopAncient).
func TestReadBlock_KVPath(t *testing.T) {
	t.Parallel()

	cdb := NewMemoryChainDB()
	pb := newBlockProto(11, 22222)
	block := types.NewBlockFromPB(pb)
	if err := WriteBlock(cdb, block); err != nil {
		t.Fatalf("WriteBlock: %v", err)
	}

	got := ReadBlock(cdb, 11)
	if got == nil {
		t.Fatal("ReadBlock returned nil; expected KV hit")
	}
	if got.Number() != 11 {
		t.Fatalf("number: got %d, want 11", got.Number())
	}
	if got.Hash() != block.Hash() {
		t.Fatalf("hash: got %x, want %x", got.Hash(), block.Hash())
	}
}

// TestReadBlockNumber_KVOnly confirms ReadBlockNumber is KV-only — slice
// 1 of the freezer spec keeps `bh-<hash>` hot, so an ancient hit must
// not satisfy this read.
func TestReadBlockNumber_KVOnly(t *testing.T) {
	t.Parallel()

	cdb := NewMemoryChainDB()
	pb := newBlockProto(3, 9999)
	block := types.NewBlockFromPB(pb)
	if err := WriteBlock(cdb, block); err != nil {
		t.Fatalf("WriteBlock: %v", err)
	}

	got := ReadBlockNumber(cdb, block.Hash())
	if got == nil || *got != 3 {
		t.Fatalf("KV path: got %v, want *3", got)
	}

	// Unknown hash returns nil even when an ancient is attached.
	anc := newFakeAncient()
	// Seed something under `bodies` to prove that even if the freezer is
	// populated with the same number, the reverse-index accessor still
	// returns nil for an unknown hash.
	anc.put(ancientBlocks, 3, []byte("dummy"))
	cdb2 := NewChainDB(NewMemoryDatabase(), anc)
	if got := ReadBlockNumber(cdb2, block.Hash()); got != nil {
		t.Fatalf("unknown-hash with ancient populated: want nil, got *%d", *got)
	}
}

// TestReadTransactionInfosByBlock_AncientFallthrough verifies the
// tx-infos accessor consults the freezer first when the block is below
// the cutoff.
func TestReadTransactionInfosByBlock_AncientFallthrough(t *testing.T) {
	t.Parallel()

	infos := []*corepb.TransactionInfo{
		{Id: bytes.Repeat([]byte{0x01}, 32), Fee: 11, BlockNumber: 5, BlockTimeStamp: 1000},
		{Id: bytes.Repeat([]byte{0x02}, 32), Fee: 22, BlockNumber: 5, BlockTimeStamp: 1000},
	}
	ret := &corepb.TransactionRet{
		BlockNumber:     5,
		BlockTimeStamp:  1000,
		Transactioninfo: infos,
	}
	data, err := proto.Marshal(ret)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	anc := newFakeAncient()
	anc.put(ancientTxInfos, 5, data)
	cdb := NewChainDB(NewMemoryDatabase(), anc)

	got := ReadTransactionInfosByBlock(cdb, 5)
	if len(got) != 2 {
		t.Fatalf("len: got %d, want 2", len(got))
	}
	if got[0].Fee != 11 || got[1].Fee != 22 {
		t.Fatalf("fees: got %d/%d, want 11/22", got[0].Fee, got[1].Fee)
	}
}

// TestReadTransactionInfosByBlock_KVPath verifies the same accessor
// reads from Pebble when no ancient row exists.
func TestReadTransactionInfosByBlock_KVPath(t *testing.T) {
	t.Parallel()

	cdb := NewMemoryChainDB()
	infos := []*corepb.TransactionInfo{
		{Id: bytes.Repeat([]byte{0xAA}, 32), Fee: 99, BlockNumber: 12, BlockTimeStamp: 4000},
	}
	WriteTransactionInfosByBlock(cdb, 12, infos)

	got := ReadTransactionInfosByBlock(cdb, 12)
	if len(got) != 1 || got[0].Fee != 99 {
		t.Fatalf("KV path: got %#v", got)
	}
}

// TestReadTransactionInfo_KVOnly proves the per-tx index accessor never
// consults the freezer (slice 1 leaves `ti-<txid>` hot).
func TestReadTransactionInfo_KVOnly(t *testing.T) {
	t.Parallel()

	cdb := NewMemoryChainDB()
	txID := bytes.Repeat([]byte{0xBB}, 32)
	info := &corepb.TransactionInfo{Id: txID, Fee: 77}
	WriteTransactionInfo(cdb, txID, info)

	got := ReadTransactionInfo(cdb, txID)
	if got == nil || got.Fee != 77 {
		t.Fatalf("got %#v", got)
	}
}

// TestReadTransactionIndex_KVOnly mirrors TestReadTransactionInfo_KVOnly
// for the tx-hash → block-number reverse index.
func TestReadTransactionIndex_KVOnly(t *testing.T) {
	t.Parallel()

	cdb := NewMemoryChainDB()
	txHash := bytes.Repeat([]byte{0xCC}, 32)
	WriteTransactionIndex(cdb, txHash, 42)

	got := ReadTransactionIndex(cdb, txHash)
	if got == nil || *got != 42 {
		t.Fatalf("got %v", got)
	}
}

// TestReadBlockStateRoot_AncientFallthrough exercises the two-step
// hash → num → state_roots[num] fall-through path. The KV side is
// missing the bsr-<hash> row; the bh-<hash> reverse index resolves
// to a number whose state_roots[num] entry lives in the freezer.
func TestReadBlockStateRoot_AncientFallthrough(t *testing.T) {
	t.Parallel()

	pb := newBlockProto(9, 0)
	block := types.NewBlockFromPB(pb)
	hash := block.Hash()
	want := common.HexToHash("aabbccddeeff00112233445566778899aabbccddeeff00112233445566778899")

	kv := NewMemoryDatabase()
	// Seed the still-hot bh-<hash> reverse index (slice 1 keeps it in KV).
	numBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(numBytes, 9)
	if err := kv.Put(blockHashKey(hash.Bytes()), numBytes); err != nil {
		t.Fatalf("put bh: %v", err)
	}

	anc := newFakeAncient()
	anc.put(ancientStateRoots, 9, want.Bytes())

	cdb := NewChainDB(kv, anc)
	got := ReadBlockStateRoot(cdb, hash)
	if got != want {
		t.Fatalf("ancient state root: got %x, want %x", got, want)
	}
}

// TestReadBlockStateRoot_KVPath proves the KV side is preferred when
// the hot bsr-<hash> row exists, even with an ancient row present (so
// any future race during slice-3 freezing won't accidentally serve
// stale ancient data).
func TestReadBlockStateRoot_KVPath(t *testing.T) {
	t.Parallel()

	pb := newBlockProto(4, 0)
	block := types.NewBlockFromPB(pb)
	hot := common.HexToHash("1111111111111111111111111111111111111111111111111111111111111111")
	cold := common.HexToHash("2222222222222222222222222222222222222222222222222222222222222222")

	kv := NewMemoryDatabase()
	if err := kv.Put(blockStateRootKey(block.Hash().Bytes()), hot.Bytes()); err != nil {
		t.Fatalf("put bsr: %v", err)
	}
	// Even if ancient has a (different) state root at the same num, KV wins.
	numBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(numBytes, 4)
	if err := kv.Put(blockHashKey(block.Hash().Bytes()), numBytes); err != nil {
		t.Fatalf("put bh: %v", err)
	}
	anc := newFakeAncient()
	anc.put(ancientStateRoots, 4, cold.Bytes())

	cdb := NewChainDB(kv, anc)
	got := ReadBlockStateRoot(cdb, block.Hash())
	if got != hot {
		t.Fatalf("hot KV state root: got %x, want %x", got, hot)
	}
}

// TestReadBlockStateRoot_Missing returns the zero hash when neither
// store has the requested entry.
func TestReadBlockStateRoot_Missing(t *testing.T) {
	t.Parallel()

	cdb := NewMemoryChainDB()
	got := ReadBlockStateRoot(cdb, common.HexToHash("dead"))
	if got != (common.Hash{}) {
		t.Fatalf("expected zero hash, got %x", got)
	}
}

// TestReadBlock_AncientCorrupt confirms a malformed ancient blob is
// surfaced as "not found" (nil) rather than panicking; matches the
// pre-slice-2 accessor contract.
func TestReadBlock_AncientCorrupt(t *testing.T) {
	t.Parallel()

	anc := newFakeAncient()
	anc.put(ancientBlocks, 0, []byte("not-a-valid-proto"))
	cdb := NewChainDB(NewMemoryDatabase(), anc)
	if got := ReadBlock(cdb, 0); got != nil {
		t.Fatalf("expected nil for corrupt ancient blob, got %#v", got)
	}
}
