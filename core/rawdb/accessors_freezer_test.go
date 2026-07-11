package rawdb

import (
	"bytes"
	"encoding/binary"
	"errors"
	"strings"
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
	errs map[string]map[uint64]error
}

type fakeChainIndex struct {
	blocks    map[common.Hash]uint64
	txs       map[common.Hash]uint64
	positions map[common.Hash]ChainIndexTxLookup
	blockErr  error
}

func newFakeAncient() *fakeAncient {
	return &fakeAncient{
		rows: make(map[string]map[uint64][]byte),
		errs: make(map[string]map[uint64]error),
	}
}

func (f *fakeAncient) put(kind string, num uint64, data []byte) {
	tbl, ok := f.rows[kind]
	if !ok {
		tbl = make(map[uint64][]byte)
		f.rows[kind] = tbl
	}
	tbl[num] = data
}

func (f *fakeAncient) setErr(kind string, num uint64, err error) {
	tbl, ok := f.errs[kind]
	if !ok {
		tbl = make(map[uint64]error)
		f.errs[kind] = tbl
	}
	tbl[num] = err
}

func (f *fakeAncient) Ancient(kind string, number uint64) ([]byte, error) {
	if tbl, ok := f.errs[kind]; ok {
		if err := tbl[number]; err != nil {
			return nil, err
		}
	}
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
	if tbl, ok := f.errs[kind]; ok {
		if err := tbl[number]; err != nil {
			return false, err
		}
	}
	tbl, ok := f.rows[kind]
	if !ok {
		return false, nil
	}
	_, ok = tbl[number]
	return ok, nil
}

func (f *fakeChainIndex) BlockNumberByHash(hash common.Hash) (uint64, bool, error) {
	if f == nil {
		return 0, false, nil
	}
	if f.blockErr != nil {
		return 0, false, f.blockErr
	}
	num, ok := f.blocks[hash]
	return num, ok, nil
}

func (f *fakeChainIndex) TransactionBlockNumberByHash(hash common.Hash) (uint64, bool, error) {
	if f == nil {
		return 0, false, nil
	}
	num, ok := f.txs[hash]
	return num, ok, nil
}

func (f *fakeChainIndex) TransactionIndexByHash(hash common.Hash) (ChainIndexTxLookup, bool, error) {
	if f == nil {
		return ChainIndexTxLookup{}, false, nil
	}
	lookup, ok := f.positions[hash]
	return lookup, ok, nil
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

func TestReadBlockRaw_AncientFallthrough(t *testing.T) {
	t.Parallel()

	want := []byte("raw-block-7")
	anc := newFakeAncient()
	anc.put(ancientBlocks, 7, want)
	cdb := NewChainDB(NewMemoryDatabase(), anc)

	got := ReadBlockRaw(cdb, 7)
	if !bytes.Equal(got, want) {
		t.Fatalf("ReadBlockRaw = %q, want %q", got, want)
	}
}

func TestDeleteFrozenBlockRangeWithStateRoots(t *testing.T) {
	db := NewMemoryDatabase()
	blocks := make([]*types.Block, 0, 2)
	for number := uint64(0); number < 2; number++ {
		block := types.NewBlockFromPB(newBlockProto(number, int64(10_000+number)))
		if err := WriteBlock(db, block); err != nil {
			t.Fatalf("WriteBlock(%d): %v", number, err)
		}
		if err := WriteTransactionInfosRaw(db, number, []byte{byte(number)}); err != nil {
			t.Fatalf("WriteTransactionInfosRaw(%d): %v", number, err)
		}
		var root common.Hash
		root[0] = byte(number + 1)
		if err := WriteBlockStateRoot(db, block.Hash(), root); err != nil {
			t.Fatalf("WriteBlockStateRoot(%d): %v", number, err)
		}
		blocks = append(blocks, block)
	}
	if err := DeleteFrozenBlockRangeWithStateRoots(db, 0, 1, []common.Hash{blocks[0].Hash(), blocks[1].Hash()}); err != nil {
		t.Fatalf("DeleteFrozenBlockRangeWithStateRoots: %v", err)
	}
	for number, block := range blocks {
		if _, err := db.Get(blockKey(uint64(number))); err == nil {
			t.Fatalf("hot block %d survived frozen delete", number)
		}
		if _, err := db.Get(txInfoBlockKey(uint64(number))); err == nil {
			t.Fatalf("hot tx info %d survived frozen delete", number)
		}
		if root := ReadBlockStateRootRaw(db, block.Hash()); root != nil {
			t.Fatalf("hot state root %d survived frozen delete: %x", number, root)
		}
	}
}

func TestDeleteFrozenBlockRangeWithStateRootsRejectsMismatchedHashes(t *testing.T) {
	db := NewMemoryDatabase()
	block := types.NewBlockFromPB(newBlockProto(0, 10_000))
	if err := WriteBlock(db, block); err != nil {
		t.Fatalf("WriteBlock: %v", err)
	}
	if err := DeleteFrozenBlockRangeWithStateRoots(db, 0, 0, nil); err == nil {
		t.Fatal("DeleteFrozenBlockRangeWithStateRoots succeeded with missing hash, want error")
	}
	if _, err := db.Get(blockKey(0)); err != nil {
		t.Fatalf("hot block after rejected frozen delete: %v", err)
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

func TestReadBlockHashByNumber_AncientFallthrough(t *testing.T) {
	t.Parallel()

	pb := newBlockProto(13, 33333)
	block := types.NewBlockFromPB(pb)
	data, err := block.Marshal()
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	anc := newFakeAncient()
	anc.put(ancientBlocks, 13, data)
	cdb := NewChainDB(NewMemoryDatabase(), anc)

	got := ReadBlockHashByNumber(cdb, 13)
	if got != block.Hash() {
		t.Fatalf("hash: got %x, want %x", got, block.Hash())
	}
}

func TestReadBlockHashByNumberStrictSurfacesBlockErrors(t *testing.T) {
	t.Parallel()

	anc := newFakeAncient()
	anc.put(ancientBlocks, 13, []byte("not-a-valid-proto"))
	cdb := NewChainDB(NewMemoryDatabase(), anc)
	hash, ok, err := ReadBlockHashByNumberStrict(cdb, 13)
	if err == nil || !ok || hash != (common.Hash{}) || !strings.Contains(err.Error(), "block 13 decode") {
		t.Fatalf("strict hash malformed ancient = %x/%v/%v, want zero/true/decode error", hash, ok, err)
	}

	block := types.NewBlockFromPB(newBlockProto(15, 1515))
	data, err := block.Marshal()
	if err != nil {
		t.Fatalf("marshal block: %v", err)
	}
	anc = newFakeAncient()
	anc.setErr(ancientBlocks, 15, errors.New("ancient hash source corrupt"))
	cdb = NewChainDB(NewMemoryDatabase(), anc)
	if err := cdb.Put(blockKey(15), data); err != nil {
		t.Fatalf("put hot block: %v", err)
	}
	hash, ok, err = ReadBlockHashByNumberStrict(cdb, 15)
	if err == nil || ok || hash != (common.Hash{}) || !strings.Contains(err.Error(), "ancient hash source corrupt") {
		t.Fatalf("strict hash ancient error = %x/%v/%v, want zero/false/error", hash, ok, err)
	}

	wrong := types.NewBlockFromPB(newBlockProto(17, 1717))
	wrongRaw, err := wrong.Marshal()
	if err != nil {
		t.Fatalf("marshal wrong block: %v", err)
	}
	hot := NewMemoryChainDB()
	if err := hot.Put(blockKey(16), wrongRaw); err != nil {
		t.Fatalf("put wrong hot block: %v", err)
	}
	hash, ok, err = ReadBlockHashByNumberStrict(hot, 16)
	if err == nil || !ok || hash != (common.Hash{}) || !strings.Contains(err.Error(), "block hash lookup row 16 contains block number 17") {
		t.Fatalf("strict hash mismatched hot row = %x/%v/%v, want zero/true/mismatch", hash, ok, err)
	}
}

// TestReadBlockNumber_KVPath confirms ReadBlockNumber still prefers the hot
// bh-<hash> row. An ancient block body alone must not satisfy hash lookup.
func TestReadBlockNumber_KVPath(t *testing.T) {
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

func TestReadBlockNumber_ColdIndexFallback(t *testing.T) {
	t.Parallel()

	block := types.NewBlockFromPB(newBlockProto(33, 2026))
	cdb := NewMemoryChainDB()
	cdb.SetChainIndexReader(&fakeChainIndex{
		blocks: map[common.Hash]uint64{block.Hash(): 33},
	})

	got := ReadBlockNumber(cdb, block.Hash())
	if got == nil || *got != 33 {
		t.Fatalf("cold chain-index fallback: got %v, want *33", got)
	}
}

func TestReadBlockNumberStrictSurfacesMalformedHotRow(t *testing.T) {
	t.Parallel()

	block := types.NewBlockFromPB(newBlockProto(34, 2027))
	cdb := NewMemoryChainDB()
	if err := cdb.Put(blockHashKey(block.Hash().Bytes()), []byte{0x01, 0x02}); err != nil {
		t.Fatalf("put malformed block hash row: %v", err)
	}

	num, ok, err := ReadBlockNumberStrict(cdb, block.Hash())
	if err == nil || !ok || num != 0 || !strings.Contains(err.Error(), "has length 2, want 8") {
		t.Fatalf("ReadBlockNumberStrict malformed = %d/%v/%v, want ok/error", num, ok, err)
	}
	if got := ReadBlockNumber(cdb, block.Hash()); got != nil {
		t.Fatalf("ReadBlockNumber compatibility = %v, want nil on malformed row", got)
	}
}

func TestReadBlockNumberStrictSurfacesColdIndexError(t *testing.T) {
	t.Parallel()

	block := types.NewBlockFromPB(newBlockProto(35, 2028))
	cdb := NewMemoryChainDB()
	cdb.SetChainIndexReader(&fakeChainIndex{blockErr: errors.New("cold block index corrupt")})

	num, ok, err := ReadBlockNumberStrict(cdb, block.Hash())
	if err == nil || ok || num != 0 || !strings.Contains(err.Error(), "cold block index corrupt") {
		t.Fatalf("ReadBlockNumberStrict cold err = %d/%v/%v, want cold error", num, ok, err)
	}
	if got := ReadBlockNumber(cdb, block.Hash()); got != nil {
		t.Fatalf("ReadBlockNumber compatibility = %v, want nil on cold error", got)
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

func TestReadTransactionInfosByBlockRejectsMismatchedAncientRet(t *testing.T) {
	t.Parallel()

	ret := &corepb.TransactionRet{
		BlockNumber: 6,
		Transactioninfo: []*corepb.TransactionInfo{
			{Id: bytes.Repeat([]byte{0x03}, 32), Fee: 33, BlockNumber: 5, BlockTimeStamp: 1000},
		},
	}
	data, err := proto.Marshal(ret)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	anc := newFakeAncient()
	anc.put(ancientTxInfos, 5, data)
	cdb := NewChainDB(NewMemoryDatabase(), anc)

	if got := ReadTransactionInfosByBlock(cdb, 5); got != nil {
		t.Fatalf("mismatched ancient TransactionRet read = %+v, want nil", got)
	}
}

func TestReadTransactionInfosRaw_AncientFallthrough(t *testing.T) {
	t.Parallel()

	want := []byte("raw-tx-infos-5")
	anc := newFakeAncient()
	anc.put(ancientTxInfos, 5, want)
	cdb := NewChainDB(NewMemoryDatabase(), anc)

	got := ReadTransactionInfosRaw(cdb, 5)
	if !bytes.Equal(got, want) {
		t.Fatalf("ReadTransactionInfosRaw = %q, want %q", got, want)
	}
}

func TestReadTransactionInfosRawStrictSurfacesAncientReadError(t *testing.T) {
	t.Parallel()

	hot := []byte("hot-tx-infos-6")
	anc := newFakeAncient()
	anc.setErr(ancientTxInfos, 6, errors.New("ancient tx infos corrupt"))
	cdb := NewChainDB(NewMemoryDatabase(), anc)
	if err := cdb.Put(txInfoBlockKey(6), hot); err != nil {
		t.Fatalf("put hot tx infos: %v", err)
	}

	if got := ReadTransactionInfosRaw(cdb, 6); !bytes.Equal(got, hot) {
		t.Fatalf("legacy ReadTransactionInfosRaw = %q, want hot fallback %q", got, hot)
	}
	got, ok, err := ReadTransactionInfosRawStrict(cdb, 6)
	if err == nil || ok || got != nil || !strings.Contains(err.Error(), "ancient tx infos corrupt") {
		t.Fatalf("ReadTransactionInfosRawStrict ancient error = %q/%v/%v, want nil/false/error", got, ok, err)
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

// TestReadTransactionInfo_KVPath proves the hot per-tx info row is still
// preferred.
func TestReadTransactionInfo_KVPath(t *testing.T) {
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

func TestReadTransactionInfo_ColdIndexAndAncientBlockFallback(t *testing.T) {
	t.Parallel()

	txID := bytes.Repeat([]byte{0xBD}, 32)
	ret := &corepb.TransactionRet{
		BlockNumber: 88,
		Transactioninfo: []*corepb.TransactionInfo{
			{Id: bytes.Repeat([]byte{0x01}, 32), Fee: 1, BlockNumber: 88},
			{Id: txID, Fee: 888, BlockNumber: 88},
		},
	}
	data, err := proto.Marshal(ret)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	anc := newFakeAncient()
	anc.put(ancientTxInfos, 88, data)
	cdb := NewChainDB(NewMemoryDatabase(), anc)
	cdb.SetChainIndexReader(&fakeChainIndex{
		txs: map[common.Hash]uint64{common.BytesToHash(txID): 88},
	})

	got := ReadTransactionInfo(cdb, txID)
	if got == nil || got.Fee != 888 {
		t.Fatalf("cold tx info fallback: got %#v, want fee 888", got)
	}
}

func TestReadTransactionInfo_ColdReceiptAfterHotPrune(t *testing.T) {
	t.Parallel()

	txID := bytes.Repeat([]byte{0xBE}, 32)
	otherID := bytes.Repeat([]byte{0xBF}, 32)
	infos := []*corepb.TransactionInfo{
		{Id: otherID, Fee: 1, BlockNumber: 99},
		{
			Id:          txID,
			Fee:         1234,
			BlockNumber: 99,
			Receipt: &corepb.ResourceReceipt{
				EnergyUsage:      45,
				EnergyFee:        678,
				EnergyUsageTotal: 90,
				NetUsage:         12,
			},
		},
	}
	ret := &corepb.TransactionRet{
		BlockNumber:     99,
		Transactioninfo: infos,
	}
	data, err := proto.Marshal(ret)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	kv := NewMemoryDatabase()
	cdb := NewChainDB(kv, newFakeAncient())
	if err := WriteTransactionInfo(cdb, txID, infos[1]); err != nil {
		t.Fatalf("write hot tx info: %v", err)
	}
	if err := WriteTransactionInfosByBlock(cdb, 99, infos); err != nil {
		t.Fatalf("write hot tx infos by block: %v", err)
	}
	if err := WriteTransactionIndex(cdb, txID, 99); err != nil {
		t.Fatalf("write hot tx index: %v", err)
	}
	if err := DeleteTransactionInfo(cdb, txID); err != nil {
		t.Fatalf("delete hot tx info: %v", err)
	}
	if err := DeleteTransactionInfosByBlock(cdb, 99); err != nil {
		t.Fatalf("delete hot tx infos by block: %v", err)
	}
	if err := DeleteTransactionIndex(cdb, txID); err != nil {
		t.Fatalf("delete hot tx index: %v", err)
	}
	if _, err := kv.Get(txInfoKey(txID)); err == nil {
		t.Fatal("hot per-tx info row survived prune setup")
	}
	if _, err := kv.Get(txInfoBlockKey(99)); err == nil {
		t.Fatal("hot per-block tx infos row survived prune setup")
	}
	if _, err := kv.Get(txKey(txID)); err == nil {
		t.Fatal("hot tx index row survived prune setup")
	}

	anc := newFakeAncient()
	anc.put(ancientTxInfos, 99, data)
	cdb = NewChainDB(kv, anc)
	cdb.SetChainIndexReader(&fakeChainIndex{
		txs: map[common.Hash]uint64{common.BytesToHash(txID): 99},
		positions: map[common.Hash]ChainIndexTxLookup{
			common.BytesToHash(txID): {BlockNum: 99, TxIndex: 1},
		},
	})

	got := ReadTransactionInfo(cdb, txID)
	if got == nil || got.Fee != 1234 || got.Receipt == nil {
		t.Fatalf("cold receipt fallback = %+v, want fee 1234 with receipt", got)
	}
	if got.Receipt.EnergyUsage != 45 || got.Receipt.EnergyFee != 678 || got.Receipt.EnergyUsageTotal != 90 || got.Receipt.NetUsage != 12 {
		t.Fatalf("cold receipt = %+v, want energy/net fields preserved", got.Receipt)
	}
}

// TestReadTransactionIndex_KVPath mirrors TestReadTransactionInfo_KVPath
// for the tx-hash -> block-number reverse index.
func TestReadTransactionIndex_KVPath(t *testing.T) {
	t.Parallel()

	cdb := NewMemoryChainDB()
	txHash := bytes.Repeat([]byte{0xCC}, 32)
	WriteTransactionIndex(cdb, txHash, 42)

	got := ReadTransactionIndex(cdb, txHash)
	if got == nil || *got != 42 {
		t.Fatalf("got %v", got)
	}
}

func TestReadTransactionIndex_ColdIndexFallback(t *testing.T) {
	t.Parallel()

	cdb := NewMemoryChainDB()
	txHash := common.HexToHash("cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc")
	cdb.SetChainIndexReader(&fakeChainIndex{
		txs: map[common.Hash]uint64{txHash: 77},
	})

	got := ReadTransactionIndex(cdb, txHash[:])
	if got == nil || *got != 77 {
		t.Fatalf("cold tx-index fallback: got %v, want *77", got)
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

func TestReadBlockStateRoot_ColdIndexFallback(t *testing.T) {
	t.Parallel()

	block := types.NewBlockFromPB(newBlockProto(19, 0))
	want := common.HexToHash("9090909090909090909090909090909090909090909090909090909090909090")
	anc := newFakeAncient()
	anc.put(ancientStateRoots, 19, want.Bytes())
	cdb := NewChainDB(NewMemoryDatabase(), anc)
	cdb.SetChainIndexReader(&fakeChainIndex{
		blocks: map[common.Hash]uint64{block.Hash(): 19},
	})

	got := ReadBlockStateRoot(cdb, block.Hash())
	if got != want {
		t.Fatalf("cold state root fallback: got %x, want %x", got, want)
	}
}

func TestReadBlockStateRootRaw_ColdIndexFallback(t *testing.T) {
	t.Parallel()

	block := types.NewBlockFromPB(newBlockProto(29, 0))
	want := common.HexToHash("2929292929292929292929292929292929292929292929292929292929292929")
	anc := newFakeAncient()
	anc.put(ancientStateRoots, 29, want.Bytes())
	cdb := NewChainDB(NewMemoryDatabase(), anc)
	cdb.SetChainIndexReader(&fakeChainIndex{
		blocks: map[common.Hash]uint64{block.Hash(): 29},
	})

	got := ReadBlockStateRootRaw(cdb, block.Hash())
	if !bytes.Equal(got, want.Bytes()) {
		t.Fatalf("ReadBlockStateRootRaw = %x, want %x", got, want.Bytes())
	}
	strict, ok, err := ReadBlockStateRootRawStrict(cdb, block.Hash())
	if err != nil || !ok || !bytes.Equal(strict, want.Bytes()) {
		t.Fatalf("ReadBlockStateRootRawStrict = %x/%v/%v, want %x/true/nil", strict, ok, err, want.Bytes())
	}
	strictRoot, ok, err := ReadBlockStateRootStrict(cdb, block.Hash())
	if err != nil || !ok || strictRoot != want {
		t.Fatalf("ReadBlockStateRootStrict = %x/%v/%v, want %x/true/nil", strictRoot, ok, err, want)
	}
}

func TestReadBlockStateRootRawStrictSurfacesColdIndexError(t *testing.T) {
	t.Parallel()

	block := types.NewBlockFromPB(newBlockProto(31, 0))
	cdb := NewMemoryChainDB()
	cdb.SetChainIndexReader(&fakeChainIndex{
		blockErr: errors.New("cold block index corrupt"),
	})

	got, ok, err := ReadBlockStateRootRawStrict(cdb, block.Hash())
	if err == nil || ok || got != nil || !strings.Contains(err.Error(), "cold block index corrupt") {
		t.Fatalf("ReadBlockStateRootRawStrict cold error = %x/%v/%v, want nil/false/cold error", got, ok, err)
	}
	root, ok, err := ReadBlockStateRootStrict(cdb, block.Hash())
	if err == nil || ok || root != (common.Hash{}) || !strings.Contains(err.Error(), "cold block index corrupt") {
		t.Fatalf("ReadBlockStateRootStrict cold error = %x/%v/%v, want zero/false/cold error", root, ok, err)
	}
	if got := ReadBlockStateRootRaw(cdb, block.Hash()); got != nil {
		t.Fatalf("legacy ReadBlockStateRootRaw cold error = %x, want nil", got)
	}
}

func TestReadBlockStateRootIgnoresMalformedHotRowAndFallsBackToAncient(t *testing.T) {
	t.Parallel()

	block := types.NewBlockFromPB(newBlockProto(39, 0))
	want := common.HexToHash("3939393939393939393939393939393939393939393939393939393939393939")
	kv := NewMemoryDatabase()
	if err := kv.Put(blockStateRootKey(block.Hash().Bytes()), []byte{0x01}); err != nil {
		t.Fatalf("put malformed bsr: %v", err)
	}
	numBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(numBytes, 39)
	if err := kv.Put(blockHashKey(block.Hash().Bytes()), numBytes); err != nil {
		t.Fatalf("put bh: %v", err)
	}
	anc := newFakeAncient()
	anc.put(ancientStateRoots, 39, want.Bytes())

	cdb := NewChainDB(kv, anc)
	if got := ReadBlockStateRoot(cdb, block.Hash()); got != want {
		t.Fatalf("ReadBlockStateRoot malformed hot fallback = %x, want %x", got, want)
	}
	if got := ReadBlockStateRootRaw(cdb, block.Hash()); !bytes.Equal(got, want.Bytes()) {
		t.Fatalf("ReadBlockStateRootRaw malformed hot fallback = %x, want %x", got, want.Bytes())
	}
	if got, ok, err := ReadBlockStateRootRawStrict(cdb, block.Hash()); err == nil || !ok || got != nil || !strings.Contains(err.Error(), "has length 1") {
		t.Fatalf("ReadBlockStateRootRawStrict malformed hot = %x/%v/%v, want nil/true/length error", got, ok, err)
	}
	if root, ok, err := ReadBlockStateRootStrict(cdb, block.Hash()); err == nil || !ok || root != (common.Hash{}) || !strings.Contains(err.Error(), "has length 1") {
		t.Fatalf("ReadBlockStateRootStrict malformed hot = %x/%v/%v, want zero/true/length error", root, ok, err)
	}
}

func TestReadBlockStateRootRejectsMalformedAncientRow(t *testing.T) {
	t.Parallel()

	block := types.NewBlockFromPB(newBlockProto(49, 0))
	kv := NewMemoryDatabase()
	numBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(numBytes, 49)
	if err := kv.Put(blockHashKey(block.Hash().Bytes()), numBytes); err != nil {
		t.Fatalf("put bh: %v", err)
	}
	anc := newFakeAncient()
	anc.put(ancientStateRoots, 49, bytes.Repeat([]byte{0x49}, common.HashLength-1))

	cdb := NewChainDB(kv, anc)
	if got := ReadBlockStateRoot(cdb, block.Hash()); got != (common.Hash{}) {
		t.Fatalf("ReadBlockStateRoot malformed ancient = %x, want zero", got)
	}
	if got := ReadBlockStateRootRaw(cdb, block.Hash()); got != nil {
		t.Fatalf("ReadBlockStateRootRaw malformed ancient = %x, want nil", got)
	}
	if got, ok, err := ReadBlockStateRootRawStrict(cdb, block.Hash()); err == nil || !ok || got != nil || !strings.Contains(err.Error(), "ancient block state root") {
		t.Fatalf("ReadBlockStateRootRawStrict malformed ancient = %x/%v/%v, want nil/true/length error", got, ok, err)
	}
	if root, ok, err := ReadBlockStateRootStrict(cdb, block.Hash()); err == nil || !ok || root != (common.Hash{}) || !strings.Contains(err.Error(), "ancient block state root") {
		t.Fatalf("ReadBlockStateRootStrict malformed ancient = %x/%v/%v, want zero/true/length error", root, ok, err)
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
	root, ok, err := ReadBlockStateRootStrict(cdb, common.HexToHash("dead"))
	if err != nil || ok || root != (common.Hash{}) {
		t.Fatalf("ReadBlockStateRootStrict missing = %x/%v/%v, want zero/false/nil", root, ok, err)
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

func TestReadBlockStrictSurfacesMalformedAncientBlock(t *testing.T) {
	t.Parallel()

	anc := newFakeAncient()
	anc.put(ancientBlocks, 0, []byte("not-a-valid-proto"))
	cdb := NewChainDB(NewMemoryDatabase(), anc)

	block, ok, err := ReadBlockStrict(cdb, 0)
	if err == nil || !ok || block != nil || !strings.Contains(err.Error(), "block 0 decode") {
		t.Fatalf("ReadBlockStrict malformed ancient = %#v/%v/%v, want nil/true/decode error", block, ok, err)
	}
	raw, ok, err := ReadBlockRawStrict(cdb, 0)
	if err != nil || !ok || !bytes.Equal(raw, []byte("not-a-valid-proto")) {
		t.Fatalf("ReadBlockRawStrict malformed ancient raw = %q/%v/%v", raw, ok, err)
	}
}

func TestReadBlockStrictSurfacesAncientReadError(t *testing.T) {
	t.Parallel()

	block := types.NewBlockFromPB(newBlockProto(2, 222))
	data, err := block.Marshal()
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	anc := newFakeAncient()
	anc.setErr(ancientBlocks, 2, errors.New("ancient block file corrupt"))
	cdb := NewChainDB(NewMemoryDatabase(), anc)
	if err := cdb.Put(blockKey(2), data); err != nil {
		t.Fatalf("put hot block: %v", err)
	}

	if got := ReadBlock(cdb, 2); got == nil || got.Hash() != block.Hash() {
		t.Fatalf("legacy ReadBlock should fall back to hot row, got %#v", got)
	}
	got, ok, err := ReadBlockStrict(cdb, 2)
	if err == nil || ok || got != nil || !strings.Contains(err.Error(), "ancient block file corrupt") {
		t.Fatalf("ReadBlockStrict ancient read error = %#v/%v/%v, want nil/false/error", got, ok, err)
	}
	raw, ok, err := ReadBlockRawStrict(cdb, 2)
	if err == nil || ok || raw != nil || !strings.Contains(err.Error(), "ancient block file corrupt") {
		t.Fatalf("ReadBlockRawStrict ancient read error = %x/%v/%v, want nil/false/error", raw, ok, err)
	}
}

func TestReadBlockStrictRejectsMismatchedHotBlockNumber(t *testing.T) {
	t.Parallel()

	block := types.NewBlockFromPB(newBlockProto(4, 444))
	data, err := block.Marshal()
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	cdb := NewMemoryChainDB()
	if err := cdb.Put(blockKey(3), data); err != nil {
		t.Fatalf("put mismatched hot block: %v", err)
	}

	if got := ReadBlock(cdb, 3); got == nil || got.Number() != 4 {
		t.Fatalf("legacy ReadBlock = %#v, want mismatched block for compatibility", got)
	}
	got, ok, err := ReadBlockStrict(cdb, 3)
	if err == nil || !ok || got == nil || got.Number() != 4 || !strings.Contains(err.Error(), "block row 3 contains block number 4") {
		t.Fatalf("ReadBlockStrict mismatched hot row = %#v/%v/%v, want block/true/mismatch error", got, ok, err)
	}
}
