package rawdb

import (
	"bytes"
	"strings"
	"testing"

	"github.com/tronprotocol/go-tron/common"
	contractpb "github.com/tronprotocol/go-tron/proto/core/contract"
)

type fakeBalanceTraceReader struct {
	blockTraces   map[int64]*contractpb.BlockBalanceTrace
	accountTraces map[string]map[int64]int64
	accountLookup map[string]fakeAccountTraceLookup
}

type fakeAccountTraceLookup struct {
	block   int64
	balance int64
	ok      bool
}

func newFakeBalanceTraceReader() *fakeBalanceTraceReader {
	return &fakeBalanceTraceReader{
		blockTraces:   make(map[int64]*contractpb.BlockBalanceTrace),
		accountTraces: make(map[string]map[int64]int64),
		accountLookup: make(map[string]fakeAccountTraceLookup),
	}
}

func (r *fakeBalanceTraceReader) BlockBalanceTrace(blockNum int64) (*contractpb.BlockBalanceTrace, bool, error) {
	trace, ok := r.blockTraces[blockNum]
	return trace, ok, nil
}

func (r *fakeBalanceTraceReader) AccountTraceAtOrBefore(owner []byte, blockNum int64) (int64, int64, bool, error) {
	if row, ok := r.accountLookup[string(owner)]; ok {
		return row.block, row.balance, row.ok, nil
	}
	rows := r.accountTraces[string(owner)]
	var bestBlock int64
	var bestBalance int64
	var ok bool
	for n, balance := range rows {
		if n > blockNum {
			continue
		}
		if !ok || n > bestBlock {
			bestBlock = n
			bestBalance = balance
			ok = true
		}
	}
	return bestBlock, bestBalance, ok, nil
}

func (r *fakeBalanceTraceReader) putBlockTrace(blockNum int64, trace *contractpb.BlockBalanceTrace) {
	r.blockTraces[blockNum] = trace
}

func (r *fakeBalanceTraceReader) putAccountTrace(owner []byte, blockNum int64, balance int64) {
	key := string(owner)
	if r.accountTraces[key] == nil {
		r.accountTraces[key] = make(map[int64]int64)
	}
	r.accountTraces[key][blockNum] = balance
}

func (r *fakeBalanceTraceReader) setAccountTraceLookup(owner []byte, blockNum int64, balance int64, ok bool) {
	r.accountLookup[string(owner)] = fakeAccountTraceLookup{
		block:   blockNum,
		balance: balance,
		ok:      ok,
	}
}

func TestBlockBalanceTrace_FallsThroughToColdReader(t *testing.T) {
	db := NewMemoryChainDB()
	cold := newFakeBalanceTraceReader()
	db.SetBalanceTraceReader(cold)

	trace := &contractpb.BlockBalanceTrace{
		BlockIdentifier: &contractpb.BlockBalanceTrace_BlockIdentifier{
			Hash:   bytes.Repeat([]byte{0x77}, 32),
			Number: 12,
		},
		Timestamp: 1200,
	}
	cold.putBlockTrace(12, trace)

	if !HasBlockBalanceTrace(db, 12) {
		t.Fatal("HasBlockBalanceTrace missed cold trace")
	}
	got := ReadBlockBalanceTrace(db, 12)
	if got == nil || got.GetTimestamp() != trace.GetTimestamp() {
		t.Fatalf("ReadBlockBalanceTrace cold = %+v, want timestamp %d", got, trace.GetTimestamp())
	}
	if got := ReadBlockBalanceTrace(db, 13); got != nil {
		t.Fatalf("ReadBlockBalanceTrace missing cold row = %+v, want nil", got)
	}
}

func TestBlockBalanceTraceRejectsColdBlockNumberMismatch(t *testing.T) {
	db := NewMemoryChainDB()
	cold := newFakeBalanceTraceReader()
	db.SetBalanceTraceReader(cold)
	cold.putBlockTrace(12, &contractpb.BlockBalanceTrace{
		BlockIdentifier: &contractpb.BlockBalanceTrace_BlockIdentifier{
			Hash:   bytes.Repeat([]byte{0x77}, 32),
			Number: 13,
		},
		Timestamp: 1200,
	})

	if HasBlockBalanceTrace(db, 12) {
		t.Fatal("HasBlockBalanceTrace accepted cold block-number mismatch")
	}
	if got := ReadBlockBalanceTrace(db, 12); got != nil {
		t.Fatalf("ReadBlockBalanceTrace cold block-number mismatch = %+v, want nil", got)
	}
	if trace, ok, err := ReadBlockBalanceTraceStrict(db, 12); err == nil || !ok || trace == nil {
		t.Fatalf("ReadBlockBalanceTraceStrict cold block-number mismatch = trace %+v ok %v err %v, want trace/ok/error", trace, ok, err)
	}
}

func TestBlockBalanceTraceRejectsHotCanonicalHashMismatch(t *testing.T) {
	db := NewMemoryChainDB()
	block := testSyncStagedBlock(12, common.Hash{0x11})
	if err := WriteBlock(db, block); err != nil {
		t.Fatalf("WriteBlock: %v", err)
	}
	if err := WriteBlockBalanceTrace(db, int64(block.Number()), &contractpb.BlockBalanceTrace{
		BlockIdentifier: &contractpb.BlockBalanceTrace_BlockIdentifier{
			Hash:   bytes.Repeat([]byte{0xee}, 32),
			Number: int64(block.Number()),
		},
		Timestamp: 1200,
	}); err != nil {
		t.Fatalf("WriteBlockBalanceTrace: %v", err)
	}

	if got := ReadBlockBalanceTrace(db, int64(block.Number())); got != nil {
		t.Fatalf("ReadBlockBalanceTrace hot hash mismatch = %+v, want nil compatibility miss", got)
	}
	if trace, ok, err := ReadBlockBalanceTraceStrict(db, int64(block.Number())); err == nil || !ok || trace == nil || !strings.Contains(err.Error(), "does not match canonical block") {
		t.Fatalf("ReadBlockBalanceTraceStrict hot hash mismatch = trace %+v ok %v err %v, want canonical hash error", trace, ok, err)
	}
}

func TestBlockBalanceTraceRejectsColdCanonicalHashMismatch(t *testing.T) {
	db := NewMemoryChainDB()
	block := testSyncStagedBlock(14, common.Hash{0x12})
	if err := WriteBlock(db, block); err != nil {
		t.Fatalf("WriteBlock: %v", err)
	}
	cold := newFakeBalanceTraceReader()
	db.SetBalanceTraceReader(cold)
	cold.putBlockTrace(int64(block.Number()), &contractpb.BlockBalanceTrace{
		BlockIdentifier: &contractpb.BlockBalanceTrace_BlockIdentifier{
			Hash:   bytes.Repeat([]byte{0xdd}, 32),
			Number: int64(block.Number()),
		},
		Timestamp: 1400,
	})

	if got := ReadBlockBalanceTrace(db, int64(block.Number())); got != nil {
		t.Fatalf("ReadBlockBalanceTrace cold hash mismatch = %+v, want nil compatibility miss", got)
	}
	if trace, ok, err := ReadBlockBalanceTraceStrict(db, int64(block.Number())); err == nil || !ok || trace == nil || !strings.Contains(err.Error(), "does not match canonical block") {
		t.Fatalf("ReadBlockBalanceTraceStrict cold hash mismatch = trace %+v ok %v err %v, want canonical hash error", trace, ok, err)
	}
}

func TestBlockBalanceTraceRejectsHotMalformedHashLength(t *testing.T) {
	db := NewMemoryChainDB()
	if err := WriteBlockBalanceTrace(db, 16, &contractpb.BlockBalanceTrace{
		BlockIdentifier: &contractpb.BlockBalanceTrace_BlockIdentifier{
			Hash:   []byte{0x16},
			Number: 16,
		},
		Timestamp: 1600,
	}); err != nil {
		t.Fatalf("WriteBlockBalanceTrace: %v", err)
	}

	if got := ReadBlockBalanceTrace(db, 16); got != nil {
		t.Fatalf("ReadBlockBalanceTrace hot malformed hash = %+v, want nil compatibility miss", got)
	}
	if trace, ok, err := ReadBlockBalanceTraceStrict(db, 16); err == nil || !ok || trace == nil || !strings.Contains(err.Error(), "payload hash length 1") {
		t.Fatalf("ReadBlockBalanceTraceStrict hot malformed hash = trace %+v ok %v err %v, want hash length error", trace, ok, err)
	}
}

func TestBlockBalanceTraceRejectsColdMalformedHashLength(t *testing.T) {
	db := NewMemoryChainDB()
	cold := newFakeBalanceTraceReader()
	db.SetBalanceTraceReader(cold)
	cold.putBlockTrace(18, &contractpb.BlockBalanceTrace{
		BlockIdentifier: &contractpb.BlockBalanceTrace_BlockIdentifier{
			Hash:   []byte{0x18},
			Number: 18,
		},
		Timestamp: 1800,
	})

	if got := ReadBlockBalanceTrace(db, 18); got != nil {
		t.Fatalf("ReadBlockBalanceTrace cold malformed hash = %+v, want nil compatibility miss", got)
	}
	if trace, ok, err := ReadBlockBalanceTraceStrict(db, 18); err == nil || !ok || trace == nil || !strings.Contains(err.Error(), "payload hash length 1") {
		t.Fatalf("ReadBlockBalanceTraceStrict cold malformed hash = trace %+v ok %v err %v, want hash length error", trace, ok, err)
	}
}

func TestAccountTrace_FallsThroughToColdReader(t *testing.T) {
	db := NewMemoryChainDB()
	cold := newFakeBalanceTraceReader()
	db.SetBalanceTraceReader(cold)
	owner := mustAddr(0xe1)
	cold.putAccountTrace(owner, 7, 700)

	if got, ok := ReadAccountTrace(db, owner, 7); !ok || got != 700 {
		t.Fatalf("ReadAccountTrace cold exact = %d/%v, want 700/true", got, ok)
	}
	if got, ok := ReadAccountTrace(db, owner, 8); ok || got != 0 {
		t.Fatalf("ReadAccountTrace cold non-exact = %d/%v, want 0/false", got, ok)
	}
	block, balance, ok, err := ReadAccountTraceAtOrBefore(db, owner, 8)
	if err != nil {
		t.Fatalf("ReadAccountTraceAtOrBefore cold: %v", err)
	}
	if !ok || block != 7 || balance != 700 {
		t.Fatalf("ReadAccountTraceAtOrBefore cold = block %d balance %d ok %v, want 7/700/true", block, balance, ok)
	}
}

func TestAccountTrace_AtOrBeforeChoosesNewestAcrossHotAndCold(t *testing.T) {
	db := NewMemoryChainDB()
	cold := newFakeBalanceTraceReader()
	db.SetBalanceTraceReader(cold)
	owner := mustAddr(0xe2)
	cold.putAccountTrace(owner, 90, 900)
	if err := WriteAccountTrace(db, owner, 80, 800); err != nil {
		t.Fatalf("WriteAccountTrace: %v", err)
	}

	block, balance, ok, err := ReadAccountTraceAtOrBefore(db, owner, 100)
	if err != nil {
		t.Fatalf("ReadAccountTraceAtOrBefore: %v", err)
	}
	if !ok || block != 90 || balance != 900 {
		t.Fatalf("hot+cold newest = block %d balance %d ok %v, want 90/900/true", block, balance, ok)
	}

	if err := WriteAccountTrace(db, owner, 95, 950); err != nil {
		t.Fatalf("WriteAccountTrace hot newest: %v", err)
	}
	block, balance, ok, err = ReadAccountTraceAtOrBefore(db, owner, 100)
	if err != nil {
		t.Fatalf("ReadAccountTraceAtOrBefore hot newest: %v", err)
	}
	if !ok || block != 95 || balance != 950 {
		t.Fatalf("hot newest = block %d balance %d ok %v, want 95/950/true", block, balance, ok)
	}
}

func TestAccountTraceRejectsColdFutureLookup(t *testing.T) {
	db := NewMemoryChainDB()
	cold := newFakeBalanceTraceReader()
	db.SetBalanceTraceReader(cold)
	owner := mustAddr(0xe3)
	cold.setAccountTraceLookup(owner, 11, 1100, true)

	if got, ok := ReadAccountTrace(db, owner, 10); ok || got != 0 {
		t.Fatalf("ReadAccountTrace cold future = %d/%v, want 0/false", got, ok)
	}
	if got, ok, err := ReadAccountTraceStrict(db, owner, 10); err == nil || ok || got != 0 {
		t.Fatalf("ReadAccountTraceStrict cold future = %d/%v/%v, want future-block error", got, ok, err)
	}
	block, balance, ok, err := ReadAccountTraceAtOrBefore(db, owner, 10)
	if err == nil || ok || block != 0 || balance != 0 {
		t.Fatalf("ReadAccountTraceAtOrBefore cold future = block %d balance %d ok %v err %v, want future-block error", block, balance, ok, err)
	}
}
