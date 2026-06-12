package rawdb

import (
	"bytes"
	"testing"

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
	block, balance, ok, err := ReadAccountTraceAtOrBefore(db, owner, 10)
	if err != nil {
		t.Fatalf("ReadAccountTraceAtOrBefore cold future: %v", err)
	}
	if ok || block != 0 || balance != 0 {
		t.Fatalf("ReadAccountTraceAtOrBefore cold future = block %d balance %d ok %v, want zero/false", block, balance, ok)
	}
}
