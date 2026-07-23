package jsonrpc

import (
	"sync/atomic"
	"testing"

	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/types"
	corepb "github.com/tronprotocol/go-tron/proto/core"
)

type countingFilterBackend struct {
	Backend
	info      *corepb.TransactionInfo
	infoCalls atomic.Int64
}

func (b *countingFilterBackend) GetTransactionInfo(_ common.Hash) (*corepb.TransactionInfo, error) {
	b.infoCalls.Add(1)
	return b.info, nil
}

func filterFastPathBlock(txCount int) *types.Block {
	txs := make([]*corepb.Transaction, txCount)
	for i := range txs {
		txs[i] = &corepb.Transaction{RawData: &corepb.TransactionRaw{Timestamp: int64(i + 1)}}
	}
	return types.NewBlockFromPB(&corepb.Block{
		BlockHeader: &corepb.BlockHeader{RawData: &corepb.BlockHeaderRaw{
			Number:    42,
			Timestamp: 3_000,
		}},
		Transactions: txs,
	})
}

func TestFilterFanOutSkipsReceiptsWithoutConsumers(t *testing.T) {
	backend := new(countingFilterBackend)
	fm := NewFilterManager(backend)
	fm.subMgr = newSubscriptionManager()

	fm.fanOut(filterFastPathBlock(100))
	if got := backend.infoCalls.Load(); got != 0 {
		t.Fatalf("GetTransactionInfo calls: got %d, want 0", got)
	}
}

func TestFilterFanOutBlockConsumersSkipReceipts(t *testing.T) {
	backend := new(countingFilterBackend)
	fm := NewFilterManager(backend)
	id, err := fm.NewBlockFilter()
	if err != nil {
		t.Fatal(err)
	}

	fm.fanOut(filterFastPathBlock(100))
	if got := backend.infoCalls.Load(); got != 0 {
		t.Fatalf("GetTransactionInfo calls: got %d, want 0", got)
	}
	changes, ok := fm.GetFilterChanges(id)
	if !ok {
		t.Fatal("block filter missing")
	}
	hashes := changes.([]string)
	if len(hashes) != 1 {
		t.Fatalf("pending block hashes: got %d, want 1", len(hashes))
	}
}

func TestFilterFanOutLogConsumerReadsReceipts(t *testing.T) {
	backend := &countingFilterBackend{info: &corepb.TransactionInfo{Log: []*corepb.TransactionInfo_Log{{
		Address: make([]byte, 21),
		Data:    []byte{0xde, 0xad},
	}}}}
	fm := NewFilterManager(backend)
	id, err := fm.NewLogFilter(LogFilter{})
	if err != nil {
		t.Fatal(err)
	}

	fm.fanOut(filterFastPathBlock(3))
	if got := backend.infoCalls.Load(); got != 3 {
		t.Fatalf("GetTransactionInfo calls: got %d, want 3", got)
	}
	changes, ok := fm.GetFilterChanges(id)
	if !ok {
		t.Fatal("log filter missing")
	}
	logs := changes.([]*RPCLog)
	if len(logs) != 3 {
		t.Fatalf("pending logs: got %d, want 3", len(logs))
	}
}

func TestFilterFanOutNewHeadsSubscriptionSkipsReceipts(t *testing.T) {
	backend := new(countingFilterBackend)
	fm := NewFilterManager(backend)
	sm := newSubscriptionManager()
	out := make(chan []byte, 1)
	sm.subs["sub"] = &wsSub{id: "sub", kind: "newHeads", outCh: out}
	fm.subMgr = sm

	fm.fanOut(filterFastPathBlock(100))
	if got := backend.infoCalls.Load(); got != 0 {
		t.Fatalf("GetTransactionInfo calls: got %d, want 0", got)
	}
	select {
	case <-out:
	default:
		t.Fatal("newHeads subscriber did not receive block")
	}
}

func BenchmarkFilterFanOut(b *testing.B) {
	block := filterFastPathBlock(100)

	b.Run("no-consumers", func(b *testing.B) {
		backend := new(countingFilterBackend)
		fm := NewFilterManager(backend)
		fm.subMgr = newSubscriptionManager()
		b.ReportAllocs()
		for b.Loop() {
			fm.fanOut(block)
		}
	})
	b.Run("log-demand-no-receipts", func(b *testing.B) {
		backend := new(countingFilterBackend)
		fm := NewFilterManager(backend)
		if _, err := fm.NewLogFilter(LogFilter{}); err != nil {
			b.Fatal(err)
		}
		b.ReportAllocs()
		for b.Loop() {
			fm.fanOut(block)
		}
	})
}
