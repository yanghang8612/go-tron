package state

import (
	tcommon "github.com/tronprotocol/go-tron/common"
	contractpb "github.com/tronprotocol/go-tron/proto/core/contract"
)

const balanceTraceSuccessStatus = "SUCCESS"

type balanceTraceRecorder struct {
	blockNum  int64
	blockHash []byte
	timestamp int64

	currentTx *contractpb.TransactionBalanceTrace
	txs       []*contractpb.TransactionBalanceTrace

	touched    map[tcommon.Address]struct{}
	touchOrder []tcommon.Address
}

type balanceTraceSnapshot struct {
	recorder *balanceTraceRecorder

	txCount         int
	currentTx       *contractpb.TransactionBalanceTrace
	currentTxOps    int
	currentTxStatus string
	touchCount      int
}

func newBalanceTraceRecorder(blockNum int64, blockHash []byte, timestamp int64) *balanceTraceRecorder {
	return &balanceTraceRecorder{
		blockNum:  blockNum,
		blockHash: append([]byte(nil), blockHash...),
		timestamp: timestamp,
		touched:   make(map[tcommon.Address]struct{}),
	}
}

func (r *balanceTraceRecorder) snapshot() balanceTraceSnapshot {
	snap := balanceTraceSnapshot{
		recorder:   r,
		txCount:    len(r.txs),
		currentTx:  r.currentTx,
		touchCount: len(r.touchOrder),
	}
	if r.currentTx != nil {
		snap.currentTxOps = len(r.currentTx.Operation)
		snap.currentTxStatus = r.currentTx.Status
	}
	return snap
}

func (r *balanceTraceRecorder) revert(snap balanceTraceSnapshot) {
	if r == nil || snap.recorder != r {
		return
	}
	if snap.txCount < len(r.txs) {
		for i := snap.txCount; i < len(r.txs); i++ {
			r.txs[i] = nil
		}
		r.txs = r.txs[:snap.txCount]
	}
	r.currentTx = snap.currentTx
	if r.currentTx != nil {
		if snap.currentTxOps < len(r.currentTx.Operation) {
			for i := snap.currentTxOps; i < len(r.currentTx.Operation); i++ {
				r.currentTx.Operation[i] = nil
			}
			r.currentTx.Operation = r.currentTx.Operation[:snap.currentTxOps]
		}
		r.currentTx.Status = snap.currentTxStatus
	}
	if snap.touchCount < len(r.touchOrder) {
		for _, addr := range r.touchOrder[snap.touchCount:] {
			delete(r.touched, addr)
		}
		r.touchOrder = r.touchOrder[:snap.touchCount]
	}
}

func (r *balanceTraceRecorder) beginTransaction(txID []byte, txType string) {
	if r == nil {
		return
	}
	r.currentTx = &contractpb.TransactionBalanceTrace{
		TransactionIdentifier: append([]byte(nil), txID...),
		Type:                  txType,
	}
}

func (r *balanceTraceRecorder) endTransaction(status string) {
	if r == nil || r.currentTx == nil {
		return
	}
	if status == "" || status == "DEFAULT" {
		status = balanceTraceSuccessStatus
	}
	r.currentTx.Status = status
	if len(r.currentTx.Operation) > 0 {
		r.txs = append(r.txs, r.currentTx)
	}
	r.currentTx = nil
}

func (r *balanceTraceRecorder) recordBalanceChange(addr tcommon.Address, amount int64) {
	if r == nil {
		return
	}
	if amount == 0 {
		return
	}
	r.touch(addr)
	if r.currentTx == nil {
		return
	}
	r.currentTx.Operation = append(r.currentTx.Operation, &contractpb.TransactionBalanceTrace_Operation{
		OperationIdentifier: int64(len(r.currentTx.Operation)),
		Address:             addr.Bytes(),
		Amount:              amount,
	})
}

func (r *balanceTraceRecorder) touch(addr tcommon.Address) {
	if r == nil {
		return
	}
	if _, ok := r.touched[addr]; ok {
		return
	}
	r.touched[addr] = struct{}{}
	r.touchOrder = append(r.touchOrder, addr)
}

func (r *balanceTraceRecorder) blockTrace() *contractpb.BlockBalanceTrace {
	if r == nil {
		return nil
	}
	txs := make([]*contractpb.TransactionBalanceTrace, len(r.txs))
	copy(txs, r.txs)
	return &contractpb.BlockBalanceTrace{
		BlockIdentifier: &contractpb.BlockBalanceTrace_BlockIdentifier{
			Hash:   append([]byte(nil), r.blockHash...),
			Number: r.blockNum,
		},
		Timestamp:               r.timestamp,
		TransactionBalanceTrace: txs,
	}
}

// BeginBalanceTrace starts java-tron-compatible balance history capture for a
// canonical block. Calls outside history-enabled block replay should leave this
// disabled to keep the hot path at zero overhead.
func (s *StateDB) BeginBalanceTrace(blockNum int64, blockHash []byte, timestamp int64) {
	s.balanceTrace = newBalanceTraceRecorder(blockNum, blockHash, timestamp)
}

// ClearBalanceTrace discards any in-progress balance trace capture.
func (s *StateDB) ClearBalanceTrace() {
	s.balanceTrace = nil
	for i := range s.balanceTraceSnapshots {
		s.balanceTraceSnapshots[i] = balanceTraceSnapshot{}
	}
}

// BeginBalanceTraceTransaction starts per-transaction operation capture. Balance
// changes made while no transaction is active still update AccountTrace state,
// but do not appear in BlockBalanceTrace.transaction_balance_trace.
func (s *StateDB) BeginBalanceTraceTransaction(txID []byte, txType string) {
	if s.balanceTrace != nil {
		s.balanceTrace.beginTransaction(txID, txType)
	}
}

// EndBalanceTraceTransaction finalizes the current transaction trace, appending
// it to the block trace only if it contains at least one balance operation.
func (s *StateDB) EndBalanceTraceTransaction(status string) {
	if s.balanceTrace != nil {
		s.balanceTrace.endTransaction(status)
	}
}

// FinishBalanceTrace returns the per-block operation trace plus final balances
// for every account touched during the block, then disables capture.
func (s *StateDB) FinishBalanceTrace() (*contractpb.BlockBalanceTrace, map[tcommon.Address]int64) {
	if s.balanceTrace == nil {
		return nil, nil
	}
	rec := s.balanceTrace
	trace := rec.blockTrace()
	balances := make(map[tcommon.Address]int64, len(rec.touchOrder))
	for _, addr := range rec.touchOrder {
		if s.AccountExists(addr) {
			balances[addr] = s.GetBalance(addr)
		} else {
			balances[addr] = 0
		}
	}
	s.ClearBalanceTrace()
	return trace, balances
}

func (s *StateDB) recordBalanceTraceChange(addr tcommon.Address, amount int64) {
	if s.balanceTrace != nil {
		s.balanceTrace.recordBalanceChange(addr, amount)
	}
}

func (s *StateDB) touchBalanceTraceAccount(addr tcommon.Address) {
	if s.balanceTrace != nil {
		s.balanceTrace.touch(addr)
	}
}
