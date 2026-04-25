package rawdb

import (
	"github.com/ethereum/go-ethereum/ethdb"
	contractpb "github.com/tronprotocol/go-tron/proto/core/contract"
	"google.golang.org/protobuf/proto"
)

// WriteBlockBalanceTrace stores the per-block balance trace. Mirrors
// java-tron BalanceTraceStore.putBlockBalanceTrace; gated on
// isHistoryBalanceLookup — callers must check the flag before writing.
func WriteBlockBalanceTrace(db ethdb.KeyValueWriter, blockNum int64, trace *contractpb.BlockBalanceTrace) {
	data, err := proto.Marshal(trace)
	if err != nil {
		return
	}
	_ = db.Put(balanceTraceKey(blockNum), data)
}

// ReadBlockBalanceTrace returns the BlockBalanceTrace for blockNum, or nil
// if absent.
func ReadBlockBalanceTrace(db ethdb.KeyValueReader, blockNum int64) *contractpb.BlockBalanceTrace {
	data, err := db.Get(balanceTraceKey(blockNum))
	if err != nil || len(data) == 0 {
		return nil
	}
	var trace contractpb.BlockBalanceTrace
	if err := proto.Unmarshal(data, &trace); err != nil {
		return nil
	}
	return &trace
}

// HasBlockBalanceTrace reports whether a trace is stored for blockNum.
func HasBlockBalanceTrace(db ethdb.KeyValueReader, blockNum int64) bool {
	ok, _ := db.Has(balanceTraceKey(blockNum))
	return ok
}

// DeleteBlockBalanceTrace removes the balance trace for blockNum.
func DeleteBlockBalanceTrace(db ethdb.KeyValueWriter, blockNum int64) error {
	return db.Delete(balanceTraceKey(blockNum))
}
