package rawdb

import (
	"fmt"

	"google.golang.org/protobuf/proto"

	"github.com/ethereum/go-ethereum/ethdb"
	contractpb "github.com/tronprotocol/go-tron/proto/core/contract"
)

// WriteAccountTrace records `balance` at `blockNum` for `owner`. Mirrors
// java-tron AccountTraceStore.recordBalanceWithBlock.
func WriteAccountTrace(db ethdb.KeyValueWriter, owner []byte, blockNum int64, balance int64) error {
	if len(owner) == 0 {
		return fmt.Errorf("account trace: empty owner")
	}
	data, err := proto.Marshal(&contractpb.AccountTrace{Balance: balance})
	if err != nil {
		return fmt.Errorf("account trace: marshal: %w", err)
	}
	return db.Put(accountTraceKey(owner, blockNum), data)
}

// ReadAccountTrace returns the balance recorded for (owner, blockNum) or
// 0 + false if no trace exists at that height. For "latest balance at or
// before block N" use iteration with a prefix of accountTracePrefix ||
// owner starting at the key for N — but that API isn't exposed yet.
func ReadAccountTrace(db ethdb.KeyValueReader, owner []byte, blockNum int64) (int64, bool) {
	data, err := db.Get(accountTraceKey(owner, blockNum))
	if err != nil || len(data) == 0 {
		return 0, false
	}
	var at contractpb.AccountTrace
	if err := proto.Unmarshal(data, &at); err != nil {
		return 0, false
	}
	return at.Balance, true
}

// DeleteAccountTrace removes the record.
func DeleteAccountTrace(db ethdb.KeyValueWriter, owner []byte, blockNum int64) error {
	return db.Delete(accountTraceKey(owner, blockNum))
}
