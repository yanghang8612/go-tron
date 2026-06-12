package rawdb

import (
	"encoding/binary"
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
// 0 + false if no trace exists at that exact height.
func ReadAccountTrace(db ethdb.KeyValueReader, owner []byte, blockNum int64) (int64, bool) {
	balance, ok, err := readHotAccountTrace(db, owner, blockNum)
	if err != nil || !ok {
		traceBlock, balance, ok, err := readColdAccountTraceAtOrBefore(db, owner, blockNum)
		if err != nil || !ok || traceBlock != blockNum {
			return 0, false
		}
		return balance, true
	}
	return balance, true
}

func readHotAccountTrace(db ethdb.KeyValueReader, owner []byte, blockNum int64) (int64, bool, error) {
	if db == nil {
		return 0, false, fmt.Errorf("account trace: nil database")
	}
	key := accountTraceKey(owner, blockNum)
	exists, err := db.Has(key)
	if err != nil || !exists {
		return 0, false, err
	}
	data, err := db.Get(key)
	if err != nil || len(data) == 0 {
		return 0, false, err
	}
	var at contractpb.AccountTrace
	if err := proto.Unmarshal(data, &at); err != nil {
		return 0, false, fmt.Errorf("account trace: unmarshal: %w", err)
	}
	return at.Balance, true, nil
}

// ReadAccountTraceAtOrBefore returns the newest account-trace row whose block
// number is <= blockNum. This mirrors java-tron's AccountTraceStore.getPrevBalance.
func ReadAccountTraceAtOrBefore(db ethdb.Iteratee, owner []byte, blockNum int64) (traceBlock int64, balance int64, ok bool, err error) {
	if db == nil {
		return 0, 0, false, fmt.Errorf("account trace: nil database")
	}
	if len(owner) == 0 {
		return 0, 0, false, fmt.Errorf("account trace: empty owner")
	}
	if blockNum < 0 {
		return 0, 0, false, fmt.Errorf("account trace: negative block %d", blockNum)
	}
	prefix := accountTraceOwnerPrefix(owner)
	start := accountTraceBlockSuffix(blockNum)
	it := db.NewIterator(prefix, start)
	defer it.Release()
	var hotBlock int64
	var hotBalance int64
	var hotOK bool
	if it.Next() {
		key := it.Key()
		if len(key) != len(prefix)+8 {
			return 0, 0, false, fmt.Errorf("account trace: malformed key length %d", len(key))
		}
		var at contractpb.AccountTrace
		if err := proto.Unmarshal(it.Value(), &at); err != nil {
			return 0, 0, false, fmt.Errorf("account trace: unmarshal: %w", err)
		}
		hotBlock = accountTraceBlockFromSuffix(key[len(prefix):])
		hotBalance = at.Balance
		hotOK = true
	} else if err := it.Error(); err != nil {
		return 0, 0, false, err
	}

	coldBlock, coldBalance, coldOK, err := readColdAccountTraceAtOrBefore(db, owner, blockNum)
	if err != nil {
		return 0, 0, false, err
	}
	if coldOK && (!hotOK || coldBlock > hotBlock) {
		return coldBlock, coldBalance, true, nil
	}
	if hotOK {
		return hotBlock, hotBalance, true, nil
	}
	return 0, 0, false, nil
}

// DeleteAccountTrace removes the record.
func DeleteAccountTrace(db ethdb.KeyValueWriter, owner []byte, blockNum int64) error {
	return db.Delete(accountTraceKey(owner, blockNum))
}

// IterateAccountTraceRows walks hot AccountTrace rows in raw key order, filtering
// to the inclusive block range. It does not consult cold snapshot sidecars.
func IterateAccountTraceRows(db ethdb.Iteratee, fromBlock, toBlock int64, fn func(owner []byte, blockNum int64, balance int64) (bool, error)) error {
	if db == nil {
		return fmt.Errorf("account trace: nil database")
	}
	if fn == nil {
		return fmt.Errorf("account trace: nil callback")
	}
	if fromBlock < 0 || toBlock < 0 {
		return fmt.Errorf("account trace: negative range [%d,%d]", fromBlock, toBlock)
	}
	if toBlock < fromBlock {
		return fmt.Errorf("account trace: inverted range [%d,%d]", fromBlock, toBlock)
	}
	it := db.NewIterator(accountTracePrefix, nil)
	defer it.Release()
	for it.Next() {
		key := it.Key()
		if len(key) <= len(accountTracePrefix)+8 {
			return fmt.Errorf("account trace: malformed key length %d", len(key))
		}
		owner := key[len(accountTracePrefix) : len(key)-8]
		blockNum := accountTraceBlockFromSuffix(key[len(key)-8:])
		if blockNum < fromBlock || blockNum > toBlock {
			continue
		}
		var at contractpb.AccountTrace
		if err := proto.Unmarshal(it.Value(), &at); err != nil {
			return fmt.Errorf("account trace: unmarshal block %d owner %x: %w", blockNum, owner, err)
		}
		keepGoing, err := fn(append([]byte(nil), owner...), blockNum, at.Balance)
		if err != nil {
			return err
		}
		if !keepGoing {
			break
		}
	}
	return it.Error()
}

func accountTraceOwnerPrefix(owner []byte) []byte {
	k := make([]byte, 0, len(accountTracePrefix)+len(owner))
	k = append(k, accountTracePrefix...)
	return append(k, owner...)
}

func accountTraceBlockSuffix(blockNum int64) []byte {
	const longMax int64 = 0x7FFFFFFFFFFFFFFF
	var b [8]byte
	binary.BigEndian.PutUint64(b[:], uint64(blockNum^longMax))
	return b[:]
}

func accountTraceBlockFromSuffix(suffix []byte) int64 {
	const longMax int64 = 0x7FFFFFFFFFFFFFFF
	return int64(binary.BigEndian.Uint64(suffix)) ^ longMax
}

func readColdAccountTraceAtOrBefore(db interface{}, owner []byte, blockNum int64) (int64, int64, bool, error) {
	chain, ok := db.(*ChainDB)
	if !ok || chain == nil || chain.balanceTrace == nil {
		return 0, 0, false, nil
	}
	traceBlock, balance, ok, err := chain.balanceTrace.AccountTraceAtOrBefore(owner, blockNum)
	if err != nil || !ok {
		return 0, 0, false, err
	}
	if traceBlock > blockNum {
		return 0, 0, false, nil
	}
	return traceBlock, balance, true, nil
}
