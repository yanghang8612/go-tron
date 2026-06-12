package rawdb

import (
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/ethereum/go-ethereum/ethdb"
	contractpb "github.com/tronprotocol/go-tron/proto/core/contract"
	"google.golang.org/protobuf/proto"
)

// WriteBlockBalanceTrace stores the per-block balance trace. Mirrors
// java-tron BalanceTraceStore.putBlockBalanceTrace; gated on
// isHistoryBalanceLookup — callers must check the flag before writing.
func WriteBlockBalanceTrace(db ethdb.KeyValueWriter, blockNum int64, trace *contractpb.BlockBalanceTrace) error {
	if trace == nil {
		return errors.New("rawdb: nil BlockBalanceTrace")
	}
	data, err := proto.Marshal(trace)
	if err != nil {
		return err
	}
	return db.Put(balanceTraceKey(blockNum), data)
}

// ReadBlockBalanceTrace returns the BlockBalanceTrace for blockNum, or nil
// if absent.
func ReadBlockBalanceTrace(db ethdb.KeyValueReader, blockNum int64) *contractpb.BlockBalanceTrace {
	data, err := db.Get(balanceTraceKey(blockNum))
	if err != nil || len(data) == 0 {
		return readColdBlockBalanceTrace(db, blockNum)
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
	if ok {
		return true
	}
	return readColdBlockBalanceTrace(db, blockNum) != nil
}

// DeleteBlockBalanceTrace removes the balance trace for blockNum.
func DeleteBlockBalanceTrace(db ethdb.KeyValueWriter, blockNum int64) error {
	return db.Delete(balanceTraceKey(blockNum))
}

// IterateBlockBalanceTraceRows walks hot BlockBalanceTrace rows in ascending
// block order over the inclusive block range. It deliberately does not consult
// cold snapshot sidecars; snapshot builders and repair tools use it to freeze
// the current hot rawdb trace keyspace.
func IterateBlockBalanceTraceRows(db ethdb.Iteratee, fromBlock, toBlock int64, fn func(blockNum int64, raw []byte) (bool, error)) error {
	if db == nil {
		return fmt.Errorf("rawdb: nil balance trace iterator")
	}
	if fn == nil {
		return fmt.Errorf("rawdb: nil balance trace callback")
	}
	if fromBlock < 0 || toBlock < 0 {
		return fmt.Errorf("rawdb: negative balance trace range [%d,%d]", fromBlock, toBlock)
	}
	if toBlock < fromBlock {
		return fmt.Errorf("rawdb: inverted balance trace range [%d,%d]", fromBlock, toBlock)
	}
	var start [8]byte
	binary.BigEndian.PutUint64(start[:], uint64(fromBlock))
	it := db.NewIterator(balanceTracePrefix, start[:])
	defer it.Release()
	for it.Next() {
		key := it.Key()
		if len(key) != len(balanceTracePrefix)+8 {
			return fmt.Errorf("rawdb: malformed balance trace key length %d", len(key))
		}
		blockNum := int64(binary.BigEndian.Uint64(key[len(balanceTracePrefix):]))
		if blockNum > toBlock {
			break
		}
		keepGoing, err := fn(blockNum, append([]byte(nil), it.Value()...))
		if err != nil {
			return err
		}
		if !keepGoing {
			break
		}
	}
	return it.Error()
}

func readColdBlockBalanceTrace(db ethdb.KeyValueReader, blockNum int64) *contractpb.BlockBalanceTrace {
	chain, ok := db.(*ChainDB)
	if !ok || chain == nil || chain.balanceTrace == nil {
		return nil
	}
	trace, ok, err := chain.balanceTrace.BlockBalanceTrace(blockNum)
	if err != nil || !ok {
		return nil
	}
	if id := trace.GetBlockIdentifier(); id != nil && id.GetNumber() != blockNum {
		return nil
	}
	return trace
}
