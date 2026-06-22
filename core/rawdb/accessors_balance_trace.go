package rawdb

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/tronprotocol/go-tron/common"
	contractpb "github.com/tronprotocol/go-tron/proto/core/contract"
	"google.golang.org/protobuf/proto"
)

// WriteBlockBalanceTrace stores the per-block balance trace. Mirrors
// java-tron BalanceTraceStore.putBlockBalanceTrace; gated on
// isHistoryBalanceLookup — callers must check the flag before writing.
func WriteBlockBalanceTrace(db ethdb.KeyValueWriter, blockNum int64, trace *contractpb.BlockBalanceTrace) error {
	if err := validateBlockBalanceTraceForKey(blockNum, trace, "write block balance trace"); err != nil {
		return err
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
	trace, ok, err := ReadBlockBalanceTraceStrict(db, blockNum)
	if err != nil || !ok {
		return nil
	}
	return trace
}

// ReadBlockBalanceTraceStrict returns the BlockBalanceTrace plus an existence
// flag and surfaces malformed or mismatched hot/cold rows as data errors.
func ReadBlockBalanceTraceStrict(db ethdb.KeyValueReader, blockNum int64) (*contractpb.BlockBalanceTrace, bool, error) {
	return readBlockBalanceTraceStrict(db, blockNum)
}

// HasBlockBalanceTrace reports whether a trace is stored for blockNum.
func HasBlockBalanceTrace(db ethdb.KeyValueReader, blockNum int64) bool {
	return ReadBlockBalanceTrace(db, blockNum) != nil
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

func readBlockBalanceTraceStrict(db ethdb.KeyValueReader, blockNum int64) (*contractpb.BlockBalanceTrace, bool, error) {
	if db == nil {
		return nil, false, fmt.Errorf("rawdb: nil database during read block balance trace")
	}
	key := balanceTraceKey(blockNum)
	exists, err := db.Has(key)
	if err != nil {
		return nil, false, err
	}
	if exists {
		data, err := db.Get(key)
		if err != nil {
			return nil, false, err
		}
		if len(data) != 0 {
			var trace contractpb.BlockBalanceTrace
			if err := proto.Unmarshal(data, &trace); err != nil {
				return nil, true, err
			}
			if err := validateBlockBalanceTraceForKey(blockNum, &trace, "read block balance trace"); err != nil {
				return &trace, true, err
			}
			if err := validateBlockBalanceTraceCanonicalHash(db, blockNum, &trace, "read block balance trace"); err != nil {
				return &trace, true, err
			}
			return &trace, true, nil
		}
	}

	chain, ok := db.(*ChainDB)
	if !ok || chain == nil || chain.balanceTrace == nil {
		return nil, false, nil
	}
	trace, ok, err := chain.balanceTrace.BlockBalanceTrace(blockNum)
	if err != nil || !ok {
		return nil, ok, err
	}
	if err := validateBlockBalanceTraceForKey(blockNum, trace, "read cold block balance trace"); err != nil {
		return trace, true, err
	}
	if err := validateBlockBalanceTraceCanonicalHash(db, blockNum, trace, "read cold block balance trace"); err != nil {
		return trace, true, err
	}
	return trace, true, nil
}

func validateBlockBalanceTraceForKey(blockNum int64, trace *contractpb.BlockBalanceTrace, context string) error {
	if trace == nil {
		return errors.New("rawdb: nil BlockBalanceTrace")
	}
	if id := trace.GetBlockIdentifier(); id != nil && id.GetNumber() != blockNum {
		return fmt.Errorf("rawdb: block balance trace payload number %d does not match key %d during %s", id.GetNumber(), blockNum, context)
	}
	return nil
}

func validateBlockBalanceTraceCanonicalHash(db ethdb.KeyValueReader, blockNum int64, trace *contractpb.BlockBalanceTrace, context string) error {
	id := trace.GetBlockIdentifier()
	if id == nil || len(id.GetHash()) == 0 || blockNum < 0 {
		return nil
	}
	if len(id.GetHash()) != common.HashLength {
		return fmt.Errorf("rawdb: block balance trace payload hash length %d during %s", len(id.GetHash()), context)
	}
	chain, ok := db.(*ChainDB)
	if !ok || chain == nil {
		return nil
	}
	block, exists, err := ReadBlockStrict(chain, uint64(blockNum))
	if err != nil {
		return fmt.Errorf("rawdb: block balance trace canonical block read during %s: %w", context, err)
	}
	if !exists {
		return nil
	}
	canonicalHash := block.Hash()
	if !bytes.Equal(id.GetHash(), canonicalHash.Bytes()) {
		return fmt.Errorf("rawdb: block balance trace payload hash %x does not match canonical block %d hash %x during %s", id.GetHash(), blockNum, canonicalHash, context)
	}
	return nil
}
