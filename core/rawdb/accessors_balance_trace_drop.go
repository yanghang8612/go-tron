package rawdb

import (
	"errors"
	"fmt"

	"github.com/ethereum/go-ethereum/ethdb"
)

// BalanceTraceKeyspaceBounds returns the half-open key ranges occupied by the
// java-tron-specific block and account balance trace stores. Callers may use
// the ranges for targeted compaction after DropBalanceTraceKeyspaces commits
// its range tombstones.
func BalanceTraceKeyspaceBounds() (blockStart, blockLimit, accountStart, accountLimit []byte) {
	return append([]byte(nil), balanceTracePrefix...), prefixUpperBound(balanceTracePrefix),
		append([]byte(nil), accountTracePrefix...), prefixUpperBound(accountTracePrefix)
}

// DropBalanceTraceKeyspaces atomically removes every hot BlockBalanceTrace and
// AccountTrace row. Ethereum-compatible historical state remains in
// StateTxRange/StateDomainChange and is deliberately outside these ranges.
func DropBalanceTraceKeyspaces(db ethdb.KeyValueStore) error {
	if db == nil {
		return errors.New("rawdb: nil database while dropping balance traces")
	}
	blockStart, blockLimit, accountStart, accountLimit := BalanceTraceKeyspaceBounds()
	batch := db.NewBatch()
	defer batch.Close()
	if err := batch.DeleteRange(blockStart, blockLimit); err != nil {
		return fmt.Errorf("rawdb: delete block balance trace range: %w", err)
	}
	if err := batch.DeleteRange(accountStart, accountLimit); err != nil {
		return fmt.Errorf("rawdb: delete account trace range: %w", err)
	}
	if err := batch.Write(); err != nil {
		return fmt.Errorf("rawdb: commit balance trace range deletion: %w", err)
	}
	if err := db.SyncKeyValue(); err != nil {
		return fmt.Errorf("rawdb: sync balance trace range deletion: %w", err)
	}
	return nil
}
