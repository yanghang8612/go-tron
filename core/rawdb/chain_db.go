// ChainDB composes gtron's hot KV store with an AncientReader so that
// chain accessors (slice 2) can transparently fall through to the freezer
// for blocks below the cutoff. Slice 1 ships the type + a constructor;
// callers still take `ethdb.KeyValueStore` directly until slice 2 migrates
// them.

package rawdb

import (
	"errors"
	"fmt"

	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb/freezer"
	"github.com/tronprotocol/go-tron/core/types"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	contractpb "github.com/tronprotocol/go-tron/proto/core/contract"
	"google.golang.org/protobuf/proto"
)

// ChainDB is the gtron chain database: a hot Pebble store plus an
// AncientReader fall-through for frozen rows.
//
// Embedding `ethdb.KeyValueStore` means every existing accessor that takes
// `ethdb.KeyValueReader` / `ethdb.KeyValueWriter` keeps working without
// signature changes — the new fall-through is opt-in for accessors that
// migrate in slice 2.
type ChainDB struct {
	ethdb.KeyValueStore
	AncientReader
	chainIndex   ChainIndexReader
	balanceTrace BalanceTraceReader
	sectionBloom SectionBloomReader
	eventLog     EventLogReader
}

// ChainIndexReader is an optional cold lookup sidecar. It is defined in rawdb
// instead of snapshots so the chain accessors can use it without importing the
// snapshot package and creating a package cycle.
type ChainIndexReader interface {
	BlockNumberByHash(hash common.Hash) (uint64, bool, error)
	TransactionBlockNumberByHash(hash common.Hash) (uint64, bool, error)
}

// ChainIndexTxLookup is the block-local transaction position returned by cold
// chain-index sidecars.
type ChainIndexTxLookup struct {
	BlockNum uint64
	TxIndex  uint32
}

// ChainIndexTxPositionReader is an optional extension for cold chain-index
// sidecars that can resolve a transaction hash to its block-local index, not
// just the containing block number.
type ChainIndexTxPositionReader interface {
	TransactionIndexByHash(hash common.Hash) (ChainIndexTxLookup, bool, error)
}

// BalanceTraceReader is an optional cold trace sidecar. It lets archive APIs
// keep account/balance trace reads working after hot rawdb trace rows are
// pruned, without tying rawdb accessors to the snapshot package.
type BalanceTraceReader interface {
	BlockBalanceTrace(blockNum int64) (*contractpb.BlockBalanceTrace, bool, error)
	AccountTraceAtOrBefore(owner []byte, blockNum int64) (traceBlock int64, balance int64, ok bool, err error)
}

var _ BalanceTraceReader = (*ChainDB)(nil)

// SectionBloomReader is an optional cold section-bloom sidecar. It lets log
// filters keep using section-bloom prefilters after hot `sb-` rows are pruned.
type SectionBloomReader interface {
	SectionBloom(section, bitIndex uint64) ([]byte, bool, error)
}

// SectionBloomBitSetReader is an optional extension for cold section-bloom
// sidecars that can return already-decoded bitsets. Strict log-prefilter reads
// use it to let verified segment readers validate payloads without forcing an
// extra decode after the raw row is returned.
type SectionBloomBitSetReader interface {
	SectionBloomBitSet(section, bitIndex uint64) ([]byte, bool, error)
}

// EventLogFilter is the raw cold-event-log query shape shared by snapshots and
// JSON-RPC. Topics[i] uses nil/empty as wildcard and non-empty as OR values.
type EventLogFilter struct {
	Addresses []common.Address
	Topics    [][]common.Hash
}

// EventLog is a decoded cold TVM log row with enough positional metadata to
// render an eth_getLogs-compatible response without scanning block bodies.
type EventLog struct {
	BlockNum  uint64
	TxIndex   uint64
	LogIndex  uint64
	TxHash    common.Hash
	BlockHash common.Hash
	Address   common.Address
	Log       *corepb.TransactionInfo_Log
}

// EventLogReader is an optional cold event-log sidecar. It lets log queries
// read verified immutable event-log segments after hot TransactionRet rows have
// been pruned or moved to ancient storage.
type EventLogReader interface {
	EventLogRangeCovered(fromBlock, toBlock uint64) (bool, error)
	IterateEventLogs(fromBlock, toBlock uint64, filter EventLogFilter, fn func(EventLog) (bool, error)) error
}

// FilteredEventLogCoverageReader is an optional extension for cold event-log
// readers with a global lookup sidecar. It allows filtered archive reads to
// verify only the immutable event-log segments that can satisfy the filter.
type FilteredEventLogCoverageReader interface {
	EventLogRangeCoveredForFilter(fromBlock, toBlock uint64, filter EventLogFilter) (bool, error)
}

// CoveredEventLogReader is an optional extension for cold event-log readers
// that can bind coverage verification and iteration to the same underlying
// immutable view.
type CoveredEventLogReader interface {
	IterateCoveredEventLogs(fromBlock, toBlock uint64, filter EventLogFilter, fn func(EventLog) (bool, error)) (bool, error)
}

// NewChainDB wraps a hot KV store and an ancient reader into a `*ChainDB`.
// `anc` may be `NoopAncient{}` when the freezer is disabled or in tests
// that don't want a freezer on disk.
func NewChainDB(kv ethdb.KeyValueStore, anc AncientReader) *ChainDB {
	if anc == nil {
		anc = NoopAncient{}
	}
	return &ChainDB{KeyValueStore: kv, AncientReader: anc}
}

// SetChainIndexReader attaches a cold hash lookup sidecar. Passing nil disables
// the sidecar and leaves all reads on the hot KV/freezer paths.
func (db *ChainDB) SetChainIndexReader(reader ChainIndexReader) {
	if db == nil {
		return
	}
	db.chainIndex = reader
}

// SetBalanceTraceReader attaches a cold account/balance trace sidecar. Passing
// nil disables the sidecar and leaves trace reads on hot rawdb rows only.
func (db *ChainDB) SetBalanceTraceReader(reader BalanceTraceReader) {
	if db == nil {
		return
	}
	db.balanceTrace = reader
}

// BlockBalanceTrace implements BalanceTraceReader over the composed ChainDB
// view: hot rows are preferred and the attached cold sidecar is consulted only
// on a miss.
func (db *ChainDB) BlockBalanceTrace(blockNum int64) (*contractpb.BlockBalanceTrace, bool, error) {
	if db == nil {
		return nil, false, fmt.Errorf("rawdb: nil database during read block balance trace")
	}
	return readBlockBalanceTraceStrict(db, blockNum)
}

// AccountTraceAtOrBefore implements BalanceTraceReader over the composed ChainDB
// view, choosing the newest hot or cold account trace at or below blockNum.
func (db *ChainDB) AccountTraceAtOrBefore(owner []byte, blockNum int64) (int64, int64, bool, error) {
	if db == nil {
		return 0, 0, false, fmt.Errorf("account trace: nil database")
	}
	return ReadAccountTraceAtOrBefore(db, owner, blockNum)
}

// SetSectionBloomReader attaches a cold section-bloom sidecar. Passing nil
// disables the sidecar and leaves bloom reads on hot rawdb rows only.
func (db *ChainDB) SetSectionBloomReader(reader SectionBloomReader) {
	if db == nil {
		return
	}
	db.sectionBloom = reader
}

// SetEventLogReader attaches a cold event-log sidecar. Passing nil disables the
// sidecar and leaves log queries on block/TransactionRet scan paths.
func (db *ChainDB) SetEventLogReader(reader EventLogReader) {
	if db == nil {
		return
	}
	db.eventLog = reader
}

func (db *ChainDB) EventLogRangeCovered(fromBlock, toBlock uint64) (bool, error) {
	if db == nil || db.eventLog == nil {
		return false, nil
	}
	return db.eventLog.EventLogRangeCovered(fromBlock, toBlock)
}

func (db *ChainDB) EventLogRangeCoveredForFilter(fromBlock, toBlock uint64, filter EventLogFilter) (bool, error) {
	if db == nil || db.eventLog == nil {
		return false, nil
	}
	if reader, ok := db.eventLog.(FilteredEventLogCoverageReader); ok {
		return reader.EventLogRangeCoveredForFilter(fromBlock, toBlock, filter)
	}
	return db.eventLog.EventLogRangeCovered(fromBlock, toBlock)
}

func (db *ChainDB) IterateEventLogs(fromBlock, toBlock uint64, filter EventLogFilter, fn func(EventLog) (bool, error)) error {
	if db == nil || db.eventLog == nil {
		return nil
	}
	return db.eventLog.IterateEventLogs(fromBlock, toBlock, filter, fn)
}

func (db *ChainDB) IterateCoveredEventLogs(fromBlock, toBlock uint64, filter EventLogFilter, fn func(EventLog) (bool, error)) (bool, error) {
	if db == nil || db.eventLog == nil {
		return false, nil
	}
	if reader, ok := db.eventLog.(CoveredEventLogReader); ok {
		return reader.IterateCoveredEventLogs(fromBlock, toBlock, filter, db.coveredEventLogValidator(fromBlock, toBlock, filter, fn))
	}
	covered, err := db.EventLogRangeCoveredForFilter(fromBlock, toBlock, filter)
	if err != nil || !covered {
		return covered, err
	}
	return true, db.IterateEventLogs(fromBlock, toBlock, filter, db.coveredEventLogValidator(fromBlock, toBlock, filter, fn))
}

func (db *ChainDB) coveredEventLogValidator(fromBlock, toBlock uint64, filter EventLogFilter, fn func(EventLog) (bool, error)) func(EventLog) (bool, error) {
	var (
		last        EventLog
		hasLast     bool
		blocks      = make(map[uint64]*types.Block)
		blockExists = make(map[uint64]bool)
		infos       = make(map[uint64][]*corepb.TransactionInfo)
		infoExists  = make(map[uint64]bool)
	)
	return func(row EventLog) (bool, error) {
		if err := validateCoveredEventLogRow(fromBlock, toBlock, row); err != nil {
			return false, err
		}
		if !coveredEventLogRowMatchesFilter(row, filter) {
			return true, nil
		}
		if err := db.validateCoveredEventLogCanonicalRow(row, blocks, blockExists, infos, infoExists); err != nil {
			return false, err
		}
		if hasLast && compareEventLogPosition(row, last) <= 0 {
			return false, fmt.Errorf(
				"rawdb: cold event log row block=%d tx=%d log=%d is not after previous block=%d tx=%d log=%d",
				row.BlockNum, row.TxIndex, row.LogIndex,
				last.BlockNum, last.TxIndex, last.LogIndex,
			)
		}
		hasLast = true
		last = row
		return fn(row)
	}
}

func coveredEventLogRowMatchesFilter(row EventLog, filter EventLogFilter) bool {
	if len(filter.Addresses) > 0 {
		matched := false
		for _, address := range filter.Addresses {
			if row.Address == address {
				matched = true
				break
			}
		}
		if !matched {
			return false
		}
	}
	topics := row.Log.GetTopics()
	for pos, candidates := range filter.Topics {
		if len(candidates) == 0 {
			continue
		}
		if pos >= len(topics) {
			return false
		}
		got := common.BytesToHash(topics[pos])
		matched := false
		for _, candidate := range candidates {
			if got == candidate {
				matched = true
				break
			}
		}
		if !matched {
			return false
		}
	}
	return true
}

func (db *ChainDB) validateCoveredEventLogCanonicalRow(row EventLog, blockCache map[uint64]*types.Block, blockOK map[uint64]bool, infoCache map[uint64][]*corepb.TransactionInfo, infoOK map[uint64]bool) error {
	if db == nil {
		return nil
	}
	block, cached := blockCache[row.BlockNum]
	ok := blockOK[row.BlockNum]
	if !cached {
		var err error
		block, ok, err = ReadBlockStrict(db, row.BlockNum)
		if err != nil {
			return fmt.Errorf("rawdb: cold event log row block=%d canonical block read: %w", row.BlockNum, err)
		}
		blockCache[row.BlockNum] = block
		blockOK[row.BlockNum] = ok
	}
	if !ok {
		return nil
	}
	canonicalHash := block.Hash()
	if row.BlockHash != canonicalHash {
		return fmt.Errorf("rawdb: cold event log row block=%d hash %x does not match canonical hash %x", row.BlockNum, row.BlockHash, canonicalHash)
	}
	txs := block.Transactions()
	if row.TxIndex >= uint64(len(txs)) {
		return fmt.Errorf("rawdb: cold event log row block=%d tx index %d outside canonical transaction count %d", row.BlockNum, row.TxIndex, len(txs))
	}
	tx := txs[int(row.TxIndex)]
	if tx == nil {
		return fmt.Errorf("rawdb: cold event log row block=%d tx index %d is nil in canonical block", row.BlockNum, row.TxIndex)
	}
	canonicalTxHash := tx.Hash()
	if row.TxHash != canonicalTxHash {
		return fmt.Errorf("rawdb: cold event log row block=%d tx index %d hash %x does not match canonical transaction hash %x", row.BlockNum, row.TxIndex, row.TxHash, canonicalTxHash)
	}
	infos, cached := infoCache[row.BlockNum]
	hasInfos := infoOK[row.BlockNum]
	if !cached {
		var err error
		infos, hasInfos, err = ReadTransactionInfosByBlockStrict(db, row.BlockNum)
		if err != nil {
			return fmt.Errorf("rawdb: cold event log row block=%d canonical transaction infos read: %w", row.BlockNum, err)
		}
		infoCache[row.BlockNum] = infos
		infoOK[row.BlockNum] = hasInfos
	}
	if !hasInfos {
		return nil
	}
	if err := ValidateTransactionInfosForBlock(row.BlockNum, txs, infos, "covered cold event log row"); err != nil {
		if errors.Is(err, ErrIncompleteTransactionInfoCoverage) {
			return nil
		}
		return fmt.Errorf("rawdb: cold event log row block=%d canonical transaction infos: %w", row.BlockNum, err)
	}
	if err := validateCoveredEventLogCanonicalLog(row, infos); err != nil {
		return err
	}
	return nil
}

func validateCoveredEventLogCanonicalLog(row EventLog, infos []*corepb.TransactionInfo) error {
	var canonicalLogIndex uint64
	for txIndex, info := range infos {
		for _, log := range info.GetLog() {
			if log == nil {
				continue
			}
			if canonicalLogIndex != row.LogIndex {
				canonicalLogIndex++
				continue
			}
			if uint64(txIndex) != row.TxIndex {
				return fmt.Errorf("rawdb: cold event log row block=%d log index %d belongs to canonical tx index %d, not row tx index %d", row.BlockNum, row.LogIndex, txIndex, row.TxIndex)
			}
			if !proto.Equal(row.Log, log) {
				return fmt.Errorf("rawdb: cold event log row block=%d tx=%d log=%d payload does not match canonical transaction info log", row.BlockNum, row.TxIndex, row.LogIndex)
			}
			return nil
		}
	}
	return fmt.Errorf("rawdb: cold event log row block=%d log index %d outside canonical log count %d", row.BlockNum, row.LogIndex, canonicalLogIndex)
}

func validateCoveredEventLogRow(fromBlock, toBlock uint64, row EventLog) error {
	if row.BlockNum < fromBlock || row.BlockNum > toBlock {
		return fmt.Errorf("rawdb: cold event log row block %d outside covered range [%d,%d]", row.BlockNum, fromBlock, toBlock)
	}
	if row.Log == nil {
		return fmt.Errorf("rawdb: cold event log row block=%d tx=%d log=%d is nil", row.BlockNum, row.TxIndex, row.LogIndex)
	}
	payloadAddress := common.BytesToAddress(row.Log.GetAddress())
	if row.Address != payloadAddress {
		return fmt.Errorf("rawdb: cold event log row block=%d tx=%d log=%d address %x does not match payload address %x", row.BlockNum, row.TxIndex, row.LogIndex, row.Address, payloadAddress)
	}
	return nil
}

func compareEventLogPosition(a, b EventLog) int {
	if a.BlockNum != b.BlockNum {
		if a.BlockNum < b.BlockNum {
			return -1
		}
		return 1
	}
	if a.TxIndex != b.TxIndex {
		if a.TxIndex < b.TxIndex {
			return -1
		}
		return 1
	}
	if a.LogIndex != b.LogIndex {
		if a.LogIndex < b.LogIndex {
			return -1
		}
		return 1
	}
	return 0
}

// freezerReader wraps a `*freezer.Freezer` and translates the freezer's
// package-private "out of bounds" / "unknown table" errors into the public
// `ErrNotInAncient` sentinel that slice-2 accessors will key off.
//
// Slice 3 (the freezing goroutine) needs read+write access; this wrapper
// only implements the read half so it composes naturally into `ChainDB`.
type freezerReader struct {
	f *freezer.Freezer
}

// NewFreezerReader adapts a `*freezer.Freezer` so it satisfies
// `AncientReader`. Out-of-bounds reads surface as `ErrNotInAncient`.
func NewFreezerReader(f *freezer.Freezer) AncientReader {
	if f == nil {
		return NoopAncient{}
	}
	return freezerReader{f: f}
}

func (r freezerReader) Ancient(kind string, number uint64) ([]byte, error) {
	data, err := r.f.Ancient(kind, number)
	if err != nil {
		return nil, translateFreezerErr(err)
	}
	return data, nil
}

func (r freezerReader) AncientRange(kind string, start, count, maxBytes uint64) ([][]byte, error) {
	out, err := r.f.AncientRange(kind, start, count, maxBytes)
	if err != nil {
		return nil, translateFreezerErr(err)
	}
	return out, nil
}

func (r freezerReader) AncientCount(kind string) (uint64, error) {
	n, err := r.f.AncientCount(kind)
	if err != nil {
		return 0, translateFreezerErr(err)
	}
	return n, nil
}

func (r freezerReader) HasAncient(kind string, number uint64) (bool, error) {
	ok, err := r.f.HasAncient(kind, number)
	if err != nil {
		return false, translateFreezerErr(err)
	}
	return ok, nil
}

func (r freezerReader) Stats() (freezer.Stats, error) {
	return r.f.Stats()
}

// translateFreezerErr maps the freezer package's internal sentinels to
// public `core/rawdb` errors. Unknown errors pass through unchanged.
func translateFreezerErr(err error) error {
	switch {
	case errors.Is(err, freezer.ErrOutOfBounds):
		return ErrNotInAncient
	case errors.Is(err, freezer.ErrUnknownTable):
		return ErrNotInAncient
	default:
		return err
	}
}
