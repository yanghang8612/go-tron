// ChainDB composes gtron's hot KV store with an AncientReader so that
// chain accessors (slice 2) can transparently fall through to the freezer
// for blocks below the cutoff. Slice 1 ships the type + a constructor;
// callers still take `ethdb.KeyValueStore` directly until slice 2 migrates
// them.

package rawdb

import (
	"errors"

	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb/freezer"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	contractpb "github.com/tronprotocol/go-tron/proto/core/contract"
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

// BalanceTraceReader is an optional cold trace sidecar. It lets archive APIs
// keep account/balance trace reads working after hot rawdb trace rows are
// pruned, without tying rawdb accessors to the snapshot package.
type BalanceTraceReader interface {
	BlockBalanceTrace(blockNum int64) (*contractpb.BlockBalanceTrace, bool, error)
	AccountTraceAtOrBefore(owner []byte, blockNum int64) (traceBlock int64, balance int64, ok bool, err error)
}

// SectionBloomReader is an optional cold section-bloom sidecar. It lets log
// filters keep using section-bloom prefilters after hot `sb-` rows are pruned.
type SectionBloomReader interface {
	SectionBloom(section, bitIndex uint64) ([]byte, bool, error)
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
