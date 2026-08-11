package rawdb

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"sort"

	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb/etl"
	"github.com/tronprotocol/go-tron/core/types"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	contractpb "github.com/tronprotocol/go-tron/proto/core/contract"
)

var (
	ErrIncompleteTransactionInfoCoverage   = errors.New("rawdb: incomplete transaction info coverage")
	ErrTransactionLookupRebuildInterrupted = errors.New("rawdb: transaction lookup rebuild interrupted")
	ErrStateHistoryIndexRebuildInterrupted = errors.New("rawdb: state history index rebuild interrupted")
)

type RebuildTransactionDerivedIndexesResult struct {
	FromBlock           uint64
	ToBlock             uint64
	BlocksScanned       uint64
	TransactionsIndexed uint64
	BlocksWithTxInfo    uint64
	// TransactionInfosIndexed counts receipt entries validated and published in
	// per-block TransactionRet rows. It does not imply ti-<txid> rows were
	// materialized; receipt-by-ID resolves through tx- plus tib-.
	TransactionInfosIndexed uint64
	ETL                     etl.Stats
}

// RebuildTransactionLookupResult describes one transaction hash lookup rebuild
// pass. Unlike RebuildTransactionDerivedIndexesFromBlocks it deliberately does
// not touch transaction receipts or per-block receipt rows: those are produced
// on the canonical execution path and are not a prerequisite for tx lookup.
type RebuildTransactionLookupResult struct {
	FromBlock            uint64
	ToBlock              uint64
	BlocksScanned        uint64
	AncientBlocksScanned uint64
	HotBlocksScanned     uint64
	TransactionsIndexed  uint64
	ETL                  etl.Stats
}

// RebuildStateHistoryIndexResult describes one posting-256 pass over
// authoritative temporal changesets. ChangesScanned counts journal rows;
// PostingRows counts immutable frames after latest-key/block deduplication.
type RebuildStateHistoryIndexResult struct {
	FromBlock      uint64
	ToBlock        uint64
	BlocksScanned  uint64
	ChangesScanned uint64
	DirectoryRows  uint64
	PostingRows    uint64
	ETL            etl.Stats
}

type RebuildSectionBloomsResult struct {
	FromBlock                  uint64
	ToBlock                    uint64
	BlocksScanned              uint64
	BlocksWithTransactionInfos uint64
	BlocksWithLogs             uint64
	LogEntriesIndexed          uint64
	BloomItemsIndexed          uint64
	BloomBitsIndexed           uint64
	SectionBloomRows           uint64
	ETL                        etl.Stats
}

type RebuildAccountTracesResult struct {
	FromBlock              uint64
	ToBlock                uint64
	BlocksScanned          uint64
	BlocksWithBalanceTrace uint64
	TransactionsScanned    uint64
	OperationsApplied      uint64
	AccountTraceRows       uint64
	ETL                    etl.Stats
}

type BalanceTraceCoverageIssue struct {
	BlockNum uint64
	Kind     string
	Detail   string
}

type AuditBlockBalanceTraceCoverageResult struct {
	FromBlock                   uint64
	ToBlock                     uint64
	BlocksScanned               uint64
	BlocksWithBalanceTrace      uint64
	BlocksWithEmptyTxTrace      uint64
	MissingBlockBalanceTrace    uint64
	MissingAccountTrace         uint64
	MismatchedBlockBalanceTrace uint64
	Issues                      []BalanceTraceCoverageIssue
}

func (r *AuditBlockBalanceTraceCoverageResult) Complete() bool {
	return r != nil && r.MissingBlockBalanceTrace == 0 && r.MissingAccountTrace == 0 && r.MismatchedBlockBalanceTrace == 0
}

type accountTraceRebuildReader interface {
	ethdb.KeyValueReader
	ethdb.Iteratee
}

// RebuildTransactionDerivedIndexesFromBlocks rebuilds transaction lookup/info
// rows from retained canonical blocks plus existing per-block TransactionRet
// rows. It is intended for offline repair/backfill paths, not the per-block
// consensus execution hot path.
func RebuildTransactionDerivedIndexesFromBlocks(chain *ChainDB, writer ethdb.KeyValueWriter, fromBlock, toBlock uint64, opts etl.Options) (*RebuildTransactionDerivedIndexesResult, error) {
	if chain == nil {
		return nil, errors.New("rawdb: nil chain db")
	}
	if writer == nil {
		return nil, errors.New("rawdb: nil derived index writer")
	}
	if toBlock < fromBlock {
		return nil, fmt.Errorf("rawdb: inverted transaction derived index rebuild range [%d,%d]", fromBlock, toBlock)
	}
	collector, err := NewDerivedIndexCollector(opts)
	if err != nil {
		return nil, err
	}
	defer collector.Close()

	result := &RebuildTransactionDerivedIndexesResult{
		FromBlock: fromBlock,
		ToBlock:   toBlock,
	}
	for blockNum := fromBlock; ; blockNum++ {
		block, ok, err := ReadBlockStrict(chain, blockNum)
		if err != nil {
			return nil, err
		}
		if !ok {
			return nil, fmt.Errorf("rawdb: missing block %d during transaction derived index rebuild", blockNum)
		}
		result.BlocksScanned++
		for _, tx := range block.Transactions() {
			if tx == nil {
				continue
			}
			txHash := tx.Hash()
			if err := collector.PutTransactionIndex(txHash[:], blockNum); err != nil {
				return nil, err
			}
			result.TransactionsIndexed++
		}
		infos, hasInfos, err := ReadTransactionInfosByBlockStrict(chain, blockNum)
		if err != nil {
			return nil, err
		}
		if hasInfos {
			if err := ValidateTransactionInfosForBlock(blockNum, block.Transactions(), infos, "transaction derived index rebuild"); err != nil {
				return nil, err
			}
			if err := collector.PutCompactTransactionInfosByBlock(blockNum, infos); err != nil {
				return nil, err
			}
			for _, info := range infos {
				if err := collector.DeleteTransactionInfo(info.Id); err != nil {
					return nil, err
				}
			}
			result.BlocksWithTxInfo++
			// Keep the one authoritative TransactionRet payload by block. Receipt
			// reads resolve an individual transaction through tx- plus tib-, so
			// rebuilding another full ti- protobuf for every transaction would
			// only reintroduce the duplicate canonical write removed from sync. A
			// legacy ti- row is removed only after this verified coverage exists;
			// without it, that row may be the sole recoverable receipt payload.
			result.TransactionInfosIndexed += uint64(len(infos))
		}
		if blockNum == toBlock {
			break
		}
	}
	stats, err := collector.Load(writer)
	if err != nil {
		return nil, err
	}
	result.ETL = stats
	return result, nil
}

// RebuildTransactionLookupFromBlocks rebuilds only tx-hash to block-number
// rows from retained canonical block bodies. It is the recoverable TxLookup
// stage payload used after bulk sync; keeping it independent lets execution
// avoid unordered tx- writes without delaying receipt availability.
func RebuildTransactionLookupFromBlocks(chain *ChainDB, writer ethdb.KeyValueWriter, fromBlock, toBlock uint64, opts etl.Options) (*RebuildTransactionLookupResult, error) {
	return RebuildTransactionLookupFromBlocksInterruptible(chain, writer, fromBlock, toBlock, opts, nil)
}

// RebuildTransactionLookupFromBlocksInterruptible rebuilds tx-hash lookup
// rows while allowing a sync-service shutdown to abandon an unpublished ETL
// pass. Ancient bodies are fetched as one contiguous range and hot Pebble
// bodies are consumed by a single prefix iterator; this avoids the Has+Get
// random-point-read pair previously issued for every block.
func RebuildTransactionLookupFromBlocksInterruptible(chain *ChainDB, writer ethdb.KeyValueWriter, fromBlock, toBlock uint64, opts etl.Options, interrupted func() bool) (*RebuildTransactionLookupResult, error) {
	if chain == nil {
		return nil, errors.New("rawdb: nil chain db")
	}
	if writer == nil {
		return nil, errors.New("rawdb: nil transaction lookup writer")
	}
	if toBlock < fromBlock {
		return nil, fmt.Errorf("rawdb: inverted transaction lookup rebuild range [%d,%d]", fromBlock, toBlock)
	}
	collector, err := NewDerivedIndexCollector(opts)
	if err != nil {
		return nil, err
	}
	defer collector.Close()

	result := &RebuildTransactionLookupResult{FromBlock: fromBlock, ToBlock: toBlock}
	consume := func(blockNum uint64, encoded []byte, ancient bool) error {
		if interrupted != nil && interrupted() {
			return ErrTransactionLookupRebuildInterrupted
		}
		block, err := types.UnmarshalBlock(encoded)
		if err != nil {
			return fmt.Errorf("rawdb: block %d decode: %w", blockNum, err)
		}
		if block.Number() != blockNum {
			return fmt.Errorf("rawdb: block row %d contains block number %d", blockNum, block.Number())
		}
		result.BlocksScanned++
		if ancient {
			result.AncientBlocksScanned++
		} else {
			result.HotBlocksScanned++
		}
		if HasAncientTransactionIndex(chain, blockNum) {
			return nil
		}
		for _, tx := range block.Transactions() {
			if tx == nil {
				continue
			}
			txHash := tx.Hash()
			if err := collector.PutTransactionIndex(txHash[:], blockNum); err != nil {
				return err
			}
			result.TransactionsIndexed++
		}
		return nil
	}
	if err := iterateTransactionLookupBlockRange(chain, fromBlock, toBlock, consume); err != nil {
		return nil, err
	}
	if interrupted != nil && interrupted() {
		return nil, ErrTransactionLookupRebuildInterrupted
	}
	stats, err := collector.LoadInterruptible(writer, interrupted)
	if err != nil {
		if errors.Is(err, etl.ErrLoadInterrupted) {
			return nil, ErrTransactionLookupRebuildInterrupted
		}
		return nil, err
	}
	result.ETL = stats
	return result, nil
}

// RebuildStateHistoryIndexInterruptible collects hash -> block postings and a
// logical-key directory from canonical temporal changesets, then loads them in
// physical key order. expectedHash binds every StateTxRange source row to the canonical
// branch before its derived rows are published. A failed or interrupted pass
// leaves the stage watermark to the caller; partially loaded puts are
// idempotently overwritten by the next pass. Keep the source scans range-based:
// per-block iterators repeatedly rebuild block-buffer snapshot overlays while
// catching up, turning this stage into the dominant early-sync CPU cost.
func RebuildStateHistoryIndexInterruptible(source StateKVHistoryReader, writer ethdb.KeyValueWriter, fromBlock, toBlock uint64, opts etl.Options, expectedHash func(uint64) (common.Hash, bool, error), interrupted func() bool) (*RebuildStateHistoryIndexResult, error) {
	if source == nil {
		return nil, errors.New("rawdb: nil state history source")
	}
	if writer == nil {
		return nil, errors.New("rawdb: nil state history index writer")
	}
	if expectedHash == nil {
		return nil, errors.New("rawdb: nil canonical hash reader")
	}
	if toBlock < fromBlock {
		return nil, fmt.Errorf("rawdb: inverted state history index range [%d,%d]", fromBlock, toBlock)
	}
	collector, err := newStateChangePostingCollector(fromBlock, toBlock, opts)
	if err != nil {
		return nil, err
	}
	defer collector.Close()

	blockCount := toBlock - fromBlock + 1
	if blockCount == 0 || blockCount > uint64(^uint(0)>>1) {
		return nil, fmt.Errorf("rawdb: state history index range [%d,%d] is too large", fromBlock, toBlock)
	}
	blockHashes := make([]common.Hash, int(blockCount))
	result := &RebuildStateHistoryIndexResult{FromBlock: fromBlock, ToBlock: toBlock}
	if err := IterateStateTxRangesByBlockRangeBorrowed(source, fromBlock, toBlock, func(txRange *StateTxRange) (bool, error) {
		if interrupted != nil && interrupted() {
			return false, ErrStateHistoryIndexRebuildInterrupted
		}
		expectedBlock := fromBlock + result.BlocksScanned
		if txRange.BlockNum != expectedBlock {
			if txRange.BlockNum > expectedBlock {
				return false, fmt.Errorf("rawdb: missing state tx range %d during history index rebuild", expectedBlock)
			}
			return false, fmt.Errorf("rawdb: state tx ranges are not ordered at block %d during history index rebuild", txRange.BlockNum)
		}
		canonicalHash, ok, err := expectedHash(txRange.BlockNum)
		if err != nil {
			return false, fmt.Errorf("rawdb: read canonical hash %d during history index rebuild: %w", txRange.BlockNum, err)
		}
		if !ok {
			return false, fmt.Errorf("rawdb: missing canonical hash %d during history index rebuild", txRange.BlockNum)
		}
		if txRange.BlockHash != canonicalHash {
			return false, fmt.Errorf("rawdb: state tx range %d canonical mismatch: row block=%d hash=%x canonical=%x", txRange.BlockNum, txRange.BlockNum, txRange.BlockHash, canonicalHash)
		}
		blockHashes[result.BlocksScanned] = txRange.BlockHash
		result.BlocksScanned++
		return true, nil
	}); err != nil {
		return nil, err
	}
	if result.BlocksScanned != blockCount {
		return nil, fmt.Errorf("rawdb: missing state tx range %d during history index rebuild", fromBlock+result.BlocksScanned)
	}
	if err := IterateStateDomainChangesByBlockRange(source, fromBlock, toBlock, func(change *StateDomainChange) (bool, error) {
		if interrupted != nil && interrupted() {
			return false, ErrStateHistoryIndexRebuildInterrupted
		}
		if change.BlockNum < fromBlock || change.BlockNum > toBlock {
			return false, fmt.Errorf("rawdb: state domain change block %d outside history index rebuild range [%d,%d]", change.BlockNum, fromBlock, toBlock)
		}
		change.BlockHash = blockHashes[change.BlockNum-fromBlock]
		if err := collector.Collect(change); err != nil {
			return false, err
		}
		result.ChangesScanned++
		return true, nil
	}); err != nil {
		return nil, err
	}
	if interrupted != nil && interrupted() {
		return nil, ErrStateHistoryIndexRebuildInterrupted
	}
	build, err := collector.Load(writer, interrupted)
	if err != nil {
		return nil, err
	}
	result.DirectoryRows = build.DirectoryRows
	result.PostingRows = build.PostingRows
	result.ETL = build.ETL
	return result, nil
}

func iterateTransactionLookupBlockRange(chain *ChainDB, fromBlock, toBlock uint64, consume func(uint64, []byte, bool) error) error {
	next := fromBlock
	hasAncient, err := chain.HasAncient(ancientBlocks, next)
	if err != nil {
		return fmt.Errorf("rawdb: check ancient block %d: %w", next, err)
	}
	if hasAncient {
		count := toBlock - next + 1
		rows, rangeErr := chain.AncientRange(ancientBlocks, next, count, 0)
		if rangeErr != nil {
			return fmt.Errorf("rawdb: read ancient block range [%d,%d]: %w", next, toBlock, rangeErr)
		}
		for _, encoded := range rows {
			if len(encoded) == 0 {
				return fmt.Errorf("rawdb: empty ancient block %d during transaction lookup rebuild", next)
			}
			if err := consume(next, encoded, true); err != nil {
				return err
			}
			next++
		}
	}
	if next > toBlock {
		return nil
	}

	var seek [8]byte
	binary.BigEndian.PutUint64(seek[:], next)
	it := chain.NewIterator(blockPrefix, seek[:])
	defer it.Release()
	for expected := next; ; expected++ {
		if !it.Next() {
			if err := it.Error(); err != nil {
				return fmt.Errorf("rawdb: iterate block %d during transaction lookup rebuild: %w", expected, err)
			}
			return fmt.Errorf("rawdb: missing block %d during transaction lookup rebuild", expected)
		}
		key := it.Key()
		if len(key) != len(blockPrefix)+8 {
			return fmt.Errorf("rawdb: malformed block key %x during transaction lookup rebuild", key)
		}
		blockNum := binary.BigEndian.Uint64(key[len(blockPrefix):])
		if blockNum != expected {
			return fmt.Errorf("rawdb: missing block %d during transaction lookup rebuild (next %d)", expected, blockNum)
		}
		if err := consume(blockNum, it.Value(), false); err != nil {
			return err
		}
		if expected == toBlock {
			return nil
		}
	}
}

// RebuildAccountTracesFromBlockBalanceTraces rebuilds AccountTrace rows from
// retained BlockBalanceTrace operation diffs. It does not synthesize missing
// BlockBalanceTrace rows; callers that do not have them must re-execute blocks
// with history-balance capture enabled.
func RebuildAccountTracesFromBlockBalanceTraces(chain *ChainDB, traceReader accountTraceRebuildReader, writer ethdb.KeyValueWriter, fromBlock, toBlock uint64, opts etl.Options) (*RebuildAccountTracesResult, error) {
	if chain == nil {
		return nil, errors.New("rawdb: nil chain db")
	}
	if traceReader == nil {
		return nil, errors.New("rawdb: nil balance trace reader")
	}
	if writer == nil {
		return nil, errors.New("rawdb: nil account trace writer")
	}
	if toBlock < fromBlock {
		return nil, fmt.Errorf("rawdb: inverted account trace rebuild range [%d,%d]", fromBlock, toBlock)
	}
	if toBlock > math.MaxInt64 {
		return nil, fmt.Errorf("rawdb: account trace rebuild to-block %d exceeds int64 block number range", toBlock)
	}
	collector, err := NewDerivedIndexCollector(opts)
	if err != nil {
		return nil, err
	}
	defer collector.Close()

	result := &RebuildAccountTracesResult{
		FromBlock: fromBlock,
		ToBlock:   toBlock,
	}
	balances := make(map[string]int64)
	loadBalance := func(addr []byte) (int64, error) {
		key := string(addr)
		if bal, ok := balances[key]; ok {
			return bal, nil
		}
		if fromBlock == 0 {
			balances[key] = 0
			return 0, nil
		}
		_, bal, ok, err := ReadAccountTraceAtOrBefore(traceReader, addr, int64(fromBlock-1))
		if err != nil {
			return 0, err
		}
		if !ok {
			bal = 0
		}
		balances[key] = bal
		return bal, nil
	}

	for blockNum := fromBlock; ; blockNum++ {
		block, ok, err := ReadBlockStrict(chain, blockNum)
		if err != nil {
			return nil, err
		}
		if !ok {
			return nil, fmt.Errorf("rawdb: missing block %d during account trace rebuild", blockNum)
		}
		result.BlocksScanned++
		trace, hasTrace, err := readBlockBalanceTraceStrict(traceReader, int64(blockNum))
		if err != nil {
			return nil, err
		}
		if hasTrace {
			if err := validateBlockBalanceTraceForRebuild(blockNum, block.Hash().Bytes(), trace); err != nil {
				return nil, err
			}
			result.BlocksWithBalanceTrace++
			touched := make(map[string][]byte)
			for _, txTrace := range trace.GetTransactionBalanceTrace() {
				if txTrace == nil {
					continue
				}
				result.TransactionsScanned++
				for _, op := range txTrace.GetOperation() {
					if op == nil {
						continue
					}
					addr := op.GetAddress()
					if len(addr) != common.AddressLength {
						return nil, fmt.Errorf("rawdb: malformed balance trace address length %d at block %d", len(addr), blockNum)
					}
					bal, err := loadBalance(addr)
					if err != nil {
						return nil, err
					}
					next, err := addBalanceTraceAmount(bal, op.GetAmount(), blockNum, addr)
					if err != nil {
						return nil, err
					}
					key := string(addr)
					balances[key] = next
					touched[key] = append([]byte(nil), addr...)
					result.OperationsApplied++
				}
			}
			keys := make([]string, 0, len(touched))
			for key := range touched {
				keys = append(keys, key)
			}
			sort.Strings(keys)
			for _, key := range keys {
				if err := collector.PutAccountTrace(touched[key], int64(blockNum), balances[key]); err != nil {
					return nil, err
				}
				result.AccountTraceRows++
			}
		}
		if blockNum == toBlock {
			break
		}
	}
	stats, err := collector.Load(writer)
	if err != nil {
		return nil, err
	}
	result.ETL = stats
	return result, nil
}

// AuditBlockBalanceTraceCoverage checks that every retained canonical block in
// the range has a BlockBalanceTrace row whose payload identifies that block,
// and that every account touched by those trace operations has an exact-height
// hot or cold AccountTrace row. It does not require TransactionBalanceTrace entries:
// java-tron-compatible history can legitimately emit an empty per-tx trace for
// blocks that only touch balances outside a transaction or do not touch
// balances at all.
func AuditBlockBalanceTraceCoverage(chain *ChainDB, traceReader ethdb.KeyValueReader, fromBlock, toBlock uint64, maxIssues int) (*AuditBlockBalanceTraceCoverageResult, error) {
	if chain == nil {
		return nil, errors.New("rawdb: nil chain db")
	}
	if traceReader == nil {
		return nil, errors.New("rawdb: nil balance trace reader")
	}
	if toBlock < fromBlock {
		return nil, fmt.Errorf("rawdb: inverted balance trace coverage range [%d,%d]", fromBlock, toBlock)
	}
	if toBlock > math.MaxInt64 {
		return nil, fmt.Errorf("rawdb: balance trace coverage to-block %d exceeds int64 block number range", toBlock)
	}
	if maxIssues < 0 {
		maxIssues = 0
	}
	result := &AuditBlockBalanceTraceCoverageResult{
		FromBlock: fromBlock,
		ToBlock:   toBlock,
	}
	addIssue := func(blockNum uint64, kind, detail string) {
		if maxIssues == 0 || len(result.Issues) >= maxIssues {
			return
		}
		result.Issues = append(result.Issues, BalanceTraceCoverageIssue{
			BlockNum: blockNum,
			Kind:     kind,
			Detail:   detail,
		})
	}
	for blockNum := fromBlock; ; blockNum++ {
		block, ok, err := ReadBlockStrict(chain, blockNum)
		if err != nil {
			return nil, err
		}
		if !ok {
			return nil, fmt.Errorf("rawdb: missing block %d during balance trace coverage audit", blockNum)
		}
		result.BlocksScanned++
		trace, hasTrace, err := readBlockBalanceTraceStrict(traceReader, int64(blockNum))
		if err != nil {
			if trace != nil {
				result.BlocksWithBalanceTrace++
				if len(trace.GetTransactionBalanceTrace()) == 0 {
					result.BlocksWithEmptyTxTrace++
				}
			}
			result.MismatchedBlockBalanceTrace++
			addIssue(blockNum, "mismatch", err.Error())
		} else if !hasTrace {
			result.MissingBlockBalanceTrace++
			addIssue(blockNum, "missing", "missing BlockBalanceTrace")
		} else {
			result.BlocksWithBalanceTrace++
			if len(trace.GetTransactionBalanceTrace()) == 0 {
				result.BlocksWithEmptyTxTrace++
			}
			if err := validateBlockBalanceTraceForRebuild(blockNum, block.Hash().Bytes(), trace); err != nil {
				result.MismatchedBlockBalanceTrace++
				addIssue(blockNum, "mismatch", err.Error())
			} else {
				touched := make(map[string][]byte)
				for _, txTrace := range trace.GetTransactionBalanceTrace() {
					if txTrace == nil {
						continue
					}
					for _, op := range txTrace.GetOperation() {
						if op == nil {
							continue
						}
						addr := op.GetAddress()
						if len(addr) != common.AddressLength {
							result.MismatchedBlockBalanceTrace++
							addIssue(blockNum, "mismatch", fmt.Sprintf("malformed balance trace address length %d", len(addr)))
							continue
						}
						touched[string(addr)] = append([]byte(nil), addr...)
					}
				}
				for _, addr := range touched {
					if _, ok, err := ReadAccountTraceStrict(traceReader, addr, int64(blockNum)); err != nil {
						return nil, err
					} else if !ok {
						result.MissingAccountTrace++
						addIssue(blockNum, "missing-account", fmt.Sprintf("missing AccountTrace owner=%x", addr))
					}
				}
			}
		}
		if blockNum == toBlock {
			break
		}
	}
	return result, nil
}

// RebuildSectionBloomsFromTransactionInfos rebuilds java-tron-compatible
// section-bloom rows from retained canonical blocks and their TransactionRet
// log payloads. Existing section rows are read and ORed before writing so
// partial-range repair does not clear block bits outside the requested range.
func RebuildSectionBloomsFromTransactionInfos(chain *ChainDB, sectionReader ethdb.KeyValueReader, writer ethdb.KeyValueWriter, fromBlock, toBlock uint64, opts etl.Options) (*RebuildSectionBloomsResult, error) {
	if chain == nil {
		return nil, errors.New("rawdb: nil chain db")
	}
	if sectionReader == nil {
		return nil, errors.New("rawdb: nil section bloom reader")
	}
	if writer == nil {
		return nil, errors.New("rawdb: nil section bloom writer")
	}
	if toBlock < fromBlock {
		return nil, fmt.Errorf("rawdb: inverted section bloom rebuild range [%d,%d]", fromBlock, toBlock)
	}
	collector, err := NewDerivedIndexCollector(opts)
	if err != nil {
		return nil, err
	}
	defer collector.Close()

	result := &RebuildSectionBloomsResult{
		FromBlock: fromBlock,
		ToBlock:   toBlock,
	}
	var acc *sectionBloomAccumulator
	flush := func() error {
		if acc == nil {
			return nil
		}
		rows, err := acc.flush(collector)
		if err != nil {
			return err
		}
		result.SectionBloomRows += rows
		acc = nil
		return nil
	}

	for blockNum := fromBlock; ; blockNum++ {
		block, ok, err := ReadBlockStrict(chain, blockNum)
		if err != nil {
			return nil, err
		}
		if !ok {
			return nil, fmt.Errorf("rawdb: missing block %d during section bloom rebuild", blockNum)
		}
		result.BlocksScanned++
		infos, _, err := ReadTransactionInfosByBlockStrict(chain, blockNum)
		if err != nil {
			return nil, err
		}
		if err := ValidateTransactionInfosForBlock(blockNum, block.Transactions(), infos, "section bloom rebuild"); err != nil {
			return nil, err
		}
		if len(infos) != 0 {
			result.BlocksWithTransactionInfos++
		}
		bits, logs, items := sectionBloomBitsFromTransactionInfos(infos)
		if len(bits) != 0 {
			result.BlocksWithLogs++
			result.LogEntriesIndexed += logs
			result.BloomItemsIndexed += items
			result.BloomBitsIndexed += uint64(len(bits))
			section := blockNum / SectionBloomBlockPerSection
			if acc == nil || acc.section != section {
				if err := flush(); err != nil {
					return nil, err
				}
				acc = newSectionBloomAccumulator(sectionReader, section)
			}
			blockOffset := blockNum % SectionBloomBlockPerSection
			for _, bitIndex := range bits {
				if err := acc.set(bitIndex, blockOffset); err != nil {
					return nil, err
				}
			}
		}
		if blockNum == toBlock {
			break
		}
	}
	if err := flush(); err != nil {
		return nil, err
	}
	stats, err := collector.Load(writer)
	if err != nil {
		return nil, err
	}
	result.ETL = stats
	return result, nil
}

// ValidateTransactionInfosForBlock verifies that per-block TransactionInfo rows
// describe the canonical transaction list before derived log indexes trust them.
func ValidateTransactionInfosForBlock(blockNum uint64, txs []*types.Transaction, infos []*corepb.TransactionInfo, context string) error {
	// java-tron's genesis allocation transactions materialize the initial state,
	// but they are not executed as ordinary block transactions and therefore do
	// not have TransactionInfo receipts. Preserve those transactions in the
	// canonical/cold body while accepting the intentionally empty receipt row.
	if blockNum == 0 && len(infos) == 0 {
		return nil
	}
	if len(infos) != len(txs) {
		return fmt.Errorf("%w for block %d during %s: have %d entries for %d transactions", ErrIncompleteTransactionInfoCoverage, blockNum, context, len(infos), len(txs))
	}
	for txIndex, info := range infos {
		if info == nil {
			return fmt.Errorf("rawdb: nil transaction info at block %d index %d during %s", blockNum, txIndex, context)
		}
		if info.BlockNumber != 0 && info.BlockNumber != int64(blockNum) {
			return fmt.Errorf("rawdb: transaction info block number %d at block %d index %d during %s", info.BlockNumber, blockNum, txIndex, context)
		}
		tx := txs[txIndex]
		if tx == nil {
			return fmt.Errorf("rawdb: nil canonical transaction at block %d index %d during %s", blockNum, txIndex, context)
		}
		if len(info.Id) == 0 {
			continue
		}
		if len(info.Id) != common.HashLength {
			return fmt.Errorf("rawdb: transaction info id length %d at block %d index %d during %s", len(info.Id), blockNum, txIndex, context)
		}
		txHash := tx.Hash()
		if !bytes.Equal(info.Id, txHash[:]) {
			return fmt.Errorf("rawdb: transaction info id %x does not match canonical tx %x at block %d index %d during %s", info.Id, txHash[:], blockNum, txIndex, context)
		}
	}
	return nil
}

func validateBlockBalanceTraceForRebuild(blockNum uint64, blockHash []byte, trace *contractpb.BlockBalanceTrace) error {
	id := trace.GetBlockIdentifier()
	if id == nil {
		return nil
	}
	if id.GetNumber() != int64(blockNum) {
		return fmt.Errorf("rawdb: block balance trace payload number %d does not match key %d", id.GetNumber(), blockNum)
	}
	if len(id.GetHash()) == 0 {
		return nil
	}
	if len(id.GetHash()) != common.HashLength {
		return fmt.Errorf("rawdb: block balance trace payload hash length %d at block %d", len(id.GetHash()), blockNum)
	}
	if !bytes.Equal(id.GetHash(), blockHash) {
		return fmt.Errorf("rawdb: block balance trace payload hash does not match canonical block %d", blockNum)
	}
	return nil
}

func addBalanceTraceAmount(balance, amount int64, blockNum uint64, addr []byte) (int64, error) {
	if amount > 0 && balance > math.MaxInt64-amount {
		return 0, fmt.Errorf("rawdb: account trace balance overflow at block %d account %x", blockNum, addr)
	}
	if amount == math.MinInt64 || (amount < 0 && balance < -amount) {
		return 0, fmt.Errorf("rawdb: negative account trace balance at block %d account %x", blockNum, addr)
	}
	next := balance + amount
	if next < 0 {
		return 0, fmt.Errorf("rawdb: negative account trace balance at block %d account %x", blockNum, addr)
	}
	return next, nil
}

type sectionBloomAccumulator struct {
	reader  ethdb.KeyValueReader
	section uint64
	rows    map[uint64][]byte
}

func newSectionBloomAccumulator(reader ethdb.KeyValueReader, section uint64) *sectionBloomAccumulator {
	return &sectionBloomAccumulator{
		reader:  reader,
		section: section,
		rows:    make(map[uint64][]byte),
	}
}

func (a *sectionBloomAccumulator) set(bitIndex, blockOffset uint64) error {
	row, ok := a.rows[bitIndex]
	if !ok {
		existing, exists, err := ReadSectionBloomBitSetStrict(a.reader, a.section, bitIndex)
		if err != nil {
			return err
		}
		if exists {
			row = append([]byte(nil), existing...)
		}
	}
	row = setSectionBloomBit(row, blockOffset)
	a.rows[bitIndex] = row
	return nil
}

func (a *sectionBloomAccumulator) flush(collector *DerivedIndexCollector) (uint64, error) {
	if a == nil || len(a.rows) == 0 {
		return 0, nil
	}
	bitIndexes := make([]uint64, 0, len(a.rows))
	for bitIndex := range a.rows {
		bitIndexes = append(bitIndexes, bitIndex)
	}
	sort.Slice(bitIndexes, func(i, j int) bool { return bitIndexes[i] < bitIndexes[j] })
	var rows uint64
	for _, bitIndex := range bitIndexes {
		bitset := trimTrailingZeroes(a.rows[bitIndex])
		if len(bitset) == 0 {
			continue
		}
		value, err := EncodeSectionBloomBitSet(bitset)
		if err != nil {
			return rows, err
		}
		if err := collector.PutSectionBloom(a.section, bitIndex, value); err != nil {
			return rows, err
		}
		rows++
	}
	return rows, nil
}

func sectionBloomBitsFromTransactionInfos(infos []*corepb.TransactionInfo) ([]uint64, uint64, uint64) {
	seen := make(map[uint64]struct{})
	var logs uint64
	var items uint64
	add := func(data []byte) {
		for _, bitIndex := range SectionBloomBitIndexes(data) {
			seen[bitIndex] = struct{}{}
		}
		items++
	}
	for _, info := range infos {
		if info == nil {
			continue
		}
		for _, log := range info.GetLog() {
			if log == nil {
				continue
			}
			logs++
			add(log.GetAddress())
			for _, topic := range log.GetTopics() {
				add(topic)
			}
		}
	}
	if len(seen) == 0 {
		return nil, logs, items
	}
	bits := make([]uint64, 0, len(seen))
	for bitIndex := range seen {
		bits = append(bits, bitIndex)
	}
	sort.Slice(bits, func(i, j int) bool { return bits[i] < bits[j] })
	return bits, logs, items
}

func setSectionBloomBit(bitset []byte, bit uint64) []byte {
	byteIndex := bit / 8
	if byteIndex >= uint64(len(bitset)) {
		grown := make([]byte, byteIndex+1)
		copy(grown, bitset)
		bitset = grown
	}
	bitset[byteIndex] |= 1 << (bit % 8)
	return bitset
}
