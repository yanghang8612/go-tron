package rawdb

import (
	"bytes"
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

type RebuildTransactionDerivedIndexesResult struct {
	FromBlock               uint64
	ToBlock                 uint64
	BlocksScanned           uint64
	TransactionsIndexed     uint64
	BlocksWithTxInfo        uint64
	TransactionInfosIndexed uint64
	ETL                     etl.Stats
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
		block := ReadBlock(chain, blockNum)
		if block == nil {
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
			if err := collector.PutTransactionInfosByBlock(blockNum, infos); err != nil {
				return nil, err
			}
			result.BlocksWithTxInfo++
			for _, info := range infos {
				if info == nil || len(info.Id) == 0 {
					continue
				}
				if err := collector.PutTransactionInfo(info.Id, info); err != nil {
					return nil, err
				}
				result.TransactionInfosIndexed++
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
		block := ReadBlock(chain, blockNum)
		if block == nil {
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
// hot AccountTrace row. It does not require TransactionBalanceTrace entries:
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
		block := ReadBlock(chain, blockNum)
		if block == nil {
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
					if _, ok, err := readHotAccountTrace(traceReader, addr, int64(blockNum)); err != nil {
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
		block := ReadBlock(chain, blockNum)
		if block == nil {
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
	if len(infos) != len(txs) {
		return fmt.Errorf("rawdb: incomplete transaction info coverage for block %d during %s: have %d entries for %d transactions", blockNum, context, len(infos), len(txs))
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
		existing, exists, err := ReadSectionBloomBitSet(a.reader, a.section, bitIndex)
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
