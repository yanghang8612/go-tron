package rawdb

import (
	"errors"
	"fmt"
	"sort"

	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb/etl"
	corepb "github.com/tronprotocol/go-tron/proto/core"
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
		infos := ReadTransactionInfosByBlock(chain, blockNum)
		if len(infos) != 0 {
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
		infos := ReadTransactionInfosByBlock(chain, blockNum)
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
		hash := common.Keccak256(data)
		for _, movement := range []uint64{
			(uint64(hash[0]&0x07) << 8) | uint64(hash[1]),
			(uint64(hash[2]&0x07) << 8) | uint64(hash[3]),
			(uint64(hash[4]&0x07) << 8) | uint64(hash[5]),
		} {
			seen[sectionBloomBitIndex(movement)] = struct{}{}
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

func sectionBloomBitIndex(movement uint64) uint64 {
	byteIndex := SectionBloomByteSize - 1 - movement/8
	return byteIndex*8 + movement%8
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
