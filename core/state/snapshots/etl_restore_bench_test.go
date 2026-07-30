package snapshots

import (
	"bytes"
	"fmt"
	"path/filepath"
	"testing"

	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/rawdb/etl"
	"github.com/tronprotocol/go-tron/core/state/kvdomains"
	"github.com/tronprotocol/go-tron/core/types"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	"google.golang.org/protobuf/proto"
)

func BenchmarkSnapshotRestoreETL(b *testing.B) {
	latestRows := makeLatestRestoreBenchRows(256)
	historyChanges, historyRanges := makeStateHistoryRestoreBenchRows(256)
	freezerRows := makeChainFreezerIndexRestoreBenchRows(b, 256)
	chainIndexBlocks, chainIndexTxs := makeChainIndexBuildBenchRows(256)

	b.Run("latest/direct_unordered", func(b *testing.B) {
		benchmarkRestoreWrites(b, false, func(w ethdb.KeyValueWriter, _ string) error {
			return writeLatestRestoreBenchRows(w, latestRows)
		})
	})
	b.Run("latest/sorted_etl", func(b *testing.B) {
		benchmarkRestoreWrites(b, true, func(w ethdb.KeyValueWriter, tempDir string) error {
			collector, err := etl.NewCollector(etl.Options{TempDir: tempDir, BufferLimit: 32 << 10})
			if err != nil {
				return err
			}
			defer collector.Close()
			if err := writeLatestRestoreBenchRows(collector, latestRows); err != nil {
				return err
			}
			_, err = collector.Load(w)
			return err
		})
	})

	b.Run("state_history/direct_unordered", func(b *testing.B) {
		benchmarkRestoreWrites(b, false, func(w ethdb.KeyValueWriter, _ string) error {
			return writeStateHistoryRestoreBenchRows(w, historyChanges, historyRanges)
		})
	})
	b.Run("state_history/sorted_etl", func(b *testing.B) {
		benchmarkRestoreWrites(b, true, func(w ethdb.KeyValueWriter, tempDir string) error {
			collector, err := etl.NewCollector(etl.Options{TempDir: tempDir, BufferLimit: 32 << 10})
			if err != nil {
				return err
			}
			defer collector.Close()
			if err := writeStateHistoryRestoreBenchRows(collector, historyChanges, historyRanges); err != nil {
				return err
			}
			_, err = collector.Load(w)
			return err
		})
	})

	b.Run("chain_freezer_indexes/direct_unordered", func(b *testing.B) {
		benchmarkRestoreWrites(b, false, func(w ethdb.KeyValueWriter, _ string) error {
			return writeChainFreezerIndexRestoreBenchRows(w, freezerRows)
		})
	})
	b.Run("chain_freezer_indexes/sorted_etl", func(b *testing.B) {
		benchmarkRestoreWrites(b, true, func(w ethdb.KeyValueWriter, tempDir string) error {
			collector, err := etl.NewCollector(etl.Options{TempDir: tempDir, BufferLimit: 32 << 10})
			if err != nil {
				return err
			}
			defer collector.Close()
			if err := writeChainFreezerIndexRestoreBenchRows(collector, freezerRows); err != nil {
				return err
			}
			_, err = collector.Load(w)
			return err
		})
	})

	b.Run("chain_index_build/direct_in_memory", func(b *testing.B) {
		benchmarkChainIndexBuild(b, false, chainIndexBlocks, chainIndexTxs)
	})
	b.Run("chain_index_build/sorted_etl", func(b *testing.B) {
		benchmarkChainIndexBuild(b, true, chainIndexBlocks, chainIndexTxs)
	})
}

type restoreBenchFunc func(w ethdb.KeyValueWriter, tempDir string) error

func benchmarkRestoreWrites(b *testing.B, expectSorted bool, restore restoreBenchFunc) {
	b.Helper()
	b.ReportAllocs()
	tempDir := b.TempDir()
	var totalPuts uint64
	var totalOutOfOrder uint64
	for i := 0; i < b.N; i++ {
		sink := newRestoreOrderSink()
		if err := restore(sink, tempDir); err != nil {
			b.Fatal(err)
		}
		if expectSorted && sink.outOfOrder != 0 {
			b.Fatalf("restore stream had %d out-of-order writes, want sorted", sink.outOfOrder)
		}
		if !expectSorted && sink.outOfOrder == 0 {
			b.Fatal("direct restore setup produced no out-of-order writes")
		}
		totalPuts += sink.puts
		totalOutOfOrder += sink.outOfOrder
	}
	if totalPuts > 0 {
		b.ReportMetric(float64(totalOutOfOrder)/float64(totalPuts), "out_of_order/put")
	}
}

type restoreOrderSink struct {
	lastKey    []byte
	puts       uint64
	outOfOrder uint64
}

func newRestoreOrderSink() *restoreOrderSink {
	return &restoreOrderSink{}
}

func (s *restoreOrderSink) Put(key, value []byte) error {
	if len(s.lastKey) != 0 && bytes.Compare(s.lastKey, key) > 0 {
		s.outOfOrder++
	}
	s.lastKey = append(s.lastKey[:0], key...)
	s.puts++
	return nil
}

func (s *restoreOrderSink) Delete(key []byte) error {
	if len(s.lastKey) != 0 && bytes.Compare(s.lastKey, key) > 0 {
		s.outOfOrder++
	}
	s.lastKey = append(s.lastKey[:0], key...)
	return nil
}

type latestRestoreBenchRow struct {
	owner      common.Address
	generation uint64
	domain     kvdomains.KVDomain
	key        []byte
	value      []byte
	account    []byte
	codeHash   common.Hash
	code       []byte
}

func makeLatestRestoreBenchRows(n int) []latestRestoreBenchRow {
	rows := make([]latestRestoreBenchRow, 0, n)
	for i := 0; i < n; i++ {
		code := []byte(fmt.Sprintf("bench-code-%04d", i))
		rows = append(rows, latestRestoreBenchRow{
			owner:      benchmarkAddress(i),
			generation: uint64(i%7 + 1),
			domain:     kvdomains.ContractStorage,
			key:        []byte(fmt.Sprintf("slot/%04d", i)),
			value:      []byte(fmt.Sprintf("value-%04d", i)),
			account:    []byte(fmt.Sprintf("account-%04d", i)),
			codeHash:   common.Keccak256(code),
			code:       code,
		})
	}
	return rows
}

func writeLatestRestoreBenchRows(w ethdb.KeyValueWriter, rows []latestRestoreBenchRow) error {
	for _, row := range rows {
		if err := rawdb.WriteStateCode(w, row.codeHash, row.code); err != nil {
			return err
		}
		if err := rawdb.WriteStateKVLatest(w, row.owner, row.generation, row.domain, row.key, row.value); err != nil {
			return err
		}
		if err := rawdb.WriteStateAccountLatest(w, row.owner, row.account); err != nil {
			return err
		}
		if err := rawdb.WriteStateKVGeneration(w, row.owner, row.generation); err != nil {
			return err
		}
	}
	return nil
}

func makeStateHistoryRestoreBenchRows(n int) ([]*rawdb.StateDomainChange, []*rawdb.StateTxRange) {
	changes := make([]*rawdb.StateDomainChange, 0, n)
	ranges := make([]*rawdb.StateTxRange, 0, n)
	for i := 0; i < n; i++ {
		blockNum := uint64(i + 1)
		txNum := uint64(1_000 + i)
		blockHash := common.BytesToHash([]byte(fmt.Sprintf("history-block-%04d", i)))
		changes = append(changes, &rawdb.StateDomainChange{
			BlockNum:   blockNum,
			BlockHash:  blockHash,
			TxNum:      txNum,
			Seq:        1,
			FlatDomain: rawdb.StateFlatDomainKVLatest,
			Owner:      benchmarkAddress(i),
			Generation: uint64(i%5 + 1),
			Domain:     kvdomains.ContractStorage,
			Key:        []byte(fmt.Sprintf("slot/%04d", i)),
			PrevExists: true,
			Prev:       []byte(fmt.Sprintf("old-%04d", i)),
			NextExists: true,
			Next:       []byte(fmt.Sprintf("new-%04d", i)),
		})
		ranges = append(ranges, &rawdb.StateTxRange{
			BlockNum:   blockNum,
			BlockHash:  blockHash,
			BeginTxNum: txNum,
			EndTxNum:   txNum,
		})
	}
	return changes, ranges
}

func writeStateHistoryRestoreBenchRows(w ethdb.KeyValueWriter, changes []*rawdb.StateDomainChange, ranges []*rawdb.StateTxRange) error {
	for _, change := range changes {
		if err := rawdb.WriteStateDomainChangeRow(w, change); err != nil {
			return err
		}
		if err := rawdb.WriteStateDomainChangeInverseIndex(w, change); err != nil {
			return err
		}
	}
	for _, row := range ranges {
		if err := rawdb.WriteStateTxRange(w, row.BlockNum, row.BlockHash, row.BeginTxNum, row.EndTxNum); err != nil {
			return err
		}
	}
	return nil
}

func makeChainFreezerIndexRestoreBenchRows(b *testing.B, n int) []chainFreezerRow {
	b.Helper()
	rows := make([]chainFreezerRow, 0, n)
	for i := 0; i < n; i++ {
		rows = append(rows, makeChainFreezerIndexRestoreBenchRow(b, uint64(i)))
	}
	return rows
}

func makeChainFreezerIndexRestoreBenchRow(b *testing.B, number uint64) chainFreezerRow {
	b.Helper()
	txPB := &corepb.Transaction{
		RawData: &corepb.TransactionRaw{
			Timestamp:  int64(10_000 + number),
			Expiration: int64(20_000 + number),
		},
	}
	tx := types.NewTransactionFromPB(txPB)
	txHash := tx.Hash()
	block := types.NewBlockFromPB(&corepb.Block{
		BlockHeader: &corepb.BlockHeader{
			RawData: &corepb.BlockHeaderRaw{
				Number:    int64(number),
				Timestamp: int64(30_000 + number),
			},
		},
		Transactions: []*corepb.Transaction{txPB},
	})
	blockRaw, err := block.Marshal()
	if err != nil {
		b.Fatalf("marshal block %d: %v", number, err)
	}
	ret := &corepb.TransactionRet{
		BlockNumber: int64(number),
		Transactioninfo: []*corepb.TransactionInfo{
			{
				Id:          txHash[:],
				Fee:         int64(700 + number),
				BlockNumber: int64(number),
			},
		},
	}
	txInfoRaw, err := proto.Marshal(ret)
	if err != nil {
		b.Fatalf("marshal tx info %d: %v", number, err)
	}
	return chainFreezerRow{
		blockNum:   number,
		blockRaw:   blockRaw,
		txInfosRaw: txInfoRaw,
	}
}

func writeChainFreezerIndexRestoreBenchRows(w ethdb.KeyValueWriter, rows []chainFreezerRow) error {
	for _, row := range rows {
		if _, err := restoreChainFreezerIndexesForRow(w, row); err != nil {
			return err
		}
	}
	return nil
}

func makeChainIndexBuildBenchRows(n int) ([]chainIndexBlockEntry, []chainIndexTxEntry) {
	blocks := make([]chainIndexBlockEntry, 0, n)
	txs := make([]chainIndexTxEntry, 0, n*2)
	for i := 0; i < n; i++ {
		blockNum := uint64(i)
		blocks = append(blocks, chainIndexBlockEntry{
			hash:     common.BytesToHash([]byte(fmt.Sprintf("chain-index-block-%04d", i))),
			blockNum: blockNum,
		})
		txs = append(txs, chainIndexTxEntry{
			hash:     common.BytesToHash([]byte(fmt.Sprintf("chain-index-tx-%04d-a", i))),
			blockNum: blockNum,
			txIndex:  0,
		})
		txs = append(txs, chainIndexTxEntry{
			hash:     common.BytesToHash([]byte(fmt.Sprintf("chain-index-tx-%04d-b", i))),
			blockNum: blockNum,
			txIndex:  1,
		})
	}
	return blocks, txs
}

func benchmarkChainIndexBuild(b *testing.B, useETL bool, blocks []chainIndexBlockEntry, txs []chainIndexTxEntry) {
	b.Helper()
	b.ReportAllocs()
	baseDir := b.TempDir()
	for i := 0; i < b.N; i++ {
		ref := SegmentRef{
			Dataset:   SegmentDatasetChainFreezer,
			Kind:      SegmentChainIndex,
			FromTxNum: 0,
			ToTxNum:   uint64(len(blocks) - 1),
			Path:      fmt.Sprintf("chain/index-bench-%d.idx", i),
		}
		if useETL {
			if err := benchmarkChainIndexBuildETL(baseDir, ref, blocks, txs); err != nil {
				b.Fatal(err)
			}
			continue
		}
		blockCopy := append([]chainIndexBlockEntry(nil), blocks...)
		txCopy := append([]chainIndexTxEntry(nil), txs...)
		sortChainIndexEntries(blockCopy, txCopy)
		if _, err := writeChainIndexSegment(baseDir, ref, blockCopy, txCopy); err != nil {
			b.Fatal(err)
		}
	}
}

func benchmarkChainIndexBuildETL(baseDir string, ref SegmentRef, blocks []chainIndexBlockEntry, txs []chainIndexTxEntry) error {
	collector, err := etl.NewCollector(etl.Options{
		TempDir:     filepath.Join(baseDir, "etl"),
		BufferLimit: 32 << 10,
	})
	if err != nil {
		return err
	}
	defer collector.Close()
	for _, block := range blocks {
		if err := collector.Put(chainIndexBlockETLKey(block.hash, block.blockNum), nil); err != nil {
			return err
		}
	}
	for _, tx := range txs {
		if err := collector.Put(chainIndexTxETLKey(tx.hash, tx.blockNum, tx.txIndex), nil); err != nil {
			return err
		}
	}
	_, err = writeChainIndexSegmentFromETL(baseDir, ref, collector, uint64(len(blocks)))
	return err
}

func benchmarkAddress(i int) common.Address {
	var id common.AccountID
	id[0] = byte(i >> 8)
	id[1] = byte(i)
	for j := 2; j < len(id); j++ {
		id[j] = byte(i + j)
	}
	return id.Address(common.AddressPrefixMainnet)
}
