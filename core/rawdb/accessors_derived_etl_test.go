package rawdb

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/tronprotocol/go-tron/core/rawdb/etl"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	contractpb "github.com/tronprotocol/go-tron/proto/core/contract"
)

func TestDerivedIndexCollectorLoadsSortedRowsAndRoundTrips(t *testing.T) {
	root := t.TempDir()
	tempDir := filepath.Join(root, "etl-scratch")
	collector, err := NewDerivedIndexCollector(etl.Options{TempDir: tempDir, BufferLimit: 1})
	if err != nil {
		t.Fatalf("NewDerivedIndexCollector: %v", err)
	}
	defer collector.Close()

	owner := mustAddr(0x42)
	txID := bytes.Repeat([]byte{0xab}, 32)
	txInfo := &corepb.TransactionInfo{Id: txID, Fee: 123, BlockNumber: 9, BlockTimeStamp: 777}
	if err := collector.PutSectionBloom(3, 42, []byte{0xde, 0xad}); err != nil {
		t.Fatalf("PutSectionBloom: %v", err)
	}
	if err := collector.PutTransactionIndex(txID, 9); err != nil {
		t.Fatalf("PutTransactionIndex: %v", err)
	}
	if err := collector.PutTransactionInfo(txID, txInfo); err != nil {
		t.Fatalf("PutTransactionInfo: %v", err)
	}
	if err := collector.PutTransactionInfosByBlock(9, []*corepb.TransactionInfo{txInfo}); err != nil {
		t.Fatalf("PutTransactionInfosByBlock: %v", err)
	}
	if err := collector.PutBlockBalanceTrace(9, &contractpb.BlockBalanceTrace{Timestamp: 900}); err != nil {
		t.Fatalf("PutBlockBalanceTrace: %v", err)
	}
	if err := collector.PutAccountTrace(owner, 9, 100); err != nil {
		t.Fatalf("PutAccountTrace old: %v", err)
	}
	if err := collector.PutAccountTrace(owner, 9, 200); err != nil {
		t.Fatalf("PutAccountTrace new: %v", err)
	}

	rec := newDerivedIndexRecordingWriter()
	stats, err := collector.Load(rec)
	if err != nil {
		t.Fatalf("Load recording writer: %v", err)
	}
	if stats.SpilledRuns == 0 {
		t.Fatalf("spilled runs = %d, want forced ETL spill", stats.SpilledRuns)
	}
	if rec.outOfOrder != 0 {
		t.Fatalf("derived index load had %d out-of-order keys", rec.outOfOrder)
	}
	if _, err := os.Stat(tempDir); err != nil {
		t.Fatalf("ETL temp parent stat: %v", err)
	}
	if got, ok := ReadAccountTrace(rec, owner, 9); !ok || got != 200 {
		t.Fatalf("ReadAccountTrace = %d/%v, want 200/true", got, ok)
	}
	if got := ReadBlockBalanceTrace(rec, 9); got == nil || got.Timestamp != 900 {
		t.Fatalf("ReadBlockBalanceTrace = %+v, want timestamp 900", got)
	}
	if got := ReadSectionBloom(rec, 3, 42); !bytes.Equal(got, []byte{0xde, 0xad}) {
		t.Fatalf("ReadSectionBloom = %x, want dead", got)
	}

	db := NewMemoryChainDB()
	collector2, err := NewDerivedIndexCollector(etl.Options{TempDir: tempDir, BufferLimit: 1})
	if err != nil {
		t.Fatalf("NewDerivedIndexCollector #2: %v", err)
	}
	defer collector2.Close()
	if err := collector2.PutTransactionIndex(txID, 9); err != nil {
		t.Fatalf("PutTransactionIndex #2: %v", err)
	}
	if err := collector2.PutTransactionInfo(txID, txInfo); err != nil {
		t.Fatalf("PutTransactionInfo #2: %v", err)
	}
	if err := collector2.PutTransactionInfosByBlock(9, []*corepb.TransactionInfo{txInfo}); err != nil {
		t.Fatalf("PutTransactionInfosByBlock #2: %v", err)
	}
	if _, err := collector2.Load(db); err != nil {
		t.Fatalf("Load chain db: %v", err)
	}
	if got := ReadTransactionIndex(db, txID); got == nil || *got != 9 {
		t.Fatalf("ReadTransactionIndex = %v, want 9", got)
	}
	if got := ReadTransactionInfo(db, txID); got == nil || got.Fee != 123 {
		t.Fatalf("ReadTransactionInfo = %+v, want fee 123", got)
	}
	if got := ReadTransactionInfosByBlock(db, 9); len(got) != 1 || got[0].Fee != 123 {
		t.Fatalf("ReadTransactionInfosByBlock = %+v, want one fee 123", got)
	}
}

func TestDerivedIndexCollectorRejectsInvalidRowsAndLifecycle(t *testing.T) {
	collector, err := NewDerivedIndexCollector(etl.Options{TempDir: t.TempDir()})
	if err != nil {
		t.Fatalf("NewDerivedIndexCollector: %v", err)
	}
	if err := collector.PutAccountTrace(nil, 1, 1); err == nil {
		t.Fatal("PutAccountTrace accepted empty owner")
	}
	if err := collector.PutBlockBalanceTrace(1, nil); err == nil {
		t.Fatal("PutBlockBalanceTrace accepted nil trace")
	}
	txID := bytes.Repeat([]byte{0x31}, 32)
	otherID := bytes.Repeat([]byte{0x32}, 32)
	if err := collector.PutTransactionInfo(txID, &corepb.TransactionInfo{Id: otherID}); err == nil {
		t.Fatal("PutTransactionInfo accepted mismatched transaction id")
	}
	if err := collector.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := collector.PutSectionBloom(1, 2, []byte{0x01}); !errors.Is(err, etl.ErrCollectorClosed) {
		t.Fatalf("PutSectionBloom after Close error = %v, want %v", err, etl.ErrCollectorClosed)
	}
}

type derivedIndexRecordingWriter struct {
	data       map[string][]byte
	lastKey    []byte
	outOfOrder uint64
	puts       uint64
}

func newDerivedIndexRecordingWriter() *derivedIndexRecordingWriter {
	return &derivedIndexRecordingWriter{data: make(map[string][]byte)}
}

func (w *derivedIndexRecordingWriter) Put(key, value []byte) error {
	if len(w.lastKey) != 0 && bytes.Compare(w.lastKey, key) > 0 {
		w.outOfOrder++
	}
	w.lastKey = append(w.lastKey[:0], key...)
	w.data[string(key)] = append([]byte(nil), value...)
	w.puts++
	return nil
}

func (w *derivedIndexRecordingWriter) Delete(key []byte) error {
	if len(w.lastKey) != 0 && bytes.Compare(w.lastKey, key) > 0 {
		w.outOfOrder++
	}
	w.lastKey = append(w.lastKey[:0], key...)
	delete(w.data, string(key))
	return nil
}

func (w *derivedIndexRecordingWriter) Get(key []byte) ([]byte, error) {
	value, ok := w.data[string(key)]
	if !ok {
		return nil, errors.New("not found")
	}
	return append([]byte(nil), value...), nil
}

func (w *derivedIndexRecordingWriter) Has(key []byte) (bool, error) {
	_, ok := w.data[string(key)]
	return ok, nil
}

func BenchmarkDerivedIndexCollector(b *testing.B) {
	rows := makeDerivedIndexBenchRows(256)
	b.Run("direct_unordered", func(b *testing.B) {
		benchmarkDerivedIndexWrites(b, false, func(w *derivedIndexRecordingWriter, _ string) error {
			return writeDerivedIndexBenchRowsDirect(w, rows)
		})
	})
	b.Run("sorted_etl", func(b *testing.B) {
		benchmarkDerivedIndexWrites(b, true, func(w *derivedIndexRecordingWriter, tempDir string) error {
			return writeDerivedIndexBenchRowsETL(w, tempDir, rows)
		})
	})
}

type derivedIndexBenchRow struct {
	owner    []byte
	blockNum uint64
	txID     []byte
	info     *corepb.TransactionInfo
	trace    *contractpb.BlockBalanceTrace
	section  uint64
	bitIndex uint64
	bloom    []byte
}

type derivedIndexBenchFunc func(w *derivedIndexRecordingWriter, tempDir string) error

func benchmarkDerivedIndexWrites(b *testing.B, expectSorted bool, write derivedIndexBenchFunc) {
	b.Helper()
	b.ReportAllocs()
	tempDir := b.TempDir()
	var totalPuts uint64
	var totalOutOfOrder uint64
	for i := 0; i < b.N; i++ {
		sink := newDerivedIndexRecordingWriter()
		if err := write(sink, tempDir); err != nil {
			b.Fatal(err)
		}
		if expectSorted && sink.outOfOrder != 0 {
			b.Fatalf("derived index stream had %d out-of-order writes, want sorted", sink.outOfOrder)
		}
		if !expectSorted && sink.outOfOrder == 0 {
			b.Fatal("direct derived index setup produced no out-of-order writes")
		}
		totalPuts += sink.puts
		totalOutOfOrder += sink.outOfOrder
	}
	if totalPuts > 0 {
		b.ReportMetric(float64(totalOutOfOrder)/float64(totalPuts), "out_of_order/put")
	}
}

func makeDerivedIndexBenchRows(n int) []derivedIndexBenchRow {
	rows := make([]derivedIndexBenchRow, 0, n)
	for i := 0; i < n; i++ {
		txID := make([]byte, 32)
		txID[0] = byte(i >> 8)
		txID[1] = byte(i)
		for j := 2; j < len(txID); j++ {
			txID[j] = byte(i + j)
		}
		blockNum := uint64(i + 1)
		rows = append(rows, derivedIndexBenchRow{
			owner:    mustAddr(byte(i)),
			blockNum: blockNum,
			txID:     txID,
			info: &corepb.TransactionInfo{
				Id:             txID,
				Fee:            int64(1_000 + i),
				BlockNumber:    int64(blockNum),
				BlockTimeStamp: int64(10_000 + i),
			},
			trace: &contractpb.BlockBalanceTrace{
				Timestamp: int64(20_000 + i),
			},
			section:  uint64(i / 256),
			bitIndex: uint64(i % 256),
			bloom:    []byte(fmt.Sprintf("bloom-%04d", i)),
		})
	}
	return rows
}

func writeDerivedIndexBenchRowsDirect(w *derivedIndexRecordingWriter, rows []derivedIndexBenchRow) error {
	for _, row := range rows {
		if err := WriteSectionBloom(w, row.section, row.bitIndex, row.bloom); err != nil {
			return err
		}
		if err := WriteTransactionIndex(w, row.txID, row.blockNum); err != nil {
			return err
		}
		if err := WriteTransactionInfo(w, row.txID, row.info); err != nil {
			return err
		}
		if err := WriteTransactionInfosByBlock(w, row.blockNum, []*corepb.TransactionInfo{row.info}); err != nil {
			return err
		}
		if err := WriteBlockBalanceTrace(w, int64(row.blockNum), row.trace); err != nil {
			return err
		}
		if err := WriteAccountTrace(w, row.owner, int64(row.blockNum), row.info.Fee); err != nil {
			return err
		}
	}
	return nil
}

func writeDerivedIndexBenchRowsETL(w *derivedIndexRecordingWriter, tempDir string, rows []derivedIndexBenchRow) error {
	collector, err := NewDerivedIndexCollector(etl.Options{TempDir: tempDir, BufferLimit: 32 << 10})
	if err != nil {
		return err
	}
	defer collector.Close()
	for _, row := range rows {
		if err := collector.PutSectionBloom(row.section, row.bitIndex, row.bloom); err != nil {
			return err
		}
		if err := collector.PutTransactionIndex(row.txID, row.blockNum); err != nil {
			return err
		}
		if err := collector.PutTransactionInfo(row.txID, row.info); err != nil {
			return err
		}
		if err := collector.PutTransactionInfosByBlock(row.blockNum, []*corepb.TransactionInfo{row.info}); err != nil {
			return err
		}
		if err := collector.PutBlockBalanceTrace(int64(row.blockNum), row.trace); err != nil {
			return err
		}
		if err := collector.PutAccountTrace(row.owner, int64(row.blockNum), row.info.Fee); err != nil {
			return err
		}
	}
	_, err = collector.Load(w)
	return err
}
