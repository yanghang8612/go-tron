package rawdb

import (
	"context"
	"testing"

	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/tronprotocol/go-tron/common"
)

type ownerSpansBenchView struct{ ethdb.KeyValueStore }

func (*ownerSpansBenchView) IsPinnedKeyValueView() bool { return true }

func BenchmarkHistoryOwnerSpansSelected(b *testing.B) {
	rows := ownerSpansRows()
	// Repeat a realistic mix while leaving the selected owner sparse.
	for len(rows) < 1800 {
		for _, row := range ownerSpansRows() {
			copy := cloneStateDomainChange(row)
			copy.Seq = 5 + uint64(len(rows))
			copy.TxNum = 100 + uint64(len(rows)/3)
			rows = append(rows, copy)
		}
	}
	raw := encodeBorrowedStateDomainChangeTestBlock(b, rows)
	pack, _ := encodeStateDomainChangeBlockStorage(raw)
	index, err := buildHistoryOwnerSpans(raw, pack, 9, rows)
	if err != nil || len(index) == 0 {
		b.Fatal(err)
	}
	db := &ownerSpansBenchView{NewMemoryDatabase()}
	defer db.Close()
	if err := db.Put(stateChangeOwnerSpansKey(9), index); err != nil {
		b.Fatal(err)
	}
	owner := rows[0].Owner
	selected := func(o common.Address) bool { return o == owner }
	for _, indexed := range []bool{false, true} {
		name := "full"
		if indexed {
			name = "spans"
		}
		b.Run(name, func(b *testing.B) {
			b.ReportAllocs()
			b.SetBytes(int64(len(raw)))
			for i := 0; i < b.N; i++ {
				var scratch StateDomainChange
				count := 0
				visit := func(row *StateDomainChange) (bool, error) {
					if selected(row.Owner) {
						count++
					}
					return true, nil
				}
				if indexed {
					used, err := iterateHistoryOwnerSpans(context.Background(), db, pack, 9, selected, &scratch, visit)
					if err != nil || !used {
						b.Fatal(used, err)
					}
				} else if _, err := iteratePersistedStateDomainChangeBlockBorrowedWithScratch(pack, 9, &scratch, visit); err != nil {
					b.Fatal(err)
				}
				if count != 260 {
					b.Fatal("wrong selected count", count)
				}
			}
		})
	}
}

type ownerSpansWriteBench struct {
	ethdb.KeyValueStore
	batch  ethdb.Batch
	atomic bool
}

func (w *ownerSpansWriteBench) StateHistoryChunkWritesAtomic() bool { return w.atomic }
func (w *ownerSpansWriteBench) Put(key, value []byte) error         { return w.batch.Put(key, value) }

func BenchmarkHistoryOwnerSpansWriter(b *testing.B) {
	rows := ownerSpansRows()
	db := NewMemoryDatabase()
	defer db.Close()
	for _, indexed := range []bool{false, true} {
		name := "without-index"
		if indexed {
			name = "with-index"
		}
		b.Run(name, func(b *testing.B) {
			batch := db.NewBatch()
			defer batch.Close()
			w := &ownerSpansWriteBench{KeyValueStore: db, batch: batch, atomic: indexed}
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				batch.Reset()
				if err := writeStateDomainChangeBlockRows(w, rows, false, false); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
