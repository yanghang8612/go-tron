package snapshots

import (
	"fmt"
	"path/filepath"
	"testing"

	"github.com/tronprotocol/go-tron/core/rawdb"
)

// Runs unchanged on ea3cb83b; measures complete authenticated FromChain output
// with realistic contract bodies, not an isolated decoder.
func BenchmarkColdEventDecode(b *testing.B) {
	for _, external := range []bool{false, true} {
		b.Run(fmt.Sprintf("external_%v", external), func(b *testing.B) {
			source := eventIdentityChainFixture(b, 64, 32, 2, 2048)
			db, err := rawdb.NewPebbleDB(b.TempDir(), 64, 64)
			if err != nil {
				b.Fatal(err)
			}
			defer db.Close()
			it := source.NewIterator(nil, nil)
			for it.Next() {
				if err := db.Put(it.Key(), it.Value()); err != nil {
					b.Fatal(err)
				}
			}
			if err := it.Error(); err != nil {
				b.Fatal(err)
			}
			it.Release()
			chain := rawdb.NewChainDB(db, nil)
			if external {
				rows := collectEventIdentityRows(b, eventLogV3ChainReader{chain: chain}, 1, 64, EventLogFilter{})
				chain.SetEventLogReader(replayRangeReader{eventLogRowsReader{rows: rows}})
				for n := uint64(1); n <= 64; n++ {
					raw, ok, err := rawdb.ReadTransactionInfosRawStrict(chain, n)
					if err != nil || !ok {
						b.Fatal(err)
					}
					data, _, err := rawdb.ExternalizeTransactionInfoLogs(raw)
					if err != nil {
						b.Fatal(err)
					}
					if err := rawdb.WriteTransactionInfosRaw(db, n, data); err != nil {
						b.Fatal(err)
					}
				}
			}
			root := b.TempDir()
			var checksum string
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				ref, err := BuildEventLogV4SegmentFromChain(chain, filepath.Join(root, fmt.Sprint(i)), "", 1, 64)
				if err != nil {
					b.Fatal(err)
				}
				if checksum == "" {
					checksum = ref.Checksum
				} else if checksum != ref.Checksum {
					b.Fatal("output changed")
				}
			}
		})
	}
}
