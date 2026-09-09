package freezer

import (
	"bytes"
	"context"
	"crypto/sha256"
	"os"
	"path/filepath"
	"testing"

	"github.com/tronprotocol/go-tron/core/rawdb"
	rawdbfreezer "github.com/tronprotocol/go-tron/core/rawdb/freezer"
	coretypes "github.com/tronprotocol/go-tron/core/types"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	"google.golang.org/protobuf/proto"
)

// The two modes use identical immutable bodies and real run publication. DB
// setup, reopening the cold store (empty frame cache), and verification are
// excluded from timing. This isolates the saved body copy/hash scan; memorydb
// deletion does not model production Pebble WAL/compaction latency.
func BenchmarkTransactionIndexHashReuse(b *testing.B) {
	const blocks = 8192
	dir := b.TempDir()
	payload := bytes.Repeat([]byte("transaction-index-replay-payload/"), 16)
	bodyFor := func(number uint64) []byte {
		block := &corepb.Block{BlockHeader: &corepb.BlockHeader{RawData: &corepb.BlockHeaderRaw{Number: int64(number)}}}
		for ordinal := 0; ordinal < 16; ordinal++ {
			block.Transactions = append(block.Transactions, &corepb.Transaction{RawData: &corepb.TransactionRaw{
				Timestamp: int64(number*16 + uint64(ordinal) + 1), Data: payload,
			}})
		}
		data, err := proto.Marshal(block)
		if err != nil {
			b.Fatal(err)
		}
		return data
	}
	fz, err := rawdbfreezer.NewFreezer(dir, "", false, 1<<30, FreezerTableSet())
	if err != nil {
		b.Fatal(err)
	}
	if _, err := fz.MigrateV2(rawdbfreezer.V2MigrationOptions{
		Tables:        []string{rawdbAncientBlocks, rawdbAncientTxInfos, rawdbAncientStateRoots},
		SegmentBlocks: blocks, FrameBlocks: 64, SourceHead: blocks, Online: true,
		Source: func(kind string, number uint64) ([]byte, error) {
			if kind == rawdbAncientBlocks {
				return bodyFor(number), nil
			}
			if kind == rawdbAncientStateRoots {
				return stateRootBytes(number), nil
			}
			return nil, nil
		},
	}); err != nil {
		_ = fz.Close()
		b.Fatal(err)
	}
	if err := fz.Close(); err != nil {
		b.Fatal(err)
	}
	var expected [32]byte
	var haveExpected bool
	for _, reuse := range []bool{false, true} {
		name := "double_scan"
		if reuse {
			name = "hash_replay"
		}
		b.Run(name, func(b *testing.B) {
			b.ReportAllocs()
			var bodyReads int
			var outputBytes int
			for iteration := 0; iteration < b.N; iteration++ {
				b.StopTimer()
				if err := os.RemoveAll(filepath.Join(dir, "tx-index")); err != nil {
					b.Fatal(err)
				}
				fz, err := rawdbfreezer.NewFreezer(dir, "", false, 1<<30, FreezerTableSet())
				if err != nil {
					b.Fatal(err)
				}
				chain := newFakeChain()
				for number := uint64(0); number < blocks; number++ {
					block, err := coretypes.UnmarshalBlockBorrowed(bodyFor(number))
					if err != nil {
						b.Fatal(err)
					}
					for _, tx := range block.Transactions() {
						hash := tx.Hash()
						if err := rawdb.WriteTransactionIndex(chain.db, hash[:], number); err != nil {
							b.Fatal(err)
						}
					}
				}
				store := &transactionReplayStore{freezerWriter: &freezerWriter{AncientReader: rawdb.NewFreezerReader(fz), f: fz}}
				r := New(chain, store, Config{Enabled: true, V2Enabled: true, V2SegmentBlocks: blocks, TransactionIndexEnabled: true, TransactionIndexPrefixBits: 20})
				b.StartTimer()
				if reuse {
					_, err = r.buildAndPruneTransactionIndexContext(context.Background(), blocks)
				} else {
					_, err = r.ensureTransactionIndexCoverageContext(context.Background(), blocks)
					if err == nil {
						_, err = r.pruneTransactionIndexDebtContext(context.Background(), blocks)
					}
				}
				b.StopTimer()
				if err != nil {
					b.Fatal(err)
				}
				bodyReads += store.reads
				data, err := os.ReadFile(rawdbfreezer.TransactionIndexRunPath(dir, 0, blocks))
				if err != nil {
					b.Fatal(err)
				}
				outputBytes = len(data)
				got := sha256.Sum256(data)
				if haveExpected && got != expected {
					b.Fatal("hash reuse changed immutable index bytes")
				}
				expected, haveExpected = got, true
				if p, ok, err := rawdb.ReadStageProgress(chain.db, rawdb.StageFreezerTxIndexPrune); err != nil || !ok || p != blocks {
					b.Fatalf("prune stage=%d/%t/%v", p, ok, err)
				}
				if err := fz.Close(); err != nil {
					b.Fatal(err)
				}
				_ = chain.db.Close()
			}
			b.ReportMetric(float64(bodyReads)/float64(b.N), "body-reads/op")
			b.ReportMetric(float64(outputBytes), "index-bytes/op")
			b.ReportMetric(blocks*16, "transactions/op")
		})
	}
}
