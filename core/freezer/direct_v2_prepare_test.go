package freezer

import (
	"bytes"
	"context"
	"fmt"
	"math/rand"
	"testing"

	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	rawdbfreezer "github.com/tronprotocol/go-tron/core/rawdb/freezer"
	coretypes "github.com/tronprotocol/go-tron/core/types"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	"google.golang.org/protobuf/proto"
)

type directV2PreparedChain struct {
	*fakeChain
	bodyReads uint64
}

func (c *directV2PreparedChain) ReadBlockRawStrict(n uint64) ([]byte, bool, error) {
	c.bodyReads++ // Source reads must remain on the caller goroutine.
	return c.fakeChain.ReadBlockRawStrict(n)
}

func (c *directV2PreparedChain) ReadBlockStateRootRaw(tcommon.Hash) ([]byte, error) {
	return nil, nil
}

func newDirectV2PreparedFixture(t testing.TB, blocks, transactions int) *directV2PreparedChain {
	t.Helper()
	chain := &directV2PreparedChain{fakeChain: newFakeChain()}
	rng := rand.New(rand.NewSource(20260909))
	for n := 0; n < blocks; n++ {
		pb := &corepb.Block{BlockHeader: &corepb.BlockHeader{RawData: &corepb.BlockHeaderRaw{Number: int64(n), Timestamp: int64(n) * 3000}}}
		ret := &corepb.TransactionRet{BlockNumber: int64(n)}
		for ordinal := 0; ordinal < transactions; ordinal++ {
			payload := make([]byte, 384)
			_, _ = rng.Read(payload)
			tx := &corepb.Transaction{RawData: &corepb.TransactionRaw{Timestamp: int64(n)*3000 + int64(ordinal), Data: payload}}
			pb.Transactions = append(pb.Transactions, tx)
			ret.Transactioninfo = append(ret.Transactioninfo, &corepb.TransactionInfo{
				Id: coretypes.NewTransactionFromPB(tx).Hash().Bytes(), BlockNumber: int64(n),
				Log: []*corepb.TransactionInfo_Log{{Address: bytes.Repeat([]byte{byte(ordinal)}, 20), Topics: [][]byte{bytes.Repeat([]byte{0x13}, 32)}, Data: payload[:128]}},
			})
		}
		block := coretypes.NewBlockFromPB(pb)
		body, err := block.Marshal()
		if err != nil {
			t.Fatal(err)
		}
		receipts, err := proto.Marshal(ret)
		if err != nil {
			t.Fatal(err)
		}
		chain.blockRaw[uint64(n)] = body
		chain.txInfosRaw[uint64(n)] = receipts
		chain.blockHashByNo[uint64(n)] = block.Hash()
	}
	return chain
}

func TestDirectV2PreparationPreservesValidatedRecords(t *testing.T) {
	for _, external := range []bool{false, true} {
		for _, workers := range []int{1, 4} {
			t.Run(fmt.Sprintf("external_%t/workers_%d", external, workers), func(t *testing.T) {
				chain := newDirectV2PreparedFixture(t, 256, 8)
				store := newFreezer(t)
				r := New(chain, wrapFreezer(store), Config{Enabled: true, V2Enabled: true, V2FrameBlocks: 8, V2SegmentBlocks: 256})
				hashes, err := r.appendDirectV2SegmentWithWorkers(context.Background(), wrapFreezer(store).(V2DirectAppender), 0, 256, external, false, workers)
				if err != nil {
					t.Fatal(err)
				}
				for n := uint64(0); n < 256; n++ {
					if hashes[n] != chain.blockHashByNo[n] {
						t.Fatalf("hash %d mismatch", n)
					}
					compact := rawdb.CompactAncientV2Record
					if external {
						compact = rawdb.CompactAncientV2RecordWithExternalLogs
					}
					want, err := compact(rawdbAncientTxInfos, n, chain.txInfosRaw[n], chain.blockRaw[n])
					if err != nil {
						t.Fatal(err)
					}
					got, err := store.Ancient(rawdbAncientTxInfos, n)
					if err != nil || !bytes.Equal(got, want) {
						t.Fatalf("receipt %d mismatch: %v", n, err)
					}
					got, err = store.Ancient(rawdbAncientBlocks, n)
					if err != nil || !bytes.Equal(got, chain.blockRaw[n]) {
						t.Fatalf("body %d mismatch: %v", n, err)
					}
				}
				// Two full passes plus 256 dictionary samples and six verification
				// samples. The former receipt Source path added another full pass.
				if chain.bodyReads != 2*256+256+6 {
					t.Fatalf("duplicate full body scan remains: %d reads", chain.bodyReads)
				}
			})
		}
	}
}

func TestDirectV2PreparationRejectsBadRecordWithoutPublishing(t *testing.T) {
	for _, kind := range []string{"body", "receipt"} {
		t.Run(kind, func(t *testing.T) {
			chain := newDirectV2PreparedFixture(t, 256, 8)
			if kind == "body" {
				chain.blockRaw[5] = chain.blockRaw[6] // first parallel batch, before dictionary sampling
			} else {
				chain.txInfosRaw[149] = chain.txInfosRaw[150] // valid protobuf, wrong identities
			}
			store := newFreezer(t)
			r := New(chain, wrapFreezer(store), Config{Enabled: true, V2Enabled: true, V2FrameBlocks: 8, V2SegmentBlocks: 256})
			if _, err := r.appendDirectV2SegmentWithWorkers(context.Background(), wrapFreezer(store).(V2DirectAppender), 0, 256, true, false, 4); err == nil {
				t.Fatal("corrupt source accepted")
			}
			if store.V2Coverage() != 0 {
				t.Fatal("corrupt segment published")
			}
		})
	}
}

// Both configurations use the same protobuf corpus, dictionary and frame
// compression workers. This measures preparation parallelism within complete
// three-table migration (including verification/fsync/manifest), not Pebble I/O.
func BenchmarkDirectV2PreparedMigration(b *testing.B) {
	chain := newDirectV2PreparedFixture(b, 1024, 48)
	for _, workers := range []int{1, 4} {
		b.Run(fmt.Sprintf("workers_%d", workers), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				b.StopTimer()
				store, err := rawdbfreezer.NewFreezer(b.TempDir(), "", false, 2049, FreezerTableSet())
				if err != nil {
					b.Fatal(err)
				}
				r := New(chain, wrapFreezer(store), Config{Enabled: true, V2Enabled: true, V2FrameBlocks: 64, V2SegmentBlocks: 1024})
				b.StartTimer()
				_, err = r.appendDirectV2SegmentWithWorkers(context.Background(), wrapFreezer(store).(V2DirectAppender), 0, 1024, true, false, workers)
				b.StopTimer()
				if err != nil {
					store.Close()
					b.Fatal(err)
				}
				stats, err := store.Stats()
				if err != nil {
					b.Fatal(err)
				}
				var size uint64
				for _, table := range stats.Tables {
					size += table.V2Size
				}
				b.ReportMetric(float64(size), "output_B")
				if err := store.Close(); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
