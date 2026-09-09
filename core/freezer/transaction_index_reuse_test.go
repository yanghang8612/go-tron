package freezer

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	rawdbfreezer "github.com/tronprotocol/go-tron/core/rawdb/freezer"
	coretypes "github.com/tronprotocol/go-tron/core/types"
)

type transactionReplayStore struct {
	*freezerWriter
	reads        int
	failPublish  error
	afterPublish func()
}

func (s *transactionReplayStore) Ancient(kind string, number uint64) ([]byte, error) {
	if kind == rawdbAncientBlocks {
		s.reads++
	}
	return s.freezerWriter.Ancient(kind, number)
}

func (s *transactionReplayStore) PublishTransactionIndexRun(result rawdbfreezer.TransactionIndexBuildResult) error {
	if s.failPublish != nil {
		return s.failPublish
	}
	if err := s.freezerWriter.PublishTransactionIndexRun(result); err != nil {
		return err
	}
	if s.afterPublish != nil {
		s.afterPublish()
	}
	return nil
}

func newTransactionReplayFixture(t *testing.T) (*Runner, *transactionReplayStore, []common.Hash) {
	t.Helper()
	chain := newFakeChain()
	t.Cleanup(func() { _ = chain.db.Close() })
	fz := newFreezer(t)
	var hashes []common.Hash
	for n := uint64(0); n < 4; n++ {
		body, err := coretypes.UnmarshalBlockBorrowed(auditTransactionBlockBytes(n))
		if err != nil {
			t.Fatal(err)
		}
		hash := body.Transactions()[0].Hash()
		hashes = append(hashes, hash)
		if err := rawdb.WriteTransactionIndex(chain.db, hash[:], n); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := fz.MigrateV2(rawdbfreezer.V2MigrationOptions{
		Tables:        []string{rawdbAncientBlocks, rawdbAncientTxInfos, rawdbAncientStateRoots},
		SegmentBlocks: 4, FrameBlocks: 2, SourceHead: 4, Online: true,
		Source: func(kind string, number uint64) ([]byte, error) {
			if kind == rawdbAncientBlocks {
				return auditTransactionBlockBytes(number), nil
			}
			if kind == rawdbAncientStateRoots {
				return stateRootBytes(number), nil
			}
			return nil, nil
		},
	}); err != nil {
		t.Fatal(err)
	}
	store := &transactionReplayStore{freezerWriter: &freezerWriter{AncientReader: rawdb.NewFreezerReader(fz), f: fz}}
	r := New(chain, store, Config{Enabled: true, V2Enabled: true, TransactionIndexEnabled: true, V2SegmentBlocks: 4, TransactionIndexPrefixBits: 8, SyncActive: func() bool { return true }})
	return r, store, hashes
}

func TestTransactionIndexReplayReadsBodiesOnceAndPreservesColdLookups(t *testing.T) {
	r, store, hashes := newTransactionReplayFixture(t)
	if changed, err := r.MaintainTransactionIndexOnce(); err != nil || !changed {
		t.Fatalf("build/prune = %t/%v", changed, err)
	}
	if store.reads != 4 {
		t.Fatalf("read %d bodies, want one pass over four blocks", store.reads)
	}
	for number, hash := range hashes {
		if got := rawdb.ReadTransactionIndex(rawdb.NewChainDB(r.chain.DB(), nil), hash[:]); got != nil {
			t.Fatalf("hot index %d remains", number)
		}
		locations, err := store.f.TransactionIndexCandidates(hash)
		want, _ := rawdb.EncodeTransactionLocation(uint64(number), 0)
		if err != nil || len(locations) != 1 || locations[0] != want {
			t.Fatalf("cold lookup %d = %v/%v", number, locations, err)
		}
	}
	if p, ok, err := rawdb.ReadStageProgress(r.chain.DB(), rawdb.StageFreezerTxIndexPrune); err != nil || !ok || p != 4 {
		t.Fatalf("durable prune = %d/%t/%v", p, ok, err)
	}
	dir, _ := store.AncientDatadir()
	if entries, err := os.ReadDir(filepath.Join(dir, "tx-index", "etl")); err != nil || len(entries) != 0 {
		t.Fatalf("replay/collector scratch leaked: %v/%v", entries, err)
	}
}

func TestTransactionIndexReplayPublicationFailureAndCanceledPruneRecover(t *testing.T) {
	for _, mode := range []string{"publication", "canceled_prune"} {
		t.Run(mode, func(t *testing.T) {
			r, store, hashes := newTransactionReplayFixture(t)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			if mode == "publication" {
				store.failPublish = errors.New("injected publish failure")
			} else {
				store.afterPublish = cancel
			}
			if _, err := r.maintainTransactionIndexOnceContext(ctx); err == nil {
				t.Fatal("injected failure was lost")
			}
			for _, hash := range hashes {
				if got := rawdb.ReadTransactionIndex(rawdb.NewChainDB(r.chain.DB(), nil), hash[:]); got == nil {
					t.Fatal("failed publication/canceled prune deleted hot lookup")
				}
			}
			if p, _, _ := rawdb.ReadStageProgress(r.chain.DB(), rawdb.StageFreezerTxIndexPrune); p != 0 {
				t.Fatal("failed prune advanced durable cursor")
			}
			store.failPublish, store.afterPublish = nil, nil
			// A new runner has no in-process replay. The orphan run is verified,
			// published when necessary, and the original body scan repays deletion.
			r = New(r.chain, store, r.cfg)
			store.reads = 0
			if changed, err := r.maintainTransactionIndexOnceContext(context.Background()); err != nil || !changed {
				t.Fatalf("recovery = %t/%v", changed, err)
			}
			if store.reads != 4 {
				t.Fatalf("recovery read %d bodies, want four", store.reads)
			}
			for _, hash := range hashes {
				if got := rawdb.ReadTransactionIndex(rawdb.NewChainDB(r.chain.DB(), nil), hash[:]); got != nil {
					t.Fatal("recovery left hot duplicate")
				}
			}
		})
	}
}

func TestTransactionHashReplayBoundedSpillAndCorruption(t *testing.T) {
	for _, spill := range []bool{false, true} {
		s := &transactionHashReplay{dir: t.TempDir(), limit: 64}
		defer s.close()
		rows := 2
		if spill {
			rows = 19
		}
		var expected []byte
		for n := 0; n < rows; n++ {
			hash := make([]byte, 32)
			binary.BigEndian.PutUint64(hash, uint64(n))
			expected = append(expected, hash...)
			if err := s.append(hash); err != nil {
				t.Fatal(err)
			}
			if cap(s.buffer) > s.limit {
				t.Fatalf("actual allocation %d exceeds budget %d", cap(s.buffer), s.limit)
			}
		}
		if err := s.seal(uint64(rows)); err != nil {
			t.Fatal(err)
		}
		if (s.file != nil) != spill {
			t.Fatalf("unexpected spill %t", s.file != nil)
		}
		var got []byte
		if err := s.iterate(context.Background(), func(hash []byte) error { got = append(got, hash...); return nil }); err != nil || !bytes.Equal(got, expected) {
			t.Fatalf("replay differs: %v", err)
		}
		if spill {
			if _, err := s.file.WriteAt([]byte{0xff}, 8); err != nil {
				t.Fatal(err)
			}
			calls := 0
			if err := s.iterate(context.Background(), func([]byte) error { calls++; return nil }); err == nil || calls != 0 {
				t.Fatalf("corrupt chunk consumed: calls=%d err=%v", calls, err)
			}
		}
	}
}
