package state

import (
	"bytes"
	"fmt"
	"testing"

	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/blockbuffer"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/state/kvdomains"
	corepb "github.com/tronprotocol/go-tron/proto/core"
)

// The old live route is the oracle. Both arms receive exactly the same logical
// sequence, but own independent databases, commitments and pending overlays.
func TestCanonicalStorageScopeLifecycleDifferential(t *testing.T) {
	var reference []tcommon.Hash
	for _, enabled := range []bool{false, true} {
		t.Run(fmt.Sprint(enabled), func(t *testing.T) {
			disk, err := rawdb.NewPebbleDB(t.TempDir(), 16, 16)
			if err != nil {
				t.Fatal(err)
			}
			defer disk.Close()
			history, err := rawdb.NewPebbleDB(t.TempDir(), 16, 16)
			if err != nil {
				t.Fatal(err)
			}
			defer history.Close()
			db := rawdb.NewDirectHistoryStore(disk, history)
			s, err := New(tcommon.Hash{}, NewDatabase(rawdb.WrapKeyValueStore(db)))
			if err != nil {
				t.Fatal(err)
			}
			addr := testAddr(0xfa)
			s.CreateAccount(addr, corepb.AccountType_Contract)
			var slots [16]tcommon.Hash
			for i := range slots {
				slots[i][31] = byte(i + 1)
				if i%4 != 0 {
					s.SetState(addr, slots[i], tcommon.Hash{31: byte(i + 1)})
				}
			}
			if _, err := s.Commit(); err != nil {
				t.Fatal(err)
			}
			buffer := blockbuffer.New(db)
			buffer.SetBaseReadCacheSize(1 << 20)
			s.SetAccountKVIndexStore(buffer)
			commits := s.NewCommitScope()
			defer commits.Discard()
			for block := uint64(1); block <= 12; block++ {
				buffer.BeginBlock(tcommon.Hash{31: byte(block)}, block)
				s.stateObjects[addr].releaseStorage()
				closeScope := func() error { return nil }
				if enabled {
					closeScope, err = s.BeginCanonicalStorageReadScope(block)
					if err != nil || s.storageReadScope == nil {
						t.Fatalf("open %v", err)
					}
					for _, entry := range []struct {
						name string
						copy func() (*StateDB, error)
					}{{"Copy", s.Copy}, {"CopyBlockExecutionBase", s.CopyBlockExecutionBase}} {
						copy, err := entry.copy()
						if err != nil {
							t.Fatalf("%s: %v", entry.name, err)
						}
						if copy.storageReadScope != nil || copy.storageReadScopeReader != nil {
							t.Fatalf("%s inherited canonical scope", entry.name)
						}
					}
				}
				// A completed older commit can flush and prune its pending map
				// after capture. The retained layer and base are a matching cut.
				if block > 1 {
					if err := buffer.FlushUpTo(block-1, db); err != nil {
						t.Fatal(err)
					}
					commits.latestWriter.prunePending(block - 1)
				}
				for i, slot := range slots {
					got, exists := s.GetStateWithExist(addr, slot)
					if i > 0 && (exists != (i%4 != 0) || (exists && got != (tcommon.Hash{31: byte(i + 1)}))) {
						t.Fatalf("slot%d %x/%v", i, got, exists)
					}
				}
				snapshot := s.Snapshot()
				s.SetState(addr, slots[5], tcommon.Hash{0: 0xbb})
				s.RevertToSnapshot(snapshot)
				if got := s.GetState(addr, slots[5]); got != (tcommon.Hash{31: 6}) {
					t.Fatalf("revert %x", got)
				}
				reset := s.Snapshot()
				if err := s.ResetAccountKV(addr); err != nil {
					t.Fatal(err)
				}
				if got, exists := s.GetStateWithExist(addr, slots[5]); exists || got != (tcommon.Hash{}) {
					t.Fatalf("reset read old generation: %x/%v", got, exists)
				}
				s.SetState(addr, slots[5], tcommon.Hash{0: 0xcc})
				s.RevertToSnapshot(reset)
				if got := s.GetState(addr, slots[5]); got != (tcommon.Hash{31: 6}) {
					t.Fatalf("reset revert %x", got)
				}
				s.SetState(addr, slots[0], tcommon.Hash{30: byte(block), 31: 1})
				if got := s.GetState(addr, slots[0]); got != (tcommon.Hash{30: byte(block), 31: 1}) {
					t.Fatalf("SSTORE %x", got)
				}
				if err := closeScope(); err != nil {
					t.Fatal(err)
				}
				if s.storageReadScope != nil {
					t.Fatal("scope retained after close")
				}
				root, _, err := s.CommitWithStatsOptionsInScope(commits, CommitOptions{BlockNumber: block})
				if err != nil {
					t.Fatal(err)
				}
				if enabled {
					if root != reference[block-1] {
						t.Fatalf("block%d root %x want %x", block, root, reference[block-1])
					}
				} else {
					reference = append(reference, root)
				}
				if err := commits.FlushLatest(); err != nil {
					t.Fatal(err)
				}
				buffer.CommitBlock()
			}
		})
	}
}

func TestCanonicalStorageScopeRejectsReaderReplacement(t *testing.T) {
	db, err := rawdb.NewPebbleDB(t.TempDir(), 16, 16)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	s, err := New(tcommon.Hash{}, NewDatabase(rawdb.WrapKeyValueStore(db)))
	if err != nil {
		t.Fatal(err)
	}
	buffer := blockbuffer.New(db)
	s.SetAccountKVIndexStore(buffer)
	commits := s.NewCommitScope()
	defer commits.Discard()
	buffer.BeginBlock(tcommon.Hash{1}, 1)
	closeScope, err := s.BeginCanonicalStorageReadScope(1)
	if err != nil || s.storageReadScope == nil {
		t.Fatalf("open %v", err)
	}
	defer closeScope()
	s.accountKVLatestReader = &commitScopeLatestReader{state: s, writer: commits.latestWriter}
	if _, _, err := s.readAccountKVLatestForDecoding(testAddr(1), 0, kvdomains.ContractStorage, bytes.Repeat([]byte{1}, 32)); err == nil {
		t.Fatal("replacement reader accepted old scope")
	}
}
