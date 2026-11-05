package state

import (
	"fmt"
	"testing"

	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/blockbuffer"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/state/domains"
	corepb "github.com/tronprotocol/go-tron/proto/core"
)

// A deleted account still writes its generation counter on commit. That
// physical tombstone needs history even when creation and deletion cancel out
// the account envelope and every ordinary KV change in the same transaction.
func TestDeletedAccountGenerationHistoryUnwind(t *testing.T) {
	for _, initialCounter := range []bool{false, true} {
		for _, transient := range []bool{false, true} {
			for _, selfDestruct := range []bool{false, true} {
				for _, revertDelete := range []bool{false, true} {
					t.Run(fmt.Sprintf("counter=%v/transient=%v/selfdestruct=%v/revert=%v", initialCounter, transient, selfDestruct, revertDelete), func(t *testing.T) {
						disk := rawdb.NewMemoryDatabase()
						defer disk.Close()
						db := NewDatabase(rawdb.WrapKeyValueStore(disk))
						s, err := New(common.Hash{}, db)
						if err != nil {
							t.Fatal(err)
						}
						anchor, addr := testAddr(0xa0), testAddr(0xa1)
						s.CreateAccount(anchor, corepb.AccountType_Normal)
						s.AddBalance(anchor, 100)
						if !transient {
							s.CreateAccount(addr, corepb.AccountType_Contract)
						}
						parentRoot, err := s.Commit()
						if err != nil {
							t.Fatal(err)
						}
						if initialCounter {
							if err = rawdb.WriteStateKVGeneration(disk, addr, 0); err != nil {
								t.Fatal(err)
							}
							parentRoot, err = domains.RebuildMuHashCommitment(disk)
							if err != nil {
								t.Fatal(err)
							}
						}
						head, present, err := rawdb.ReadStateCommitmentHead(disk)
						if err != nil || !present {
							t.Fatal(present, err)
						}
						parentHash, nextHash := common.Hash{0x11}, common.Hash{0x12}
						head.BlockHash = parentHash
						if err = rawdb.WriteStateCommitmentHead(disk, head); err != nil {
							t.Fatal(err)
						}
						s, err = New(parentRoot, db)
						if err != nil {
							t.Fatal(err)
						}
						stage, err := s.BeginDomainChangeStage(disk, &rawdb.StateTxRange{BlockNum: 1, BlockHash: nextHash, BeginTxNum: 1, EndTxNum: 2})
						if err != nil {
							t.Fatal(err)
						}
						mark := stage.JournalMark()
						snap := s.Snapshot()
						if transient {
							s.CreateAccount(addr, corepb.AccountType_Contract)
						}
						if selfDestruct {
							s.SelfDestruct(addr)
						} else {
							s.DeleteAccount(addr)
						}
						s.FinalizeTransaction()
						if revertDelete {
							s.RevertToSnapshot(snap)
						}
						if err = stage.FlushOrdinal(mark, 0); err != nil {
							t.Fatal(err)
						}
						if err = stage.FlushFinal(); err != nil {
							t.Fatal(err)
						}
						if _, _, err = s.CommitWithStatsOptions(CommitOptions{BlockNumber: 1, BlockHash: nextHash, ParentHash: parentHash, CommitmentTxNum: 2}); err != nil {
							t.Fatal(err)
						}
						current := assertCompactFullMuHash(t, disk)
						view := blockbuffer.New(disk)
						view.BeginBlock(parentHash, 0)
						defer view.Discard()
						_, err = domains.UnwindMuHashCommitment(view,
							domains.CommitmentIdentity{BlockNum: 1, BlockHash: nextHash, ParentHash: parentHash, TxNum: current.TxNum},
							domains.CommitmentIdentity{BlockNum: 0, BlockHash: parentHash}, parentRoot)
						if err != nil {
							t.Fatalf("history cannot restore exact pre-deletion physical state: %v", err)
						}
						got, present, err := rawdb.ReadStateKVGeneration(view, addr)
						if err != nil || present != initialCounter || got != 0 {
							t.Fatalf("unwind retained a generation tombstone: generation=%d present=%v err=%v", got, present, err)
						}
					})
				}
			}
		}
	}
}

func TestDeletedAccountGenerationHistoryRecreate(t *testing.T) {
	disk := rawdb.NewMemoryDatabase()
	defer disk.Close()
	db := NewDatabase(rawdb.WrapKeyValueStore(disk))
	s, err := New(common.Hash{}, db)
	if err != nil {
		t.Fatal(err)
	}
	anchor, addr := testAddr(0xb0), testAddr(0xb1)
	s.CreateAccount(anchor, corepb.AccountType_Normal)
	parentRoot, err := s.Commit()
	if err != nil {
		t.Fatal(err)
	}
	parentHash, nextHash := common.Hash{0x21}, common.Hash{0x22}
	head, _, err := rawdb.ReadStateCommitmentHead(disk)
	if err != nil {
		t.Fatal(err)
	}
	head.BlockHash = parentHash
	if err = rawdb.WriteStateCommitmentHead(disk, head); err != nil {
		t.Fatal(err)
	}
	s, err = New(parentRoot, db)
	if err != nil {
		t.Fatal(err)
	}
	stage, err := s.BeginDomainChangeStage(disk, &rawdb.StateTxRange{BlockNum: 1, BlockHash: nextHash, BeginTxNum: 1, EndTxNum: 3})
	if err != nil {
		t.Fatal(err)
	}
	mark := stage.JournalMark()
	s.CreateAccount(addr, corepb.AccountType_Contract)
	s.SelfDestruct(addr)
	s.FinalizeTransaction()
	if err = stage.FlushOrdinal(mark, 0); err != nil {
		t.Fatal(err)
	}
	mark = stage.JournalMark()
	s.CreateAccount(addr, corepb.AccountType_Normal)
	s.AddBalance(addr, 7)
	s.FinalizeTransaction()
	if err = stage.FlushOrdinal(mark, 1); err != nil {
		t.Fatal(err)
	}
	if err = stage.FlushFinal(); err != nil {
		t.Fatal(err)
	}
	if _, _, err = s.CommitWithStatsOptions(CommitOptions{BlockNumber: 1, BlockHash: nextHash, ParentHash: parentHash, CommitmentTxNum: 3}); err != nil {
		t.Fatal(err)
	}
	current := assertCompactFullMuHash(t, disk)
	view := blockbuffer.New(disk)
	view.BeginBlock(parentHash, 0)
	defer view.Discard()
	_, err = domains.UnwindMuHashCommitment(view, domains.CommitmentIdentity{BlockNum: 1, BlockHash: nextHash, ParentHash: parentHash, TxNum: current.TxNum}, domains.CommitmentIdentity{BlockHash: parentHash}, parentRoot)
	if err != nil {
		t.Fatalf("recreate history lost the first physical absence: %v", err)
	}
}
