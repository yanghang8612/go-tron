package state

import (
	"testing"

	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/blockbuffer"
	"github.com/tronprotocol/go-tron/core/rawdb"
	statedomains "github.com/tronprotocol/go-tron/core/state/domains"
)

type mismatchedCommitmentRootSource struct {
	root tcommon.Hash
}

func (s mismatchedCommitmentRootSource) GetCommitmentRoot(uint64) (tcommon.Hash, bool, error) {
	return s.root, true, nil
}

func TestCommitmentFoldRecoversMismatchedBranchBase(t *testing.T) {
	for _, async := range []bool{false, true} {
		name := "sync"
		if async {
			name = "async-captured"
		}
		t.Run(name, func(t *testing.T) {
			disk := rawdb.NewMemoryDatabase()
			baseOwner := tcommon.Address{0x41, 0x11}
			if err := rawdb.WriteStateAccountLatest(disk, baseOwner, []byte("base-account")); err != nil {
				t.Fatal(err)
			}
			baseRoot, err := statedomains.NewStagedCommitmentStore(disk).Rebuild()
			if err != nil {
				t.Fatal(err)
			}
			if err := rawdb.WriteCommitmentBranchBase(disk, rawdb.CommitmentBranchBase{
				Generation: 9, SnapshotTxNum: 97019760, Root: baseRoot,
			}); err != nil {
				t.Fatal(err)
			}
			if err := rawdb.DeleteCommitmentBranches(disk); err != nil {
				t.Fatal(err)
			}

			buffer := blockbuffer.New(disk)
			buffer.BeginBlock(tcommon.Hash{0x43}, 20674403)
			handle, ok := buffer.NewestInflight()
			if !ok {
				t.Fatal("missing in-flight layer")
			}
			view := buffer.ViewLayer(handle)
			blockOwner := tcommon.Address{0x41, 0x22}
			blockValue := []byte("current-block-account")
			if err := rawdb.WriteStateAccountLatest(view, blockOwner, blockValue); err != nil {
				t.Fatal(err)
			}
			updates := []rawdb.StateCommitmentUpdate{
				rawdb.NewStateCommitmentPut(rawdb.StateAccountLatestCommitmentKey(blockOwner), blockValue),
			}
			repair := statedomains.CommitmentSnapshotRepair{
				Source: mismatchedCommitmentRootSource{root: tcommon.Hash{0xee}},
				TxNum:  97019760,
			}

			var got tcommon.Hash
			if async {
				captured := &CapturedCommit{
					batch:  &commitmentUpdateBatch{updates: updates},
					repair: repair,
				}
				got, err = captured.Fold(view)
				if captured.batch != nil {
					t.Fatal("captured recovery fold retained its batch")
				}
			} else {
				got, err = FoldLatestCommitment(view, updates, repair)
			}
			if err != nil {
				t.Fatalf("fold after base mismatch: %v", err)
			}

			reference := rawdb.NewMemoryDatabase()
			if err := rawdb.WriteStateAccountLatest(reference, baseOwner, []byte("base-account")); err != nil {
				t.Fatal(err)
			}
			if err := rawdb.WriteStateAccountLatest(reference, blockOwner, blockValue); err != nil {
				t.Fatal(err)
			}
			want, err := statedomains.NewStagedCommitmentStore(reference).Rebuild()
			if err != nil {
				t.Fatal(err)
			}
			if got != want {
				t.Fatalf("recovered fold root %x, want %x", got, want)
			}
			if _, ok, err := rawdb.ReadCommitmentBranchBase(view); err != nil || ok {
				t.Fatalf("layer marker survived recovery ok=%v err=%v", ok, err)
			}
			if _, ok, err := rawdb.ReadCommitmentBranchBase(disk); err != nil || !ok {
				t.Fatalf("disk marker changed before layer commit ok=%v err=%v", ok, err)
			}
		})
	}
}
