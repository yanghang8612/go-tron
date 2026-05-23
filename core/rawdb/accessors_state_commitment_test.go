package rawdb

import (
	"testing"

	ethrawdb "github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/state/kvdomains"
)

func TestStateCommitmentCheckpointRoundTrip(t *testing.T) {
	db := ethrawdb.NewMemoryDatabase()
	cp := &StateCommitmentCheckpoint{
		BlockNum:  12,
		BlockHash: common.Hash{0x12},
		Root:      common.Hash{0xaa},
		Scheme:    LatestDomainCommitmentScheme,
	}
	if _, ok, err := ReadStateCommitmentCheckpoint(db, 12); err != nil || ok {
		t.Fatalf("pre-read = ok:%v err:%v", ok, err)
	}
	if err := WriteStateCommitmentCheckpoint(db, cp); err != nil {
		t.Fatalf("write checkpoint: %v", err)
	}
	got, ok, err := ReadStateCommitmentCheckpoint(db, 12)
	if err != nil || !ok {
		t.Fatalf("read checkpoint = ok:%v err:%v", ok, err)
	}
	if got.BlockNum != cp.BlockNum || got.BlockHash != cp.BlockHash || got.Root != cp.Root || got.Scheme != cp.Scheme {
		t.Fatalf("checkpoint = %+v, want %+v", got, cp)
	}
}

func TestComputeLatestDomainRootDeterministicAndSensitive(t *testing.T) {
	db1 := ethrawdb.NewMemoryDatabase()
	db2 := ethrawdb.NewMemoryDatabase()
	owner := common.Address{0x41, 0x01}
	other := common.Address{0x41, 0x02}

	mustWriteStateKVLatest(t, db1, owner, 0, kvdomains.SystemReward, []byte("b"), []byte("2"))
	mustWriteStateKVLatest(t, db1, owner, 0, kvdomains.SystemReward, []byte("a"), []byte("1"))
	if err := WriteStateKVGeneration(db1, owner, 0); err != nil {
		t.Fatal(err)
	}

	if err := WriteStateKVGeneration(db2, owner, 0); err != nil {
		t.Fatal(err)
	}
	mustWriteStateKVLatest(t, db2, owner, 0, kvdomains.SystemReward, []byte("a"), []byte("1"))
	mustWriteStateKVLatest(t, db2, owner, 0, kvdomains.SystemReward, []byte("b"), []byte("2"))

	root1, err := ComputeLatestDomainRoot(db1)
	if err != nil {
		t.Fatalf("root1: %v", err)
	}
	root2, err := ComputeLatestDomainRoot(db2)
	if err != nil {
		t.Fatalf("root2: %v", err)
	}
	if root1 != root2 {
		t.Fatalf("same latest rows produced different roots: %x vs %x", root1, root2)
	}

	mustWriteStateKVLatest(t, db2, other, 0, kvdomains.SystemReward, []byte("a"), []byte("1"))
	root3, err := ComputeLatestDomainRoot(db2)
	if err != nil {
		t.Fatalf("root3: %v", err)
	}
	if root3 == root2 {
		t.Fatal("commitment root did not change after latest-domain mutation")
	}
}
