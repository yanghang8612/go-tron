package rawdb

import (
	"context"
	"errors"
	"testing"

	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/tronprotocol/go-tron/core/pointread"
)

// Fail the source claim publication after target receipt durability. This is
// the exact legal checkpoint an executor upgrade must accept and preserve.
type stagingUpgradeClaimFault struct {
	ethdb.KeyValueStore
	fail bool
}

func (s *stagingUpgradeClaimFault) Put(key, value []byte) error {
	if s.fail {
		return errors.New("claim publication interrupted")
	}
	return s.KeyValueStore.Put(key, value)
}
func (s *stagingUpgradeClaimFault) SyncKeyValue() error {
	return s.KeyValueStore.(interface{ SyncKeyValue() error }).SyncKeyValue()
}
func (s *stagingUpgradeClaimFault) NewKeyValueSnapshot() (pointread.KeyValueSnapshot, error) {
	return s.KeyValueStore.(pointread.KeyValueSnapshotter).NewKeyValueSnapshot()
}
func TestHistoryStagingReceiptDurableBeforeReadyClaimReopens(t *testing.T) {
	ctx := context.Background()
	m, closeStores, hotPath, stagePath, identity := stagingProtocolStores(t)
	if err := m.InitializeOfflineSourceRoutes(ctx, 1, 1, 1); err != nil {
		t.Fatal(err)
	}
	proof := stagingProtocolProof(t, m, 1)
	stagingProtocolRows(t, m, 1024)
	claim, err := m.BeginClaim(ctx, proof, [32]byte{7})
	if err != nil {
		t.Fatal(err)
	}
	fault := &stagingUpgradeClaimFault{KeyValueStore: m.hot, fail: true}
	m.hot = fault
	if _, err = m.CopyClaim(ctx, claim, stagingProtocolLimits()); err == nil {
		t.Fatal("claim write interruption not observed")
	}
	receipt, present, err := m.ReadReceipt(1)
	if err != nil || !present {
		t.Fatalf("target receipt was not durable: %v", err)
	}
	current, present, err := m.ReadClaim(1)
	if err != nil || !present || current.TargetReady {
		t.Fatalf("source claim moved past interrupted checkpoint: %v", err)
	}
	closeStores()
	hot, err := NewPebbleDB(hotPath, 16, 16)
	if err != nil {
		t.Fatal(err)
	}
	defer hot.Close()
	stage, err := NewHistoryStagingPebbleDB(stagePath, 16, 16, false)
	if err != nil {
		t.Fatal(err)
	}
	defer stage.Close()
	reopened, err := NewHistoryStagingManager(hot, stage, identity)
	if err != nil {
		t.Fatal(err)
	}
	state, err := reopened.InspectBucket(1)
	if err != nil || !state.HasClaim || state.Claim.TargetReady || !state.HasReceipt || state.Receipt != receipt || state.Route.Owner != HistoryStagingOwnerSource {
		t.Fatalf("reopen lost precise checkpoint: route=%v claim=%v ready=%v receipt=%v err=%v", state.HasRoute, state.HasClaim, state.Claim.TargetReady, state.HasReceipt, err)
	}
	copied, err := reopened.CopyClaim(ctx, state.Claim, stagingProtocolLimits())
	if err != nil || copied != receipt {
		t.Fatalf("same claim resume changed receipt: %v", err)
	}
	if _, err = reopened.AdoptClaim(ctx, state.Claim, proof); err != nil {
		t.Fatal(err)
	}
	if err = reopened.ClearSource(ctx, state.Claim, stagingProtocolLimits()); err != nil {
		t.Fatal(err)
	}
	done, err := reopened.InspectBucket(1)
	if err != nil || !done.Route.SourceCleared || done.HasClaim || done.Receipt != receipt {
		t.Fatalf("resume did not preserve immutable receipt: cleared=%v claim=%v err=%v", done.Route.SourceCleared, done.HasClaim, err)
	}
}
