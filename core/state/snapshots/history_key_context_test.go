package snapshots

import (
	"context"
	"testing"

	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/maintenance"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/state/kvdomains"
)

func TestHistoryKeySeekWithMaintenanceContextBindsAccessorOnce(t *testing.T) {
	dir := t.TempDir()
	refs := historyVerificationSHAFixture(t, dir, "context", 0)
	h, _, a := historyVerificationSHARefs(t, refs)
	calls := 0
	ctx := maintenance.WithWorkCheckpoint(context.Background(), func(uint64) error { calls++; return nil })
	owner := common.Address{common.AddressPrefixMainnet}
	key := stateDomainChangeBinaryAccessorLookupKey(rawdb.StateFlatDomainKVLatest, owner, 0, kvdomains.ContractStorage, []byte{1})
	seen := 0
	err := iterateStateDomainChangeBinarySegmentByAccessorKeysFile(ctx, dir, h, a, [][]byte{key}, 1, 512, func(_ []byte, c *rawdb.StateDomainChange) (bool, error) { seen++; return false, nil })
	if err != nil || seen != 1 || calls == 0 {
		t.Fatalf("seen=%d checkpoints=%d err=%v", seen, calls, err)
	}
}
