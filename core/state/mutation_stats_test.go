package state

import (
	"strings"
	"testing"

	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/state/kvdomains"
	corepb "github.com/tronprotocol/go-tron/proto/core"
)

func TestCommitMutationStatsReportsStateTypesAndDomains(t *testing.T) {
	sdb := newTestStateDB(t)
	acct := testAddr(0x31)
	contract := testAddr(0x32)
	sdb.CreateAccount(acct, corepb.AccountType_Normal)
	sdb.SetState(contract, mutationTestHash(0x01), mutationTestHash(0x02))
	if err := sdb.SetAccountKV(acct, kvdomains.SystemReward, []byte("cycle"), []byte("reward")); err != nil {
		t.Fatal(err)
	}

	if _, stats, err := sdb.CommitWithStats(); err != nil {
		t.Fatalf("commit: %v", err)
	} else {
		m := stats.Mutations
		if m.AccountCreates != 2 {
			t.Fatalf("AccountCreates=%d, want 2", m.AccountCreates)
		}
		if m.StoragePuts != 1 {
			t.Fatalf("StoragePuts=%d, want 1", m.StoragePuts)
		}
		if got := m.KVDomain(kvdomains.ContractStorage).Puts; got != 1 {
			t.Fatalf("ContractStorage puts=%d, want 1", got)
		}
		if got := m.KVDomain(kvdomains.SystemReward).Puts; got != 1 {
			t.Fatalf("SystemReward puts=%d, want 1", got)
		}
		if top := m.TopKindsString(2); !strings.Contains(top, "accountCreate=2") {
			t.Fatalf("top kinds %q missing accountCreate=2", top)
		}
		if top := m.TopKVDomainsString(4); !strings.Contains(top, "ContractStorage:p1/d0/n0") || !strings.Contains(top, "SystemReward:p1/d0/n0") {
			t.Fatalf("top domains %q missing expected domains", top)
		}
	}
}

func mutationTestHash(b byte) tcommon.Hash {
	var h tcommon.Hash
	h[31] = b
	return h
}

func TestCommitMutationStatsAddMergesDomains(t *testing.T) {
	var a, b CommitMutationStats
	a.AccountUpdates = 2
	a.addKV(kvdomains.ContractStorage, false)
	b.AccountUpdates = 3
	b.addKV(kvdomains.ContractStorage, true)
	b.KVNoopItems = 1
	if idx, ok := kvDomainStatIndex(kvdomains.ContractStorage); ok {
		b.KVDomains[idx].Noops = 1
	}

	a.Add(b)
	if a.AccountUpdates != 5 {
		t.Fatalf("AccountUpdates=%d, want 5", a.AccountUpdates)
	}
	if a.KVPutItems != 1 || a.KVDeleteItems != 1 || a.KVNoopItems != 1 {
		t.Fatalf("kv totals put=%d delete=%d noop=%d, want 1/1/1", a.KVPutItems, a.KVDeleteItems, a.KVNoopItems)
	}
	if got := a.KVDomain(kvdomains.ContractStorage); got.Puts != 1 || got.Deletes != 1 || got.Noops != 1 {
		t.Fatalf("ContractStorage stats=%+v, want 1/1/1", got)
	}
}
