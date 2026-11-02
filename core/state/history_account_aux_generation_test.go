package state

import (
	"bytes"
	"testing"

	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/state/kvdomains"
	"github.com/tronprotocol/go-tron/core/types"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	"google.golang.org/protobuf/proto"
)

type permissionHistoryCandidateSpy struct {
	readerDB
	prefixes [][]byte
}

func (s *permissionHistoryCandidateSpy) NewIterator(prefix, start []byte) ethdb.Iterator {
	s.prefixes = append(s.prefixes, bytes.Clone(prefix))
	return s.readerDB.NewIterator(prefix, start)
}

func TestHistoricalAccountAuxCurrentGenerationCandidates(t *testing.T) {
	owner := testAddr(0xe1)
	for _, f := range []struct {
		name           string
		accountExists  bool
		corruptAccount bool
		generation     uint64
		watermark      bool
	}{
		{name: "gen0-no-watermark", accountExists: true},
		{name: "recreated-gen1", accountExists: true, generation: 1, watermark: true},
		{name: "deleted-owner-orphan-watermark", generation: 2, watermark: true},
		{name: "invalid-current-envelope", accountExists: true, corruptAccount: true, generation: 2, watermark: true},
	} {
		t.Run(f.name, func(t *testing.T) {
			disk := rawdb.NewMemoryDatabase()
			defer disk.Close()
			for h := uint64(1); h <= 2; h++ {
				if err := rawdb.WriteStateTxRange(disk, h, common.Hash{byte(h)}, h, h); err != nil {
					t.Fatal(err)
				}
			}
			if f.accountExists {
				core, err := types.NewAccount(owner, corepb.AccountType_Normal).MarshalStorageCoreV4(owner)
				if err != nil {
					t.Fatal(err)
				}
				value, err := (&StateAccountV3{Version: StateAccountVersion, AccountProto: core, AccountKVRoot: EmptyKVRoot, AccountKVGeneration: f.generation}).Encode()
				if err != nil {
					t.Fatal(err)
				}
				if f.corruptAccount {
					value = []byte("corrupt-account-envelope")
				}
				if err := rawdb.WriteStateAccountLatest(disk, owner, value); err != nil {
					t.Fatal(err)
				}
			}
			if f.watermark {
				if err := rawdb.WriteStateKVGeneration(disk, owner, f.generation); err != nil {
					t.Fatal(err)
				}
			}
			permission := types.MakeDefaultOwnerPermission(owner)
			value, err := appendAccountPermissionForOwner(nil, owner, accountOwnerPermissionKey, permission)
			if err != nil {
				t.Fatal(err)
			}
			if err := rawdb.WriteStateKVLatest(disk, owner, f.generation, kvdomains.AccountPermissionAux, accountOwnerPermissionKey, value); err != nil {
				t.Fatal(err)
			}
			if f.generation != 0 {
				old := types.MakeDefaultOwnerPermission(owner)
				old.PermissionName = "obsolete-generation"
				value, err := appendAccountPermissionForOwner(nil, owner, accountOwnerPermissionKey, old)
				if err != nil {
					t.Fatal(err)
				}
				if err := rawdb.WriteStateKVLatest(disk, owner, 0, kvdomains.AccountPermissionAux, accountOwnerPermissionKey, value); err != nil {
					t.Fatal(err)
				}
			}
			spy := &permissionHistoryCandidateSpy{readerDB: disk}
			reader := NewPersistentHistoryReaderWithColdHistory(spy, nil, 2, new(keyedColdHistoryStub))
			pb := new(corepb.Account)
			err = reader.materializeHistoricalAccountAux(pb, owner, f.generation, 1)
			if (err != nil) != f.corruptAccount {
				t.Fatalf("historical auxiliary error=%v, corrupt current=%v", err, f.corruptAccount)
			}
			wantPermission := f.accountExists && !f.corruptAccount
			if wantPermission && !proto.Equal(pb.OwnerPermission, permission) || !wantPermission && pb.OwnerPermission != nil {
				t.Fatalf("historical permission=%v, account exists=%v", pb.OwnerPermission, f.accountExists)
			}
			for _, domain := range accountSplitDomains {
				prefix := rawdb.StateKVLatestCommitmentKey(owner, f.generation, domain, nil)
				found := false
				for _, actual := range spy.prefixes {
					found = found || bytes.Equal(actual, prefix)
					if f.generation != 0 && bytes.Equal(actual, rawdb.StateKVLatestCommitmentKey(owner, 0, domain, nil)) {
						t.Fatal("candidate enumeration leaked an obsolete generation", domain)
					}
				}
				if found != wantPermission {
					t.Fatalf("domain %v current-generation candidate scan=%v, want=%v", domain, found, wantPermission)
				}
			}
		})
	}
}
