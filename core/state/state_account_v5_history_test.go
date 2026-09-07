package state

import (
	"bytes"
	"testing"

	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/state/domains"
	"github.com/tronprotocol/go-tron/core/state/snapshots"
	"github.com/tronprotocol/go-tron/core/types"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	"google.golang.org/protobuf/proto"
)

func accountV5HistoryFixture(t *testing.T) (*historyFixture, tcommon.Address, [][]byte, []tcommon.Hash, []*corepb.Account) {
	t.Helper()
	f := newHistoryFixture(t)
	addr := testAddr(0x6f)
	var encodings [][]byte
	var roots []tcommon.Hash
	var accounts []*corepb.Account
	for block := byte(1); block <= 3; block++ {
		f.applyBlock(tcommon.Hash{block}, func(s *StateDB) {
			if block == 1 {
				s.CreateAccount(addr, corepb.AccountType_Contract)
			}
			if block == 2 {
				// A reverted name/code change must not leak into either the
				// persisted post-image or its subsequent historical pre-image.
				before, _, err := encodeAccountLatestObject(s.getStateObject(addr), true)
				if err != nil {
					t.Fatal(err)
				}
				mark := s.Snapshot()
				s.SetAccountName(addr, "discarded")
				s.SetCode(addr, []byte{0xff})
				s.RevertToSnapshot(mark)
				after, _, err := encodeAccountLatestObject(s.getStateObject(addr), true)
				if err != nil || !bytes.Equal(before, after) {
					t.Fatalf("snapshot revert changed envelope: %v", err)
				}
			}
			s.AddBalance(addr, int64(block)*100)
			s.SetAccountName(addr, string([]byte{'a', block}))
			code := []byte{0x60, block}
			s.SetCode(addr, code)
			pb := s.GetStateObject(addr).Proto()
			pb.CodeHash = tcommon.Keccak256(code).Bytes()
			pb.CreateTime = 123456789
			pb.LatestOprationTime = 123456789 + int64(block)
			pb.ProtoReflect().SetUnknown([]byte{0xa0, 6, block, 0xaa, 6, 0})
			s.getStateObject(addr).invalidateAccountProto()
		})
		encoded, ok, err := rawdb.ReadStateAccountLatest(f.disk, addr)
		if err != nil || !ok {
			t.Fatalf("account latest: %t %v", ok, err)
		}
		envelope, err := DecodeStateAccountV3(encoded)
		if err != nil || envelope.Version != 5 || envelope.AccountKVRoot != EmptyKVRoot {
			t.Fatalf("normal commit did not write V5: %+v %v", envelope, err)
		}
		account, err := types.UnmarshalAccountStorageCoreV4(envelope.AccountProto)
		if err != nil {
			t.Fatal(err)
		}
		// The explicit V4 reader preserves the protobuf/API representation. The
		// internal commitment intentionally hashes the actual storage bytes.
		v4 := *envelope
		v4.Version = 4
		old, _ := v4.Encode()
		decodedOld, err := DecodeStateAccountV3(old)
		if err != nil {
			t.Fatal(err)
		}
		oldAccount, err := types.UnmarshalAccountStorageCoreV4(decodedOld.AccountProto)
		if err != nil || !proto.Equal(oldAccount.Proto(), account.Proto()) {
			t.Fatalf("V4/V5 logical account differs: %v", err)
		}
		if len(old)-len(encoded) < 64 {
			t.Fatal("runtime code hash reference did not reduce both hashes")
		}
		encodings = append(encodings, bytes.Clone(encoded))
		accounts = append(accounts, proto.Clone(account.Proto()).(*corepb.Account))
		root, ok, err := rawdb.ReadLatestDomainCommitmentRoot(f.disk)
		if err != nil || !ok {
			t.Fatalf("commitment root: %t %v", ok, err)
		}
		roots = append(roots, root)
		f.state, err = New(root, f.state.db)
		if err != nil {
			t.Fatal(err)
		}
	}
	return f, addr, encodings, roots, accounts
}

func TestStateAccountV5HotColdHistoryAndCodeHashes(t *testing.T) {
	f, addr, encodings, _, accounts := accountV5HistoryFixture(t)
	check := func(label string, r *PersistentHistoryReader) {
		t.Helper()
		missing, err := r.AccountAt(addr, 0)
		if err != nil || missing != nil {
			t.Fatalf("%s pre-creation account: %v %v", label, missing, err)
		}
		for block := uint64(1); block <= 3; block++ {
			account, err := r.AccountAt(addr, block)
			if err != nil || account == nil || !proto.Equal(account.Proto(), accounts[block-1]) {
				t.Fatalf("%s block %d account/unknown changed: %v", label, block, err)
			}
			code, err := r.CodeAt(addr, block)
			if err != nil || !bytes.Equal(code, []byte{0x60, byte(block)}) {
				t.Fatalf("%s block %d code=%x err=%v", label, block, code, err)
			}
		}
	}
	check("hot", NewPersistentHistoryReader(f.disk, nil, 3))
	for block := uint64(2); block <= 3; block++ {
		found := false
		err := rawdb.IterateStateDomainChanges(f.disk, block, func(c *rawdb.StateDomainChange) (bool, error) {
			if c.FlatDomain == rawdb.StateFlatDomainAccountLatest && c.Owner == addr {
				found = true
				if !c.PrevExists || !bytes.Equal(c.Prev, encodings[block-2]) {
					t.Fatal("hot history did not preserve exact V5 previous bytes")
				}
			}
			return true, nil
		})
		if err != nil || !found {
			t.Fatalf("missing hot previous image: %v", err)
		}
	}
	dir := t.TempDir()
	refs, err := snapshots.BuildStateDomainChangeHistorySegmentsFromDB(f.disk, dir, 1, 3, "history/state-domain-change-account-v5.seg")
	if err != nil {
		t.Fatal(err)
	}
	if err := snapshots.PublishManifest(dir, snapshots.NewManifest(1, 3, refs)); err != nil {
		t.Fatal(err)
	}
	manager, err := snapshots.OpenManager(dir)
	if err != nil {
		t.Fatal(err)
	}
	f.pruneHotStateDomainHistory()
	check("cold", NewPersistentHistoryReaderWithColdHistory(f.disk, nil, 3, manager))
}

func TestStateAccountV5CommitmentReorgRestoresExactEnvelope(t *testing.T) {
	f, addr, encodings, roots, accounts := accountV5HistoryFixture(t)
	root, err := domains.UnwindCommitment(f.disk, domains.NewStagedCommitmentStore(f.disk), 3, 1, roots[0])
	if err != nil || root != roots[0] {
		t.Fatalf("V5 commitment unwind: root=%x want=%x err=%v", root, roots[0], err)
	}
	got, ok, err := rawdb.ReadStateAccountLatest(f.disk, addr)
	if err != nil || !ok || !bytes.Equal(got, encodings[0]) {
		t.Fatalf("unwind did not restore byte-exact V5 envelope: %v", err)
	}
	reopened, err := New(root, f.state.db)
	if err != nil {
		t.Fatal(err)
	}
	if account := reopened.GetAccount(addr); account == nil || !proto.Equal(account.Proto(), accounts[0]) {
		t.Fatal("reorg changed API account/unknown fields")
	}
	if got := reopened.GetCode(addr); !bytes.Equal(got, []byte{0x60, 1}) {
		t.Fatalf("reorg code=%x", got)
	}
}
