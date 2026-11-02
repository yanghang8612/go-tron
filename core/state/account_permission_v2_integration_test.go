package state

import (
	"bytes"
	"strings"
	"testing"

	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/state/domains"
	"github.com/tronprotocol/go-tron/core/state/kvdomains"
	"github.com/tronprotocol/go-tron/core/state/snapshots"
	"github.com/tronprotocol/go-tron/core/state/statecodec"
	"github.com/tronprotocol/go-tron/core/types"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	"google.golang.org/protobuf/proto"
)

func TestAccountPermissionV2HistoryUnwindSnapshotAndContinue(t *testing.T) {
	owner := testAddr(0xd5)
	permissions := func(h uint64) (*corepb.Permission, *corepb.Permission, []*corepb.Permission) {
		if h == 3 {
			return nil, nil, nil // Explicit absence must survive every layer.
		}
		own := types.MakeDefaultOwnerPermission(owner)
		active := types.MakeDefaultActivePermission(owner, bytes.Repeat([]byte{byte(h)}, 32))
		var witness *corepb.Permission
		if h > 1 {
			witness = types.MakeDefaultWitnessPermission(owner)
		}
		if h == 2 || h == 5 {
			own.PermissionName, own.Threshold = "custom-owner", 2
			own.Keys = append(own.Keys, &corepb.Key{Address: testAddr(0xd6).Bytes(), Weight: 1})
			active.Id, active.PermissionName = 3, "custom-active"
		}
		return own, witness, []*corepb.Permission{active}
	}
	rewardVotes := func(h uint64, cycle int64) ([]*corepb.Vote, bool) {
		if cycle == 8 && h == 1 {
			return nil, false
		}
		if h == 1 || h == 4 || cycle == 8 && h == 2 {
			return nil, true
		}
		vote := &corepb.Vote{VoteAddress: owner.Bytes(), VoteCount: int64(h)}
		vote.ProtoReflect().SetUnknown([]byte{0xa0, 0x06, 0x01}) // Unknown field 100 remains opaque.
		return []*corepb.Vote{vote, {VoteAddress: owner.Bytes(), VoteCount: -1}}, true
	}
	disk := rawdb.NewMemoryDatabase()
	defer disk.Close()
	db := NewDatabase(rawdb.WrapKeyValueStore(disk))
	defer db.Close()
	s, err := New(common.Hash{}, db)
	if err != nil {
		t.Fatal(err)
	}
	commit := func(s *StateDB, store ethdb.KeyValueStore, h uint64) common.Hash {
		t.Helper()
		hash := common.Hash{byte(h)}
		s.BeginDomainChangeJournalCapture(store, h, hash, h, h)
		mark := s.DomainChangeJournalMark()
		if h == 1 {
			s.CreateAccount(owner, corepb.AccountType_Normal)
		}
		own, witness, actives := permissions(h)
		s.SetPermissions(owner, own, witness, actives)
		for _, cycle := range []int64{7, 8} {
			if votes, present := rewardVotes(h, cycle); present {
				if err := s.WriteCycleAccountVotes(cycle, owner.Bytes(), votes); err != nil {
					t.Fatal(err)
				}
			}
		}
		// Exercise an already-decoded compact permission across journal revert.
		if h == 4 {
			before, err := s.AccountPermissionByID(owner, 0)
			if err != nil || !proto.Equal(before, own) {
				t.Fatal(before, err)
			}
			mark := s.Snapshot()
			s.SetPermissions(owner, nil, nil, nil)
			s.RevertToSnapshot(mark)
			if p, err := s.AccountPermissionByID(owner, 0); err != nil || !proto.Equal(p, own) {
				t.Fatal("permission snapshot revert", p, err)
			}
		}
		if err := s.FlushDomainChangesSince(mark, h); err != nil {
			t.Fatal(err)
		}
		root, _, err := s.CommitWithStatsOptions(CommitOptions{BlockNumber: h, BlockHash: hash, ParentHash: common.Hash{byte(h - 1)}, CommitmentTxNum: h})
		if err != nil {
			t.Fatal(err)
		}
		if err := rawdb.WriteBlockNumber(store, hash, h); err != nil {
			t.Fatal(err)
		}
		if err := rawdb.WriteStateExecutionProperties(store, rawdb.StateExecutionProperties{BlockNum: int64(h), Timestamp: int64(h * 3000), BlockHash: hash}); err != nil {
			t.Fatal(err)
		}
		assertCompactFullMuHash(t, store)
		return root
	}
	roots := make([]common.Hash, 0, 5)
	for h := uint64(1); h <= 4; h++ {
		roots = append(roots, commit(s, disk, h))
	}
	if _, exists, err := rawdb.ReadStateKVGeneration(disk, owner); err != nil || exists {
		t.Fatal("fixture must cover normal gen0 without a generation watermark", exists, err)
	}
	// Native templates remain present rows with the unchanged exact keys.
	for _, key := range [][]byte{accountOwnerPermissionKey, accountWitnessPermissionKey, accountActivePermissionKey(2)} {
		value, exists, err := rawdb.ReadStateKVLatest(disk, owner, 0, kvdomains.AccountPermissionAux, key)
		if err != nil || !exists || len(value) < 7 || value[4] != accountPermissionRowVersion {
			t.Fatalf("compact permission row %x: %x/%v/%v", key, value, exists, err)
		}
	}
	dir := t.TempDir()
	built, err := snapshots.NewAggregator(dir).BuildLatest(disk, snapshots.AggregatorBuildOptions{FromTxNum: 1, ToTxNum: 4})
	if err != nil {
		t.Fatal(err)
	}
	identity := snapshots.ChainIdentity{ChainID: 1, NetworkID: 1, GenesisHash: strings.Repeat("01", 32), ForkConfigHash: "sha256:" + strings.Repeat("02", 32)}
	built.Manifest.Chain = &identity
	if err := snapshots.PublishManifest(dir, built.Manifest); err != nil {
		t.Fatal(err)
	}
	restored := rawdb.NewMemoryDatabase()
	defer restored.Close()
	if _, err := snapshots.RestoreLatestFromVerifiedManifest(restored, dir, identity); err != nil {
		t.Fatal(err)
	}
	if h := assertCompactFullMuHash(t, restored); h.Root != roots[3] {
		t.Fatal("restored permission root differs")
	}
	rdb := NewDatabase(rawdb.WrapKeyValueStore(restored))
	defer rdb.Close()
	rs, err := New(roots[3], rdb)
	if err != nil {
		t.Fatal(err)
	}
	for _, id := range []int32{0, 1, 2} {
		p, err := rs.AccountPermissionByID(owner, id)
		if err != nil || p == nil || !bytes.Equal(p.Keys[0].Address, owner[:]) {
			t.Fatal("restored template point read", id, p, err)
		}
	}
	for _, cycle := range []int64{7, 8} {
		votes, present, err := rs.ReadCycleAccountVotesStrict(cycle, owner.Bytes())
		if err != nil || !present || len(votes) != 0 {
			t.Fatal("restored present empty reward", cycle, votes, present, err)
		}
	}
	next := commit(s, disk, 5)
	roots = append(roots, next)
	if other := commit(rs, restored, 5); other != next {
		t.Fatal("permission continuation root differs after restore")
	}
	checkHistory := func(reader *PersistentHistoryReader) {
		t.Helper()
		for h := uint64(1); h <= 5; h++ {
			got, err := reader.AccountAt(owner, h)
			if err != nil || got == nil {
				t.Fatalf("permission AccountAt %d: %v", h, err)
			}
			own, witness, actives := permissions(h)
			if !proto.Equal(got.OwnerPermission(), own) || !proto.Equal(got.WitnessPermission(), witness) || len(got.ActivePermission()) != len(actives) {
				t.Fatalf("permission AccountAt %d lost fields/presence: %v", h, got.Proto())
			}
			for i := range actives {
				if !proto.Equal(got.ActivePermission()[i], actives[i]) {
					t.Fatalf("permission AccountAt %d active mismatch", h)
				}
			}
			for _, cycle := range []int64{7, 8} {
				want, present := rewardVotes(h, cycle)
				raw, exists, err := reader.AccountKVAt(common.SystemAccountAddress, kvdomains.SystemReward, rawdb.CycleAccountVoteStateKey(cycle, owner.Bytes()), h)
				if err != nil || exists != present {
					t.Fatalf("reward history h=%d cycle=%d presence=%v want=%v: %v", h, cycle, exists, present, err)
				}
				if !present {
					continue
				}
				votes, err := statecodec.UnmarshalRewardVotes(raw)
				if err != nil || !proto.Equal(&corepb.Account{Votes: votes}, &corepb.Account{Votes: want}) {
					t.Fatalf("reward history h=%d cycle=%d differs: %v", h, cycle, err)
				}
			}
		}
	}
	checkHistory(NewPersistentHistoryReader(disk, s, 5))
	updates, err := rawdb.CollectStateUnwind(disk, 5, 1)
	if err != nil {
		t.Fatal(err)
	}
	deltas, err := rawdb.StateCommitmentDeltasFromUpdates(updates)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := domains.ApplyMuHashDeltas(disk, deltas, domains.CommitmentIdentity{BlockNum: 1, BlockHash: common.Hash{1}, TxNum: 1}, roots[0]); err != nil {
		t.Fatal(err)
	}
	assertCompactFullMuHash(t, disk)
	s, err = New(roots[0], db)
	if err != nil {
		t.Fatal(err)
	}
	if p, err := s.AccountPermissionByID(owner, 1); err != nil || p != nil {
		t.Fatal("unwind fabricated absent witness", p, err)
	}
	for h := uint64(2); h <= 5; h++ {
		if err := rawdb.DeleteStateDomainChanges(disk, h); err != nil {
			t.Fatal(err)
		}
		if err := rawdb.DeleteStateTxRange(disk, h); err != nil {
			t.Fatal(err)
		}
		if got := commit(s, disk, h); got != roots[h-1] {
			t.Fatalf("permission replay root differs at %d", h)
		}
	}
	coldDir := t.TempDir()
	refs, err := snapshots.BuildStateDomainChangeHistorySegmentsFromDB(disk, coldDir, 1, 5, "history/state-domain-change-permission-v2.seg")
	if err != nil {
		t.Fatal(err)
	}
	if err := snapshots.PublishManifest(coldDir, snapshots.NewManifest(1, 5, refs)); err != nil {
		t.Fatal(err)
	}
	manager, err := snapshots.OpenManager(coldDir)
	if err != nil {
		t.Fatal(err)
	}
	for h := uint64(1); h <= 5; h++ {
		if err := rawdb.DeleteStateDomainChanges(disk, h); err != nil {
			t.Fatal(err)
		}
		if err := rawdb.DeleteStateTxRange(disk, h); err != nil {
			t.Fatal(err)
		}
	}
	checkHistory(NewPersistentHistoryReaderWithColdHistory(disk, s, 5, manager))
}
