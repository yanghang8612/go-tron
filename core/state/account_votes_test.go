package state

import (
	"testing"

	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/state/kvdomains"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	"google.golang.org/protobuf/proto"
)

func splitTestVote(marker byte, count int64) *corepb.Vote {
	return &corepb.Vote{VoteAddress: testAddr(marker).Bytes(), VoteCount: count}
}

func TestAccountVotesPersistOutsideAccountEnvelope(t *testing.T) {
	sdb := newTestStateDB(t)
	addr := testAddr(0x96)
	sdb.CreateAccount(addr, corepb.AccountType_Normal)
	vote1 := splitTestVote(0x51, 11)
	vote2 := splitTestVote(0x52, 22)
	sdb.SetVotes(addr, []*corepb.Vote{vote2, vote1})

	root, err := sdb.Commit()
	if err != nil {
		t.Fatal(err)
	}
	raw, ok, err := rawdb.ReadStateAccountLatest(sdb.db.DiskDB(), addr)
	if err != nil || !ok {
		t.Fatalf("read account latest: ok=%v err=%v", ok, err)
	}
	envelope, err := DecodeStateAccountV3(raw)
	if err != nil {
		t.Fatal(err)
	}
	var stored corepb.Account
	if err := proto.Unmarshal(envelope.AccountProto, &stored); err != nil {
		t.Fatal(err)
	}
	if len(stored.Votes) != 0 {
		t.Fatalf("split votes leaked into account envelope: %+v", stored.Votes)
	}
	for index, want := range []*corepb.Vote{vote2, vote1} {
		value, exists, readErr := rawdb.ReadStateKVLatest(sdb.db.DiskDB(), addr, 0, kvdomains.AccountVotesAux, accountVoteKey(uint32(index)))
		if readErr != nil || !exists {
			t.Fatalf("read vote row %d: exists=%v err=%v", index, exists, readErr)
		}
		var got corepb.Vote
		if err := proto.Unmarshal(value, &got); err != nil {
			t.Fatalf("decode vote row %d: %v", index, err)
		}
		if !proto.Equal(&got, want) {
			t.Fatalf("vote row %d = %+v, want %+v", index, &got, want)
		}
	}

	reopened, err := New(root, sdb.db)
	if err != nil {
		t.Fatal(err)
	}
	votes := reopened.GetVotes(addr)
	if len(votes) != 2 || !proto.Equal(votes[0], vote2) || !proto.Equal(votes[1], vote1) {
		t.Fatalf("materialized votes = %+v", votes)
	}
	if account := reopened.GetAccount(addr); account == nil || len(account.Votes()) != 2 || !proto.Equal(account.Votes()[0], vote2) {
		t.Fatalf("full account votes = %+v", account)
	}
}

func TestAccountVotesSnapshotRevertInvalidatesMaterializedCache(t *testing.T) {
	sdb := newTestStateDB(t)
	addr := testAddr(0x97)
	sdb.CreateAccount(addr, corepb.AccountType_Normal)
	vote1 := splitTestVote(0x61, 11)
	vote2 := splitTestVote(0x62, 22)
	sdb.SetVotes(addr, []*corepb.Vote{vote1})
	if got := sdb.GetVotes(addr); len(got) != 1 || !proto.Equal(got[0], vote1) {
		t.Fatalf("initial votes = %+v", got)
	}

	snapshot := sdb.Snapshot()
	sdb.SetVotes(addr, []*corepb.Vote{vote2})
	if got := sdb.GetVotes(addr); len(got) != 1 || !proto.Equal(got[0], vote2) {
		t.Fatalf("updated votes = %+v", got)
	}
	sdb.RevertToSnapshot(snapshot)
	if got := sdb.GetVotes(addr); len(got) != 1 || !proto.Equal(got[0], vote1) {
		t.Fatalf("votes after revert = %+v", got)
	}
}

func TestStateDBCopyKeepsSplitFieldsOutOfAccountEnvelope(t *testing.T) {
	sdb := newTestStateDB(t)
	addr := testAddr(0x98)
	sdb.CreateAccount(addr, corepb.AccountType_Normal)
	sdb.SetTRC10Balance(addr, 1_000_001, 77)
	sdb.SetPermissions(addr, splitTestPermission(corepb.Permission_Owner, 0, "owner", 0x71), nil, nil)
	sdb.SetVotes(addr, []*corepb.Vote{splitTestVote(0x72, 88)})
	sdb.SetEnergyUsage(addr, 99)
	sdb.FreezeV1Bandwidth(addr, 111, 222)
	sdb.FreezeV1TronPower(addr, 333, 444)
	if account := sdb.GetAccount(addr); account == nil || len(account.Votes()) != 1 || account.Proto().AssetV2["1000001"] != 77 {
		t.Fatalf("materialized source account = %+v", account)
	}

	copied, err := sdb.Copy()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := copied.Commit(); err != nil {
		t.Fatal(err)
	}
	raw, ok, err := rawdb.ReadStateAccountLatest(sdb.db.DiskDB(), addr)
	if err != nil || !ok {
		t.Fatalf("read copied account latest: ok=%v err=%v", ok, err)
	}
	envelope, err := DecodeStateAccountV3(raw)
	if err != nil {
		t.Fatal(err)
	}
	var stored corepb.Account
	if err := proto.Unmarshal(envelope.AccountProto, &stored); err != nil {
		t.Fatal(err)
	}
	if len(stored.AssetV2) != 0 || stored.OwnerPermission != nil || len(stored.Votes) != 0 || stored.AccountResource != nil || len(stored.Frozen) != 0 || stored.TronPower != nil {
		t.Fatalf("copied account leaked split fields into envelope: %+v", &stored)
	}
}
