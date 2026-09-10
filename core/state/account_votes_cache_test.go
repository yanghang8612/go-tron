package state

import (
	"reflect"
	"testing"

	ethrawdb "github.com/ethereum/go-ethereum/core/rawdb"
	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/state/kvdomains"
	"github.com/tronprotocol/go-tron/params"
	corepb "github.com/tronprotocol/go-tron/proto/core"
)

func TestAccountVotesCacheRecordsEveryPhysicalSlotEachTransaction(t *testing.T) {
	s := newTestStateDB(t)
	owner, witness := testAddr(0x31), testAddr(0x32)
	s.CreateAccount(owner, corepb.AccountType_Normal)
	s.SetVotes(owner, []*corepb.Vote{{VoteAddress: witness.Bytes(), VoteCount: 10}})
	root, err := s.Commit()
	if err != nil {
		t.Fatal(err)
	}
	s, err = New(root, s.db)
	if err != nil {
		t.Fatal(err)
	}
	capture := func(s *StateDB) map[TransactionAccessKey]TransactionAccessMode {
		t.Helper()
		var recorder TransactionAccessRecorder
		recorder.Reset(40)
		s.SetTransactionAccessRecorder(&recorder)
		_ = s.GetVotes(owner)
		s.SetTransactionAccessRecorder(nil)
		reads := make(map[TransactionAccessKey]TransactionAccessMode)
		for _, read := range recorder.CaptureReadSet().Reads {
			reads[read.Key] = read.Mode
		}
		return reads
	}
	cold := capture(s)
	generation := TransactionAccessKey{Kind: TransactionAccessAccountKVGeneration, Address: owner}
	if cold[generation] != TransactionAccessRead {
		t.Fatal("cold vote read omitted namespace generation")
	}
	for slot := uint32(0); slot < uint32(params.MaxVoteNumber); slot++ {
		key := TransactionAccessKey{Kind: TransactionAccessAccountKV, Address: owner, KVDomain: kvdomains.AccountVotesAux, LogicalKey: string(accountVoteKey(slot))}
		if cold[key] != TransactionAccessRead {
			t.Fatalf("cold vote read omitted slot %d: got %+v, wanted key %+v", slot, cold, key)
		}
	}
	if got := capture(s); !reflect.DeepEqual(got, cold) {
		t.Fatalf("warm next-transaction dependencies differ: got %+v want %+v", got, cold)
	}
	copy, err := s.Copy()
	if err != nil {
		t.Fatal(err)
	}
	if got := capture(copy); !reflect.DeepEqual(got, cold) {
		t.Fatalf("copied warm dependencies differ: got %+v want %+v", got, cold)
	}

	// A cached read includes both the occupied slot and all missing slots; the
	// read set must intersect either kind of predecessor write plus resets.
	for _, key := range []TransactionAccessKey{
		{Kind: TransactionAccessAccountKV, Address: owner, KVDomain: kvdomains.AccountVotesAux, LogicalKey: string(accountVoteKey(0))},
		{Kind: TransactionAccessAccountKV, Address: owner, KVDomain: kvdomains.AccountVotesAux, LogicalKey: string(accountVoteKey(1))},
		generation,
	} {
		if capture(copy)[key] != TransactionAccessRead {
			t.Fatalf("cached read omitted predecessor conflict key %+v", key)
		}
	}
	mark := s.Snapshot()
	s.SetVotes(owner, []*corepb.Vote{{VoteAddress: witness.Bytes(), VoteCount: 20}, {VoteAddress: testAddr(0x33).Bytes(), VoteCount: 30}})
	if got := s.GetVotes(owner); len(got) != 2 || got[0].VoteCount != 20 {
		t.Fatalf("dirty votes = %v", got)
	}
	s.RevertToSnapshot(mark)
	if got := s.GetVotes(owner); len(got) != 1 || got[0].VoteCount != 10 {
		t.Fatalf("reverted votes = %v", got)
	}
	if got := capture(s); !reflect.DeepEqual(got, cold) {
		t.Fatalf("reverted warm dependencies differ: got %+v want %+v", got, cold)
	}
	if err := s.ResetAccountKV(owner); err != nil {
		t.Fatal(err)
	}
	s.SetVotes(owner, []*corepb.Vote{{VoteAddress: witness.Bytes(), VoteCount: 40}})
	if got := capture(s); !reflect.DeepEqual(got, cold) {
		t.Fatalf("new-generation dependencies differ: got %+v want %+v", got, cold)
	}
	if got := s.GetVotes(owner); len(got) != 1 || got[0].VoteCount != 40 {
		t.Fatalf("new-generation votes = %v", got)
	}
}

func BenchmarkAccountVotesCachedWithoutRecorder(b *testing.B) {
	db := NewDatabaseWithConfig(ethrawdb.NewMemoryDatabase(), DatabaseConfig{})
	b.Cleanup(func() {
		if err := db.Close(); err != nil {
			b.Error(err)
		}
	})
	s, err := New(tcommon.Hash{}, db)
	if err != nil {
		b.Fatal(err)
	}
	owner := testAddr(0x31)
	s.CreateAccount(owner, corepb.AccountType_Normal)
	s.SetVotes(owner, []*corepb.Vote{{VoteAddress: testAddr(0x32).Bytes(), VoteCount: 10}})
	_ = s.GetVotes(owner)
	b.Run("before", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			materializedVotesBenchmarkSink = getCachedVotesBeforeAccessReplay(s, owner)
		}
	})
	b.Run("after", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			materializedVotesBenchmarkSink = s.GetVotes(owner)
		}
	})
}

func getCachedVotesBeforeAccessReplay(s *StateDB, owner tcommon.Address) []*corepb.Vote {
	obj := s.getStateObject(owner)
	if obj == nil {
		return nil
	}
	if err := materializeCachedVotesBeforeAccessReplay(s, obj); err != nil {
		return nil
	}
	return obj.account.Votes()
}

// Preserve the call boundary of the original, non-inlineable materializer.
// The unloaded fallback is unused by this cached-read benchmark.
//
//go:noinline
func materializeCachedVotesBeforeAccessReplay(s *StateDB, obj *stateObject) error {
	if obj == nil || obj.account == nil || obj.accountVotesLoaded {
		return nil
	}
	return s.materializeAccountVotes(obj)
}
