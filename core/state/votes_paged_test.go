package state

import (
	"encoding/binary"
	"reflect"
	"testing"

	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/state/kvdomains"
)

func pagedVoter(i uint64) tcommon.Address {
	a := tcommon.Address{0x41}
	binary.BigEndian.PutUint64(a[13:], i)
	return a
}

func TestVotesPagesOrderDuplicateClearAndRevert(t *testing.T) {
	s := newTestStateDB(t)
	if _, present, err := s.ReadVotesIndexStrict(); err != nil || present {
		t.Fatal(present, err)
	}
	var want []tcommon.Address
	for i := uint64(0); i < 3*votesIndexPageEntries+7; i++ {
		a := pagedVoter(i)
		if err := s.AppendVotesIndex(a); err != nil {
			t.Fatal(err)
		}
		want = append(want, a)
	}
	mark := s.Snapshot()
	if err := s.AppendVotesIndex(want[0]); err != nil {
		t.Fatal(err)
	}
	if got := s.ReadVotesIndex(); !reflect.DeepEqual(got, want) {
		t.Fatal("duplicate/order")
	}
	if err := s.DeleteVotes(want[5]); err != nil {
		t.Fatal(err)
	}
	if got := s.ReadVotesIndex(); !reflect.DeepEqual(got, want) {
		t.Fatal("delete changed index")
	}
	if err := s.WriteVotesIndex(nil); err != nil {
		t.Fatal(err)
	}
	if got, present, err := s.ReadVotesIndexStrict(); err != nil || !present || len(got) != 0 {
		t.Fatal(got, present, err)
	}
	for _, prefix := range [][]byte{rawdb.VotesIndexPageStatePrefix(), rawdb.VotesIndexMemberStatePrefix()} {
		if err := s.IterateAccountKV(tcommon.SystemAccountAddress, kvdomains.WitnessVoteState, prefix, func(k, v []byte) (bool, error) { t.Fatal("clear retained row"); return false, nil }); err != nil {
			t.Fatal(err)
		}
	}
	s.RevertToSnapshot(mark)
	if got := s.ReadVotesIndex(); !reflect.DeepEqual(got, want) {
		t.Fatal("revert")
	}
	want = []tcommon.Address{want[4], want[1], want[4]}
	if err := s.WriteVotesIndex(want); err != nil {
		t.Fatal(err)
	}
	if err := s.AppendVotesIndex(want[0]); err != nil {
		t.Fatal(err)
	}
	if got := s.ReadVotesIndex(); !reflect.DeepEqual(got, want) {
		t.Fatal("explicit duplicates lost")
	}
}

func TestVotesPageCorruptionDoesNotBecomeEmptyOrOverwrite(t *testing.T) {
	s := newTestStateDB(t)
	if err := s.AppendVotesIndex(pagedVoter(1)); err != nil {
		t.Fatal(err)
	}
	key := rawdb.VotesIndexPageStateKey(0)
	if err := s.SystemKVPut(kvdomains.WitnessVoteState, key, []byte{0}); err != nil {
		t.Fatal(err)
	}
	if _, _, err := s.ReadVotesIndexStrict(); err == nil {
		t.Fatal("corrupt page accepted")
	}
	if err := s.AppendVotesIndex(pagedVoter(2)); err == nil {
		t.Fatal("corrupt tail overwritten")
	}
	v, _, _ := s.SystemKVGet(kvdomains.WitnessVoteState, key)
	if !reflect.DeepEqual(v, []byte{0}) {
		t.Fatal("corrupt page mutated")
	}
}
