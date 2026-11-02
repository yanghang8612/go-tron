package main

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"flag"
	"math/big"
	"os"
	"path/filepath"
	"testing"

	"github.com/ethereum/go-ethereum/ethdb/memorydb"
	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/reward"
	"github.com/tronprotocol/go-tron/core/state"
	"github.com/tronprotocol/go-tron/core/state/kvdomains"
	"github.com/tronprotocol/go-tron/core/state/statecodec"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	"google.golang.org/protobuf/proto"
)

func accountFixture() *corepb.Account {
	a := &corepb.Account{Address: common.Address{0x41, 7}.Bytes(), Balance: 999, Allowance: 73, AssetV2: map[string]int64{"1000001": 123, "1000002": 0}, OwnerPermission: &corepb.Permission{Threshold: 1}, ActivePermission: []*corepb.Permission{{Id: 2, Threshold: 2}}, Votes: []*corepb.Vote{{VoteAddress: common.Address{0x41, 9}.Bytes(), VoteCount: 5}, {VoteAddress: common.Address{0x41, 9}.Bytes(), VoteCount: 7}}}
	a.Votes[0].ProtoReflect().SetUnknown([]byte{0xa0, 0x06, 1})
	a.ProtoReflect().SetUnknown([]byte{0xa8, 0x06, 3})
	return a
}
func projectFixture(t *testing.T, a *corepb.Account) ([]byte, *corepb.Account) {
	t.Helper()
	full, e := statecodec.Marshal(a)
	if e != nil {
		t.Fatal(e)
	}
	p, v, e := projectVotes(full)
	if e != nil {
		t.Fatal(e)
	}
	var out corepb.Account
	if e := statecodec.Unmarshal(p, &out); e != nil {
		t.Fatal(e)
	}
	if _, e := verifyVotes(v, out.Votes); e != nil {
		t.Fatal(e)
	}
	again, e := statecodec.Marshal(&out)
	if e != nil || !bytes.Equal(again, p) {
		t.Fatalf("native reencode %v", e)
	}
	return p, &out
}
func TestProjectionKeepsVotesAndUnknownOnly(t *testing.T) {
	a := accountFixture()
	p, b := projectFixture(t, a)
	if !proto.Equal(&corepb.Account{Votes: a.Votes}, b) {
		t.Fatal("Votes including unknown fields changed")
	}
	if proto.Equal(a, b) {
		t.Fatal("experiment unexpectedly claims complete Account equality")
	}
	if len(b.AssetV2) != 0 || b.OwnerPermission != nil || len(b.ActivePermission) != 0 || b.Balance != 0 || b.Allowance != 0 || len(b.ProtoReflect().GetUnknown()) != 0 {
		t.Fatal("non-Votes fields retained")
	}
	native, _ := statecodec.Marshal(a)
	for i := 0; i < len(native); i++ {
		if _, _, e := projectVotes(native[:i]); e == nil {
			t.Fatalf("source truncation %d accepted", i)
		}
	}
	present, empty := projectedPresence(p, true)
	if !present || empty {
		t.Fatal("wrong nonempty presence")
	}
}
func TestProjectionEmptyAndAbsentRemainDistinct(t *testing.T) {
	p, a := projectFixture(t, &corepb.Account{Address: []byte{0x41, 5}, Allowance: 99})
	if len(a.Votes) != 0 {
		t.Fatal("votes gained")
	}
	if present, empty := projectedPresence(p, true); !present || !empty {
		t.Fatal("present empty snapshot lost")
	}
	if present, empty := projectedPresence(nil, false); present || empty {
		t.Fatal("absent snapshot gained")
	}
	wrapped := rawdb.EncodeStateKVLatestValue(p)
	back, e := rawdb.DecodeStateKVLatestValue(wrapped)
	if e != nil || !bytes.Equal(back, p) {
		t.Fatalf("empty projection envelope %v", e)
	}
}

type fixtureStore struct{}

func (fixtureStore) ReadCycleReward(cycle int64, _ []byte) int64 { return 1000 + cycle*10 }
func (fixtureStore) ReadCycleVote(cycle int64, _ []byte) int64   { return 100 + cycle }
func (fixtureStore) ReadWitnessVI(cycle int64, _ []byte) *big.Int {
	return new(big.Int).Mul(big.NewInt(cycle+1), reward.DecimalOfViReward)
}
func entries(v []*corepb.Vote) []reward.VoteEntry {
	out := make([]reward.VoteEntry, len(v))
	for i, x := range v {
		out[i] = reward.VoteEntry{Witness: common.BytesToAddress(x.VoteAddress), Count: x.VoteCount}
	}
	return out
}

// These fixtures pin the read-set consequence of the checked Java source:
// MortgageService/VoteRewardUtil reward math consumes getVotesList only, and
// current-cycle early return uses snapshot existence, not snapshot allowance.
// They execute Go's already-separated actuator/TVM math, not a Java binary.
func TestProjectionRewardReadSetFixtures(t *testing.T) {
	a := accountFixture()
	p, b := projectFixture(t, a)
	before, after := entries(a.Votes), entries(b.Votes)
	store := fixtureStore{}
	for _, fork := range []int64{0, 3, 100} {
		for _, opt := range []bool{false, true} {
			dp := state.NewDynamicProperties()
			dp.SetNewRewardAlgorithmEffectiveCycle(fork)
			dp.SetAllowOldRewardOpt(opt)
			for _, cut := range [][2]int64{{2, 2}, {3, 2}, {1, 2}, {1, 5}, {3, 4}} {
				want := reward.ComputeVoterReward(store, dp, before, cut[0], cut[1])
				got := reward.ComputeVoterReward(store, dp, after, cut[0], cut[1])
				if want != got {
					t.Fatalf("actuator reward changed fork=%d opt=%v cut=%v", fork, opt, cut)
				}
				if reward.ComputeVoterRewardTVM(store, before, cut[0], cut[1]) != reward.ComputeVoterRewardTVM(store, after, cut[0], cut[1]) {
					t.Fatal("TVM reward changed")
				}
			}
		}
	}
	for _, present := range []bool{false, true} {
		for _, cut := range [][3]int64{{3, 4, 3}, {3, 4, 4}, {2, 3, 4}} {
			oldEarly := cut[0] == cut[2] && present
			newPresent, _ := projectedPresence(p, present)
			if oldEarly != (cut[0] == cut[2] && newPresent) {
				t.Fatal("current-cycle presence edge changed")
			}
		}
	}
}

func TestProjectionRunner(t *testing.T) {
	dir := t.TempDir()
	in := filepath.Join(dir, "rows.bin")
	db := memorydb.New()
	defer db.Close()
	for i := 0; i < 200; i++ {
		addr := common.Address{0x41}
		binary.BigEndian.PutUint64(addr[13:], uint64(i))
		a := accountFixture()
		if i == 0 {
			a = &corepb.Account{Allowance: 77}
		}
		v, e := statecodec.Marshal(a)
		if e != nil {
			t.Fatal(e)
		}
		if e := rawdb.WriteStateKVLatest(db, common.SystemAccountAddress, 0, kvdomains.SystemReward, rawdb.CycleAccountVoteStateKey(3, addr.Bytes()), v); e != nil {
			t.Fatal(e)
		}
	}
	if e := rawdb.WriteStateKVLatest(db, common.SystemAccountAddress, 0, kvdomains.SystemReward, rawdb.BeginCycleStateKey(common.Address{0x41, 9}.Bytes()), make([]byte, 8)); e != nil {
		t.Fatal(e)
	}
	f, e := os.Create(in)
	if e != nil {
		t.Fatal(e)
	}
	it := db.NewIterator(nil, nil)
	for it.Next() {
		k, v := it.Key(), it.Value()
		b := binary.BigEndian.AppendUint32(nil, uint32(len(k)))
		b = append(b, k...)
		b = binary.BigEndian.AppendUint32(b, uint32(len(v)))
		b = append(b, v...)
		if _, e := f.Write(b); e != nil {
			t.Fatal(e)
		}
	}
	if e := it.Error(); e != nil {
		t.Fatal(e)
	}
	it.Release()
	if e := f.Close(); e != nil {
		t.Fatal(e)
	}
	savedArgs, savedFlag := os.Args, flag.CommandLine
	t.Cleanup(func() { os.Args, flag.CommandLine = savedArgs, savedFlag })
	out := filepath.Join(dir, "out")
	os.Args = []string{"projection-probe", "-rows", in, "-out", out, "-decode-iterations", "1"}
	flag.CommandLine = flag.NewFlagSet("projection-probe", flag.ContinueOnError)
	if e := run(); e != nil {
		t.Fatal(e)
	}
	raw, e := os.ReadFile(filepath.Join(out, "results.json"))
	if e != nil {
		t.Fatal(e)
	}
	var d struct {
		Rows         int       `json:"rows"`
		Votes        int       `json:"account_vote_rows"`
		Empty        int       `json:"present_empty_snapshots"`
		FullEquality bool      `json:"full_account_byte_equality_claimed"`
		MuHash       bool      `json:"production_muhash_verified"`
		Candidate    tableStat `json:"candidate_table"`
	}
	if e := json.Unmarshal(raw, &d); e != nil {
		t.Fatal(e)
	}
	if d.Rows != 201 || d.Votes != 200 || d.Empty != 1 || d.FullEquality || d.MuHash || d.Candidate.Rows != 201 || d.Candidate.Files != 1 {
		t.Fatalf("wrong result %+v", d)
	}
}
