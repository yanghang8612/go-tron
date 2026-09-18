package main

import (
	"context"
	"encoding/hex"
	"fmt"
	"math"
	"math/big"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/reward"
	"github.com/tronprotocol/go-tron/core/state"
	"github.com/tronprotocol/go-tron/core/types"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	"google.golang.org/protobuf/proto"
)

func TestPlanHistoricalWithdrawRewardSegments(t *testing.T) {
	inputs := historicalRewardInputs{
		AccountVotes:     []historicalVote{{Witness: "4102", Count: 90}},
		BeginCycle:       historicalRawInt64{Value: 10},
		EndCycle:         historicalRawInt64{Value: 11},
		CurrentCycle:     historicalRawInt64{Value: 13},
		ChangeDelegation: historicalRawInt64{Value: 1},
		BeginSnapshot: &historicalAccountSnapshot{
			Present: true,
			Votes:   []historicalVote{{Witness: "4101", Count: 250}},
		},
	}
	plan := planHistoricalRewardSegments(inputs)
	if plan.EarlyReturnReason != "" || len(plan.Segments) != 2 {
		t.Fatalf("plan = %+v", plan)
	}
	if got := plan.Segments[0]; got.Source != "begin_cycle_account_vote_snapshot" || got.BeginCycle != 10 || got.EndCycle != 11 || !reflect.DeepEqual(got.Votes, inputs.BeginSnapshot.Votes) {
		t.Fatalf("snapshot segment = %+v", got)
	}
	if got := plan.Segments[1]; got.Source != "prestate_account_votes" || got.BeginCycle != 11 || got.EndCycle != 13 || !reflect.DeepEqual(got.Votes, inputs.AccountVotes) {
		t.Fatalf("current-vote segment = %+v", got)
	}
	if plan.PostBeginCycle != 13 || plan.PostEndCycle != 14 {
		t.Fatalf("post cursors = %d/%d", plan.PostBeginCycle, plan.PostEndCycle)
	}

	inputs.BeginCycle.Value = 13
	inputs.EndCycle.Value = 14
	inputs.CurrentCycle.Value = 13
	plan = planHistoricalRewardSegments(inputs)
	if plan.EarlyReturnReason != "current_cycle_snapshot_already_present" || len(plan.Segments) != 0 {
		t.Fatalf("current-cycle early return = %+v", plan)
	}

	hybrid := historicalRewardSegment{BeginCycle: 10, EndCycle: 20}
	if got, want := historicalVIEndpointCycles(hybrid, 15), []int64{9, 14, 19}; !reflect.DeepEqual(got, want) {
		t.Fatalf("hybrid VI endpoints = %v, want %v", got, want)
	}
}

type historicalRewardTestStore struct {
	rewards map[int64]map[tcommon.Address]int64
	votes   map[int64]map[tcommon.Address]int64
	vis     map[int64]map[tcommon.Address]*big.Int
}

func (s historicalRewardTestStore) ReadCycleReward(cycle int64, addr []byte) int64 {
	return s.rewards[cycle][tcommon.BytesToAddress(addr)]
}

func (s historicalRewardTestStore) ReadCycleVote(cycle int64, addr []byte) int64 {
	return s.votes[cycle][tcommon.BytesToAddress(addr)]
}

func (s historicalRewardTestStore) ReadWitnessVI(cycle int64, addr []byte) *big.Int {
	if value := s.vis[cycle][tcommon.BytesToAddress(addr)]; value != nil {
		return new(big.Int).Set(value)
	}
	return new(big.Int)
}

func TestCalculateHistoricalReward_JavaCompoundAssignment(t *testing.T) {
	w1 := tcommon.BytesToAddress([]byte{0x41, 1})
	w2 := tcommon.BytesToAddress([]byte{0x41, 2})
	segment := historicalRewardSegment{
		BeginCycle: 1,
		EndCycle:   2,
		Votes: []historicalVote{
			{Witness: fmtAddress(w1), Count: 1},
			{Witness: fmtAddress(w2), Count: 1},
		},
		Cycles: []historicalRewardCycle{{Cycle: 1, Rows: []historicalRewardRow{
			{Witness: fmtAddress(w1), UserVotes: 1, CycleVote: historicalRawInt64{Value: 1}, CycleReward: historicalRawInt64{Value: 1}},
			{Witness: fmtAddress(w2), UserVotes: 1, CycleVote: historicalRawInt64{Value: 49}, CycleReward: historicalRawInt64{Value: 49}},
		}}},
	}
	plan := historicalRewardPlan{Segments: []historicalRewardSegment{segment}}
	deployed, fixed, err := calculateHistoricalReward(&plan, math.MaxInt64, false)
	if err != nil {
		t.Fatal(err)
	}
	if deployed != 1 || fixed != 2 {
		t.Fatalf("deployed/fixed = %d/%d, want 1/2", deployed, fixed)
	}

	dp := state.NewDynamicProperties()
	dp.SetNewRewardAlgorithmEffectiveCycle(math.MaxInt64)
	store := historicalRewardTestStore{
		rewards: map[int64]map[tcommon.Address]int64{1: {w1: 1, w2: 49}},
		votes:   map[int64]map[tcommon.Address]int64{1: {w1: 1, w2: 49}},
	}
	production := reward.ComputeVoterReward(store, dp, []reward.VoteEntry{{Witness: w1, Count: 1}, {Witness: w2, Count: 1}}, 1, 2)
	if production != fixed {
		t.Fatalf("production fixed reward = %d, exporter fixed reward = %d", production, fixed)
	}
}

func TestCalculateHistoricalReward_HybridAndPaidGate(t *testing.T) {
	w := tcommon.BytesToAddress([]byte{0x41, 1})
	wHex := fmtAddress(w)
	decimal := new(big.Int).Exp(big.NewInt(10), big.NewInt(18), nil)
	vi2 := new(big.Int).Mul(big.NewInt(100), decimal)
	vi3 := new(big.Int).Mul(big.NewInt(105), decimal)
	hybrid := historicalRewardSegment{
		BeginCycle: 1,
		EndCycle:   4,
		Votes:      []historicalVote{{Witness: wHex, Count: 10}},
		Cycles: []historicalRewardCycle{
			{Cycle: 1, Rows: []historicalRewardRow{{Witness: wHex, UserVotes: 10, CycleVote: historicalRawInt64{Value: 100}, CycleReward: historicalRawInt64{Value: 100}}}},
			{Cycle: 2, Rows: []historicalRewardRow{{Witness: wHex, UserVotes: 10, CycleVote: historicalRawInt64{Value: 100}, CycleReward: historicalRawInt64{Value: 100}}}},
			{Cycle: 3, Rows: []historicalRewardRow{{Witness: wHex, UserVotes: 10, CycleVote: historicalRawInt64{Value: 100}, CycleReward: historicalRawInt64{Value: 0}}}},
		},
		VIEndpoints: []historicalVIEndpoint{
			{Cycle: 0, Witness: wHex, Value: historicalRawBigInt{Value: "0"}},
			{Cycle: 2, Witness: wHex, Value: historicalRawBigInt{Value: vi2.String()}},
			{Cycle: 3, Witness: wHex, Value: historicalRawBigInt{Value: vi3.String()}},
		},
	}
	plan := historicalRewardPlan{Segments: []historicalRewardSegment{hybrid}}
	_, fixed, err := calculateHistoricalReward(&plan, 3, false)
	if err != nil {
		t.Fatal(err)
	}
	store := historicalRewardTestStore{
		rewards: map[int64]map[tcommon.Address]int64{1: {w: 100}, 2: {w: 100}},
		votes:   map[int64]map[tcommon.Address]int64{1: {w: 100}, 2: {w: 100}},
		vis:     map[int64]map[tcommon.Address]*big.Int{2: {w: vi2}, 3: {w: vi3}},
	}
	dp := state.NewDynamicProperties()
	dp.SetNewRewardAlgorithmEffectiveCycle(3)
	production := reward.ComputeVoterReward(store, dp, []reward.VoteEntry{{Witness: w, Count: 10}}, 1, 4)
	if fixed != 70 || production != fixed {
		t.Fatalf("hybrid fixed/production = %d/%d, want 70", fixed, production)
	}

	overflow := historicalRewardSegment{
		BeginCycle: 1,
		EndCycle:   3,
		Votes:      []historicalVote{{Witness: wHex, Count: math.MaxInt64}},
		Cycles: []historicalRewardCycle{
			{Cycle: 1, Rows: []historicalRewardRow{{Witness: wHex, UserVotes: math.MaxInt64, CycleVote: historicalRawInt64{Value: 1}, CycleReward: historicalRawInt64{Value: 2}}}},
			{Cycle: 2, Rows: []historicalRewardRow{{Witness: wHex, UserVotes: math.MaxInt64, CycleVote: historicalRawInt64{Value: 1}, CycleReward: historicalRawInt64{Value: 2}}}},
		},
	}
	plan = historicalRewardPlan{Segments: []historicalRewardSegment{overflow}}
	deployed, fixed, err := calculateHistoricalReward(&plan, math.MaxInt64, false)
	if err != nil {
		t.Fatal(err)
	}
	if deployed != 0 || fixed != 0 || plan.Segments[0].Computation.DeployedR1Raw != -2 || plan.Segments[0].Computation.FixedJavaRaw != -2 {
		t.Fatalf("paid<=0 gate = deployed %d fixed %d computation %+v", deployed, fixed, plan.Segments[0].Computation)
	}
}

func fmtAddress(addr tcommon.Address) string {
	return fmt.Sprintf("%x", addr.Bytes())
}

func TestHistoricalWithdrawHashFormattingAndComparison(t *testing.T) {
	const knownWithdrawTxID = "22636ad43e891be19129b2a19fac9bde68047241f5b861cb78795c29e1d69734"
	// raw_data_hex returned for the incident transaction. Hashing the decoded
	// protobuf exercises Transaction.Hash and findExpectedWithdraw rather than
	// comparing two strings produced by the formatter under test.
	const knownWithdrawRawData = "0a0217a8220819437f5959495fcc40a8ce9eefc92f5a53080d124f0a34747970652e676f6f676c65617069732e636f6d2f70726f746f636f6c2e576974686472617742616c616e6365436f6e747261637412170a15418924da63576c7c332cd336217f6a888004d4948c709d8c9befc92f"
	rawBytes, err := hex.DecodeString(knownWithdrawRawData)
	if err != nil {
		t.Fatal(err)
	}
	var raw corepb.TransactionRaw
	if err := proto.Unmarshal(rawBytes, &raw); err != nil {
		t.Fatal(err)
	}
	ownerBytes, err := hex.DecodeString("418924da63576c7c332cd336217f6a888004d4948c")
	if err != nil {
		t.Fatal(err)
	}
	owner := tcommon.BytesToAddress(ownerBytes)

	// These deterministic synthetic headers exercise block-hash formatting;
	// their fixed hashes below are not the mainnet hashes at these heights.
	preBlock := types.NewBlockFromPB(&corepb.Block{BlockHeader: &corepb.BlockHeader{RawData: &corepb.BlockHeaderRaw{
		Number: 34740158, Timestamp: 1634736719000, AccountStateRoot: []byte{1, 2, 3},
	}}})
	withdrawBlock := types.NewBlockFromPB(&corepb.Block{
		BlockHeader: &corepb.BlockHeader{RawData: &corepb.BlockHeaderRaw{
			Number: 34740159, Timestamp: 1634736722000, ParentHash: preBlock.Hash().Bytes(), AccountStateRoot: []byte{4, 5, 6},
		}},
		Transactions: []*corepb.Transaction{{RawData: &raw}},
	})
	index, txID, preceding, err := findExpectedWithdraw(withdrawBlock, owner, knownWithdrawTxID)
	if err != nil {
		t.Fatal(err)
	}
	if index != 0 || txID != knownWithdrawTxID || len(preceding) != 0 {
		t.Fatalf("known withdrawal = index %d txid %q preceding %d", index, txID, len(preceding))
	}

	prestateHash, withdrawHash := historicalBlockHashStrings(preBlock, withdrawBlock)
	if want := "00000000021217beb941e24739c3ee414a02c5b456b50f28d545402670494560"; prestateHash != want {
		t.Fatalf("prestate hash = %q, want %q", prestateHash, want)
	}
	if want := "00000000021217bfc34c1e9f09039aee0a36b54a791989a56cb7945a04a807b3"; withdrawHash != want {
		t.Fatalf("withdraw block hash = %q, want %q", withdrawHash, want)
	}

	known := tcommon.HexToHash(knownWithdrawTxID)
	if err := requireHistoricalHashMatch("state tx range block hash", known, known); err != nil {
		t.Fatalf("equal block hashes rejected: %v", err)
	}

	different := known
	different[0] ^= 0xff
	err = requireHistoricalHashMatch("withdraw block parent", different, known)
	if err == nil {
		t.Fatal("different block hashes accepted")
	}
	wantError := "withdraw block parent " + different.Hex() + " does not match prestate hash " + knownWithdrawTxID
	if err.Error() != wantError {
		t.Fatalf("block hash mismatch error = %q, want %q", err, wantError)
	}
	if strings.Contains(err.Error(), "32323633") {
		t.Fatalf("block hash mismatch contains double-encoded ASCII hex: %q", err)
	}
}

func TestHistoricalWithdrawExportLimits(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	kv := &historicalKVReader{ctx: ctx, max: 1}
	if _, _, err := kv.raw(tcommon.SystemAccountAddress, 0, nil); err != context.Canceled {
		t.Fatalf("canceled raw read error = %v, want context.Canceled", err)
	}
	invalid := historicalWithdrawExportOptions{
		Timeout:        time.Minute,
		MaxCycles:      historicalWithdrawMaxCycles,
		MaxKeys:        historicalWithdrawMaxKeys + 1,
		MaxOutputBytes: historicalWithdrawMaxOutputBytes,
	}
	if err := exportHistoricalWithdrawInputs(invalid); err == nil || !strings.Contains(err.Error(), "limits") {
		t.Fatalf("invalid MaxKeys error = %v", err)
	}

	dir := t.TempDir()
	datadir := filepath.Join(dir, "datadir")
	if err := os.MkdirAll(datadir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := validateHistoricalOutputPath(datadir, filepath.Join(datadir, "evidence.json")); err == nil {
		t.Fatal("accepted output inside datadir")
	}
	out := filepath.Join(dir, "evidence.json")
	if err := validateHistoricalOutputPath(datadir, out); err != nil {
		t.Fatal(err)
	}
	if err := writeHistoricalExportAtomic(out, []byte("first\n")); err != nil {
		t.Fatal(err)
	}
	if err := writeHistoricalExportAtomic(out, []byte("second\n")); err == nil {
		t.Fatal("atomic evidence publish overwrote existing output")
	}
	got, err := os.ReadFile(out)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "first\n" {
		t.Fatalf("output = %q, want first evidence", got)
	}
}
