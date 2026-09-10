package actuator

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math/big"
	"testing"

	ethrawdb "github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/blockbuffer"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/reward"
	"github.com/tronprotocol/go-tron/core/state"
	"github.com/tronprotocol/go-tron/core/state/kvdomains"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	"google.golang.org/protobuf/proto"
)

// Reads use real blockbuffer latest-state layers, while the embedded database
// provides the remaining database interface. Reward mutations stay in StateDB's
// dirty overlay during these tests and benchmarks.
type rewardLayerDatabase struct {
	ethdb.Database
	overlay     *blockbuffer.Buffer
	prefixReads int
}

func (d *rewardLayerDatabase) Get(key []byte) ([]byte, error) { return d.overlay.Get(key) }
func (d *rewardLayerDatabase) Has(key []byte) (bool, error)   { return d.overlay.Has(key) }
func (d *rewardLayerDatabase) NewIterator(prefix, start []byte) ethdb.Iterator {
	return d.overlay.NewIterator(prefix, start)
}
func (d *rewardLayerDatabase) NewStateKVLatestIterator(prefix []byte, accountID common.AccountID, physical []byte) ethdb.Iterator {
	d.prefixReads++
	return d.overlay.NewStateKVLatestIterator(prefix, accountID, physical)
}
func (d *rewardLayerDatabase) GetNoCopyCachedStateKVLatest(prefix []byte, accountID common.AccountID, generation uint64, domain uint16, key []byte) ([]byte, error) {
	return d.overlay.GetNoCopyCachedStateKVLatest(prefix, accountID, generation, domain, key)
}

type rewardSelectiveCase struct {
	name                                            string
	begin, end, current                             int64
	votes, snapshot, snapshotVotes, missing, legacy bool
}

var rewardSelectiveCases = []rewardSelectiveCase{
	{name: "pending_votes", begin: 1, end: 1, current: 10, votes: true},
	{name: "snapshot_and_current_votes", begin: 8, end: 9, current: 10, votes: true, snapshot: true, snapshotVotes: true},
	{name: "snapshot_without_current_votes", begin: 8, end: 9, current: 10, snapshot: true, snapshotVotes: true},
	{name: "empty_snapshot", begin: 8, end: 9, current: 10, votes: true, snapshot: true},
	{name: "no_votes", begin: 1, end: 1, current: 10},
	{name: "future", begin: 11, end: 12, current: 10, votes: true},
	{name: "current_snapshot", begin: 10, end: 11, current: 10, votes: true, snapshot: true, snapshotVotes: true},
	{name: "current_without_snapshot", begin: 10, end: 11, current: 10, votes: true},
	{name: "missing_account", begin: 1, end: 1, current: 10, missing: true},
	{name: "legacy_rewards", begin: 1, end: 1, current: 10, votes: true, legacy: true},
}

func newRewardSelectiveFixture(tb testing.TB, tc rewardSelectiveCase, layers int) (*state.Database, *state.DynamicProperties, common.Address, *rewardLayerDatabase) {
	tb.Helper()
	disk := ethrawdb.NewMemoryDatabase()
	tb.Cleanup(func() { _ = disk.Close() })
	seedDB := state.NewDatabaseWithConfig(disk, state.DatabaseConfig{})
	tb.Cleanup(func() { _ = seedDB.Close() })
	s, err := state.New(common.Hash{}, seedDB)
	if err != nil {
		tb.Fatal(err)
	}
	voter, witness := makeTestAddr(0x20), makeTestAddr(0x30)
	if !tc.missing {
		seedAccount(s, voter, 12345)
		s.SetAllowance(voter, 50)
		for i := 0; i < 24; i++ {
			name := fmt.Sprintf("asset-%02d", i)
			s.SetTRC10BalanceByName(voter, []byte(name), int64(i+1))
			s.SetTRC10Balance(voter, 1000000+int64(i), int64(2*i+1))
			s.SetFreeAssetNetUsage(voter, name, int64(3*i))
			s.SetFreeAssetNetUsageV2(voter, name, int64(4*i))
			s.SetLatestAssetOperationTime(voter, name, int64(5*i))
			s.SetLatestAssetOperationTimeV2(voter, name, int64(6*i))
		}
		s.FreezeV1Bandwidth(voter, 17, 123456)
		s.FreezeV1Energy(voter, 19, 123457)
		s.SetPermissions(voter, &corepb.Permission{Type: corepb.Permission_Owner, Threshold: 1, Keys: []*corepb.Key{{Address: voter.Bytes(), Weight: 1}}}, nil, nil)
		if tc.votes {
			s.SetVotes(voter, []*corepb.Vote{{VoteAddress: witness.Bytes(), VoteCount: 100}})
		}
	}
	if err := s.WriteBeginCycle(voter.Bytes(), tc.begin); err != nil {
		tb.Fatal(err)
	}
	if err := s.WriteEndCycle(voter.Bytes(), tc.end); err != nil {
		tb.Fatal(err)
	}
	for cycle := int64(0); cycle <= tc.current; cycle++ {
		if err := s.WriteWitnessVI(cycle, witness.Bytes(), new(big.Int).Mul(big.NewInt(cycle), reward.DecimalOfViReward)); err != nil {
			tb.Fatal(err)
		}
		if err := s.WriteCycleReward(cycle, witness.Bytes(), 1000); err != nil {
			tb.Fatal(err)
		}
		if err := s.WriteCycleVote(cycle, witness.Bytes(), 1000); err != nil {
			tb.Fatal(err)
		}
	}
	if tc.snapshot {
		snapshot := &corepb.Account{Address: voter.Bytes(), Allowance: 7}
		if tc.snapshotVotes {
			snapshot.Votes = []*corepb.Vote{{VoteAddress: witness.Bytes(), VoteCount: 40}}
		}
		data, err := proto.Marshal(snapshot)
		if err != nil {
			tb.Fatal(err)
		}
		if err := s.WriteCycleAccountVote(tc.begin, voter.Bytes(), data); err != nil {
			tb.Fatal(err)
		}
	}
	if _, err := s.Commit(); err != nil {
		tb.Fatal(err)
	}
	buffer := blockbuffer.New(disk)
	buffer.SetBaseReadCacheSize(1 << 20)
	for layer := 0; layer < layers; layer++ {
		buffer.BeginBlock(common.Hash{byte(layer + 1)}, uint64(layer+1))
		for n := 0; n < 64; n++ {
			owner := makeTestAddr(byte(n + 1))
			key := []byte(fmt.Sprintf("overlay-%02d", layer))
			var value [8]byte
			binary.BigEndian.PutUint64(value[:], uint64(layer+n+1))
			if err := rawdb.WriteStateKVLatest(buffer, owner, 0, kvdomains.AccountAsset, key, value[:]); err != nil {
				tb.Fatal(err)
			}
		}
		buffer.CommitBlock()
	}
	wrapped := &rewardLayerDatabase{Database: disk, overlay: buffer}
	db := state.NewDatabaseWithConfig(wrapped, state.DatabaseConfig{})
	tb.Cleanup(func() { _ = db.Close() })
	dp := state.NewDynamicProperties()
	dp.SetChangeDelegation(true)
	dp.SetCurrentCycleNumber(tc.current)
	if tc.legacy {
		dp.SetNewRewardAlgorithmEffectiveCycle(100)
	} else {
		dp.SetNewRewardAlgorithmEffectiveCycle(0)
	}
	return db, dp, voter, wrapped
}

func newRewardSelectiveState(tb testing.TB, db *state.Database) *state.StateDB {
	tb.Helper()
	s, err := state.New(common.Hash{}, db)
	if err != nil {
		tb.Fatal(err)
	}
	return s
}

func TestRewardSelectiveReadsMatchEagerReference(t *testing.T) {
	for _, tc := range rewardSelectiveCases {
		t.Run(tc.name, func(t *testing.T) {
			db, dp, voter, _ := newRewardSelectiveFixture(t, tc, 3)
			for _, mode := range []string{"cold", "warm", "dirty_revert_copy", "new_generation"} {
				t.Run(mode, func(t *testing.T) {
					makeView := func() *state.StateDB {
						s := newRewardSelectiveState(t, db)
						if tc.missing {
							return s
						}
						switch mode {
						case "warm":
							_ = s.GetAccount(voter)
						case "dirty_revert_copy":
							_ = s.GetVotes(voter)
							snap := s.Snapshot()
							s.SetVotes(voter, []*corepb.Vote{{VoteAddress: makeTestAddr(0x30).Bytes(), VoteCount: 700}})
							s.SetAllowance(voter, 999)
							s.RevertToSnapshot(snap)
							s.SetAllowance(voter, 75)
							s.SetTRC10Balance(voter, 1000001, 98765)
							var err error
							s, err = s.Copy()
							if err != nil {
								t.Fatal(err)
							}
						case "new_generation":
							if err := s.ResetAccountKV(voter); err != nil {
								t.Fatal(err)
							}
							if tc.votes {
								s.SetVotes(voter, []*corepb.Vote{{VoteAddress: makeTestAddr(0x30).Bytes(), VoteCount: 25}})
							}
						}
						return s
					}
					want, got := makeView(), makeView()
					wantQuery := queryRewardBeforeSelectiveAccountRead(nil, want, dp, voter)
					gotQuery := queryReward(nil, got, dp, voter)
					if gotQuery != wantQuery {
						t.Fatalf("query = %d, want %d", gotQuery, wantQuery)
					}
					// Reopen the same views so query hydration cannot mask withdrawal differences.
					want, got = makeView(), makeView()
					withdrawRewardBeforeSelectiveAccountRead(nil, want, dp, voter)
					withdrawReward(nil, got, dp, voter)
					if want.Error() != nil || got.Error() != nil {
						t.Fatalf("state errors: got %v, reference %v", got.Error(), want.Error())
					}
					if !bytes.Equal(marshalAccountVote(got.GetAccount(voter)), marshalAccountVote(want.GetAccount(voter))) {
						t.Fatal("complete post-settlement account differs")
					}
					if got.ReadBeginCycle(voter.Bytes()) != want.ReadBeginCycle(voter.Bytes()) || got.ReadEndCycle(voter.Bytes()) != want.ReadEndCycle(voter.Bytes()) {
						t.Fatal("cycle cursors differ")
					}
					for _, cycle := range []int64{tc.begin, tc.current} {
						// The API reader decodes native storage and uses proto.Marshal,
						// whose map order is unspecified. Compare the actual stored bytes.
						key := rawdb.CycleAccountVoteStateKey(cycle, voter.Bytes())
						gotRaw, gotExists, gotErr := got.GetAccountKV(common.SystemAccountAddress, kvdomains.SystemReward, key)
						wantRaw, wantExists, wantErr := want.GetAccountKV(common.SystemAccountAddress, kvdomains.SystemReward, key)
						if gotErr != nil || wantErr != nil || gotExists != wantExists || !bytes.Equal(gotRaw, wantRaw) {
							t.Fatalf("complete historical storage bytes differ at cycle %d: got exists=%v err=%v want exists=%v err=%v", cycle, gotExists, gotErr, wantExists, wantErr)
						}
					}
				})
			}
		})
	}
}

func TestRewardSelectiveReadDependenciesAndErrors(t *testing.T) {
	tc := rewardSelectiveCases[0]
	db, dp, voter, wrapped := newRewardSelectiveFixture(t, tc, 3)
	for _, warm := range []bool{false, true} {
		s := newRewardSelectiveState(t, db)
		if warm {
			_ = s.GetVotes(voter)
		}
		var recorder state.TransactionAccessRecorder
		recorder.Reset(32)
		s.SetTransactionAccessRecorder(&recorder)
		wrapped.prefixReads = 0
		if got := queryReward(nil, s, dp, voter); got != 950 {
			t.Fatalf("query = %d, want 950", got)
		}
		if wrapped.prefixReads != 0 {
			t.Fatalf("query opened %d unrelated prefix iterators", wrapped.prefixReads)
		}
		reads := recorder.CaptureReadSet()
		account, generation, votes := false, false, false
		for _, read := range reads.Reads {
			if read.Key.Address != voter {
				continue
			}
			account = account || read.Key.Kind == state.TransactionAccessAccount
			generation = generation || read.Key.Kind == state.TransactionAccessAccountKVGeneration
			votes = votes || read.Key.Kind == state.TransactionAccessAccountKV && read.Key.KVDomain == kvdomains.AccountVotesAux
		}
		// Both cold and cached votes retain the account barrier, physical rows,
		// and namespace generation across transaction boundaries.
		if !account || !generation || !votes {
			t.Fatalf("missing vote dependency: warm=%v reads=%+v", warm, reads)
		}
	}
	for _, run := range []struct {
		name string
		call func(*state.StateDB)
	}{
		{"query", func(s *state.StateDB) { _ = queryReward(nil, s, dp, voter) }},
		{"withdraw", func(s *state.StateDB) { withdrawReward(nil, s, dp, voter) }},
	} {
		t.Run(run.name+"_corrupt_vote", func(t *testing.T) {
			s := newRewardSelectiveState(t, db)
			if err := s.SetAccountKV(voter, kvdomains.AccountVotesAux, []byte{0, 0, 0, 0}, []byte{0xff}); err != nil {
				t.Fatal(err)
			}
			run.call(s)
			if s.Error() == nil {
				t.Fatal("corrupt vote did not fail closed")
			}
			if s.GetAllowance(voter) != 50 || s.ReadBeginCycle(voter.Bytes()) != tc.begin || s.ReadEndCycle(voter.Bytes()) != tc.end {
				t.Fatal("corrupt vote changed allowance or cursors")
			}
		})
	}
}

var rewardSelectiveBenchmarkSink int64

func BenchmarkRewardSelectiveAccountReads(b *testing.B) {
	for _, scenario := range []string{"query_votes", "withdraw_votes", "withdraw_no_votes", "withdraw_future", "withdraw_current_snapshot"} {
		b.Run(scenario, func(b *testing.B) {
			tc := rewardSelectiveCases[0]
			switch scenario {
			case "withdraw_no_votes":
				tc = rewardSelectiveCases[4]
			case "withdraw_future":
				tc = rewardSelectiveCases[5]
			case "withdraw_current_snapshot":
				tc = rewardSelectiveCases[6]
			}
			db, dp, voter, _ := newRewardSelectiveFixture(b, tc, 8)
			for _, warm := range []bool{false, true} {
				temperature := "cold"
				if warm {
					temperature = "warm"
				}
				b.Run(temperature, func(b *testing.B) {
					for _, before := range []bool{true, false} {
						implementation := "after"
						if before {
							implementation = "before"
						}
						b.Run(implementation, func(b *testing.B) {
							b.ReportAllocs()
							for i := 0; i < b.N; i++ {
								b.StopTimer()
								s := newRewardSelectiveState(b, db)
								if warm {
									_ = s.GetAccount(voter)
								}
								b.StartTimer()
								if scenario == "query_votes" {
									if before {
										rewardSelectiveBenchmarkSink = queryRewardBeforeSelectiveAccountRead(nil, s, dp, voter)
									} else {
										rewardSelectiveBenchmarkSink = queryReward(nil, s, dp, voter)
									}
								} else {
									if before {
										withdrawRewardBeforeSelectiveAccountRead(nil, s, dp, voter)
									} else {
										withdrawReward(nil, s, dp, voter)
									}
								}
							}
						})
					}
				})
			}
		})
	}
}

func TestWithdrawRewardSelectiveEarlyPathsAvoidPrefixReads(t *testing.T) {
	for _, index := range []int{2, 4, 5, 6} {
		tc := rewardSelectiveCases[index]
		t.Run(tc.name, func(t *testing.T) {
			db, dp, voter, wrapped := newRewardSelectiveFixture(t, tc, 3)
			s := newRewardSelectiveState(t, db)
			withdrawReward(nil, s, dp, voter)
			if s.Error() != nil {
				t.Fatal(s.Error())
			}
			if wrapped.prefixReads != 0 {
				t.Fatalf("early or no-vote path opened %d unrelated prefix iterators", wrapped.prefixReads)
			}
		})
	}
}
