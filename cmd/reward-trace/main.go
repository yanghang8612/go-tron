// reward-trace dumps the per-cycle voter-reward store (cycleReward, cycleVote,
// witnessVI) for a set of witnesses, plus a voter's per-cycle account-vote
// snapshots, and replays ComputeVoterReward over explicit cycle ranges. It is a
// one-shot diagnostic for "why did this voter's withdraw_amount diverge from
// java-tron by N SUN?".
//
// The reward store (cycleReward / cycleVote / witnessVI) is append-only — each
// cycle's value is written once at maintenance and never overwritten — so the
// values for historical cycles are still readable from the HEAD state. The
// voter's account-vote snapshots (WriteCycleAccountVote at each settlement)
// reconstruct the settlement-period boundaries and the vote config per period
// WITHOUT needing historical flat state.
//
// First user: Nile block 21,210,788 stall — owner TMChEDeYhvxY3sAqjYRYpPviHA5mwUDVze
// withdrew 27 SUN less than java at the WithdrawBalance in block 19,800,778,
// across a transient 1→2→1-witness vote change (w2 413b173c…eeb63202 voted 10k
// only in [19,773,986, 19,792,240]).
//
// Usage:
//
//	reward-trace --datadir=/path/to/gtron \
//	  --witnesses=417e1cbcf4e01ab995bf554e80ff553c690721c5cb,413b173c6048a84b0bd1f2ef13a0bcb7b4eeb63202 \
//	  --owner=417b365a8f5656136efbb12680d91244835ce64315 \
//	  --from-cycle=96240 --to-cycle=96360 \
//	  --replay=96320:96352:120000:10000   (begin:end:w1count:w2count, repeatable via comma)
package main

import (
	"encoding/hex"
	"flag"
	"fmt"
	"math/big"
	"os"
	"path/filepath"
	"sort"
	"strings"

	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/common/log"
	chainfreezer "github.com/tronprotocol/go-tron/core/freezer"
	"github.com/tronprotocol/go-tron/core/rawdb"
	rawdbfreezer "github.com/tronprotocol/go-tron/core/rawdb/freezer"
	"github.com/tronprotocol/go-tron/core/rawdb/pebbledb"
	"github.com/tronprotocol/go-tron/core/reward"
	"github.com/tronprotocol/go-tron/core/state"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	"google.golang.org/protobuf/proto"
)

func mustAddr(h string) tcommon.Address {
	b, err := hex.DecodeString(h)
	if err != nil || len(b) != 21 {
		log.Crit("bad 41-hex address", "addr", h, "err", err)
	}
	return tcommon.BytesToAddress(b)
}

func main() {
	log.SetupCLI()
	datadir := flag.String("datadir", "/tmp/gtron-nile", "gtron datadir (contains gtron/chaindata)")
	witnessesCSV := flag.String("witnesses", "", "comma-separated 41-hex witness addresses to dump")
	ownerHex := flag.String("owner", "", "41-hex voter address to dump snapshots/replay for")
	fromCycle := flag.Int64("from-cycle", 96000, "first cycle to dump (inclusive)")
	toCycle := flag.Int64("to-cycle", 96600, "last cycle to dump (inclusive)")
	replaySpec := flag.String("replay", "", "comma list of begin:end:w1count:w2count ranges to replay ComputeVoterReward over")
	analyzeCycle := flag.Int64("analyze-cycle", -1, "dump ALL witnesses' cycleVote/cycleReward/cycleBrokerage for this cycle and flag per-vote-rate outliers (consistency check, no java needed)")
	dumpVotes := flag.String("dump-votes", "", "comma-separated cycles: dump EVERY witness's cycleVote sorted (vote DESC, hexaddr DESC) — diff-friendly vs the java DelegationStore reference dump")
	flag.Parse()

	var witnesses []tcommon.Address
	for _, h := range strings.Split(*witnessesCSV, ",") {
		h = strings.TrimSpace(h)
		if h != "" {
			witnesses = append(witnesses, mustAddr(h))
		}
	}
	if len(witnesses) == 0 && *analyzeCycle < 0 && *dumpVotes == "" {
		log.Crit("--witnesses is required (or use --analyze-cycle / --dump-votes)")
	}

	dbPath := filepath.Join(*datadir, "gtron", "chaindata")
	if _, err := os.Stat(dbPath); err != nil {
		log.Crit("datadir not accessible", "path", dbPath, "err", err)
	}
	// Read-only open so this can run against a live (or stalled) node's datadir
	// without contending for Pebble's write lock.
	db, err := pebbledb.New(dbPath, 256, 500, "", true, pebbledb.DefaultOptions())
	if err != nil {
		log.Crit("open pebble (read-only)", "err", err)
	}
	defer db.Close()
	var ancient rawdb.AncientReader = rawdb.NoopAncient{}
	ancientPath := filepath.Join(*datadir, "gtron", "ancient")
	if info, err := os.Stat(ancientPath); err == nil && info.IsDir() {
		fz, err := rawdbfreezer.NewFreezer(ancientPath, "", true, rawdbfreezer.FreezerTableSize, chainfreezer.FreezerTableSet())
		if err != nil {
			log.Crit("open ancient", "path", ancientPath, "err", err)
		}
		defer fz.Close()
		ancient = rawdb.NewFreezerReader(fz)
	}
	chaindb := rawdb.NewChainDB(db, ancient)

	headHash := rawdb.ReadHeadBlockHash(db)
	if headHash == (tcommon.Hash{}) {
		log.Crit("no head block", "path", dbPath)
	}
	headRoot := rawdb.ReadBlockStateRoot(chaindb, headHash)
	if headRoot == (tcommon.Hash{}) {
		headRoot = rawdb.ReadGenesisStateRoot(db)
	}
	stateDB := state.NewDatabase(rawdb.WrapKeyValueStore(db))
	statedb, err := state.New(headRoot, stateDB)
	if err != nil {
		log.Crit("open statedb", "root", fmt.Sprintf("%x", headRoot[:]), "err", err)
	}
	dp := state.LoadDynamicProperties(db, statedb)
	if hn := rawdb.ReadBlockNumber(chaindb, headHash); hn != nil {
		fmt.Printf("head block=%d currentCycle=%d newRewardAlgoEffectiveCycle=%d allowOldRewardOpt=%v changeDelegation=%v\n",
			*hn, dp.CurrentCycleNumber(), dp.NewRewardAlgorithmEffectiveCycle(), dp.AllowOldRewardOpt(), dp.ChangeDelegation())
	}

	dec := reward.DecimalOfViReward

	// ---- analyze-cycle: dump ALL witnesses for one cycle (consistency check) ----
	if *analyzeCycle >= 0 {
		c := *analyzeCycle
		type wrow struct {
			hx     string
			vote   int64
			reward int64
			brok   int
		}
		addrs := statedb.ReadWitnessIndex()
		rows := make([]wrow, 0, len(addrs))
		for _, a := range addrs {
			rows = append(rows, wrow{
				hx:     fmt.Sprintf("%x", a.Bytes()),
				vote:   statedb.ReadCycleVote(c, a.Bytes()),
				reward: statedb.ReadCycleReward(c, a.Bytes()),
				brok:   statedb.ReadCycleBrokerage(c, a.Bytes()),
			})
		}
		sort.Slice(rows, func(i, j int) bool { return rows[i].vote > rows[j].vote })
		var voteSum int64
		top := len(rows)
		if top > 127 {
			top = 127
		}
		for i := 0; i < top; i++ {
			if rows[i].vote >= 1 {
				voteSum += rows[i].vote
			}
		}
		w127 := dp.Witness127PayPerBlock()
		wpb := dp.WitnessPayPerBlock()
		fmt.Printf("=== analyze cycle %d ===\n", c)
		fmt.Printf("witnessCount=%d  top127VoteSum=%d  Witness127PayPerBlock=%d  WitnessPayPerBlock=%d\n",
			len(rows), voteSum, w127, wpb)
		if voteSum > 0 {
			fmt.Printf("eachVotePay = %d/%d = %.12f\n", w127, voteSum, float64(w127)/float64(voteSum))
		}
		// rate metric: reward/(vote*(100-brok)) ~ blocks*eachVotePay/100 (CONSTANT for pure standby;
		// higher for active SRs that also earn block reward). w2 should sit in the standby cluster.
		fmt.Printf("%-4s %-44s %14s %16s %5s %22s %16s\n",
			"rank", "witness", "cycleVote", "cycleReward", "brok", "reward/(vote*(100-brok))", "perBlockPay(floor)")
		for i, r := range rows {
			mark := ""
			if strings.HasSuffix(r.hx, "0721c5cb") {
				mark = "  <<< w1"
			}
			if strings.HasSuffix(r.hx, "eeb63202") {
				mark = "  <<< w2"
			}
			rate := 0.0
			if d := r.vote * int64(100-r.brok); d != 0 {
				rate = float64(r.reward) / float64(d)
			}
			perBlock := int64(0)
			if voteSum > 0 {
				perBlock = int64(float64(r.vote) * (float64(w127) / float64(voteSum)))
			}
			if i < 135 || mark != "" {
				fmt.Printf("%-4d %-44s %14d %16d %5d %22.12f %16d%s\n",
					i+1, r.hx, r.vote, r.reward, r.brok, rate, perBlock, mark)
			}
		}
		return
	}

	// ---- dump-votes: every witness's cycleVote for given cycles (diff-friendly) ----
	// Output matches the java DelegationStore reference dump line-for-line:
	//   "<rank> <hexaddr> <vote>[ [TOP]]" then "voteSum(top127)=N nWitness=M".
	// Sort is (vote DESC, hexaddr DESC); voteSum sums the top-127 with vote>=1,
	// exactly like java WitnessStore.getWitnessStandby + MortgageService voteSum.
	if *dumpVotes != "" {
		for _, cs := range strings.Split(*dumpVotes, ",") {
			cs = strings.TrimSpace(cs)
			if cs == "" {
				continue
			}
			var c int64
			if _, err := fmt.Sscanf(cs, "%d", &c); err != nil {
				fmt.Printf("bad cycle %q: %v\n", cs, err)
				continue
			}
			type wv struct {
				hx string
				v  int64
			}
			addrs := statedb.ReadWitnessIndex()
			arr := make([]wv, 0, len(addrs))
			for _, a := range addrs {
				arr = append(arr, wv{fmt.Sprintf("%x", a.Bytes()), statedb.ReadCycleVote(c, a.Bytes())})
			}
			sort.Slice(arr, func(i, j int) bool {
				if arr[i].v != arr[j].v {
					return arr[i].v > arr[j].v
				}
				return arr[i].hx > arr[j].hx
			})
			fmt.Printf("=== dump-votes cycle %d ===\n", c)
			var voteSum int64
			for i, e := range arr {
				inTop := i < 127 && e.v >= 1
				tag := ""
				if inTop {
					voteSum += e.v
					tag = " [TOP]"
				}
				fmt.Printf("%3d %s %d%s\n", i, e.hx, e.v, tag)
			}
			fmt.Printf("voteSum(top127)=%d nWitness=%d\n", voteSum, len(arr))
		}
		return
	}

	// ---- per-witness per-cycle reward store dump ----
	for _, w := range witnesses {
		fmt.Printf("\n=== witness %x  (in witnessIndex=%v) ===\n", w.Bytes(), statedb.IsWitness(w))
		fmt.Printf("%-8s %16s %16s %26s %22s %14s\n", "cycle", "cycleReward", "cycleVote", "storedVI", "VIdelta", "expectVIdelta")
		var prevVI = big.NewInt(0)
		havePrev := false
		for c := *fromCycle; c <= *toCycle; c++ {
			cr := statedb.ReadCycleReward(c, w.Bytes())
			cv := statedb.ReadCycleVote(c, w.Bytes())
			vi := statedb.ReadWitnessVI(c, w.Bytes())
			if vi == nil {
				vi = big.NewInt(0)
			}
			delta := new(big.Int)
			if havePrev {
				delta.Sub(vi, prevVI)
			}
			// expected per-cycle VI delta = floor(cycleReward * 1e18 / cycleVote)
			expect := big.NewInt(0)
			if cr != 0 && cv != 0 && cv != rawdb.RewardRemark {
				expect = new(big.Int).Quo(new(big.Int).Mul(big.NewInt(cr), dec), big.NewInt(cv))
			}
			mark := ""
			if havePrev && delta.Cmp(expect) != 0 {
				mark = "  <<< VIdelta != expect"
			}
			// only print cycles with any activity or near the window
			if cr != 0 || cv != 0 || vi.Sign() != 0 {
				fmt.Printf("%-8d %16d %16d %26s %22s %14s%s\n", c, cr, cv, vi.String(), delta.String(), expect.String(), mark)
			}
			prevVI = vi
			havePrev = true
		}
	}

	// ---- owner per-cycle account-vote snapshots ----
	if *ownerHex != "" {
		owner := mustAddr(*ownerHex)
		fmt.Printf("\n=== owner %x snapshots / cursors ===\n", owner.Bytes())
		fmt.Printf("current: beginCycle=%d endCycle=%d allowance=%d balance=%d\n",
			statedb.ReadBeginCycle(owner.Bytes()), statedb.ReadEndCycle(owner.Bytes()),
			statedb.GetAllowance(owner), statedb.GetBalance(owner))
		fmt.Printf("current votes: %s\n", votesStr(statedb.GetVotes(owner)))
		fmt.Printf("%-8s %s\n", "cycle", "snapshotVotes (ReadCycleAccountVote)")
		for c := *fromCycle; c <= *toCycle; c++ {
			raw := statedb.ReadCycleAccountVote(c, owner.Bytes())
			if len(raw) == 0 {
				continue
			}
			snap := &corepb.Account{}
			if err := proto.Unmarshal(raw, snap); err != nil {
				fmt.Printf("%-8d <decode error: %v>\n", c, err)
				continue
			}
			fmt.Printf("%-8d %s  allowance=%d\n", c, votesStr(snap.Votes), snap.GetAllowance())
		}

		// ---- replay ComputeVoterReward over explicit ranges ----
		if *replaySpec != "" {
			fmt.Printf("\n=== replay ComputeVoterReward ===\n")
			for _, spec := range strings.Split(*replaySpec, ",") {
				var begin, end, c1, c2 int64
				if _, err := fmt.Sscanf(spec, "%d:%d:%d:%d", &begin, &end, &c1, &c2); err != nil {
					fmt.Printf("bad replay spec %q: %v\n", spec, err)
					continue
				}
				var votes []reward.VoteEntry
				if c1 != 0 {
					votes = append(votes, reward.VoteEntry{Witness: witnesses[0], Count: c1})
				}
				if c2 != 0 && len(witnesses) > 1 {
					votes = append(votes, reward.VoteEntry{Witness: witnesses[1], Count: c2})
				}
				total := reward.ComputeVoterReward(statedb, dp, votes, begin, end)
				fmt.Printf("range [%d,%d) votes=%s => reward=%d\n", begin, end, votesEntryStr(votes), total)
				// per-witness breakdown via VI (new-algo path)
				for _, v := range votes {
					bvi := statedb.ReadWitnessVI(begin-1, v.Witness.Bytes())
					evi := statedb.ReadWitnessVI(end-1, v.Witness.Bytes())
					if bvi == nil {
						bvi = big.NewInt(0)
					}
					if evi == nil {
						evi = big.NewInt(0)
					}
					d := new(big.Int).Sub(evi, bvi)
					share := new(big.Int).Quo(new(big.Int).Mul(d, big.NewInt(v.Count)), dec)
					fmt.Printf("    witness %x: VI[%d]-VI[%d]=%s  x%d/1e18 = %s\n",
						v.Witness.Bytes(), end-1, begin-1, d.String(), v.Count, share.String())
				}
				// THREE-WAY path comparison on the SAME data:
				//   A = new-VI (what gtron uses for cycles >= newAlgoCycle) == `total` above
				//   B = old per-cycle FLOAT (oldRewardSum): sum_cycle sum_witness floor(count/cycleVote * cycleReward)
				//   C = old opt TELESCOPING (oldRewardSumOpt): per-witness floor(sum_cycle floor(cr*1e18/cv) * count / 1e18)
				var oldFloat int64
				var optTotal int64
				for _, v := range votes {
					viSum := new(big.Int)
					for c := begin; c < end; c++ {
						cr := statedb.ReadCycleReward(c, v.Witness.Bytes())
						cv := statedb.ReadCycleVote(c, v.Witness.Bytes())
						if cr > 0 && cv != rawdb.RewardRemark && cv != 0 {
							voteRate := float64(v.Count) / float64(cv)
							oldFloat += int64(voteRate * float64(cr))
						}
						if cr != 0 && cv != 0 {
							viSum.Add(viSum, new(big.Int).Quo(new(big.Int).Mul(big.NewInt(cr), dec), big.NewInt(cv)))
						}
					}
					if viSum.Sign() > 0 {
						optTotal += new(big.Int).Quo(new(big.Int).Mul(viSum, big.NewInt(v.Count)), dec).Int64()
					}
				}
				fmt.Printf("    PATHS: [A new-VI]=%d  [B old-per-cycle-float]=%d  [C old-opt-telescoping]=%d\n",
					total, oldFloat, optTotal)
			}
		}
	}
}

func votesStr(vs []*corepb.Vote) string {
	parts := make([]string, 0, len(vs))
	for _, v := range vs {
		parts = append(parts, fmt.Sprintf("%x:%d", v.VoteAddress, v.VoteCount))
	}
	return "[" + strings.Join(parts, " ") + "]"
}

func votesEntryStr(vs []reward.VoteEntry) string {
	parts := make([]string, 0, len(vs))
	for _, v := range vs {
		parts = append(parts, fmt.Sprintf("%x:%d", v.Witness.Bytes(), v.Count))
	}
	return "[" + strings.Join(parts, " ") + "]"
}
