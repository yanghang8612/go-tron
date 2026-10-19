package state

import (
	"encoding/binary"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/state/kvdomains"
	"github.com/tronprotocol/go-tron/core/state/snapshots"
)

// Synthetic random-looking addresses, one append/commit per block. This measures
// actual history files, not a mature-chain storage forecast. Explicit opt-in.
func TestVotesIndexSpaceMeasurement(t *testing.T) {
	out := os.Getenv("GTRON_VOTES_MEASURE_OUT")
	if out == "" {
		t.Skip("explicit new output directory required")
	}
	if err := os.Mkdir(out, 0700); err != nil {
		t.Fatal(err)
	}
	type result struct {
		Paged                                           bool
		Voters                                          int
		LatestBytes, HistoryBytes, PreviousPayloadBytes uint64
		AppendNS, EnumerateNS                           int64
		FullMuHashVerified                              bool
	}
	var results []result
	const count = 2048
	for _, paged := range []bool{false, true} {
		disk := rawdb.NewMemoryDatabase()
		db := NewDatabase(rawdb.WrapKeyValueStore(disk))
		s, err := New(tcommon.Hash{}, db)
		if err != nil {
			t.Fatal(err)
		}
		var voters []tcommon.Address
		var appendNS int64
		var parent tcommon.Hash
		for i := uint64(1); i <= count; i++ {
			var seed [8]byte
			binary.BigEndian.PutUint64(seed[:], i)
			hash := tcommon.Keccak256(seed[:])
			var addr tcommon.Address
			addr[0] = 0x41
			copy(addr[1:], hash[12:])
			voters = append(voters, addr)
			blockHash := tcommon.Keccak256(seed[:])
			s.BeginDomainChangeJournalCapture(disk, i, blockHash, i, i)
			mark := s.DomainChangeJournalMark()
			start := time.Now()
			if paged {
				err = s.AppendVotesIndex(addr)
			} else {
				err = s.SystemKVPut(kvdomains.WitnessVoteState, rawdb.LegacyVotesIndexStateKey(), encodeAddressList(voters))
			}
			appendNS += time.Since(start).Nanoseconds()
			if err != nil {
				t.Fatal(err)
			}
			if err = s.FlushDomainChangesSince(mark, i); err != nil {
				t.Fatal(err)
			}
			_, _, err = s.CommitWithStatsOptions(CommitOptions{BlockNumber: i, BlockHash: blockHash, ParentHash: parent, CommitmentTxNum: i})
			if err != nil {
				t.Fatal(err)
			}
			parent = blockHash
			if err = rawdb.WriteBlockNumber(disk, blockHash, i); err != nil {
				t.Fatal(err)
			}
			if err = rawdb.WriteStateExecutionProperties(disk, rawdb.StateExecutionProperties{BlockNum: int64(i), Timestamp: int64(i * 3000), BlockHash: blockHash}); err != nil {
				t.Fatal(err)
			}
		}
		assertCompactFullMuHash(t, disk)
		var enumerationNS int64
		for repeat := 0; repeat < 100; repeat++ {
			start := time.Now()
			if paged {
				got, _, err := s.ReadVotesIndexStrict()
				if err != nil || len(got) != count {
					t.Fatal("enumeration", err)
				}
			} else {
				raw, _, err := s.SystemKVGet(kvdomains.WitnessVoteState, rawdb.LegacyVotesIndexStateKey())
				if err != nil {
					t.Fatal(err)
				}
				got, err := decodeAddressListStrict("baseline", raw)
				if err != nil || len(got) != count {
					t.Fatal("baseline enumeration", err)
				}
			}
			enumerationNS += time.Since(start).Nanoseconds()
		}
		name := "baseline"
		if paged {
			name = "paged"
		}
		dir := filepath.Join(out, name)
		latest, err := snapshots.NewAggregator(dir).BuildLatest(disk, snapshots.AggregatorBuildOptions{FromTxNum: 1, ToTxNum: count})
		if err != nil {
			t.Fatal(err)
		}
		var latestBytes uint64
		for _, ref := range latest.Manifest.Segments {
			latestBytes += ref.Size
		}
		refs, err := snapshots.BuildStateDomainChangeHistorySegmentsFromDB(disk, dir, 1, count, "history/votes.seg")
		if err != nil {
			t.Fatal(err)
		}
		var historyBytes, previousBytes uint64
		for _, ref := range refs {
			historyBytes += ref.Size
			if ref.Kind == snapshots.SegmentHistory {
				cfg, _ := snapshots.DefaultDomainRegistry().ConfigForRef(ref)
				err = cfg.IterateHistoryRange(dir, nil, ref, 1, count, func(c *rawdb.StateDomainChange) (bool, error) {
					if c.FlatDomain == rawdb.StateFlatDomainKVLatest && c.Domain == kvdomains.WitnessVoteState && c.PrevExists {
						previousBytes += uint64(len(c.Prev))
					}
					return true, nil
				})
				if err != nil {
					t.Fatal(err)
				}
			}
		}
		results = append(results, result{paged, count, latestBytes, historyBytes, previousBytes, appendNS / count, enumerationNS / 100, true})
		db.Close()
		disk.Close()
	}
	b, err := json.MarshalIndent(results, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(filepath.Join(out, "results.json"), b, 0600); err != nil {
		t.Fatal(err)
	}
}
