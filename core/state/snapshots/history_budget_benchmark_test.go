package snapshots

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"testing"
	"time"

	"github.com/tronprotocol/go-tron/core/rawdb"
)

const (
	historyBudgetBenchmarkLeaves         = 256
	historyBudgetBenchmarkRecordsPerLeaf = 64
	historyBudgetBenchmarkPrevBytes      = 4096
)

type historyBudgetBenchmarkRound struct {
	Sources             uint64 `json:"sources"`
	Records             uint64 `json:"records"`
	InputLogicalBytes   uint64 `json:"input_logical_bytes"`
	InputPhysicalBytes  uint64 `json:"input_physical_bytes"`
	OutputLogicalBytes  uint64 `json:"output_logical_bytes"`
	OutputPhysicalBytes uint64 `json:"output_physical_bytes"`
	WallNanos           int64  `json:"wall_ns"`
	AllocatedBytes      uint64 `json:"allocated_bytes"`
	Mallocs             uint64 `json:"mallocs"`
}

type historyBudgetBenchmarkReport struct {
	Mode                     string                        `json:"mode"`
	GoVersion                string                        `json:"go_version"`
	GOOS                     string                        `json:"goos"`
	GOARCH                   string                        `json:"goarch"`
	GOMAXPROCS               int                           `json:"gomaxprocs"`
	InputLeaves              uint64                        `json:"input_leaves"`
	RecordsPerLeaf           uint64                        `json:"records_per_leaf"`
	PrevBytesPerRecord       uint64                        `json:"prev_bytes_per_record"`
	FixtureLogicalBytes      uint64                        `json:"fixture_logical_bytes"`
	FixturePhysicalBytes     uint64                        `json:"fixture_physical_bytes"`
	FinalLogicalBytes        uint64                        `json:"final_logical_bytes"`
	FinalPhysicalBytes       uint64                        `json:"final_physical_bytes"`
	FinalHistoryFiles        uint64                        `json:"final_history_files"`
	RewrittenInputSources    uint64                        `json:"rewritten_input_sources"`
	RewrittenInputRecords    uint64                        `json:"rewritten_input_records"`
	RewrittenLogicalBytes    uint64                        `json:"rewritten_logical_bytes"`
	RewrittenPhysicalBytes   uint64                        `json:"rewritten_physical_bytes"`
	TotalOutputLogicalBytes  uint64                        `json:"total_output_logical_bytes"`
	TotalOutputPhysicalBytes uint64                        `json:"total_output_physical_bytes"`
	TotalMergeWallNanos      int64                         `json:"total_merge_wall_ns"`
	CampaignWallNanos        int64                         `json:"campaign_wall_ns"`
	MaxRoundWallNanos        int64                         `json:"max_round_wall_ns"`
	MaxRoundAllocatedBytes   uint64                        `json:"max_round_allocated_bytes"`
	TotalAllocatedBytes      uint64                        `json:"total_allocated_bytes"`
	TotalMallocs             uint64                        `json:"total_mallocs"`
	VerifiedRecords          uint64                        `json:"verified_records"`
	VerifiedBoundaryQueries  uint64                        `json:"verified_boundary_queries"`
	CanonicalDigest          string                        `json:"canonical_digest"`
	SequenceValidation       string                        `json:"sequence_validation"`
	Budget                   CompactionConfig              `json:"budget"`
	Rounds                   []historyBudgetBenchmarkRound `json:"rounds"`
}

// BenchmarkHistoryCompactionBusyBudget compares complete coverage, not one
// small pass with one large pass. Both modes consume the same 256 immutable
// compressed leaves. The legacy selector makes one 256-leaf output; the busy
// selector makes sixteen 16-leaf outputs and leaves those outputs immutable.
//
// Run with -benchtime=1x. Fixture creation, verification and per-pass accounting
// are excluded from benchmark time/allocations. No scheduler sleeps or importer
// run here. Hard-linked inputs share warm filesystem caches. Consequently this
// bounds work size for a synthetic 64 MiB Prev workload; it is neither a 20M
// mainnet comparison nor proof that production imports suffer zero interference.
func BenchmarkHistoryCompactionBusyBudget(b *testing.B) {
	fixture := b.TempDir()
	var refs []SegmentRef
	for leaf := uint64(0); leaf < historyBudgetBenchmarkLeaves; leaf++ {
		from := leaf*historyBudgetBenchmarkRecordsPerLeaf + 1
		to := from + historyBudgetBenchmarkRecordsPerLeaf - 1
		changes := make([]*rawdb.StateDomainChange, 0, historyBudgetBenchmarkRecordsPerLeaf)
		for tx := from; tx <= to; tx++ {
			changes = append(changes, historyBudgetBenchmarkChange(tx))
		}
		refs = append(refs, benchmarkWriteCompressedStateDomainHistorySegment(b, fixture, from, to, changes)...)
	}
	const totalRecords = historyBudgetBenchmarkLeaves * historyBudgetBenchmarkRecordsPerLeaf
	if err := PublishManifest(fixture, NewManifest(1, totalRecords, refs)); err != nil {
		b.Fatal(err)
	}
	manifest, err := LoadProductionManifest(fixture)
	if err != nil {
		b.Fatal(err)
	}
	domain, _ := DefaultDomainRegistry().Dataset(SegmentDatasetStateDomainChange)
	sources := historyCompactionCandidates(manifest, domain)
	costs := make(map[string]historyCompactionInputCost, len(sources))
	var fixtureCost historyCompactionInputCost
	for _, source := range sources {
		cost, err := readHistoryCompactionInputCost(context.Background(), fixture, source)
		if err != nil {
			b.Fatal(err)
		}
		costs[source.history.Path] = cost
		fixtureCost.bytes += cost.bytes
		fixtureCost.logicalBytes += cost.logicalBytes
		fixtureCost.records += cost.records
	}
	if len(sources) != historyBudgetBenchmarkLeaves || fixtureCost.records != totalRecords {
		b.Fatalf("fixture coverage: sources=%d cost=%+v", len(sources), fixtureCost)
	}
	wantDigest, _, _ := verifyHistoryBudgetBenchmark(b, fixture)

	for _, mode := range []struct {
		name string
		cfg  CompactionConfig
	}{
		{"legacy_256", CompactionConfig{MaxSteps: 256, MinSteps: 256, DeleteObsolete: true}},
		{"busy_16", CompactionConfig{
			MaxSteps: 256, DeleteObsolete: true, BusyLeafOnly: true,
			MaxSources: busyHistoryCompactionSources, MaxInputBytes: busyHistoryCompactionInputBytes,
			MaxInputLogicalBytes: busyHistoryCompactionLogicalBytes, MaxInputRecords: busyHistoryCompactionInputRecords,
		}},
	} {
		b.Run(mode.name, func(b *testing.B) {
			b.StopTimer()
			b.ReportAllocs()
			for iteration := 0; iteration < b.N; iteration++ {
				dir := b.TempDir()
				for _, ref := range refs {
					path := filepath.Join(dir, ref.Path)
					if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
						b.Fatal(err)
					}
					if err := os.Link(filepath.Join(fixture, ref.Path), path); err != nil {
						b.Fatal(err)
					}
				}
				if err := PublishManifest(dir, NewManifest(1, totalRecords, refs)); err != nil {
					b.Fatal(err)
				}
				report := historyBudgetBenchmarkReport{
					Mode: mode.name, GoVersion: runtime.Version(), GOOS: runtime.GOOS, GOARCH: runtime.GOARCH, GOMAXPROCS: runtime.GOMAXPROCS(0),
					InputLeaves: historyBudgetBenchmarkLeaves, RecordsPerLeaf: historyBudgetBenchmarkRecordsPerLeaf,
					PrevBytesPerRecord: historyBudgetBenchmarkPrevBytes, FixtureLogicalBytes: fixtureCost.logicalBytes,
					FixturePhysicalBytes: fixtureCost.bytes, Budget: mode.cfg,
					SequenceValidation: "hydrated segment-local Seq checked against manifest; normalized to 1 in semantic digest",
				}
				rewrites := make(map[string]int, len(sources))
				campaignStart := time.Now()
				for pass := 0; ; pass++ {
					if pass > historyBudgetBenchmarkLeaves {
						b.Fatal("compaction did not terminate")
					}
					var before, after runtime.MemStats
					runtime.ReadMemStats(&before)
					b.StartTimer()
					started := time.Now()
					result, err := CompactHistoryDomain(dir, SegmentDatasetStateDomainChange, mode.cfg)
					elapsed := time.Since(started)
					b.StopTimer()
					runtime.ReadMemStats(&after)
					if err != nil {
						b.Fatal(err)
					}
					if !result.Merged {
						break
					}
					round := historyBudgetBenchmarkRound{WallNanos: elapsed.Nanoseconds(), AllocatedBytes: after.TotalAlloc - before.TotalAlloc, Mallocs: after.Mallocs - before.Mallocs}
					for _, source := range sources {
						if source.history.FromTxNum < result.FromTxNum || source.history.ToTxNum > result.ToTxNum {
							continue
						}
						cost := costs[source.history.Path]
						round.Sources++
						round.Records += cost.records
						round.InputLogicalBytes += cost.logicalBytes
						round.InputPhysicalBytes += cost.bytes
						rewrites[source.history.Path]++
					}
					if result.InputSources != round.Sources || mode.cfg.BusyLeafOnly && (result.InputBytes != round.InputPhysicalBytes || result.InputLogicalBytes != round.InputLogicalBytes || result.InputRecords != round.Records) {
						b.Fatalf("admission accounting differs: result=%+v measured=%+v", result, round)
					}
					for _, ref := range result.Segments {
						round.OutputPhysicalBytes += ref.Size
						if ref.Kind == SegmentHistory {
							logical, err := readHistoryCompactionLogicalBytes(context.Background(), dir, ref)
							if err != nil {
								b.Fatal(err)
							}
							round.OutputLogicalBytes += logical
						}
					}
					report.Rounds = append(report.Rounds, round)
					report.TotalMergeWallNanos += round.WallNanos
					report.MaxRoundWallNanos = max(report.MaxRoundWallNanos, round.WallNanos)
					report.MaxRoundAllocatedBytes = max(report.MaxRoundAllocatedBytes, round.AllocatedBytes)
					report.TotalAllocatedBytes += round.AllocatedBytes
					report.TotalMallocs += round.Mallocs
					report.RewrittenInputSources += round.Sources
					report.RewrittenInputRecords += round.Records
					report.RewrittenLogicalBytes += round.InputLogicalBytes
					report.RewrittenPhysicalBytes += round.InputPhysicalBytes
					report.TotalOutputLogicalBytes += round.OutputLogicalBytes
					report.TotalOutputPhysicalBytes += round.OutputPhysicalBytes
				}
				report.CampaignWallNanos = time.Since(campaignStart).Nanoseconds()
				for _, source := range sources {
					if rewrites[source.history.Path] != 1 {
						b.Fatalf("leaf %s rewritten %d times, want once", source.history.Path, rewrites[source.history.Path])
					}
				}
				final, err := LoadProductionManifest(dir)
				if err != nil {
					b.Fatal(err)
				}
				for _, ref := range final.Segments {
					report.FinalPhysicalBytes += ref.Size
					if ref.Kind == SegmentHistory {
						report.FinalHistoryFiles++
						logical, err := readHistoryCompactionLogicalBytes(context.Background(), dir, ref)
						if err != nil {
							b.Fatal(err)
						}
						report.FinalLogicalBytes += logical
					}
				}
				report.CanonicalDigest, report.VerifiedRecords, report.VerifiedBoundaryQueries = verifyHistoryBudgetBenchmark(b, dir)
				if report.CanonicalDigest != wantDigest || report.RewrittenInputSources != historyBudgetBenchmarkLeaves || report.RewrittenInputRecords != totalRecords || report.RewrittenLogicalBytes != fixtureCost.logicalBytes || report.RewrittenPhysicalBytes != fixtureCost.bytes {
					b.Fatalf("unequal complete coverage: %+v, want digest %s", report, wantDigest)
				}
				b.ReportMetric(float64(report.MaxRoundWallNanos), "max-round-ns")
				b.ReportMetric(float64(report.MaxRoundAllocatedBytes), "max-round-alloc-B")
				b.ReportMetric(float64(len(report.Rounds)), "merge-rounds")
				b.ReportMetric(float64(report.FinalPhysicalBytes), "final-physical-B")
				encoded, err := json.Marshal(report)
				if err != nil {
					b.Fatal(err)
				}
				b.Logf("HISTORY_BUDGET_RESULT %s", encoded)
			}
		})
	}
}

func historyBudgetBenchmarkChange(tx uint64) *rawdb.StateDomainChange {
	change := binaryStateDomainChange(tx, tx, 1, fmt.Sprintf("budget-key-%08d", tx))
	change.Owner = binaryAddress(0x29)
	change.Generation = 0
	change.Prev = bytes.Repeat([]byte{byte(tx % 16)}, historyBudgetBenchmarkPrevBytes)
	binary.BigEndian.PutUint64(change.Prev[:8], tx)
	binary.BigEndian.PutUint64(change.Prev[len(change.Prev)-8:], tx^0xd1b54a32d192ed03)
	change.Next, change.NextExists = nil, false
	return change
}

func verifyHistoryBudgetBenchmark(b *testing.B, dir string) (string, uint64, uint64) {
	b.Helper()
	manager, err := OpenManager(dir)
	if err != nil {
		b.Fatal(err)
	}
	const totalRecords = historyBudgetBenchmarkLeaves * historyBudgetBenchmarkRecordsPerLeaf
	manifest, err := LoadProductionManifest(dir)
	if err != nil {
		b.Fatal(err)
	}
	sequences := make(map[uint64]uint64, totalRecords)
	for _, ref := range manifest.Segments {
		if ref.Dataset == SegmentDatasetStateDomainChange && ref.Kind == SegmentHistory {
			for tx := ref.FromTxNum; tx <= ref.ToTxNum; tx++ {
				if _, exists := sequences[tx]; exists {
					b.Fatalf("overlapping history coverage at tx %d", tx)
				}
				sequences[tx] = tx - ref.FromTxNum + 1
			}
		}
	}
	// V5+ deliberately reconstructs Seq from the segment-local record ordinal;
	// merging changes that ordinal. Validate it separately before comparing all
	// semantic fields. This fixture has one mutation per block/transaction.
	check := func(change, want *rawdb.StateDomainChange) error {
		if change.Seq != sequences[want.TxNum] {
			return fmt.Errorf("hydrated sequence at tx %d: got %d want %d", want.TxNum, change.Seq, sequences[want.TxNum])
		}
		change.Seq = want.Seq
		if !reflect.DeepEqual(change, want) {
			return fmt.Errorf("canonical record mismatch at tx %d", want.TxNum)
		}
		return nil
	}
	var records, queries uint64
	digest := sha256.New()
	encoder := json.NewEncoder(digest)
	if err := manager.IterateStateDomainChanges(1, totalRecords, func(change *rawdb.StateDomainChange) (bool, error) {
		records++
		want := historyBudgetBenchmarkChange(records)
		if err := check(change, want); err != nil {
			return false, err
		}
		return true, encoder.Encode(change)
	}); err != nil {
		b.Fatal(err)
	}
	if records != totalRecords {
		b.Fatalf("history returned %d records, want %d", records, totalRecords)
	}
	for leaf := uint64(0); leaf < historyBudgetBenchmarkLeaves; leaf++ {
		for _, tx := range []uint64{leaf*historyBudgetBenchmarkRecordsPerLeaf + 1, (leaf + 1) * historyBudgetBenchmarkRecordsPerLeaf} {
			want := historyBudgetBenchmarkChange(tx)
			var matches uint64
			// Query across the original leaf boundary, exercising both merged
			// accessors and remaining files rather than only a whole-range scan.
			from, to := max(uint64(1), tx-1), min(uint64(totalRecords), tx+1)
			if err := manager.IterateStateDomainChangesByKey(from, to, want.FlatDomain, want.Owner, want.Generation, want.Domain, want.Key, func(change *rawdb.StateDomainChange) (bool, error) {
				if err := check(change, want); err != nil {
					return false, err
				}
				matches++
				return true, nil
			}); err != nil {
				b.Fatal(err)
			}
			if matches != 1 {
				b.Fatalf("boundary key tx %d returned %d matches", tx, matches)
			}
			queries++
		}
	}
	return hex.EncodeToString(digest.Sum(nil)), records, queries
}
