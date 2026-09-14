package snapshots

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"testing"

	"github.com/tronprotocol/go-tron/core/rawdb"
)

var (
	frontierBenchmarkBlock uint64
	frontierBenchmarkOK    bool
	frontierBenchmarkRange coldSidecarBlockRange
)

// The large fixture matches generation 35095's family counts, not its exact
// ranges or serialized bytes. Retired rows remain part of the input object's
// lifetime but are ignored by every timed operation. Metadata setup and JSON
// decoding are excluded; each measured call owns its temporary allocations.
func frontierBenchmarkManifest(eventCount, historyCount, retiredCount int, head uint64) *Manifest {
	m := &Manifest{Version: CurrentManifestVersion, Generation: 35095}
	m.Segments = make([]SegmentRef, 0, 2*eventCount+3*historyCount)
	for i := 0; i < eventCount; i++ {
		from, to := uint64(i)*head/uint64(eventCount)+1, uint64(i+1)*head/uint64(eventCount)
		m.Segments = append(m.Segments, frontierTestPair(from, to)...)
	}
	for i := 0; i < historyCount; i++ {
		for _, kind := range []SegmentKind{SegmentHistory, SegmentAccessor, SegmentInverted} {
			m.Segments = append(m.Segments, SegmentRef{
				Dataset: SegmentDatasetStateDomainChange, Kind: kind,
				FromTxNum: uint64(i)*65536 + 1, ToTxNum: uint64(i+1) * 65536,
				Path: fmt.Sprintf("state-domain-change-%s-%09d.seg", kind, i), Size: 4096,
				Checksum: "0000000000000000000000000000000000000000000000000000000000000000",
			})
		}
	}
	m.Retired = make([]SegmentRef, retiredCount)
	for i := range m.Retired {
		m.Retired[i] = frontierTestRef(SegmentEventLog, 1, ^uint64(0))
	}
	sortSegments(m.Segments)
	return m
}

func runEventFrontierBenchmarks(b *testing.B, catalogs []struct {
	name string
	m    *Manifest
}) {
	for _, operation := range []string{"Frontier", "PlannerCovered", "PlannerGap", "StageWrite"} {
		b.Run(operation, func(b *testing.B) {
			for _, catalog := range catalogs {
				b.Run(catalog.name, func(b *testing.B) {
					m := catalog.m
					head, ok := frontier833EventLogBuildBlockFromManifest(m)
					if !ok || head == ^uint64(0) {
						b.Fatal("benchmark needs finite positive continuous coverage")
					}
					if operation == "PlannerGap" {
						// Make the next event companion overlap the first uncovered
						// index block, exercising the unchanged full-ref repair scan.
						m = cloneManifest(m)
						for i := range m.Segments {
							ref := &m.Segments[i]
							if ref.Kind == SegmentEventLogIndex && ref.normalizedDataset() == SegmentDatasetEventLog && ref.ToTxNum == head {
								ref.FromTxNum++
							}
						}
					}
					for _, variant := range []string{"Frozen833", "Candidate"} {
						b.Run(variant, func(b *testing.B) {
							frontier := frontier833EventLogBuildBlockFromManifest
							planner := frontier833NextEventLogCatchupRange
							stage := frontier833WriteEventLogBuildStage
							if variant == "Candidate" {
								frontier = eventLogBuildBlockFromManifest
								planner = nextEventLogCatchupRange
								stage = writeEventLogBuildStage
							}
							db := rawdb.NewMemoryDatabase()
							defer db.Close()
							if err := rawdb.WriteBlock(db, aggregatorTestBlock(head)); err != nil {
								b.Fatal(err)
							}
							want, wantOK := frontier833EventLogBuildBlockFromManifest(m)
							got, gotOK := frontier(m)
							if got != want || gotOK != wantOK {
								b.Fatal("frontier oracle mismatch")
							}
							wantRange, wantRangeOK := frontier833NextEventLogCatchupRange(m, head, 256)
							gotRange, gotRangeOK := planner(m, head, 256)
							if gotRange != wantRange || gotRangeOK != wantRangeOK {
								b.Fatal("planner oracle mismatch")
							}
							if operation == "PlannerCovered" && wantRangeOK {
								b.Fatal("covered fixture unexpectedly has work")
							}
							if operation == "PlannerGap" && !wantRangeOK {
								b.Fatal("gap fixture has no work")
							}
							b.ReportAllocs()
							b.ResetTimer()
							for i := 0; i < b.N; i++ {
								switch operation {
								case "Frontier":
									frontierBenchmarkBlock, frontierBenchmarkOK = frontier(m)
								case "PlannerCovered", "PlannerGap":
									frontierBenchmarkRange, frontierBenchmarkOK = planner(m, head, 256)
								case "StageWrite":
									if err := stage(db, m); err != nil {
										b.Fatal(err)
									}
								}
							}
							b.StopTimer()
						})
					}
				})
			}
		})
	}
}

func BenchmarkEventFrontierAllocation(b *testing.B) {
	runEventFrontierBenchmarks(b, []struct {
		name string
		m    *Manifest
	}{
		{"Small", frontierBenchmarkManifest(8, 2, 12, 800)},
		{"Mixed73113", frontierBenchmarkManifest(32229, 2885, 96630, 30730803)},
	})
}

// Opt-in metadata replay. Read one explicitly supplied file and verify its
// caller-pinned SHA before JSON decoding. No segment files or live DB are read.
func BenchmarkEventFrontierAllocationRealManifest(b *testing.B) {
	path, wantSHA := os.Getenv("GTRON_FRONTIER_MANIFEST"), os.Getenv("GTRON_FRONTIER_MANIFEST_SHA256")
	if path == "" {
		b.Skip("set GTRON_FRONTIER_MANIFEST and GTRON_FRONTIER_MANIFEST_SHA256")
	}
	if len(wantSHA) != 64 {
		b.Fatal("require exact 64-character manifest SHA256")
	}
	data, err := os.ReadFile(path)
	if err != nil {
		b.Fatal(err)
	}
	digest := sha256.Sum256(data)
	if hex.EncodeToString(digest[:]) != wantSHA {
		b.Fatal("manifest SHA256 mismatch")
	}
	var m Manifest
	if err := json.Unmarshal(data, &m); err != nil {
		b.Fatal(err)
	}
	if err := m.Validate(); err != nil {
		b.Fatal(err)
	}
	b.Logf("manifest sha256=%s bytes=%d generation=%d active=%d retired=%d", wantSHA, len(data), m.Generation, len(m.Segments), len(m.Retired))
	data = nil
	runEventFrontierBenchmarks(b, []struct {
		name string
		m    *Manifest
	}{{"RealManifest", &m}})
}
