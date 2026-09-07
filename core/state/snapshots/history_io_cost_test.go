package snapshots

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"
)

type historyIOObservation struct {
	Fixture, Format, InputSHA256                               string
	LogicalBytes, StoredBytes, SpoolWriteBytes, SpoolReadBytes uint64
	FactoryWall, ProcessUserCPU, ProcessSystemCPU              time.Duration
	CPUAvailable                                               bool
	FactoryWorkers                                             int
}

// Observe the actual factory and its final metadata, including file Sync and
// rename. Decoder/hash oracle checks are outside this work measurement. The
// directory spool is written/read once by CDC; final directory/footer bytes
// are already included in StoredBytes. This is application byte accounting,
// not device-sector writes, filesystem metadata traffic or an IOPS measurement.
func observeHistoryIO(t testing.TB, dir, name, format string, input []byte) historyIOObservation {
	t.Helper()
	workers := historyCompressionConcurrency(runtime.GOMAXPROCS(0))
	user0, sys0, cpu0 := historyIOProcessCPU()
	started := time.Now()
	w, err := newHistoryCompressedStreamFormat(context.Background(), dir, historyCompressChunkSize, workers, format)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Abort()
	if _, err = w.Write(input); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "history.seg")
	metadata, err := w.FinishWithMetadataContext(context.Background(), path)
	if err != nil {
		t.Fatal(err)
	}
	wall := time.Since(started)
	user1, sys1, cpu1 := historyIOProcessCPU()
	out := historyIOObservation{Fixture: name, Format: format, LogicalBytes: uint64(len(input)), StoredBytes: metadata.size, FactoryWall: wall, FactoryWorkers: workers}
	if cpu0 && cpu1 && user1 >= user0 && sys1 >= sys0 {
		out.ProcessUserCPU, out.ProcessSystemCPU, out.CPUAvailable = user1-user0, sys1-sys0, true
	}
	if cdc, ok := w.(*cdcStreamWriter); ok {
		out.SpoolWriteBytes = cdc.count * cdcEntrySize
		out.SpoolReadBytes = out.SpoolWriteBytes
	}
	inputSHA := sha256.Sum256(input)
	out.InputSHA256 = hex.EncodeToString(inputSHA[:])
	encoded, err := os.ReadFile(path)
	if err != nil || uint64(len(encoded)) != metadata.size || sha256.Sum256(encoded) != metadata.checksum {
		t.Fatalf("actual file metadata differs: %v", err)
	}
	decoded, err := decompressBlockBlob(encoded)
	if err != nil || !bytes.Equal(decoded, input) {
		t.Fatalf("actual codec oracle differs format=%s: %v", format, err)
	}
	files, err := os.ReadDir(dir)
	if err != nil || len(files) != 1 {
		t.Fatalf("codec scratch survived finish: %v %v", files, err)
	}
	return out
}

type historyIOLSMHypothesis struct {
	Name                                        string
	WAL, Flush, CompactionRead, CompactionWrite uint64
}

// Multipliers are user-supplied sensitivity parameters against the logical
// container bytes. They are NOT measured LSM amplification or a claim that
// hot Pebble representation has the same size as this cold logical stream.
func historyIOHypotheticalCost(o historyIOObservation, sourceReads uint64, lsm historyIOLSMHypothesis) (historyIOCost, error) {
	multiply := func(factor uint64) (uint64, error) {
		if factor != 0 && o.LogicalBytes > ^uint64(0)/factor {
			return 0, fmt.Errorf("history I/O hypothesis overflow")
		}
		return o.LogicalBytes * factor, nil
	}
	out := historyIOCost{FinalWrite: o.StoredBytes, SpoolRead: o.SpoolReadBytes, SpoolWrite: o.SpoolWriteBytes}
	for _, item := range []struct {
		dst    *uint64
		factor uint64
	}{
		{&out.SourceRead, sourceReads}, {&out.WALWrite, lsm.WAL}, {&out.FlushWrite, lsm.Flush},
		{&out.CompactionRead, lsm.CompactionRead}, {&out.CompactionWrite, lsm.CompactionWrite},
	} {
		n, err := multiply(item.factor)
		if err != nil {
			return historyIOCost{}, err
		}
		*item.dst = n
	}
	_, err := out.bytes()
	return out, err
}

type historyIOScenario struct {
	Fixture, Format, InputSHA256, LSMHypothesis       string
	BandwidthMiBPerSecond, SourceReadLogicalMultiples uint64
	Artifacts, ProducerWorkers, WaitingQueueCapacity  int
	CostPerArtifact                                   historyIOCost
	ObservedFactoryWallPerArtifact                    time.Duration
	Modeled                                           historyIOModelResult
}

func historyIOScenarios(o historyIOObservation) ([]historyIOScenario, error) {
	var scenarios []historyIOScenario
	// Explicit bounds for sensitivity analysis, not inferred measurements.
	lsmCases := []historyIOLSMHypothesis{
		{Name: "container-only; no LSM charge"},
		{Name: "illustrative 4x logical LSM I/O", WAL: 1, Flush: 1, CompactionRead: 1, CompactionWrite: 1},
		{Name: "illustrative 10x logical LSM I/O", WAL: 1, Flush: 1, CompactionRead: 4, CompactionWrite: 4},
	}
	for _, lsm := range lsmCases {
		for _, reads := range []uint64{0, 1, 2} {
			cost, err := historyIOHypotheticalCost(o, reads, lsm)
			if err != nil {
				return nil, err
			}
			for _, rate := range []uint64{50, 100, 250} {
				for _, workers := range []int{1, 2} {
					jobs := make([]historyIOJob, 8)
					for i := range jobs {
						jobs[i] = historyIOJob{Work: o.FactoryWall, Cost: cost}
					}
					result, err := modelHistoryIO(jobs, workers, 2, rate<<20)
					if err != nil {
						return nil, err
					}
					scenarios = append(scenarios, historyIOScenario{o.Fixture, o.Format, o.InputSHA256, lsm.Name, rate, reads, len(jobs), workers, 2, cost, o.FactoryWall, result})
				}
			}
		}
	}
	return scenarios, nil
}

func TestHistoryIOCostActualCodecFilesAndSpool(t *testing.T) {
	inputs := []struct {
		name string
		data []byte
	}{{"RepeatedSmallCanary", cdcRepeatedInput(256<<10, 4)}}
	random := make([]byte, 256<<10)
	rand.New(rand.NewSource(707)).Read(random)
	inputs = append(inputs, struct {
		name string
		data []byte
	}{"RandomSmallCanary", random})
	for _, input := range inputs {
		var priorSHA string
		for _, format := range []string{"2", "3"} {
			o := observeHistoryIO(t, t.TempDir(), input.name, format, input.data)
			if o.FactoryWall <= 0 || o.StoredBytes == 0 || format == "2" && (o.SpoolReadBytes != 0 || o.SpoolWriteBytes != 0) || format == "3" && o.SpoolReadBytes == 0 {
				t.Fatalf("bad real byte observation: %+v", o)
			}
			if priorSHA != "" && priorSHA != o.InputSHA256 {
				t.Fatal("codecs received different input")
			}
			priorSHA = o.InputSHA256
			scenarios, err := historyIOScenarios(o)
			if err != nil || len(scenarios) != 54 {
				t.Fatalf("scenario coverage %d: %v", len(scenarios), err)
			}
			for _, s := range scenarios {
				if s.Modeled.MaxQueued > 2 || s.Modeled.MaxOccupiedProducers > s.ProducerWorkers || s.Modeled.Wall < s.Modeled.DiskBusy {
					t.Fatalf("model exceeded resource budget: %+v", s)
				}
			}
		}
	}
}

func TestHistoryIOCostCommonReadsReduceRelativeBenefit(t *testing.T) {
	v2 := historyIOObservation{LogicalBytes: 16 << 20, StoredBytes: 16 << 20, FactoryWall: time.Millisecond}
	v3 := v2
	v3.StoredBytes, v3.SpoolReadBytes, v3.SpoolWriteBytes = 2<<20, 1024, 1024
	var ratios []float64
	for _, reads := range []uint64{0, 1, 2} {
		var elapsed []time.Duration
		for _, o := range []historyIOObservation{v2, v3} {
			cost, err := historyIOHypotheticalCost(o, reads, historyIOLSMHypothesis{})
			if err != nil {
				t.Fatal(err)
			}
			r, err := modelHistoryIO([]historyIOJob{{Work: o.FactoryWall, Cost: cost}}, 1, 2, 100<<20)
			if err != nil {
				t.Fatal(err)
			}
			elapsed = append(elapsed, r.Wall)
		}
		ratios = append(ratios, float64(elapsed[0])/float64(elapsed[1]))
	}
	if !(ratios[0] > ratios[1] && ratios[1] > ratios[2] && ratios[2] > 1) {
		t.Fatal("common reads were hidden", ratios)
	}
	if _, err := historyIOHypotheticalCost(historyIOObservation{LogicalBytes: ^uint64(0)}, 2, historyIOLSMHypothesis{}); err == nil {
		t.Fatal("hypothesis wrapped")
	}
}

// Opt in after coordinating host CPU use; ordinary go test never runs this
// larger observation. There are no bandwidth sleeps: all scenario times are
// modeled from actual codec observations. Output is confined to benchmark data.
func TestHistoryIOCostReport(t *testing.T) {
	if os.Getenv("GTRON_HISTORY_IO_REPORT") != "1" {
		t.Skip("opt-in real codec observations + discrete-event report")
	}
	type report struct {
		CreatedUTC, GoVersion, OS, Arch string
		GOMAXPROCS                      int
		Notes                           []string
		Observations                    []historyIOObservation
		Scenarios                       []historyIOScenario
	}
	r := report{CreatedUTC: time.Now().UTC().Format(time.RFC3339), GoVersion: runtime.Version(), OS: runtime.GOOS, Arch: runtime.GOARCH, GOMAXPROCS: runtime.GOMAXPROCS(0), Notes: []string{
		"All JSON durations are integer nanoseconds; byte counts are bytes; one MiB is 1048576 bytes.",
		"Synthetic fixed fixtures from cdcBenchmarkInputs; exact decoded bytes and input SHA are checked for each format.",
		"FactoryWall includes actual codec, temp-file writes, Sync and rename on this development host; it is not isolated CPU time.",
		"Process CPU user/system are separate Getrusage deltas, including all goroutines in this test process; CPU seconds are not critical-path wall time.",
		"Modeled time is a discrete-event shared byte-bandwidth server, not measured throttled device or mainnet end-to-end time; no sleep emulates throughput.",
		"Final stored bytes include directory/footer; CDC temporary table contributes one additional write and read of 28 bytes per non-prefix chunk.",
		"Source read 0/1/2 times logical are cache/miss sensitivity assumptions for existing build traversals, not measured disk reads or a literal cold/hot byte mapping.",
		"LSM WAL/flush/compaction charges 0/4/10 times logical are labeled illustrative user parameters, not inferred or measured amplification.",
		"Eight artifacts; producer count 1/2; waiting queue capacity 2; one shared disk service. No intra-artifact compute/I/O overlap is modeled.",
		"All read/write charges are aggregated into each artifact's disk service after producer work; source-read-before-decode dependencies and phase scheduling are not simulated.",
		"Excludes key/posting ETL, accessor/index construction, filesystem metadata and fsync latency, verification/cold reads, physical reclaim and other services except explicitly supplied charges.",
		"Common reads/LSM charges do not disappear when final output shrinks. No fixed-disk capacity or global speedup prediction follows from this model.",
	}}
	for _, input := range cdcBenchmarkInputs() {
		for _, format := range []string{"2", "3"} {
			dir := t.TempDir()
			observeHistoryIO(t, dir, input.name, format, input.data) // Warm shared codec initialization.
			o := observeHistoryIO(t, dir, input.name, format, input.data)
			r.Observations = append(r.Observations, o)
			scenarios, err := historyIOScenarios(o)
			if err != nil {
				t.Fatal(err)
			}
			r.Scenarios = append(r.Scenarios, scenarios...)
		}
	}
	_, source, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("cannot resolve benchmark output directory")
	}
	dir := filepath.Clean(filepath.Join(filepath.Dir(source), "..", "..", "..", "build", "benchmarks", "20260907-history-io"))
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	data, err := json.MarshalIndent(r, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, fmt.Sprintf("observations-%s-%d.json", time.Now().UTC().Format("20060102T150405.000000000Z"), os.Getpid()))
	if err := os.WriteFile(path, append(data, '\n'), 0o644); err != nil {
		t.Fatal(err)
	}
	t.Logf("Actual codec observations and explicitly modeled scenarios: %s", path)
}
