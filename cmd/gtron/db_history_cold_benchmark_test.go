package main

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"math/rand"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/blockbuffer"
	"github.com/tronprotocol/go-tron/core/pointread"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/state/kvdomains"
	"github.com/tronprotocol/go-tron/core/state/snapshots"
	"github.com/tronprotocol/go-tron/core/types"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	"github.com/urfave/cli/v2"
)

func coldBenchmarkFixture(t *testing.T, repairs bool) (string, string, historyRangeExportManifest) {
	t.Helper()
	source := t.TempDir()
	db, err := rawdb.NewPebbleDB(chainDataDir(source), 16, 16)
	if err != nil {
		t.Fatal(err)
	}
	rawdb.SetStateHistoryCrossBlockDedup(true)
	defer rawdb.SetStateHistoryCrossBlockDedup(false)
	previous := make([]byte, 256<<10)
	_, _ = rand.New(rand.NewSource(20260915)).Read(previous)
	var parent common.Hash
	for n := uint64(1); n <= 4; n++ {
		block := types.NewBlockFromPB(&corepb.Block{BlockHeader: &corepb.BlockHeader{RawData: &corepb.BlockHeaderRaw{Number: int64(n), Timestamp: int64(n * 3000), ParentHash: parent.Bytes()}}})
		parent = block.Hash()
		if err := rawdb.WriteBlock(db, block); err != nil {
			t.Fatal(err)
		}
		begin, end := n, n
		if n == 2 {
			end = 3
		} else if n > 2 {
			begin, end = n+1, n+1
		}
		if err := rawdb.WriteStateTxRange(db, n, block.Hash(), begin, end); err != nil {
			t.Fatal(err)
		}
		if n == 2 || n == 4 {
			buffer := blockbuffer.New(db)
			buffer.BeginBlock(block.Hash(), n)
			row := &rawdb.StateDomainChange{BlockNum: n, TxNum: begin, Seq: 1, FlatDomain: rawdb.StateFlatDomainKVLatest, Owner: common.Address{0x41, 1}, Domain: kvdomains.SystemDelegation, Key: []byte("large"), PrevExists: true, Prev: previous}
			rows := []*rawdb.StateDomainChange{row}
			if n == 2 {
				second := *row
				second.TxNum, second.Seq, second.Key = end, 2, []byte("second")
				rows = append(rows, &second)
			}
			if err := rawdb.WriteStateDomainChangeBlockRows(buffer, rows); err != nil {
				t.Fatal(err)
			}
			buffer.CommitBlock()
			if err := buffer.Flush(db); err != nil {
				t.Fatal(err)
			}
			buffer.Discard()
			if n == 2 && repairs {
				// Repair after tx=3 physically returns to tx=2, exercising the
				// production fallback sort and diagnostic ordered-row equivalence.
				repair := *row
				repair.Seq, repair.Key, repair.Prev = 9, []byte("repair"), []byte("repair-old")
				if err := rawdb.WriteStateDomainChangeRow(db, &repair); err != nil {
					t.Fatal(err)
				}
			}
		}
		if n == 1 {
			if err := rawdb.WriteStageProgressWithHash(db, rawdb.StageSnapshotBuild, n, block.Hash()); err != nil {
				t.Fatal(err)
			}
		}
		if n == 4 {
			if err := rawdb.WriteStageProgressWithHash(db, rawdb.StageFinish, n, block.Hash()); err != nil {
				t.Fatal(err)
			}
		}
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	input := filepath.Join(t.TempDir(), "input")
	m, text, err := runHistoryRangeTest(context.Background(), source, input, "--blocks", "3")
	if err != nil || !m.Export.Complete || m.Export.UniqueChunkCount == 0 || m.Export.BlockDetails[1].PackPresent || m.Export.BlockDetails[0].RepairRows != map[bool]uint64{false: 0, true: 1}[repairs] {
		t.Fatalf("fixture export: %+v %v %s", m.Export, err, text)
	}
	return source, input, m
}

func coldBenchmarkOptions(input, output string) historyColdBenchmarkOptions {
	return historyColdBenchmarkOptions{InputDir: input, OutputDir: output, Iterations: 1, MaxDuration: time.Minute, CopyMode: "owned", CompressionFormat: "auto", SharedReadWorkers: 2}
}

func TestDBHistoryColdBenchmarkCompleteModesAndReadOnly(t *testing.T) {
	source, input, _ := coldBenchmarkFixture(t, false)
	sourceBefore, inputBefore := historyInspectionFileHashes(t, source), historyInspectionFileHashes(t, input)
	// An invalid process environment must neither select a different format nor
	// be rewritten by this standalone diagnostic.
	t.Setenv("GTRON_HISTORY_COMPRESSION_FORMAT", "invalid-diagnostic-control")
	for _, format := range []string{"auto", "2", "3"} {
		var reports []historyColdBenchmarkReport
		for _, mode := range []string{"defensive", "owned"} {
			opts := coldBenchmarkOptions(input, filepath.Join(t.TempDir(), mode))
			opts.CopyMode = mode
			opts.CompressionFormat = format
			opts.Iterations = 2
			opts.CPUProfile = mode == "owned"
			report, err := benchmarkHistoryCold(context.Background(), opts)
			if err != nil || !report.Complete || report.CompressionFormat != format || report.Options.CompressionFormat != format || !report.PhysicalVerified || len(report.Iterations) != 2 || report.SourceDigest.Rows != 3 || report.SourceDigest.TxRanges != 3 {
				t.Fatalf("%s report=%+v err=%v", mode, report, err)
			}
			digest := report.SourceDigest
			if digest.PrevBytes != 3*(256<<10) || digest.MaxPrevBytes != 256<<10 || digest.LargePrevRows != 3 || digest.LargePrevBytes != digest.PrevBytes || digest.DelegationRows != 3 || digest.DelegationPrevBytes != digest.PrevBytes {
				t.Fatalf("distribution not preserved: %+v", digest)
			}
			if mode == "owned" && (report.CPUProfileSHA256 == "" || !strings.Contains(report.CPUProfileScope, "build only")) {
				t.Fatal("CPU profile provenance missing")
			}
			for _, iteration := range report.Iterations {
				if !iteration.Equivalent || iteration.BuildWallNanos <= 0 || iteration.VerificationWallNanos <= 0 || iteration.BuildAllocatedBytes == 0 || iteration.OutputBytes == 0 || len(iteration.Refs) != 3 {
					t.Fatalf("incomplete iteration: %+v", iteration)
				}
			}
			data, err := os.ReadFile(filepath.Join(opts.OutputDir, "report.json"))
			if err != nil {
				t.Fatal(err)
			}
			var durable historyColdBenchmarkReport
			if err := json.Unmarshal(data, &durable); err != nil || !reflect.DeepEqual(durable, report) {
				t.Fatalf("durable report differs: %v", err)
			}
			info, _ := os.Stat(opts.OutputDir)
			if info.Mode().Perm() != 0700 {
				t.Fatal("output is not private")
			}
			reports = append(reports, report)
		}
		if os.Getenv("GTRON_HISTORY_COMPRESSION_FORMAT") != "invalid-diagnostic-control" {
			t.Fatal("diagnostic mutated environment")
		}
		if reports[0].SourceDigest != reports[1].SourceDigest {
			t.Fatal("mode changed source semantics")
		}
		for i := range reports[0].Iterations {
			if !reflect.DeepEqual(reports[0].Iterations[i].Refs, reports[1].Iterations[i].Refs) {
				t.Fatal("copy mode changed complete trio bytes/ref checksums")
			}
		}
	}
	if !reflect.DeepEqual(sourceBefore, historyInspectionFileHashes(t, source)) || !reflect.DeepEqual(inputBefore, historyInspectionFileHashes(t, input)) {
		t.Fatal("benchmark changed source or private input")
	}
}

func TestDBHistoryColdBenchmarkPhysicalCorruptionRejected(t *testing.T) {
	for _, mode := range []string{"extra", "missing", "altered", "manifest-digest", "declared", "incomplete"} {
		t.Run(mode, func(t *testing.T) {
			_, input, m := coldBenchmarkFixture(t, false)
			if mode == "extra" || mode == "missing" || mode == "altered" {
				db, err := rawdb.NewPebbleDB(filepath.Join(input, "pebble"), 16, 16)
				if err != nil {
					t.Fatal(err)
				}
				key, _ := hex.DecodeString(m.Export.Entries[0].KeyHex)
				switch mode {
				case "extra":
					err = db.Put([]byte("unexpected"), []byte("row"))
				case "missing":
					err = db.Delete(key)
				case "altered":
					err = db.Put(key, []byte("changed"))
				}
				if err != nil {
					t.Fatal(err)
				}
				if err := db.Close(); err != nil {
					t.Fatal(err)
				}
			} else {
				switch mode {
				case "manifest-digest":
					m.Export.ManifestSHA256 = strings.Repeat("0", 64)
				case "declared":
					m.Export.DeclaredDecodedBytes++
				case "incomplete":
					m.Export.Complete = false
				}
				data, _ := json.Marshal(m)
				if err := os.WriteFile(filepath.Join(input, "manifest.json"), data, 0600); err != nil {
					t.Fatal(err)
				}
			}
			output := filepath.Join(t.TempDir(), "out")
			report, err := benchmarkHistoryCold(context.Background(), coldBenchmarkOptions(input, output))
			if err == nil || report.Complete || report.PhysicalVerified || len(report.Iterations) != 0 {
				t.Fatalf("accepted %s: %+v %v", mode, report, err)
			}
			if mode != "incomplete" {
				data, err := os.ReadFile(filepath.Join(output, "report.json"))
				if err != nil {
					t.Fatal(err)
				}
				var durable historyColdBenchmarkReport
				if err := json.Unmarshal(data, &durable); err != nil || durable.Complete || durable.Error == "" {
					t.Fatal("failure not durable")
				}
			}
		})
	}
}

func TestDBHistoryColdBenchmarkPathsOptionsAndCancellation(t *testing.T) {
	source, input, _ := coldBenchmarkFixture(t, false)
	for _, output := range []string{input, filepath.Join(input, "child"), source, filepath.Join(source, "child")} {
		if report, err := benchmarkHistoryCold(context.Background(), coldBenchmarkOptions(input, output)); err == nil || report.Complete {
			t.Fatalf("accepted unsafe output %s", output)
		}
	}
	alias := filepath.Join(t.TempDir(), "source-alias")
	if err := os.Symlink(source, alias); err != nil {
		t.Fatal(err)
	}
	if _, err := benchmarkHistoryCold(context.Background(), coldBenchmarkOptions(input, filepath.Join(alias, "child"))); err == nil {
		t.Fatal("accepted source alias")
	}
	for _, mutate := range []func(*historyColdBenchmarkOptions){func(o *historyColdBenchmarkOptions) { o.Iterations = 0 }, func(o *historyColdBenchmarkOptions) { o.Iterations = 6 }, func(o *historyColdBenchmarkOptions) { o.MaxDuration = 11 * time.Minute }, func(o *historyColdBenchmarkOptions) { o.MaxDuration = 0 }, func(o *historyColdBenchmarkOptions) { o.CopyMode = "unknown" }, func(o *historyColdBenchmarkOptions) { o.CompressionFormat = "1" }, func(o *historyColdBenchmarkOptions) { o.CompressionFormat = "" }, func(o *historyColdBenchmarkOptions) { o.OutputDir = "relative" }} {
		opts := coldBenchmarkOptions(input, filepath.Join(t.TempDir(), "out"))
		mutate(&opts)
		if _, err := benchmarkHistoryCold(context.Background(), opts); err == nil {
			t.Fatal("accepted invalid options")
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	opts := coldBenchmarkOptions(input, filepath.Join(t.TempDir(), "canceled"))
	if report, err := benchmarkHistoryCold(ctx, opts); !errors.Is(err, context.Canceled) || report.Complete {
		t.Fatalf("cancellation: %+v %v", report, err)
	}
	if _, err := os.Stat(opts.OutputDir); !os.IsNotExist(err) {
		t.Fatal("created output after pre-cancellation")
	}
}

func TestDBHistoryColdBenchmarkPinnedCapabilityAndColdCorruption(t *testing.T) {
	_, input, m := coldBenchmarkFixture(t, false)
	db, err := rawdb.NewPebbleDBReadOnly(filepath.Join(input, "pebble"), 16, 16)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	view, release, err := rawdb.AcquireStateHistoryReadView(db)
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	if owned, ok := view.(pointread.OwnedKeyValueReader); !ok || !owned.GetReturnsOwnedBytes() {
		t.Fatal("owned fixture missing guarantee")
	}
	defensive := defensiveHistoryColdView{view}
	if _, ok := any(defensive).(pointread.OwnedKeyValueReader); ok {
		t.Fatal("defensive wrapper leaked ownership")
	}
	nested, nestedRelease, err := rawdb.AcquireStateHistoryReadView(defensive)
	if err != nil {
		t.Fatal(err)
	}
	defer nestedRelease()
	if nested != defensive || !nested.IsPinnedKeyValueView() {
		t.Fatal("nested acquisition escaped defensive snapshot")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	e := m.Export
	if _, err := snapshots.BuildDiagnosticStateHistoryTrioContext(ctx, view, t.TempDir(), e.FromTxNum, e.ToTxNum, e.FromBlock, e.ToBlock, "history/state-domain-change-range.seg", "auto"); !errors.Is(err, context.Canceled) {
		t.Fatalf("build cancellation: %v", err)
	}
	dir := t.TempDir()
	refs, err := snapshots.BuildDiagnosticStateHistoryTrioContext(context.Background(), view, dir, e.FromTxNum, e.ToTxNum, e.FromBlock, e.ToBlock, "history/state-domain-change-range.seg", "auto")
	if err != nil {
		t.Fatal(err)
	}
	file := filepath.Join(dir, refs[0].Path)
	blob, err := os.ReadFile(file)
	if err != nil {
		t.Fatal(err)
	}
	blob[len(blob)/2] ^= 1
	if err := os.WriteFile(file, blob, 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := snapshots.DigestColdStateHistoryContext(context.Background(), dir, refs); err == nil {
		t.Fatal("cold corruption accepted")
	}
}

func TestDBHistoryColdBenchmarkCLIFlags(t *testing.T) {
	var out bytes.Buffer
	app := &cli.App{Writer: &out, ErrWriter: &out, Commands: []*cli.Command{dbHistoryColdBenchmarkCommand()}}
	err := app.Run([]string{"gtron", "benchmark-history-cold", "--input-dir", "/missing", "--output-dir", "/also-missing", "--iterations", "6"})
	var report historyColdBenchmarkReport
	if json.Unmarshal(out.Bytes(), &report) != nil || err == nil || report.Complete || report.Options.Iterations != 6 || report.Options.CopyMode != "owned" || report.Options.CompressionFormat != "auto" {
		t.Fatalf("CLI result: %s %v", out.String(), err)
	}
}

type cancelingColdBenchmarkView struct {
	rawdb.StateHistoryReadView
	cancel context.CancelFunc
	gets   int
}

func (v *cancelingColdBenchmarkView) Get(key []byte) ([]byte, error) {
	value, err := v.StateHistoryReadView.Get(key)
	v.gets++
	if v.gets == 2 {
		v.cancel()
	}
	return value, err
}

func TestDBHistoryColdBenchmarkLegacyAndActiveCancellation(t *testing.T) {
	_, input, m := coldBenchmarkFixture(t, true)
	output := filepath.Join(t.TempDir(), "repair-output")
	report, err := benchmarkHistoryCold(context.Background(), coldBenchmarkOptions(input, output))
	if !errors.Is(err, rawdb.ErrStateDomainChangeBorrowedLegacyRows) || report.Complete || !report.PhysicalVerified || len(report.Iterations) != 0 {
		t.Fatalf("production legacy failure lost: %+v %v", report, err)
	}
	if report.Error == "" {
		t.Fatal("legacy failure missing report")
	}
	db, err := rawdb.NewPebbleDBReadOnly(filepath.Join(input, "pebble"), 16, 16)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	view, release, err := rawdb.AcquireStateHistoryReadView(db)
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	for _, sourceDigest := range []bool{false, true} {
		ctx, cancel := context.WithCancel(context.Background())
		controlled := &cancelingColdBenchmarkView{StateHistoryReadView: view, cancel: cancel}
		dir := t.TempDir()
		e := m.Export
		if sourceDigest {
			_, err = snapshots.DigestHotStateHistoryContext(ctx, controlled, dir, e.FromTxNum, e.ToTxNum, e.FromBlock, e.ToBlock, 4<<30)
		} else {
			_, err = snapshots.BuildDiagnosticStateHistoryTrioContext(ctx, controlled, dir, e.FromTxNum, e.ToTxNum, e.FromBlock, e.ToBlock, "history/state-domain-change-range.seg", "auto")
		}
		cancel()
		if !errors.Is(err, context.Canceled) || controlled.gets < 2 {
			t.Fatalf("active cancellation source=%v gets=%d err=%v", sourceDigest, controlled.gets, err)
		}
		entries, err := os.ReadDir(filepath.Join(dir, "history"))
		if err != nil && !os.IsNotExist(err) {
			t.Fatal(err)
		}
		for _, entry := range entries {
			if strings.HasSuffix(entry.Name(), ".seg") || strings.HasSuffix(entry.Name(), ".kv") || strings.HasSuffix(entry.Name(), ".ef") {
				t.Fatal("canceled builder published a partial trio")
			}
		}
	}
}
