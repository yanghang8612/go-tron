package main

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"testing"

	chainfreezer "github.com/tronprotocol/go-tron/core/freezer"
	rawdbfreezer "github.com/tronprotocol/go-tron/core/rawdb/freezer"
	"github.com/urfave/cli/v2"
)

func TestPlanAncientBenchmarkRanges(t *testing.T) {
	ranges := planAncientBenchmarkRanges(10_000, 1_000, 4)
	if got := sumAncientBenchmarkRanges(ranges); got != 1_000 {
		t.Fatalf("sampled rows = %d, want 1000", got)
	}
	if len(ranges) != 4 {
		t.Fatalf("windows = %d, want 4", len(ranges))
	}
	for i, sampleRange := range ranges {
		if sampleRange.Count != 250 {
			t.Errorf("range %d count = %d, want 250", i, sampleRange.Count)
		}
		if i > 0 && ranges[i-1].Start+ranges[i-1].Count > sampleRange.Start {
			t.Fatalf("ranges overlap: %+v", ranges)
		}
	}
}

func TestParseAncientFrameBlocks(t *testing.T) {
	got, err := parseAncientFrameBlocks("128, 32,128,64")
	if err != nil {
		t.Fatal(err)
	}
	want := []uint64{32, 64, 128}
	if len(got) != len(want) {
		t.Fatalf("frames = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("frames = %v, want %v", got, want)
		}
	}
	if _, err := parseAncientFrameBlocks("0"); err == nil {
		t.Fatal("zero frame size accepted")
	}
}

func TestDBBenchmarkAncientCommandJSON(t *testing.T) {
	datadir := t.TempDir()
	tables := chainfreezer.FreezerTableSet()
	freezer, err := rawdbfreezer.NewFreezer(ancientDataDir(datadir), "", false, 2049, tables)
	if err != nil {
		t.Fatalf("NewFreezer: %v", err)
	}
	if _, err := freezer.ModifyAncients(func(op rawdbfreezer.AncientWriteOp) error {
		for n := uint64(0); n < 512; n++ {
			var suffix [8]byte
			binary.BigEndian.PutUint64(suffix[:], n)
			body := append(bytes.Repeat([]byte("repeated-block-and-contract-shape"), 20), suffix[:]...)
			infos := append(bytes.Repeat([]byte("repeated-receipt-and-log-shape"), 30), suffix[:]...)
			if err := op.AppendRaw("bodies", n, body); err != nil {
				return err
			}
			if err := op.AppendRaw("tx_infos", n, infos); err != nil {
				return err
			}
			if err := op.AppendRaw("state_roots", n, suffix[:]); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatalf("ModifyAncients: %v", err)
	}
	if err := freezer.Sync(); err != nil {
		t.Fatalf("Sync: %v", err)
	}
	if err := freezer.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	var stdout, stderr bytes.Buffer
	app := &cli.App{Writer: &stdout, ErrWriter: &stderr, Commands: []*cli.Command{dbCommand()}}
	if err := app.Run([]string{
		"gtron", "db", "benchmark-ancient",
		"--datadir", datadir,
		"--sample-blocks", "256",
		"--windows", "4",
		"--frames", "16,64",
		"--progress", "0s",
		"--json",
	}); err != nil {
		t.Fatalf("benchmark ancient: %v\nstderr: %s", err, stderr.String())
	}
	var report ancientBenchmarkOutput
	if err := json.Unmarshal(stdout.Bytes(), &report); err != nil {
		t.Fatalf("decode JSON: %v\noutput: %s", err, stdout.String())
	}
	if report.SampleBlocks != 256 || len(report.Tables) != 2 {
		t.Fatalf("report summary = %+v", report)
	}
	for _, table := range report.Tables {
		if table.SampledRows != 256 || table.RawBytes == 0 || table.PhysicalBytes == 0 {
			t.Errorf("bad table stats: %+v", table)
		}
		if len(table.Codecs) != 4 {
			t.Fatalf("%s codecs = %d, want 4", table.Name, len(table.Codecs))
		}
		if table.Codecs[3].SavingsPercent <= 0 {
			t.Errorf("expected framed Zstd saving for %s: %+v", table.Name, table.Codecs[3])
		}
	}
}
