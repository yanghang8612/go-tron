package main

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"testing"

	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/urfave/cli/v2"
)

func TestTxIndexPrefixFingerprint(t *testing.T) {
	var hash [32]byte
	copy(hash[:], []byte{0xab, 0xcd, 0xef, 0x12, 0x34, 0x56, 0x78, 0x9a, 0xbc, 0xde, 0xf0})
	prefix, fingerprint := txIndexPrefixFingerprint(hash, 20)
	if prefix != 0xabcde {
		t.Fatalf("prefix = %x, want abcde", prefix)
	}
	if fingerprint != 0xf123456789abcdef {
		t.Fatalf("fingerprint = %x, want f123456789abcdef", fingerprint)
	}
}

func TestBuildTxIndexBenchmarkTableLookup(t *testing.T) {
	samples := make([]rawdb.TransactionIndexSample, 1024)
	for i := range samples {
		binary.BigEndian.PutUint64(samples[i].Hash[:8], uint64(i)<<40)
		binary.BigEndian.PutUint64(samples[i].Hash[8:16], uint64(i)*17)
		samples[i].Location = uint64(i + 100)
	}
	table := buildTxIndexBenchmarkTable(samples, 12)
	for _, sample := range samples {
		start, end := table.lookup(sample.Hash)
		if end-start != 1 || table.locations[start] != sample.Location {
			t.Fatalf("lookup %x = [%d,%d)", sample.Hash[:4], start, end)
		}
	}
	missing := samples[10].Hash
	missing[9] ^= 0x80
	if start, end := table.lookup(missing); start != end {
		t.Fatalf("missing lookup returned %d candidates", end-start)
	}
}

func TestProjectTxIndexCandidates(t *testing.T) {
	candidates := projectTxIndexCandidates(1_000_000_000, 20, (1<<20+1)*8, 0)
	if len(candidates) != 4 {
		t.Fatalf("candidates = %d, want 4", len(candidates))
	}
	compact := candidates[2]
	if compact.EntryBytes != 16 || compact.SavingsPercent < 60 || compact.SavingsPercent > 64 {
		t.Fatalf("compact candidate = %+v", compact)
	}
	if compact.ExpectedCollisionPairs >= 0.001 {
		t.Fatalf("unexpected collision projection: %g", compact.ExpectedCollisionPairs)
	}
}

func TestDBBenchmarkTxIndexCommandJSON(t *testing.T) {
	datadir := t.TempDir()
	db, err := rawdb.NewPebbleDB(chainDataDir(datadir), 16, 16)
	if err != nil {
		t.Fatal(err)
	}
	const rows = 4096
	for i := 0; i < rows; i++ {
		var hash [32]byte
		// Cover the complete leading 16-bit space while retaining deterministic
		// sorted keys and unique suffix fingerprints.
		binary.BigEndian.PutUint16(hash[:2], uint16(i*16))
		binary.BigEndian.PutUint64(hash[8:16], uint64(i)*0x9e3779b97f4a7c15)
		if err := rawdb.WriteTransactionLocation(db, hash[:], uint64(i/8), i%8); err != nil {
			t.Fatal(err)
		}
	}
	rawdb.WriteTotalTransactionCount(db, rows)
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	var stdout, stderr bytes.Buffer
	app := &cli.App{Writer: &stdout, ErrWriter: &stderr, Commands: []*cli.Command{dbCommand()}}
	if err := app.Run([]string{
		"gtron", "db", "benchmark-tx-index",
		"--datadir", datadir,
		"--db.cache", "16",
		"--db.handles", "16",
		"--sample-transactions", "1024",
		"--windows", "64",
		"--prefix-bits", "12",
		"--lookups", "10000",
		"--progress", "0s",
		"--json",
	}); err != nil {
		t.Fatalf("benchmark tx index: %v\nstderr: %s", err, stderr.String())
	}
	var report txIndexBenchmarkOutput
	if err := json.Unmarshal(stdout.Bytes(), &report); err != nil {
		t.Fatalf("decode JSON: %v\noutput: %s", err, stdout.String())
	}
	if report.SampleTransactions != 1024 || report.ProjectedRows != rows || len(report.Candidates) != 4 {
		t.Fatalf("report = %+v", report)
	}
	if !report.ProjectionUsesCounter || report.Lookup.PositiveNanosecondsPerOp <= 0 {
		t.Fatalf("report metadata = %+v", report)
	}
}
