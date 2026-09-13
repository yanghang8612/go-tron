package main

import (
	"bytes"
	"context"
	"encoding/json"
	"math/rand/v2"
	"os"
	"path/filepath"
	"testing"

	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/blockbuffer"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/state/kvdomains"
)

func TestDBHistoryPackExportSharedV2AndLegacyV1Benchmark(t *testing.T) {
	datadir := t.TempDir()
	db, err := rawdb.NewPebbleDB(chainDataDir(datadir), 16, 16)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	rawdb.SetStateHistoryCrossBlockDedup(true)
	t.Cleanup(func() { rawdb.SetStateHistoryCrossBlockDedup(false) })
	previous := make([]byte, 256<<10)
	rng := rand.New(rand.NewPCG(17, 19))
	for i := range previous {
		previous[i] = byte(rng.Uint64())
	}
	b := blockbuffer.New(db)
	b.BeginBlock(common.Hash{7}, 7)
	row := &rawdb.StateDomainChange{BlockNum: 7, Seq: 1, TxNum: 101, FlatDomain: rawdb.StateFlatDomainKVLatest,
		Owner: common.Address{common.AddressPrefixMainnet, 1}, Domain: kvdomains.SystemDelegation,
		Key: []byte("drax-export"), PrevExists: true, Prev: previous}
	if err := rawdb.WriteStateDomainChangeBlockRows(b, []*rawdb.StateDomainChange{row}); err != nil {
		t.Fatal(err)
	}
	b.CommitBlock()
	if err := b.Flush(db); err != nil {
		t.Fatal(err)
	}
	b.Discard()
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	// Readers and offline diagnostics must work with new writes disabled.
	rawdb.SetStateHistoryCrossBlockDedup(false)
	directory := filepath.Join(t.TempDir(), "packs")
	report, output, err := runDBInspectHistoryTest(context.Background(), datadir, "--from-block", "7", "--to-block", "7", "--export-packs", directory)
	if err != nil || !report.Complete || len(report.Samples) != 1 || report.Samples[0].Codec != "shared3" {
		t.Fatalf("v3 inspect: %s %v", output, err)
	}
	manifestPath := filepath.Join(directory, "manifest.json")
	data, err := os.ReadFile(manifestPath)
	if err != nil {
		t.Fatal(err)
	}
	var manifest historyPackExportManifest
	if err := json.Unmarshal(data, &manifest); err != nil {
		t.Fatal(err)
	}
	if manifest.Version != 2 || len(manifest.Entries) != 1 {
		t.Fatalf("manifest=%+v", manifest)
	}
	entry := manifest.Entries[0]
	if !entry.Materialized || entry.SourceCodec != "shared3" || entry.Codec != "raw" || entry.SourcePackBytes != report.Samples[0].EncodedBytes || entry.SourceChunkReadBytes != report.ChunkReadBytes || entry.EncodedBytes != report.ExportBytesAccepted || entry.SourcePackBytes >= entry.EncodedBytes {
		t.Fatalf("source/file accounting confused: entry=%+v report=%+v", entry, report)
	}
	encoded, err := os.ReadFile(filepath.Join(directory, entry.File))
	if err != nil {
		t.Fatal(err)
	}
	codec, size, err := rawdb.InspectStateHistoryPackEncoding(encoded)
	if err != nil || codec != "raw" || size != entry.DecodedBytes || !bytes.Contains(encoded, previous) {
		t.Fatalf("materialized file codec=%q size=%d err=%v", codec, size, err)
	}
	bench, err := benchmarkHistoryExport(context.Background(), directory)
	if err != nil || !bench.Complete || bench.ManifestVersion != 2 || bench.MaterializedPacks != 1 {
		t.Fatalf("v2 benchmark=%+v err=%v", bench, err)
	}
	// The same standalone representation is a valid v1 input once all v2-only
	// accounting is removed. This exercises compatibility with old exports.
	manifest.Version = 1
	manifest.Entries[0].SourceCodec = ""
	manifest.Entries[0].SourcePackBytes = 0
	manifest.Entries[0].SourceChunkReadBytes = 0
	manifest.Entries[0].SourceChunkReads = 0
	manifest.Entries[0].Materialized = false
	writeManifest := func() {
		t.Helper()
		data, err := json.Marshal(manifest)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(manifestPath, data, 0600); err != nil {
			t.Fatal(err)
		}
	}
	writeManifest()
	bench, err = benchmarkHistoryExport(context.Background(), directory)
	if err != nil || !bench.Complete || bench.ManifestVersion != 1 || bench.MaterializedPacks != 0 {
		t.Fatalf("v1 benchmark=%+v err=%v", bench, err)
	}
	manifest.Entries[0].Codec = "snappy1"
	writeManifest()
	bench, err = benchmarkHistoryExport(context.Background(), directory)
	if err == nil || bench.Complete || bench.Stats.Attempts != 0 {
		t.Fatalf("file codec mismatch accepted: %+v %v", bench, err)
	}
}

func TestDBHistoryPackExportRejectsAmbiguousV2Source(t *testing.T) {
	entry := historyPackExportEntry{Block: 1, Codec: "raw", SHA256: "0000000000000000000000000000000000000000000000000000000000000000",
		EncodedBytes: 100, DecodedBytes: 100, SourceCodec: "shared3", SourcePackBytes: 80, SourceChunkReadBytes: 100, SourceChunkReads: 1, Materialized: true}
	if err := validateHistoryPackExportEntry(2, entry); err != nil {
		t.Fatal(err)
	}
	for _, mutate := range []func(*historyPackExportEntry){
		func(e *historyPackExportEntry) { e.Materialized = false },
		func(e *historyPackExportEntry) { e.Codec = "shared3" },
		func(e *historyPackExportEntry) { e.SourceChunkReadBytes = 0 },
		func(e *historyPackExportEntry) { e.SourceChunkReads = 0 },
		func(e *historyPackExportEntry) { e.SourcePackBytes = 0 },
		func(e *historyPackExportEntry) { e.SourceCodec = "raw" },
	} {
		bad := entry
		mutate(&bad)
		if err := validateHistoryPackExportEntry(2, bad); err == nil {
			t.Fatalf("ambiguous source accepted: %+v", bad)
		}
	}
}
