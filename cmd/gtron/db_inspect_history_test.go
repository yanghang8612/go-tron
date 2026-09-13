package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io/fs"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/state/kvdomains"
	"github.com/urfave/cli/v2"
)

func runDBInspectHistoryTest(ctx context.Context, datadir string, extra ...string) (rawdb.HistoryPrevInspection, string, error) {
	var out, errOut bytes.Buffer
	app := &cli.App{Writer: &out, ErrWriter: &errOut, Commands: []*cli.Command{dbCommand()}}
	args := []string{"gtron", "db", "inspect-history-prev", "--datadir", datadir, "--db.cache", "16", "--db.handles", "16"}
	err := app.RunContext(ctx, append(args, extra...))
	var result struct {
		Inspection rawdb.HistoryPrevInspection `json:"inspection"`
	}
	if jsonErr := json.Unmarshal(out.Bytes(), &result); jsonErr != nil && err == nil {
		return result.Inspection, out.String(), jsonErr
	}
	return result.Inspection, out.String(), err
}

func historyInspectionFileHashes(t *testing.T, dir string) map[string][32]byte {
	t.Helper()
	out := make(map[string][32]byte)
	err := filepath.WalkDir(dir, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			return nil
		}
		contents, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(dir, path)
		if err != nil {
			return err
		}
		out[rel] = sha256.Sum256(contents)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return out
}

func TestDBInspectHistoryPrevReadOnlyAndPartial(t *testing.T) {
	datadir := t.TempDir()
	db, err := rawdb.NewPebbleDB(chainDataDir(datadir), 16, 16)
	if err != nil {
		t.Fatal(err)
	}
	prev := bytes.Repeat([]byte("DO_NOT_EMIT_PREV_VALUE"), 1024)
	rows := []*rawdb.StateDomainChange{
		{BlockNum: 7, Seq: 1, TxNum: 101, FlatDomain: rawdb.StateFlatDomainKVLatest, Owner: common.Address{0x41, 1}, Generation: 1, Domain: kvdomains.SystemDelegation, Key: []byte("drax-test"), PrevExists: true, Prev: prev},
		{BlockNum: 7, Seq: 2, TxNum: 102, FlatDomain: rawdb.StateFlatDomainKVLatest, Owner: common.Address{0x41, 1}, Generation: 1, Domain: kvdomains.SystemDelegation, Key: []byte("drax-test"), PrevExists: true, Prev: prev},
	}
	if err := rawdb.WriteStateDomainChangeBlockRows(db, rows); err != nil {
		_ = db.Close()
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	// An unrelated ancient fixture must remain untouched; inspection reads only
	// the chaindata path, without constructing a node or opening a freezer.
	ancient := filepath.Join(filepath.Dir(chainDataDir(datadir)), "ancient")
	if err := os.MkdirAll(ancient, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(ancient, "unparseable.fixture"), []byte("not a freezer"), 0600); err != nil {
		t.Fatal(err)
	}
	before := historyInspectionFileHashes(t, datadir)
	report, output, err := runDBInspectHistoryTest(context.Background(), datadir, "--from-block", "7", "--to-block", "8")
	if err != nil || !report.Complete || report.PacksComplete != 1 || report.MissingPacks != 1 || report.Rows != 2 {
		t.Fatalf("inspection: %+v %v\n%s", report, err, output)
	}
	if strings.Contains(output, "DO_NOT_EMIT_PREV_VALUE") {
		t.Fatal("Prev value leaked into JSON")
	}
	if !reflect.DeepEqual(before, historyInspectionFileHashes(t, datadir)) {
		t.Fatal("read-only inspection changed source paths or file bytes")
	}
	report, output, err = runDBInspectHistoryTest(context.Background(), datadir, "--from-block", "7", "--to-block", "8", "--max-rows", "1")
	if err == nil || report.Complete || report.StopReason != "rows_budget" || report.Rows != 1 {
		t.Fatalf("partial CLI must print JSON and fail: %+v %v\n%s", report, err, output)
	}
	if !reflect.DeepEqual(before, historyInspectionFileHashes(t, datadir)) {
		t.Fatal("partial inspection changed source")
	}
}

func TestDBInspectHistoryPrevRefusesMissingDatabaseAndActiveLock(t *testing.T) {
	t.Run("missing", func(t *testing.T) {
		datadir := t.TempDir()
		_, _, err := runDBInspectHistoryTest(context.Background(), datadir, "--from-block", "0", "--to-block", "1")
		if err == nil {
			t.Fatal("missing database accepted")
		}
		if _, err := os.Stat(chainDataDir(datadir)); !os.IsNotExist(err) {
			t.Fatalf("missing database was created: %v", err)
		}
	})
	t.Run("locked", func(t *testing.T) {
		datadir := t.TempDir()
		db, err := rawdb.NewPebbleDB(chainDataDir(datadir), 16, 16)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = db.Close() })
		_, _, err = runDBInspectHistoryTest(context.Background(), datadir, "--from-block", "0", "--to-block", "1")
		if err == nil || !strings.Contains(err.Error(), "stop gtron") {
			t.Fatalf("online inspection must refuse lock: %v", err)
		}
	})
}

func TestDBInspectHistoryPrevExplicitRangeAndHardLimits(t *testing.T) {
	for _, args := range [][]string{
		{}, {"--from-block", "0"}, {"--to-block", "1"},
		{"--from-block", "2", "--to-block", "1"},
		{"--from-block", "0", "--to-block", "1", "--samples", "4097"},
		{"--from-block", "0", "--to-block", "1", "--max-decoded-bytes", "4294967297"},
		{"--from-block", "0", "--to-block", "1", "--max-duration", "6m"},
	} {
		datadir := t.TempDir()
		if _, _, err := runDBInspectHistoryTest(context.Background(), datadir, args...); err == nil {
			t.Fatalf("invalid flags accepted: %v", args)
		}
		if _, err := os.Stat(chainDataDir(datadir)); !os.IsNotExist(err) {
			t.Fatal("opened database before validating flags")
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, _, err := runDBInspectHistoryTest(ctx, t.TempDir(), "--from-block", "0", "--to-block", "1"); err == nil {
		t.Fatal("cancelled invocation accepted")
	}
}

func TestDBInspectHistoryPrevExportCompletePacksOnly(t *testing.T) {
	datadir := t.TempDir()
	db, err := rawdb.NewPebbleDB(chainDataDir(datadir), 16, 16)
	if err != nil {
		t.Fatal(err)
	}
	for _, block := range []uint64{7, 8} {
		var rows []*rawdb.StateDomainChange
		for seq := uint64(1); seq <= 2; seq++ {
			rows = append(rows, &rawdb.StateDomainChange{BlockNum: block, Seq: seq, TxNum: 100 + seq,
				FlatDomain: rawdb.StateFlatDomainKVLatest, Owner: common.Address{0x41, 1}, Generation: 1, Domain: kvdomains.SystemDelegation,
				Key: []byte("drax-test"), PrevExists: true, Prev: bytes.Repeat([]byte{0xf1}, 1024)})
		}
		if err := rawdb.WriteStateDomainChangeBlockRows(db, rows); err != nil {
			_ = db.Close()
			t.Fatal(err)
		}
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	before := historyInspectionFileHashes(t, datadir)
	for _, tc := range []struct {
		name     string
		rows     string
		entries  int
		complete bool
	}{
		{"complete", "4", 2, true}, {"partial", "3", 1, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			export := filepath.Join(t.TempDir(), "packs")
			report, output, err := runDBInspectHistoryTest(context.Background(), datadir, "--from-block", "7", "--to-block", "8", "--max-rows", tc.rows, "--export-packs", export)
			if (err == nil) != tc.complete || report.Complete != tc.complete {
				t.Fatalf("export contract: %v\n%s", err, output)
			}
			info, err := os.Stat(export)
			if err != nil || info.Mode().Perm() != 0700 {
				t.Fatalf("export directory is not private: %v", err)
			}
			data, err := os.ReadFile(filepath.Join(export, "manifest.json"))
			if err != nil {
				t.Fatal(err)
			}
			var manifest historyPackExportManifest
			if err := json.Unmarshal(data, &manifest); err != nil {
				t.Fatal(err)
			}
			if manifest.Complete != tc.complete || len(manifest.Entries) != tc.entries || manifest.StopReason != report.StopReason {
				t.Fatalf("manifest/report mismatch: %+v", manifest)
			}
			for _, entry := range manifest.Entries {
				pack, err := os.ReadFile(filepath.Join(export, entry.File))
				if err != nil {
					t.Fatal(err)
				}
				digest := sha256.Sum256(pack)
				if hex.EncodeToString(digest[:]) != entry.SHA256 || uint64(len(pack)) != entry.EncodedBytes {
					t.Fatal("export checksum/length mismatch")
				}
				info, err := os.Stat(filepath.Join(export, entry.File))
				if err != nil || info.Mode().Perm() != 0600 {
					t.Fatalf("pack is not private: %v", err)
				}
			}
			entries, err := os.ReadDir(export)
			if err != nil {
				t.Fatal(err)
			}
			if len(entries) != tc.entries+1 {
				t.Fatal("exported an incomplete pack")
			}
			if !reflect.DeepEqual(before, historyInspectionFileHashes(t, datadir)) {
				t.Fatal("export changed database")
			}
			// A second run cannot overwrite or reuse the existing directory.
			exportBefore := historyInspectionFileHashes(t, export)
			if _, _, err := runDBInspectHistoryTest(context.Background(), datadir, "--from-block", "7", "--to-block", "8", "--export-packs", export); err == nil {
				t.Fatal("existing directory accepted")
			}
			if !reflect.DeepEqual(exportBefore, historyInspectionFileHashes(t, export)) {
				t.Fatal("existing export overwritten")
			}
		})
	}
	for _, destination := range []string{chainDataDir(datadir), filepath.Join(chainDataDir(datadir), "diagnostic-packs")} {
		if _, _, err := runDBInspectHistoryTest(context.Background(), datadir, "--from-block", "7", "--to-block", "8", "--export-packs", destination); err == nil {
			t.Fatal("export within database accepted")
		}
	}
	if !reflect.DeepEqual(before, historyInspectionFileHashes(t, datadir)) {
		t.Fatal("rejected export modified database")
	}
}
