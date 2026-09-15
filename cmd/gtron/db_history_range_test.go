package main

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/state/kvdomains"
	"github.com/tronprotocol/go-tron/core/types"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	"github.com/urfave/cli/v2"
)

func historyRangeTestFixture(t *testing.T) string {
	t.Helper()
	datadir := t.TempDir()
	db, err := rawdb.NewPebbleDB(chainDataDir(datadir), 16, 16)
	if err != nil {
		t.Fatal(err)
	}
	var parent common.Hash
	for n := uint64(1); n <= 3; n++ {
		block := types.NewBlockFromPB(&corepb.Block{BlockHeader: &corepb.BlockHeader{RawData: &corepb.BlockHeaderRaw{Number: int64(n), Timestamp: int64(n * 3000), ParentHash: parent.Bytes()}}})
		parent = block.Hash()
		if err := rawdb.WriteBlock(db, block); err != nil {
			t.Fatal(err)
		}
		if err := rawdb.WriteStateTxRange(db, n, block.Hash(), n, n); err != nil {
			t.Fatal(err)
		}
		rows := []*rawdb.StateDomainChange{{BlockNum: n, Seq: 1, TxNum: n, FlatDomain: rawdb.StateFlatDomainKVLatest, Domain: kvdomains.SystemDelegation, Key: []byte("key"), PrevExists: true, Prev: []byte("PRIVATE_PREV_NOT_IN_JSON")}}
		if err := rawdb.WriteStateDomainChangeBlockRows(db, rows); err != nil {
			t.Fatal(err)
		}
		if n == 1 {
			if err := rawdb.WriteStageProgressWithHash(db, rawdb.StageSnapshotBuild, n, block.Hash()); err != nil {
				t.Fatal(err)
			}
		}
		if n == 3 {
			if err := rawdb.WriteStageProgressWithHash(db, rawdb.StageFinish, n, block.Hash()); err != nil {
				t.Fatal(err)
			}
		}
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	return datadir
}

func runHistoryRangeTest(ctx context.Context, datadir, output string, extra ...string) (historyRangeExportManifest, string, error) {
	var out bytes.Buffer
	app := &cli.App{Writer: &out, ErrWriter: &out, Commands: []*cli.Command{dbCommand()}}
	args := []string{"gtron", "db", "export-history-range", "--datadir", datadir, "--db.cache", "16", "--db.handles", "16", "--output-dir", output, "--blocks", "2"}
	err := app.RunContext(ctx, append(args, extra...))
	var manifest historyRangeExportManifest
	if decodeErr := json.Unmarshal(out.Bytes(), &manifest); decodeErr != nil && err == nil {
		err = decodeErr
	}
	return manifest, out.String(), err
}

func TestDBHistoryRangeExportReadOnlyAndPartial(t *testing.T) {
	source := historyRangeTestFixture(t)
	before := historyInspectionFileHashes(t, source)
	output := filepath.Join(t.TempDir(), "range")
	m, text, err := runHistoryRangeTest(context.Background(), source, output)
	if err != nil || !m.Export.Complete || m.Export.FromBlock != 2 || m.Export.ToBlock != 3 {
		t.Fatalf("export: %+v %v %s", m, err, text)
	}
	if strings.Contains(text, "PRIVATE_PREV_NOT_IN_JSON") {
		t.Fatal("export report exposed values")
	}
	if !reflect.DeepEqual(before, historyInspectionFileHashes(t, source)) {
		t.Fatal("export modified source")
	}
	copied, err := rawdb.NewPebbleDBReadOnly(filepath.Join(output, "pebble"), 16, 16)
	if err != nil {
		t.Fatal(err)
	}
	view, release, err := rawdb.AcquireStateHistoryReadView(copied)
	if err != nil {
		t.Fatal(err)
	}
	var rows int
	err = rawdb.IterateStateDomainChangesByBlockTxRangeBorrowed(view, 2, 3, 2, 3, func(row *rawdb.StateDomainChange) (bool, error) {
		rows++
		if string(row.Prev) != "PRIVATE_PREV_NOT_IN_JSON" {
			t.Error("source value changed")
		}
		return true, nil
	})
	if err != nil || rows != 2 {
		t.Fatalf("copy: rows=%d err=%v", rows, err)
	}
	if err := release(); err != nil {
		t.Fatal(err)
	}
	if err := copied.Close(); err != nil {
		t.Fatal(err)
	}
	partial := filepath.Join(t.TempDir(), "partial")
	m, _, err = runHistoryRangeTest(context.Background(), source, partial, "--max-bytes", "1")
	if err == nil || m.Export.Complete {
		t.Fatalf("partial falsely complete: %+v %v", m, err)
	}
	data, err := os.ReadFile(filepath.Join(partial, "manifest.json"))
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(data, &m); err != nil || m.Export.Complete {
		t.Fatalf("partial manifest: %s %v", data, err)
	}
	if !reflect.DeepEqual(before, historyInspectionFileHashes(t, source)) {
		t.Fatal("partial export modified source")
	}
}

func TestDBHistoryRangeExportBoundariesAndActiveLock(t *testing.T) {
	source := historyRangeTestFixture(t)
	before := historyInspectionFileHashes(t, source)
	for _, args := range [][]string{{"--blocks", "0"}, {"--blocks", "257"}, {"--from-block", "1"}, {"--from-block", "3"}, {"--max-bytes", "1073741825"}, {"--max-duration", "6m"}} {
		if _, _, err := runHistoryRangeTest(context.Background(), source, filepath.Join(t.TempDir(), "bad"), args...); err == nil {
			t.Fatalf("accepted %v", args)
		}
	}
	if _, _, err := runHistoryRangeTest(context.Background(), source, filepath.Join(source, "unsafe")); err == nil {
		t.Fatal("export accepted production datadir")
	}
	if !reflect.DeepEqual(before, historyInspectionFileHashes(t, source)) {
		t.Fatal("invalid export modified source")
	}
	locked, err := rawdb.NewPebbleDB(chainDataDir(source), 16, 16)
	if err != nil {
		t.Fatal(err)
	}
	_, _, err = runHistoryRangeTest(context.Background(), source, filepath.Join(t.TempDir(), "locked"))
	if err == nil || !strings.Contains(err.Error(), "stop gtron") {
		t.Fatalf("active lock accepted: %v", err)
	}
	if err := locked.Close(); err != nil {
		t.Fatal(err)
	}
}
