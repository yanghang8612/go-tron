package main

import (
	"crypto/sha256"
	"encoding/binary"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/types"
	corepb "github.com/tronprotocol/go-tron/proto/core"
)

func databaseFileHashes(t *testing.T, path string) map[string][32]byte {
	t.Helper()
	entries, err := os.ReadDir(path)
	if err != nil {
		t.Fatal(err)
	}
	out := make(map[string][32]byte)
	for _, entry := range entries {
		if entry.IsDir() || entry.Name() == "LOCK" {
			continue
		}
		data, err := os.ReadFile(filepath.Join(path, entry.Name()))
		if err != nil {
			t.Fatal(err)
		}
		out[entry.Name()] = sha256.Sum256(data)
	}
	return out
}

func TestStatusReadOnlyAndMissingStore(t *testing.T) {
	path := filepath.Join(t.TempDir(), "chaindata")
	if _, err := readStatus(path, "", false, false); err == nil {
		t.Fatal("missing store accepted")
	}
	db, err := rawdb.NewPebbleDB(path, 16, 32)
	if err != nil {
		t.Fatal(err)
	}
	want := common.HexToHash("0x1234")
	rawdb.WriteHeadBlockHash(db, want)
	rawdb.WriteHeadSolidBlockHash(db, want)
	if err := rawdb.WriteStageProgressWithHash(db, rawdb.StageFinish, 7, want); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	before, err := os.ReadDir(path)
	if err != nil {
		t.Fatal(err)
	}
	report, err := readStatus(path, "", false, false)
	if err != nil {
		t.Fatal(err)
	}
	if report.Guard.HeadHash.Value != want.Hex() || report.Guard.SolidHash.Value != want.Hex() || !report.Guard.Finish.Present || report.HotInspection != nil {
		t.Fatalf("unexpected quick status: %+v", report)
	}
	after, err := os.ReadDir(path)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(before, after) {
		t.Fatal("quick status changed store files")
	}
	if _, err := readStatus(path, path, false, false); err == nil {
		t.Fatal("same hot and stage directory accepted")
	}
}

func seedCompactStore(t *testing.T, path string) {
	t.Helper()
	db, err := rawdb.NewPebbleDB(path, 16, 32)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	block := types.NewBlockFromPB(&corepb.Block{BlockHeader: &corepb.BlockHeader{RawData: &corepb.BlockHeaderRaw{Number: 1, Timestamp: 1}}})
	if err := rawdb.WriteBlock(db, block); err != nil {
		t.Fatal(err)
	}
	h := block.Hash()
	rawdb.WriteHeadBlockHash(db, h)
	rawdb.WriteHeadSolidBlockHash(db, h)
	rawdb.WriteDynamicProperty(db, "latest_block_header_hash", h.Bytes())
	var n [8]byte
	binary.BigEndian.PutUint64(n[:], 1)
	rawdb.WriteDynamicProperty(db, "latest_block_header_number", n[:])
	rawdb.WriteDynamicProperty(db, "latest_solidified_block_num", n[:])
	for _, stage := range []rawdb.StageID{rawdb.StageExecution, rawdb.StageFinish, rawdb.StageStateHistoryIndex, rawdb.StageCommitment} {
		if err := rawdb.WriteStageProgressWithHash(db, stage, 1, h); err != nil {
			t.Fatal(err)
		}
	}
	if err := rawdb.WriteLatestDomainCommitmentRoot(db, h); err != nil {
		t.Fatal(err)
	}
	if err := rawdb.WriteCommitmentEngineState(db, []byte("fixture engine")); err != nil {
		t.Fatal(err)
	}
	if err := rawdb.WriteStateAccountLatest(db, common.Address{}, []byte("fixture account")); err != nil {
		t.Fatal(err)
	}
}

func TestCompactDryRunAndPreservingAll(t *testing.T) {
	path := filepath.Join(t.TempDir(), "chaindata")
	seedCompactStore(t, path)
	args := []string{"--hot-dir", path, "--range", "all", "--min-free-gib", "1", "--max-sst-write-gib", "1"}
	before := databaseFileHashes(t, path)
	if err := runCompact(args); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(before, databaseFileHashes(t, path)) {
		t.Fatal("dry-run modified database files")
	}
	if err := runCompact(append(append([]string{}, args...), "--yes")); err == nil {
		t.Fatal("write-open without explicit WAL permission was accepted")
	}
	if err := runCompact(append(append([]string{}, args...), "--yes", "--allow-wal-replay")); err != nil {
		t.Fatal(err)
	}
	report, err := readStatus(path, "", false, false)
	if err != nil || report.Guard.HeadHash.Value == "" || report.Guard.LatestCommitmentRoot.Value == "" {
		t.Fatalf("post compact status: %+v %v", report, err)
	}
}

func TestCompactRejectsChangedBoundaryBeforeWrite(t *testing.T) {
	path := filepath.Join(t.TempDir(), "chaindata")
	seedCompactStore(t, path)
	db, err := rawdb.NewPebbleDB(path, 16, 32)
	if err != nil {
		t.Fatal(err)
	}
	if err := rawdb.WriteStageProgress(db, rawdb.StageFinish, 1); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	before := databaseFileHashes(t, path)
	if err := runCompact([]string{"--hot-dir", path, "--range", "all", "--yes", "--allow-wal-replay", "--min-free-gib", "1", "--max-sst-write-gib", "1"}); err == nil {
		t.Fatal("unbound Finish was accepted")
	}
	if !reflect.DeepEqual(before, databaseFileHashes(t, path)) {
		t.Fatal("rejected compaction changed files")
	}
}

func TestCompactRejectsAncientOnlyHotAnchorBeforeWrite(t *testing.T) {
	path := filepath.Join(t.TempDir(), "chaindata")
	seedCompactStore(t, path)
	db, err := rawdb.NewPebbleDB(path, 16, 32)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Delete(rawdb.BlockStorageKey(1)); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	before := databaseFileHashes(t, path)
	if err := runCompact([]string{"--hot-dir", path, "--range", "all", "--yes", "--allow-wal-replay", "--min-free-gib", "1", "--max-sst-write-gib", "1"}); err == nil {
		t.Fatal("missing hot canonical body was accepted")
	}
	if !reflect.DeepEqual(before, databaseFileHashes(t, path)) {
		t.Fatal("failed hot anchor preflight modified files")
	}
}
