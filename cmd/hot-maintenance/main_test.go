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

func seedCompactStore(t *testing.T, path string, optionalFields bool) {
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
	if optionalFields {
		rawdb.WriteHeadSolidBlockHash(db, h)
	}
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
	if optionalFields {
		if err := rawdb.WriteCommitmentEngineState(db, []byte("fixture engine")); err != nil {
			t.Fatal(err)
		}
	}
	if err := rawdb.WriteStateAccountLatest(db, common.Address{}, []byte("fixture account")); err != nil {
		t.Fatal(err)
	}
}

func TestCompactDryRunAndPreservingAll(t *testing.T) {
	path := filepath.Join(t.TempDir(), "chaindata")
	seedCompactStore(t, path, true)
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
	seedCompactStore(t, path, true)
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
	seedCompactStore(t, path, true)
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

func TestCompactAcceptsAbsentOptionalMetadata(t *testing.T) {
	path := filepath.Join(t.TempDir(), "chaindata")
	seedCompactStore(t, path, false)
	args := []string{"--hot-dir", path, "--range", "all", "--min-free-gib", "1", "--max-sst-write-gib", "1"}
	if err := runCompact(args); err != nil {
		t.Fatalf("dry-run with absent optional fields: %v", err)
	}
	if err := runCompact(append(append([]string{}, args...), "--yes", "--allow-wal-replay")); err != nil {
		t.Fatalf("compact with absent optional fields: %v", err)
	}
	report, err := readStatus(path, "", false, false)
	if err != nil || report.Guard.SolidHash.Present || report.Guard.EngineStateSHA256.Present || !report.Guard.LatestCommitmentRoot.Present {
		t.Fatalf("optional presence after reopen: %+v %v", report.Guard, err)
	}
}

func TestCompactRejectsIncorrectStoredSolidHash(t *testing.T) {
	path := filepath.Join(t.TempDir(), "chaindata")
	seedCompactStore(t, path, true)
	db, err := rawdb.NewPebbleDB(path, 16, 32)
	if err != nil {
		t.Fatal(err)
	}
	rawdb.WriteHeadSolidBlockHash(db, common.HexToHash("0x9876"))
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	before := databaseFileHashes(t, path)
	if err := runCompact([]string{"--hot-dir", path, "--range", "all", "--yes", "--allow-wal-replay", "--min-free-gib", "1", "--max-sst-write-gib", "1"}); err == nil {
		t.Fatal("incorrect stored solid hash accepted")
	}
	if !reflect.DeepEqual(before, databaseFileHashes(t, path)) {
		t.Fatal("failed solid mismatch preflight modified files")
	}
}

func TestProtectedOptionalPresenceCannotChange(t *testing.T) {
	before := protectedState{Guard: guard{
		SolidHash:         field[string]{Present: false},
		EngineStateSHA256: field[string]{Present: false},
	}}
	if err := verifyProtectedUnchanged(before, before); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name   string
		mutate func(*protectedState)
	}{
		{"solid appeared", func(s *protectedState) { s.Guard.SolidHash = field[string]{Present: true, Value: "0x1234"} }},
		{"engine appeared", func(s *protectedState) { s.Guard.EngineStateSHA256 = field[string]{Present: true, Value: "abcd"} }},
		{"solid disappeared", func(s *protectedState) { s.Guard.SolidHash = field[string]{} }},
		{"engine disappeared", func(s *protectedState) { s.Guard.EngineStateSHA256 = field[string]{} }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			start := before
			if tc.name == "solid disappeared" {
				start.Guard.SolidHash = field[string]{Present: true, Value: "0x1234"}
			}
			if tc.name == "engine disappeared" {
				start.Guard.EngineStateSHA256 = field[string]{Present: true, Value: "abcd"}
			}
			end := start
			tc.mutate(&end)
			if err := verifyProtectedUnchanged(start, end); err == nil {
				t.Fatal("optional field presence change was accepted")
			}
		})
	}
}

func TestRouteCensusColdIntervalsAndMissingRoute(t *testing.T) {
	c := routeCensus{HeadBucket: 6, nextNonColdCandidate: 1}
	for _, row := range []struct {
		bucket uint64
		cold   bool
	}{{0, true}, {1, true}, {2, true}, {4, true}, {5, false}} {
		c.observeColdRoute(row.bucket, row.cold)
	}
	if c.FirstNonColdFromBucketOne == nil || *c.FirstNonColdFromBucketOne != 3 {
		t.Fatalf("missing route bucket 3 must be first non-COLD: %+v", c)
	}
	want := []coldBucketInterval{
		{StartBucket: 1, EndBucket: 2, StartBlock: rawdb.StateHistoryChunkBucketBlocks, EndBlock: 3*rawdb.StateHistoryChunkBucketBlocks - 1},
		{StartBucket: 4, EndBucket: 4, StartBlock: 4 * rawdb.StateHistoryChunkBucketBlocks, EndBlock: 5*rawdb.StateHistoryChunkBucketBlocks - 1},
	}
	if !reflect.DeepEqual(c.ColdIntervals, want) {
		t.Fatalf("intervals %+v, want %+v", c.ColdIntervals, want)
	}
	stale := routeCensus{HeadBucket: 2, nextNonColdCandidate: 1}
	stale.observeColdRoute(1, false) // An old-epoch COLD route is not current COLD.
	if stale.FirstNonColdFromBucketOne == nil || *stale.FirstNonColdFromBucketOne != 1 {
		t.Fatalf("old-epoch route misread as COLD: %+v", stale)
	}
}
