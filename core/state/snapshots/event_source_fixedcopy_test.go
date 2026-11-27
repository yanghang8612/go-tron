package snapshots

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"hash"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/tronprotocol/go-tron/core/rawdb"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	"google.golang.org/protobuf/proto"
)

// Explicit opt-in. The source must be an offline, consistent chaindata copy,
// never the live DB. No StateDB, V3 HEAD, source catalog or source DB is opened
// for writing. The only writes are new V5 event files and in-memory stages.
//
// GTRON_EVENT_PERF_CHAINDATA=/captured/chaindata
// GTRON_EVENT_PERF_OUT=/new/output (must not exist)
// GTRON_EVENT_PERF_FROM=52625408 GTRON_EVENT_PERF_TO=52690943
// go test ./core/state/snapshots -run '^TestIndependentEventFixedCopy$' -v -count=1 -timeout=30m
func TestIndependentEventFixedCopy(t *testing.T) {
	sourcePath := os.Getenv("GTRON_EVENT_PERF_CHAINDATA")
	if sourcePath == "" {
		t.Skip("set GTRON_EVENT_PERF_CHAINDATA for the bounded fixed-copy experiment")
	}
	out := os.Getenv("GTRON_EVENT_PERF_OUT")
	if out == "" {
		t.Fatal("GTRON_EVENT_PERF_OUT is required")
	}
	from, err := strconv.ParseUint(os.Getenv("GTRON_EVENT_PERF_FROM"), 10, 64)
	if err != nil || from == 0 {
		t.Fatal("positive FROM required", err)
	}
	to, err := strconv.ParseUint(os.Getenv("GTRON_EVENT_PERF_TO"), 10, 64)
	if err != nil || to < from || to-from >= 65536 {
		t.Fatal("TO must bound at most one 65536-block segment", err)
	}
	sourcePath, err = filepath.EvalSymlinks(sourcePath)
	if err != nil {
		t.Fatal(err)
	}
	sourcePath, _ = filepath.Abs(sourcePath)
	out, err = filepath.Abs(out)
	if err != nil {
		t.Fatal(err)
	}
	// Keep all experiment output outside the captured tree, including symlinked
	// parent aliases. Existing paths are refused rather than overwritten.
	parent, err := filepath.EvalSymlinks(filepath.Dir(out))
	if err != nil {
		t.Fatal(err)
	}
	out = filepath.Join(parent, filepath.Base(out))
	if out == sourcePath || strings.HasPrefix(out, sourcePath+string(os.PathSeparator)) || strings.HasPrefix(sourcePath, out+string(os.PathSeparator)) {
		t.Fatal("source/output overlap")
	}
	if err := os.Mkdir(out, 0700); err != nil {
		t.Fatal(err)
	}
	base, err := rawdb.NewPebbleDBReadOnly(sourcePath, 256, 256)
	if err != nil {
		t.Fatal(err)
	}
	defer base.Close()
	store := &eventPerfOverlay{KeyValueStore: base, stages: make(map[string][]byte)}
	chainDB := rawdb.NewChainDB(store, nil)
	properties, err := rawdb.ReadStateExecutionProperties(chainDB)
	if err != nil || properties.SolidifiedBlockNum < 0 {
		t.Fatal(properties, err)
	}
	chain := &coldBuilderChain{db: chainDB, solidified: properties.SolidifiedBlockNum}
	r := &Runner{chain: chain}
	cap, err := r.independentEventSourceCap(chainDB)
	if err != nil {
		t.Fatal(err)
	}
	if cap < to {
		t.Fatalf("requested end %d is beyond hash-verified Finish/solid %d", to, cap)
	}
	finish, _, err := rawdb.ReadStageProgressRow(chainDB, rawdb.StageFinish)
	if err != nil {
		t.Fatal(err)
	}
	old, oldPresent, err := rawdb.ReadStageProgressRow(chainDB, rawdb.StageSnapshotBuild)
	if err != nil {
		t.Fatal(err)
	}
	oldVerified, oldOK, oldErr := r.verifiedSnapshotBuildStageBlock(chainDB)
	oldError := ""
	if oldErr != nil {
		oldError = oldErr.Error()
	}
	anchor, err := r.snapshotBuildStageBoundaryHash(chainDB, rawdb.StageSnapshotEventLogBuild, to)
	if err != nil {
		t.Fatal(err)
	}
	report := map[string]any{
		"started_utc": time.Now().UTC().Format(time.RFC3339Nano), "source_read_only": true, "state_db_opened": false,
		"source_chaindata": sourcePath, "output": out, "from_block": from, "to_block": to, "blocks": to - from + 1,
		"source_solid": properties.SolidifiedBlockNum, "source_finish": finish, "source_snapshot_build": old, "source_snapshot_build_present": oldPresent,
		"old_stage_verified": oldOK && oldErr == nil, "old_stage_verification_error": oldError,
		"old_verified_cap": oldVerified, "old_stored_boundary_reaches_requested_end": oldPresent && old.HasBlockHash && old.BlockNum >= to,
		"old_verified_frontier_allows_full_requested_range": oldOK && oldErr == nil && oldVerified >= to,
		"independent_verified_cap":                          cap, "independent_allows_full_requested_range": true, "source_terminal_hash": anchor.Hex(),
		"method_limit": "Actual same-source event build and full requested-range coverage; existing prefix files/ancient are not captured, so this is not complete lifecycle throughput or equal-work speedup.",
	}
	started, cpu := time.Now(), eventPerfCPU()
	built, err := NewAggregator(out).BuildEventLogsWithBuildOptions(chainDB, from, to, EventLogBuildOptions{Version: EventLogSegmentV4Version})
	if err != nil {
		t.Fatal(err)
	}
	report["build_wall_ns"], report["build_process_cpu_ns"] = time.Since(started).Nanoseconds(), eventPerfCPU()-cpu
	if err := r.verifyIndependentEventSource(chainDB, coldSidecarBlockRange{from: from, to: to}, anchor); err != nil {
		t.Fatal(err)
	}
	manager, err := OpenManager(out)
	if err != nil {
		t.Fatal(err)
	}
	manager.SetEventLogCanonicalSource(chain)
	covered, err := manager.EventLogIndexedRangeCovered(from, to)
	if err != nil || !covered {
		t.Fatal("actual direct-V2 required range not covered", covered, err)
	}
	report["required_range_indexed_covered"] = covered
	report["range_is_complete_65536_block_segment"] = from%65536 == 0 && to-from+1 == 65536
	frontier, hasFrontier := eventLogBuildBlockFromManifest(built.Manifest)
	report["whole_prefix_frontier"], report["whole_prefix_covered"] = frontier, hasFrontier
	started, cpu = time.Now(), eventPerfCPU()
	directDigest, directRows, err := eventPerfDigest(func(fn func(EventLog) (bool, error)) error {
		return (eventLogV3ChainReader{chain: chainDB}).IterateEventLogs(from, to, EventLogFilter{}, fn)
	})
	if err != nil {
		t.Fatal(err)
	}
	report["source_query_wall_ns"], report["source_query_process_cpu_ns"] = time.Since(started).Nanoseconds(), eventPerfCPU()-cpu
	started, cpu = time.Now(), eventPerfCPU()
	coldDigest, coldRows, err := eventPerfDigest(func(fn func(EventLog) (bool, error)) error {
		return manager.IterateEventLogs(from, to, EventLogFilter{}, fn)
	})
	if err != nil || directDigest != coldDigest || directRows != coldRows {
		t.Fatal("full-range source/cold log oracle mismatch", directRows, coldRows, err)
	}
	report["cold_query_wall_ns"], report["cold_query_process_cpu_ns"] = time.Since(started).Nanoseconds(), eventPerfCPU()-cpu
	report["source_log_digest"], report["cold_log_digest"], report["log_rows"], report["all_return_values_equal"] = directDigest, coldDigest, coldRows, true
	report["segments"] = built.Segments
	var fileBytes uint64
	for _, ref := range built.Segments {
		fileBytes += ref.Size
	}
	report["event_pair_bytes"], report["finished_utc"] = fileBytes, time.Now().UTC().Format(time.RFC3339Nano)
	data, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(out, "result.json"), append(data, '\n'), 0600); err != nil {
		t.Fatal(err)
	}
	t.Logf("EVENT_FIXED_COPY_OK output=%s blocks=%d rows=%d bytes=%d", out, to-from+1, coldRows, fileBytes)
}

type eventPerfOverlay struct {
	ethdb.KeyValueStore
	stages map[string][]byte
}

func (s *eventPerfOverlay) Get(k []byte) ([]byte, error) {
	if v, ok := s.stages[string(k)]; ok {
		return bytes.Clone(v), nil
	}
	return s.KeyValueStore.Get(k)
}
func (s *eventPerfOverlay) Has(k []byte) (bool, error) {
	if _, ok := s.stages[string(k)]; ok {
		return true, nil
	}
	return s.KeyValueStore.Has(k)
}
func (s *eventPerfOverlay) Put(k, v []byte) error { s.stages[string(k)] = bytes.Clone(v); return nil }
func (s *eventPerfOverlay) Delete(k []byte) error {
	return fmt.Errorf("fixed-copy experiment forbids delete %x", k)
}
func (s *eventPerfOverlay) NewBatch() ethdb.Batch {
	return &eventPerfBatch{Batch: rawdb.NewMemoryDatabase().NewBatch(), target: s}
}
func (s *eventPerfOverlay) NewBatchWithSize(int) ethdb.Batch { return s.NewBatch() }

type eventPerfBatch struct {
	ethdb.Batch
	target *eventPerfOverlay
}

func (b *eventPerfBatch) Write() error { return b.Batch.Replay(b.target) }

func eventPerfCPU() int64 {
	var r syscall.Rusage
	if syscall.Getrusage(syscall.RUSAGE_SELF, &r) != nil {
		return 0
	}
	return (r.Utime.Sec+r.Stime.Sec)*1e9 + int64(r.Utime.Usec+r.Stime.Usec)*1000
}
func eventPerfDigest(iterate func(func(EventLog) (bool, error)) error) (string, uint64, error) {
	h := sha256.New()
	var rows uint64
	err := iterate(func(row EventLog) (bool, error) {
		for _, n := range []uint64{row.BlockNum, row.TxIndex, row.LogIndex} {
			var b [8]byte
			binary.BigEndian.PutUint64(b[:], n)
			h.Write(b[:])
		}
		h.Write(row.BlockHash[:])
		h.Write(row.TxHash[:])
		h.Write(row.Address[:])
		raw, err := proto.Marshal(row.Log)
		if err != nil {
			return false, err
		}
		eventPerfHashBytes(h, raw)
		rows++
		return true, nil
	})
	return hex.EncodeToString(h.Sum(nil)), rows, err
}
func eventPerfHashBytes(h hash.Hash, b []byte) {
	var n [8]byte
	binary.BigEndian.PutUint64(n[:], uint64(len(b)))
	h.Write(n[:])
	h.Write(b)
}

func TestIndependentEventFixedCopySmoke(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "source")
	db, err := rawdb.NewPebbleDB(dbPath, 16, 16)
	if err != nil {
		t.Fatal(err)
	}
	chainDB := rawdb.NewChainDB(db, nil)
	for n := uint64(1); n <= 4; n++ {
		var logs []*corepb.TransactionInfo_Log
		if n < 4 {
			logs = []*corepb.TransactionInfo_Log{{Address: eventLogTestAddress(0x41), Data: []byte{byte(n)}}}
		}
		block, infos := coldBuilderEventLogBlock(t, n, logs)
		if err := rawdb.WriteBlock(chainDB, block); err != nil {
			t.Fatal(err)
		}
		if err := rawdb.WriteTransactionInfosByBlock(chainDB, n, infos); err != nil {
			t.Fatal(err)
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
			if err := rawdb.WriteStateExecutionProperties(db, rawdb.StateExecutionProperties{BlockNum: 4, BlockHash: block.Hash(), Timestamp: block.Timestamp(), SolidifiedBlockNum: 4}); err != nil {
				t.Fatal(err)
			}
		}
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	out := filepath.Join(dir, "output")
	t.Setenv("GTRON_EVENT_PERF_CHAINDATA", dbPath)
	t.Setenv("GTRON_EVENT_PERF_OUT", out)
	t.Setenv("GTRON_EVENT_PERF_FROM", "2")
	t.Setenv("GTRON_EVENT_PERF_TO", "4")
	TestIndependentEventFixedCopy(t)
	data, err := os.ReadFile(filepath.Join(out, "result.json"))
	if err != nil {
		t.Fatal(err)
	}
	var report map[string]any
	if err := json.Unmarshal(data, &report); err != nil {
		t.Fatal(err)
	}
	if report["all_return_values_equal"] != true || report["old_verified_frontier_allows_full_requested_range"] != false || report["independent_allows_full_requested_range"] != true {
		t.Fatal(string(data))
	}
	// The experiment's stage publication was an overlay write only.
	check, err := rawdb.NewPebbleDBReadOnly(dbPath, 16, 16)
	if err != nil {
		t.Fatal(err)
	}
	defer check.Close()
	if _, ok, err := rawdb.ReadStageProgress(check, rawdb.StageSnapshotEventLogBuild); err != nil || ok {
		t.Fatal("experiment wrote source stage", ok, err)
	}
}
