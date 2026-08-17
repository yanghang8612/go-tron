package snapshots

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	coretypes "github.com/tronprotocol/go-tron/core/types"
	corepb "github.com/tronprotocol/go-tron/proto/core"
)

func TestEventLogQueryReusesVerifiedCompanionsWithoutScanningNonCandidates(t *testing.T) {
	dir := t.TempDir()
	db := rawdb.NewMemoryChainDB()
	matchAddress := eventLogTestAddress(0x71)
	otherAddress := eventLogTestAddress(0x72)
	matchTopic := common.Hash{0xe1}
	otherTopic := common.Hash{0xe2}
	block1, infos1 := eventLogTestBlock(t, 1, []*corepb.TransactionInfo_Log{{
		Address: matchAddress,
		Topics:  [][]byte{matchTopic[:]},
		Data:    []byte{0x11},
	}})
	block2, infos2 := eventLogTestBlock(t, 2, []*corepb.TransactionInfo_Log{{
		Address: otherAddress,
		Topics:  [][]byte{otherTopic[:]},
		Data:    []byte{0x22},
	}})
	for _, block := range []*coretypes.Block{block1, block2} {
		if err := rawdb.WriteBlock(db, block); err != nil {
			t.Fatalf("WriteBlock %d: %v", block.Number(), err)
		}
	}
	if err := rawdb.WriteTransactionInfosByBlock(db, 1, infos1); err != nil {
		t.Fatalf("WriteTransactionInfosByBlock 1: %v", err)
	}
	if err := rawdb.WriteTransactionInfosByBlock(db, 2, infos2); err != nil {
		t.Fatalf("WriteTransactionInfosByBlock 2: %v", err)
	}

	ref1, err := BuildEventLogSegmentFromChain(db, dir, "log/event-log-query-cache-1.seg", 1, 1)
	if err != nil {
		t.Fatalf("BuildEventLogSegmentFromChain 1: %v", err)
	}
	ref2, err := BuildEventLogSegmentFromChain(db, dir, "log/event-log-query-cache-2.seg", 2, 2)
	if err != nil {
		t.Fatalf("BuildEventLogSegmentFromChain 2: %v", err)
	}
	indexRef, err := BuildEventLogIndexSegmentFromEventLogSegments(dir, []SegmentRef{ref1, ref2}, "log/event-log-query-cache.idx")
	if err != nil {
		t.Fatalf("BuildEventLogIndexSegmentFromEventLogSegments: %v", err)
	}
	if err := PublishManifest(dir, NewManifest(0, 0, []SegmentRef{ref1, ref2, indexRef})); err != nil {
		t.Fatalf("PublishManifest: %v", err)
	}
	mgr, err := OpenManager(dir)
	if err != nil {
		t.Fatalf("OpenManager: %v", err)
	}
	filter := EventLogFilter{
		Addresses: []common.Address{common.BytesToAddress(matchAddress)},
		Topics:    [][]common.Hash{{matchTopic}},
	}
	collect := func(manager *Manager) []EventLog {
		t.Helper()
		var rows []EventLog
		if err := manager.IterateEventLogs(1, 2, filter, func(row EventLog) (bool, error) {
			rows = append(rows, row)
			return true, nil
		}); err != nil {
			t.Fatalf("IterateEventLogs: %v", err)
		}
		return rows
	}
	assertRows := func(rows []EventLog) {
		t.Helper()
		if len(rows) != 1 || rows[0].BlockNum != 1 || !bytes.Equal(rows[0].Log.GetData(), []byte{0x11}) {
			t.Fatalf("rows = %+v, want only block 1", rows)
		}
	}

	assertRows(collect(mgr))
	first := mgr.chainVerificationCache.Stats()
	if first.EventFullVerified != 1 || first.EventMemoryHits != 0 {
		t.Fatalf("first query verification stats = %+v, want one full proof", first)
	}
	restarted, err := OpenManager(dir)
	if err != nil {
		t.Fatalf("OpenManager after persisted proof: %v", err)
	}

	// An in-process memory hit must not reopen or audit a non-candidate event
	// segment. Mode changes leave the cache key intact while making an attempted
	// data read fail for the test process.
	nonCandidatePath := filepath.Join(dir, ref2.Path)
	if err := os.Chmod(nonCandidatePath, 0); err != nil {
		t.Fatalf("chmod non-candidate: %v", err)
	}
	defer os.Chmod(nonCandidatePath, 0o600)
	assertRows(collect(mgr))
	second := mgr.chainVerificationCache.Stats()
	if second.EventFullVerified != first.EventFullVerified {
		t.Fatalf("cached query repeated full verification: before=%+v after=%+v", first, second)
	}
	if second.EventMemoryHits <= first.EventMemoryHits {
		t.Fatalf("cached query did not hit memory proof: before=%+v after=%+v", first, second)
	}
	if err := os.Chmod(nonCandidatePath, 0o600); err != nil {
		t.Fatalf("restore non-candidate mode: %v", err)
	}
	assertRows(collect(restarted))
	if stats := restarted.chainVerificationCache.Stats(); stats.EventPersistentHits != 1 || stats.EventFullVerified != 0 {
		t.Fatalf("restart proof stats=%+v, want one identity-bound persistent hit", stats)
	}
}

func TestEventLogRangeCoveredReusesVerifiedCompanions(t *testing.T) {
	dir, _ := buildEventLogVerificationFixture(t, 2, 1)
	if err := os.Remove(filepath.Join(dir, chainFreezerVerificationCacheFile)); err != nil && !os.IsNotExist(err) {
		t.Fatal(err)
	}
	mgr, err := OpenManager(dir)
	if err != nil {
		t.Fatal(err)
	}
	covered, err := mgr.EventLogRangeCovered(1, 1)
	if err != nil || !covered {
		t.Fatalf("first EventLogRangeCovered = %v/%v, want true/nil", covered, err)
	}
	first := mgr.chainVerificationCache.Stats()
	if first.EventFullVerified != 1 {
		t.Fatalf("first coverage stats = %+v, want one full proof", first)
	}
	covered, err = mgr.EventLogRangeCovered(1, 1)
	if err != nil || !covered {
		t.Fatalf("second EventLogRangeCovered = %v/%v, want true/nil", covered, err)
	}
	second := mgr.chainVerificationCache.Stats()
	if second.EventFullVerified != first.EventFullVerified || second.EventMemoryHits <= first.EventMemoryHits {
		t.Fatalf("coverage did not reuse proof: first=%+v second=%+v", first, second)
	}
}

func TestEventLogPersistentProofRehashesSameIdentityCompanion(t *testing.T) {
	dir := t.TempDir()
	row := eventLogV3TestRow(
		1, 0, 0,
		common.BytesToAddress(eventLogTestAddress(0x73)),
		common.Hash{0x31}, common.Hash{0x41}, common.Hash{0xe3}, []byte{0x33},
	)
	ref, err := BuildEventLogV4SegmentFromReader(eventLogRowsReader{rows: []EventLog{row}}, dir, "log/event-log-same-identity.seg", 1, 1)
	if err != nil {
		t.Fatal(err)
	}
	indexRef, err := BuildEventLogIndexSegmentFromEventLogSegments(dir, []SegmentRef{ref}, "log/event-log-same-identity.idx")
	if err != nil {
		t.Fatal(err)
	}
	if err := PublishManifest(dir, NewManifest(0, 0, []SegmentRef{ref, indexRef})); err != nil {
		t.Fatal(err)
	}
	mgr, err := OpenManager(dir)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := mgr.IterateCoveredEventLogs(1, 1, EventLogFilter{}, func(EventLog) (bool, error) { return true, nil }); err != nil {
		t.Fatal(err)
	}
	if stats := mgr.chainVerificationCache.Stats(); stats.EventFullVerified != 1 {
		t.Fatalf("initial verification stats=%+v, want one full proof", stats)
	}

	path := filepath.Join(dir, ref.Path)
	before, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	file, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	header, err := readEventLogHeader(file)
	if err != nil {
		_ = file.Close()
		t.Fatal(err)
	}
	if header.v3 == nil {
		_ = file.Close()
		t.Fatal("fixture is not V3")
	}
	offset := int64(header.v3.txDictOffset + 8)
	var one [1]byte
	if _, err := file.ReadAt(one[:], offset); err != nil {
		_ = file.Close()
		t.Fatal(err)
	}
	one[0] ^= 0xff
	if _, err := file.WriteAt(one[:], offset); err != nil {
		_ = file.Close()
		t.Fatal(err)
	}
	if err := file.Sync(); err != nil {
		_ = file.Close()
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.Chtimes(path, before.ModTime(), before.ModTime()); err != nil {
		t.Fatal(err)
	}
	after, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if after.Size() != before.Size() || after.ModTime().UnixNano() != before.ModTime().UnixNano() {
		t.Fatalf("mutation changed identity: before=%d/%d after=%d/%d", before.Size(), before.ModTime().UnixNano(), after.Size(), after.ModTime().UnixNano())
	}

	restarted, err := OpenManager(dir)
	if err != nil {
		t.Fatal(err)
	}
	_, err = restarted.IterateCoveredEventLogs(1, 1, EventLogFilter{}, func(EventLog) (bool, error) { return true, nil })
	if err == nil {
		t.Fatal("persistent proof accepted same-size/same-mtime companion corruption")
	}
}

func TestEventLogQueryCachePersistenceFailureIsAdvisory(t *testing.T) {
	dir, _ := buildEventLogVerificationFixture(t, 2, 1)
	if err := os.Remove(filepath.Join(dir, chainFreezerVerificationCacheFile)); err != nil && !os.IsNotExist(err) {
		t.Fatal(err)
	}
	mgr, err := OpenManager(dir)
	if err != nil {
		t.Fatal(err)
	}
	badCacheDir := filepath.Join(dir, "not-a-directory")
	if err := os.WriteFile(badCacheDir, []byte("block cache writes"), 0o600); err != nil {
		t.Fatal(err)
	}
	mgr.chainVerificationCache.dir = badCacheDir

	var topic common.Hash
	topic[0] = 1
	filter := EventLogFilter{
		Addresses: []common.Address{common.BytesToAddress(eventLogTestAddress(1))},
		Topics:    [][]common.Hash{{topic}},
	}
	var rows []EventLog
	if err := mgr.IterateEventLogs(1, 1, filter, func(row EventLog) (bool, error) {
		rows = append(rows, row)
		return true, nil
	}); err != nil {
		t.Fatalf("advisory cache write made query fail: %v", err)
	}
	if len(rows) != 1 || rows[0].BlockNum != 1 {
		t.Fatalf("rows=%+v, want block 1", rows)
	}
	if stats := mgr.chainVerificationCache.Stats(); stats.EventPersistErrors != 1 {
		t.Fatalf("cache stats=%+v, want one advisory persist error", stats)
	}
}

func TestUnfilteredEventLogQueryReusesCompanionProof(t *testing.T) {
	dir, _ := buildEventLogVerificationFixture(t, 2, 1)
	if err := os.Remove(filepath.Join(dir, chainFreezerVerificationCacheFile)); err != nil && !os.IsNotExist(err) {
		t.Fatal(err)
	}
	mgr, err := OpenManager(dir)
	if err != nil {
		t.Fatal(err)
	}
	query := func() {
		t.Helper()
		var rows []EventLog
		covered, err := mgr.IterateCoveredEventLogs(1, 1, EventLogFilter{}, func(row EventLog) (bool, error) {
			rows = append(rows, row)
			return true, nil
		})
		if err != nil {
			t.Fatal(err)
		}
		if !covered {
			t.Fatal("event-log range is not covered")
		}
		if len(rows) != 1 || rows[0].BlockNum != 1 {
			t.Fatalf("rows=%+v, want block 1", rows)
		}
	}
	query()
	first := mgr.chainVerificationCache.Stats()
	if first.EventFullVerified != 1 {
		t.Fatalf("first unfiltered query stats=%+v, want one companion proof", first)
	}
	query()
	second := mgr.chainVerificationCache.Stats()
	if second.EventFullVerified != first.EventFullVerified || second.EventMemoryHits <= first.EventMemoryHits {
		t.Fatalf("unfiltered proof not reused: first=%+v second=%+v", first, second)
	}
}
