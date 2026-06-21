package rawdb

import (
	"bytes"
	"errors"
	"strings"
	"testing"

	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb/freezer"
	coretypes "github.com/tronprotocol/go-tron/core/types"
	corepb "github.com/tronprotocol/go-tron/proto/core"
)

// TestChainDBNoopAncient composes a memdb with a NoopAncient and confirms
// the embedded KV interface still works while every Ancient call reports
// "not found".
func TestChainDBNoopAncient(t *testing.T) {
	t.Parallel()

	kv := NewMemoryDatabase()
	cdb := NewChainDB(kv, NoopAncient{})

	// KV round-trip via the embedded interface.
	if err := cdb.Put([]byte("k"), []byte("v")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	got, err := cdb.Get([]byte("k"))
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if !bytes.Equal(got, []byte("v")) {
		t.Fatalf("Get returned %q", got)
	}

	// Every kind reports zero count and a not-in-ancient error.
	for _, kind := range []string{"headers", "bodies", "tx_infos", "state_roots"} {
		count, err := cdb.AncientCount(kind)
		if err != nil {
			t.Fatalf("AncientCount(%s): %v", kind, err)
		}
		if count != 0 {
			t.Fatalf("AncientCount(%s)=%d on NoopAncient", kind, count)
		}
		if _, err := cdb.Ancient(kind, 0); !errors.Is(err, ErrNotInAncient) {
			t.Fatalf("Ancient(%s, 0): want ErrNotInAncient, got %v", kind, err)
		}
		ok, err := cdb.HasAncient(kind, 0)
		if err != nil {
			t.Fatalf("HasAncient(%s, 0): %v", kind, err)
		}
		if ok {
			t.Fatalf("HasAncient(%s, 0) returned true on NoopAncient", kind)
		}
	}
}

// TestChainDBNilAncient confirms NewChainDB substitutes NoopAncient when the
// caller passes a nil reader (matches the slice-1 "freezer disabled" config).
func TestChainDBNilAncient(t *testing.T) {
	t.Parallel()
	cdb := NewChainDB(NewMemoryDatabase(), nil)
	if cdb.AncientReader == nil {
		t.Fatalf("nil AncientReader after NewChainDB(_, nil)")
	}
	if _, err := cdb.Ancient("headers", 0); !errors.Is(err, ErrNotInAncient) {
		t.Fatalf("Ancient on nil-AncientReader path: want ErrNotInAncient, got %v", err)
	}
}

func TestFallbackAncientReaderUsesLaterSourceOnMiss(t *testing.T) {
	t.Parallel()

	local := newFakeAncient()
	local.put("headers", 0, []byte("local-0"))
	cold := newFakeAncient()
	cold.put("headers", 0, []byte("cold-0"))
	cold.put("headers", 1, []byte("cold-1"))

	reader := NewFallbackAncientReader(local, cold)
	if got, err := reader.Ancient("headers", 0); err != nil || !bytes.Equal(got, []byte("local-0")) {
		t.Fatalf("Ancient local row = %q/%v, want local-0/nil", got, err)
	}
	if got, err := reader.Ancient("headers", 1); err != nil || !bytes.Equal(got, []byte("cold-1")) {
		t.Fatalf("Ancient cold fallback row = %q/%v, want cold-1/nil", got, err)
	}
	if _, err := reader.Ancient("headers", 2); !errors.Is(err, ErrNotInAncient) {
		t.Fatalf("Ancient missing row err = %v, want ErrNotInAncient", err)
	}
	if count, err := reader.AncientCount("headers"); err != nil || count != 2 {
		t.Fatalf("AncientCount = %d/%v, want 2/nil", count, err)
	}
	if ok, err := reader.HasAncient("headers", 1); err != nil || !ok {
		t.Fatalf("HasAncient cold fallback = %v/%v, want true/nil", ok, err)
	}
	rows, err := reader.AncientRange("headers", 0, 3, 0)
	if err != nil {
		t.Fatalf("AncientRange: %v", err)
	}
	if len(rows) != 2 || !bytes.Equal(rows[0], []byte("local-0")) || !bytes.Equal(rows[1], []byte("cold-1")) {
		t.Fatalf("AncientRange rows = %q, want local-0,cold-1", rows)
	}
}

// TestChainDBFreezerReader plumbs a real on-disk freezer through ChainDB and
// confirms the error translation maps freezer.ErrOutOfBounds → ErrNotInAncient.
func TestChainDBFreezerReader(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	tables := map[string]freezer.TableConfig{
		"headers": {NoSnappy: false},
	}
	f, err := freezer.NewFreezer(dir, "", false, 2049, tables)
	if err != nil {
		t.Fatalf("freezer: %v", err)
	}
	t.Cleanup(func() { _ = f.Close() })

	if _, err := f.ModifyAncients(func(op freezer.AncientWriteOp) error {
		return op.AppendRaw("headers", 0, []byte("first"))
	}); err != nil {
		t.Fatalf("ModifyAncients: %v", err)
	}

	cdb := NewChainDB(NewMemoryDatabase(), NewFreezerReader(f))

	// In-bounds read works.
	got, err := cdb.Ancient("headers", 0)
	if err != nil {
		t.Fatalf("Ancient: %v", err)
	}
	if !bytes.Equal(got, []byte("first")) {
		t.Fatalf("Ancient: %x", got)
	}
	// Out-of-bounds translates to ErrNotInAncient.
	if _, err := cdb.Ancient("headers", 1); !errors.Is(err, ErrNotInAncient) {
		t.Fatalf("post-head read: want ErrNotInAncient, got %v", err)
	}
	// Unknown table translates to ErrNotInAncient.
	if _, err := cdb.Ancient("missing", 0); !errors.Is(err, ErrNotInAncient) {
		t.Fatalf("unknown table: want ErrNotInAncient, got %v", err)
	}
	// HasAncient at the existing item is true.
	ok, err := cdb.HasAncient("headers", 0)
	if err != nil {
		t.Fatalf("HasAncient: %v", err)
	}
	if !ok {
		t.Fatalf("HasAncient(headers,0)=false")
	}
}

func TestChainDBEventLogCoverageUsesFilteredReaderWhenAvailable(t *testing.T) {
	t.Parallel()

	filter := EventLogFilter{
		Addresses: []common.Address{common.BytesToAddress([]byte{0x41})},
		Topics:    [][]common.Hash{{common.Hash{0x42}}},
	}
	reader := &recordingEventLogReader{filteredCovered: true}
	cdb := NewMemoryChainDB()
	cdb.SetEventLogReader(reader)

	covered, err := cdb.EventLogRangeCoveredForFilter(7, 9, filter)
	if err != nil || !covered {
		t.Fatalf("filtered coverage = %v/%v, want true/nil", covered, err)
	}
	if reader.filteredCalls != 1 || reader.coveredCalls != 0 {
		t.Fatalf("reader calls filtered=%d covered=%d, want filtered only", reader.filteredCalls, reader.coveredCalls)
	}
	if reader.lastFrom != 7 || reader.lastTo != 9 || len(reader.lastFilter.Addresses) != 1 || reader.lastFilter.Addresses[0] != filter.Addresses[0] {
		t.Fatalf("forwarded filter = from:%d to:%d filter:%+v, want original", reader.lastFrom, reader.lastTo, reader.lastFilter)
	}
}

func TestChainDBEventLogCoverageFallsBackToUnfilteredReader(t *testing.T) {
	t.Parallel()

	reader := &recordingBasicEventLogReader{covered: true}
	cdb := NewMemoryChainDB()
	cdb.SetEventLogReader(reader)

	covered, err := cdb.EventLogRangeCoveredForFilter(3, 5, EventLogFilter{Topics: [][]common.Hash{{common.Hash{0x99}}}})
	if err != nil || !covered {
		t.Fatalf("fallback coverage = %v/%v, want true/nil", covered, err)
	}
	if reader.coveredCalls != 1 || reader.lastFrom != 3 || reader.lastTo != 5 {
		t.Fatalf("basic reader calls=%d range=%d..%d, want one call 3..5", reader.coveredCalls, reader.lastFrom, reader.lastTo)
	}
}

func TestChainDBIterateEventLogsForwardsColdRows(t *testing.T) {
	t.Parallel()

	wantLog := &corepb.TransactionInfo_Log{Address: []byte{0x01}}
	want := EventLog{
		BlockNum:  11,
		TxIndex:   2,
		LogIndex:  3,
		TxHash:    common.Hash{0xaa},
		BlockHash: common.Hash{0xbb},
		Address:   common.BytesToAddress([]byte{0x41}),
		Log:       wantLog,
	}
	reader := &recordingEventLogReader{recordingBasicEventLogReader: recordingBasicEventLogReader{rows: []EventLog{want}}}
	cdb := NewMemoryChainDB()
	cdb.SetEventLogReader(reader)

	var got []EventLog
	err := cdb.IterateEventLogs(10, 12, EventLogFilter{Addresses: []common.Address{want.Address}}, func(row EventLog) (bool, error) {
		got = append(got, row)
		return true, nil
	})
	if err != nil {
		t.Fatalf("IterateEventLogs: %v", err)
	}
	if reader.iterCalls != 1 || len(got) != 1 || got[0].BlockNum != want.BlockNum || got[0].Log != wantLog {
		t.Fatalf("iter calls=%d rows=%+v, want one forwarded row", reader.iterCalls, got)
	}
}

func TestChainDBIterateCoveredEventLogsUsesAtomicReaderWhenAvailable(t *testing.T) {
	t.Parallel()

	want := EventLog{BlockNum: 21, Address: testChainDBEventLogAddress(0x21), Log: &corepb.TransactionInfo_Log{Address: []byte{0x21}}}
	reader := &recordingCoveredEventLogReader{
		covered: true,
		rows:    []EventLog{want},
	}
	cdb := NewMemoryChainDB()
	cdb.SetEventLogReader(reader)

	var got []EventLog
	covered, err := cdb.IterateCoveredEventLogs(20, 22, EventLogFilter{}, func(row EventLog) (bool, error) {
		got = append(got, row)
		return true, nil
	})
	if err != nil || !covered {
		t.Fatalf("IterateCoveredEventLogs = covered %v err %v, want true/nil", covered, err)
	}
	if reader.coveredIterCalls != 1 || reader.coveredCalls != 0 || reader.iterCalls != 0 {
		t.Fatalf("reader calls coveredIter=%d covered=%d iter=%d, want atomic only", reader.coveredIterCalls, reader.coveredCalls, reader.iterCalls)
	}
	if len(got) != 1 || got[0].BlockNum != want.BlockNum {
		t.Fatalf("covered rows = %+v, want block %d", got, want.BlockNum)
	}
}

func TestChainDBIterateCoveredEventLogsFallsBackToCoverageAndIteration(t *testing.T) {
	t.Parallel()

	want := EventLog{BlockNum: 31, Address: testChainDBEventLogAddress(0x31), Log: &corepb.TransactionInfo_Log{Address: []byte{0x31}}}
	reader := &recordingBasicEventLogReader{covered: true, rows: []EventLog{want}}
	cdb := NewMemoryChainDB()
	cdb.SetEventLogReader(reader)

	var got []EventLog
	covered, err := cdb.IterateCoveredEventLogs(30, 32, EventLogFilter{}, func(row EventLog) (bool, error) {
		got = append(got, row)
		return true, nil
	})
	if err != nil || !covered {
		t.Fatalf("fallback IterateCoveredEventLogs = covered %v err %v, want true/nil", covered, err)
	}
	if reader.coveredCalls != 1 || reader.iterCalls != 1 || len(got) != 1 || got[0].BlockNum != want.BlockNum {
		t.Fatalf("fallback calls covered=%d iter=%d rows=%+v, want one coverage+iteration", reader.coveredCalls, reader.iterCalls, got)
	}
}

func TestChainDBIterateCoveredEventLogsSkipsIterationWhenUncovered(t *testing.T) {
	t.Parallel()

	reader := &recordingBasicEventLogReader{covered: false, rows: []EventLog{{BlockNum: 41}}}
	cdb := NewMemoryChainDB()
	cdb.SetEventLogReader(reader)

	covered, err := cdb.IterateCoveredEventLogs(40, 42, EventLogFilter{}, func(EventLog) (bool, error) {
		t.Fatal("callback called for uncovered cold event-log range")
		return true, nil
	})
	if err != nil || covered {
		t.Fatalf("uncovered IterateCoveredEventLogs = covered %v err %v, want false/nil", covered, err)
	}
	if reader.coveredCalls != 1 || reader.iterCalls != 0 {
		t.Fatalf("uncovered calls covered=%d iter=%d, want coverage only", reader.coveredCalls, reader.iterCalls)
	}
}

func TestChainDBIterateCoveredEventLogsValidatesFallbackRows(t *testing.T) {
	t.Parallel()

	reader := &recordingBasicEventLogReader{
		covered: true,
		rows: []EventLog{{
			BlockNum: 43,
			Address:  testChainDBEventLogAddress(0x43),
			Log:      &corepb.TransactionInfo_Log{Address: []byte{0x43}},
		}},
	}
	cdb := NewMemoryChainDB()
	cdb.SetEventLogReader(reader)

	covered, err := cdb.IterateCoveredEventLogs(40, 42, EventLogFilter{}, func(EventLog) (bool, error) {
		t.Fatal("callback called for out-of-range fallback cold event-log row")
		return true, nil
	})
	if err == nil || !covered || !strings.Contains(err.Error(), "outside covered range [40,42]") {
		t.Fatalf("fallback out-of-range = covered %v err %v, want covered error", covered, err)
	}
	if reader.coveredCalls != 1 || reader.iterCalls != 1 {
		t.Fatalf("fallback calls covered=%d iter=%d, want one coverage+iteration", reader.coveredCalls, reader.iterCalls)
	}
}

func TestChainDBIterateCoveredEventLogsValidatesFallbackOrder(t *testing.T) {
	t.Parallel()

	reader := &recordingBasicEventLogReader{
		covered: true,
		rows: []EventLog{
			{BlockNum: 50, TxIndex: 1, LogIndex: 0, Address: testChainDBEventLogAddress(0x50), Log: &corepb.TransactionInfo_Log{Address: []byte{0x50}}},
			{BlockNum: 50, TxIndex: 0, LogIndex: 9, Address: testChainDBEventLogAddress(0x50), Log: &corepb.TransactionInfo_Log{Address: []byte{0x50}}},
		},
	}
	cdb := NewMemoryChainDB()
	cdb.SetEventLogReader(reader)

	var rows int
	covered, err := cdb.IterateCoveredEventLogs(50, 50, EventLogFilter{}, func(EventLog) (bool, error) {
		rows++
		return true, nil
	})
	if err == nil || !covered || !strings.Contains(err.Error(), "is not after previous") {
		t.Fatalf("fallback unsorted = covered %v err %v, want ordering error", covered, err)
	}
	if rows != 1 {
		t.Fatalf("callback rows = %d, want only first row before fallback ordering error", rows)
	}
}

func TestChainDBIterateCoveredEventLogsRejectsOutOfRangeAtomicRow(t *testing.T) {
	t.Parallel()

	reader := &recordingCoveredEventLogReader{
		covered: true,
		rows: []EventLog{{
			BlockNum: 13,
			Address:  testChainDBEventLogAddress(0x13),
			Log:      &corepb.TransactionInfo_Log{Address: []byte{0x13}},
		}},
	}
	cdb := NewMemoryChainDB()
	cdb.SetEventLogReader(reader)

	covered, err := cdb.IterateCoveredEventLogs(10, 12, EventLogFilter{}, func(EventLog) (bool, error) {
		t.Fatal("callback called for out-of-range cold event-log row")
		return true, nil
	})
	if err == nil || !covered || !strings.Contains(err.Error(), "outside covered range [10,12]") {
		t.Fatalf("IterateCoveredEventLogs out-of-range = covered %v err %v, want covered error", covered, err)
	}
	if reader.coveredIterCalls != 1 || reader.coveredCalls != 0 || reader.iterCalls != 0 {
		t.Fatalf("reader calls coveredIter=%d covered=%d iter=%d, want atomic only", reader.coveredIterCalls, reader.coveredCalls, reader.iterCalls)
	}
}

func TestChainDBIterateCoveredEventLogsRejectsNilAtomicRowPayload(t *testing.T) {
	t.Parallel()

	reader := &recordingCoveredEventLogReader{
		covered: true,
		rows: []EventLog{{
			BlockNum: 11,
			TxIndex:  2,
			LogIndex: 3,
		}},
	}
	cdb := NewMemoryChainDB()
	cdb.SetEventLogReader(reader)

	covered, err := cdb.IterateCoveredEventLogs(10, 12, EventLogFilter{}, func(EventLog) (bool, error) {
		t.Fatal("callback called for nil cold event-log row")
		return true, nil
	})
	if err == nil || !covered || !strings.Contains(err.Error(), "cold event log row block=11 tx=2 log=3 is nil") {
		t.Fatalf("IterateCoveredEventLogs nil row = covered %v err %v, want nil-row error", covered, err)
	}
}

func TestChainDBIterateCoveredEventLogsRejectsPayloadAddressMismatch(t *testing.T) {
	t.Parallel()

	reader := &recordingCoveredEventLogReader{
		covered: true,
		rows: []EventLog{{
			BlockNum: 10,
			TxIndex:  1,
			LogIndex: 2,
			Address:  testChainDBEventLogAddress(0xaa),
			Log:      &corepb.TransactionInfo_Log{Address: []byte{0xbb}},
		}},
	}
	cdb := NewMemoryChainDB()
	cdb.SetEventLogReader(reader)

	covered, err := cdb.IterateCoveredEventLogs(10, 10, EventLogFilter{}, func(EventLog) (bool, error) {
		t.Fatal("callback called for address-mismatched cold event-log row")
		return true, nil
	})
	if err == nil || !covered || !strings.Contains(err.Error(), "does not match payload address") {
		t.Fatalf("IterateCoveredEventLogs address mismatch = covered %v err %v, want payload address error", covered, err)
	}
}

func TestChainDBIterateCoveredEventLogsRejectsCanonicalHashMismatch(t *testing.T) {
	t.Parallel()

	block := testSyncStagedBlock(12, common.Hash{0x0b})
	reader := &recordingCoveredEventLogReader{
		covered: true,
		rows: []EventLog{{
			BlockNum:  block.Number(),
			TxIndex:   1,
			LogIndex:  2,
			BlockHash: common.Hash{0xff},
			Address:   testChainDBEventLogAddress(0x12),
			Log:       &corepb.TransactionInfo_Log{Address: []byte{0x12}},
		}},
	}
	cdb := NewMemoryChainDB()
	if err := WriteBlock(cdb, block); err != nil {
		t.Fatalf("WriteBlock: %v", err)
	}
	cdb.SetEventLogReader(reader)

	covered, err := cdb.IterateCoveredEventLogs(block.Number(), block.Number(), EventLogFilter{}, func(EventLog) (bool, error) {
		t.Fatal("callback called for hash-mismatched cold event-log row")
		return true, nil
	})
	if err == nil || !covered || !strings.Contains(err.Error(), "does not match canonical hash") {
		t.Fatalf("IterateCoveredEventLogs hash mismatch = covered %v err %v, want canonical hash error", covered, err)
	}
}

func TestChainDBIterateCoveredEventLogsRejectsCanonicalTxHashMismatch(t *testing.T) {
	t.Parallel()

	block, _ := testChainDBEventLogBlock(13)
	reader := &recordingCoveredEventLogReader{
		covered: true,
		rows: []EventLog{{
			BlockNum:  block.Number(),
			TxIndex:   0,
			LogIndex:  1,
			BlockHash: block.Hash(),
			TxHash:    common.Hash{0xee},
			Address:   testChainDBEventLogAddress(0x13),
			Log:       &corepb.TransactionInfo_Log{Address: []byte{0x13}},
		}},
	}
	cdb := NewMemoryChainDB()
	if err := WriteBlock(cdb, block); err != nil {
		t.Fatalf("WriteBlock: %v", err)
	}
	cdb.SetEventLogReader(reader)

	covered, err := cdb.IterateCoveredEventLogs(block.Number(), block.Number(), EventLogFilter{}, func(EventLog) (bool, error) {
		t.Fatal("callback called for tx-hash-mismatched cold event-log row")
		return true, nil
	})
	if err == nil || !covered || !strings.Contains(err.Error(), "does not match canonical transaction hash") {
		t.Fatalf("IterateCoveredEventLogs tx hash mismatch = covered %v err %v, want canonical tx hash error", covered, err)
	}
}

func TestChainDBIterateCoveredEventLogsRejectsCanonicalTxIndexOutOfRange(t *testing.T) {
	t.Parallel()

	block, txHash := testChainDBEventLogBlock(14)
	reader := &recordingCoveredEventLogReader{
		covered: true,
		rows: []EventLog{{
			BlockNum:  block.Number(),
			TxIndex:   1,
			LogIndex:  0,
			BlockHash: block.Hash(),
			TxHash:    txHash,
			Address:   testChainDBEventLogAddress(0x14),
			Log:       &corepb.TransactionInfo_Log{Address: []byte{0x14}},
		}},
	}
	cdb := NewMemoryChainDB()
	if err := WriteBlock(cdb, block); err != nil {
		t.Fatalf("WriteBlock: %v", err)
	}
	cdb.SetEventLogReader(reader)

	covered, err := cdb.IterateCoveredEventLogs(block.Number(), block.Number(), EventLogFilter{}, func(EventLog) (bool, error) {
		t.Fatal("callback called for tx-index-out-of-range cold event-log row")
		return true, nil
	})
	if err == nil || !covered || !strings.Contains(err.Error(), "outside canonical transaction count 1") {
		t.Fatalf("IterateCoveredEventLogs tx index range = covered %v err %v, want canonical tx index error", covered, err)
	}
}

func TestChainDBIterateCoveredEventLogsRejectsCanonicalLogPayloadMismatch(t *testing.T) {
	t.Parallel()

	block, txHash := testChainDBEventLogBlock(15)
	canonicalLog := &corepb.TransactionInfo_Log{Address: []byte{0x15}, Topics: [][]byte{{0x01}}, Data: []byte{0xaa}}
	reader := &recordingCoveredEventLogReader{
		covered: true,
		rows: []EventLog{{
			BlockNum:  block.Number(),
			TxIndex:   0,
			LogIndex:  0,
			BlockHash: block.Hash(),
			TxHash:    txHash,
			Address:   testChainDBEventLogAddress(0x15),
			Log:       &corepb.TransactionInfo_Log{Address: []byte{0x15}, Topics: [][]byte{{0x01}}, Data: []byte{0xbb}},
		}},
	}
	cdb := NewMemoryChainDB()
	if err := WriteBlock(cdb, block); err != nil {
		t.Fatalf("WriteBlock: %v", err)
	}
	if err := WriteTransactionInfosByBlock(cdb, block.Number(), []*corepb.TransactionInfo{{
		BlockNumber: int64(block.Number()),
		Log:         []*corepb.TransactionInfo_Log{canonicalLog},
	}}); err != nil {
		t.Fatalf("WriteTransactionInfosByBlock: %v", err)
	}
	cdb.SetEventLogReader(reader)

	covered, err := cdb.IterateCoveredEventLogs(block.Number(), block.Number(), EventLogFilter{}, func(EventLog) (bool, error) {
		t.Fatal("callback called for payload-mismatched cold event-log row")
		return true, nil
	})
	if err == nil || !covered || !strings.Contains(err.Error(), "payload does not match canonical transaction info log") {
		t.Fatalf("IterateCoveredEventLogs payload mismatch = covered %v err %v, want canonical log payload error", covered, err)
	}
}

func TestChainDBIterateCoveredEventLogsRejectsCanonicalLogIndexOutOfRange(t *testing.T) {
	t.Parallel()

	block, txHash := testChainDBEventLogBlock(16)
	canonicalLog := &corepb.TransactionInfo_Log{Address: []byte{0x16}, Data: []byte{0x01}}
	reader := &recordingCoveredEventLogReader{
		covered: true,
		rows: []EventLog{{
			BlockNum:  block.Number(),
			TxIndex:   0,
			LogIndex:  1,
			BlockHash: block.Hash(),
			TxHash:    txHash,
			Address:   testChainDBEventLogAddress(0x16),
			Log:       &corepb.TransactionInfo_Log{Address: []byte{0x16}, Data: []byte{0x01}},
		}},
	}
	cdb := NewMemoryChainDB()
	if err := WriteBlock(cdb, block); err != nil {
		t.Fatalf("WriteBlock: %v", err)
	}
	if err := WriteTransactionInfosByBlock(cdb, block.Number(), []*corepb.TransactionInfo{{
		BlockNumber: int64(block.Number()),
		Log:         []*corepb.TransactionInfo_Log{canonicalLog},
	}}); err != nil {
		t.Fatalf("WriteTransactionInfosByBlock: %v", err)
	}
	cdb.SetEventLogReader(reader)

	covered, err := cdb.IterateCoveredEventLogs(block.Number(), block.Number(), EventLogFilter{}, func(EventLog) (bool, error) {
		t.Fatal("callback called for out-of-range log-index cold event-log row")
		return true, nil
	})
	if err == nil || !covered || !strings.Contains(err.Error(), "outside canonical log count 1") {
		t.Fatalf("IterateCoveredEventLogs log index range = covered %v err %v, want canonical log index error", covered, err)
	}
}

func TestChainDBIterateCoveredEventLogsRejectsCanonicalLogTxIndexMismatch(t *testing.T) {
	t.Parallel()

	block, txHashes := testChainDBEventLogBlockWithTransactions(17, 2)
	canonicalLog := &corepb.TransactionInfo_Log{Address: []byte{0x17}, Data: []byte{0x02}}
	reader := &recordingCoveredEventLogReader{
		covered: true,
		rows: []EventLog{{
			BlockNum:  block.Number(),
			TxIndex:   0,
			LogIndex:  0,
			BlockHash: block.Hash(),
			TxHash:    txHashes[0],
			Address:   testChainDBEventLogAddress(0x17),
			Log:       canonicalLog,
		}},
	}
	cdb := NewMemoryChainDB()
	if err := WriteBlock(cdb, block); err != nil {
		t.Fatalf("WriteBlock: %v", err)
	}
	if err := WriteTransactionInfosByBlock(cdb, block.Number(), []*corepb.TransactionInfo{
		{BlockNumber: int64(block.Number())},
		{BlockNumber: int64(block.Number()), Log: []*corepb.TransactionInfo_Log{canonicalLog}},
	}); err != nil {
		t.Fatalf("WriteTransactionInfosByBlock: %v", err)
	}
	cdb.SetEventLogReader(reader)

	covered, err := cdb.IterateCoveredEventLogs(block.Number(), block.Number(), EventLogFilter{}, func(EventLog) (bool, error) {
		t.Fatal("callback called for tx-index-mismatched canonical log row")
		return true, nil
	})
	if err == nil || !covered || !strings.Contains(err.Error(), "belongs to canonical tx index 1") {
		t.Fatalf("IterateCoveredEventLogs log tx mismatch = covered %v err %v, want canonical log tx-index error", covered, err)
	}
}

func TestChainDBIterateCoveredEventLogsRejectsUnsortedAtomicRows(t *testing.T) {
	t.Parallel()

	reader := &recordingCoveredEventLogReader{
		covered: true,
		rows: []EventLog{
			{BlockNum: 10, TxIndex: 1, LogIndex: 1, Address: testChainDBEventLogAddress(0x10), Log: &corepb.TransactionInfo_Log{Address: []byte{0x10}}},
			{BlockNum: 10, TxIndex: 1, LogIndex: 0, Address: testChainDBEventLogAddress(0x10), Log: &corepb.TransactionInfo_Log{Address: []byte{0x10}}},
		},
	}
	cdb := NewMemoryChainDB()
	cdb.SetEventLogReader(reader)

	var rows int
	covered, err := cdb.IterateCoveredEventLogs(10, 10, EventLogFilter{}, func(EventLog) (bool, error) {
		rows++
		return true, nil
	})
	if err == nil || !covered || !strings.Contains(err.Error(), "is not after previous") {
		t.Fatalf("IterateCoveredEventLogs unsorted = covered %v err %v, want ordering error", covered, err)
	}
	if rows != 1 {
		t.Fatalf("callback rows = %d, want only first sorted row before error", rows)
	}
}

type recordingBasicEventLogReader struct {
	covered      bool
	coveredCalls int
	lastFrom     uint64
	lastTo       uint64
	rows         []EventLog
	iterCalls    int
}

func (r *recordingBasicEventLogReader) EventLogRangeCovered(fromBlock, toBlock uint64) (bool, error) {
	r.coveredCalls++
	r.lastFrom = fromBlock
	r.lastTo = toBlock
	return r.covered, nil
}

func (r *recordingBasicEventLogReader) IterateEventLogs(fromBlock, toBlock uint64, _ EventLogFilter, fn func(EventLog) (bool, error)) error {
	r.iterCalls++
	r.lastFrom = fromBlock
	r.lastTo = toBlock
	for _, row := range r.rows {
		cont, err := fn(row)
		if err != nil || !cont {
			return err
		}
	}
	return nil
}

type recordingEventLogReader struct {
	recordingBasicEventLogReader
	filteredCovered bool
	filteredCalls   int
	lastFilter      EventLogFilter
}

func (r *recordingEventLogReader) EventLogRangeCoveredForFilter(fromBlock, toBlock uint64, filter EventLogFilter) (bool, error) {
	r.filteredCalls++
	r.lastFrom = fromBlock
	r.lastTo = toBlock
	r.lastFilter = filter
	return r.filteredCovered, nil
}

type recordingCoveredEventLogReader struct {
	recordingEventLogReader
	covered          bool
	rows             []EventLog
	coveredIterCalls int
}

func (r *recordingCoveredEventLogReader) IterateCoveredEventLogs(fromBlock, toBlock uint64, filter EventLogFilter, fn func(EventLog) (bool, error)) (bool, error) {
	r.coveredIterCalls++
	r.lastFrom = fromBlock
	r.lastTo = toBlock
	r.lastFilter = filter
	if !r.covered {
		return false, nil
	}
	for _, row := range r.rows {
		cont, err := fn(row)
		if err != nil || !cont {
			return true, err
		}
	}
	return true, nil
}

func testChainDBEventLogBlock(number uint64) (*coretypes.Block, common.Hash) {
	block, txHashes := testChainDBEventLogBlockWithTransactions(number, 1)
	return block, txHashes[0]
}

func testChainDBEventLogAddress(b byte) common.Address {
	return common.BytesToAddress([]byte{b})
}

func testChainDBEventLogBlockWithTransactions(number uint64, txCount int) (*coretypes.Block, []common.Hash) {
	txs := make([]*corepb.Transaction, txCount)
	txHashes := make([]common.Hash, txCount)
	for i := 0; i < txCount; i++ {
		txPB := &corepb.Transaction{
			RawData: &corepb.TransactionRaw{
				Timestamp:  int64(10_000 + number + uint64(i)),
				Expiration: int64(20_000 + number + uint64(i)),
				Data:       []byte{byte(number), byte(i)},
			},
		}
		tx := coretypes.NewTransactionFromPB(txPB)
		txs[i] = txPB
		txHashes[i] = tx.Hash()
	}
	block := coretypes.NewBlockFromPB(&corepb.Block{
		BlockHeader: &corepb.BlockHeader{
			RawData: &corepb.BlockHeaderRaw{
				Number:    int64(number),
				Timestamp: int64(30_000 + number),
			},
			WitnessSignature: make([]byte, 65),
		},
		Transactions: txs,
	})
	return block, txHashes
}
