package rawdb

import (
	"bytes"
	"errors"
	"testing"

	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb/freezer"
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
