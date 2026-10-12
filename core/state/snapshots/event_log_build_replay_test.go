package snapshots

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/tronprotocol/go-tron/core/pointread"
	"github.com/tronprotocol/go-tron/core/rawdb"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	"google.golang.org/protobuf/proto"
)

type replayCountingReader struct {
	rawdb.EventLogReader
	scans int
}

func (r *replayCountingReader) IterateEventLogs(from, to uint64, filter EventLogFilter, fn func(EventLog) (bool, error)) error {
	r.scans++
	return r.EventLogReader.IterateEventLogs(from, to, filter, fn)
}

func TestEventLogBuildReplayEquivalentAndBounded(t *testing.T) {
	rows := eventLogV4BuildBenchRows(1024, 256, 4, true)
	for _, row := range rows {
		row.Log.ProtoReflect().SetUnknown([]byte{0xa0, 0x06, 0x01})
	}
	charge := uint64(0)
	for _, row := range rows {
		charge += uint64(proto.Size(row.Log)) + 512 + uint64(len(row.Log.Topics))*48
	}
	baseline := t.TempDir()
	want, err := buildEventLogV4SegmentFromReaderWorkers(eventLogRowsReader{rows: rows}, baseline, "", 1, 32, 1)
	if err != nil {
		t.Fatal(err)
	}
	wantBytes, err := os.ReadFile(filepath.Join(baseline, want.Path))
	if err != nil {
		t.Fatal(err)
	}
	for _, limit := range []uint64{0, 1, charge - 1, charge, charge + 1, eventLogReplayMemory} {
		t.Run(fmt.Sprint(limit), func(t *testing.T) {
			root := t.TempDir()
			source := &replayCountingReader{EventLogReader: eventLogRowsReader{rows: rows}}
			replay := &eventLogBuildReplay{EventLogReader: source, limit: limit}
			got, err := buildEventLogV4SegmentFromReaderWorkers(replay, root, "", 1, 32, 1)
			if err != nil {
				t.Fatal(err)
			}
			overflow := limit < charge
			wantScans := 1
			if overflow {
				wantScans = 2
			}
			if source.scans != wantScans || replay.charge > limit || replay.overflow != overflow {
				t.Fatalf("reads=%d charge=%d overflow=%v", source.scans, replay.charge, replay.overflow)
			}
			data, err := os.ReadFile(filepath.Join(root, got.Path))
			if err != nil {
				t.Fatal(err)
			}
			if got.Checksum != want.Checksum || !bytes.Equal(data, wantBytes) {
				t.Fatal("cache/fallback changed V4 bytes")
			}
			if overflow && (len(replay.rows) != 0 || replay.charge != 0) {
				t.Fatal("overflow retained partial cache")
			}
		})
	}
}

func TestEventLogBuildReplayOwnership(t *testing.T) {
	rows := eventLogV4BuildBenchRows(32, 32, 1, false)
	source := &replayCountingReader{EventLogReader: eventLogRowsReader{rows: rows}}
	r := &eventLogBuildReplay{EventLogReader: source, limit: eventLogReplayMemory}
	before := proto.Clone(rows[0].Log)
	if err := r.IterateEventLogs(1, 1, EventLogFilter{}, func(EventLog) (bool, error) { return true, nil }); err != nil {
		t.Fatal(err)
	}
	rows[0].Log.Data[0] ^= 0xff
	if err := r.IterateEventLogs(1, 1, EventLogFilter{}, func(row EventLog) (bool, error) {
		if !proto.Equal(row.Log, before) {
			t.Fatal("cache borrowed source row")
		}
		return false, nil
	}); err != nil {
		t.Fatal(err)
	}
	if source.scans != 1 {
		t.Fatal("cache rescanned source")
	}
	if err := r.IterateEventLogs(1, 1, EventLogFilter{}, func(EventLog) (bool, error) { return true, nil }); err == nil {
		t.Fatal("third pass accepted")
	}
}

func TestEventLogBuildReplayFallbackRetainsSecondPassErrors(t *testing.T) {
	sentinel := errors.New("second pass failed")
	source := &eventLogV4SecondPassReader{rows: eventLogV4BuildBenchRows(32, 32, 1, false), fail: sentinel}
	r := &eventLogBuildReplay{EventLogReader: source, limit: 1}
	ref, err := buildEventLogV4SegmentFromReaderWorkers(r, t.TempDir(), "", 1, 1, 1)
	if !errors.Is(err, sentinel) || ref.Checksum != "" || source.passes != 2 || len(r.rows) != 0 {
		t.Fatalf("fallback lost error/cache release: %+v %v", ref, err)
	}
}

type replaySnapshotFactory struct {
	ethdb.KeyValueStore
	factory              pointread.KeyValueSnapshotter
	openErr, closeErr    error
	opened, closed, gets int
	onOpen               func()
}

func (s *replaySnapshotFactory) NewKeyValueSnapshot() (pointread.KeyValueSnapshot, error) {
	if s.openErr != nil {
		return nil, s.openErr
	}
	v, err := s.factory.NewKeyValueSnapshot()
	if err != nil {
		return nil, err
	}
	s.opened++
	if s.onOpen != nil {
		s.onOpen()
	}
	return &replayCountSnapshot{KeyValueSnapshot: v, owner: s}, nil
}

type replayCountSnapshot struct {
	pointread.KeyValueSnapshot
	owner *replaySnapshotFactory
}

func (s *replayCountSnapshot) Get(key []byte) ([]byte, error) {
	s.owner.gets++
	return s.KeyValueSnapshot.Get(key)
}
func (s *replayCountSnapshot) Close() error {
	s.owner.closed++
	return errors.Join(s.KeyValueSnapshot.Close(), s.owner.closeErr)
}

func TestEventLogChainReplaySnapshotAndClose(t *testing.T) {
	db, err := rawdb.NewPebbleDB(t.TempDir(), 16, 16)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	seedParallelHistoryEvent(t, db, 8, 4, 2, 128)
	baseline := t.TempDir()
	want, err := BuildEventLogV4SegmentFromReader(eventLogV3ChainReader{chain: rawdb.NewChainDB(db, nil), identities: newEventLogChainIdentityCache()}, baseline, "", 1, 8)
	if err != nil {
		t.Fatal(err)
	}
	for _, mode := range []string{"success", "mutation", "open-error", "close-error", "unsupported"} {
		t.Run(mode, func(t *testing.T) {
			source := &replaySnapshotFactory{KeyValueStore: db, factory: db.(pointread.KeyValueSnapshotter)}
			sentinel := errors.New("snapshot failure")
			if mode == "open-error" {
				source.openErr = sentinel
			}
			if mode == "close-error" {
				source.closeErr = sentinel
			}
			if mode == "unsupported" {
				source.openErr = pointread.ErrKeyValueSnapshotUnsupported
			}
			var restore func()
			if mode == "mutation" {
				source.onOpen = func() {
					infos, _, err := rawdb.ReadTransactionInfosByBlockStrict(rawdb.NewChainDB(db, nil), 1)
					if err != nil {
						t.Fatal(err)
					}
					old := proto.Clone(infos[0])
					infos[0].Log[0].Data = []byte("after snapshot")
					if err := rawdb.WriteTransactionInfosByBlock(db, 1, infos); err != nil {
						t.Fatal(err)
					}
					restore = func() {
						infos[0] = old.(*corepb.TransactionInfo)
						if err := rawdb.WriteTransactionInfosByBlock(db, 1, infos); err != nil {
							t.Fatal(err)
						}
					}
				}
			}
			defer func() {
				if restore != nil {
					restore()
				}
			}()
			root := t.TempDir()
			ref, err := BuildEventLogV4SegmentFromChain(rawdb.NewChainDB(source, nil), root, "", 1, 8)
			if mode == "open-error" || mode == "close-error" {
				if !errors.Is(err, sentinel) || ref.Checksum != "" {
					t.Fatalf("failed snapshot returned valid ref: %+v %v", ref, err)
				}
			} else if err != nil || ref.Checksum != want.Checksum {
				t.Fatalf("snapshot build: %+v %v", ref, err)
			}
			if mode != "open-error" && mode != "unsupported" && (source.opened != 1 || source.closed != 1 || source.gets == 0) {
				t.Fatalf("snapshot lifetime: %+v", source)
			}
			if _, ok, err := rawdb.ReadBlockStrict(rawdb.NewChainDB(db, nil), 1); err != nil || !ok {
				t.Fatal("closed live DB")
			}
		})
	}
}

func TestEventLogChainReplayRetainsExternalReceiptRouting(t *testing.T) {
	db, err := rawdb.NewPebbleDB(t.TempDir(), 16, 16)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	seedParallelHistoryEvent(t, db, 4, 8, 2, 256)
	chain := rawdb.NewChainDB(db, nil)
	rows := collectEventIdentityRows(t, eventLogV3ChainReader{chain: chain}, 1, 4, EventLogFilter{})
	baseline, err := BuildEventLogV4SegmentFromChain(chain, t.TempDir(), "", 1, 4)
	if err != nil {
		t.Fatal(err)
	}
	for number := uint64(1); number <= 4; number++ {
		raw, ok, err := rawdb.ReadTransactionInfosRawStrict(chain, number)
		if err != nil || !ok {
			t.Fatalf("raw receipts: %v %v", ok, err)
		}
		external, _, err := rawdb.ExternalizeTransactionInfoLogs(raw)
		if err != nil {
			t.Fatal(err)
		}
		if err := rawdb.WriteTransactionInfosRaw(db, number, external); err != nil {
			t.Fatal(err)
		}
	}
	chain.SetEventLogReader(replayRangeReader{eventLogRowsReader{rows: rows}})
	got, err := BuildEventLogV4SegmentFromChain(chain, t.TempDir(), "", 1, 4)
	if err != nil || got.Checksum != baseline.Checksum || got.Size != baseline.Size {
		t.Fatalf("external logs lost through snapshot chain: %+v %v", got, err)
	}
	chain.SetEventLogReader(nil)
	if ref, err := BuildEventLogV4SegmentFromChain(chain, t.TempDir(), "", 1, 4); err == nil || ref.Checksum != "" {
		t.Fatal("missing external coverage accepted")
	}
}

type replayFailReader struct {
	rawdb.EventLogReader
	fail error
}

func (r replayFailReader) IterateEventLogs(from, to uint64, filter EventLogFilter, fn func(EventLog) (bool, error)) error {
	if err := r.EventLogReader.IterateEventLogs(from, to, filter, fn); err != nil {
		return err
	}
	return r.fail
}
func TestEventLogBuildReplaySourceFailure(t *testing.T) {
	sentinel := errors.New("source scan failed after rows")
	r := &eventLogBuildReplay{EventLogReader: replayFailReader{eventLogRowsReader{rows: eventLogV4BuildBenchRows(32, 128, 1, false)}, sentinel}, limit: 1}
	ref, err := buildEventLogV4SegmentFromReaderWorkers(r, t.TempDir(), "", 1, 1, 1)
	if !errors.Is(err, sentinel) || ref.Checksum != "" || r.passes != 1 {
		t.Fatalf("source failure: %+v %v", ref, err)
	}
}

// Hydration asks the sidecar for one block at a time, unlike the build test's
// simple whole-range row reader.
type replayRangeReader struct{ rawdb.EventLogReader }

func (r replayRangeReader) IterateEventLogs(from, to uint64, filter EventLogFilter, fn func(EventLog) (bool, error)) error {
	return r.EventLogReader.IterateEventLogs(from, to, filter, func(row EventLog) (bool, error) {
		if row.BlockNum < from || row.BlockNum > to {
			return true, nil
		}
		return fn(row)
	})
}

func TestEventLogBuildReplayOverflowKeepsSameSnapshot(t *testing.T) {
	db, err := rawdb.NewPebbleDB(t.TempDir(), 16, 16)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	seedParallelHistoryEvent(t, db, 4, 8, 2, 256)
	chain := rawdb.NewChainDB(db, nil)
	want, err := BuildEventLogV4SegmentFromChain(chain, t.TempDir(), "", 1, 4)
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := chain.NewKeyValueSnapshot()
	if err != nil {
		t.Fatal(err)
	}
	defer snapshot.Close()
	view := *chain
	view.KeyValueStore = eventLogSnapshotStore{KeyValueStore: db, snapshot: snapshot}
	source := &eventIdentityMutatingReader{eventLogV3ChainReader: eventLogV3ChainReader{chain: &view, identities: newEventLogChainIdentityCache()}, mutate: func() {
		infos, _, err := rawdb.ReadTransactionInfosByBlockStrict(chain, 1)
		if err != nil {
			t.Fatal(err)
		}
		infos[0].Log[0].Data = []byte("changed between source passes")
		if err := rawdb.WriteTransactionInfosByBlock(db, 1, infos); err != nil {
			t.Fatal(err)
		}
	}}
	cache := &eventLogBuildReplay{EventLogReader: source, limit: 1}
	ref, err := BuildEventLogV4SegmentFromReader(cache, t.TempDir(), "", 1, 4)
	if err != nil || ref.Checksum != want.Checksum || !cache.overflow || source.passes != 2 || len(cache.rows) != 0 {
		t.Fatalf("overflow moved source view: %+v %v", ref, err)
	}
}
