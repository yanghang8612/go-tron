package snapshots

import (
	"bytes"
	"errors"
	"os"
	"reflect"
	"strings"
	"testing"

	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
)

type anchoredEventLogRowsReader struct {
	eventLogRowsReader
	anchors map[uint64]common.Hash
}

func (r anchoredEventLogRowsReader) EventLogSourceBlockHash(_, to uint64) (common.Hash, bool, error) {
	h, ok := r.anchors[to]
	return h, ok, nil
}

// Actual canonical source identities, a log-free terminal block, and hot
// receipt rows removed before rewriting. Runtime must still serve the same
// externalized logs through both migration and generic from-cold publication.
func TestEventRewriteRetainsAnchorsAfterHotLogsPruned(t *testing.T) {
	for _, mode := range []string{"migrate-merge", "migrate-preserve-index", "from-reader-v2", "from-reader-v4"} {
		t.Run(mode, func(t *testing.T) {
			r, chain := independentEventFixture(t)
			db := chain.db.(*rawdb.ChainDB)
			// Block 4 deliberately has a valid receipt and no log row.
			block, infos := coldBuilderEventLogBlock(t, 4, nil)
			if err := rawdb.WriteBlock(db, block); err != nil {
				t.Fatal(err)
			}
			if err := rawdb.WriteTransactionInfosByBlock(db, 4, infos); err != nil {
				t.Fatal(err)
			}
			if err := rawdb.WriteStageProgressWithHash(db, rawdb.StageFinish, 4, block.Hash()); err != nil {
				t.Fatal(err)
			}
			for _, span := range [][2]uint64{{1, 2}, {3, 4}} {
				if _, err := NewAggregator(r.cfg.Dir).BuildEventLogs(db, span[0], span[1]); err != nil {
					t.Fatal(err)
				}
			}
			for n := uint64(1); n <= 4; n++ {
				if err := rawdb.DeleteTransactionInfosByBlock(db, n); err != nil {
					t.Fatal(err)
				}
			}
			// Pin the original source before integration retires its old refs.
			source, err := OpenManager(r.cfg.Dir)
			if err != nil {
				t.Fatal(err)
			}
			source.SetEventLogCanonicalVerifier(NewCanonicalEventLogVerifier(chain))
			if covered, err := source.EventLogRangeCovered(1, 4); err != nil || !covered {
				t.Fatal(covered, err)
			}
			var targetDir string
			switch mode {
			case "migrate-merge":
				targetDir = r.cfg.Dir
				if _, err := MigrateEventLogsV4(targetDir, EventLogV4MigrationOptions{FromBlock: 1, Merge: 2, Publish: true}); err != nil {
					t.Fatal(err)
				}
			case "migrate-preserve-index":
				targetDir = r.cfg.Dir
				manifest, err := LoadProductionManifest(targetDir)
				if err != nil {
					t.Fatal(err)
				}
				idx, err := BuildEventLogIndexSegmentFromEventLogSegments(targetDir, eventLogRefs(manifest), "log/rewrite-global.idx")
				if err != nil {
					t.Fatal(err)
				}
				if _, err := NewAggregator(targetDir).Integrate(0, 0, []SegmentRef{idx}); err != nil {
					t.Fatal(err)
				}
				if _, err := MigrateEventLogsV4(targetDir, EventLogV4MigrationOptions{FromBlock: 3, ToBlock: 4, ToBlockSet: true, Publish: true}); err != nil {
					t.Fatal(err)
				}
			default:
				targetDir = t.TempDir()
				version := uint32(EventLogSegmentVersion)
				if mode == "from-reader-v4" {
					version = EventLogSegmentV4Version
				}
				if _, err := NewAggregator(targetDir).BuildEventLogsFromReaderWithBuildOptions(source, 1, 4, EventLogBuildOptions{Version: version}); err != nil {
					t.Fatal(err)
				}
			}
			manager, err := OpenManager(targetDir)
			if err != nil {
				t.Fatal(err)
			}
			manager.SetEventLogCanonicalVerifier(NewCanonicalEventLogVerifier(chain))
			if covered, err := manager.EventLogIndexedRangeCovered(1, 4); err != nil || !covered {
				t.Fatal("rewrite dropped runtime authority", covered, err)
			}
			var seen uint64
			if err := manager.IterateEventLogs(1, 4, EventLogFilter{}, func(row EventLog) (bool, error) {
				seen++
				if row.BlockNum != seen || !bytes.Equal(row.Log.Data, []byte{byte(seen)}) {
					t.Fatalf("wrong rewritten event %+v", row)
				}
				return true, nil
			}); err != nil || seen != 3 {
				t.Fatal(seen, err)
			}
			manifest, err := LoadProductionManifest(targetDir)
			if err != nil {
				t.Fatal(err)
			}
			last := eventLogRefs(manifest)[len(eventLogRefs(manifest))-1]
			if last.SourceBlockHash != block.Hash().Hex() {
				t.Fatal("log-free terminal inherited wrong source identity", last.SourceBlockHash)
			}
		})
	}
}

func TestEventRewriteRejectsMissingPartialAndChangedSourceProof(t *testing.T) {
	r, chain := independentEventFixture(t)
	if _, err := NewAggregator(r.cfg.Dir).BuildEventLogs(chain.db.(*rawdb.ChainDB), 1, 4); err != nil {
		t.Fatal(err)
	}
	manager, err := OpenManager(r.cfg.Dir)
	if err != nil {
		t.Fatal(err)
	}
	for _, end := range []uint64{1, 3} {
		out := t.TempDir()
		if _, err := NewAggregator(out).BuildEventLogsFromReaderWithBuildOptions(manager, 1, end, EventLogBuildOptions{Version: EventLogSegmentV4Version}); err == nil {
			t.Fatal("partial end manufactured a source identity", end)
		}
		if _, err := os.Stat(out + "/" + ManifestFile); !os.IsNotExist(err) {
			t.Fatal("partial proof published", err)
		}
	}
	// An exact terminal alone does not permit dropping the source prefix in
	// a same-directory rewrite of only the end of a wider active file.
	if _, err := NewAggregator(r.cfg.Dir).BuildEventLogsFromReaderWithBuildOptions(manager, 3, 4, EventLogBuildOptions{Version: EventLogSegmentV4Version}); err == nil {
		t.Fatal("partial rewrite retired a wider active source")
	}
	manifest, err := LoadProductionManifest(r.cfg.Dir)
	if err != nil {
		t.Fatal(err)
	}
	for i := range manifest.Segments {
		if manifest.Segments[i].Kind == SegmentEventLog {
			manifest.Segments[i].SourceBlockHash = ""
		}
	}
	if err := PublishManifest(r.cfg.Dir, manifest); err != nil {
		t.Fatal(err)
	}
	if _, err := MigrateEventLogsV4(r.cfg.Dir, EventLogV4MigrationOptions{FromBlock: 1, Merge: 1, Publish: true}); err == nil {
		t.Fatal("unanchored migration published")
	}
	reader := &changingEventAnchorReader{anchoredEventLogRowsReader: anchoredEventLogRowsReader{anchors: map[uint64]common.Hash{1: {1}}}}
	if _, err := NewAggregator(t.TempDir()).BuildEventLogsFromReaderWithBuildOptions(reader, 1, 1, EventLogBuildOptions{Version: EventLogSegmentV4Version}); err == nil {
		t.Fatal("changing source proof published")
	}
	if _, err := NewAggregator(t.TempDir()).BuildEventLogsFromReader(eventLogRowsReader{}, 1, 1); err == nil {
		t.Fatal("reader without source proof published")
	}
}

type changingEventAnchorReader struct {
	anchoredEventLogRowsReader
	calls int
}

func (r *changingEventAnchorReader) EventLogSourceBlockHash(from, to uint64) (common.Hash, bool, error) {
	r.calls++
	if r.calls > 1 {
		return common.Hash{}, false, errors.New("source replaced")
	}
	return r.anchoredEventLogRowsReader.EventLogSourceBlockHash(from, to)
}

func TestEventRewriteRejectsCrossingIndexBeforeConstruction(t *testing.T) {
	r, chain := independentEventFixture(t)
	for _, span := range [][2]uint64{{1, 2}, {3, 4}} {
		if _, err := NewAggregator(r.cfg.Dir).BuildEventLogs(chain.db.(*rawdb.ChainDB), span[0], span[1]); err != nil {
			t.Fatal(err)
		}
	}
	before, err := LoadProductionManifest(r.cfg.Dir)
	if err != nil {
		t.Fatal(err)
	}
	index, err := BuildEventLogIndexSegmentFromEventLogSegments(r.cfg.Dir, eventLogRefs(before), "log/crossing-rewrite.idx")
	if err != nil {
		t.Fatal(err)
	}
	before, err = NewAggregator(r.cfg.Dir).Integrate(0, 0, []SegmentRef{index})
	if err != nil {
		t.Fatal(err)
	}
	manager, err := OpenManager(r.cfg.Dir)
	if err != nil {
		t.Fatal(err)
	}
	manager.SetEventLogCanonicalSource(chain)
	reader := &eventRewriteTrackingReader{Manager: manager}
	files, err := os.ReadDir(r.cfg.Dir + "/log")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := NewAggregator(r.cfg.Dir).BuildEventLogsFromReaderWithBuildOptions(reader, 3, 4, EventLogBuildOptions{Version: EventLogSegmentV4Version}); err == nil || !strings.Contains(err.Error(), string(SegmentEventLogIndex)) {
		t.Fatal("crossing lookup was not rejected", err)
	}
	if reader.iterations != 0 {
		t.Fatal("candidate constructed before crossing-index rejection", reader.iterations)
	}
	afterFiles, err := os.ReadDir(r.cfg.Dir + "/log")
	if err != nil || len(afterFiles) != len(files) {
		t.Fatal("rejected rewrite wrote candidate files", err)
	}
	after, err := LoadProductionManifest(r.cfg.Dir)
	if err != nil || !reflect.DeepEqual(before.Segments, after.Segments) || before.Generation != after.Generation {
		t.Fatal("rejected rewrite changed catalog", err)
	}
	for _, span := range [][2]uint64{{1, 2}, {1, 4}} {
		if covered, err := manager.EventLogIndexedRangeCovered(span[0], span[1]); err != nil || !covered {
			t.Fatal("crossing rewrite lost prior indexed coverage", span, covered, err)
		}
	}
}

type eventRewriteTrackingReader struct {
	*Manager
	iterations int
}

func (r *eventRewriteTrackingReader) IterateEventLogs(from, to uint64, filter EventLogFilter, fn func(EventLog) (bool, error)) error {
	r.iterations++
	return r.Manager.IterateEventLogs(from, to, filter, fn)
}
