package snapshots

import (
	"bytes"
	"errors"
	"fmt"
	"math/rand"
	"reflect"
	"slices"
	"testing"

	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/state/kvdomains"
)

func frontierTestRef(kind SegmentKind, from, to uint64) SegmentRef {
	return SegmentRef{Dataset: SegmentDatasetEventLog, Kind: kind, FromTxNum: from, ToTxNum: to,
		Path: fmt.Sprintf("%s-%020d-%020d", kind, from, to), Size: 17, Checksum: "metadata-only"}
}

func frontierTestPair(from, to uint64) []SegmentRef {
	return []SegmentRef{frontierTestRef(SegmentEventLog, from, to), frontierTestRef(SegmentEventLogIndex, from, to)}
}

func checkEventFrontierOracle(t *testing.T, manifest *Manifest) (uint64, bool) {
	t.Helper()
	before := cloneManifest(manifest)
	if before != nil {
		before.lookup = manifest.lookup
		before.Segments = slices.Clone(manifest.Segments)
		before.Retired = slices.Clone(manifest.Retired)
	}
	got, ok := eventLogBuildBlockFromManifest(manifest)
	want, wantOK := frontier833EventLogBuildBlockFromManifest(manifest)
	if got != want || ok != wantOK {
		t.Fatalf("frontier = %d/%v, frozen = %d/%v, manifest=%+v", got, ok, want, wantOK, manifest)
	}
	for _, end := range []uint64{0, 1, 2, 7, 30, 100, ^uint64(0)} {
		for _, batch := range []uint64{0, 1, 5, ^uint64(0)} {
			a, aOK := nextEventLogCatchupRange(manifest, end, batch)
			b, bOK := frontier833NextEventLogCatchupRange(manifest, end, batch)
			if a != b || aOK != bOK {
				t.Fatalf("planner end=%d batch=%d: %+v/%v != %+v/%v", end, batch, a, aOK, b, bOK)
			}
		}
	}
	if !reflect.DeepEqual(manifest, before) {
		t.Fatal("frontier/planner mutated manifest or reference order")
	}
	return got, ok
}

func TestEventFrontierAllocationBoundaries(t *testing.T) {
	max := ^uint64(0)
	tests := []struct {
		name string
		refs []SegmentRef
		want uint64
		ok   bool
	}{
		{"empty", nil, 0, false},
		{"event-only", []SegmentRef{frontierTestRef(SegmentEventLog, 1, 8)}, 0, false},
		{"index-only", []SegmentRef{frontierTestRef(SegmentEventLogIndex, 1, 8)}, 0, false},
		{"zero-only", frontierTestPair(0, 0), 0, false},
		{"starts-zero", frontierTestPair(0, 8), 8, true},
		{"starts-two", frontierTestPair(2, 8), 0, false},
		{"one", frontierTestPair(1, 1), 1, true},
		{"event-gap", append(frontierTestPair(1, 3), frontierTestPair(5, 8)...), 3, true},
		{"index-shorter", []SegmentRef{frontierTestRef(SegmentEventLog, 1, 10), frontierTestRef(SegmentEventLogIndex, 1, 5)}, 5, true},
		{"index-longer", []SegmentRef{frontierTestRef(SegmentEventLog, 1, 5), frontierTestRef(SegmentEventLogIndex, 1, 10)}, 5, true},
		{"overlap", append(frontierTestPair(1, 5), frontierTestPair(3, 10)...), 10, true},
		{"inverted", append(frontierTestPair(1, 5), frontierTestPair(9, 7)...), 5, true},
		{"max", frontierTestPair(1, max), max, true},
		{"max-last-only", frontierTestPair(max, max), 0, false},
		{"max-joined", append(frontierTestPair(1, max-1), frontierTestPair(max, max)...), max, true},
		{"max-index-cap", []SegmentRef{frontierTestRef(SegmentEventLog, 1, max-1), frontierTestRef(SegmentEventLogIndex, 1, max)}, max - 1, true},
		{"max-event-cap", []SegmentRef{frontierTestRef(SegmentEventLog, 1, max), frontierTestRef(SegmentEventLogIndex, 1, max-1)}, max - 1, true},
	}
	checkEventFrontierOracle(t, nil)
	checkEventFrontierOracle(t, &Manifest{Segments: []SegmentRef{}, Retired: []SegmentRef{}})
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := &Manifest{Generation: 19, Segments: tt.refs, Retired: frontierTestPair(1, max), Progress: &Progress{HotPruneTxNum: 7}}
			got, ok := checkEventFrontierOracle(t, m)
			if got != tt.want || ok != tt.ok {
				t.Fatalf("frontier = %d/%v, want %d/%v", got, ok, tt.want, tt.ok)
			}
		})
	}
	for _, kind := range []SegmentKind{SegmentEventLog, SegmentEventLogIndex} {
		for _, dataset := range []SegmentDataset{"", SegmentDatasetStateDomainChange, SegmentDatasetKVLatest} {
			refs := frontierTestPair(1, 9)
			for i := range refs {
				if refs[i].Kind == kind {
					refs[i].Dataset = dataset
				}
			}
			if _, ok := checkEventFrontierOracle(t, &Manifest{Segments: refs}); ok {
				t.Fatalf("accepted %s with dataset %q", kind, dataset)
			}
		}
	}
}

func TestEventFrontierAllocationOrderingAndPlannerRepair(t *testing.T) {
	refs := append(frontierTestPair(1, 3), frontierTestPair(4, 8)...)
	refs = append(refs, frontierTestPair(7, 10)...)
	for i := range refs {
		refs[i].Domain = kvdomains.KVDomain(i % 3)
		refs[i].Path = fmt.Sprintf("reverse-path-%d", len(refs)-i)
	}
	// Identical paths and ranges, different remaining identity fields. Sorting
	// tie-breaks cannot change a bounds-only result or mutate caller metadata.
	refs = append(refs, refs[0], refs[1])
	refs[len(refs)-1].Checksum = "different-checksum"
	slices.Reverse(refs)
	if got, ok := checkEventFrontierOracle(t, &Manifest{Segments: refs}); got != 10 || !ok {
		t.Fatalf("duplicate/ordering frontier=%d/%v", got, ok)
	}
	m := &Manifest{Segments: []SegmentRef{
		frontierTestRef(SegmentEventLog, 1, 3), frontierTestRef(SegmentEventLogIndex, 1, 3),
		frontierTestRef(SegmentEventLog, 4, 10), frontierTestRef(SegmentEventLogIndex, 6, 12),
		frontierTestRef(SegmentEventLog, 11, 15),
	}}
	checkEventFrontierOracle(t, m)
	got, ok := nextEventLogCatchupRange(m, 20, 1)
	if got != (coldSidecarBlockRange{from: 4, to: 15}) || !ok {
		t.Fatalf("connected repair=%+v/%v; must preserve whole component beyond batch", got, ok)
	}
}

func TestEventFrontierAllocationRandomizedOracle(t *testing.T) {
	rng := rand.New(rand.NewSource(833))
	values := []uint64{0, 1, 2, 3, 5, 7, 10, 20, ^uint64(0) - 1, ^uint64(0)}
	for trial := 0; trial < 400; trial++ {
		m := &Manifest{Retired: frontierTestPair(1, ^uint64(0))}
		for n := rng.Intn(35); n > 0; n-- {
			kind := []SegmentKind{SegmentEventLog, SegmentEventLogIndex, SegmentHistory, SegmentLatest}[rng.Intn(4)]
			ref := frontierTestRef(kind, values[rng.Intn(len(values))], values[rng.Intn(len(values))])
			ref.Dataset = []SegmentDataset{SegmentDatasetEventLog, SegmentDatasetEventLog, "", SegmentDatasetStateDomainChange}[rng.Intn(4)]
			ref.Path = fmt.Sprintf("duplicate-path-%d", rng.Intn(3))
			ref.Domain = kvdomains.KVDomain(rng.Intn(4))
			m.Segments = append(m.Segments, ref)
		}
		checkEventFrontierOracle(t, m)
	}
}

type frontierTraceDB struct {
	base  ethdb.KeyValueStore
	trace []string
	fail  int
}

var errFrontierInjected = errors.New("frontier injected database failure")

func (db *frontierTraceDB) operation(kind string, key, value []byte) error {
	db.trace = append(db.trace, fmt.Sprintf("%s:%x:%x", kind, key, value))
	if db.fail > 0 && len(db.trace) == db.fail {
		return errFrontierInjected
	}
	return nil
}

func (db *frontierTraceDB) Has(key []byte) (bool, error) {
	if err := db.operation("Has", key, nil); err != nil {
		return false, err
	}
	return db.base.Has(key)
}
func (db *frontierTraceDB) Get(key []byte) ([]byte, error) {
	if err := db.operation("Get", key, nil); err != nil {
		return nil, err
	}
	return db.base.Get(key)
}
func (db *frontierTraceDB) Put(key, value []byte) error {
	if err := db.operation("Put", key, value); err != nil {
		return err
	}
	return db.base.Put(key, value)
}
func (db *frontierTraceDB) Delete(key []byte) error {
	if err := db.operation("Delete", key, nil); err != nil {
		return err
	}
	return db.base.Delete(key)
}

type frontierWriteOnly struct{ ethdb.KeyValueWriter }
type frontierReadOnly struct{ ethdb.KeyValueReader }

func frontierDatabaseRows(t *testing.T, db ethdb.KeyValueStore) []string {
	t.Helper()
	it := db.NewIterator(nil, nil)
	defer it.Release()
	var rows []string
	for it.Next() {
		rows = append(rows, fmt.Sprintf("%x:%x", it.Key(), it.Value()))
	}
	if err := it.Error(); err != nil {
		t.Fatal(err)
	}
	return rows
}

func TestEventFrontierAllocationStageOracle(t *testing.T) {
	m := &Manifest{Segments: frontierTestPair(1, 9)}
	for _, mode := range []string{"canonical", "fallback", "missing", "corrupt-body", "corrupt-range", "zero-hash", "wrong-body-number"} {
		t.Run(mode, func(t *testing.T) {
			fixture := func() *frontierTraceDB {
				base := rawdb.NewMemoryDatabase()
				t.Cleanup(func() { base.Close() })
				if mode == "canonical" || mode == "corrupt-body" || mode == "wrong-body-number" {
					capture := &frontierTraceDB{base: base}
					if err := rawdb.WriteBlock(capture, aggregatorTestBlock(9)); err != nil {
						t.Fatal(err)
					}
					// Find the body key through the accessor's write order, without
					// reimplementing the production key encoding in this test.
					it := base.NewIterator(nil, nil)
					var bodyKey []byte
					for it.Next() {
						if bytes.Equal(it.Value(), mustFrontierBlockBytes(t, 9)) {
							bodyKey = bytes.Clone(it.Key())
							break
						}
					}
					it.Release()
					if len(bodyKey) == 0 {
						t.Fatal("body key not found")
					}
					if mode == "corrupt-body" {
						base.Put(bodyKey, []byte{0xff})
					}
					if mode == "wrong-body-number" {
						base.Put(bodyKey, mustFrontierBlockBytes(t, 10))
					}
				}
				if mode == "fallback" || mode == "zero-hash" || mode == "corrupt-range" {
					hash := common.BytesToHash([]byte{9})
					if mode == "zero-hash" {
						hash = common.Hash{}
					}
					if err := rawdb.WriteStateTxRange(base, 9, hash, 20, 30); err != nil {
						t.Fatal(err)
					}
					if mode == "corrupt-range" {
						it := base.NewIterator(nil, nil)
						it.Next()
						key := bytes.Clone(it.Key())
						it.Release()
						base.Put(key, []byte{0xff})
					}
				}
				return &frontierTraceDB{base: base}
			}
			probe := fixture()
			_ = frontier833WriteEventLogBuildStage(probe, m)
			for fail := 0; fail <= len(probe.trace); fail++ {
				a, b := fixture(), fixture()
				a.fail, b.fail = fail, fail
				errA := writeEventLogBuildStage(a, m)
				errB := frontier833WriteEventLogBuildStage(b, m)
				if fmt.Sprint(errA) != fmt.Sprint(errB) || !reflect.DeepEqual(a.trace, b.trace) || !reflect.DeepEqual(frontierDatabaseRows(t, a.base), frontierDatabaseRows(t, b.base)) {
					t.Fatalf("fail=%d: errors %v / %v, trace %v / %v", fail, errA, errB, a.trace, b.trace)
				}
				if fail == 0 && (mode == "canonical" || mode == "fallback") && errA != nil {
					t.Fatal(errA)
				}
				if fail == 0 && mode != "canonical" && mode != "fallback" && errA == nil {
					t.Fatalf("%s accepted", mode)
				}
			}
		})
	}
	base := &frontierTraceDB{base: rawdb.NewMemoryDatabase()}
	defer base.base.Close()
	for _, db := range []any{nil, struct{}{}, frontierReadOnly{base}, frontierWriteOnly{base}, base} {
		for _, manifest := range []*Manifest{nil, {}, m} {
			a, aOK, aErr := eventLogBuildStageProgress(db, manifest)
			b, bOK, bErr := frontier833EventLogBuildStageProgress(db, manifest)
			if a != b || aOK != bOK || fmt.Sprint(aErr) != fmt.Sprint(bErr) {
				t.Fatalf("capability mismatch: %T", db)
			}
		}
	}
	base.trace = nil
	if err := writeEventLogBuildStage(base, &Manifest{Segments: frontierTestPair(2, 9)}); err != nil || len(base.trace) != 0 {
		t.Fatalf("uncovered stage touched db: %v %v", err, base.trace)
	}
}

func mustFrontierBlockBytes(t *testing.T, number uint64) []byte {
	t.Helper()
	b, err := aggregatorTestBlock(number).Marshal()
	if err != nil {
		t.Fatal(err)
	}
	return b
}
