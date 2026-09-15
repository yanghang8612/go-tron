package rawdb_test

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/tronprotocol/go-tron/core/pointread"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/state/snapshots"
)

// The interface embedding retains fixed snapshot/read/iterator behavior while
// hiding the optional owned-Get guarantee. Thus the unchanged ordinary fallback
// still performs the pre-optimization defensive copy, on exactly the same input.
// It exposes no GetWithPresence; the underlying snapshot does not have one.
type defensiveCopyHistoryView struct{ rawdb.StateHistoryReadView }

func TestStateHistoryOwnedReadViewDirectAndIndependent(t *testing.T) {
	db, from, _, _ := rawdb.OwnedReadBenchmarkFixtureForTest(t, "HighReuseRaw")
	view, release, err := rawdb.AcquireStateHistoryReadView(db)
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	capability, ok := view.(pointread.OwnedKeyValueReader)
	if !ok || !capability.GetReturnsOwnedBytes() {
		t.Fatal("ordinary history factory lost owned snapshot capability")
	}
	nested, nestedRelease, err := rawdb.AcquireStateHistoryReadView(view)
	if err != nil || nested != view {
		t.Fatal("nested history view changed", err)
	}
	defer nestedRelease()
	// The caller's returned block rows remain valid after another complete read.
	first, found, err := rawdb.ReadStateDomainChange(view, from, 1)
	if err != nil || !found {
		t.Fatal("first history read", found, err)
	}
	second, found, err := rawdb.ReadStateDomainChange(view, from, 1)
	if err != nil || !found {
		t.Fatal("second history read", found, err)
	}
	before := second.Prev[0]
	first.Prev[0] ^= 0xff
	if second.Prev[0] != before {
		t.Fatal("independent shared history outputs alias")
	}
}

// Measures complete construction: new fixed snapshot, two authenticated scans,
// V6 key dictionary/records, CDC compression, companion files and self-checks.
// Controlled 16-block inputs are not production distributions; the outer
// manifest/pruning lifecycle is not included. No authentication or I/O is elided.
func BenchmarkStateHistoryOwnedColdBuild(b *testing.B) {
	for _, shape := range []string{"Unique", "LowReuse", "HighReuseRaw", "HighReuseSnappy", "OverBudget"} {
		b.Run(shape, func(b *testing.B) {
			db, from, to, total := rawdb.OwnedReadBenchmarkFixtureForTest(b, shape)
			for _, owned := range []bool{false, true} {
				name := "DefensiveCopy"
				if owned {
					name = "OwnedSnapshot"
				}
				b.Run(name, func(b *testing.B) {
					dir := b.TempDir()
					b.ReportAllocs()
					b.SetBytes(int64(total))
					for b.Loop() {
						view, release, err := rawdb.AcquireStateHistoryReadView(db)
						if err != nil {
							b.Fatal(err)
						}
						if !owned {
							view = defensiveCopyHistoryView{view}
						}
						refs, err := snapshots.BuildStateDomainChangeHistorySegmentsFromDBByBlockRange(view, dir, from, to, from, to, "state-domain-change-owned-bench.seg")
						closeErr := release()
						if err != nil || closeErr != nil || len(refs) != 3 {
							b.Fatalf("refs=%d err=%v close=%v", len(refs), err, closeErr)
						}
					}
				})
			}
		})
	}
}

type ownershipObservedHistoryView struct {
	rawdb.StateHistoryReadView
	owned  bool
	trace  []string
	failAt int
}

func (v *ownershipObservedHistoryView) GetReturnsOwnedBytes() bool { return v.owned }
func (v *ownershipObservedHistoryView) Has(key []byte) (bool, error) {
	v.trace = append(v.trace, "Has:"+string(key))
	if v.failAt == len(v.trace) {
		return false, errOwnedColdReadFixture
	}
	return v.StateHistoryReadView.Has(key)
}
func (v *ownershipObservedHistoryView) Get(key []byte) ([]byte, error) {
	v.trace = append(v.trace, "Get:"+string(key))
	if v.failAt == len(v.trace) {
		return nil, errOwnedColdReadFixture
	}
	return v.StateHistoryReadView.Get(key)
}

var errOwnedColdReadFixture = errors.New("owned cold read fixture")

func TestStateHistoryOwnedColdBuildEquivalent(t *testing.T) {
	t.Setenv("GTRON_HISTORY_COMPRESSION_FORMAT", "3")
	for _, shape := range []string{"HighReuseRaw", "HighReuseSnappy"} {
		t.Run(shape, func(t *testing.T) {
			db, from, to, _ := rawdb.OwnedReadBenchmarkFixtureForTest(t, shape)
			for _, failAt := range []int{0, 1, 2, 17} {
				t.Run(fmt.Sprint(failAt), func(t *testing.T) {
					var expectedRefs []snapshots.SegmentRef
					var expectedTrace []string
					var expectedErr string
					var expectedFiles [][]byte
					for _, owned := range []bool{false, true} {
						view, release, err := rawdb.AcquireStateHistoryReadView(db)
						if err != nil {
							t.Fatal(err)
						}
						observed := &ownershipObservedHistoryView{StateHistoryReadView: view, owned: owned, failAt: failAt}
						dir := t.TempDir()
						refs, buildErr := snapshots.BuildStateDomainChangeHistorySegmentsFromDBByBlockRange(observed, dir, from, to, from, to, "state-domain-change-owned-equivalence.seg")
						if err := release(); err != nil {
							t.Fatal(err)
						}
						var files [][]byte
						for _, ref := range refs {
							data, err := os.ReadFile(filepath.Join(dir, ref.Path))
							if err != nil {
								t.Fatal(err)
							}
							files = append(files, data)
						}
						if failAt == 0 && (buildErr != nil || len(refs) != 3) {
							t.Fatalf("complete builder: %d %v", len(refs), buildErr)
						}
						if failAt > 0 && !errors.Is(buildErr, errOwnedColdReadFixture) {
							t.Fatalf("read failure not preserved: %v", buildErr)
						}
						if !owned {
							expectedRefs, expectedTrace, expectedErr, expectedFiles = refs, observed.trace, fmt.Sprint(buildErr), files
						} else if !reflect.DeepEqual(refs, expectedRefs) || !reflect.DeepEqual(observed.trace, expectedTrace) || fmt.Sprint(buildErr) != expectedErr || !reflect.DeepEqual(files, expectedFiles) {
							t.Fatal("complete files, refs, read prefix or error differ")
						}
					}
				})
			}
		})
	}
}
