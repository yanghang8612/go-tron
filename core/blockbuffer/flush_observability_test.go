package blockbuffer

import (
	"bytes"
	"errors"
	"testing"

	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/metrics"
	"github.com/tronprotocol/go-tron/core/rawdb"
)

func TestFlushStopReasons(t *testing.T) {
	owner := new(Buffer)
	point := func(key string) *layer {
		l := newLayer(bufHash(1), 1)
		owner.putIntoString(l, key, bytes.Repeat([]byte{1}, 100))
		return l
	}
	ranged := newLayer(bufHash(2), 2)
	ranges := []layerRangeDelete{{"r", "s"}}
	ranged.rangeDeletes.Store(&ranges)
	for _, tc := range []struct {
		name     string
		layers   []*layer
		limits   flushGroupLimits
		end      int
		reason   flushStopReason
		hits     uint8
		oversize bool
	}{
		{"extended_exhaustion", []*layer{point("a"), point("a")}, flushGroupLimits{150, 200, 1000, 1000}, 2, flushStopSelectedEnd, 0, false},
		{"source_value", []*layer{point("a"), point("a")}, flushGroupLimits{150, 200, 150, 1000}, 1, flushStopSourceCap, flushCapSourceValue, false},
		{"source_encoded", []*layer{point("a"), point("a")}, flushGroupLimits{150, 200, 1000, 200}, 1, flushStopSourceCap, flushCapSourceEncoded, false},
		{"source_both_before_final", []*layer{point("a"), point("b")}, flushGroupLimits{150, 200, 150, 200}, 1, flushStopSourceCap, flushCapSourceValue | flushCapSourceEncoded, false},
		{"final_value", []*layer{point("a"), point("b")}, flushGroupLimits{150, 1000, 1000, 1000}, 1, flushStopFinalCap, flushCapFinalValue, false},
		{"final_encoded", []*layer{point("a"), point("b")}, flushGroupLimits{1000, 200, 1000, 1000}, 1, flushStopFinalCap, flushCapFinalEncoded, false},
		{"final_both", []*layer{point("a"), point("b")}, flushGroupLimits{150, 200, 1000, 1000}, 1, flushStopFinalCap, flushCapFinalValue | flushCapFinalEncoded, false},
		{"own_range", []*layer{ranged, point("a")}, flushGroupLimits{150, 200, 1000, 1000}, 1, flushStopOwnRange, 0, false},
		{"next_range", []*layer{point("a"), ranged}, flushGroupLimits{150, 200, 1000, 1000}, 1, flushStopNextRange, 0, false},
		{"extension_next_range", []*layer{point("a"), point("a"), ranged}, flushGroupLimits{150, 200, 1000, 1000}, 2, flushStopNextRange, 0, false},
		{"oversized_single", []*layer{point("a")}, flushGroupLimits{50, 60, 1000, 1000}, 1, flushStopSelectedEnd, 0, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			sizes := make([]layerBatchSize, len(tc.layers))
			for i, l := range tc.layers {
				v, e, o := layerWriteStats(l)
				sizes[i] = layerBatchSize{v, e, o}
			}
			p := selectFlushGroup(tc.layers, sizes, 0, tc.limits)
			defer returnFlushMergedOps(p.merged)
			if p.end != tc.end || p.stopReason != tc.reason || p.capHits != tc.hits || p.oversize != tc.oversize {
				t.Fatalf("end/reason/hits/oversize=%d/%d/%x/%v, want%d/%d/%x/%v", p.end, p.stopReason, p.capHits, p.oversize, tc.end, tc.reason, tc.hits, tc.oversize)
			}
			if tc.name == "extended_exhaustion" && (p.outputValue != 101 || p.inputValue != 202) {
				t.Fatalf("coalesced sizes=%d/%d", p.outputValue, p.inputValue)
			}
		})
	}
}

type observedFailureStore struct {
	ethdb.KeyValueStore
	writes, failAt int
	partial        bool
}
type observedFailureBatch struct {
	ethdb.Batch
	store *observedFailureStore
}

func (s *observedFailureStore) NewBatchWithSize(n int) ethdb.Batch {
	return &observedFailureBatch{s.KeyValueStore.NewBatchWithSize(n), s}
}
func (b *observedFailureBatch) DeleteRange(start, end []byte) error {
	return b.Batch.(ethdb.KeyValueRangeDeleter).DeleteRange(start, end)
}
func (b *observedFailureBatch) Write() error {
	b.store.writes++
	if b.store.writes == b.store.failAt {
		if b.store.partial {
			if err := b.Batch.Write(); err != nil {
				return err
			}
		}
		return errForcedBatchWrite
	}
	return b.Batch.Write()
}
func TestFlushObservationFailedPrefixAndRetry(t *testing.T) {
	for _, partial := range []bool{false, true} {
		t.Run(map[bool]string{false: "before_apply", true: "after_apply"}[partial], func(t *testing.T) {
			disk := rawdb.NewMemoryDatabase()
			defer disk.Close()
			store := &observedFailureStore{KeyValueStore: disk, failAt: 2, partial: partial}
			buf := New(disk)
			for n := 1; n <= 3; n++ {
				buf.BeginBlock(bufHash(byte(n)), uint64(n))
				if n == 1 {
					if err := buf.DeleteRange([]byte("r"), []byte("s")); err != nil {
						t.Fatal(err)
					}
				}
				if err := buf.Put([]byte{byte('a' + n)}, []byte{byte(n)}); err != nil {
					t.Fatal(err)
				}
				buf.CommitBlock()
			}
			input, output, groups := flushCallInput.sum.Snapshot().Count(), flushCallOutput.sum.Snapshot().Count(), flushCallGroups.sum.Snapshot().Count()
			errorsBefore, solidBefore := flushCallErrors.Snapshot().Count(), flushCallSolidCutoff.Snapshot().Count()
			plannedBefore := flushPlanStopCounters[flushStopSelectedEnd].groups.Snapshot().Count()
			committedBefore := flushCommittedStopCounters[flushStopSelectedEnd].groups.Snapshot().Count()
			if err := buf.FlushUpTo(2, store); !errors.Is(err, errForcedBatchWrite) {
				t.Fatalf("err=%v", err)
			}
			// First group: one range r..s (5 record bytes) and b=1 (5).
			// Second selected group: c=2 (5), even when backend applied before error.
			if got := flushCallInput.sum.Snapshot().Count() - input; got != 15 {
				t.Fatalf("eligible record bytes=%d want15", got)
			}
			if got := flushCallOutput.sum.Snapshot().Count() - output; got != 10 {
				t.Fatalf("committed record bytes=%d want10", got)
			}
			if got := flushCallGroups.sum.Snapshot().Count() - groups; got != 1 {
				t.Fatalf("committed groups=%d", got)
			}
			if flushCallErrors.Snapshot().Count()-errorsBefore != 1 || flushCallSolidCutoff.Snapshot().Count()-solidBefore != 1 {
				t.Fatal("failed/solid boundary missing")
			}
			if flushPlanStopCounters[flushStopSelectedEnd].groups.Snapshot().Count()-plannedBefore != 1 || flushCommittedStopCounters[flushStopSelectedEnd].groups.Snapshot().Count() != committedBefore {
				t.Fatal("failed plan claimed committed")
			}
			if len(buf.PendingBlocks()) != 2 {
				t.Fatal("success prefix not dropped")
			}
			if ok, _ := disk.Has([]byte("c")); ok != partial {
				t.Fatalf("partial backend state=%v", ok)
			}
			store.failAt = 0
			if err := buf.FlushUpTo(2, store); err != nil {
				t.Fatal(err)
			}
			if got := flushCallOutput.sum.Snapshot().Count() - output; got != 15 {
				t.Fatalf("retry total committed bytes=%d", got)
			}
			if len(buf.PendingBlocks()) != 1 {
				t.Fatal("retry dropped future layer")
			}
			if got, err := disk.Get([]byte("c")); err != nil || !bytes.Equal(got, []byte{2}) {
				t.Fatalf("c=%x err%v", got, err)
			}
		})
	}
}
func TestFlushObservationBucketsAndRegistration(t *testing.T) {
	d := flushDistribution{bounds: []int64{0, 8}, buckets: []*metrics.Counter{metrics.NewCounter(), metrics.NewCounter(), metrics.NewCounter()}, sum: metrics.NewCounter(), max: metrics.NewGauge()}
	for _, n := range []int64{0, 1, 8, 9} {
		d.observe(n)
	}
	if d.buckets[0].Snapshot().Count() != 1 || d.buckets[1].Snapshot().Count() != 2 || d.buckets[2].Snapshot().Count() != 1 || d.sum.Snapshot().Count() != 18 || d.max.Snapshot().Value() != 9 {
		t.Fatal("non-cumulative bucket/sum/max mismatch")
	}
	for _, name := range []string{"blockbuffer/flush/call/eligible_layers/bucket/le_96", "blockbuffer/flush/call/eligible_input_encoded/bucket/le_134217728", "blockbuffer/flush/call/committed_output_encoded/max", "blockbuffer/flush/plan_stop/source_cap/groups", "blockbuffer/flush/cap_hit/source_encoded"} {
		if metrics.DefaultRegistry.Get(name) == nil {
			t.Fatalf("missing registered metric %s", name)
		}
	}
	disk := rawdb.NewMemoryDatabase()
	defer disk.Close()
	buf := New(disk)
	before := flushCallEmpty.Snapshot().Count()
	if err := buf.FlushUpTo(1, disk); err != nil {
		t.Fatal(err)
	}
	if flushCallEmpty.Snapshot().Count()-before != 1 {
		t.Fatal("empty canonical call missing")
	}
}
