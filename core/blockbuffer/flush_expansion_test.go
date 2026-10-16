package blockbuffer

import (
	"bytes"
	"errors"
	"testing"

	"github.com/ethereum/go-ethereum/metrics"
	"github.com/tronprotocol/go-tron/core/rawdb"
)

func TestFlushExpansionGuardBoundaries(t *testing.T) {
	p := flushGroupPlan{inputValue: 202, outputValue: 101, outputEncoded: 117, merged: &flushMergedOps{ops: map[string]mergedLayerOp{"a": {}}}}
	limits := flushGroupLimits{batchValue: 150, batchEncoded: 200, mergeValue: 1000, mergeEncoded: 1000, expandedValue: 300, expandedEncoded: 400}
	next := layerBatchSize{ops: maxPooledFlushWriteKeys - 1, encoded: 284}
	if !canExpandFlushGroup(&p, next, 199, 283, limits) {
		t.Fatal("exact ratio, row bound and max sizes must be accepted")
	}
	if canExpandFlushGroup(&p, layerBatchSize{ops: 1, encoded: 10}, 0, 10, limits) {
		t.Fatal("zero marginal elimination must be rejected even below maximum size")
	}
	for _, tc := range []struct {
		name               string
		input, ops, dv, de int
		limit              flushGroupLimits
	}{
		{"ratio_below_two", 201, next.ops, 0, 0, limits},
		{"row_upper_bound", 202, next.ops + 1, 0, 0, limits},
		{"value_above_max", 202, next.ops, 200, 0, limits},
		{"encoded_above_max", 202, next.ops, 0, 284, limits},
		{"negative_op_count", 202, -1, 0, 0, limits},
		{"disabled", 202, next.ops, 0, 0, flushGroupLimits{batchValue: 150, batchEncoded: 200}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			q := p
			q.inputValue = tc.input
			if canExpandFlushGroup(&q, layerBatchSize{ops: tc.ops, encoded: 1000}, tc.dv, tc.de, tc.limit) {
				t.Fatal("guard admitted rejected boundary")
			}
		})
	}
	for name, want := range map[string]int64{"base_value_bytes": 32 << 20, "base_encoded_bytes": 32 << 20, "max_value_bytes": 64 << 20, "max_encoded_bytes": 64 << 20, "source_value_bytes": 128 << 20, "source_encoded_bytes": 128 << 20, "expansion_min_input_output_ratio_ppm": 2000000, "expansion_max_entries": 262144} {
		g, ok := metrics.DefaultRegistry.Get("blockbuffer/flush/limit/" + name).(*metrics.Gauge)
		if !ok || g.Snapshot().Value() != want {
			t.Fatalf("limit gauge %s missing/wrong", name)
		}
	}
}
func TestFlushExpansionRechecksRatioAndKeepsBoundaries(t *testing.T) {
	owner := new(Buffer)
	point := func(k string, n int) *layer {
		l := newLayer(bufHash(1), 1)
		owner.putIntoString(l, k, bytes.Repeat([]byte{1}, n))
		return l
	}
	third := point("a", 100)
	owner.putIntoString(third, "b", bytes.Repeat([]byte{1}, 180))
	layers := []*layer{point("a", 100), point("a", 100), third, point("a", 100)}
	lim := flushGroupLimits{batchValue: 150, batchEncoded: 200, mergeValue: 1000, mergeEncoded: 1000, expandedValue: 300, expandedEncoded: 400}
	plan := func(ls []*layer, l flushGroupLimits) flushGroupPlan {
		sizes := make([]layerBatchSize, len(ls))
		for i, x := range ls {
			v, e, o := layerWriteStats(x)
			sizes[i] = layerBatchSize{v, e, o}
		}
		return selectFlushGroup(ls, sizes, 0, l)
	}
	p := plan(layers, lim)
	if p.end != 3 || !p.expanded || p.oversize || p.outputValue != 282 || p.stopReason != flushStopFinalCap {
		t.Fatalf("ratio must be rechecked after expansion: %+v", p)
	}
	returnFlushMergedOps(p.merged)
	pure := plan([]*layer{layers[0], layers[1], point("b", 80)}, lim)
	if pure.end != 2 || pure.expanded || pure.stopReason != flushStopFinalCap {
		t.Fatalf("prefix coverage admitted a purely new next layer: %+v", pure)
	}
	returnFlushMergedOps(pure.merged)
	l := lim
	l.mergeValue = 250
	p = plan(layers, l)
	if p.end != 2 || p.expanded || p.stopReason != flushStopSourceCap {
		t.Fatalf("source guard changed: %+v", p)
	}
	returnFlushMergedOps(p.merged)
	ranged := point("r", 1)
	rs := []layerRangeDelete{{"r", "s"}}
	ranged.rangeDeletes.Store(&rs)
	p = plan([]*layer{layers[0], layers[1], ranged}, lim)
	if p.end != 2 || p.expanded || p.stopReason != flushStopNextRange {
		t.Fatalf("crossed range: %+v", p)
	}
	returnFlushMergedOps(p.merged)
	p = plan([]*layer{ranged, layers[0]}, lim)
	if p.end != 1 || p.expanded || p.stopReason != flushStopOwnRange {
		t.Fatalf("range not isolated: %+v", p)
	}
	returnFlushMergedOps(p.merged)
	p = plan([]*layer{point("huge", 400)}, lim)
	if p.end != 1 || p.expanded || !p.oversize {
		t.Fatalf("single oversize tightened: %+v", p)
	}
	returnFlushMergedOps(p.merged)
}
func TestFlushExpansionPartialFailureRetryAndReorg(t *testing.T) {
	for _, partial := range []bool{false, true} {
		t.Run(map[bool]string{false: "before_apply", true: "after_apply"}[partial], func(t *testing.T) {
			disk := rawdb.NewMemoryDatabase()
			defer disk.Close()
			store := &observedFailureStore{KeyValueStore: disk, failAt: 2, partial: partial}
			buf := New(disk)
			a := bytes.Repeat([]byte{1}, 16<<20)
			a2 := bytes.Repeat([]byte{2}, 16<<20)
			b := bytes.Repeat([]byte{3}, 24<<20)
			for n := 1; n <= 6; n++ {
				buf.BeginBlock(bufHash(byte(n)), uint64(n))
				var err error
				switch n {
				case 1, 2, 3:
					err = buf.PutOwnedValue([]byte("a"), a)
				case 4:
					err = buf.PutOwnedValue([]byte("a"), a)
					if err == nil {
						err = buf.PutOwnedValue([]byte("b"), b)
					}
				case 5:
					err = buf.PutOwnedValue([]byte("a"), a2)
					if err == nil {
						err = buf.PutOwnedValue([]byte("c"), b)
					}
				case 6:
					err = buf.Put([]byte("future"), []byte("orphan"))
				}
				if err != nil {
					t.Fatal(err)
				}
				buf.CommitBlock()
			}
			pb, cb := flushExpandedPlanned.Snapshot().Count(), flushExpandedCommitted.Snapshot().Count()
			if err := buf.FlushUpTo(5, store); !errors.Is(err, errForcedBatchWrite) {
				t.Fatalf("error=%v", err)
			}
			if store.writes != 2 || len(buf.PendingBlocks()) != 2 {
				t.Fatalf("wrong committed prefix writes%d pending%v", store.writes, buf.PendingBlocks())
			}
			if flushExpandedPlanned.Snapshot().Count()-pb != 1 || flushExpandedCommitted.Snapshot().Count()-cb != 1 {
				t.Fatal("expanded planned/committed accounting changed")
			}
			want := a
			if partial {
				want = a2
			}
			if got, err := disk.Get([]byte("a")); err != nil || !bytes.Equal(got, want) {
				t.Fatalf("partial durable prefix incorrect: %v", err)
			}
			if got, err := buf.Get([]byte("a")); err != nil || !bytes.Equal(got, a2) {
				t.Fatalf("retry layer not readable: %v", err)
			}
			store.failAt = 0
			if err := buf.FlushUpTo(5, store); err != nil {
				t.Fatal(err)
			}
			if len(buf.PendingBlocks()) != 1 {
				t.Fatal("retry dropped future layer")
			}
			buf.DiscardBlock(bufHash(6))
			if ok, _ := buf.Has([]byte("future")); ok {
				t.Fatal("reorg future value survived")
			}
			if got, err := disk.Get([]byte("a")); err != nil || !bytes.Equal(got, a2) {
				t.Fatalf("retry result incorrect: %v", err)
			}
		})
	}
}
