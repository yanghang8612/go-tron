package blockbuffer

import (
	"strconv"

	"github.com/ethereum/go-ethereum/metrics"
)

// These reasons describe the actual selector boundary, not a hypothetical
// larger group. In particular, source rejection precedes final-size projection.
type flushStopReason uint8

const (
	flushStopSelectedEnd flushStopReason = iota
	flushStopOwnRange
	flushStopNextRange
	flushStopSourceCap
	flushStopFinalCap
	flushStopReasonCount
)

var flushStopNames = [...]string{"selected_end", "own_range", "next_range", "source_cap", "final_cap"}

const (
	flushCapSourceValue uint8 = 1 << iota
	flushCapSourceEncoded
	flushCapFinalValue
	flushCapFinalEncoded
)

type flushStopCounters struct{ groups, layers, input, output *metrics.Counter }

var flushPlanStopCounters, flushCommittedStopCounters = newFlushStopCounters("plan_stop"), newFlushStopCounters("committed_stop")

func newFlushStopCounters(kind string) [flushStopReasonCount]flushStopCounters {
	var result [flushStopReasonCount]flushStopCounters
	for i, name := range flushStopNames {
		prefix := "blockbuffer/flush/" + kind + "/" + name + "/"
		result[i] = flushStopCounters{metrics.NewRegisteredCounter(prefix+"groups", nil), metrics.NewRegisteredCounter(prefix+"layers", nil), metrics.NewRegisteredCounter(prefix+"input_bytes", nil), metrics.NewRegisteredCounter(prefix+"output_bytes", nil)}
	}
	return result
}

var flushCapHitCounters = func() [4]*metrics.Counter {
	var result [4]*metrics.Counter
	for i, name := range [...]string{"source_value", "source_encoded", "final_value", "final_encoded"} {
		result[i] = metrics.NewRegisteredCounter("blockbuffer/flush/cap_hit/"+name, nil)
	}
	return result
}()
var flushAcceptedOversizeCounter = metrics.NewRegisteredCounter("blockbuffer/flush/accepted_oversize/groups", nil)

func observeFlushPlan(plan *flushGroupPlan, layers int, committed bool) {
	counters := &flushPlanStopCounters[plan.stopReason]
	if committed {
		counters = &flushCommittedStopCounters[plan.stopReason]
	}
	counters.groups.Inc(1)
	counters.layers.Inc(int64(layers))
	counters.input.Inc(int64(plan.inputValue))
	counters.output.Inc(int64(plan.outputValue))
	if committed {
		return
	}
	for i, counter := range flushCapHitCounters {
		if plan.capHits&(1<<i) != 0 {
			counter.Inc(1)
		}
	}
	if plan.oversize {
		flushAcceptedOversizeCounter.Inc(1)
	}
}

type flushDistribution struct {
	bounds  []int64
	buckets []*metrics.Counter
	sum     *metrics.Counter
	max     *metrics.Gauge
}

func newFlushDistribution(name string, bounds []int64) flushDistribution {
	prefix := "blockbuffer/flush/call/" + name + "/"
	d := flushDistribution{bounds: bounds, buckets: make([]*metrics.Counter, len(bounds)+1), sum: metrics.NewRegisteredCounter(prefix+"sum", nil), max: metrics.NewRegisteredGauge(prefix+"max", nil)}
	for i, bound := range bounds {
		d.buckets[i] = metrics.NewRegisteredCounter(prefix+"bucket/le_"+strconv.FormatInt(bound, 10), nil)
	}
	d.buckets[len(bounds)] = metrics.NewRegisteredCounter(prefix+"bucket/overflow", nil)
	return d
}
func (d *flushDistribution) observe(value int64) {
	i := 0
	for i < len(d.bounds) && value > d.bounds[i] {
		i++
	}
	d.buckets[i].Inc(1)
	d.sum.Inc(value)
	d.max.UpdateIfGt(value)
}

var (
	flushCallAttempts     = metrics.NewRegisteredCounter("blockbuffer/flush/call/attempts", nil)
	flushCallEmpty        = metrics.NewRegisteredCounter("blockbuffer/flush/call/empty", nil)
	flushCallSuccess      = metrics.NewRegisteredCounter("blockbuffer/flush/call/success", nil)
	flushCallErrors       = metrics.NewRegisteredCounter("blockbuffer/flush/call/error", nil)
	flushCallBatched      = metrics.NewRegisteredCounter("blockbuffer/flush/call/path/batched", nil)
	flushCallUnbatched    = metrics.NewRegisteredCounter("blockbuffer/flush/call/path/unbatched", nil)
	flushCallAllCommitted = metrics.NewRegisteredCounter("blockbuffer/flush/call/end/all_committed", nil)
	flushCallSolidCutoff  = metrics.NewRegisteredCounter("blockbuffer/flush/call/end/solid_cutoff", nil)
	flushCallLayers       = newFlushDistribution("eligible_layers", []int64{0, 1, 8, 16, 32, 64, 96, 128, 256})
	flushCallInput        = newFlushDistribution("eligible_input_encoded", []int64{0, 1 << 20, 8 << 20, 16 << 20, 32 << 20, 64 << 20, 96 << 20, 128 << 20, 192 << 20, 256 << 20})
	flushCallOutput       = newFlushDistribution("committed_output_encoded", []int64{0, 1 << 20, 8 << 20, 16 << 20, 32 << 20, 64 << 20, 96 << 20, 128 << 20, 192 << 20, 256 << 20})
	flushCallGroups       = newFlushDistribution("committed_groups", []int64{0, 1, 2, 4, 8, 16})
)

// Only FlushUpTo supplies this observation. Input is the complete selected
// prefix; output/groups are successful Batch.Write calls, including a prefix
// before failure. Encoded sizes exclude the per-batch header and scratch slack.
// "Committed" is API success, not an additional fsync guarantee.
type flushCallObservation struct {
	eligibleLayers                      int
	inputEncoded, outputEncoded, groups int64
	batched, failed, solidCutoff        bool
}

func (o *flushCallObservation) publish() {
	flushCallLayers.observe(int64(o.eligibleLayers))
	if o.eligibleLayers == 0 {
		flushCallEmpty.Inc(1)
		return
	}
	if o.failed {
		flushCallErrors.Inc(1)
	} else {
		flushCallSuccess.Inc(1)
	}
	if o.solidCutoff {
		flushCallSolidCutoff.Inc(1)
	} else {
		flushCallAllCommitted.Inc(1)
	}
	if !o.batched {
		flushCallUnbatched.Inc(1)
		return
	}
	flushCallBatched.Inc(1)
	flushCallInput.observe(o.inputEncoded)
	flushCallOutput.observe(o.outputEncoded)
	flushCallGroups.observe(o.groups)
}
