package blockbuffer

import "github.com/ethereum/go-ethereum/metrics"

// Only an already profitable, bounded point merge may cross its base cap.
// The caller has already checked both source limits and the range boundary.
// The next layer must itself eliminate encoded records; past prefix coverage
// alone does not justify making a larger batch for purely new records.
func canExpandFlushGroup(plan *flushGroupPlan, next layerBatchSize, valueDelta, encodedDelta int, limits flushGroupLimits) bool {
	return limits.expandedValue > limits.batchValue && limits.expandedEncoded > limits.batchEncoded &&
		plan.outputValue > 0 && plan.inputValue/2 >= plan.outputValue &&
		next.encoded > encodedDelta &&
		next.ops >= 0 && next.ops <= maxPooledFlushWriteKeys && len(plan.merged.ops) <= maxPooledFlushWriteKeys-next.ops &&
		plan.outputValue+valueDelta <= limits.expandedValue && plan.outputEncoded+encodedDelta <= limits.expandedEncoded
}

var flushExpandedPlanned = metrics.NewRegisteredCounter("blockbuffer/flush/expanded/planned/groups", nil)
var flushExpandedCommitted = metrics.NewRegisteredCounter("blockbuffer/flush/expanded/committed/groups", nil)

func observeFlushExpansion(plan *flushGroupPlan, committed bool) {
	if !plan.expanded {
		return
	}
	if committed {
		flushExpandedCommitted.Inc(1)
	} else {
		flushExpandedPlanned.Inc(1)
	}
}
func init() {
	for name, value := range map[string]int64{"base_value_bytes": maxFlushBatchValueSize, "base_encoded_bytes": maxFlushBatchEncodedSize, "max_value_bytes": maxExpandedFlushBatchValueSize, "max_encoded_bytes": maxExpandedFlushBatchEncodedSize, "source_value_bytes": maxFlushMergeValueSize, "source_encoded_bytes": maxFlushMergeEncodedSize, "expansion_min_input_output_ratio_ppm": 2000000, "expansion_max_entries": maxPooledFlushWriteKeys} {
		metrics.NewRegisteredGauge("blockbuffer/flush/limit/"+name, nil).Update(value)
	}
}
