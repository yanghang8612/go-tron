package core

import "github.com/ethereum/go-ethereum/metrics"

const stateDomainChangePruneGuardMetricPrefix = "state/prune/history/delete/range/guard/"

// These counters aggregate guard calls across all BlockChains in this process.
// They are never reset when a pruner/DB closes and are independent of successful
// prune-pass publication. JSON exports Counter.count, not Gauge.value.
//
// attempts increments on entry; admitted increments immediately before work.
// Other classifications are terminal pre-work outcomes. At quiescence:
// attempts = busyIndex + busyChain + admitted + prefixUnsettled + proofErrors +
// otherErrors. In-flight verification and non-atomic scrapes can create a gap.
// workErrors is a subset of admitted and must not be added to this partition.
//
// proofErrors includes invalid proof parameters, unavailable head/solidified
// boundaries and canonical/stage verification errors (including storage errors).
// otherErrors covers cancellation, missing work/DB, lifecycle and async failures.
type stateDomainChangePruneGuardMetrics struct {
	attempts, busyIndex, busyChain, admitted              *metrics.Counter
	prefixUnsettled, proofErrors, otherErrors, workErrors *metrics.Counter
	queuedAttempts, queuedChainBusy                       *metrics.Counter
	queuedChainWaitTotal, queuedChainHeldTotal            *metrics.Counter
	queuedChainWaitMax, queuedChainHeldMax                *metrics.Gauge
}

var stateDomainChangePruneGuardProcessMetrics = newStateDomainChangePruneGuardMetrics(nil)

// Tests use a private registry and pass this recorder into the private helper;
// they do not reset or assert deltas against process-wide production counters.
func newStateDomainChangePruneGuardMetrics(registry metrics.Registry) *stateDomainChangePruneGuardMetrics {
	prefix := stateDomainChangePruneGuardMetricPrefix
	return &stateDomainChangePruneGuardMetrics{
		attempts:             metrics.GetOrRegisterCounter(prefix+"attempts", registry),
		busyIndex:            metrics.GetOrRegisterCounter(prefix+"busy_index", registry),
		busyChain:            metrics.GetOrRegisterCounter(prefix+"busy_chain", registry),
		admitted:             metrics.GetOrRegisterCounter(prefix+"admitted", registry),
		prefixUnsettled:      metrics.GetOrRegisterCounter(prefix+"prefix_unsettled", registry),
		proofErrors:          metrics.GetOrRegisterCounter(prefix+"proof_errors", registry),
		otherErrors:          metrics.GetOrRegisterCounter(prefix+"other_errors", registry),
		workErrors:           metrics.GetOrRegisterCounter(prefix+"work_errors", registry),
		queuedAttempts:       metrics.GetOrRegisterCounter(prefix+"queued/attempts", registry),
		queuedChainBusy:      metrics.GetOrRegisterCounter(prefix+"queued/chain_busy", registry),
		queuedChainWaitTotal: metrics.GetOrRegisterCounter(prefix+"queued/chain_wait/total_ns", registry),
		queuedChainWaitMax:   metrics.GetOrRegisterGauge(prefix+"queued/chain_wait/max_ns", registry),
		queuedChainHeldTotal: metrics.GetOrRegisterCounter(prefix+"queued/chain_held/total_ns", registry),
		queuedChainHeldMax:   metrics.GetOrRegisterGauge(prefix+"queued/chain_held/max_ns", registry),
	}
}
