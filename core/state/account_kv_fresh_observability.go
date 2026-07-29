package state

import "github.com/ethereum/go-ethereum/metrics"

var (
	accountKVFreshPointReadsAvoidedCounter      = metrics.NewRegisteredCounter("state/account_kv/fresh/point_reads_avoided", nil)
	accountKVFreshPreimageReadsAvoidedCounter   = metrics.NewRegisteredCounter("state/account_kv/fresh/preimage_reads_avoided", nil)
	accountKVFreshPrefixIteratorsAvoidedCounter = metrics.NewRegisteredCounter("state/account_kv/fresh/prefix_iterators_avoided", nil)
)

func recordFreshAccountKVPointReadsAvoided(count int64) {
	if count > 0 {
		accountKVFreshPointReadsAvoidedCounter.Inc(count)
	}
}

func recordFreshAccountKVPreimageReadAvoided() {
	accountKVFreshPreimageReadsAvoidedCounter.Inc(1)
}

func recordFreshAccountKVPrefixIteratorAvoided() {
	accountKVFreshPrefixIteratorsAvoidedCounter.Inc(1)
}
