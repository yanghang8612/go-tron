package snapshots

import (
	"math"

	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
)

const (
	historyCompressionLargePrevBytes = uint64(128 << 10)
	historyCompressionIdentityLimit  = 4096
	// Bound retained variable-length identities as well as the entry count.
	// A missed identity only makes the policy fall back to the ordinary codec.
	historyCompressionIdentityBytes = 4 << 20
)

// historyCompressionPolicy observes records during the existing dictionary-key
// collection pass. It never retains Prev bytes or scans them for similarity.
// It is zero-value ready and belongs to one build, not concurrent builders.
// Repeated large values for one key are a compression candidate, not proof
// that those values share bytes or that CDC will improve a particular input.
type historyCompressionPolicy struct {
	stats      historyCompressionPolicyStats
	identities map[string]struct{}
	keyScratch []byte
}

type historyCompressionPolicyStats struct {
	Records, TotalPrevBytes, LargeRecords, LargePrevBytes uint64
	TrackedIdentities, TrackedIdentityBytes               uint64
	RepeatedLargeKey                                      bool
	IdentityLimitReached                                  bool
	Overflow, InvalidInput                                bool
}

// Observe adds one effective logical record, after pack/repair resolution.
// Callers must observe each record once; this helper does not establish source
// ordering, coverage or authenticity and never replaces those writer gates.
func (p *historyCompressionPolicy) Observe(change *rawdb.StateDomainChange) {
	if p == nil {
		return
	}
	if change == nil {
		p.stats.InvalidInput = true
		return
	}
	p.add(&p.stats.Records, 1)
	n := uint64(len(change.Prev))
	p.add(&p.stats.TotalPrevBytes, n)
	if !change.PrevExists && n != 0 {
		p.stats.InvalidInput = true
		return
	}
	switch change.FlatDomain {
	case rawdb.StateFlatDomainAccountLatest, rawdb.StateFlatDomainKVGeneration:
		// These domains' canonical identity is flat-domain + owner account ID.
	case rawdb.StateFlatDomainKVLatest:
		// Check before allocating the same key encoded by CollectKey. Account ID,
		// generation and domain are all part of this identity; no digest aliases.
		const prefix = 1 + common.AccountIDLength + 8 + 2
		if len(change.Key) > math.MaxUint16-prefix {
			p.stats.InvalidInput = true
			return
		}
	default:
		p.stats.InvalidInput = true
		return
	}
	if n < historyCompressionLargePrevBytes {
		return
	}
	p.add(&p.stats.LargeRecords, 1)
	p.add(&p.stats.LargePrevBytes, n)
	if p.stats.Overflow || p.stats.InvalidInput || p.stats.RepeatedLargeKey {
		return
	}
	p.keyScratch = appendStateDomainChangeBinaryAccessorLookupKey(p.keyScratch[:0], change.FlatDomain, change.Owner, change.Generation, change.Domain, change.Key)
	if _, found := p.identities[string(p.keyScratch)]; found {
		p.stats.RepeatedLargeKey = true
		return
	}
	if p.stats.IdentityLimitReached || len(p.identities) >= historyCompressionIdentityLimit || uint64(len(p.keyScratch)) > historyCompressionIdentityBytes-p.stats.TrackedIdentityBytes {
		p.stats.IdentityLimitReached = true
		return
	}
	if p.identities == nil {
		p.identities = make(map[string]struct{})
	}
	// Conversion for insertion makes an owned identity; later borrowed key
	// reuse and mutations cannot change this map entry.
	p.identities[string(p.keyScratch)] = struct{}{}
	p.stats.TrackedIdentities++
	p.stats.TrackedIdentityBytes += uint64(len(p.keyScratch))
}

func (p *historyCompressionPolicy) add(dst *uint64, n uint64) {
	if n > math.MaxUint64-*dst {
		*dst = math.MaxUint64
		p.stats.Overflow = true
		return
	}
	*dst += n
}

// RecommendCDC requires at least half the observed Prev bytes to come from
// >=128KiB values and two large observations of one exact logical key. Using
// subtraction avoids overflow from doubling LargePrevBytes. Saturated or
// malformed observations always fall back to V2, even after an earlier hit.
func (p *historyCompressionPolicy) RecommendCDC() bool {
	if p == nil {
		return false
	}
	s := p.stats
	return !s.Overflow && !s.InvalidInput && s.RepeatedLargeKey && s.TotalPrevBytes > 0 &&
		s.LargePrevBytes <= s.TotalPrevBytes && s.LargePrevBytes >= s.TotalPrevBytes-s.LargePrevBytes
}

func (p *historyCompressionPolicy) Snapshot() historyCompressionPolicyStats {
	if p == nil {
		return historyCompressionPolicyStats{}
	}
	return p.stats
}
