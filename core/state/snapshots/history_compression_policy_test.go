package snapshots

import (
	"bytes"
	"encoding/binary"
	"math"
	"testing"

	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/state/kvdomains"
)

func compressionPolicyChange(prev []byte) rawdb.StateDomainChange {
	return rawdb.StateDomainChange{
		FlatDomain: rawdb.StateFlatDomainKVLatest,
		Owner:      common.Address{common.AddressPrefixMainnet, 1}, Generation: 3,
		Domain: kvdomains.SystemDelegation, Key: []byte("drax0-owner"),
		PrevExists: true, Prev: prev,
	}
}

func TestHistoryCompressionPolicyThresholdAndExactHalf(t *testing.T) {
	large := make([]byte, historyCompressionLargePrevBytes)
	change := compressionPolicyChange(large)
	var p historyCompressionPolicy
	p.Observe(&change)
	if p.RecommendCDC() {
		t.Fatal("one large value is not repeated")
	}
	p.Observe(&change)
	if !p.RecommendCDC() || p.Snapshot().LargeRecords != 2 {
		t.Fatalf("exact size threshold: %+v", p.Snapshot())
	}
	change.Prev = large[:len(large)/2]
	for i := 0; i < 4; i++ {
		p.Observe(&change)
	}
	if !p.RecommendCDC() || p.Snapshot().TotalPrevBytes != 2*p.Snapshot().LargePrevBytes {
		t.Fatalf("exact 50%% must qualify: %+v", p.Snapshot())
	}
	change.Prev = large[:1]
	p.Observe(&change)
	if p.RecommendCDC() {
		t.Fatal("less than half must fall back")
	}
	p = historyCompressionPolicy{}
	change.Prev = large[:len(large)-1]
	p.Observe(&change)
	p.Observe(&change)
	if p.RecommendCDC() || p.Snapshot().LargeRecords != 0 {
		t.Fatalf("below 128KiB: %+v", p.Snapshot())
	}
}

func TestHistoryCompressionPolicyUsesWholeLogicalIdentity(t *testing.T) {
	base := compressionPolicyChange(make([]byte, historyCompressionLargePrevBytes))
	for name, mutate := range map[string]func(*rawdb.StateDomainChange){
		"flat-domain": func(c *rawdb.StateDomainChange) { c.FlatDomain = rawdb.StateFlatDomainAccountLatest },
		"owner":       func(c *rawdb.StateDomainChange) { c.Owner[2]++ },
		"generation":  func(c *rawdb.StateDomainChange) { c.Generation++ },
		"kv-domain":   func(c *rawdb.StateDomainChange) { c.Domain = kvdomains.ContractStorage },
		"key":         func(c *rawdb.StateDomainChange) { c.Key = []byte("drax0-other") },
	} {
		t.Run(name, func(t *testing.T) {
			var p historyCompressionPolicy
			p.Observe(&base)
			other := base
			mutate(&other)
			p.Observe(&other)
			if p.RecommendCDC() || p.Snapshot().RepeatedLargeKey {
				t.Fatalf("distinct logical identity aliased: %+v", p.Snapshot())
			}
			p.Observe(&base)
			if !p.RecommendCDC() {
				t.Fatal("exact identity was not retained")
			}
		})
	}
	// Account and generation flat keys intentionally exclude KV fields in the
	// authoritative key encoder. Match that identity instead of inventing one.
	for _, domain := range []rawdb.StateFlatDomain{rawdb.StateFlatDomainAccountLatest, rawdb.StateFlatDomainKVGeneration} {
		var p historyCompressionPolicy
		a := base
		a.FlatDomain = domain
		p.Observe(&a)
		a.Domain, a.Generation, a.Key = kvdomains.ContractStorage, 77, []byte("ignored-by-flat-key")
		p.Observe(&a)
		if !p.RecommendCDC() {
			t.Fatal("canonical non-KV identity differs from accessor encoder")
		}
	}
}

func TestHistoryCompressionPolicyOwnsOnlyKeysAndStopsGrowing(t *testing.T) {
	large := make([]byte, historyCompressionLargePrevBytes)
	change := compressionPolicyChange(large)
	change.Key = make([]byte, 8)
	var p historyCompressionPolicy
	for i := 0; i < historyCompressionIdentityLimit; i++ {
		binary.BigEndian.PutUint64(change.Key, uint64(i))
		p.Observe(&change)
	}
	if len(p.identities) != historyCompressionIdentityLimit || p.RecommendCDC() {
		t.Fatalf("identity cap setup: %+v", p.Snapshot())
	}
	binary.BigEndian.PutUint64(change.Key, historyCompressionIdentityLimit)
	p.Observe(&change)
	p.Observe(&change)
	if len(p.identities) != historyCompressionIdentityLimit || !p.Snapshot().IdentityLimitReached || p.RecommendCDC() {
		t.Fatalf("overflow identity was retained or aliased: %+v", p.Snapshot())
	}
	// Mutating the borrowed key and value did not mutate retained identities;
	// a lookup of the earliest key must still work even after the map is full.
	large[0] = 99
	binary.BigEndian.PutUint64(change.Key, 0)
	p.Observe(&change)
	if !p.RecommendCDC() {
		t.Fatal("full map stopped checking retained identities")
	}
	if _, ok := p.identities[string(stateDomainChangeBinaryAccessorKey(&change))]; !ok {
		t.Fatal("identity differs from production encoder")
	}
}

func TestHistoryCompressionPolicyIdentityBytesRemainBounded(t *testing.T) {
	change := compressionPolicyChange(make([]byte, historyCompressionLargePrevBytes))
	change.Key = bytes.Repeat([]byte{1}, math.MaxUint16-(1+common.AccountIDLength+8+2))
	var p historyCompressionPolicy
	for i := 0; i < 100; i++ {
		binary.BigEndian.PutUint64(change.Key, uint64(i))
		p.Observe(&change)
	}
	s := p.Snapshot()
	if !s.IdentityLimitReached || s.TrackedIdentityBytes > historyCompressionIdentityBytes || s.TrackedIdentities >= historyCompressionIdentityLimit || p.RecommendCDC() {
		t.Fatalf("key bytes unbounded: %+v", s)
	}
	before := len(p.identities)
	change.Key = []byte("smaller-but-still-new")
	p.Observe(&change)
	if len(p.identities) != before {
		t.Fatal("insertion resumed after byte budget was reached")
	}
	change.Key = bytes.Repeat([]byte{1}, math.MaxUint16-(1+common.AccountIDLength+8+2))
	binary.BigEndian.PutUint64(change.Key, 0)
	p.Observe(&change)
	if !p.RecommendCDC() {
		t.Fatal("byte cap stopped lookup of earlier identity")
	}
}

func TestHistoryCompressionPolicyPresenceEmptyAndMalformed(t *testing.T) {
	var p historyCompressionPolicy
	change := compressionPolicyChange(nil)
	p.Observe(&change) // Present empty is a valid zero-byte image.
	change.PrevExists = false
	p.Observe(&change) // Absent empty is a different image, also zero bytes.
	if p.RecommendCDC() || p.Snapshot().Records != 2 || p.Snapshot().TotalPrevBytes != 0 || p.Snapshot().InvalidInput {
		t.Fatalf("empty images: %+v", p.Snapshot())
	}
	for name, mutate := range map[string]func(*rawdb.StateDomainChange){
		"absent-nonempty": func(c *rawdb.StateDomainChange) { c.PrevExists = false },
		"unknown-domain":  func(c *rawdb.StateDomainChange) { c.FlatDomain = rawdb.StateFlatDomainUnknown },
		"oversize-key":    func(c *rawdb.StateDomainChange) { c.Key = make([]byte, math.MaxUint16) },
	} {
		t.Run(name, func(t *testing.T) {
			var p historyCompressionPolicy
			c := compressionPolicyChange(make([]byte, historyCompressionLargePrevBytes))
			p.Observe(&c)
			p.Observe(&c)
			if !p.RecommendCDC() {
				t.Fatal("setup")
			}
			mutate(&c)
			p.Observe(&c)
			if !p.Snapshot().InvalidInput || p.RecommendCDC() {
				t.Fatalf("malformed observation retained earlier recommendation: %+v", p.Snapshot())
			}
		})
	}
	p.Observe(nil)
	if !p.Snapshot().InvalidInput || p.RecommendCDC() {
		t.Fatal("nil input must not select CDC")
	}
	var nilPolicy *historyCompressionPolicy
	nilPolicy.Observe(&change)
	if nilPolicy.RecommendCDC() || nilPolicy.Snapshot() != (historyCompressionPolicyStats{}) {
		t.Fatal("nil receiver")
	}
}

func TestHistoryCompressionPolicySaturatesAndFallsBack(t *testing.T) {
	for _, field := range []string{"records", "total", "large-records", "large-bytes"} {
		t.Run(field, func(t *testing.T) {
			var p historyCompressionPolicy
			c := compressionPolicyChange(make([]byte, historyCompressionLargePrevBytes))
			p.Observe(&c)
			p.Observe(&c)
			var target *uint64
			switch field {
			case "records":
				target = &p.stats.Records
			case "total":
				target = &p.stats.TotalPrevBytes
			case "large-records":
				target = &p.stats.LargeRecords
			case "large-bytes":
				target = &p.stats.LargePrevBytes
			}
			*target = math.MaxUint64
			p.Observe(&c)
			if *target != math.MaxUint64 || !p.Snapshot().Overflow || p.RecommendCDC() {
				t.Fatalf("wrapped/saturated recommendation: %+v", p.Snapshot())
			}
		})
	}
	// Valid counters near uint64's limit must not overflow the ratio check.
	p := historyCompressionPolicy{stats: historyCompressionPolicyStats{TotalPrevBytes: math.MaxUint64, LargePrevBytes: math.MaxUint64/2 + 1, RepeatedLargeKey: true}}
	if !p.RecommendCDC() {
		t.Fatal("overflow-free majority calculation rejected valid majority")
	}
	p.stats.LargePrevBytes--
	if p.RecommendCDC() {
		t.Fatal("minority near uint64 limit was accepted")
	}
}
