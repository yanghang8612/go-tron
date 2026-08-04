package blockbuffer

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"strings"
	"sync"
	"testing"
	"unsafe"

	"github.com/tronprotocol/go-tron/core/rawdb"
)

var baseReadCacheEntryBenchmarkSink *baseReadCacheEntry

func TestBaseReadCacheEntryStaysInEightyByteClass(t *testing.T) {
	if got := unsafe.Sizeof(baseReadCacheEntry{}); got != 80 {
		t.Fatalf("baseReadCacheEntry size = %d, want 80", got)
	}
}

func testBaseReadCacheSet(c *baseReadCache, key, value []byte) {
	for attempt := 0; attempt < 2; attempt++ {
		_, _, epoch := c.getWithEpoch(key)
		if _, stored := c.setIfEpoch(key, value, epoch); stored {
			return
		}
	}
	panic("base-read cache test fill did not complete admission")
}

func TestBaseReadCache_TwoHitAdmissionRejectsOneHitScan(t *testing.T) {
	c := newBaseReadCache(1 << 20)
	value := []byte("durable-value")
	for i := 0; i < 10_000; i++ {
		key := []byte(fmt.Sprintf("cold-branch-%08d", i))
		_, _, epoch := c.getWithEpoch(key)
		if _, stored := c.setIfEpoch(key, value, epoch); stored {
			t.Fatalf("one-hit key %q was admitted", key)
		}
	}
	resident := 0
	for i := range c.shards {
		resident += len(c.shards[i].entries)
	}
	if resident != 0 {
		t.Fatalf("one-hit scan retained %d resident entries, want 0", resident)
	}

	hotKey := []byte("repeated-hot-branch")
	_, _, epoch := c.getWithEpoch(hotKey)
	if _, stored := c.setIfEpoch(hotKey, value, epoch); stored {
		t.Fatal("first hot-key sighting bypassed probation")
	}
	_, _, epoch = c.getWithEpoch(hotKey)
	if _, stored := c.setIfEpoch(hotKey, value, epoch); !stored {
		t.Fatal("second hot-key sighting was not admitted")
	}
	if got, ok, _ := c.getWithEpoch(hotKey); !ok || !bytes.Equal(got, value) {
		t.Fatalf("admitted hot key = (%q,%v), want (%q,true)", got, ok, value)
	}
}

func TestBaseReadCache_CommitmentTrunkAdmitsOnceAndSurvivesTailChurn(t *testing.T) {
	const commitmentPrefix = "state-commitment-branch-v1-"
	c := newBaseReadCacheWithTrunk(baseReadCacheShardCount*4096, baseReadCacheTrunkDepth, commitmentPrefix)
	value := bytes.Repeat([]byte{0x5a}, 64)

	// Physical branch paths store one byte per nibble. Find a depth-four path
	// routed to shard zero so the test can put both tiers under the same budget.
	var trunkKey []byte
	for i := 0; i < 1<<16; i++ {
		key := append([]byte(commitmentPrefix), byte(i>>12), byte(i>>8&15), byte(i>>4&15), byte(i&15))
		if baseReadCacheShardIndex(key) == 0 {
			trunkKey = key
			break
		}
	}
	if trunkKey == nil {
		t.Fatal("no depth-four trunk key routed to shard zero")
	}
	_, _, epoch := c.getWithEpoch(trunkKey)
	if _, stored := c.setIfEpoch(trunkKey, value, epoch); !stored {
		t.Fatal("first shallow commitment read was not admitted to the trunk")
	}
	s := &c.shards[0]
	entry := s.entries[string(trunkKey)]
	if entry == nil || !entry.trunk || s.trunkUsed != entry.charge {
		t.Fatalf("trunk entry=%p pinned=%v used=%d", entry, entry != nil && entry.trunk, s.trunkUsed)
	}

	// Repeated deep-branch admissions overflow the shard and churn the CLOCK
	// tail. The fixed trunk must remain resident without exceeding either bound.
	for _, key := range testBaseReadCacheKeysForShard(t, commitmentPrefix+"deep-tail-", 0, 128) {
		testBaseReadCacheSet(c, key, value)
	}
	if got := s.entries[string(trunkKey)]; got != entry || !got.trunk {
		t.Fatal("commitment trunk was displaced by deep branch churn")
	}
	if s.used > s.limit || s.trunkUsed > s.trunkLimit {
		t.Fatalf("bounded trunk accounting used=%d/%d trunk=%d/%d", s.used, s.limit, s.trunkUsed, s.trunkLimit)
	}

	c.del(trunkKey)
	if s.trunkUsed != 0 || s.entries[string(trunkKey)] != nil {
		t.Fatalf("deleted trunk retained accounting=%d entry=%p", s.trunkUsed, s.entries[string(trunkKey)])
	}
}

func TestBaseReadCache_CommitmentDeltaGenerationIsSchemaNotDepth(t *testing.T) {
	c := newBaseReadCacheWithTrunk(1<<20, 4, rawdb.CommitmentBranchKeyPrefix)
	deltaSchema := make([]byte, len(rawdb.CommitmentBranchDeltaKeyPrefix)+8)
	copy(deltaSchema, rawdb.CommitmentBranchDeltaKeyPrefix)
	binary.BigEndian.PutUint64(deltaSchema[len(rawdb.CommitmentBranchDeltaKeyPrefix):], 17)

	for _, test := range []struct {
		name  string
		key   []byte
		depth int
		trunk bool
	}{
		{name: "legacy root", key: []byte(rawdb.CommitmentBranchKeyPrefix), depth: 0, trunk: true},
		{name: "legacy depth four", key: append([]byte(rawdb.CommitmentBranchKeyPrefix), 1, 2, 3, 4), depth: 4, trunk: true},
		{name: "delta root", key: append([]byte(nil), deltaSchema...), depth: 0, trunk: true},
		{name: "delta depth four", key: append(append([]byte(nil), deltaSchema...), 1, 2, 3, 4), depth: 4, trunk: true},
		{name: "delta depth five", key: append(append([]byte(nil), deltaSchema...), 1, 2, 3, 4, 5), depth: 5, trunk: false},
	} {
		depth, ok := c.commitmentKeyDepthBytes(test.key)
		if !ok || depth != test.depth {
			t.Fatalf("%s byte depth = %d ok=%v, want %d,true", test.name, depth, ok, test.depth)
		}
		stringDepth, stringOK := c.commitmentKeyDepthString(string(test.key))
		if !stringOK || stringDepth != test.depth {
			t.Fatalf("%s string depth = %d ok=%v, want %d,true", test.name, stringDepth, stringOK, test.depth)
		}
		if got := c.isCommitmentTrunkKey(test.key); got != test.trunk {
			t.Fatalf("%s trunk = %v, want %v", test.name, got, test.trunk)
		}
		if c.isOtherKeyBytes(test.key) || c.isOtherKeyString(string(test.key)) {
			t.Fatalf("%s classified as non-commitment", test.name)
		}
	}
	if _, ok := c.commitmentKeyDepthBytes([]byte(rawdb.CommitmentBranchBaseKey)); ok {
		t.Fatal("base marker classified as branch row")
	}
}

func TestBaseReadCache_DeepCommitmentOneHitScanStaysInWindow(t *testing.T) {
	const commitmentPrefix = "state-commitment-branch-v1-"
	const shard = uint32(0)
	c := newBaseReadCacheWithTrunk(baseReadCacheShardCount*4096, baseReadCacheTrunkDepth, commitmentPrefix)
	s := &c.shards[shard]
	value := bytes.Repeat([]byte{0x31}, 64)
	keys := testBaseReadCacheKeysForShard(t, commitmentPrefix+"deep-scan-", shard, 128)

	for _, key := range keys {
		_, _, epoch := c.getWithEpoch(key)
		if _, stored := c.setIfEpoch(key, value, epoch); !stored {
			t.Fatalf("first deep commitment read %q was not retained in the window", key)
		}
	}
	if s.windowUsed > s.windowLimit || s.used > s.limit {
		t.Fatalf("window scan exceeded bounds window=%d/%d total=%d/%d", s.windowUsed, s.windowLimit, s.used, s.limit)
	}
	if got := len(s.queue) - s.head; got != 0 {
		t.Fatalf("one-hit scan polluted main CLOCK with %d tokens", got)
	}
	for _, entry := range s.entries {
		if !entry.window || entry.trunk || entry.nonCommitment {
			t.Fatalf("one-hit deep entry has wrong class: window=%v trunk=%v other=%v", entry.window, entry.trunk, entry.nonCommitment)
		}
	}
}

func TestBaseReadCache_DeepCommitmentWindowPromotesReuse(t *testing.T) {
	const commitmentPrefix = "state-commitment-branch-v1-"
	const shard = uint32(0)
	c := newBaseReadCacheWithTrunk(baseReadCacheShardCount*4096, baseReadCacheTrunkDepth, commitmentPrefix)
	s := &c.shards[shard]
	value := bytes.Repeat([]byte{0x42}, 64)
	keys := testBaseReadCacheKeysForShard(t, commitmentPrefix+"deep-window-", shard, 8)

	storeFirst := func(key []byte) {
		t.Helper()
		_, _, epoch := c.getWithEpoch(key)
		if _, stored := c.setIfEpoch(key, value, epoch); !stored {
			t.Fatalf("first deep commitment read %q was not retained", key)
		}
	}
	storeFirst(keys[0])
	if entry := s.entries[string(keys[0])]; entry == nil || !entry.window {
		t.Fatal("first deep read did not enter the window")
	}
	if _, found, _ := c.getWithEpoch(keys[0]); !found {
		t.Fatal("window did not resolve the repeated read")
	}

	// Overflowing the small per-shard window promotes the referenced oldest
	// row, while the next untouched oldest row is discarded as scan traffic.
	storeFirst(keys[1])
	storeFirst(keys[2])
	hot := s.entries[string(keys[0])]
	if hot == nil || hot.window || hot.trunk || hot.nonCommitment {
		t.Fatalf("reused deep row was not promoted to main CLOCK: %#v", hot)
	}
	storeFirst(keys[3])
	if s.entries[string(keys[1])] != nil {
		t.Fatal("untouched window row survived FIFO pressure")
	}

	// The first observation's fingerprint survives a window eviction, so the
	// next durable sighting enters the main CLOCK without another window cycle.
	_, _, epoch := c.getWithEpoch(keys[1])
	if _, stored := c.setIfEpoch(keys[1], value, epoch); !stored {
		t.Fatal("second sighting after window eviction did not enter main CLOCK")
	}
	if entry := s.entries[string(keys[1])]; entry == nil || entry.window {
		t.Fatal("second sighting was returned to the first-read window")
	}
	if s.windowUsed > s.windowLimit || s.used > s.limit {
		t.Fatalf("promotion exceeded bounds window=%d/%d total=%d/%d", s.windowUsed, s.windowLimit, s.used, s.limit)
	}
}

func TestBaseReadCache_FlushProtectsCommitmentWindowEntryForPromotion(t *testing.T) {
	const commitmentPrefix = "state-commitment-branch-v1-"
	c := newBaseReadCacheWithTrunk(baseReadCacheShardCount*4096, baseReadCacheTrunkDepth, commitmentPrefix)
	key := []byte(commitmentPrefix + "deep-flushed-branch")
	value := []byte("parent-v1")
	_, _, epoch := c.getWithEpoch(key)
	if _, stored := c.setIfEpoch(key, value, epoch); !stored {
		t.Fatal("first deep commitment read was not retained")
	}
	s := &c.shards[baseReadCacheShardIndex(key)]
	entry := s.entries[string(key)]
	if entry == nil || !entry.window || s.windowUsed != entry.charge {
		t.Fatalf("window entry=%p marked=%v used=%d", entry, entry != nil && entry.window, s.windowUsed)
	}

	c.setFlushed(string(key), []byte("child-v2"))
	if got, ok, _ := c.getWithEpoch(key); !ok || string(got) != "child-v2" {
		t.Fatalf("flush-promoted value=(%q,%v), want child-v2/true", got, ok)
	}
	for _, churnKey := range testBaseReadCacheKeysForShard(t, commitmentPrefix+"deep-flush-churn-", baseReadCacheShardIndex(key), 8) {
		_, _, churnEpoch := c.getWithEpoch(churnKey)
		if _, stored := c.setIfEpoch(churnKey, value, churnEpoch); !stored {
			t.Fatalf("first churn read %q was not retained", churnKey)
		}
		if !entry.window {
			break
		}
	}
	if entry.window {
		t.Fatal("flush-protected window entry was not promoted under FIFO pressure")
	}
	c.del(key)
	if s.entries[string(key)] != nil {
		t.Fatalf("delete retained promoted entry: entry=%p", s.entries[string(key)])
	}
}

func TestBaseReadCache_WindowAdmissionAdaptsToObservedReuse(t *testing.T) {
	var shard baseReadCacheShard
	for wantShift := uint8(1); wantShift <= baseReadCacheWindowMaxAdmissionShift; wantShift++ {
		for i := 0; i < baseReadCacheWindowOutcomeBatch; i++ {
			shard.observeWindowOutcome(false)
		}
		if shard.windowAdmissionShift != wantShift {
			t.Fatalf("dry-scan admission shift=%d, want %d", shard.windowAdmissionShift, wantShift)
		}
	}

	admitted := 0
	for i := 0; i < 6400; i++ {
		if shard.admitWindowFirstRead() {
			admitted++
		}
	}
	if admitted != 100 {
		t.Fatalf("1/64 probe admissions=%d, want 100", admitted)
	}

	// Resident hits relax sampling on the next candidate batch without waiting
	// for a large, slowly rotating sampled window to reach its FIFO boundary.
	shard.windowProbeCandidates = 0
	shard.windowProbeAdmissions = 0
	shard.windowHitEvents.Store(4)
	for i := 0; i < baseReadCacheWindowOutcomeBatch; i++ {
		shard.admitWindowFirstRead()
	}
	if got, want := shard.windowAdmissionShift, uint8(baseReadCacheWindowMaxAdmissionShift-1); got != want {
		t.Fatalf("hit-feedback admission shift=%d, want %d", got, want)
	}

	for wantShift := int(shard.windowAdmissionShift) - 1; wantShift >= 0; wantShift-- {
		for i := 0; i < baseReadCacheWindowOutcomeBatch; i++ {
			shard.observeWindowOutcome(true)
		}
		if int(shard.windowAdmissionShift) != wantShift {
			t.Fatalf("reused-window admission shift=%d, want %d", shard.windowAdmissionShift, wantShift)
		}
	}
}

func TestBaseReadCache_WindowSamplingPreservesTwoHitAdmission(t *testing.T) {
	const commitmentPrefix = "state-commitment-branch-v1-"
	const shardIndex = uint32(0)
	c := newBaseReadCacheWithTrunk(baseReadCacheShardCount*4096, baseReadCacheTrunkDepth, commitmentPrefix)
	s := &c.shards[shardIndex]
	s.windowAdmissionShift = baseReadCacheWindowMaxAdmissionShift
	key := testBaseReadCacheKeysForShard(t, commitmentPrefix+"deep-sampled-", shardIndex, 1)[0]
	value := []byte("durable-parent")

	_, _, epoch := c.getWithEpoch(key)
	if _, stored := c.setIfEpoch(key, value, epoch); stored {
		t.Fatal("unsampled first sighting was retained")
	}
	_, _, epoch = c.getWithEpoch(key)
	if _, stored := c.setIfEpoch(key, value, epoch); !stored {
		t.Fatal("second sighting did not bypass window sampling")
	}
	entry := s.entries[string(key)]
	if entry == nil || entry.window || entry.trunk || entry.nonCommitment {
		t.Fatalf("second sighting entered wrong cache class: %#v", entry)
	}
}

func TestBaseReadCache_ProductionAdmissionHistoryBudget(t *testing.T) {
	c := newBaseReadCache(128 << 20)
	var totalSlots int
	for i := range c.shards {
		if got := len(c.shards[i].admission); got != baseReadCacheMaxAdmissionSlots {
			t.Fatalf("shard %d admission slots = %d, want %d", i, got, baseReadCacheMaxAdmissionSlots)
		}
		totalSlots += len(c.shards[i].admission)
	}
	if got, want := totalSlots*8, 4<<20; got != want {
		t.Fatalf("admission history bytes = %d, want %d", got, want)
	}

	partitioned := newBaseReadCache(128<<20, "commitment-")
	otherSlots := 0
	for i := range partitioned.shards {
		otherSlots += len(partitioned.shards[i].nonCommitmentAdmission)
	}
	if got, want := otherSlots*8, 1<<20; got != want {
		t.Fatalf("other admission history bytes = %d, want %d", got, want)
	}
	if got, want := (totalSlots+otherSlots)*8, 5<<20; got != want {
		t.Fatalf("partitioned admission history bytes = %d, want %d", got, want)
	}
}

func testBaseReadCacheKeysForShard(t *testing.T, prefix string, shard uint32, count int) [][]byte {
	t.Helper()
	keys := make([][]byte, 0, count)
	for i := 0; len(keys) < count && i < 1_000_000; i++ {
		key := []byte(fmt.Sprintf("%s%08d", prefix, i))
		if baseReadCacheShardIndex(key) == shard {
			keys = append(keys, key)
		}
	}
	if len(keys) != count {
		t.Fatalf("found %d %q keys for shard %d, want %d", len(keys), prefix, shard, count)
	}
	return keys
}

func TestBaseReadCache_OtherReservationIsSoftAndBounded(t *testing.T) {
	const (
		commitmentPrefix = "commitment-"
		shard            = uint32(0)
	)
	c := newBaseReadCache(baseReadCacheShardCount*4096, commitmentPrefix)
	s := &c.shards[shard]
	value := bytes.Repeat([]byte{0x5a}, 320)
	otherKeys := testBaseReadCacheKeysForShard(t, "flat-latest-", shard, 3)

	// With no commitment pressure, other rows borrow beyond their nominal
	// quarter instead of leaving the shard underfilled.
	for _, key := range otherKeys {
		testBaseReadCacheSet(c, key, value)
	}
	if s.nonCommitmentUsed <= s.nonCommitmentLimit || s.used > s.limit {
		t.Fatalf("other borrowing used=%d reservation=%d total=%d/%d", s.nonCommitmentUsed, s.nonCommitmentLimit, s.used, s.limit)
	}

	// Under sustained commitment pressure the mix converges to the weighted
	// share, allowing at most one entry of charge granularity above the target.
	for _, key := range testBaseReadCacheKeysForShard(t, commitmentPrefix, shard, 24) {
		testBaseReadCacheSet(c, key, value)
	}
	if s.used > s.limit {
		t.Fatalf("total used=%d exceeds limit=%d", s.used, s.limit)
	}
	maxEntryCharge := len(otherKeys[0]) + len(value) + baseReadCacheEntryOverhead
	if s.nonCommitmentUsed > s.nonCommitmentLimit+maxEntryCharge {
		t.Fatalf("other used=%d exceeds weighted target=%d plus entry=%d", s.nonCommitmentUsed, s.nonCommitmentLimit, maxEntryCharge)
	}
	protected := make([]string, 0, len(otherKeys))
	for key, entry := range s.entries {
		if entry.nonCommitment {
			protected = append(protected, key)
		}
	}
	if len(protected) == 0 {
		t.Fatal("weighted reservation retained no other rows")
	}
}

func TestBaseReadCache_CommitmentBorrowsUnusedReservation(t *testing.T) {
	const (
		commitmentPrefix = "commitment-"
		shard            = uint32(0)
	)
	c := newBaseReadCache(baseReadCacheShardCount*4096, commitmentPrefix)
	s := &c.shards[shard]
	value := bytes.Repeat([]byte{0x6b}, 320)
	for _, key := range testBaseReadCacheKeysForShard(t, commitmentPrefix, shard, 24) {
		testBaseReadCacheSet(c, key, value)
	}
	if s.nonCommitmentUsed != 0 {
		t.Fatalf("unused other reservation retained %d bytes", s.nonCommitmentUsed)
	}
	if s.used <= s.limit-s.nonCommitmentLimit {
		t.Fatalf("commitment used=%d did not borrow unused reservation above %d", s.used, s.limit-s.nonCommitmentLimit)
	}
	if s.used > s.limit {
		t.Fatalf("commitment used=%d exceeds total limit=%d", s.used, s.limit)
	}
}

func TestBaseReadCache_OtherReclaimsBorrowedCommitmentCapacity(t *testing.T) {
	const (
		commitmentPrefix = "commitment-"
		shard            = uint32(0)
	)
	c := newBaseReadCache(baseReadCacheShardCount*4096, commitmentPrefix)
	s := &c.shards[shard]
	value := bytes.Repeat([]byte{0x7c}, 320)
	commitmentKeys := testBaseReadCacheKeysForShard(t, commitmentPrefix, shard, 24)
	for _, key := range commitmentKeys {
		testBaseReadCacheSet(c, key, value)
	}
	commitmentBefore := 0
	for _, entry := range s.entries {
		if !entry.nonCommitment {
			commitmentBefore++
		}
	}
	if s.used <= s.limit-s.nonCommitmentLimit {
		t.Fatalf("precondition: commitment did not borrow reservation: used=%d limit=%d reserve=%d", s.used, s.limit, s.nonCommitmentLimit)
	}

	otherKeys := testBaseReadCacheKeysForShard(t, "flat-reclaim-", shard, 3)
	for _, key := range otherKeys {
		testBaseReadCacheSet(c, key, value)
	}
	commitmentAfter := 0
	otherAfter := 0
	for _, entry := range s.entries {
		if entry.nonCommitment {
			otherAfter++
		} else {
			commitmentAfter++
		}
	}
	maxEntryCharge := len(otherKeys[0]) + len(value) + baseReadCacheEntryOverhead
	if otherAfter == 0 || s.nonCommitmentUsed == 0 || s.nonCommitmentUsed > s.nonCommitmentLimit+maxEntryCharge {
		t.Fatalf("other reclaim entries=%d used=%d limit=%d", otherAfter, s.nonCommitmentUsed, s.nonCommitmentLimit)
	}
	if commitmentAfter >= commitmentBefore {
		t.Fatalf("commitment entries before=%d after=%d, want reclaimed capacity", commitmentBefore, commitmentAfter)
	}
	if s.used > s.limit {
		t.Fatalf("reclaimed total used=%d exceeds limit=%d", s.used, s.limit)
	}

	// Later commitment pressure must keep the newly established reservation.
	protected := make([]string, 0, otherAfter)
	for key, entry := range s.entries {
		if entry.nonCommitment {
			protected = append(protected, key)
		}
	}
	for _, key := range testBaseReadCacheKeysForShard(t, commitmentPrefix+"later-", shard, 24) {
		testBaseReadCacheSet(c, key, value)
	}
	for _, key := range protected {
		if entry := s.entries[key]; entry == nil || !entry.nonCommitment {
			t.Fatalf("reclaimed other key %q displaced by later commitment churn", key)
		}
	}
}

func TestBaseReadCache_AdmissionEvidenceIsolatedByClass(t *testing.T) {
	const commitmentPrefix = "commitment-"
	c := newBaseReadCache(1<<20, commitmentPrefix)
	otherKey := []byte("flat-admission-evidence")
	s := &c.shards[baseReadCacheShardIndex(otherKey)]
	otherFingerprint := baseReadCacheAdmissionFingerprint(otherKey)
	otherIndex := otherFingerprint & uint64(len(s.nonCommitmentAdmission)-1)

	_, _, epoch := c.getWithEpoch(otherKey)
	if _, stored := c.setIfEpoch(otherKey, []byte("flat"), epoch); stored {
		t.Fatal("first other sighting bypassed probation")
	}
	if s.nonCommitmentAdmission[otherIndex] != otherFingerprint {
		t.Fatal("other probation evidence was not recorded in its own table")
	}

	// Commitment activity uses a disjoint table even if its direct-map index is
	// identical, so a streaming branch scan cannot erase the flat row's first
	// observation.
	commitmentKeys := testBaseReadCacheKeysForShard(t, commitmentPrefix, baseReadCacheShardIndex(otherKey), 1)
	commitmentKey := commitmentKeys[0]
	_, _, commitmentEpoch := c.getWithEpoch(commitmentKey)
	c.setIfEpoch(commitmentKey, []byte("branch"), commitmentEpoch)
	if s.nonCommitmentAdmission[otherIndex] != otherFingerprint {
		t.Fatal("commitment probation displaced other evidence")
	}

	_, _, epoch = c.getWithEpoch(otherKey)
	if got, stored := c.setIfEpoch(otherKey, []byte("flat"), epoch); !stored || string(got) != "flat" {
		t.Fatalf("second other sighting = (%q,%v), want admitted", got, stored)
	}
}

func TestBaseReadCache_OtherRefreshDeleteAndClearAccounting(t *testing.T) {
	c := newBaseReadCache(1<<20, "commitment-")
	key := []byte("flat-latest-account-row")
	testBaseReadCacheSet(c, key, []byte("old"))
	s := &c.shards[baseReadCacheShardIndex(key)]
	entry := s.entries[string(key)]
	if entry == nil || !entry.nonCommitment || s.nonCommitmentUsed != entry.charge {
		t.Fatalf("initial other accounting entry=%p used=%d", entry, s.nonCommitmentUsed)
	}
	c.setFlushed(string(key), []byte("replacement-value"))
	entry = s.entries[string(key)]
	if entry == nil || string(entry.value) != "replacement-value" || s.nonCommitmentUsed != entry.charge {
		t.Fatalf("refreshed other accounting entry=%p used=%d", entry, s.nonCommitmentUsed)
	}
	c.del(key)
	if s.nonCommitmentUsed != 0 || s.used != 0 || len(s.entries) != 0 {
		t.Fatalf("delete accounting used=%d other=%d entries=%d", s.used, s.nonCommitmentUsed, len(s.entries))
	}
	testBaseReadCacheSet(c, key, []byte("again"))
	c.clear()
	if s.nonCommitmentUsed != 0 || s.used != 0 || len(s.entries) != 0 || len(s.nonCommitmentQueue) != 0 {
		t.Fatalf("clear accounting used=%d other=%d entries=%d queue=%d", s.used, s.nonCommitmentUsed, len(s.entries), len(s.nonCommitmentQueue))
	}
}

func TestBaseReadCache_NoPrefixKeepsLegacySingleClock(t *testing.T) {
	c := newBaseReadCache(1 << 20)
	key := []byte("flat-looking-key-without-policy")
	testBaseReadCacheSet(c, key, []byte("value"))
	s := &c.shards[baseReadCacheShardIndex(key)]
	entry := s.entries[string(key)]
	if entry == nil || entry.nonCommitment {
		t.Fatalf("legacy entry=%p other=%v", entry, entry != nil && entry.nonCommitment)
	}
	if s.nonCommitmentLimit != 0 || len(s.nonCommitmentAdmission) != 0 || len(s.nonCommitmentQueue) != 0 {
		t.Fatalf("legacy cache unexpectedly enabled policy: limit=%d admission=%d queue=%d", s.nonCommitmentLimit, len(s.nonCommitmentAdmission), len(s.nonCommitmentQueue))
	}
}

func TestBaseReadCache_OtherStaleQueueCompactsAndReinserts(t *testing.T) {
	const shard = uint32(0)
	c := newBaseReadCache(8<<20, "commitment-")
	s := &c.shards[shard]
	keys := testBaseReadCacheKeysForShard(t, "flat-compact-", shard, 2500)
	for _, key := range keys {
		testBaseReadCacheSet(c, key, []byte("value"))
	}
	for _, key := range keys {
		c.del(key)
	}
	if s.used != 0 || s.nonCommitmentUsed != 0 || len(s.entries) != 0 {
		t.Fatalf("post-delete used=%d other=%d entries=%d", s.used, s.nonCommitmentUsed, len(s.entries))
	}
	if live := len(s.nonCommitmentQueue) - s.nonCommitmentHead; live >= 2048 {
		t.Fatalf("stale other CLOCK tokens=%d, want compacted below 2048", live)
	}

	for _, key := range keys[:32] {
		testBaseReadCacheSet(c, key, []byte("again"))
		if entry := s.entries[string(key)]; entry == nil || !entry.nonCommitment {
			t.Fatalf("reinserted key %q missing from other CLOCK", key)
		}
	}
	if s.used > s.limit || s.nonCommitmentUsed > s.used {
		t.Fatalf("reinsert accounting used=%d other=%d limit=%d", s.used, s.nonCommitmentUsed, s.limit)
	}
}

func TestBaseReadCache_RecyclesPrivateKeyStorage(t *testing.T) {
	shard := baseReadCacheShard{limit: 1 << 20}
	firstKey := strings.Repeat("a", 64)
	firstValue := bytes.Repeat([]byte{0x11}, 128)
	entry := shard.acquireEntryString(firstKey, firstValue, false, 1)
	firstStorage := unsafe.StringData(entry.key)
	firstValueStorage := unsafe.SliceData(entry.value)
	shard.recycleEntry(entry)
	if got := shard.freeValueBytes; got != cap(entry.value) {
		t.Fatalf("free value bytes = %d, want %d", got, cap(entry.value))
	}

	secondKey := strings.Repeat("b", 48)
	secondValue := bytes.Repeat([]byte{0x22}, 96)
	reused := shard.acquireEntryString(secondKey, secondValue, false, 2)
	if reused != entry {
		t.Fatal("recycled entry metadata was not reused")
	}
	if got := unsafe.StringData(reused.key); got != firstStorage {
		t.Fatalf("recycled key storage pointer = %p, want %p", got, firstStorage)
	}
	if reused.keyCapacity != uint32(len(firstKey)) {
		t.Fatalf("recycled key capacity = %d, want %d", reused.keyCapacity, len(firstKey))
	}
	if got := unsafe.SliceData(reused.value); got != firstValueStorage {
		t.Fatalf("recycled value storage pointer = %p, want %p", got, firstValueStorage)
	}
	if reused.key != secondKey || !bytes.Equal(reused.value, secondValue) {
		t.Fatalf("reused entry key/value mismatch: key=%q valueBytes=%d", reused.key, len(reused.value))
	}
	if got, want := reused.charge, int(reused.keyCapacity)+cap(reused.value)+baseReadCacheEntryOverhead; got != want {
		t.Fatalf("reused entry charge = %d, want physical capacity charge %d", got, want)
	}
	if shard.freeValueBytes != 0 {
		t.Fatalf("borrowed value storage remained charged to free pool: %d", shard.freeValueBytes)
	}
	shard.recycleEntry(reused)
	thirdKey := strings.Repeat("c", 60)
	reused = shard.acquireEntryBytes([]byte(thirdKey), nil, true, 3)
	if got := unsafe.StringData(reused.key); got != firstStorage {
		t.Fatalf("capacity lost after shorter key: pointer = %p, want %p", got, firstStorage)
	}
	if reused.key != thirdKey {
		t.Fatalf("byte-source reused key = %q, want %q", reused.key, thirdKey)
	}
}

func TestBaseReadCache_RecycledValueStorageIsBoundedAndUnexposed(t *testing.T) {
	shard := baseReadCacheShard{limit: 8 << 10}
	exposed := shard.acquireEntryString("exposed-key", bytes.Repeat([]byte{0x31}, 512), false, 1)
	exposed.exposed.Store(true)
	shard.recycleEntry(exposed)
	if exposed.value != nil || shard.freeValueBytes != 0 {
		t.Fatalf("exposed value entered free pool: valueBytes=%d freeBytes=%d", len(exposed.value), shard.freeValueBytes)
	}

	first := shard.acquireEntryString("first-key", bytes.Repeat([]byte{0x41}, 768), false, 2)
	second := shard.acquireEntryString("second-key", bytes.Repeat([]byte{0x42}, 768), false, 3)
	shard.recycleEntry(first)
	shard.recycleEntry(second)
	if got, want := shard.freeValueBytes, 768; got != want {
		t.Fatalf("bounded free value bytes = %d, want %d", got, want)
	}
	if second.value != nil {
		t.Fatal("value exceeding the shard free budget was retained")
	}
}

func TestBaseReadCache_SnapshotVersionRejectsFutureReplacement(t *testing.T) {
	c := newBaseReadCache(1 << 20)
	key := []byte("commitment-hot-branch")
	testBaseReadCacheSet(c, key, []byte("old"))
	captured := c.version.Load()
	if got, ok, _, _ := c.getAtVersion(key, captured); !ok || string(got) != "old" {
		t.Fatalf("captured cache = (%q,%v), want old/true", got, ok)
	}
	c.advanceVersion()
	c.setFlushed(string(key), []byte("new"))
	if got, ok, _, cacheable := c.getAtVersion(key, captured); ok || got != nil || cacheable {
		t.Fatalf("future replacement = (%q,%v), want nil/false", got, ok)
	}
	if got, ok, _, _ := c.getAtVersion(key, c.version.Load()); !ok || string(got) != "new" {
		t.Fatalf("current cache = (%q,%v), want new/true", got, ok)
	}
}

func TestBaseReadCache_MissingAdmissionAndFlushRefresh(t *testing.T) {
	c := newBaseReadCache(1 << 20)
	key := []byte("missing-permission-row")

	_, _, epoch := c.getWithEpoch(key)
	if c.setMissingIfEpoch(key, epoch) {
		t.Fatal("first missing-row sighting bypassed probation")
	}
	_, _, epoch = c.getWithEpoch(key)
	if !c.setMissingIfEpoch(key, epoch) {
		t.Fatal("second missing-row sighting was not admitted")
	}
	if got, ok, _ := c.getWithEpoch(key); !ok || got != nil {
		t.Fatalf("cached missing row = (%v,%v), want (nil,true)", got, ok)
	}

	// A canonical put refreshes the resident miss before its layer is dropped.
	c.setFlushed(string(key), []byte("permission"))
	if got, ok, _ := c.getWithEpoch(key); !ok || string(got) != "permission" {
		t.Fatalf("flushed replacement = (%q,%v), want (permission,true)", got, ok)
	}

	// Present empty values must stay distinct from the nil miss sentinel.
	c.setFlushed(string(key), []byte{})
	if got, ok, _ := c.getWithEpoch(key); !ok || got == nil || len(got) != 0 {
		t.Fatalf("present empty replacement = (%v,%v), want (non-nil empty,true)", got, ok)
	}
}

func TestBaseReadCache_BoundedPayloadAndInvalidationQueue(t *testing.T) {
	const size = 64 * 256
	c := newBaseReadCache(size)
	totalLimit := 0
	for i := range c.shards {
		totalLimit += c.shards[i].limit
	}
	if totalLimit != size {
		t.Fatalf("shard limits sum to %d, want exact configured budget %d", totalLimit, size)
	}
	value := make([]byte, 96)
	for i := 0; i < 10_000; i++ {
		key := []byte(fmt.Sprintf("branch-%08d", i))
		testBaseReadCacheSet(c, key, value)
	}
	for i := range c.shards {
		s := &c.shards[i]
		if s.used > s.limit {
			t.Fatalf("shard %d retained %d bytes above limit %d", i, s.used, s.limit)
		}
	}

	// Repeated populate→flush-invalidate cycles must not accumulate one stale
	// CLOCK queue entry per block for the lifetime of a long sync session.
	key := []byte("repeated-hot-branch")
	shard := &c.shards[baseReadCacheShardIndex(key)]
	for i := 0; i < 10_000; i++ {
		testBaseReadCacheSet(c, key, value)
		c.del(key)
	}
	if live := len(shard.queue) - shard.head; live >= 2048 {
		t.Fatalf("invalidation queue retained %d stale entries, want <2048", live)
	}
}

func TestBaseReadCache_SecondChancePreservesReferencedOldestEntry(t *testing.T) {
	c := newBaseReadCache(1 << 20)
	keys := make([][]byte, 0, 3)
	for candidate := 0; len(keys) < 3; candidate++ {
		key := []byte(fmt.Sprintf("clock-key-%08d", candidate))
		if len(keys) == 0 || baseReadCacheShardIndex(key) == baseReadCacheShardIndex(keys[0]) {
			keys = append(keys, key)
		}
	}
	value := []byte("v")
	charge := len(keys[0]) + len(value) + baseReadCacheEntryOverhead
	s := &c.shards[baseReadCacheShardIndex(keys[0])]
	s.limit = 2 * charge

	testBaseReadCacheSet(c, keys[0], value)
	testBaseReadCacheSet(c, keys[1], value)
	if _, ok, _ := c.getWithEpoch(keys[0]); !ok {
		t.Fatal("oldest entry missed before marking it referenced")
	}
	testBaseReadCacheSet(c, keys[2], value)

	if _, ok, _ := c.getWithEpoch(keys[0]); !ok {
		t.Fatal("referenced oldest entry did not receive a second chance")
	}
	if _, ok, _ := c.getWithEpoch(keys[1]); ok {
		t.Fatal("unreferenced entry survived ahead of the referenced oldest entry")
	}
	if _, ok, _ := c.getWithEpoch(keys[2]); !ok {
		t.Fatal("newly admitted entry was not retained")
	}
	if s.used > s.limit {
		t.Fatalf("used bytes=%d exceed limit=%d", s.used, s.limit)
	}
}

func TestBaseReadCache_RepeatedHitsAccumulateBoundedClockCredit(t *testing.T) {
	value := bytes.Repeat([]byte{0x7c}, 128)
	c := newBaseReadCache(1 << 20)
	keys := testBaseReadCacheKeysForShard(t, "credit-", 0, 1)
	charge := len(keys[0]) + len(value) + baseReadCacheEntryOverhead
	s := &c.shards[baseReadCacheShardIndex(keys[0])]
	s.limit = charge

	testBaseReadCacheSet(c, keys[0], value)
	for range baseReadCacheMaxReferenceCredit + 4 {
		if _, ok, _ := c.getWithEpoch(keys[0]); !ok {
			t.Fatal("hot entry missed while accumulating CLOCK credit")
		}
	}
	entry := s.entries[string(keys[0])]
	if got := entry.references.Load(); got != baseReadCacheMaxReferenceCredit {
		t.Fatalf("reference credit = %d, want saturated %d", got, baseReadCacheMaxReferenceCredit)
	}

	// Each transient insert is admitted through the normal two-hit probation.
	// The hot entry should spend one credit per sweep and survive all three;
	// the fourth pressure wave may finally evict it if it has not been hit again.
	for wave := uint32(0); wave < baseReadCacheMaxReferenceCredit; wave++ {
		key := testBaseReadCacheKeysForShard(t, fmt.Sprintf("wave-%d-", wave), 0, 1)[0]
		testBaseReadCacheSet(c, key, value)
		if _, ok, _ := c.getWithEpoch(keys[0]); !ok {
			t.Fatalf("hot entry evicted on credited wave %d", wave)
		}
		// Undo the observation made by the assertion so the test checks the
		// originally accumulated budget rather than replenishing it.
		entry.references.Add(^uint32(0))
	}
}

func TestBaseReadCache_EvictionReusesEntryMetadata(t *testing.T) {
	c := newBaseReadCache(1 << 20)
	keys := make([][]byte, 0, 3)
	for candidate := 0; len(keys) < cap(keys); candidate++ {
		key := []byte(fmt.Sprintf("recycle-entry-%08d", candidate))
		if len(keys) == 0 || baseReadCacheShardIndex(key) == baseReadCacheShardIndex(keys[0]) {
			keys = append(keys, key)
		}
	}
	value := []byte("v")
	s := &c.shards[baseReadCacheShardIndex(keys[0])]
	s.limit = len(keys[0]) + len(value) + baseReadCacheEntryOverhead

	testBaseReadCacheSet(c, keys[0], value)
	first := s.entries[string(keys[0])]
	if first == nil {
		t.Fatal("first entry was not admitted")
	}
	testBaseReadCacheSet(c, keys[1], value)
	if _, ok := s.entries[string(keys[0])]; ok {
		t.Fatal("first entry survived a one-entry cache eviction")
	}
	testBaseReadCacheSet(c, keys[2], value)
	if got := s.entries[string(keys[2])]; got != first {
		t.Fatalf("third admission entry = %p, want recycled first entry %p", got, first)
	}
	if first.key != string(keys[2]) || string(first.value) != string(value) || !first.live {
		t.Fatalf("recycled entry = {key:%q value:%q live:%v}", first.key, first.value, first.live)
	}
	if s.freeEntryCount != 1 {
		t.Fatalf("free entry count = %d, want the evicted second entry", s.freeEntryCount)
	}
}

func TestBaseReadCache_ConcurrentHitAndFlushRefresh(t *testing.T) {
	c := newBaseReadCache(1 << 20)
	key := []byte("concurrent-cache-hit-and-flush")
	valueA := bytes.Repeat([]byte{'a'}, 128)
	valueB := bytes.Repeat([]byte{'b'}, 128)
	testBaseReadCacheSet(c, key, valueA)

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		for i := 0; i < 10_000; i++ {
			got, ok, _ := c.getWithEpoch(key)
			if !ok || (!bytes.Equal(got, valueA) && !bytes.Equal(got, valueB)) {
				t.Errorf("concurrent hit = (%x,%v)", got, ok)
				return
			}
		}
	}()
	go func() {
		defer wg.Done()
		for i := 0; i < 10_000; i++ {
			if i&1 == 0 {
				c.setFlushed(string(key), valueB)
			} else {
				c.setFlushed(string(key), valueA)
			}
		}
	}()
	wg.Wait()
}

func TestBaseReadCache_SetFlushedRefreshesOnlyCachedKeys(t *testing.T) {
	c := newBaseReadCache(1 << 20)
	cachedKey := []byte("cached-commitment-branch")
	uncachedKey := "unrelated-block-metadata"
	oldValue := []byte("old")
	// Model commitment's putBranchesSorted layout: this tiny value is a capped
	// subslice of a much larger sibling arena. The cache must not retain it.
	arena := make([]byte, 1<<20)
	copy(arena[123:126], "new")
	newValue := arena[123:126:126]
	testBaseReadCacheSet(c, cachedKey, oldValue)
	shard := &c.shards[baseReadCacheShardIndex(cachedKey)]
	foundCachedKey := false
	for key := range shard.entries {
		if key == string(cachedKey) {
			foundCachedKey = true
			break
		}
	}
	if !foundCachedKey {
		t.Fatal("cached key missing before refresh")
	}
	keyArena := strings.Repeat("x", 1<<20) + string(cachedKey)
	flushedKey := keyArena[1<<20:]

	c.setFlushed(flushedKey, newValue)
	got, ok, _ := c.getWithEpoch(cachedKey)
	if !ok || string(got) != "new" {
		t.Fatalf("flushed cached value = (%q,%v), want (new,true)", got, ok)
	}
	if len(got) == 0 || &got[0] == &newValue[0] {
		t.Fatal("flushed arena slice was retained instead of copied into cache-owned storage")
	}
	for key := range shard.entries {
		if key == string(cachedKey) && unsafe.StringData(key) == unsafe.StringData(flushedKey) {
			t.Fatal("flush retained the layer-arena string instead of a cache-owned key")
		}
	}

	c.setFlushed(uncachedKey, []byte("metadata"))
	if _, ok, _ := c.getWithEpoch([]byte(uncachedKey)); ok {
		t.Fatal("flush admitted a key that was never read through the cache")
	}

	for i := 0; i < 10_000; i++ {
		c.setFlushed(string(cachedKey), newValue)
	}
	if live := len(shard.queue) - shard.head; live != 1 {
		t.Fatalf("flush refresh queue retained %d entries, want the original 1", live)
	}
}

func TestBaseReadCache_FlushRefreshKeepsCanonicalKeySeparateFromValue(t *testing.T) {
	c := newBaseReadCache(1 << 20)
	key := []byte("state-commitment-branch-v1-canonical-key")
	testBaseReadCacheSet(c, key, []byte("old-value"))

	s := &c.shards[baseReadCacheShardIndex(key)]
	before := s.entries[string(key)]
	if len(s.queue) != 1 {
		t.Fatalf("queue entries=%d, want 1", len(s.queue))
	}
	var keyPtr *byte
	for residentKey := range s.entries {
		keyPtr = unsafe.StringData(residentKey)
	}
	if keyPtr == nil || unsafe.StringData(s.queue[0].key) != keyPtr {
		t.Fatal("map entry and CLOCK queue entry do not share the canonical key")
	}
	oldValuePtr := unsafe.SliceData(before.value)
	beforeUsed := s.used

	c.setFlushed(string(key), []byte("replacement-value"))
	after := s.entries[string(key)]
	if after != before {
		t.Fatal("flush refresh replaced the stable cache entry")
	}
	var refreshedKeyPtr *byte
	for residentKey := range s.entries {
		refreshedKeyPtr = unsafe.StringData(residentKey)
	}
	if refreshedKeyPtr != keyPtr || unsafe.StringData(s.queue[0].key) != keyPtr {
		t.Fatal("flush refresh replaced the canonical key")
	}
	if unsafe.SliceData(after.value) == oldValuePtr {
		t.Fatal("flush refresh reused mutable value storage")
	}
	if got := string(after.value); got != "replacement-value" {
		t.Fatalf("replacement value=%q", got)
	}
	if want := beforeUsed + len("replacement-value") - len("old-value"); s.used != want {
		t.Fatalf("used bytes=%d, want %d after differently-sized refresh", s.used, want)
	}

	equalValue := append([]byte(nil), after.value...)
	if unsafe.SliceData(equalValue) == unsafe.SliceData(after.value) {
		t.Fatal("test equal value unexpectedly aliases resident storage")
	}
	residentKey := string(key)
	residentValuePtr := unsafe.SliceData(after.value)
	if allocs := testing.AllocsPerRun(100, func() {
		c.setFlushed(residentKey, equalValue)
	}); allocs != 0 {
		t.Fatalf("byte-identical flush allocations=%v, want 0", allocs)
	}
	if unsafe.SliceData(after.value) != residentValuePtr {
		t.Fatal("byte-identical flush replaced immutable resident storage")
	}
}

func TestBaseReadCache_ScopedViewAllowsInPlaceFlushRefresh(t *testing.T) {
	c := newBaseReadCache(1<<20, "state-commitment-branch-v1-")
	key := []byte("state-commitment-branch-v1-scoped-refresh")
	oldValue := []byte("branch-value-one")
	newValue := []byte("branch-value-two")

	// Complete two-hit admission without returning the cache-owned slice.
	for attempt := 0; attempt < 2; attempt++ {
		_, _, epoch := c.getWithEpoch(key)
		stored := c.storeIfEpoch(key, oldValue, epoch)
		if stored != (attempt == 1) {
			t.Fatalf("attempt %d stored=%v", attempt, stored)
		}
	}

	shard := &c.shards[baseReadCacheShardIndex(key)]
	entry := shard.entries[string(key)]
	before := unsafe.SliceData(entry.value)
	called := 0
	cached, present, _, err := c.viewWithEpoch(key, func(value []byte, stable bool) error {
		called++
		if stable || !bytes.Equal(value, oldValue) {
			t.Fatalf("scoped cache view = (%q, stable=%v)", value, stable)
		}
		return nil
	})
	if err != nil || !cached || !present || called != 1 {
		t.Fatalf("scoped view = cached=%v present=%v called=%d err=%v", cached, present, called, err)
	}

	c.setFlushed(string(key), newValue)
	if got := string(entry.value); got != string(newValue) {
		t.Fatalf("refreshed value=%q, want %q", got, newValue)
	}
	if unsafe.SliceData(entry.value) != before {
		t.Fatal("callback-scoped refresh replaced reusable value storage")
	}
}

func TestBaseReadCache_DirectGetPreventsInPlaceFlushRefresh(t *testing.T) {
	c := newBaseReadCache(1<<20, "state-commitment-branch-v1-")
	key := []byte("state-commitment-branch-v1-direct-refresh")
	oldValue := []byte("branch-value-one")
	newValue := []byte("branch-value-two")

	for attempt := 0; attempt < 2; attempt++ {
		_, _, epoch := c.getWithEpoch(key)
		c.storeIfEpoch(key, oldValue, epoch)
	}
	retained, ok, _ := c.getWithEpoch(key)
	if !ok || !bytes.Equal(retained, oldValue) {
		t.Fatalf("direct cache get = (%q,%v)", retained, ok)
	}
	retainedPtr := unsafe.SliceData(retained)
	c.setFlushed(string(key), newValue)
	if !bytes.Equal(retained, oldValue) {
		t.Fatalf("directly retained value mutated to %q", retained)
	}
	entry := c.shards[baseReadCacheShardIndex(key)].entries[string(key)]
	if got := string(entry.value); got != string(newValue) {
		t.Fatalf("refreshed value=%q, want %q", got, newValue)
	}
	if unsafe.SliceData(entry.value) == retainedPtr {
		t.Fatal("directly exposed backing was reused by flush")
	}
}

func TestBaseReadCache_FlushAdmitsReadBeforeWriteValue(t *testing.T) {
	c := newBaseReadCache(1<<20, "frequently-mutated-commitment-")
	key := []byte("frequently-mutated-commitment-branch")

	// The first durable parent read records frequency evidence without
	// retaining its value.
	_, _, epoch := c.getWithEpoch(key)
	if _, stored := c.setIfEpoch(key, []byte("parent-v1"), epoch); stored {
		t.Fatal("first parent read bypassed probation")
	}

	// Committing the block is the second observation. It must invalidate the old
	// durable generation and directly admit the newer canonical value, avoiding
	// one otherwise-mandatory Pebble read in the next block.
	c.setFlushed(string(key), []byte("child-v2"))
	if got, ok, _ := c.getWithEpoch(key); !ok || string(got) != "child-v2" {
		t.Fatalf("flush-admitted value = (%q,%v), want child-v2/true", got, ok)
	}
	if got := len(c.shards[baseReadCacheShardIndex(key)].queue); got != 1 {
		t.Fatalf("flush-admitted queue entries = %d, want 1", got)
	}
}

func TestBaseReadCache_WriteOnlyFlushDoesNotDisplaceReadProbation(t *testing.T) {
	c := newBaseReadCache(1<<20, "read-probation-")
	hotKey := []byte("read-probation-key")
	hotFingerprint := baseReadCacheAdmissionFingerprint(hotKey)
	hotShard := &c.shards[baseReadCacheShardIndex(hotKey)]
	hotIndex := hotFingerprint & uint64(len(hotShard.admission)-1)

	_, _, epoch := c.getWithEpoch(hotKey)
	if _, stored := c.setIfEpoch(hotKey, []byte("parent"), epoch); stored {
		t.Fatal("first parent read bypassed probation")
	}
	if hotShard.admission[hotIndex] != hotFingerprint {
		t.Fatal("parent read did not establish probation")
	}

	// Find a write-only key that maps to the same payload shard and probation
	// slot. Its flush must neither be admitted nor replace the read evidence.
	var writeOnly string
	for i := 0; i < 1_000_000; i++ {
		candidate := fmt.Sprintf("read-probation-write-only-%08d", i)
		fingerprint := baseReadCacheAdmissionFingerprintString(candidate)
		if baseReadCacheShardIndexString(candidate) == baseReadCacheShardIndex(hotKey) &&
			fingerprint&uint64(len(hotShard.admission)-1) == hotIndex &&
			fingerprint != hotFingerprint {
			writeOnly = candidate
			break
		}
	}
	if writeOnly == "" {
		t.Fatal("failed to find colliding write-only key")
	}
	c.setFlushed(writeOnly, []byte("metadata"))
	if _, ok, _ := c.getWithEpoch([]byte(writeOnly)); ok {
		t.Fatal("write-only flush was admitted")
	}
	if hotShard.admission[hotIndex] != hotFingerprint {
		t.Fatal("write-only flush displaced read probation")
	}

	_, _, epoch = c.getWithEpoch(hotKey)
	if got, stored := c.setIfEpoch(hotKey, []byte("parent-v2"), epoch); !stored || string(got) != "parent-v2" {
		t.Fatalf("second parent read = (%q,%v), want admitted parent-v2", got, stored)
	}
}

func TestBaseReadCache_SetFlushedRejectsLateOldGenerationFill(t *testing.T) {
	c := newBaseReadCache(1 << 20)
	key := []byte("racing-commitment-branch")
	_, _, oldEpoch := c.getWithEpoch(key)

	// There is no resident entry to refresh, but the flush must still advance
	// the generation so a read that began before it cannot publish stale bytes.
	c.setFlushed(string(key), []byte("new"))
	if _, stored := c.setIfEpoch(key, []byte("old"), oldEpoch); stored {
		t.Fatal("pre-flush read populated stale bytes after the flush")
	}
	if _, ok, _ := c.getWithEpoch(key); ok {
		t.Fatal("uncached flush should invalidate without admitting the key")
	}
}

func TestBaseReadCache_FlushAdmissionRejectsLateOldGenerationFill(t *testing.T) {
	c := newBaseReadCache(1<<20, "racing-read-before-write-")
	key := []byte("racing-read-before-write-branch")
	_, _, oldEpoch := c.getWithEpoch(key)
	if _, stored := c.setIfEpoch(key, []byte("old-parent"), oldEpoch); stored {
		t.Fatal("first parent read bypassed probation")
	}

	// A second reader started against the old generation before the canonical
	// flush admits the replacement. Its late fill must not replace new bytes.
	_, _, racingEpoch := c.getWithEpoch(key)
	c.setFlushed(string(key), []byte("new-child"))
	if _, stored := c.setIfEpoch(key, []byte("late-old-parent"), racingEpoch); stored {
		t.Fatal("late old-generation fill replaced flush-admitted value")
	}
	if got, ok, _ := c.getWithEpoch(key); !ok || string(got) != "new-child" {
		t.Fatalf("post-race cache = (%q,%v), want new-child/true", got, ok)
	}
}

func TestBaseReadCache_UnrelatedSameShardFlushKeepsFillEligible(t *testing.T) {
	c := newBaseReadCache(1 << 20)
	key := []byte("hot-account-latest-row")
	keyShard := baseReadCacheShardIndex(key)
	keySlot := baseReadCacheInvalidationSlotBytes(key, len(c.invalidations))

	var unrelated string
	for i := 0; i < 100_000; i++ {
		candidate := fmt.Sprintf("unrelated-flushed-row-%08d", i)
		if baseReadCacheShardIndexString(candidate) == keyShard &&
			baseReadCacheInvalidationSlotString(candidate, len(c.invalidations)) != keySlot {
			unrelated = candidate
			break
		}
	}
	if unrelated == "" {
		t.Fatal("failed to find test keys sharing a payload shard but not an invalidation slot")
	}

	// Complete the hot key's first probation sighting.
	_, _, epoch := c.getWithEpoch(key)
	if _, stored := c.setIfEpoch(key, []byte("first"), epoch); stored {
		t.Fatal("first sighting bypassed probation")
	}

	// Capture the generation for its second durable read, then publish an
	// unrelated key routed to the SAME 64-way payload shard. The old shard-wide
	// epoch rejected this fill even though the hot key did not change.
	_, _, epoch = c.getWithEpoch(key)
	c.setFlushed(unrelated, []byte("unrelated"))
	if _, stored := c.setIfEpoch(key, []byte("second"), epoch); !stored {
		t.Fatal("unrelated same-shard flush falsely rejected hot-key fill")
	}
}

func TestBaseReadCache_InvalidationSlotByteStringParity(t *testing.T) {
	for _, size := range []int{baseReadCacheShardCount * 256, 1 << 20, 128 << 20} {
		slots := baseReadCacheInvalidationSlots(size)
		for i := 0; i < 1_000; i++ {
			key := fmt.Sprintf("state-commitment-branch-v1-%02x-%08x-tail", i&15, i*0x9e37)
			gotBytes := baseReadCacheInvalidationSlotBytes([]byte(key), slots)
			gotString := baseReadCacheInvalidationSlotString(key, slots)
			if gotBytes != gotString {
				t.Fatalf("size=%d key=%q byte slot=%d string slot=%d", size, key, gotBytes, gotString)
			}
		}
	}
	if got := baseReadCacheInvalidationSlots(128 << 20); got != baseReadCacheMaxInvalidationSlots {
		t.Fatalf("128 MiB invalidation slots=%d, want %d", got, baseReadCacheMaxInvalidationSlots)
	}
}

func TestBaseReadCache_SetFlushedDropsOversizedReplacement(t *testing.T) {
	// 256 bytes per shard: the original row fits, the replacement does not.
	c := newBaseReadCache(baseReadCacheShardCount * 256)
	key := []byte("hot-branch")
	testBaseReadCacheSet(c, key, []byte("old"))
	if _, ok, _ := c.getWithEpoch(key); !ok {
		t.Fatal("test setup did not cache original value")
	}

	c.setFlushed(string(key), make([]byte, 512))
	if _, ok, _ := c.getWithEpoch(key); ok {
		t.Fatal("oversized flushed replacement retained a stale or over-budget entry")
	}
}

func TestBaseReadCache_InvalidationReleasesQueuedEntryPayload(t *testing.T) {
	c := newBaseReadCache(8 << 20)
	key := []byte("queued-entry-with-large-owned-payload")
	value := bytes.Repeat([]byte{0x5a}, 64<<10)
	testBaseReadCacheSet(c, key, value)

	s := &c.shards[baseReadCacheShardIndex(key)]
	entry := s.entries[string(key)]
	if entry == nil || len(s.queue) != 1 || s.queue[0] != entry {
		t.Fatal("test setup did not retain one stable queued entry")
	}
	c.del(key)
	if entry.live || entry.key != string(key) || entry.value != nil || entry.keyCapacity != uint32(len(key)) {
		t.Fatalf("invalidated queued entry retained live=%v key=%q valueBytes=%d keyCap=%d", entry.live, entry.key, len(entry.value), entry.keyCapacity)
	}
	if len(s.queue) != 1 || s.queue[0] != entry {
		t.Fatal("test requires the cleared entry to remain as a stale queue pointer")
	}
}

func TestBaseReadCache_RacingSameEpochFillsPublishOnce(t *testing.T) {
	c := newBaseReadCache(1 << 20)
	key := []byte("same-generation-branch")
	_, _, epoch := c.getWithEpoch(key)
	first, stored := c.setIfEpoch(key, []byte("durable-value"), epoch)
	if stored {
		t.Fatal("first fill bypassed probation")
	}
	second, stored := c.setIfEpoch(key, []byte("duplicate-read"), epoch)
	if !stored || string(second) != "duplicate-read" {
		t.Fatalf("second racing fill = (%q,%v), want admitted duplicate-read", second, stored)
	}
	third, stored := c.setIfEpoch(key, []byte("late-read"), epoch)
	if !stored || string(third) != "duplicate-read" {
		t.Fatalf("late racing fill = (%q,%v), want existing duplicate-read", third, stored)
	}
	_ = first
	s := &c.shards[baseReadCacheShardIndex(key)]
	if len(s.entries) != 1 || len(s.queue)-s.head != 1 {
		t.Fatalf("racing fills published map=%d queue=%d, want 1/1", len(s.entries), len(s.queue)-s.head)
	}
}

func TestBaseReadCache_PromoteFlushedShard(t *testing.T) {
	c := newBaseReadCache(1 << 20)
	keys := make([]string, 0, 3)
	for i := 0; len(keys) < cap(keys); i++ {
		key := fmt.Sprintf("promote-shard-%08x", i)
		if baseReadCacheShardIndexString(key) == 0 {
			keys = append(keys, key)
		}
	}
	refreshKey, deleteKey, uncachedKey := keys[0], keys[1], keys[2]
	testBaseReadCacheSet(c, []byte(refreshKey), []byte("old-refresh"))
	testBaseReadCacheSet(c, []byte(deleteKey), []byte("old-delete"))
	_, _, staleEpoch := c.getWithEpoch([]byte(uncachedKey))

	c.promoteFlushedShard(
		map[string][]byte{
			refreshKey:  []byte("new-refresh"),
			uncachedKey: []byte("uncached-write"),
		},
		map[string]struct{}{deleteKey: {}},
		0,
	)
	if got, ok, _ := c.getWithEpoch([]byte(refreshKey)); !ok || string(got) != "new-refresh" {
		t.Fatalf("refreshed value = (%q,%v), want new-refresh", got, ok)
	}
	if _, ok, _ := c.getWithEpoch([]byte(deleteKey)); ok {
		t.Fatal("deleted key remained resident")
	}
	if _, ok, _ := c.getWithEpoch([]byte(uncachedKey)); ok {
		t.Fatal("uncached flushed write was admitted")
	}
	if _, stored := c.setIfEpoch([]byte(uncachedKey), []byte("stale"), staleEpoch); stored {
		t.Fatal("pre-promotion epoch published after shard promotion")
	}
}

func BenchmarkBaseReadCacheFlushedHotKey(b *testing.B) {
	key := []byte("state-commitment-branch-v1-hot-prefix")
	keyString := string(key)
	value := make([]byte, 1500)
	changedValue := make([]byte, len(value))
	changedValue[0] = 1

	b.Run("invalidate_and_refill", func(b *testing.B) {
		c := newBaseReadCache(1 << 20)
		testBaseReadCacheSet(c, key, value)
		b.ReportAllocs()
		b.ResetTimer()
		for range b.N {
			c.del(key)
			_, _, epoch := c.getWithEpoch(key)
			if _, stored := c.setIfEpoch(key, value, epoch); stored {
				b.Fatal("first refill bypassed probation")
			}
			if _, stored := c.setIfEpoch(key, value, epoch); !stored {
				b.Fatal("second refill rejected")
			}
		}
	})

	b.Run("refresh_from_layer", func(b *testing.B) {
		c := newBaseReadCache(1 << 20)
		testBaseReadCacheSet(c, key, value)
		b.ReportAllocs()
		b.ResetTimer()
		for range b.N {
			c.setFlushed(keyString, value)
		}
	})

	b.Run("refresh_from_known_layer_shard", func(b *testing.B) {
		c := newBaseReadCache(1 << 20)
		testBaseReadCacheSet(c, key, value)
		shard := baseReadCacheShardIndexString(keyString)
		b.ReportAllocs()
		b.ResetTimer()
		for range b.N {
			c.setFlushedAt(keyString, value, shard)
		}
	})

	b.Run("refresh_changed_from_known_layer_shard", func(b *testing.B) {
		c := newBaseReadCache(1 << 20)
		testBaseReadCacheSet(c, key, value)
		shard := baseReadCacheShardIndexString(keyString)
		b.ReportAllocs()
		b.ResetTimer()
		for i := range b.N {
			next := value
			if i&1 == 0 {
				next = changedValue
			}
			c.setFlushedAt(keyString, next, shard)
		}
	})
}

func BenchmarkBaseReadCachePromoteShard(b *testing.B) {
	const keyCount = 1024
	value := bytes.Repeat([]byte{0xab}, 128)
	writes := make(map[string][]byte, keyCount)
	for i := 0; len(writes) < keyCount; i++ {
		key := fmt.Sprintf("commitment-branch-%08x-%08x", i*2654435761, i)
		if baseReadCacheShardIndexString(key) == 0 {
			writes[key] = value
		}
	}

	for _, bulk := range []bool{false, true} {
		name := "per-key-lock"
		if bulk {
			name = "shard-lock"
		}
		b.Run(name, func(b *testing.B) {
			c := newBaseReadCache(64 << 20)
			for key := range writes {
				testBaseReadCacheSet(c, []byte(key), value)
			}
			b.ReportAllocs()
			b.ReportMetric(keyCount, "keys/op")
			b.ResetTimer()
			for range b.N {
				if bulk {
					c.promoteFlushedShard(writes, nil, 0)
					continue
				}
				for key, value := range writes {
					c.setFlushedAt(key, value, 0)
				}
			}
		})
	}

	b.Run("uncached-write-only", func(b *testing.B) {
		c := newBaseReadCache(64 << 20)
		b.ReportAllocs()
		b.ReportMetric(keyCount, "keys/op")
		b.ResetTimer()
		for range b.N {
			c.promoteFlushedShard(writes, nil, 0)
		}
	})

	b.Run("uncached-priority-namespace", func(b *testing.B) {
		c := newBaseReadCache(64<<20, "commitment-branch-")
		b.ReportAllocs()
		b.ReportMetric(keyCount, "keys/op")
		b.ResetTimer()
		for range b.N {
			c.promoteFlushedShard(writes, nil, 0)
		}
	})
}

func BenchmarkBaseReadCachePromoteMergedFlushGroup(b *testing.B) {
	layers := benchmarkFlushLayers()
	prepared := borrowFlushMergedOps()
	mergeLayers(layers, prepared)
	b.Cleanup(func() { returnFlushMergedOps(prepared) })
	value := bytes.Repeat([]byte{0xab}, 16)

	newCache := func() *baseReadCache {
		c := newBaseReadCache(64 << 20)
		for key, op := range prepared.ops {
			if !op.delete {
				testBaseReadCacheSet(c, []byte(key), value)
			}
		}
		return c
	}

	b.Run("remerge-per-key-lock", func(b *testing.B) {
		owner := &Buffer{baseReadCache: newCache()}
		b.ReportAllocs()
		b.ReportMetric(float64(len(prepared.ops)), "output-keys/op")
		b.ResetTimer()
		for range b.N {
			merged := borrowFlushMergedOps()
			mergeLayers(layers, merged)
			for key, op := range merged.ops {
				if op.delete {
					owner.baseReadCache.delStringAt(key, uint32(op.shard))
				} else {
					owner.baseReadCache.setFlushedAt(key, op.value, uint32(op.shard))
				}
			}
			returnFlushMergedOps(merged)
		}
	})

	b.Run("reuse-merge-shard-lock", func(b *testing.B) {
		owner := &Buffer{baseReadCache: newCache()}
		b.ReportAllocs()
		b.ReportMetric(float64(len(prepared.ops)), "output-keys/op")
		b.ResetTimer()
		for range b.N {
			owner.promoteBaseReadCacheMerged(prepared)
		}
	})
}

func BenchmarkBaseReadCacheRecycledEntryStorage(b *testing.B) {
	keys := [...]string{
		strings.Repeat("a", 96),
		strings.Repeat("b", 80),
	}
	values := [...][]byte{
		bytes.Repeat([]byte{0xa1}, 1024),
		bytes.Repeat([]byte{0xb2}, 768),
	}
	shard := baseReadCacheShard{limit: 1 << 20}
	entry := shard.acquireEntryString(keys[0], values[0], false, 1)
	b.ReportAllocs()
	b.ResetTimer()
	for i := range b.N {
		shard.recycleEntry(entry)
		index := i & 1
		entry = shard.acquireEntryString(keys[index], values[index], false, uint64(i+2))
	}
	baseReadCacheEntryBenchmarkSink = entry
}

func BenchmarkBaseReadCacheAdmissionChurn(b *testing.B) {
	const keyCount = 256
	keys := make([]string, 0, keyCount)
	keyBytes := make([][]byte, 0, keyCount)
	for i := 0; len(keys) < keyCount; i++ {
		key := fmt.Sprintf("commitment-branch-%08x-%08x", i*2654435761, i)
		if baseReadCacheShardIndexString(key) != 0 {
			continue
		}
		keys = append(keys, key)
		keyBytes = append(keyBytes, []byte(key))
	}
	value := bytes.Repeat([]byte{0x5c}, 512)
	c := newBaseReadCache(layerShardCount*4096, "commitment-")
	b.ReportAllocs()
	b.ResetTimer()
	for i := range b.N {
		index := i % keyCount
		_, found, epoch := c.getWithEpoch(keyBytes[index])
		if found {
			b.Fatal("churn key unexpectedly remained resident for a full cycle")
		}
		if c.storeIfEpoch(keyBytes[index], value, epoch) {
			b.Fatal("first churn sighting bypassed probation")
		}
		c.setFlushedAt(keys[index], value, 0)
	}
}

func BenchmarkBaseReadCacheHit(b *testing.B) {
	for _, keyLen := range []int{32, 64, 96, 128} {
		b.Run(fmt.Sprintf("key=%d", keyLen), func(b *testing.B) {
			c := newBaseReadCache(1 << 20)
			key := bytes.Repeat([]byte{0xa5}, keyLen)
			// Give the tail/middle bytes representative entropy rather than
			// benchmarking a degenerate repeated-byte key.
			for i := range key {
				key[i] ^= byte(i * 37)
			}
			testBaseReadCacheSet(c, key, []byte("value"))
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				if _, ok, _ := c.getWithEpoch(key); !ok {
					b.Fatal("cache hit missed")
				}
			}
		})
	}
}
