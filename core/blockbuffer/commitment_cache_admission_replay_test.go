package blockbuffer

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"testing"

	"github.com/tronprotocol/go-tron/core/rawdb"
)

// TestCommitmentCacheAdmissionReplay is an opt-in policy experiment. Each run
// executes the production cache against the same generated operation stream;
// alternative policies are supplied only through Go's source overlay. It counts
// foreground durable lookups, not disk I/O or elapsed-time speedups. No hit-rate
// target is asserted: adverse workloads are part of the experiment.
func TestCommitmentCacheAdmissionReplay(t *testing.T) {
	if os.Getenv("GTRON_CACHE_ADMISSION_REPLAY") != "1" {
		t.Skip("set GTRON_CACHE_ADMISSION_REPLAY=1 for deterministic policy experiments")
	}
	for _, budget := range []int{4 << 20, 16 << 20} {
		for _, seed := range []uint32{17, 53, 101} {
			for _, scenario := range []string{"read_scan", "rmw_scan", "short_rmw_reuse", "long_rmw_reuse", "hot_migration", "mixed_namespaces"} {
				t.Run(fmt.Sprintf("%s/bytes_%d/seed_%d", scenario, budget, seed), func(t *testing.T) {
					runCommitmentAdmissionTrace(t, budget, seed, scenario)
				})
			}
		}
	}
}

// This adverse corpus revisits each newly flushed cohort after exceeding the
// small FIFO window but while the cohort still fits the full cache. Flush-based
// admission can legitimately avoid the second durable read in this workload.
// It is reported separately so adding it cannot change earlier trace hashes.
func TestCommitmentCacheAdmissionPairedReuse(t *testing.T) {
	if os.Getenv("GTRON_CACHE_ADMISSION_REPLAY") != "1" {
		t.Skip("set GTRON_CACHE_ADMISSION_REPLAY=1 for deterministic policy experiments")
	}
	for _, budget := range []int{4 << 20, 16 << 20} {
		for _, seed := range []uint32{17, 53, 101} {
			t.Run(fmt.Sprintf("bytes_%d/seed_%d", budget, seed), func(t *testing.T) {
				runCommitmentAdmissionTrace(t, budget, seed, "paired_rmw_reuse")
			})
		}
	}
}

type commitmentAdmissionOp struct {
	kind byte // r read, w canonical flush, d canonical delete, c cache reset
	id   uint32
	ns   byte // 0 depth 6, 1 depth 7 delta, 2 other, 3 fixed trunk
}

type commitmentAdmissionPhase struct {
	name string
	ops  []commitmentAdmissionOp
}

type commitmentAdmissionValue struct {
	key      []byte
	value    []byte
	revision uint32
	deleted  bool
}

type commitmentAdmissionResult struct {
	Scenario       string    `json:"scenario"`
	Phase          string    `json:"phase"`
	Seed           uint32    `json:"seed"`
	Budget         int       `json:"budget_bytes"`
	Reads          uint64    `json:"reads"`
	Durable        uint64    `json:"foreground_durable_gets"`
	Hits           uint64    `json:"resident_hits"`
	Writes         uint64    `json:"flushes"`
	Deletes        uint64    `json:"deletes"`
	ClassReads     [5]uint64 `json:"class_reads_depth6_depth7_other_trunk_depth5"`
	ClassDurable   [5]uint64 `json:"class_durable_gets_depth6_depth7_other_trunk_depth5"`
	ClassBytes     [5]uint64 `json:"class_durable_bytes_depth6_depth7_other_trunk_depth5"`
	PrefetchGets   uint64    `json:"prefetch_durable_gets"`
	PrefetchHits   uint64    `json:"prefetch_resident_hits"`
	Used           int       `json:"retained_charge_bytes"`
	FreeValueBytes int       `json:"free_value_bytes"`
	WindowBytes    int       `json:"window_bytes"`
	WindowEntries  int       `json:"window_entries"`
	TailEntries    int       `json:"tail_entries"`
	ShiftCounts    [7]int    `json:"window_admission_shift_shards"`
	FlushPromoted  uint64    `json:"window_flush_only_promoted"`
	AllPromoted    uint64    `json:"window_promoted"`
	ResultHash     string    `json:"returned_values_sha256"`
	TraceHash      string    `json:"operation_stream_sha256"`
}

func commitmentAdmissionTrace(budget int, seed uint32, scenario string) []commitmentAdmissionPhase {
	// A hot set is approximately one quarter of the payload capacity; the scan
	// is eight times the configured budget. Both scale with bytes, not entry
	// count alone. Value sizes later vary independently from 32 to 1024 bytes.
	hot := uint32(budget / 1536)
	scan := uint32(budget / 48)
	phases := []commitmentAdmissionPhase{{name: "cold_start"}, {name: "pressure"}, {name: "late_reuse"}}
	add := func(phase int, kind byte, id uint32, ns byte) {
		phases[phase].ops = append(phases[phase].ops, commitmentAdmissionOp{kind: kind, id: id, ns: ns})
	}
	namespace := func(id uint32) byte { return byte(((id*2654435761 + seed) >> 17) & 1) }
	// Four ordinary reads warm the same initial hot set for every policy. This
	// includes actual admission misses rather than force-installing hot rows.
	for round := uint32(0); round < 4; round++ {
		for i := uint32(0); i < hot; i++ {
			add(0, 'r', (i*997+seed)%hot, namespace((i*997+seed)%hot))
		}
	}
	for i := uint32(0); i < scan; i++ {
		id := hot + i
		ns := namespace(id)
		switch scenario {
		case "short_rmw_reuse":
			id = hot + (i % (hot * 2))
			ns = namespace(id)
		case "long_rmw_reuse":
			id = hot + (i % (hot * 8))
			ns = namespace(id)
		case "mixed_namespaces":
			if i%5 == 0 {
				ns = 2
			} else if i%17 == 0 {
				ns, id = 3, i%512
			}
		}
		add(1, 'r', id, ns)
		if scenario != "read_scan" {
			add(1, 'w', id, ns)
		}
		if i%8 == 0 {
			hotID := (i*3571 + seed) % hot
			if scenario == "hot_migration" && i >= scan/2 {
				hotID += hot + scan
			}
			add(1, 'r', hotID, namespace(hotID))
		}
		// Exercise value replacement, confirmed missing rows, re-creation, and
		// untouched write-only metadata without granting extra read evidence.
		if scenario == "mixed_namespaces" && i%257 == 0 {
			add(1, 'd', id, ns)
			add(1, 'r', id, ns)
			add(1, 'w', id, ns)
			add(1, 'w', hot+scan+i, 2)
		}
		if scenario == "mixed_namespaces" && i%19 == 0 {
			add(1, 'p', i%4096, 4)
			if i%3 != 0 {
				add(1, 'r', i%4096, 4)
			}
		}
		if scenario == "paired_rmw_reuse" {
			cohort := uint32(budget / 4096)
			if (i+1)%cohort == 0 {
				for j := i + 1 - cohort; j <= i; j++ {
					paired := hot + j
					add(1, 'r', paired, namespace(paired))
				}
			}
		}
	}
	// Delayed reads deliberately favour retaining read-then-write rows; this
	// adverse phase can expose a candidate that protects hot scans by throwing
	// away useful long-distance write reuse. Reverse order spans cache ages.
	for i := uint32(0); i < hot*8; i++ {
		id := hot + hot*8 - 1 - i
		add(2, 'r', id, namespace(id))
	}
	for i := uint32(0); i < hot; i++ {
		id := i
		if scenario == "hot_migration" {
			id += hot + scan
		}
		add(2, 'r', id, namespace(id))
	}
	return phases
}

func runCommitmentAdmissionTrace(t *testing.T, budget int, seed uint32, scenario string) {
	t.Helper()
	c := newBaseReadCacheWithTrunk(budget, baseReadCacheTrunkDepth, rawdb.CommitmentBranchKeyPrefix)
	durable := make(map[uint64]*commitmentAdmissionValue)
	lookup := func(op commitmentAdmissionOp) *commitmentAdmissionValue {
		identity := uint64(op.ns)<<32 | uint64(op.id)
		v := durable[identity]
		if v == nil {
			v = &commitmentAdmissionValue{key: commitmentAdmissionKey(op.id, op.ns, seed), revision: 1}
			v.value = commitmentAdmissionBytes(op.id, seed, v.revision)
			durable[identity] = v
		}
		return v
	}
	for _, phase := range commitmentAdmissionTrace(budget, seed, scenario) {
		result := commitmentAdmissionResult{Scenario: scenario, Phase: phase.name, Seed: seed, Budget: budget}
		digest := sha256.New()
		traceDigest := sha256.New()
		for _, op := range phase.ops {
			var event [6]byte
			event[0], event[1] = op.kind, op.ns
			binary.BigEndian.PutUint32(event[2:], op.id)
			traceDigest.Write(event[:])
			v := lookup(op)
			switch op.kind {
			case 'r':
				result.Reads++
				result.ClassReads[op.ns]++
				cached, present, _, _, epoch, cacheable, err := c.viewAtVersion(v.key, c.version.Load(), func(got []byte, _ bool) error {
					if v.deleted || !bytes.Equal(got, v.value) {
						return fmt.Errorf("incorrect resident value for %x revision %d", v.key, v.revision)
					}
					return nil
				})
				if err != nil || cached && present == v.deleted {
					t.Fatalf("read failed: cached=%v present=%v deleted=%v err=%v", cached, present, v.deleted, err)
				}
				if cached {
					result.Hits++
				} else {
					result.Durable++
					result.ClassDurable[op.ns]++
					if !v.deleted {
						result.ClassBytes[op.ns] += uint64(len(v.value))
					}
					if !cacheable {
						t.Fatal("single-generation fixture produced a non-cacheable durable read")
					}
					if v.deleted {
						c.setMissingIfEpoch(v.key, epoch)
					} else {
						c.storeIfEpoch(v.key, v.value, epoch)
					}
				}
				digest.Write(v.key)
				if v.deleted {
					digest.Write([]byte{0})
				} else {
					digest.Write([]byte{1})
					digest.Write(v.value)
				}
			case 'p':
				cached, present, epoch, cacheable := c.probeAtVersionForPrefetch(v.key, c.version.Load())
				if cached {
					if present == v.deleted {
						t.Fatal("prefetch returned incorrect present state")
					}
					result.PrefetchHits++
				} else {
					result.PrefetchGets++
					if !cacheable {
						t.Fatal("single-generation prefetch unexpectedly uncacheable")
					}
					if v.deleted {
						c.prefetchMissingIfEpoch(v.key, epoch)
					} else {
						c.prefetchIfEpoch(v.key, v.value, epoch)
					}
				}
			case 'w':
				result.Writes++
				v.revision++
				v.deleted = false
				v.value = commitmentAdmissionBytes(op.id, seed, v.revision)
				c.advanceVersion()
				c.setFlushed(string(v.key), v.value)
			case 'd':
				result.Deletes++
				v.deleted = true
				c.advanceVersion()
				c.del(v.key)
			default:
				t.Fatalf("unknown operation %q", op.kind)
			}
			shard := &c.shards[baseReadCacheShardIndex(v.key)]
			if shard.used > shard.limit || shard.windowUsed > shard.windowLimit || shard.trunkUsed > shard.trunkLimit || shard.freeValueBytes > shard.limit/baseReadCacheFreeValueBudgetDivisor {
				t.Fatalf("byte budget violated: used %d/%d window %d/%d trunk %d/%d free %d", shard.used, shard.limit, shard.windowUsed, shard.windowLimit, shard.trunkUsed, shard.trunkLimit, shard.freeValueBytes)
			}
		}
		for i := range c.shards {
			s := &c.shards[i]
			used, window := 0, 0
			for key, entry := range s.entries {
				if key != entry.key || !entry.live {
					t.Fatal("resident map and stable entry disagree")
				}
				used += entry.charge
				if entry.window {
					window += entry.charge
				} else if !entry.trunk && !entry.nonCommitment {
					result.TailEntries++
				}
			}
			if used != s.used || window != s.windowUsed {
				t.Fatal("resident accounting differs from physical entries")
			}
			result.Used += s.used
			result.FreeValueBytes += s.freeValueBytes
			result.WindowBytes += s.windowUsed
			result.WindowEntries += s.windowEntries
			result.ShiftCounts[s.windowAdmissionShift]++
			for _, depth := range s.diagnostics {
				result.FlushPromoted += depth.outcomes[baseReadCacheDiagnosticWindowPromoted][4]
				for _, count := range depth.outcomes[baseReadCacheDiagnosticWindowPromoted] {
					result.AllPromoted += count
				}
			}
		}
		if result.Used > budget {
			t.Fatalf("total retained charge %d exceeds %d", result.Used, budget)
		}
		result.ResultHash = hex.EncodeToString(digest.Sum(nil))
		result.TraceHash = hex.EncodeToString(traceDigest.Sum(nil))
		encoded, err := json.Marshal(result)
		if err != nil {
			t.Fatal(err)
		}
		t.Logf("ADMISSION_RESULT %s", encoded)
	}
}

func commitmentAdmissionKey(id uint32, ns byte, seed uint32) []byte {
	if ns == 2 {
		key := make([]byte, len("fixture-latest-")+4)
		copy(key, "fixture-latest-")
		binary.BigEndian.PutUint32(key[len(key)-4:], id^seed)
		return key
	}
	depth := 6
	prefix := []byte(rawdb.CommitmentBranchKeyPrefix)
	switch ns {
	case 1:
		depth = 7
		prefix = make([]byte, len(rawdb.CommitmentBranchDeltaKeyPrefix)+8)
		copy(prefix, rawdb.CommitmentBranchDeltaKeyPrefix)
		binary.BigEndian.PutUint64(prefix[len(prefix)-8:], uint64(seed))
	case 3:
		depth = 4
	case 4:
		depth = 5
	}
	key := make([]byte, len(prefix)+depth)
	copy(key, prefix)
	// Odd multiplication is a permutation modulo 16^depth. Real nibble paths
	// avoid fake long text keys accidentally choosing a different depth policy.
	mask := uint32(1<<(depth*4)) - 1
	path := (id + seed) & mask
	path ^= path >> 7
	path = (path * 0x9e3779b1) & mask
	path ^= path >> 11
	path = (path * 0x85ebca6b) & mask
	for i := depth - 1; i >= 0; i-- {
		key[len(prefix)+i] = byte(path & 15)
		path >>= 4
	}
	return key
}

func commitmentAdmissionBytes(id, seed, revision uint32) []byte {
	sizes := [...]int{32, 64, 128, 256, 512, 1024}
	mixed := id*2654435761 + seed
	mixed ^= mixed >> 17
	value := make([]byte, sizes[(mixed+revision/4)%uint32(len(sizes))])
	if id%101 == 0 {
		value = []byte{}
	}
	for i := range value {
		value[i] = byte((id*17 + seed + revision*41 + uint32(i)) & 255)
	}
	return value
}
