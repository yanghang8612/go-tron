package vm

import (
	"bytes"
	"math/rand"
	"sync"
	"testing"

	tcommon "github.com/tronprotocol/go-tron/common"
)

// analyzeJumpdestsRef is the pre-bitvec map implementation, kept verbatim as the
// reference oracle. The bitvec rewrite is consensus-critical: JUMP/JUMPI success
// depends on the valid-JUMPDEST set, so the new representation must yield a
// byte-for-byte identical set of valid positions for every bytecode.
func analyzeJumpdestsRef(code []byte) map[uint64]bool {
	dests := make(map[uint64]bool)
	for i := 0; i < len(code); i++ {
		op := OpCode(code[i])
		if op == JUMPDEST {
			dests[uint64(i)] = true
		} else if op >= PUSH1 && op <= PUSH32 {
			i += int(op - PUSH1 + 1)
		}
	}
	return dests
}

// assertJumpdestEquiv probes every in-range position plus a margin past the end
// (out-of-range must be rejected; a trailing PUSH whose data overruns the code
// must not leak a valid dest) and asserts IsValidJumpdest matches the reference.
func assertJumpdestEquiv(t *testing.T, name string, code []byte) {
	t.Helper()
	ref := analyzeJumpdestsRef(code)
	c := &Contract{}
	c.SetCode(tcommon.Address{}, code)
	for pos := 0; pos <= len(code)+40; pos++ {
		want := ref[uint64(pos)]
		got := c.IsValidJumpdest(uint64(pos))
		if want != got {
			t.Fatalf("%s: pos=%d code=%x: IsValidJumpdest=%v want=%v", name, pos, code, got, want)
		}
	}
}

// assertBitvecMatchesRef asserts a raw bitvec marks exactly the reference set of
// valid JUMPDEST positions over the probed range (in-range plus a past-end
// margin, where everything must be rejected).
func assertBitvecMatchesRef(t *testing.T, name string, code []byte, bv bitvec) {
	t.Helper()
	ref := analyzeJumpdestsRef(code)
	for pos := 0; pos <= len(code)+40; pos++ {
		want := ref[uint64(pos)]
		got := pos < len(code) && bv.isSet(uint64(pos))
		if want != got {
			t.Fatalf("%s: pos=%d code=%x: bitvec isSet=%v want=%v", name, pos, code, got, want)
		}
	}
}

type jumpdestCase struct {
	name string
	code []byte
}

// jumpdestGoldenCorpus is the hand-picked set of tricky bytecodes (PUSH data
// hiding 0x5b, truncated/overrunning PUSH, consecutive JUMPDESTs) shared by the
// reference-equivalence and the cache-equivalence tests.
func jumpdestGoldenCorpus() []jumpdestCase {
	return []jumpdestCase{
		{"empty", nil},
		{"single-jumpdest", []byte{0x5b}},
		{"jumpdest-first-last", []byte{0x5b, 0x00, 0x5b}},
		{"push1-hides-5b", []byte{0x60, 0x5b}}, // 0x5b is PUSH1 data
		{"push32-hides-32x5b", append([]byte{0x7f}, bytes.Repeat([]byte{0x5b}, 32)...)},
		{"jd-push1-5b-jd", []byte{0x5b, 0x60, 0x5b, 0x5b}}, // pos2 hidden; pos0,pos3 valid
		{"truncated-push1", []byte{0x60}},                  // PUSH1 with no data
		{"truncated-push32", []byte{0x7f}},                 // PUSH32 with no data
		{"push32-overrun-5b", append([]byte{0x00, 0x00, 0x7f}, bytes.Repeat([]byte{0x5b}, 5)...)},
		{"consecutive-jd", []byte{0x5b, 0x5b, 0x5b}},
		{"push2-boundary", []byte{0x5b, 0x61, 0x5b, 0x5b, 0x5b}}, // PUSH2 at 1 hides 2,3; pos0,pos4 valid
	}
}

// randomJumpdestCode generates bytecode biased toward JUMPDEST and PUSH opcodes so
// the data-skip paths and 0x5b-inside-push-data cases get heavily exercised.
func randomJumpdestCode(r *rand.Rand) []byte {
	code := make([]byte, r.Intn(300))
	for i := range code {
		switch r.Intn(3) {
		case 0:
			code[i] = byte(JUMPDEST)
		case 1:
			code[i] = byte(PUSH1) + byte(r.Intn(32)) // PUSH1..PUSH32
		default:
			code[i] = byte(r.Intn(256))
		}
	}
	return code
}

func TestAnalyzeJumpdests_BitvecMatchesReference(t *testing.T) {
	for _, tc := range jumpdestGoldenCorpus() {
		assertJumpdestEquiv(t, tc.name, tc.code)
	}
}

// benchJumpdestCode is ~8 KB of mixed opcodes with realistic JUMPDEST/PUSH density.
func benchJumpdestCode() []byte {
	r := rand.New(rand.NewSource(2))
	code := make([]byte, 8192)
	for i := range code {
		switch r.Intn(4) {
		case 0:
			code[i] = byte(JUMPDEST)
		case 1:
			code[i] = byte(PUSH1) + byte(r.Intn(32))
		default:
			code[i] = byte(r.Intn(256))
		}
	}
	return code
}

// BenchmarkAnalyzeJumpdestsMap / ...Bitvec isolate the per-contract-load
// allocation the bitvec rewrite targets (analyzeJumpdests was 6.25% of all
// alloc in the sync profile).
func BenchmarkAnalyzeJumpdestsMap(b *testing.B) {
	code := benchJumpdestCode()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = analyzeJumpdestsRef(code)
	}
}

func BenchmarkAnalyzeJumpdestsBitvec(b *testing.B) {
	code := benchJumpdestCode()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = analyzeJumpdests(code)
	}
}

func TestAnalyzeJumpdests_BitvecFuzz(t *testing.T) {
	r := rand.New(rand.NewSource(1))
	for iter := 0; iter < 5000; iter++ {
		assertJumpdestEquiv(t, "fuzz", randomJumpdestCode(r))
	}
}

// ---------------------------------------------------------------------------
// JUMPDEST analysis cache (perf). Identical contract code — a proxy's
// implementation, a CREATE2 redeploy, a hot contract called across many
// transactions in a block — is analyzed once and the bitvec reused. The reused
// set is consensus-critical, so a cache hit MUST equal a fresh analysis
// byte-for-byte; the cache must be bounded and safe for concurrent use.
// ---------------------------------------------------------------------------

func codeHashOf(code []byte) tcommon.Hash { return tcommon.Keccak256(code) }

// TestJumpdestCache_HitEqualsFreshAnalysis runs the golden corpus through a fresh
// cache twice (miss then hit) and asserts both the miss and the hit are
// byte-identical to a direct analysis, and that a non-empty code is actually
// cached and its hit reuses the stored bitvec (not a recomputation).
func TestJumpdestCache_HitEqualsFreshAnalysis(t *testing.T) {
	for _, tc := range jumpdestGoldenCorpus() {
		jc := newJumpdestCache(64)
		h := codeHashOf(tc.code)
		fresh := analyzeJumpdests(tc.code)

		miss := jc.analyze(h, tc.code)
		hit := jc.analyze(h, tc.code)

		if !bytes.Equal(miss, fresh) {
			t.Fatalf("%s: cache miss bitvec %x != fresh %x", tc.name, []byte(miss), []byte(fresh))
		}
		if !bytes.Equal(hit, fresh) {
			t.Fatalf("%s: cache hit bitvec %x != fresh %x", tc.name, []byte(hit), []byte(fresh))
		}
		assertBitvecMatchesRef(t, tc.name, tc.code, hit)

		if len(tc.code) > 0 {
			if jc.len() != 1 {
				t.Fatalf("%s: want exactly 1 cached entry, got %d", tc.name, jc.len())
			}
			// The hit must hand back the very slice stored on the miss, proving
			// reuse rather than a fresh re-analysis on every call.
			if &miss[0] != &hit[0] {
				t.Fatalf("%s: cache hit did not reuse the stored bitvec", tc.name)
			}
		}
	}
}

// TestJumpdestCache_FuzzHitEqualsFresh drives the 5000-case fuzz corpus through
// the cache and asserts every miss and hit equals a fresh analysis.
func TestJumpdestCache_FuzzHitEqualsFresh(t *testing.T) {
	r := rand.New(rand.NewSource(1))
	jc := newJumpdestCache(512)
	for iter := 0; iter < 5000; iter++ {
		code := randomJumpdestCode(r)
		h := codeHashOf(code)
		fresh := analyzeJumpdests(code)

		miss := jc.analyze(h, code)
		hit := jc.analyze(h, code)

		if !bytes.Equal(miss, fresh) || !bytes.Equal(hit, fresh) {
			t.Fatalf("iter=%d code=%x: cached bitvec != fresh analysis", iter, code)
		}
		assertBitvecMatchesRef(t, "fuzz", code, hit)
	}
}

// TestJumpdestCache_ZeroHashAndEmptyNotCached pins the fallback contract: an
// unknown code identity (zero hash, e.g. initcode not yet in state) or empty code
// is analyzed directly and never inserted, so it can never serve a wrong bitvec.
func TestJumpdestCache_ZeroHashAndEmptyNotCached(t *testing.T) {
	jc := newJumpdestCache(64)
	code := []byte{byte(JUMPDEST), byte(PUSH1), byte(JUMPDEST), byte(JUMPDEST)}

	bv := jc.analyze(tcommon.Hash{}, code)
	assertBitvecMatchesRef(t, "zero-hash", code, bv)
	if jc.len() != 0 {
		t.Fatalf("zero code hash must not be cached, got len=%d", jc.len())
	}

	bv = jc.analyze(codeHashOf(nil), nil)
	assertBitvecMatchesRef(t, "empty", nil, bv)
	if jc.len() != 0 {
		t.Fatalf("empty code must not be cached, got len=%d", jc.len())
	}
}

// TestJumpdestCache_BoundedEviction feeds far more distinct contracts than the cap
// and asserts the cache never exceeds its bound, and that an evicted entry
// recomputes to the identical bitvec.
func TestJumpdestCache_BoundedEviction(t *testing.T) {
	const capacity = 16
	jc := newJumpdestCache(capacity)
	r := rand.New(rand.NewSource(7))
	codes := make([][]byte, 256)
	for i := range codes {
		c := make([]byte, 1+r.Intn(64))
		for j := range c {
			c[j] = byte(r.Intn(256))
		}
		codes[i] = c
		jc.analyze(codeHashOf(c), c)
		if jc.len() > capacity {
			t.Fatalf("cache exceeded cap: len=%d cap=%d", jc.len(), capacity)
		}
	}
	for _, c := range codes {
		assertBitvecMatchesRef(t, "post-evict", c, jc.analyze(codeHashOf(c), c))
	}
}

// TestJumpdestCache_ConcurrentAccess hammers a shared cache from many goroutines
// over a small key set so they collide on Get/Add for the same hash. It must be
// run under -race; every result must equal the fresh analysis for that code.
func TestJumpdestCache_ConcurrentAccess(t *testing.T) {
	jc := newJumpdestCache(64)
	type entry struct {
		code  []byte
		hash  tcommon.Hash
		fresh bitvec
	}
	r := rand.New(rand.NewSource(3))
	entries := make([]entry, 32)
	for i := range entries {
		c := randomJumpdestCode(r)
		if len(c) == 0 {
			c = []byte{byte(JUMPDEST)}
		}
		entries[i] = entry{code: c, hash: codeHashOf(c), fresh: analyzeJumpdests(c)}
	}

	var wg sync.WaitGroup
	for g := 0; g < 16; g++ {
		wg.Add(1)
		go func(seed int) {
			defer wg.Done()
			rr := rand.New(rand.NewSource(int64(seed) + 100))
			for it := 0; it < 2000; it++ {
				e := entries[rr.Intn(len(entries))]
				bv := jc.analyze(e.hash, e.code)
				if !bytes.Equal(bv, e.fresh) {
					t.Errorf("concurrent analyze mismatch for code=%x", e.code)
					return
				}
			}
		}(g)
	}
	wg.Wait()
}

// TestContractSetCode_ReusesGlobalCacheByCodeHash drives the production wiring:
// when a Contract carries the StateDB code hash, SetCode consults the process-wide
// cache, so two frames running identical bytecode at different addresses (proxy /
// CREATE2 reuse) share the one analyzed bitvec. A frame with no known code hash
// (initcode) falls back to a correct direct analysis.
func TestContractSetCode_ReusesGlobalCacheByCodeHash(t *testing.T) {
	code := []byte{byte(JUMPDEST), byte(PUSH2), byte(JUMPDEST), byte(JUMPDEST), byte(JUMPDEST)}
	h := codeHashOf(code)

	c1 := &Contract{CodeHash: h}
	c1.SetCode(tcommon.Address{0x41, 0x01}, code)
	assertBitvecMatchesRef(t, "wired-c1", code, c1.jumpdests)

	c2 := &Contract{CodeHash: h}
	c2.SetCode(tcommon.Address{0x41, 0x02}, code) // different address, identical code hash
	assertBitvecMatchesRef(t, "wired-c2", code, c2.jumpdests)

	if &c1.jumpdests[0] != &c2.jumpdests[0] {
		t.Fatal("contracts with equal code hash did not share the cached jumpdest analysis")
	}

	c3 := &Contract{} // zero CodeHash: initcode / unknown identity
	c3.SetCode(tcommon.Address{0x41, 0x03}, code)
	assertBitvecMatchesRef(t, "wired-fallback", code, c3.jumpdests)
}
