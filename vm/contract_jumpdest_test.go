package vm

import (
	"bytes"
	"math/rand"
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

func TestAnalyzeJumpdests_BitvecMatchesReference(t *testing.T) {
	cases := []struct {
		name string
		code []byte
	}{
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
	for _, tc := range cases {
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
		code := make([]byte, r.Intn(300))
		for i := range code {
			// Bias toward JUMPDEST and PUSH opcodes so the data-skip paths and
			// 0x5b-inside-push-data cases get heavily exercised.
			switch r.Intn(3) {
			case 0:
				code[i] = byte(JUMPDEST)
			case 1:
				code[i] = byte(PUSH1) + byte(r.Intn(32)) // PUSH1..PUSH32
			default:
				code[i] = byte(r.Intn(256))
			}
		}
		assertJumpdestEquiv(t, "fuzz", code)
	}
}
