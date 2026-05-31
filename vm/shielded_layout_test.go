package vm

import "testing"

// TestShieldedTransferLayoutDispatch pins both verifyTransferProof input
// layouts. java-tron commit d2ab1c7304 (2020-06-16) added a 32-byte
// valueBalance word; contracts compiled before/after emit disjoint size sets,
// and replaying historical Nile/mainnet blocks (e.g. block 6,498,505) requires
// accepting the legacy form. Regression guard for the consensus fix.
func TestShieldedTransferLayoutDispatch(t *testing.T) {
	// Current layout (>= d2ab1c7304): valueBalance present, frontier@224, leafCount@1280.
	for _, n := range []int{2080, 2368, 2464, 2752} {
		fo, lo, hv, ok := shieldedTransferLayout(n)
		if !ok || fo != 224 || lo != 1280 || !hv {
			t.Errorf("len %d: got (frontier=%d leafCount=%d valueBalance=%v ok=%v), want (224 1280 true true)", n, fo, lo, hv, ok)
		}
	}
	// Legacy layout (< d2ab1c7304): no valueBalance, frontier@192, leafCount@1248.
	for _, n := range []int{2048, 2336, 2432, 2720} {
		fo, lo, hv, ok := shieldedTransferLayout(n)
		if !ok || fo != 192 || lo != 1248 || hv {
			t.Errorf("len %d: got (frontier=%d leafCount=%d valueBalance=%v ok=%v), want (192 1248 false true)", n, fo, lo, hv, ok)
		}
	}
	// Everything else is rejected.
	for _, n := range []int{0, 32, 2000, 2047, 2049, 2079, 2081, 2752 + 32, 3000} {
		if _, _, _, ok := shieldedTransferLayout(n); ok {
			t.Errorf("len %d: expected rejection, got ok=true", n)
		}
	}
}
