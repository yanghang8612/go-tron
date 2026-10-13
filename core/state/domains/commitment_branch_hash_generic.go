//go:build (!amd64 && !arm64) || purego

package domains

import "github.com/tronprotocol/go-tron/common"

// The library's generic one-shot helper allocates a fresh sponge and Sum output.
// Retain the existing reusable streaming path on fallback platforms.
func branchPreimageHash(h *pooledKeccak, preimage []byte) common.Hash {
	h.Reset()
	_, _ = h.Write(preimage)
	return readKeccakHash(h)
}
