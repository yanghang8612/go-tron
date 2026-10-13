//go:build (amd64 || arm64) && !purego

package domains

import (
	"runtime"

	fastkeccak "github.com/erigontech/fastkeccak"
	"github.com/tronprotocol/go-tron/common"
	"golang.org/x/sys/cpu"
)

// Match fastkeccak runtime dispatch; a native build can still lack acceleration.
var branchOneShotReady = (runtime.GOARCH == "amd64" && cpu.X86.HasBMI1 && cpu.X86.HasBMI2) ||
	(runtime.GOARCH == "arm64" && (runtime.GOOS == "darwin" || runtime.GOOS == "ios" || cpu.ARM64.HasSHA3))

func branchPreimageHash(h *pooledKeccak, preimage []byte) common.Hash {
	if !branchOneShotReady {
		h.Reset()
		_, _ = h.Write(preimage)
		return readKeccakHash(h)
	}
	return common.Hash(fastkeccak.Sum256(preimage))
}
