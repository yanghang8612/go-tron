package domains

import (
	"fmt"
	"testing"

	"github.com/tronprotocol/go-tron/common"
)

var benchmarkOct13BranchHash common.Hash

// Compatible with ea3cb83b, including the same reusable hasher and preimage.
func BenchmarkBranchNodeHashOct13(b *testing.B) {
	for _, count := range []int{1, 4, 16} {
		b.Run(fmt.Sprint(count), func(b *testing.B) {
			var branch BranchData
			for n := 0; n < count; n++ {
				branch.SetHashChild(uint8(n), common.Hash{byte(n + 1)})
			}
			h := borrowKeccak()
			defer returnKeccak(h)
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				benchmarkOct13BranchHash = branch.nodeHashWith(h)
			}
		})
	}
}
