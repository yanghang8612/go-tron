package domains

import (
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"testing"

	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/common/muhash3072"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/state/kvdomains"
)

func BenchmarkMuHashLegalDeltaChain(b *testing.B) {
	for _, n := range []int{1, 64, 256} {
		b.Run(fmt.Sprint(n), func(b *testing.B) {
			db := rawdb.NewMemoryDatabase()
			defer db.Close()
			if err := rawdb.InitializeStateCommitment(db); err != nil {
				b.Fatal(err)
			}
			var owner common.Address
			owner[0] = 0x41
			owner[20] = 1
			initial := make([]rawdb.StateCommitmentDelta, n)
			changes := [2][]rawdb.StateCommitmentDelta{make([]rawdb.StateCommitmentDelta, n), make([]rawdb.StateCommitmentDelta, n)}
			for i := range initial {
				var seed [9]byte
				binary.BigEndian.PutUint64(seed[:8], uint64(i))
				slot := sha256.Sum256(seed[:])
				seed[8] = 1
				a := sha256.Sum256(seed[:])
				seed[8] = 2
				c := sha256.Sum256(seed[:])
				key := rawdb.StateKVLatestCommitmentKey(owner, 1, kvdomains.ContractStorage, slot[:])
				va, vc := rawdb.EncodeStateKVLatestValue(a[:]), rawdb.EncodeStateKVLatestValue(c[:])
				initial[i] = rawdb.StateCommitmentDelta{Key: key, AfterExists: true, After: va}
				changes[0][i] = rawdb.StateCommitmentDelta{Key: key, BeforeExists: true, Before: va, AfterExists: true, After: vc}
				changes[1][i] = rawdb.StateCommitmentDelta{Key: key, BeforeExists: true, Before: vc, AfterExists: true, After: va}
			}
			hash := func(height uint64) (h common.Hash) { binary.BigEndian.PutUint64(h[24:], height); return }
			parent := hash(1)
			if _, e := ApplyMuHashDeltas(db, initial, CommitmentIdentity{BlockNum: 1, BlockHash: parent}, common.Hash{}); e != nil {
				b.Fatal(e)
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				height := uint64(i + 2)
				next := hash(height)
				if _, e := ApplyMuHashDeltas(db, changes[i&1], CommitmentIdentity{BlockNum: height, BlockHash: next, ParentHash: parent, TxNum: height}, common.Hash{}); e != nil {
					b.Fatal(e)
				}
				parent = next
			}
			b.StopTimer()
			// Independently rebuild the complete physical set after the real block
			// identity/delta chain, never compare unlike old/new commitment schemes.
			acc := muhash3072.NewAccumulator()
			if e := rawdb.IterateLatestDomainCommitmentSources(db, func(k, v []byte) (bool, error) {
				el, e := rawdb.EncodeStateCommitmentElement(k, v)
				if e == nil {
					e = acc.Insert(el)
				}
				return e == nil, e
			}); e != nil {
				b.Fatal(e)
			}
			final, e := acc.Finalize()
			if e != nil {
				b.Fatal(e)
			}
			head, ok, e := rawdb.ReadStateCommitmentHead(db)
			if e != nil || !ok || head.State != final.State || head.Root != common.Hash(final.Digest) {
				b.Fatal(e, "full set mismatch")
			}
		})
	}
}
