package muhash3072

import (
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"testing"
)

func BenchmarkAccumulatorBlockUpdates(b *testing.B) {
	for _, n := range []int{0, 1, 64, 256} {
		b.Run(fmt.Sprint(n), func(b *testing.B) {
			before, after := make([][32]byte, n), make([][32]byte, n)
			old, next := NewAccumulator(), NewAccumulator()
			for i := range before {
				var seed [9]byte
				binary.BigEndian.PutUint64(seed[:8], uint64(i))
				before[i] = sha256.Sum256(seed[:])
				seed[8] = 1
				after[i] = sha256.Sum256(seed[:])
				if err := old.Insert(before[i][:]); err != nil {
					b.Fatal(err)
				}
				if err := next.Insert(after[i][:]); err != nil {
					b.Fatal(err)
				}
			}
			base, err := old.Finalize()
			if err != nil {
				b.Fatal(err)
			}
			want, err := next.Finalize()
			if err != nil {
				b.Fatal(err)
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				a, e := FromState(base.State)
				if e != nil {
					b.Fatal(e)
				}
				a, e = a.Clone()
				if e != nil {
					b.Fatal(e)
				}
				for j := range before {
					if e := a.Remove(before[j][:]); e != nil {
						b.Fatal(e)
					}
					if e := a.Insert(after[j][:]); e != nil {
						b.Fatal(e)
					}
				}
				got, e := a.Finalize()
				if e != nil || got != want {
					b.Fatal(e, "state mismatch")
				}
			}
		})
	}
}
