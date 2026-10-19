package rawdb

import (
	"encoding/binary"
	"testing"

	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/state/kvdomains"
)

// This source also compiles against the predecessor codec for actual API
// performance comparisons, without manually approximating its decoder.
func BenchmarkStateLatestUint64Read(b *testing.B) {
	value := binary.BigEndian.AppendUint64(nil, 42)
	encoded := EncodeStateKVLatestValue(value)
	b.Run("decode", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			v, err := DecodeStateKVLatestValue(encoded)
			if err != nil || len(v) != 8 {
				b.Fatal(err)
			}
		}
		b.ReportMetric(float64(len(encoded)), "stored-bytes")
	})
	db := NewMemoryDatabase()
	defer db.Close()
	owner := common.Address{0x41, 7}
	if err := WriteStateKVLatest(db, owner, 0, kvdomains.AccountAssetV2, []byte("1000001"), value); err != nil {
		b.Fatal(err)
	}
	b.Run("point", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			v, ok, err := ReadStateKVLatestNoCopy(db, owner, 0, kvdomains.AccountAssetV2, []byte("1000001"))
			if err != nil || !ok || len(v) != 8 {
				b.Fatal(ok, err)
			}
		}
		b.ReportMetric(float64(len(encoded)), "stored-bytes")
	})
}
