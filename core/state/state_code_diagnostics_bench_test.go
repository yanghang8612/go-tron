package state

import (
	"testing"

	ethrawdb "github.com/ethereum/go-ethereum/core/rawdb"
	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
)

// Keep the object cache cold so these benchmarks include diagnostic context
// propagation through the successful hot/cold read and cache admission paths.
// The tiny admission caches deliberately cannot retain the code being read.
func BenchmarkStateCodeDiagnosticSuccess(b *testing.B) {
	for _, strict := range []bool{false, true} {
		name := "permissive"
		if strict {
			name = "strict"
		}
		for _, source := range []string{"cache_hit", "hot_read", "hot_admission", "cold_admission"} {
			b.Run(name+"/"+source, func(b *testing.B) {
				cacheBytes := 1
				switch source {
				case "cache_hit":
					cacheBytes = 1 << 20
				case "hot_read":
					cacheBytes = 0
				}
				disk := ethrawdb.NewMemoryDatabase()
				db := NewDatabaseWithConfig(disk, DatabaseConfig{CodeCacheSizeBytes: cacheBytes})
				b.Cleanup(func() { _ = db.Close() })
				code, hash := cacheTestCode(0x79, 4096)
				addr := tcommon.Address{0x41, 0x79}
				s := stateDBWithCodeHash(db, addr, hash)
				if source == "cold_admission" {
					s.SetCodeColdHistory(&countingColdCodeHistory{code: code}, 123)
				} else if err := rawdb.WriteStateCode(disk, hash, code); err != nil {
					b.Fatal(err)
				}
				if got, err := s.GetCodeStrict(addr); err != nil || len(got) != len(code) {
					b.Fatalf("warm read: bytes=%d err=%v", len(got), err)
				}
				obj := s.stateObjects[addr]
				b.SetBytes(int64(len(code)))
				b.ReportAllocs()
				b.ResetTimer()
				for b.Loop() {
					obj.code = nil
					if strict {
						var err error
						stateCodeBenchmarkSink, err = s.GetCodeStrict(addr)
						if err != nil {
							b.Fatal(err)
						}
					} else {
						stateCodeBenchmarkSink = s.GetCode(addr)
					}
				}
			})
		}
	}
}
