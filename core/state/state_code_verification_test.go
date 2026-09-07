package state

import (
	"bytes"
	"fmt"
	"sync"
	"testing"

	ethrawdb "github.com/ethereum/go-ethereum/core/rawdb"
	tcommon "github.com/tronprotocol/go-tron/common"
)

func TestStrictObjectCodeVerificationRejectsMutation(t *testing.T) {
	for _, budget := range []int{0, 256, 1 << 20} {
		t.Run(fmt.Sprint(budget), func(t *testing.T) {
			db := NewDatabaseWithConfig(ethrawdb.NewMemoryDatabase(), DatabaseConfig{CodeCacheSizeBytes: budget})
			t.Cleanup(func() { _ = db.Close() })
			code, hash := cacheTestCode(0x83, 1024)
			addr := tcommon.Address{0x41, 0x83}
			s := stateDBWithCodeHash(db, addr, hash)
			s.stateObjects[addr].code = bytes.Clone(code)
			got, err := s.GetCodeStrict(addr)
			if err != nil || !bytes.Equal(got, code) {
				t.Fatalf("initial verification: %v", err)
			}
			// Mutate the actual returned slice after it was verified. Neither a
			// cached hash nor a verified-once flag is sufficient here.
			for i := range got {
				got[i] ^= 1
				if result, err := s.GetCodeStrict(addr); err == nil || result != nil {
					t.Fatalf("accepted mutation at byte %d", i)
				}
				got[i] ^= 1
			}
			for _, changed := range [][]byte{got[:len(got)-1], append(bytes.Clone(got), 0), {}} {
				s.stateObjects[addr].code = changed
				if result, err := s.GetCodeStrict(addr); err == nil || result != nil {
					t.Fatalf("accepted length %d", len(changed))
				}
			}
			s.stateObjects[addr].code = got
			if result, err := s.GetCodeStrict(addr); err != nil || !bytes.Equal(result, code) {
				t.Fatalf("restored code: %v", err)
			}
			if budget == 1<<20 {
				other := stateDBWithCodeHash(db, addr, hash)
				result, err := other.GetCodeStrict(addr)
				if err != nil || !bytes.Equal(result, code) {
					t.Fatalf("object mutation poisoned shared cache: %v", err)
				}
			}
		})
	}
}

func TestStrictObjectCodeVerificationCopyAndRevert(t *testing.T) {
	db := NewDatabase(ethrawdb.NewMemoryDatabase())
	t.Cleanup(func() { _ = db.Close() })
	codeA, hashA := cacheTestCode(0xa1, 4096)
	codeB, hashB := cacheTestCode(0xb1, 4096)
	addr := tcommon.Address{0x41, 0x91}
	s := stateDBWithCodeHash(db, addr, hashA)
	s.journal = newJournal()
	s.stateObjects[addr].code = bytes.Clone(codeA)
	check := func(st *StateDB, want []byte, hash tcommon.Hash) {
		t.Helper()
		got, err := st.GetCodeStrict(addr)
		if err != nil || !bytes.Equal(got, want) || tcommon.Keccak256(got) != hash {
			t.Fatalf("strict code mismatch: %v", err)
		}
	}
	check(s, codeA, hashA)
	copyState, err := s.CopyBlockExecutionBase()
	if err != nil {
		t.Fatal(err)
	}
	revision := s.Snapshot()
	s.SetCode(addr, codeB)
	check(s, codeB, hashB)
	check(copyState, codeA, hashA)
	s.RevertToSnapshot(revision)
	check(s, codeA, hashA)
	// Removing both positive cache entries restores the full verification path.
	db.codeCache.close()
	check(s, codeA, hashA)
	s.stateObjects[addr].code[0] ^= 1
	if _, err := s.GetCodeStrict(addr); err == nil {
		t.Fatal("closed cache bypassed corruption check")
	}
}

func TestCodeVerificationCacheConcurrentEvictionAndClose(t *testing.T) {
	cache := newStateCodeCache(4096)
	t.Cleanup(cache.close)
	var wg sync.WaitGroup
	for worker := range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range 150 {
				code, hash := cacheTestCode(byte(worker+i), 1024)
				cache.admit(hash, code)
				cache.matches(hash, code)
				code[i%len(code)] ^= 1
				if cache.matches(hash, code) {
					t.Error("matched corrupt code during eviction")
				}
				if i == 100 {
					cache.close()
				}
			}
		}()
	}
	wg.Wait()
}

func BenchmarkStrictObjectCodeVerification(b *testing.B) {
	for _, size := range []int{256, 4 << 10, 32 << 10} {
		b.Run(fmt.Sprint(size), func(b *testing.B) {
			db := NewDatabase(ethrawdb.NewMemoryDatabase())
			b.Cleanup(func() { _ = db.Close() })
			code, hash := cacheTestCode(0x84, size)
			addr := tcommon.Address{0x41, 0x84}
			s := stateDBWithCodeHash(db, addr, hash)
			s.stateObjects[addr].code = bytes.Clone(code)
			if _, err := s.GetCodeStrict(addr); err != nil {
				b.Fatal(err)
			}
			b.SetBytes(int64(size))
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				var err error
				stateCodeBenchmarkSink, err = s.GetCodeStrict(addr)
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
