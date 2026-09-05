package state

import (
	"bytes"
	"errors"
	"fmt"
	"runtime"
	"strings"
	"sync"
	"testing"

	ethrawdb "github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/ethdb"
	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/types"
	corepb "github.com/tronprotocol/go-tron/proto/core"
)

func cacheTestCode(seed byte, size int) ([]byte, tcommon.Hash) {
	code := bytes.Repeat([]byte{seed}, size)
	return code, tcommon.Keccak256(code)
}

func TestStateCodeCacheHitReturnsOwnedBytes(t *testing.T) {
	cache := newStateCodeCache(4096)
	t.Cleanup(cache.close)
	code, hash := cacheTestCode(0x41, 256)
	if !cache.admit(hash, code) {
		t.Fatal("valid code was not admitted")
	}

	first, ok := cache.get(hash)
	if !ok || !bytes.Equal(first, code) {
		t.Fatalf("first hit = %x/%v, want %x/true", first, ok, code)
	}
	first[0] ^= 0xff
	second, ok := cache.get(hash)
	if !ok || !bytes.Equal(second, code) {
		t.Fatal("caller mutation poisoned the cached canonical bytes")
	}
	if &first[0] == &second[0] {
		t.Fatal("cache returned a mutable slice shared by callers")
	}
}

func TestStateCodeCacheBoundedLRUEviction(t *testing.T) {
	codeA, hashA := cacheTestCode(0x01, 128)
	codeB, hashB := cacheTestCode(0x02, 128)
	codeC, hashC := cacheTestCode(0x03, 128)
	entryCharge := int64(len(codeA)) + codeCacheEntryOverhead
	cache := newStateCodeCache(int(2 * entryCharge))
	t.Cleanup(cache.close)

	cache.admit(hashA, codeA)
	cache.admit(hashB, codeB)
	if _, ok := cache.get(hashA); !ok { // A becomes MRU; B is the victim.
		t.Fatal("expected A hit before eviction")
	}
	cache.admit(hashC, codeC)

	cache.mu.Lock()
	bytesUsed, entries := cache.bytes, len(cache.entries)
	_, hasA := cache.entries[hashA]
	_, hasB := cache.entries[hashB]
	_, hasC := cache.entries[hashC]
	cache.mu.Unlock()
	if bytesUsed > cache.maxBytes || entries != 2 {
		t.Fatalf("cache bounds = %d bytes/%d entries, max %d", bytesUsed, entries, cache.maxBytes)
	}
	if !hasA || hasB || !hasC {
		t.Fatalf("LRU contents A/B/C = %v/%v/%v, want true/false/true", hasA, hasB, hasC)
	}
}

func TestStateCodeCacheDoesNotCacheMissOrHashMismatch(t *testing.T) {
	cache := newStateCodeCache(4096)
	t.Cleanup(cache.close)
	code, hash := cacheTestCode(0x51, 64)
	if _, ok := cache.get(hash); ok {
		t.Fatal("unexpected hit for an unadmitted hash")
	}
	if cache.admit(hash, append([]byte(nil), code[:len(code)-1]...)) {
		t.Fatal("hash-mismatched code was admitted")
	}
	if _, ok := cache.get(hash); ok {
		t.Fatal("miss or rejected value created a cache entry")
	}
}

func TestStateCodeCacheConcurrentAccess(t *testing.T) {
	cache := newStateCodeCache(1 << 20)
	t.Cleanup(cache.close)
	const workers = 24
	var wg sync.WaitGroup
	for worker := 0; worker < workers; worker++ {
		worker := worker
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 250; i++ {
				code, hash := cacheTestCode(byte(worker+i), 96+(i%31))
				cache.admit(hash, code)
				if got, ok := cache.get(hash); ok && !bytes.Equal(got, code) {
					t.Errorf("concurrent hit returned wrong code for %x", hash)
					return
				}
			}
		}()
	}
	wg.Wait()
	cache.mu.Lock()
	defer cache.mu.Unlock()
	if cache.bytes > cache.maxBytes {
		t.Fatalf("concurrent cache bytes=%d exceed max=%d", cache.bytes, cache.maxBytes)
	}
}

func TestStateCodeCacheIsolatedPerDatabase(t *testing.T) {
	code, hash := cacheTestCode(0x61, 128)
	diskA := ethrawdb.NewMemoryDatabase()
	diskB := ethrawdb.NewMemoryDatabase()
	if err := rawdb.WriteStateCode(diskA, hash, code); err != nil {
		t.Fatal(err)
	}
	dbA := NewDatabaseWithConfig(diskA, DatabaseConfig{CodeCacheSizeBytes: 4096})
	dbB := NewDatabaseWithConfig(diskB, DatabaseConfig{CodeCacheSizeBytes: 4096})
	t.Cleanup(func() { _ = dbA.Close() })
	t.Cleanup(func() { _ = dbB.Close() })
	sdbA := &StateDB{db: dbA, codeStore: newRawDBStateCodeStore(diskA)}
	sdbB := &StateDB{db: dbB, codeStore: newRawDBStateCodeStore(diskB)}

	if got := sdbA.readStateCode(hash); !bytes.Equal(got, code) {
		t.Fatal("source database did not read and admit its code")
	}
	if got := sdbB.readStateCode(hash); got != nil {
		t.Fatal("code cache entry leaked into a different Database")
	}
}

func TestStateCodeCacheDatabaseCloseReleasesEntries(t *testing.T) {
	code, hash := cacheTestCode(0x62, 128)
	db := NewDatabaseWithConfig(ethrawdb.NewMemoryDatabase(), DatabaseConfig{CodeCacheSizeBytes: 4096})
	cache := db.codeCache
	cache.admit(hash, code)
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	cache.mu.Lock()
	closed, bytesUsed, entries := cache.closed, cache.bytes, len(cache.entries)
	cache.mu.Unlock()
	if !closed || bytesUsed != 0 || entries != 0 {
		t.Fatalf("closed cache state = closed %v, bytes %d, entries %d", closed, bytesUsed, entries)
	}
	if got, ok := cache.get(hash); ok || got != nil {
		t.Fatal("closed Database retained a readable code entry")
	}
}

type countingColdCodeHistory struct {
	code  []byte
	calls int
	err   error
}

func (h *countingColdCodeHistory) GetCodeAtOrBefore(_ tcommon.Hash, _ uint64) ([]byte, bool, error) {
	h.calls++
	if h.err != nil {
		return nil, false, h.err
	}
	return append([]byte(nil), h.code...), len(h.code) > 0, nil
}

func stateDBWithCodeHash(db *Database, addr tcommon.Address, hash tcommon.Hash) *StateDB {
	obj := newStateObject(addr, types.NewAccount(addr, corepb.AccountType_Contract))
	obj.codeHash = hash
	return &StateDB{
		db:           db,
		stateObjects: map[tcommon.Address]*stateObject{addr: obj},
		dirtyObjects: make(map[tcommon.Address]struct{}),
		codeStore:    newRawDBStateCodeStore(db.DiskDB()),
	}
}

func TestStateCodeCacheSharesColdHitAcrossStateDBLifecycle(t *testing.T) {
	code, hash := cacheTestCode(0x71, 256)
	db := NewDatabaseWithConfig(ethrawdb.NewMemoryDatabase(), DatabaseConfig{CodeCacheSizeBytes: 4096})
	t.Cleanup(func() { _ = db.Close() })
	addr := tcommon.Address{0x41, 0x01, 0x02}
	firstSource := &countingColdCodeHistory{code: code}
	first := stateDBWithCodeHash(db, addr, hash)
	first.SetCodeColdHistory(firstSource, 100)
	if got := first.GetCode(addr); !bytes.Equal(got, code) || firstSource.calls != 1 {
		t.Fatalf("first cold read = %x calls=%d", got, firstSource.calls)
	}

	secondSource := &countingColdCodeHistory{err: errors.New("cold source should not be called")}
	second := stateDBWithCodeHash(db, addr, hash)
	second.SetCodeColdHistory(secondSource, 1) // txNum differs; code remains content-addressed.
	got, err := second.GetCodeStrict(addr)
	if err != nil || !bytes.Equal(got, code) {
		t.Fatalf("cross-view cache hit = %x, err %v", got, err)
	}
	if secondSource.calls != 0 {
		t.Fatalf("cached second view called cold history %d times", secondSource.calls)
	}
}

func TestStateCodeCachePromotesStrictExecutionObjectHitForOracle(t *testing.T) {
	code, hash := cacheTestCode(0x79, 256)
	disk := ethrawdb.NewMemoryDatabase()
	db := NewDatabaseWithConfig(disk, DatabaseConfig{CodeCacheSizeBytes: 4096})
	t.Cleanup(func() { _ = db.Close() })
	addr := tcommon.Address{0x41, 0x07, 0x09}

	// Model a block-start execution copy which retained immutable code before
	// the hot row became temporarily unavailable. A successful strict worker
	// read must promote those hash-verified bytes to the Database-owned cache so
	// the independently acquired serial oracle sees the identical code.
	source := stateDBWithCodeHash(db, addr, hash)
	source.stateObjects[addr].code = append([]byte(nil), code...)
	worker, err := source.CopyBlockExecutionBase()
	if err != nil {
		t.Fatal(err)
	}
	if got, err := worker.GetCodeStrict(addr); err != nil || !bytes.Equal(got, code) {
		t.Fatalf("worker strict object hit = %x, err %v", got, err)
	}

	oracle := stateDBWithCodeHash(db, addr, hash)
	got, err := oracle.GetCodeStrict(addr)
	if err != nil || !bytes.Equal(got, code) {
		t.Fatalf("oracle shared-cache hit = %x, err %v", got, err)
	}
}

func TestStateCodeStrictRejectsCorruptExecutionObjectCode(t *testing.T) {
	code, hash := cacheTestCode(0x7a, 128)
	addr := tcommon.Address{0x41, 0x07, 0x0a}
	code[0] ^= 0xff
	for name, corruptCode := range map[string][]byte{
		"changed_bytes": code,
		"cached_empty":  {},
	} {
		t.Run(name, func(t *testing.T) {
			db := NewDatabaseWithConfig(ethrawdb.NewMemoryDatabase(), DatabaseConfig{CodeCacheSizeBytes: 4096})
			t.Cleanup(func() { _ = db.Close() })
			sdb := stateDBWithCodeHash(db, addr, hash)
			sdb.stateObjects[addr].code = corruptCode

			got, err := sdb.GetCodeStrict(addr)
			if err == nil || got != nil {
				t.Fatalf("corrupt strict object hit = %x, err %v", got, err)
			}
			for _, field := range []string{
				"contract=" + addr.Hex(), "codeHash=" + hash.Hex(),
				"actualHash=" + tcommon.Keccak256(corruptCode).Hex(),
				fmt.Sprintf("codeLen=%d", len(corruptCode)), "codeDirty=false",
			} {
				if !strings.Contains(err.Error(), field) {
					t.Errorf("mismatch error %q missing diagnostic %q", err, field)
				}
			}
			if _, ok := db.codeCache.get(hash); ok {
				t.Fatal("corrupt object code populated shared cache")
			}
		})
	}
}

type failingCodeDatabase struct {
	ethdb.Database
	err error
}

func (db failingCodeDatabase) Get([]byte) ([]byte, error) { return nil, db.err }
func (db failingCodeDatabase) Has([]byte) (bool, error)   { return true, nil }

func TestStateCodeCacheStrictMissPreservesReadError(t *testing.T) {
	wantErr := errors.New("durable read failed")
	disk := failingCodeDatabase{Database: ethrawdb.NewMemoryDatabase(), err: wantErr}
	db := NewDatabaseWithConfig(disk, DatabaseConfig{CodeCacheSizeBytes: 4096})
	t.Cleanup(func() { _ = db.Close() })
	_, hash := cacheTestCode(0x81, 64)
	sdb := &StateDB{db: db, codeStore: newRawDBStateCodeStore(disk)}
	if _, _, err := sdb.readStateCodeStrict(hash); !errors.Is(err, wantErr) {
		t.Fatalf("strict miss error = %v, want %v", err, wantErr)
	}
	if _, ok := db.codeCache.get(hash); ok {
		t.Fatal("strict read error populated the cache")
	}
}

func BenchmarkStateCodeCacheHit(b *testing.B) {
	code, hash := cacheTestCode(0x91, 32<<10)
	cache := newStateCodeCache(1 << 20)
	defer cache.close()
	cache.admit(hash, code)
	b.SetBytes(int64(len(code)))
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		stateCodeBenchmarkSink, _ = cache.get(hash)
	}
}

func BenchmarkStateCodeCacheHitParallel(b *testing.B) {
	code, hash := cacheTestCode(0x92, 32<<10)
	cache := newStateCodeCache(1 << 20)
	defer cache.close()
	cache.admit(hash, code)
	b.SetBytes(int64(len(code)))
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		var local []byte
		for pb.Next() {
			local, _ = cache.get(hash)
		}
		runtime.KeepAlive(local)
	})
}
