package blockbuffer

import (
	"bytes"
	"encoding/binary"
	"testing"

	"github.com/tronprotocol/go-tron/core/pointread"
	"github.com/tronprotocol/go-tron/core/rawdb"
)

// Use the existing in-memory callback cursor to isolate recorder overhead.
// The same file also runs against the saved pre-diagnostics production files.
func BenchmarkCommitmentParentCacheDiagnostics(b *testing.B) {
	for _, kind := range []string{"cache_hit", "no_resident", "resident_newer", "version_changed"} {
		b.Run(kind, func(b *testing.B) {
			cache := newBaseReadCache(1<<20, rawdb.CommitmentBranchKeyPrefix)
			session := &commitmentParentReadSession{
				cache: cache, snapshot: benchmarkCommitmentSnapshot{},
				cursors: make([]pointread.Cursor, 1), keyScratch: borrowCommitmentParentKeyScratch(1),
			}
			session.readContexts = borrowCommitmentParentReadContexts(session, 1)
			defer func() {
				if err := session.Close(); err != nil {
					b.Error(err)
				}
			}()
			prefix := []byte(rawdb.CommitmentBranchKeyPrefix)
			path := bytes.Repeat([]byte{0x0a}, 64)
			if kind == "resident_newer" || kind == "version_changed" {
				cache.advanceVersion()
			}
			if kind == "cache_hit" || kind == "resident_newer" {
				key := append(append([]byte(nil), prefix...), path...)
				testBaseReadCacheSet(cache, key, benchmarkCommitmentValue)
			}
			consume := func([]byte, bool) error { return nil }
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if kind == "no_resident" || kind == "version_changed" {
					// Unique keys never complete two-hit probation. This measures
					// the existing miss branch without unbounded resident growth.
					binary.BigEndian.PutUint64(path[len(path)-8:], uint64(i))
				}
				if found, err := session.ViewKeyParts(0, prefix, path, consume); err != nil || !found {
					b.Fatalf("view = (%v,%v)", found, err)
				}
			}
		})
	}
}
