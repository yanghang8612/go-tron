package rawdb

import (
	"bytes"
	"testing"

	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/tronprotocol/go-tron/internal/historychunk"
)

// Both reader contracts expose the same immutable fixed input. The ordinary
// database follows Has/Get and the production read helper's ownership copy;
// the coupled reader returns a stable borrowed value without that copy. Neither
// includes storage latency, iteration, RLP parsing, fixture encoding or GC work.
type sharedHistoryReaderBenchmarkDB map[string][]byte

func (r sharedHistoryReaderBenchmarkDB) Get(key []byte) ([]byte, error) {
	return r[string(key)], nil
}

func (r sharedHistoryReaderBenchmarkDB) Has(key []byte) (bool, error) {
	_, ok := r[string(key)]
	return ok, nil
}

type sharedHistoryReaderBenchmarkPresenceDB struct{ sharedHistoryReaderBenchmarkDB }

func (r sharedHistoryReaderBenchmarkPresenceDB) GetWithPresence(key []byte) ([]byte, bool, error) {
	v, ok := r.sharedHistoryReaderBenchmarkDB[string(key)]
	return v, ok, nil
}

// The complete pack method includes header preflight, every point read,
// envelope/size/decode checks, every chunk SHA, output construction, and whole
// pack SHA. There is no content/authentication cache between operations.
func BenchmarkSharedHistoryReaderFullPack(b *testing.B) {
	for _, tc := range []struct {
		name     string
		size     int
		count    int
		repeated bool
		codec    string
	}{
		{"tiny_raw", 64, 1, false, "raw"},
		{"small_snappy", historychunk.MinSize, 1, false, "snappy"},
		{"raw_unique", historychunk.MaxSize, 48, false, "raw"},
		{"raw_repeated", historychunk.MaxSize, 48, true, "raw"},
		{"snappy_unique", historychunk.MaxSize, 48, false, "snappy"},
		{"snappy_repeated", historychunk.MaxSize, 48, true, "snappy"},
		{"mixed_unique", historychunk.MaxSize, 48, false, "mixed"},
	} {
		b.Run(tc.name, func(b *testing.B) {
			f := newSharedHistoryReaderFixture(sharedHistoryReaderChunks(tc.size, tc.count, tc.repeated, tc.codec))
			for _, presence := range []bool{false, true} {
				var db ethdb.KeyValueReader = sharedHistoryReaderBenchmarkDB(f.values)
				contract := "has_get"
				if presence {
					db = sharedHistoryReaderBenchmarkPresenceDB{sharedHistoryReaderBenchmarkDB(f.values)}
					contract = "coupled"
				}
				for _, candidate := range []bool{false, true} {
					name, operation := "legacy", sharedHistoryReaderOperation(legacySharedHistoryReaderDecodeStateHistorySharedPack)
					if candidate {
						name, operation = "candidate", decodeStateHistorySharedPack
					}
					// Verify the exact benchmark fixture with each full operation
					// once, outside the measured loop.
					got, err := operation(db, f.pack, sharedHistoryReaderFixtureBlock)
					if err != nil || !bytes.Equal(got, f.raw) {
						b.Fatalf("benchmark fixture decode: %v", err)
					}
					b.Run(contract+"/"+name, func(b *testing.B) {
						b.SetBytes(int64(len(f.raw)))
						b.ReportAllocs()
						for b.Loop() {
							got, err := operation(db, f.pack, sharedHistoryReaderFixtureBlock)
							if err != nil || len(got) != len(f.raw) {
								b.Fatalf("full pack decode: %v", err)
							}
						}
						b.ReportMetric(float64(len(f.raw)), "raw_B/op")
						b.ReportMetric(float64(tc.count), "refs/op")
					})
				}
			}
		})
	}
}
