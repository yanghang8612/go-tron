package rawdb

import (
	"errors"
	"testing"

	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/state/kvdomains"
)

var stateKVNoCopyBenchValue []byte
var stateKVNoCopyBenchPresent bool

type structuredStateKVBenchReader struct {
	value []byte
	err   error
}

func (r structuredStateKVBenchReader) Has([]byte) (bool, error) { return r.err == nil, nil }
func (r structuredStateKVBenchReader) Get([]byte) ([]byte, error) {
	return r.value, r.err
}
func (r structuredStateKVBenchReader) GetNoCopyCachedStateKVLatest([]byte, common.AccountID, uint64, uint16, []byte) ([]byte, error) {
	return r.value, r.err
}
func (r structuredStateKVBenchReader) IsKeyNotFound(err error) bool {
	return errors.Is(err, errStructuredStateKVMissing)
}

var errStructuredStateKVMissing = errors.New("structured state kv missing")

func BenchmarkReadStateKVLatestNoCopyStructured(b *testing.B) {
	owner := common.BytesToAddress([]byte{common.AddressPrefixMainnet, 0x42})
	key := make([]byte, 32)
	key[31] = 0x73
	for _, tc := range []struct {
		name   string
		reader structuredStateKVBenchReader
	}{
		{"hit", structuredStateKVBenchReader{value: EncodeStateKVLatestValue([]byte{0x12, 0x34})}},
		{"miss", structuredStateKVBenchReader{err: errStructuredStateKVMissing}},
	} {
		b.Run(tc.name, func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				value, present, err := ReadStateKVLatestNoCopy(tc.reader, owner, 7, kvdomains.ContractStorage, key)
				if err != nil {
					b.Fatal(err)
				}
				stateKVNoCopyBenchValue, stateKVNoCopyBenchPresent = value, present
			}
		})
	}
}
