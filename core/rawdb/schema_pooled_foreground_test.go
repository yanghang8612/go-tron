package rawdb

import (
	"bytes"
	"testing"

	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/state/kvdomains"
)

func TestClassifyStateKVLatestReadDomain(t *testing.T) {
	owner := common.Address{0: 0x41, 20: 1}
	key := func(d kvdomains.KVDomain, logical []byte) []byte { return stateKVLatestKey(owner, 17, d, logical) }
	tests := []struct {
		name   string
		key    []byte
		domain kvdomains.KVDomain
		ok     bool
	}{
		{"storage", key(kvdomains.ContractStorage, make([]byte, 32)), kvdomains.ContractStorage, true},
		{"storage-short", key(kvdomains.ContractStorage, make([]byte, 31)), 0, false},
		{"storage-long", key(kvdomains.ContractStorage, make([]byte, 33)), 0, false},
		{"asset-one", key(kvdomains.AccountAssetV2, []byte("1")), kvdomains.AccountAssetV2, true},
		{"asset-max", key(kvdomains.AccountAssetV2, []byte("9223372036854775807")), kvdomains.AccountAssetV2, true},
		{"unmeasured", key(kvdomains.ContractMetadata, make([]byte, 32)), 0, false},
		{"unknown", key(kvdomains.KVDomain(0xffff), make([]byte, 32)), 0, false},
		{"other-family", StateAccountLatestCommitmentKey(owner), 0, false},
		{"nil", nil, 0, false}, {"prefix-only", stateKVLatestPrefix, 0, false},
	}
	for _, logical := range []string{"", "0", "00", "01", "-1", "+1", "1x", " 1", "1 ", "9223372036854775808", "10000000000000000000", "111111111111111111111"} {
		tests = append(tests, struct {
			name   string
			key    []byte
			domain kvdomains.KVDomain
			ok     bool
		}{"asset-invalid-" + logical, key(kvdomains.AccountAssetV2, []byte(logical)), 0, false})
	}
	good := key(kvdomains.ContractStorage, bytes.Repeat([]byte{0xa5}, 32))
	for i := 0; i < len(stateKVLatestPrefix)+common.AccountIDLength+10; i++ {
		tests = append(tests, struct {
			name   string
			key    []byte
			domain kvdomains.KVDomain
			ok     bool
		}{"truncated", good[:i], 0, false})
	}
	wrongPrefix := bytes.Clone(good)
	wrongPrefix[0] ^= 1
	tests = append(tests, struct {
		name   string
		key    []byte
		domain kvdomains.KVDomain
		ok     bool
	}{"wrong-prefix", wrongPrefix, 0, false})
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d, ok := ClassifyStateKVLatestReadDomain(tt.key)
			if d != tt.domain || ok != tt.ok {
				t.Fatalf("classify = (%x,%v), want (%x,%v)", d, ok, tt.domain, tt.ok)
			}
			if n := testing.AllocsPerRun(100, func() { ClassifyStateKVLatestReadDomain(tt.key) }); n != 0 {
				t.Fatalf("allocations=%v", n)
			}
		})
	}
}
