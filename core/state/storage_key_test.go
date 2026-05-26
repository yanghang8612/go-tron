package state

import (
	"errors"
	"testing"

	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/state/kvdomains"
	contractpb "github.com/tronprotocol/go-tron/proto/core/contract"
	"google.golang.org/protobuf/proto"
)

func TestStorageRowKeyFromFlatLatestUsesTypedMetadataReader(t *testing.T) {
	addr := testAddr(0x91)
	slot := tcommon.BytesToHash([]byte("slot"))
	meta := &contractpb.SmartContract{
		Version: 1,
		TrxHash: []byte{
			0x01, 0x02, 0x03, 0x04,
			0x05, 0x06, 0x07, 0x08,
			0x09, 0x0a, 0x0b, 0x0c,
			0x0d, 0x0e, 0x0f, 0x10,
		},
	}
	metaBytes, err := proto.Marshal(meta)
	if err != nil {
		t.Fatal(err)
	}
	reader := &storageKeyLatestReader{
		t:          t,
		owner:      addr,
		generation: 9,
		value:      metaBytes,
	}

	got, err := storageRowKeyFromFlatLatest(reader, addr, 9, slot)
	if err != nil {
		t.Fatalf("storage row key from typed latest reader: %v", err)
	}
	want := javaStorageRowKey(addr, slot, meta)
	if got != want {
		t.Fatalf("row key = %x, want %x", got, want)
	}
	if reader.calls != 1 {
		t.Fatalf("metadata reader calls = %d, want 1", reader.calls)
	}
}

func TestStorageRowKeyFromFlatLatestReturnsReaderError(t *testing.T) {
	wantErr := errors.New("metadata unavailable")
	reader := &storageKeyLatestReader{err: wantErr}
	if _, err := storageRowKeyFromFlatLatest(reader, testAddr(0x92), 1, tcommon.Hash{0x01}); !errors.Is(err, wantErr) {
		t.Fatalf("storage row key error = %v, want %v", err, wantErr)
	}
}

type storageKeyLatestReader struct {
	t          *testing.T
	owner      tcommon.Address
	generation uint64
	value      []byte
	err        error
	calls      int
}

func (r *storageKeyLatestReader) KVLatest(owner tcommon.Address, generation uint64, domain kvdomains.KVDomain, key []byte) ([]byte, bool, error) {
	r.calls++
	if r.err != nil {
		return nil, false, r.err
	}
	r.t.Helper()
	if owner != r.owner {
		r.t.Fatalf("owner = %s, want %s", owner.Hex(), r.owner.Hex())
	}
	if generation != r.generation {
		r.t.Fatalf("generation = %d, want %d", generation, r.generation)
	}
	if domain != kvdomains.ContractMetadata {
		r.t.Fatalf("domain = %#04x, want ContractMetadata", uint16(domain))
	}
	if string(key) != string(contractMetaKVKey) {
		r.t.Fatalf("key = %q, want %q", key, contractMetaKVKey)
	}
	return append([]byte(nil), r.value...), len(r.value) > 0, nil
}
