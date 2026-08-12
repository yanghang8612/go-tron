package types

import (
	"bytes"
	"encoding/binary"
	"testing"

	corepb "github.com/tronprotocol/go-tron/proto/core"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
)

func TestAccountStorageCoreV4RoundTripAndExactSize(t *testing.T) {
	pb := &corepb.Account{
		AccountName: []byte("name"), Type: corepb.AccountType_Contract,
		Address: []byte{0x41, 1, 2, 3}, Balance: -7, NetUsage: 8,
		AcquiredDelegatedFrozenBalanceForBandwidth: 9,
		DelegatedFrozenBalanceForBandwidth:         10,
		OldTronPower:                               -1, AssetOptimized: true, CreateTime: 11,
		LatestOprationTime: 12, Allowance: 13, LatestWithdrawTime: 14,
		Code: []byte{1, 2}, IsWitness: true, IsCommittee: true,
		AssetIssuedName: []byte("asset"), AssetIssued_ID: []byte("1000001"),
		FreeNetUsage: 15, LatestConsumeTime: 16, LatestConsumeFreeTime: 17,
		AccountId: []byte("id"), NetWindowSize: 18, NetWindowOptimized: true,
		CodeHash: []byte{3, 4}, DelegatedFrozenV2BalanceForBandwidth: 19,
		AcquiredDelegatedFrozenV2BalanceForBandwidth: 20,
	}
	unknown := protowire.AppendTag(nil, 100, protowire.VarintType)
	unknown = protowire.AppendVarint(unknown, 123)
	pb.ProtoReflect().SetUnknown(unknown)
	account := NewAccountFromPB(pb)

	size, err := account.StorageCoreV4Size()
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := account.MarshalStorageCoreV4()
	if err != nil {
		t.Fatal(err)
	}
	if len(encoded) != size {
		t.Fatalf("encoded size = %d, want exact %d", len(encoded), size)
	}
	if !IsAccountStorageCoreV4(encoded) {
		t.Fatalf("missing storage-v4 magic: %x", encoded)
	}
	if err := proto.Unmarshal(encoded, new(corepb.Account)); err == nil {
		t.Fatal("storage-v4 unexpectedly parsed as protobuf")
	}
	decoded, err := UnmarshalAccountStorageCoreV4(encoded)
	if err != nil {
		t.Fatal(err)
	}
	if !proto.Equal(decoded.Proto(), pb) {
		t.Fatalf("round trip = %+v, want %+v", decoded.Proto(), pb)
	}
	again, err := account.MarshalStorageCoreV4()
	if err != nil || !bytes.Equal(again, encoded) {
		t.Fatalf("non-deterministic encoding: err=%v\n%x\n%x", err, encoded, again)
	}
	prefixed, err := account.AppendStorageCoreV4([]byte{0xaa})
	if err != nil || !bytes.Equal(prefixed[1:], encoded) {
		t.Fatalf("append mismatch: err=%v got=%x", err, prefixed)
	}
}

func TestAccountStorageCoreV4RejectsCorruption(t *testing.T) {
	encoded, err := NewAccountFromPB(&corepb.Account{Address: []byte{0x41}}).MarshalStorageCoreV4()
	if err != nil {
		t.Fatal(err)
	}
	// Address length 1 starts at byte 9. Encode it non-canonically as 0x81,0x00.
	nonCanonical := append(bytes.Clone(encoded[:9]), 0x81, 0x00)
	nonCanonical = append(nonCanonical, encoded[10:]...)
	for _, value := range [][]byte{encoded[:4], encoded[:len(encoded)-1], append(bytes.Clone(encoded), 0), nonCanonical} {
		if _, err := UnmarshalAccountStorageCoreV4(value); err == nil {
			t.Fatalf("accepted corrupt value %x", value)
		}
	}
}

func TestAccountStorageCoreV4RejectsPresentDefaultValuesAndEnumAlias(t *testing.T) {
	value := func(fields uint32, payload []byte) []byte {
		raw := append([]byte(nil), accountStorageV4Magic[:]...)
		raw = append(raw, accountStorageV4CodecVersion)
		raw = binary.BigEndian.AppendUint32(raw, fields)
		return append(raw, payload...)
	}
	for name, raw := range map[string][]byte{
		"empty bytes": value(1<<0, []byte{0}),
		"zero int":    value(1<<3, []byte{0}),
		"enum alias":  value(1<<1, binary.AppendUvarint(nil, accountStorageV4Signed(1<<32))),
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := UnmarshalAccountStorageCoreV4(raw); err == nil {
				t.Fatal("non-canonical present-default or truncating enum value was accepted")
			}
		})
	}
}

func TestAccountStorageCoreV4SparseOverheadIsBounded(t *testing.T) {
	pb := &corepb.Account{Address: bytes.Repeat([]byte{0x41}, 21), Balance: 1_000_000}
	account := NewAccountFromPB(pb)
	legacy, err := account.MarshalStorageCore()
	if err != nil {
		t.Fatal(err)
	}
	native, err := account.MarshalStorageCoreV4()
	if err != nil {
		t.Fatal(err)
	}
	// Magic/version/bitmap intentionally cost nine bytes. The sparse codec must
	// not regress to a fixed-width record for ordinary accounts.
	if len(native) > len(legacy)+10 {
		t.Fatalf("sparse native size = %d, legacy protobuf = %d", len(native), len(legacy))
	}
}
