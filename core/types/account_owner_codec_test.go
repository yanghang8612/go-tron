package types

import (
	"bytes"
	"testing"

	"github.com/tronprotocol/go-tron/common"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	"google.golang.org/protobuf/proto"
)

func TestAccountOwnerCodecExactContextAndFallback(t *testing.T) {
	owner := common.Address{0x41, 7}
	for _, address := range [][]byte{nil, owner[:], {0x41}, bytes.Repeat([]byte{0x77}, 21)} {
		pb := &corepb.Account{Address: bytes.Clone(address), Balance: -17, CodeHash: bytes.Repeat([]byte{9}, 32), AccountName: []byte("test")}
		pb.ProtoReflect().SetUnknown([]byte{0xa0, 6, 1})
		a := NewAccountFromPB(pb)
		base, err := a.MarshalStorageCoreV4()
		if err != nil {
			t.Fatal(err)
		}
		encoded, err := a.MarshalStorageCoreV4(owner)
		if err != nil {
			t.Fatal(err)
		}
		elided := bytes.Equal(address, owner[:])
		if elided && (encoded[4] != 2 || len(base)-len(encoded) != 22) {
			t.Fatal("address not elided")
		}
		if !elided && !bytes.Equal(base, encoded) {
			t.Fatal("nonmatching/absent address changed")
		}
		got, err := UnmarshalAccountStorageCoreV4(encoded, owner)
		if err != nil || !proto.Equal(got.Proto(), pb) {
			t.Fatal("logical account changed", err)
		}
		if _, err := AccountStorageCoreV4CodeHash(encoded); err != nil {
			t.Fatal("hash-only reader", err)
		}
		if _, err := UnmarshalAccountStorageCoreV4(encoded); elided && err == nil {
			t.Fatal("owner context not required")
		}
		again, _ := got.MarshalStorageCoreV4(owner)
		if !bytes.Equal(again, encoded) {
			t.Fatal("noncanonical round trip")
		}
	}
}

func BenchmarkAccountOwnerCoreRead(b *testing.B) {
	owner := common.Address{0x41, 7}
	account := NewAccountFromPB(&corepb.Account{Address: owner[:], Balance: 999999, CodeHash: bytes.Repeat([]byte{9}, 32), AccountName: []byte("sample"), CreateTime: 1530000000000})
	for _, elide := range []bool{false, true} {
		var data []byte
		if elide {
			data, _ = account.MarshalStorageCoreV4(owner)
		} else {
			data, _ = account.MarshalStorageCoreV4()
		}
		name := "full-address"
		if elide {
			name = "owner-address"
		}
		b.Run(name, func(b *testing.B) {
			b.ReportAllocs()
			b.ReportMetric(float64(len(data)), "stored-bytes")
			for i := 0; i < b.N; i++ {
				if _, err := UnmarshalAccountStorageCoreV4(data, owner); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
