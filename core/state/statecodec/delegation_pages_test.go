package statecodec

import (
	"bytes"
	"encoding/binary"
	"testing"

	corepb "github.com/tronprotocol/go-tron/proto/core"
	"google.golang.org/protobuf/proto"
)

func TestDelegationPagesCodec(t *testing.T) {
	record := &corepb.DelegatedResourceAccountIndex{Account: []byte("arbitrary account"), Timestamp: -99}
	record.ProtoReflect().SetUnknown([]byte{0xa0, 6, 1})
	encoded, err := MarshalDelegationDescriptor(record, 123, 456)
	if err != nil {
		t.Fatal(err)
	}
	decoded, from, to, err := UnmarshalDelegationDescriptor(encoded)
	if err != nil || from != 123 || to != 456 || !proto.Equal(record, decoded) {
		t.Fatalf("descriptor: %v %d %d %v", decoded, from, to, err)
	}
	encoded[len(encoded)-1] ^= 1
	if !proto.Equal(record, decoded) {
		t.Fatal("descriptor decode borrowed input")
	}
	for _, list := range [][][]byte{nil, {nil, {}, []byte("same"), []byte("same"), {0, 255}}, make([][]byte, DelegationPageEntries)} {
		encoded, err := MarshalDelegationPage(list)
		if err != nil {
			t.Fatal(err)
		}
		decoded, err := UnmarshalDelegationPage(encoded)
		if err != nil || len(decoded) != len(list) {
			t.Fatalf("page: %v", err)
		}
		for i := range list {
			if !bytes.Equal(list[i], decoded[i]) {
				t.Fatalf("page item %d differs", i)
			}
		}
		clear(encoded)
		for i := range list {
			if !bytes.Equal(list[i], decoded[i]) {
				t.Fatalf("page item %d borrows input", i)
			}
		}
		if len(decoded) >= 3 {
			decoded[2] = append(decoded[2], 1)
			if !bytes.Equal(decoded[3], list[3]) {
				t.Fatal("page entry capacity aliases neighbor")
			}
		}
	}
}

func TestDelegationPagesCodecRejectsCorruption(t *testing.T) {
	for _, data := range [][]byte{nil, []byte("old-format"), []byte("DLP1"), append(make([]byte, 20), 1)} {
		if _, _, _, err := UnmarshalDelegationDescriptor(data); err == nil {
			t.Fatalf("accepted descriptor %x", data)
		}
	}
	descriptor, err := MarshalDelegationDescriptor(&corepb.DelegatedResourceAccountIndex{}, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	binary.BigEndian.PutUint64(descriptor[4:12], ^uint64(0))
	if _, _, _, err := UnmarshalDelegationDescriptor(descriptor); err == nil {
		t.Fatal("accepted overflowing page count")
	}
	metadata, err := MarshalDelegationIndex(&corepb.DelegatedResourceAccountIndex{ToAccounts: [][]byte{[]byte("embedded")}})
	if err != nil {
		t.Fatal(err)
	}
	descriptor = append(append([]byte("DLP1"), make([]byte, 16)...), metadata...)
	if _, _, _, err := UnmarshalDelegationDescriptor(descriptor); err == nil {
		t.Fatal("accepted descriptor with embedded lists")
	}
	for _, data := range [][]byte{nil, []byte("DPP1"), []byte("DPP1\x81\x01"), []byte("DPP1\x01\x80"), []byte("DPP1\x01\x02\x01"), []byte("DPP1\x00\x01")} {
		if _, err := UnmarshalDelegationPage(data); err == nil {
			t.Fatalf("accepted page %x", data)
		}
	}
	if _, err := MarshalDelegationPage(make([][]byte, DelegationPageEntries+1)); err == nil {
		t.Fatal("encoded oversized page")
	}
	if _, err := MarshalDelegationDescriptor(nil, 0, 0); err == nil {
		t.Fatal("encoded nil descriptor")
	}
}
