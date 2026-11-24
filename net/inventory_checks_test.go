package net

import (
	"encoding/binary"
	"testing"

	tcommon "github.com/tronprotocol/go-tron/common"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	"google.golang.org/protobuf/proto"
)

func TestInventoryDecodeAndIDValidation(t *testing.T) {
	id := func(value uint64) []byte {
		hash := make([]byte, tcommon.HashLength)
		binary.BigEndian.PutUint64(hash, value)
		return hash
	}
	for _, kind := range []corepb.Inventory_InventoryType{corepb.Inventory_TRX, corepb.Inventory_BLOCK} {
		data, err := proto.Marshal(&corepb.Inventory{Type: kind, Ids: [][]byte{id(1), id(2)}})
		if err != nil {
			t.Fatal(err)
		}
		decoded, err := decodeInventory(data)
		if err != nil {
			t.Fatal(err)
		}
		if decoded.Type != kind || len(decoded.Ids) != 2 {
			t.Fatalf("decoded inventory = %v", decoded)
		}
		if err := validateInventoryIDs(decoded.Ids); err != nil {
			t.Fatal(err)
		}
	}
	for _, ids := range [][][]byte{{id(1), id(1)}, {id(1)[:31]}, {append(id(1), 0)}} {
		if err := validateInventoryIDs(ids); err == nil {
			t.Fatalf("invalid inventory accepted: %x", ids)
		}
	}
	data, err := proto.Marshal(&corepb.Inventory{Type: 22, Ids: [][]byte{id(1)}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := decodeInventory(data); err == nil {
		t.Fatal("unknown inventory type accepted")
	}
}

func TestInventoryElementLimitsUseIndependentRollingWindows(t *testing.T) {
	const start = int64(1000)
	var limits peerInventoryLimits
	if !limits.allow(corepb.Inventory_BLOCK, blockInvLimit-1, start) ||
		!limits.allow(corepb.Inventory_TRX, transactionInvLimit-1, start) {
		t.Fatal("legal inventory refused")
	}
	if !limits.allow(corepb.Inventory_BLOCK, 1, start+9) ||
		!limits.allow(corepb.Inventory_TRX, 1, start+9) {
		t.Fatal("exact window limits refused")
	}
	if limits.allow(corepb.Inventory_BLOCK, 1, start+9) ||
		limits.allow(corepb.Inventory_TRX, 1, start+9) {
		t.Fatal("inventory above the window limit was admitted")
	}
	if !limits.allow(corepb.Inventory_BLOCK, blockInvLimit-1, start+10) ||
		!limits.allow(corepb.Inventory_TRX, transactionInvLimit-1, start+10) {
		t.Fatal("expired counts or rejected elements still consumed capacity")
	}
}
