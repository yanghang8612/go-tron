package vm

import (
	"bytes"
	"encoding/hex"
	"testing"

	"github.com/tronprotocol/go-tron/core/zksnark"
)

func TestShieldedTRC20NileRootBeforeTransfer6498505(t *testing.T) {
	if !zksnark.Available() {
		t.Skip("sapling backend unavailable")
	}
	commitments := []string{
		"9e1b521034d39cb583b0d0559c9fce8f7e903525280496dcdf5390ccb81b8a03",
		"cdae9963912cae1121ce55ad029b5de9340b2d2189dda9940b6cd240b967a15d",
		"36f2757bee2126ce9d045fc27fc6603efbb60e35fed519ca8a3a72133c8ad565",
		"908c09c25597ecfc1e88d074641badb678165183d68f4ddbb91743e28732e91b",
	}
	wantAnchor := mustHexNile(t, "51f143b9b98680ee7e4c9d2ea7fd69f5cf0942ed8e808badb7b372ee0e88e33b")
	var frontier [33]zksnark.PedersenHash
	var root []byte
	for i, cmHex := range commitments {
		var leaf zksnark.PedersenHash
		copy(leaf[:], mustHexNile(t, cmHex))
		out := shieldedInsertLeaves(frontier, uint64(i), []zksnark.PedersenHash{leaf})
		if len(out) < 96 || out[31] != 1 {
			t.Fatalf("insert leaf %d failed: %x", i, out)
		}
		slot := int(parseUint64FromWord(out, 32))
		if slot == 0 {
			frontier[0] = leaf
		} else {
			copy(frontier[slot][:], out[32*(1+slot):32*(2+slot)])
		}
		root = out[len(out)-32:]
		t.Logf("leaf %d slot %d root %x", i, slot, root)
	}
	if !bytes.Equal(root, wantAnchor) {
		t.Fatalf("root after four leaves = %x, want anchor %x", root, wantAnchor)
	}
}

func mustHexNile(t *testing.T, s string) []byte {
	t.Helper()
	b, err := hex.DecodeString(s)
	if err != nil {
		t.Fatal(err)
	}
	return b
}
