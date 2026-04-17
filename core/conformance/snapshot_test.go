package conformance

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"path/filepath"
	"strings"
	"testing"

	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core"
)

// TestSnapshotRoundTrip verifies the digest-algorithm-is-single-sourced
// property: a snapshot dumped from a state, when reloaded, yields the same
// DigestB as the original. This is what fixture-digest relies on to emit
// an OracleEntry that the replay engine will later match.
func TestSnapshotRoundTrip(t *testing.T) {
	dir := t.TempDir()
	witness := buildRangeFixture(t, dir, 1000)

	loaded, err := LoadSeed(filepath.Join(dir, "seed.json"))
	if err != nil {
		t.Fatal(err)
	}
	// Advance state by one block so we capture something non-trivial.
	rdr, _ := openBlocksReader(filepath.Join(dir, "blocks.bin"))
	defer rdr.Close()
	blk, err := rdr.Next()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := core.ProcessBlock(loaded.StateDB, loaded.DynProps, blk, loaded.DiskDB, []tcommon.Address{witness}, 0); err != nil {
		t.Fatal(err)
	}

	d0 := DigestB(loaded.StateDB, loaded.DiskDB, loaded.Closure, loaded.DynProps)

	// Dump → JSON → LoadSnapshot → digest.
	snap, err := DumpSnapshot(loaded, blk.Number())
	if err != nil {
		t.Fatal(err)
	}
	data, err := json.Marshal(snap)
	if err != nil {
		t.Fatal(err)
	}

	reloaded, parsedSnap, err := LoadSnapshot(bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}
	if parsedSnap.BlockNum != blk.Number() {
		t.Fatalf("BlockNum lost: got %d, want %d", parsedSnap.BlockNum, blk.Number())
	}
	d1 := DigestB(reloaded.StateDB, reloaded.DiskDB, reloaded.Closure, reloaded.DynProps)

	if d0 != d1 {
		t.Fatalf("digest drifted across snapshot round-trip:\n  original: %s\n  reloaded: %s", hex.EncodeToString(d0[:]), hex.EncodeToString(d1[:]))
	}
}

func TestSnapshotRoundTrip_PreservesContractState(t *testing.T) {
	dir := t.TempDir()
	witness := buildRangeFixture(t, dir, 1000)
	loaded, err := LoadSeed(filepath.Join(dir, "seed.json"))
	if err != nil {
		t.Fatal(err)
	}

	// Plant a contract + contract state for a separate address.
	contractAddr, _ := ParseAddress("41" + strings.Repeat("c", 40))
	loaded.Closure = append(loaded.Closure, contractAddr)
	loaded.StateDB.CreateAccount(contractAddr, 2 /*contract*/)
	loaded.StateDB.SetCode(contractAddr, []byte{0x60, 0x01, 0x00})
	_ = witness

	d0 := DigestB(loaded.StateDB, loaded.DiskDB, loaded.Closure, loaded.DynProps)

	snap, err := DumpSnapshot(loaded, 0)
	if err != nil {
		t.Fatal(err)
	}
	data, _ := json.Marshal(snap)
	reloaded, _, err := LoadSnapshot(bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}
	d1 := DigestB(reloaded.StateDB, reloaded.DiskDB, reloaded.Closure, reloaded.DynProps)
	if d0 != d1 {
		t.Fatalf("digest drift when contract present:\n  orig:     %x\n  reloaded: %x", d0, d1)
	}
}
