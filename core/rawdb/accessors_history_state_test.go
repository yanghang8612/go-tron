package rawdb

import (
	"bytes"
	"errors"
	"testing"

	"github.com/ethereum/go-ethereum/ethdb/memorydb"
	tcommon "github.com/tronprotocol/go-tron/common"
	historypb "github.com/tronprotocol/go-tron/proto/core/historystate"
)

// ---- AccountDelta -------------------------------------------------------

func TestAccountDelta_RoundTrip(t *testing.T) {
	db := memorydb.New()
	var addr tcommon.Address
	addr[0] = 0x41
	addr[20] = 0xAB
	const blockNum uint64 = 12345

	if got := ReadAccountDelta(db, blockNum, addr); got != nil {
		t.Fatalf("expected nil before write, got %v", got)
	}
	if HasAccountDelta(db, blockNum, addr) {
		t.Fatal("expected absent before write")
	}

	delta := &historypb.AccountDelta{
		ExistedPre:       true,
		AccountProtoPre:  []byte("marshalled-account-bytes"),
		CodePre:          []byte("contract-bytecode"),
		ContractMetaPre:  []byte("marshalled-contract-meta"),
	}
	if err := WriteAccountDelta(db, blockNum, addr, delta); err != nil {
		t.Fatalf("WriteAccountDelta: %v", err)
	}

	if !HasAccountDelta(db, blockNum, addr) {
		t.Fatal("expected present after write")
	}
	got := ReadAccountDelta(db, blockNum, addr)
	if got == nil {
		t.Fatal("ReadAccountDelta returned nil")
	}
	if !got.ExistedPre {
		t.Error("ExistedPre mismatch")
	}
	if string(got.AccountProtoPre) != "marshalled-account-bytes" {
		t.Errorf("AccountProtoPre mismatch: %q", got.AccountProtoPre)
	}
	if string(got.CodePre) != "contract-bytecode" {
		t.Errorf("CodePre mismatch: %q", got.CodePre)
	}
	if string(got.ContractMetaPre) != "marshalled-contract-meta" {
		t.Errorf("ContractMetaPre mismatch: %q", got.ContractMetaPre)
	}
	if !bytes.Equal(got.Addr, addr.Bytes()) {
		t.Errorf("Addr was not stamped: got %x, want %x", got.Addr, addr.Bytes())
	}
}

func TestAccountDelta_ExistedPreFalse(t *testing.T) {
	// Created-this-block account: ExistedPre=false, pre-image blobs empty.
	db := memorydb.New()
	var addr tcommon.Address
	addr[20] = 0xCD
	const blockNum uint64 = 7

	delta := &historypb.AccountDelta{ExistedPre: false}
	if err := WriteAccountDelta(db, blockNum, addr, delta); err != nil {
		t.Fatal(err)
	}
	got := ReadAccountDelta(db, blockNum, addr)
	if got == nil {
		t.Fatal("ReadAccountDelta returned nil")
	}
	if got.ExistedPre {
		t.Error("expected ExistedPre=false")
	}
	if len(got.AccountProtoPre) != 0 || len(got.CodePre) != 0 || len(got.ContractMetaPre) != 0 {
		t.Errorf("expected all pre blobs empty: %+v", got)
	}
}

func TestAccountDelta_NilWriteRejected(t *testing.T) {
	db := memorydb.New()
	var addr tcommon.Address
	if err := WriteAccountDelta(db, 1, addr, nil); err == nil {
		t.Fatal("expected error writing nil AccountDelta")
	}
}

func TestAccountDelta_Delete(t *testing.T) {
	db := memorydb.New()
	var addr tcommon.Address
	addr[0] = 0x41
	if err := WriteAccountDelta(db, 99, addr, &historypb.AccountDelta{ExistedPre: true}); err != nil {
		t.Fatal(err)
	}
	if !HasAccountDelta(db, 99, addr) {
		t.Fatal("present after write")
	}
	if err := DeleteAccountDelta(db, 99, addr); err != nil {
		t.Fatal(err)
	}
	if HasAccountDelta(db, 99, addr) {
		t.Fatal("expected absent after delete")
	}
}

func TestAccountDelta_DistinctBlocksDoNotCollide(t *testing.T) {
	db := memorydb.New()
	var addr tcommon.Address
	addr[20] = 0xEE

	if err := WriteAccountDelta(db, 1, addr, &historypb.AccountDelta{
		ExistedPre:      true,
		AccountProtoPre: []byte("block-1"),
	}); err != nil {
		t.Fatal(err)
	}
	if err := WriteAccountDelta(db, 2, addr, &historypb.AccountDelta{
		ExistedPre:      true,
		AccountProtoPre: []byte("block-2"),
	}); err != nil {
		t.Fatal(err)
	}

	if got := ReadAccountDelta(db, 1, addr); got == nil || string(got.AccountProtoPre) != "block-1" {
		t.Errorf("block 1 row corrupted: %+v", got)
	}
	if got := ReadAccountDelta(db, 2, addr); got == nil || string(got.AccountProtoPre) != "block-2" {
		t.Errorf("block 2 row corrupted: %+v", got)
	}
}

// ---- SlotDelta ----------------------------------------------------------

func TestSlotDelta_RoundTrip(t *testing.T) {
	db := memorydb.New()
	var addr tcommon.Address
	addr[20] = 0xAA
	var slot tcommon.Hash
	slot[0] = 0xDE
	slot[31] = 0xAD
	var preValue tcommon.Hash
	preValue[15] = 0xFF

	if _, ok := ReadSlotDelta(db, 100, addr, slot); ok {
		t.Fatal("expected absent before write")
	}
	if HasSlotDelta(db, 100, addr, slot) {
		t.Fatal("expected absent before write")
	}

	if err := WriteSlotDelta(db, 100, addr, slot, preValue); err != nil {
		t.Fatal(err)
	}
	if !HasSlotDelta(db, 100, addr, slot) {
		t.Fatal("expected present after write")
	}
	got, ok := ReadSlotDelta(db, 100, addr, slot)
	if !ok {
		t.Fatal("ReadSlotDelta returned !ok")
	}
	if got != preValue {
		t.Errorf("slot pre-value mismatch: got %x, want %x", got, preValue)
	}
}

func TestSlotDelta_ZeroPreValueRoundTrip(t *testing.T) {
	// An explicit zero pre-value must be distinguishable from "absent".
	db := memorydb.New()
	var addr tcommon.Address
	addr[20] = 0xBB
	var slot tcommon.Hash
	slot[5] = 0x42
	var preZero tcommon.Hash // all-zero pre-value

	if err := WriteSlotDelta(db, 50, addr, slot, preZero); err != nil {
		t.Fatal(err)
	}
	got, ok := ReadSlotDelta(db, 50, addr, slot)
	if !ok {
		t.Fatal("zero pre-value should be present but distinguishable: ok=false")
	}
	if got != preZero {
		t.Errorf("zero pre-value round-trip mismatch: got %x", got)
	}
}

func TestSlotDelta_Delete(t *testing.T) {
	db := memorydb.New()
	var addr tcommon.Address
	var slot tcommon.Hash
	slot[0] = 0x01
	var preValue tcommon.Hash
	preValue[31] = 0xCC

	if err := WriteSlotDelta(db, 5, addr, slot, preValue); err != nil {
		t.Fatal(err)
	}
	if err := DeleteSlotDelta(db, 5, addr, slot); err != nil {
		t.Fatal(err)
	}
	if _, ok := ReadSlotDelta(db, 5, addr, slot); ok {
		t.Fatal("expected absent after delete")
	}
}

// ---- StateHistoryMeta ---------------------------------------------------

func TestHistoryMeta_RoundTrip(t *testing.T) {
	db := memorydb.New()
	const blockNum uint64 = 999

	if got := ReadHistoryMeta(db, blockNum); got != nil {
		t.Fatalf("expected nil before write, got %v", got)
	}
	meta := &historypb.StateHistoryMeta{
		BlockHash: []byte("hash-bytes-32xxxxxxxxxxxxxxxxxxxx"),
		NumAddrs:  17,
		NumSlots:  42,
		SchemaVer: HistorySchemaVersion,
	}
	if err := WriteHistoryMeta(db, blockNum, meta); err != nil {
		t.Fatal(err)
	}
	got := ReadHistoryMeta(db, blockNum)
	if got == nil {
		t.Fatal("ReadHistoryMeta returned nil")
	}
	if got.BlockNum != blockNum {
		t.Errorf("BlockNum mismatch: %d vs %d", got.BlockNum, blockNum)
	}
	if string(got.BlockHash) != "hash-bytes-32xxxxxxxxxxxxxxxxxxxx" {
		t.Errorf("BlockHash mismatch: %q", got.BlockHash)
	}
	if got.NumAddrs != 17 || got.NumSlots != 42 {
		t.Errorf("counts mismatch: %+v", got)
	}
	if got.SchemaVer != HistorySchemaVersion {
		t.Errorf("SchemaVer mismatch: %d", got.SchemaVer)
	}
}

func TestHistoryMeta_Delete(t *testing.T) {
	db := memorydb.New()
	if err := WriteHistoryMeta(db, 1, &historypb.StateHistoryMeta{}); err != nil {
		t.Fatal(err)
	}
	if err := DeleteHistoryMeta(db, 1); err != nil {
		t.Fatal(err)
	}
	if got := ReadHistoryMeta(db, 1); got != nil {
		t.Fatalf("expected nil after delete, got %v", got)
	}
}

// ---- Inverse index (addr) -----------------------------------------------

func TestAddrInverse_RangeScanOrdered(t *testing.T) {
	db := memorydb.New()
	var addr tcommon.Address
	addr[0] = 0x41
	addr[10] = 0xAB

	// Insert out of order: 5, 1, 100, 7. Iterator must walk in ascending blockNum.
	blocks := []uint64{5, 1, 100, 7}
	for _, n := range blocks {
		if err := WriteAddrInverse(db, addr, n); err != nil {
			t.Fatal(err)
		}
	}

	it := IterateAddrInverse(db, addr)
	defer it.Release()
	var got []uint64
	for it.Next() {
		n, ok := AddrInverseBlockNum(it.Key())
		if !ok {
			t.Fatalf("AddrInverseBlockNum failed on key %x", it.Key())
		}
		got = append(got, n)
	}
	want := []uint64{1, 5, 7, 100}
	if len(got) != len(want) {
		t.Fatalf("got %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("ordering [%d]: got %d want %d", i, got[i], want[i])
		}
	}
}

func TestAddrInverse_DifferentAddrsDoNotInterleave(t *testing.T) {
	db := memorydb.New()
	var a, b tcommon.Address
	a[0] = 0x41
	a[20] = 0xAA
	b[0] = 0x41
	b[20] = 0xBB

	if err := WriteAddrInverse(db, a, 10); err != nil {
		t.Fatal(err)
	}
	if err := WriteAddrInverse(db, a, 20); err != nil {
		t.Fatal(err)
	}
	if err := WriteAddrInverse(db, b, 15); err != nil {
		t.Fatal(err)
	}

	collect := func(addr tcommon.Address) []uint64 {
		it := IterateAddrInverse(db, addr)
		defer it.Release()
		var out []uint64
		for it.Next() {
			n, _ := AddrInverseBlockNum(it.Key())
			out = append(out, n)
		}
		return out
	}

	if got := collect(a); len(got) != 2 || got[0] != 10 || got[1] != 20 {
		t.Errorf("addr A scan: got %v, want [10 20]", got)
	}
	if got := collect(b); len(got) != 1 || got[0] != 15 {
		t.Errorf("addr B scan: got %v, want [15]", got)
	}
}

func TestAddrInverse_Delete(t *testing.T) {
	db := memorydb.New()
	var addr tcommon.Address
	addr[0] = 0x41

	if err := WriteAddrInverse(db, addr, 7); err != nil {
		t.Fatal(err)
	}
	if !HasAddrInverse(db, addr, 7) {
		t.Fatal("expected present")
	}
	if err := DeleteAddrInverse(db, addr, 7); err != nil {
		t.Fatal(err)
	}
	if HasAddrInverse(db, addr, 7) {
		t.Fatal("expected absent after delete")
	}
}

func TestAddrInverse_BlockNumExtractorRejectsShortKey(t *testing.T) {
	short := []byte("sh-i-a-too-short")
	if _, ok := AddrInverseBlockNum(short); ok {
		t.Fatal("expected !ok on undersized key")
	}
}

// ---- Inverse index (slot) -----------------------------------------------

func TestSlotInverse_RangeScanOrdered(t *testing.T) {
	db := memorydb.New()
	var addr tcommon.Address
	addr[20] = 0xCC
	var slot tcommon.Hash
	slot[0] = 0xBE
	slot[31] = 0xEF

	for _, n := range []uint64{5, 1, 100, 7} {
		if err := WriteSlotInverse(db, addr, slot, n); err != nil {
			t.Fatal(err)
		}
	}

	it := IterateSlotInverse(db, addr, slot)
	defer it.Release()
	var got []uint64
	for it.Next() {
		n, ok := SlotInverseBlockNum(it.Key())
		if !ok {
			t.Fatalf("SlotInverseBlockNum failed on key %x", it.Key())
		}
		got = append(got, n)
	}
	want := []uint64{1, 5, 7, 100}
	if len(got) != len(want) {
		t.Fatalf("got %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("ordering [%d]: %d vs %d", i, got[i], want[i])
		}
	}
}

func TestSlotInverse_DifferentSlotsIsolated(t *testing.T) {
	db := memorydb.New()
	var addr tcommon.Address
	addr[20] = 0x11
	var slot1, slot2 tcommon.Hash
	slot1[0] = 0x01
	slot2[0] = 0x02

	if err := WriteSlotInverse(db, addr, slot1, 5); err != nil {
		t.Fatal(err)
	}
	if err := WriteSlotInverse(db, addr, slot2, 6); err != nil {
		t.Fatal(err)
	}

	collect := func(slot tcommon.Hash) []uint64 {
		it := IterateSlotInverse(db, addr, slot)
		defer it.Release()
		var out []uint64
		for it.Next() {
			n, _ := SlotInverseBlockNum(it.Key())
			out = append(out, n)
		}
		return out
	}

	if got := collect(slot1); len(got) != 1 || got[0] != 5 {
		t.Errorf("slot1 scan: %v want [5]", got)
	}
	if got := collect(slot2); len(got) != 1 || got[0] != 6 {
		t.Errorf("slot2 scan: %v want [6]", got)
	}
}

func TestSlotInverse_Delete(t *testing.T) {
	db := memorydb.New()
	var addr tcommon.Address
	var slot tcommon.Hash

	if err := WriteSlotInverse(db, addr, slot, 42); err != nil {
		t.Fatal(err)
	}
	if !HasSlotInverse(db, addr, slot, 42) {
		t.Fatal("expected present")
	}
	if err := DeleteSlotInverse(db, addr, slot, 42); err != nil {
		t.Fatal(err)
	}
	if HasSlotInverse(db, addr, slot, 42) {
		t.Fatal("expected absent after delete")
	}
}

// ---- HistoryConfig sentinel ---------------------------------------------

func TestHistoryConfig_RoundTrip(t *testing.T) {
	db := memorydb.New()

	if _, err := ReadHistoryConfig(db); !errors.Is(err, ErrHistoryConfigAbsent) {
		t.Fatalf("expected ErrHistoryConfigAbsent, got %v", err)
	}

	cfg := &historypb.HistoryConfig{
		Mode:        0, // full
		PruneWindow: 27000,
		FirstBlock:  100,
		LastBlock:   200,
		SchemaVer:   HistorySchemaVersion,
	}
	if err := WriteHistoryConfig(db, cfg); err != nil {
		t.Fatal(err)
	}
	got, err := ReadHistoryConfig(db)
	if err != nil {
		t.Fatalf("ReadHistoryConfig: %v", err)
	}
	if got.Mode != 0 || got.PruneWindow != 27000 || got.FirstBlock != 100 || got.LastBlock != 200 || got.SchemaVer != HistorySchemaVersion {
		t.Errorf("cfg round-trip mismatch: %+v", got)
	}
}

func TestHistoryConfig_Delete(t *testing.T) {
	db := memorydb.New()
	if err := WriteHistoryConfig(db, &historypb.HistoryConfig{Mode: 1}); err != nil {
		t.Fatal(err)
	}
	if err := DeleteHistoryConfig(db); err != nil {
		t.Fatal(err)
	}
	if _, err := ReadHistoryConfig(db); !errors.Is(err, ErrHistoryConfigAbsent) {
		t.Fatalf("expected absent after delete, got err=%v", err)
	}
}

// ---- Backfill resume cursor (sh-bf-cursor-) -------------------------------

func TestHistoryBackfillCursor_RoundTrip(t *testing.T) {
	db := memorydb.New()

	if _, ok := ReadHistoryBackfillCursor(db); ok {
		t.Fatal("expected no cursor on a fresh db")
	}

	if err := WriteHistoryBackfillCursor(db, 123456); err != nil {
		t.Fatal(err)
	}
	got, ok := ReadHistoryBackfillCursor(db)
	if !ok {
		t.Fatal("expected cursor present after write")
	}
	if got != 123456 {
		t.Errorf("cursor = %d, want 123456", got)
	}

	// Overwrite advances the cursor.
	if err := WriteHistoryBackfillCursor(db, 123457); err != nil {
		t.Fatal(err)
	}
	if got, _ := ReadHistoryBackfillCursor(db); got != 123457 {
		t.Errorf("cursor after overwrite = %d, want 123457", got)
	}

	// Cursor==0 is a valid value distinct from "absent" (the presence flag
	// disambiguates), though the backfill loop never persists 0.
	if err := WriteHistoryBackfillCursor(db, 0); err != nil {
		t.Fatal(err)
	}
	if got, ok := ReadHistoryBackfillCursor(db); !ok || got != 0 {
		t.Errorf("cursor=0 round-trip: got=%d ok=%v, want 0/true", got, ok)
	}
}

func TestHistoryBackfillCursor_Delete(t *testing.T) {
	db := memorydb.New()
	if err := WriteHistoryBackfillCursor(db, 42); err != nil {
		t.Fatal(err)
	}
	if err := DeleteHistoryBackfillCursor(db); err != nil {
		t.Fatal(err)
	}
	if _, ok := ReadHistoryBackfillCursor(db); ok {
		t.Fatal("expected no cursor after delete")
	}
}

// TestHistorySentinel_NoCollision verifies sh-cfg- does not collide with
// any structurally-possible sh-a- / sh-m- / sh-s- key. The sh-cfg- key is
// exactly the 7-byte byte slice "sh-cfg-"; the namespace segments differ
// from "sh-a-" / "sh-m-" / "sh-s-" / "sh-i-a-" / "sh-i-s-" so a buggy
// collision would have to fabricate a key that starts with "sh-cfg-".
// An sh-a- account key with blockNum=0 and addr=21 zero bytes is "sh-a-"
// || 8 zero bytes || 21 zero bytes (37 bytes total) — its first 5 bytes
// are "sh-a-", not "sh-cfg-". Verify both directions defensively so a
// future refactor that accidentally bridges the namespaces blows up here.
func TestHistorySentinel_NoCollisionWithDeltaKeys(t *testing.T) {
	db := memorydb.New()

	// Write the sentinel.
	if err := WriteHistoryConfig(db, &historypb.HistoryConfig{
		Mode:      0,
		SchemaVer: HistorySchemaVersion,
	}); err != nil {
		t.Fatal(err)
	}

	// 1. An sh-a- key at blockNum=0 with the zero address must NOT be
	//    confused with sh-cfg- by HasAccountDelta.
	var zeroAddr tcommon.Address
	if HasAccountDelta(db, 0, zeroAddr) {
		t.Fatal("sh-a- lookup hit the sh-cfg- sentinel — namespace bug")
	}

	// 2. Conversely, writing a sh-a- row must not satisfy ReadHistoryConfig.
	if err := WriteAccountDelta(db, 0, zeroAddr, &historypb.AccountDelta{ExistedPre: true}); err != nil {
		t.Fatal(err)
	}
	got, err := ReadHistoryConfig(db)
	if err != nil {
		t.Fatalf("ReadHistoryConfig after sh-a- write: %v", err)
	}
	if got.SchemaVer != HistorySchemaVersion {
		t.Errorf("HistoryConfig clobbered by sh-a- write: %+v", got)
	}

	// 3. The sh-m- block-zero meta and the sh-cfg- sentinel must coexist.
	if err := WriteHistoryMeta(db, 0, &historypb.StateHistoryMeta{
		BlockHash: []byte("zerohash"),
		SchemaVer: HistorySchemaVersion,
	}); err != nil {
		t.Fatal(err)
	}
	if meta := ReadHistoryMeta(db, 0); meta == nil || string(meta.BlockHash) != "zerohash" {
		t.Errorf("sh-m- block 0 row not readable after sh-cfg- write: %+v", meta)
	}
	got, err = ReadHistoryConfig(db)
	if err != nil {
		t.Fatalf("ReadHistoryConfig after sh-m- write: %v", err)
	}
	if got.SchemaVer != HistorySchemaVersion {
		t.Errorf("HistoryConfig clobbered by sh-m- write: %+v", got)
	}
}

// TestHistoryConfig_NilWriteRejected guards against accidental Put(nil)
// silently writing an empty config.
func TestHistoryConfig_NilWriteRejected(t *testing.T) {
	db := memorydb.New()
	if err := WriteHistoryConfig(db, nil); err == nil {
		t.Fatal("expected error writing nil HistoryConfig")
	}
}

// ---- Range delete by block prefix ---------------------------------------

// TestHistory_RangeDeleteByBlockPrefix locks in the helper signatures that
// Slice 5's pruner will drive: a per-block prefix scan over sh-a- / sh-s-
// and a direct sh-m- delete, applied for every block strictly below the
// cutoff. The body uses an iterate-collect-delete loop rather than a true
// range-delete; Slice 5 will swap that for a batched implementation. The
// inverse-index rows (sh-i-a-, sh-i-s-) live in a separate namespace and
// MUST survive this scan — Slice 5 will prune them via their own addr/slot
// scans.
func TestHistory_RangeDeleteByBlockPrefix(t *testing.T) {
	db := memorydb.New()
	var addrA, addrB tcommon.Address
	addrA[0], addrA[20] = 0x41, 0xAA
	addrB[0], addrB[20] = 0x41, 0xBB
	var slot1, slot2 tcommon.Hash
	slot1[31] = 0x01
	slot2[31] = 0x02

	const lo, hi, cutoff uint64 = 100, 110, 105

	// Populate: AccountDelta rows for two addrs, SlotDelta rows for addrA
	// at two slots, StateHistoryMeta rows, plus inverse-index rows that
	// MUST survive the prune.
	for n := lo; n <= hi; n++ {
		for _, a := range []tcommon.Address{addrA, addrB} {
			if err := WriteAccountDelta(db, n, a, &historypb.AccountDelta{ExistedPre: true}); err != nil {
				t.Fatalf("WriteAccountDelta(%d): %v", n, err)
			}
			if err := WriteAddrInverse(db, a, n); err != nil {
				t.Fatalf("WriteAddrInverse(%d): %v", n, err)
			}
		}
		for _, s := range []tcommon.Hash{slot1, slot2} {
			if err := WriteSlotDelta(db, n, addrA, s, tcommon.Hash{}); err != nil {
				t.Fatalf("WriteSlotDelta(%d): %v", n, err)
			}
			if err := WriteSlotInverse(db, addrA, s, n); err != nil {
				t.Fatalf("WriteSlotInverse(%d): %v", n, err)
			}
		}
		if err := WriteHistoryMeta(db, n, &historypb.StateHistoryMeta{SchemaVer: HistorySchemaVersion}); err != nil {
			t.Fatalf("WriteHistoryMeta(%d): %v", n, err)
		}
	}

	// Prune every row strictly below cutoff using the per-block prefix
	// helpers. Collect-then-delete: memorydb's iterator snapshots keys
	// but it's still cleaner to release before mutating.
	for n := lo; n < cutoff; n++ {
		collect := func(prefix []byte) [][]byte {
			it := db.NewIterator(prefix, nil)
			defer it.Release()
			var keys [][]byte
			for it.Next() {
				keys = append(keys, append([]byte{}, it.Key()...))
			}
			return keys
		}
		for _, k := range collect(historyAccountBlockPrefix(n)) {
			if err := db.Delete(k); err != nil {
				t.Fatalf("delete sh-a- key at block %d: %v", n, err)
			}
		}
		for _, k := range collect(historySlotBlockPrefix(n)) {
			if err := db.Delete(k); err != nil {
				t.Fatalf("delete sh-s- key at block %d: %v", n, err)
			}
		}
		if err := DeleteHistoryMeta(db, n); err != nil {
			t.Fatalf("DeleteHistoryMeta(%d): %v", n, err)
		}
	}

	// Rows below cutoff must be gone; rows at and above must remain.
	for n := lo; n <= hi; n++ {
		expectPresent := n >= cutoff
		for _, a := range []tcommon.Address{addrA, addrB} {
			if got := HasAccountDelta(db, n, a); got != expectPresent {
				t.Errorf("AccountDelta(block=%d, addr=%x) present=%v, want %v", n, a[:4], got, expectPresent)
			}
		}
		for _, s := range []tcommon.Hash{slot1, slot2} {
			if got := HasSlotDelta(db, n, addrA, s); got != expectPresent {
				t.Errorf("SlotDelta(block=%d, slot=%x) present=%v, want %v", n, s[:4], got, expectPresent)
			}
		}
		if got := ReadHistoryMeta(db, n) != nil; got != expectPresent {
			t.Errorf("HistoryMeta(block=%d) present=%v, want %v", n, got, expectPresent)
		}
	}

	// Inverse-index rows live in a separate namespace; this prune path
	// MUST leave them intact. Slice 5 will sweep them via per-addr /
	// per-(addr,slot) scans.
	for n := lo; n <= hi; n++ {
		for _, a := range []tcommon.Address{addrA, addrB} {
			if !HasAddrInverse(db, a, n) {
				t.Errorf("AddrInverse(addr=%x, block=%d) was incorrectly pruned", a[:4], n)
			}
		}
		for _, s := range []tcommon.Hash{slot1, slot2} {
			if !HasSlotInverse(db, addrA, s, n) {
				t.Errorf("SlotInverse(slot=%x, block=%d) was incorrectly pruned", s[:4], n)
			}
		}
	}
}

// ---- Slice-5 prune helpers ----------------------------------------------

// TestPruneHistoryBlockRange exercises the single-call range delete that
// the pruner drives once per pass. Plant five blocks of rows, ask for
// (lo=2, hi=3), and assert exactly blocks 2 and 3 disappear. The
// inverse-index rows (addr-first key layout) MUST survive — those live
// in a separate sweep.
func TestPruneHistoryBlockRange(t *testing.T) {
	db := memorydb.New()
	var addr tcommon.Address
	addr[0], addr[20] = 0x41, 0xAA
	var slot tcommon.Hash
	slot[31] = 0x01

	for n := uint64(1); n <= 5; n++ {
		if err := WriteHistoryMeta(db, n, &historypb.StateHistoryMeta{SchemaVer: HistorySchemaVersion}); err != nil {
			t.Fatalf("meta(%d): %v", n, err)
		}
		if err := WriteAccountDelta(db, n, addr, &historypb.AccountDelta{ExistedPre: true}); err != nil {
			t.Fatalf("acct(%d): %v", n, err)
		}
		if err := WriteSlotDelta(db, n, addr, slot, tcommon.Hash{}); err != nil {
			t.Fatalf("slot(%d): %v", n, err)
		}
		if err := WriteAddrInverse(db, addr, n); err != nil {
			t.Fatalf("addrinv(%d): %v", n, err)
		}
		if err := WriteSlotInverse(db, addr, slot, n); err != nil {
			t.Fatalf("slotinv(%d): %v", n, err)
		}
	}

	if err := PruneHistoryBlockRange(db, 2, 3); err != nil {
		t.Fatalf("PruneHistoryBlockRange: %v", err)
	}

	for n := uint64(1); n <= 5; n++ {
		expect := !(n == 2 || n == 3)
		if HasHistoryMeta(db, n) != expect {
			t.Errorf("HistoryMeta(%d) present=%v want=%v", n, HasHistoryMeta(db, n), expect)
		}
		if HasAccountDelta(db, n, addr) != expect {
			t.Errorf("AccountDelta(%d) present=%v want=%v", n, HasAccountDelta(db, n, addr), expect)
		}
		if HasSlotDelta(db, n, addr, slot) != expect {
			t.Errorf("SlotDelta(%d) present=%v want=%v", n, HasSlotDelta(db, n, addr, slot), expect)
		}
		// Inverse rows untouched.
		if !HasAddrInverse(db, addr, n) {
			t.Errorf("AddrInverse(%d) incorrectly pruned", n)
		}
		if !HasSlotInverse(db, addr, slot, n) {
			t.Errorf("SlotInverse(%d) incorrectly pruned", n)
		}
	}
}

// TestPruneHistoryBlockRange_EmptyRange asserts the no-op corner: lo > hi
// is a silent zero-op rather than an error.
func TestPruneHistoryBlockRange_EmptyRange(t *testing.T) {
	db := memorydb.New()
	if err := PruneHistoryBlockRange(db, 5, 3); err != nil {
		t.Fatalf("empty range: %v", err)
	}
}

// TestPruneAddrInverseBelow exercises the addr-first inverse-index
// sweep. Plant rows for two addrs at blocks {1, 5, 10}; ask for cutoff=6.
// Both addrs' rows at blocks {1, 5} must disappear; block 10 stays.
func TestPruneAddrInverseBelow(t *testing.T) {
	db := memorydb.New()
	var addrA, addrB tcommon.Address
	addrA[0], addrA[20] = 0x41, 0xAA
	addrB[0], addrB[20] = 0x41, 0xBB
	for _, a := range []tcommon.Address{addrA, addrB} {
		for _, n := range []uint64{1, 5, 10} {
			if err := WriteAddrInverse(db, a, n); err != nil {
				t.Fatalf("WriteAddrInverse(%x, %d): %v", a[:4], n, err)
			}
		}
	}

	deleted, more, err := PruneAddrInverseBelow(db, 6, 0 /* no cap */)
	if err != nil {
		t.Fatalf("PruneAddrInverseBelow: %v", err)
	}
	// 2 addrs × 2 rows below cutoff = 4 deletions.
	if deleted != 4 {
		t.Errorf("deleted=%d, want 4", deleted)
	}
	if more {
		t.Error("more=true for an uncapped scan over a fully-eligible range")
	}
	for _, a := range []tcommon.Address{addrA, addrB} {
		for _, n := range []uint64{1, 5} {
			if HasAddrInverse(db, a, n) {
				t.Errorf("sh-i-a-(%x, %d) survived prune", a[:4], n)
			}
		}
		if !HasAddrInverse(db, a, 10) {
			t.Errorf("sh-i-a-(%x, 10) wrongly deleted (above cutoff)", a[:4])
		}
	}
}

// TestPruneAddrInverseBelow_BatchLimit asserts batchLimit caps deletions
// and reports more=true so the caller can re-issue.
func TestPruneAddrInverseBelow_BatchLimit(t *testing.T) {
	db := memorydb.New()
	var addr tcommon.Address
	addr[0], addr[20] = 0x41, 0xAA
	// 50 rows below cutoff, all eligible.
	for n := uint64(1); n <= 50; n++ {
		if err := WriteAddrInverse(db, addr, n); err != nil {
			t.Fatalf("WriteAddrInverse(%d): %v", n, err)
		}
	}

	deleted, more, err := PruneAddrInverseBelow(db, 100, 10)
	if err != nil {
		t.Fatalf("PruneAddrInverseBelow: %v", err)
	}
	if deleted != 10 {
		t.Errorf("deleted=%d, want 10", deleted)
	}
	if !more {
		t.Error("more=false but only 10/50 rows pruned")
	}
}

// TestPruneSlotInverseBelow is the slot-key counterpart.
func TestPruneSlotInverseBelow(t *testing.T) {
	db := memorydb.New()
	var addr tcommon.Address
	addr[0], addr[20] = 0x41, 0xAA
	var slot1, slot2 tcommon.Hash
	slot1[31], slot2[31] = 0x01, 0x02
	for _, s := range []tcommon.Hash{slot1, slot2} {
		for _, n := range []uint64{2, 4, 8} {
			if err := WriteSlotInverse(db, addr, s, n); err != nil {
				t.Fatalf("WriteSlotInverse: %v", err)
			}
		}
	}

	deleted, _, err := PruneSlotInverseBelow(db, 5, 0)
	if err != nil {
		t.Fatalf("PruneSlotInverseBelow: %v", err)
	}
	// 2 slots × 2 rows below cutoff = 4.
	if deleted != 4 {
		t.Errorf("deleted=%d, want 4", deleted)
	}
	for _, s := range []tcommon.Hash{slot1, slot2} {
		if !HasSlotInverse(db, addr, s, 8) {
			t.Errorf("sh-i-s-(slot=%x, 8) wrongly pruned", s[:4])
		}
		if HasSlotInverse(db, addr, s, 4) {
			t.Errorf("sh-i-s-(slot=%x, 4) survived prune", s[:4])
		}
	}
}

// TestHistoryDiskSize sanity-checks the size accountant: an empty
// store reports 0, and a single AccountDelta increments the count.
func TestHistoryDiskSize(t *testing.T) {
	db := memorydb.New()
	if got := HistoryDiskSize(db); got != 0 {
		t.Errorf("empty store size=%d, want 0", got)
	}
	var addr tcommon.Address
	addr[0], addr[20] = 0x41, 0xAA
	if err := WriteAccountDelta(db, 7, addr, &historypb.AccountDelta{ExistedPre: true}); err != nil {
		t.Fatalf("WriteAccountDelta: %v", err)
	}
	if got := HistoryDiskSize(db); got == 0 {
		t.Error("post-write size still 0")
	}
}

// TestBytePlusOne pins the prefix-successor helper used to build the
// exclusive DeleteRange upper bound. The all-0xFF case must panic rather
// than return nil (a nil end key deletes to the end of the keyspace —
// silent catastrophe for a future binary-prefix caller).
func TestBytePlusOne(t *testing.T) {
	cases := []struct {
		name string
		in   []byte
		want []byte
	}{
		{"ascii prefix", []byte("sh-m-"), []byte("sh-m.")}, // '-' (0x2d) -> '.' (0x2e)
		{"interior 0xFF carry", []byte{0x73, 0x68, 0xFF}, []byte{0x73, 0x69}},
		{"trailing 0xFF only", []byte{0x01, 0xFF}, []byte{0x02}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := bytePlusOne(tc.in)
			if !bytes.Equal(got, tc.want) {
				t.Fatalf("bytePlusOne(%x) = %x, want %x", tc.in, got, tc.want)
			}
		})
	}

	t.Run("all 0xFF panics", func(t *testing.T) {
		defer func() {
			if r := recover(); r == nil {
				t.Fatal("expected panic on all-0xFF prefix, got none")
			}
		}()
		_ = bytePlusOne([]byte{0xFF, 0xFF, 0xFF})
	})
}
