package state

import (
	"bytes"
	"testing"

	tcommon "github.com/tronprotocol/go-tron/common"
)

// TestAccountNameIndexRoundTrip exercises write/read/has/delete and confirms the
// name index is case-sensitive (java-tron AccountIndexStore stores name bytes
// verbatim, unlike the lower-cased id index).
func TestAccountNameIndexRoundTrip(t *testing.T) {
	sdb := newTestStateDB(t)
	owner := testAddr(0xaa)

	if sdb.HasAccountNameIndex([]byte("alice")) {
		t.Fatal("absent name reports present")
	}
	if got := sdb.ReadAccountNameIndex([]byte("alice")); got != nil {
		t.Fatalf("absent: got %x, want nil", got)
	}

	if err := sdb.WriteAccountNameIndex([]byte("alice"), owner); err != nil {
		t.Fatal(err)
	}
	if !sdb.HasAccountNameIndex([]byte("alice")) {
		t.Fatal("after write: Has returned false")
	}
	if got := sdb.ReadAccountNameIndex([]byte("alice")); !bytes.Equal(got, owner.Bytes()) {
		t.Fatalf("read: got %x, want %x", got, owner.Bytes())
	}
	// Case-sensitive: "Alice" is a distinct key from "alice".
	if sdb.HasAccountNameIndex([]byte("Alice")) {
		t.Fatal("name index must be case-sensitive")
	}

	if err := sdb.DeleteAccountNameIndex([]byte("alice")); err != nil {
		t.Fatal(err)
	}
	if sdb.HasAccountNameIndex([]byte("alice")) {
		t.Fatal("after delete: Has returned true")
	}
}

// TestAccountIdIndexCaseInsensitive ports the java-parity case-insensitivity
// contract from the old flat accessor: ids are lower-cased at the store
// boundary, so any-case lookups resolve the same entry.
func TestAccountIdIndexCaseInsensitive(t *testing.T) {
	sdb := newTestStateDB(t)
	owner := testAddr(0xbb)

	if err := sdb.WriteAccountIdIndex([]byte("AliceID1"), owner); err != nil {
		t.Fatal(err)
	}
	if !sdb.HasAccountIdIndex([]byte("aliceid1")) {
		t.Fatal("lower-case lookup missing")
	}
	if got := sdb.ReadAccountIdIndex([]byte("ALICEID1")); !bytes.Equal(got, owner.Bytes()) {
		t.Fatalf("upper-case lookup: got %x, want %x", got, owner.Bytes())
	}
	if err := sdb.DeleteAccountIdIndex([]byte("aLiCeId1")); err != nil {
		t.Fatal(err)
	}
	if sdb.HasAccountIdIndex([]byte("AliceID1")) {
		t.Fatal("after case-insensitive delete: Has returned true")
	}
}

// TestAccountIndexTagDisambiguation confirms the name and id key-spaces never
// collide inside the shared SystemAccountIndex domain even for identical bytes.
func TestAccountIndexTagDisambiguation(t *testing.T) {
	sdb := newTestStateDB(t)
	nameOwner := testAddr(0x11)
	idOwner := testAddr(0x22)

	// Same bytes "shared" written to both key-spaces. The id is stored
	// lower-cased, so use an already-lower-case value to make the bytes equal.
	if err := sdb.WriteAccountNameIndex([]byte("shared"), nameOwner); err != nil {
		t.Fatal(err)
	}
	if err := sdb.WriteAccountIdIndex([]byte("shared"), idOwner); err != nil {
		t.Fatal(err)
	}
	if got := sdb.ReadAccountNameIndex([]byte("shared")); !bytes.Equal(got, nameOwner.Bytes()) {
		t.Fatalf("name lookup polluted by id: got %x, want %x", got, nameOwner.Bytes())
	}
	if got := sdb.ReadAccountIdIndex([]byte("shared")); !bytes.Equal(got, idOwner.Bytes()) {
		t.Fatalf("id lookup polluted by name: got %x, want %x", got, idOwner.Bytes())
	}
}

// TestAccountIndexAnchorAndRewind is the state-layer gate for this phase:
// rooting an account-index change moves the state root (anchor), and reopening
// an old root recovers the old name+id mappings (rewind). Mirrors applyBlock's
// per-block parent-root open with a fresh StateDB per commit.
func TestAccountIndexAnchorAndRewind(t *testing.T) {
	sdb := newTestStateDB(t)
	ownerA := testAddr(0x01)
	ownerB := testAddr(0x02)

	// R1: name "alice"->A, id "userone1"->A.
	if err := sdb.WriteAccountNameIndex([]byte("alice"), ownerA); err != nil {
		t.Fatal(err)
	}
	if err := sdb.WriteAccountIdIndex([]byte("userone1"), ownerA); err != nil {
		t.Fatal(err)
	}
	r1, err := sdb.Commit()
	if err != nil {
		t.Fatalf("commit R1: %v", err)
	}

	// R2 built on R1 via a fresh StateDB: add name "bob"->B and id "usertwo2"->B.
	sdb2, err := New(r1, sdb.db)
	if err != nil {
		t.Fatal(err)
	}
	if err := sdb2.WriteAccountNameIndex([]byte("bob"), ownerB); err != nil {
		t.Fatal(err)
	}
	if err := sdb2.WriteAccountIdIndex([]byte("usertwo2"), ownerB); err != nil {
		t.Fatal(err)
	}
	r2, err := sdb2.Commit()
	if err != nil {
		t.Fatalf("commit R2: %v", err)
	}

	if r1 == r2 {
		t.Fatal("anchor: account-index change did not move the state root")
	}

	// Rewind: R1 only has alice/userone1; R2 has both pairs.
	atR1, err := New(r1, sdb.db)
	if err != nil {
		t.Fatal(err)
	}
	if got := atR1.ReadAccountNameIndex([]byte("alice")); !bytes.Equal(got, ownerA.Bytes()) {
		t.Fatalf("rewind R1 name alice: got %x", got)
	}
	if atR1.HasAccountNameIndex([]byte("bob")) {
		t.Fatal("rewind R1 must not see bob")
	}
	if atR1.HasAccountIdIndex([]byte("usertwo2")) {
		t.Fatal("rewind R1 must not see usertwo2")
	}

	atR2, err := New(r2, sdb.db)
	if err != nil {
		t.Fatal(err)
	}
	if got := atR2.ReadAccountNameIndex([]byte("bob")); !bytes.Equal(got, ownerB.Bytes()) {
		t.Fatalf("R2 name bob: got %x", got)
	}
	if got := atR2.ReadAccountIdIndex([]byte("userone1")); !bytes.Equal(got, ownerA.Bytes()) {
		t.Fatalf("R2 id userone1: got %x", got)
	}
	if got := atR2.ReadAccountIdIndex([]byte("usertwo2")); !bytes.Equal(got, ownerB.Bytes()) {
		t.Fatalf("R2 id usertwo2: got %x", got)
	}
}

// TestAccountIndexValueIsAddress confirms the stored value is the raw 21-byte
// owner address, matching the prior flat on-disk format.
func TestAccountIndexValueIsAddress(t *testing.T) {
	sdb := newTestStateDB(t)
	owner := testAddr(0x7e)
	if err := sdb.WriteAccountNameIndex([]byte("name"), owner); err != nil {
		t.Fatal(err)
	}
	got := sdb.ReadAccountNameIndex([]byte("name"))
	if len(got) != tcommon.AddressLength {
		t.Fatalf("value length: got %d, want %d", len(got), tcommon.AddressLength)
	}
	if tcommon.BytesToAddress(got) != owner {
		t.Fatalf("value decode: got %x, want %x", got, owner.Bytes())
	}
}
