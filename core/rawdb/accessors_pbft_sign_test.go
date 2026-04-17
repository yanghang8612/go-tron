package rawdb

import (
	"bytes"
	"testing"

	"github.com/ethereum/go-ethereum/core/rawdb"
	corepb "github.com/tronprotocol/go-tron/proto/core"
)

func TestPbftSignData_BlockRoundTrip(t *testing.T) {
	db := rawdb.NewMemoryDatabase()

	if ReadBlockSignData(db, 100) != nil {
		t.Fatal("absent: read returned non-nil")
	}
	if HasBlockSignData(db, 100) {
		t.Fatal("absent: Has returned true")
	}

	want := &corepb.PBFTCommitResult{
		Data:      []byte("block-100-hash"),
		Signature: [][]byte{[]byte("sig1"), []byte("sig2"), []byte("sig3")},
	}
	if err := WriteBlockSignData(db, 100, want); err != nil {
		t.Fatal(err)
	}
	if !HasBlockSignData(db, 100) {
		t.Fatal("after write: Has returned false")
	}
	got := ReadBlockSignData(db, 100)
	if got == nil {
		t.Fatal("read: nil after write")
	}
	if !bytes.Equal(got.Data, want.Data) {
		t.Fatalf("data: got %q, want %q", got.Data, want.Data)
	}
	if len(got.Signature) != 3 {
		t.Fatalf("sig count: %d", len(got.Signature))
	}
	if err := DeleteBlockSignData(db, 100); err != nil {
		t.Fatal(err)
	}
	if HasBlockSignData(db, 100) {
		t.Fatal("after delete: Has returned true")
	}
}

func TestPbftSignData_SrRoundTrip(t *testing.T) {
	db := rawdb.NewMemoryDatabase()
	want := &corepb.PBFTCommitResult{
		Data:      []byte("sr-list-epoch-42"),
		Signature: [][]byte{[]byte("sig")},
	}
	if err := WriteSrSignData(db, 42, want); err != nil {
		t.Fatal(err)
	}
	got := ReadSrSignData(db, 42)
	if got == nil || !bytes.Equal(got.Data, want.Data) || len(got.Signature) != 1 {
		t.Fatalf("sr roundtrip: %+v", got)
	}
	if err := DeleteSrSignData(db, 42); err != nil {
		t.Fatal(err)
	}
	if ReadSrSignData(db, 42) != nil {
		t.Fatal("after delete: still present")
	}
}

func TestPbftSignData_BlockAndSrDisjoint(t *testing.T) {
	// Writing block 42 must not clobber SR 42.
	db := rawdb.NewMemoryDatabase()
	b := &corepb.PBFTCommitResult{Data: []byte("block-data")}
	s := &corepb.PBFTCommitResult{Data: []byte("sr-data")}
	_ = WriteBlockSignData(db, 42, b)
	_ = WriteSrSignData(db, 42, s)
	if bytes.Equal(ReadBlockSignData(db, 42).Data, ReadSrSignData(db, 42).Data) {
		t.Fatal("block/sr keys collided")
	}
}

func TestPbftSignData_KeysMatchJavaLongToString(t *testing.T) {
	// Java's Long.toString(123) → "123"; the go key builder must produce
	// "psd-BLOCK123". Verify via the constructed key bytes so the
	// wire-compat property is tested (not just the accessor behaviour).
	k := pbftBlockSignKey(123)
	want := []byte("psd-BLOCK123")
	if !bytes.Equal(k, want) {
		t.Fatalf("block key: got %q, want %q", k, want)
	}
	k = pbftSrSignKey(9876543210)
	want = []byte("psd-SRL9876543210")
	if !bytes.Equal(k, want) {
		t.Fatalf("sr key: got %q, want %q", k, want)
	}
}

func TestPbftSignData_RejectsNil(t *testing.T) {
	db := rawdb.NewMemoryDatabase()
	if err := WriteBlockSignData(db, 1, nil); err == nil {
		t.Fatal("nil block sign data must error")
	}
	if err := WriteSrSignData(db, 1, nil); err == nil {
		t.Fatal("nil sr sign data must error")
	}
}
