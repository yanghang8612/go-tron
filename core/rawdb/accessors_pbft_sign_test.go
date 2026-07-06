package rawdb

import (
	"bytes"
	"errors"
	"strings"
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
	if got, ok, err := ReadBlockSignDataStrict(db, 100); got != nil || ok || err != nil {
		t.Fatalf("strict absent: got %v ok %v err %v, want nil false nil", got, ok, err)
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
	strict, ok, err := ReadBlockSignDataStrict(db, 100)
	if err != nil || !ok || strict == nil || !bytes.Equal(strict.Data, want.Data) || len(strict.Signature) != 3 {
		t.Fatalf("strict read: %+v ok %v err %v", strict, ok, err)
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
	if got, ok, err := ReadSrSignDataStrict(db, 42); got != nil || ok || err != nil {
		t.Fatalf("strict sr absent: got %v ok %v err %v, want nil false nil", got, ok, err)
	}
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
	strict, ok, err := ReadSrSignDataStrict(db, 42)
	if err != nil || !ok || strict == nil || !bytes.Equal(strict.Data, want.Data) || len(strict.Signature) != 1 {
		t.Fatalf("strict sr read: %+v ok %v err %v", strict, ok, err)
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

func TestLatestPbftBlockNumStrict(t *testing.T) {
	db := rawdb.NewMemoryDatabase()
	if got := ReadLatestPbftBlockNum(db); got != -1 {
		t.Fatalf("compat absent latest = %d, want -1", got)
	}
	if got, ok, err := ReadLatestPbftBlockNumStrict(db); err != nil || ok || got != -1 {
		t.Fatalf("strict absent latest = %d/%v/%v, want -1/false/nil", got, ok, err)
	}
	WriteLatestPbftBlockNum(db, 123)
	if got, ok, err := ReadLatestPbftBlockNumStrict(db); err != nil || !ok || got != 123 {
		t.Fatalf("strict latest = %d/%v/%v, want 123/true/nil", got, ok, err)
	}
	if _, ok, err := ReadLatestPbftBlockNumStrict(failingStateDomainReader{reader: db, hasErr: errors.New("has boom")}); err == nil || ok || !strings.Contains(err.Error(), "presence") {
		t.Fatalf("strict latest has error ok=%v err=%v, want presence error", ok, err)
	}
	if _, ok, err := ReadLatestPbftBlockNumStrict(failingStateDomainReader{reader: db, getErr: errors.New("get boom")}); err == nil || ok || !strings.Contains(err.Error(), "get boom") {
		t.Fatalf("strict latest get error ok=%v err=%v, want get error", ok, err)
	}
	if err := db.Put(latestPbftBlockNumKey, []byte{0x01}); err != nil {
		t.Fatal(err)
	}
	if got := ReadLatestPbftBlockNum(db); got != -1 {
		t.Fatalf("compat malformed latest = %d, want -1", got)
	}
	if got, ok, err := ReadLatestPbftBlockNumStrict(db); err == nil || ok || got != -1 || !strings.Contains(err.Error(), "length 1, want 8") {
		t.Fatalf("strict malformed latest = %d/%v/%v, want length error", got, ok, err)
	}
}

func TestPbftSignDataStrictSurfacesStorageErrors(t *testing.T) {
	db := rawdb.NewMemoryDatabase()
	block := &corepb.PBFTCommitResult{Data: []byte("block-data")}
	sr := &corepb.PBFTCommitResult{Data: []byte("sr-data")}
	if err := WriteBlockSignData(db, 7, block); err != nil {
		t.Fatal(err)
	}
	if err := WriteSrSignData(db, 9, sr); err != nil {
		t.Fatal(err)
	}

	if ok, err := HasBlockSignDataStrict(failingStateDomainReader{reader: db, hasErr: errors.New("has boom")}, 7); err == nil || ok || !strings.Contains(err.Error(), "presence") {
		t.Fatalf("HasBlockSignDataStrict has error = %v/%v, want presence error", ok, err)
	}
	if _, ok, err := ReadBlockSignDataStrict(failingStateDomainReader{reader: db, hasErr: errors.New("has boom")}, 7); err == nil || ok || !strings.Contains(err.Error(), "presence") {
		t.Fatalf("ReadBlockSignDataStrict has error ok=%v err=%v, want presence error", ok, err)
	}
	if _, ok, err := ReadBlockSignDataStrict(failingStateDomainReader{reader: db, getErr: errors.New("get boom")}, 7); err == nil || ok || !strings.Contains(err.Error(), "get boom") {
		t.Fatalf("ReadBlockSignDataStrict get error ok=%v err=%v, want get error", ok, err)
	}
	if _, ok, err := ReadSrSignDataStrict(failingStateDomainReader{reader: db, hasErr: errors.New("has boom")}, 9); err == nil || ok || !strings.Contains(err.Error(), "presence") {
		t.Fatalf("ReadSrSignDataStrict has error ok=%v err=%v, want presence error", ok, err)
	}
	if _, ok, err := ReadSrSignDataStrict(failingStateDomainReader{reader: db, getErr: errors.New("get boom")}, 9); err == nil || ok || !strings.Contains(err.Error(), "get boom") {
		t.Fatalf("ReadSrSignDataStrict get error ok=%v err=%v, want get error", ok, err)
	}
	if ReadBlockSignData(failingStateDomainReader{reader: db, getErr: errors.New("get boom")}, 7) != nil {
		t.Fatal("compat ReadBlockSignData should keep nil default on storage error")
	}
	if ReadSrSignData(failingStateDomainReader{reader: db, getErr: errors.New("get boom")}, 9) != nil {
		t.Fatal("compat ReadSrSignData should keep nil default on storage error")
	}
}

func TestPbftSignDataStrictSurfacesCorruptPayloads(t *testing.T) {
	db := rawdb.NewMemoryDatabase()
	if err := db.Put(pbftBlockSignKey(11), []byte{0xff}); err != nil {
		t.Fatal(err)
	}
	if got := ReadBlockSignData(db, 11); got != nil {
		t.Fatalf("compat corrupt block sign = %v, want nil", got)
	}
	if got, ok, err := ReadBlockSignDataStrict(db, 11); err == nil || !ok || got != nil || !strings.Contains(err.Error(), "decode pbft block sign data") {
		t.Fatalf("strict corrupt block sign = %v/%v/%v, want decode error", got, ok, err)
	}
	if err := db.Put(pbftSrSignKey(12), []byte{0xff}); err != nil {
		t.Fatal(err)
	}
	if got := ReadSrSignData(db, 12); got != nil {
		t.Fatalf("compat corrupt sr sign = %v, want nil", got)
	}
	if got, ok, err := ReadSrSignDataStrict(db, 12); err == nil || !ok || got != nil || !strings.Contains(err.Error(), "decode pbft sr sign data") {
		t.Fatalf("strict corrupt sr sign = %v/%v/%v, want decode error", got, ok, err)
	}
	if err := db.Put(pbftBlockSignKey(13), nil); err != nil {
		t.Fatal(err)
	}
	if got := ReadBlockSignData(db, 13); got != nil {
		t.Fatalf("compat empty block sign = %v, want nil", got)
	}
	if got, ok, err := ReadBlockSignDataStrict(db, 13); err != nil || !ok || got == nil {
		t.Fatalf("strict empty block sign = %v/%v/%v, want empty result/true/nil", got, ok, err)
	}
}
