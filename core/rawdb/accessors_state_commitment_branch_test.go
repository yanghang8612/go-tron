package rawdb

import (
	"bytes"
	"errors"
	"strings"
	"testing"

	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/tronprotocol/go-tron/common"
)

func TestCommitmentBranchRoundTrip(t *testing.T) {
	db := NewMemoryDatabase()

	prefixes := [][]byte{
		{0x0A},
		{0x0A, 0x0B},
		{0xFF, 0x01, 0x02},
	}
	values := [][]byte{
		{0x01, 0x02, 0x03},
		{0xAA, 0xBB},
		{0xCC},
	}

	// Write 3 branches.
	for i, pfx := range prefixes {
		if err := WriteCommitmentBranch(db, pfx, values[i]); err != nil {
			t.Fatalf("WriteCommitmentBranch[%d]: %v", i, err)
		}
	}

	// Read each back and confirm equal.
	for i, pfx := range prefixes {
		got, ok, err := ReadCommitmentBranch(db, pfx)
		if err != nil {
			t.Fatalf("ReadCommitmentBranch[%d]: %v", i, err)
		}
		if !ok {
			t.Fatalf("ReadCommitmentBranch[%d]: not found", i)
		}
		if !bytes.Equal(got, values[i]) {
			t.Fatalf("ReadCommitmentBranch[%d]: got %x want %x", i, got, values[i])
		}
	}

	// Iterate and collect all 3.
	collected := make(map[string][]byte)
	if err := IterateCommitmentBranches(db, func(prefix, encoded []byte) (bool, error) {
		collected[string(prefix)] = append([]byte(nil), encoded...)
		return true, nil
	}); err != nil {
		t.Fatalf("IterateCommitmentBranches: %v", err)
	}
	if len(collected) != 3 {
		t.Fatalf("iterate: got %d entries, want 3", len(collected))
	}
	for i, pfx := range prefixes {
		got, ok := collected[string(pfx)]
		if !ok {
			t.Fatalf("iterate: prefix[%d] not found", i)
		}
		if !bytes.Equal(got, values[i]) {
			t.Fatalf("iterate: prefix[%d]: got %x want %x", i, got, values[i])
		}
	}

	// Delete one and confirm gone.
	if err := DeleteCommitmentBranch(db, prefixes[1]); err != nil {
		t.Fatalf("DeleteCommitmentBranch: %v", err)
	}
	_, ok, err := ReadCommitmentBranch(db, prefixes[1])
	if err != nil {
		t.Fatalf("read deleted: %v", err)
	}
	if ok {
		t.Fatal("read deleted: expected not found")
	}

	// Engine state row.
	engineData := []byte{0xDE, 0xAD, 0xBE, 0xEF}
	if err := WriteCommitmentEngineState(db, engineData); err != nil {
		t.Fatalf("WriteCommitmentEngineState: %v", err)
	}
	gotEngine, ok, err := ReadCommitmentEngineState(db)
	if err != nil {
		t.Fatalf("ReadCommitmentEngineState: %v", err)
	}
	if !ok {
		t.Fatal("ReadCommitmentEngineState: not found")
	}
	if !bytes.Equal(gotEngine, engineData) {
		t.Fatalf("ReadCommitmentEngineState: got %x want %x", gotEngine, engineData)
	}
}

func TestCommitmentBranchMissing(t *testing.T) {
	db := NewMemoryDatabase()
	_, ok, err := ReadCommitmentBranch(db, []byte{0x01})
	if err != nil {
		t.Fatalf("missing read: %v", err)
	}
	if ok {
		t.Fatal("missing read: expected not found")
	}

	_, ok, err = ReadCommitmentEngineState(db)
	if err != nil {
		t.Fatalf("missing engine state read: %v", err)
	}
	if ok {
		t.Fatal("missing engine state read: expected not found")
	}
}

func TestCommitmentBranchDeltaKeyspacesAreIsolated(t *testing.T) {
	db := NewMemoryDatabase()
	legacy := LegacyCommitmentBranchKeyspace()
	gen7, err := NewCommitmentBranchDeltaKeyspace(7)
	if err != nil {
		t.Fatal(err)
	}
	gen8, err := NewCommitmentBranchDeltaKeyspace(8)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := NewCommitmentBranchDeltaKeyspace(0); err == nil {
		t.Fatal("zero delta generation accepted")
	}

	prefix := []byte{0x0a, 0x0b}
	if err := legacy.Write(db, prefix, []byte("legacy")); err != nil {
		t.Fatal(err)
	}
	if err := gen7.Write(db, prefix, []byte("seven")); err != nil {
		t.Fatal(err)
	}
	// An existing zero-length value is the layered-store tombstone. It must be
	// distinguishable from a physically absent row.
	if err := gen8.Write(db, prefix, nil); err != nil {
		t.Fatal(err)
	}

	for _, test := range []struct {
		name  string
		space CommitmentBranchKeyspace
		want  []byte
	}{
		{name: "legacy", space: legacy, want: []byte("legacy")},
		{name: "generation 7", space: gen7, want: []byte("seven")},
		{name: "generation 8 tombstone", space: gen8, want: []byte{}},
	} {
		got, ok, err := test.space.ReadNoCopy(db, prefix)
		if err != nil || !ok || !bytes.Equal(got, test.want) {
			t.Fatalf("%s read = %q ok=%v err=%v, want %q,true,nil", test.name, got, ok, err, test.want)
		}
	}

	seen := 0
	if err := gen7.Iterate(db, func(gotPrefix, encoded []byte) (bool, error) {
		seen++
		if !bytes.Equal(gotPrefix, prefix) || !bytes.Equal(encoded, []byte("seven")) {
			t.Fatalf("iterate = %x/%q", gotPrefix, encoded)
		}
		return true, nil
	}); err != nil {
		t.Fatal(err)
	}
	if seen != 1 {
		t.Fatalf("generation 7 count = %d, want 1", seen)
	}
	if err := gen7.DeleteAll(db); err != nil {
		t.Fatal(err)
	}
	if _, ok, err := gen7.ReadNoCopy(db, prefix); err != nil || ok {
		t.Fatalf("deleted generation read ok=%v err=%v", ok, err)
	}
	if got, ok, err := legacy.ReadNoCopy(db, prefix); err != nil || !ok || !bytes.Equal(got, []byte("legacy")) {
		t.Fatalf("legacy after generation delete = %q ok=%v err=%v", got, ok, err)
	}
	if got, ok, err := gen8.ReadNoCopy(db, prefix); err != nil || !ok || len(got) != 0 {
		t.Fatalf("generation 8 after generation delete = %q ok=%v err=%v", got, ok, err)
	}
}

func TestCommitmentBranchBaseRoundTripAndValidation(t *testing.T) {
	db := NewMemoryDatabase()
	want := CommitmentBranchBase{
		Generation:    42,
		SnapshotTxNum: 9001,
		Root:          common.HexToHash("0x0123456789abcdef"),
	}
	if err := WriteCommitmentBranchBase(db, want); err != nil {
		t.Fatal(err)
	}
	got, ok, err := ReadCommitmentBranchBase(db)
	if err != nil || !ok || got != want {
		t.Fatalf("read base = %+v ok=%v err=%v, want %+v,true,nil", got, ok, err, want)
	}
	if err := DeleteCommitmentBranchBase(db); err != nil {
		t.Fatal(err)
	}
	if _, ok, err := ReadCommitmentBranchBase(db); err != nil || ok {
		t.Fatalf("read deleted base ok=%v err=%v", ok, err)
	}

	if _, err := EncodeCommitmentBranchBase(CommitmentBranchBase{}); err == nil {
		t.Fatal("zero base generation accepted")
	}
	valid, err := EncodeCommitmentBranchBase(want)
	if err != nil {
		t.Fatal(err)
	}
	for _, encoded := range [][]byte{
		nil,
		valid[:len(valid)-1],
		append([]byte{commitmentBranchBaseVersion + 1}, valid[1:]...),
		append([]byte{commitmentBranchBaseVersion}, make([]byte, len(valid)-1)...),
	} {
		if _, err := DecodeCommitmentBranchBase(encoded); err == nil {
			t.Fatalf("invalid base %x accepted", encoded)
		}
	}
}

func TestDeleteCommitmentBranchesLeavesOtherKeyspaces(t *testing.T) {
	db := NewMemoryDatabase()
	if err := WriteCommitmentBranch(db, nil, []byte("root")); err != nil {
		t.Fatal(err)
	}
	if err := WriteCommitmentBranch(db, []byte{0x0a}, []byte("branch")); err != nil {
		t.Fatal(err)
	}
	if err := db.Put([]byte("unrelated"), []byte("keep")); err != nil {
		t.Fatal(err)
	}
	if err := DeleteCommitmentBranches(db); err != nil {
		t.Fatalf("delete commitment branches: %v", err)
	}

	count := 0
	if err := IterateCommitmentBranches(db, func(_, _ []byte) (bool, error) {
		count++
		return true, nil
	}); err != nil {
		t.Fatalf("iterate commitment branches: %v", err)
	}
	if count != 0 {
		t.Fatalf("commitment branch count = %d, want 0", count)
	}
	value, err := db.Get([]byte("unrelated"))
	if err != nil || !bytes.Equal(value, []byte("keep")) {
		t.Fatalf("unrelated value = %q, err = %v", value, err)
	}
}

func TestDeleteCommitmentBranchesFallsBackToBoundedPointScan(t *testing.T) {
	db := &noRangeCommitmentBranchStore{db: NewMemoryDatabase()}
	for i := 0; i < resetScanBatch*2+1; i++ {
		prefix := []byte{byte(i >> 8), byte(i)}
		if err := WriteCommitmentBranch(db, prefix, []byte{byte(i)}); err != nil {
			t.Fatalf("write branch %d: %v", i, err)
		}
	}
	if err := DeleteCommitmentBranches(db); err != nil {
		t.Fatalf("delete commitment branches: %v", err)
	}
	count := 0
	if err := IterateCommitmentBranches(db, func(_, _ []byte) (bool, error) {
		count++
		return true, nil
	}); err != nil {
		t.Fatalf("iterate commitment branches: %v", err)
	}
	if count != 0 {
		t.Fatalf("commitment branch count = %d, want 0", count)
	}
}

// noRangeCommitmentBranchStore intentionally exposes only the interfaces the
// fallback needs; embedding KeyValueStore would promote DeleteRange and bypass
// the bounded point-scan path.
type noRangeCommitmentBranchStore struct {
	db ethdb.KeyValueStore
}

func (s *noRangeCommitmentBranchStore) Put(key, value []byte) error {
	return s.db.Put(key, value)
}

func (s *noRangeCommitmentBranchStore) Delete(key []byte) error {
	return s.db.Delete(key)
}

func (s *noRangeCommitmentBranchStore) NewIterator(prefix, start []byte) ethdb.Iterator {
	return s.db.NewIterator(prefix, start)
}

func TestCommitmentBranchSurfacesStorageErrors(t *testing.T) {
	db := NewMemoryDatabase()
	prefix := []byte{0x01, 0x02}
	if err := WriteCommitmentBranch(db, prefix, []byte("branch")); err != nil {
		t.Fatalf("write branch: %v", err)
	}

	_, ok, err := ReadCommitmentBranch(failingCommitmentReader{
		reader: db,
		getErr: errors.New("get boom"),
	}, prefix)
	if err == nil || ok || !strings.Contains(err.Error(), "get boom") {
		t.Fatalf("plain get error ok=%v err=%v, want get error", ok, err)
	}

	_, ok, err = ReadCommitmentBranchNoCopy(failingNoCopyCommitmentReader{
		failingCommitmentReader: failingCommitmentReader{
			reader: db,
			getErr: errors.New("nocopy boom"),
		},
	}, prefix)
	if err == nil || ok || !strings.Contains(err.Error(), "nocopy boom") {
		t.Fatalf("nocopy get error ok=%v err=%v, want get error", ok, err)
	}

	_, ok, err = ReadCommitmentBranch(failingCommitmentReader{
		reader: db,
		getErr: errors.New("get boom"),
		hasErr: errors.New("has boom"),
	}, prefix)
	if err == nil || ok || !strings.Contains(err.Error(), "presence after get error") {
		t.Fatalf("presence error ok=%v err=%v, want presence error", ok, err)
	}
}

func TestCommitmentEngineStateSurfacesStorageErrors(t *testing.T) {
	db := NewMemoryDatabase()
	if err := WriteCommitmentEngineState(db, []byte("engine")); err != nil {
		t.Fatalf("write engine state: %v", err)
	}

	if got, ok, err := ReadCommitmentEngineState(failingCommitmentReader{reader: db, hasErr: errors.New("has boom")}); err == nil || ok || got != nil || !strings.Contains(err.Error(), "presence") {
		t.Fatalf("has error = %x ok=%v err=%v, want presence error", got, ok, err)
	}
	if got, ok, err := ReadCommitmentEngineState(failingCommitmentReader{reader: db, getErr: errors.New("get boom")}); err == nil || ok || got != nil || !strings.Contains(err.Error(), "get boom") {
		t.Fatalf("get error = %x ok=%v err=%v, want get error", got, ok, err)
	}
}

type failingCommitmentReader struct {
	reader ethdb.KeyValueReader
	hasErr error
	getErr error
}

func (r failingCommitmentReader) Has(key []byte) (bool, error) {
	if r.hasErr != nil {
		return false, r.hasErr
	}
	return r.reader.Has(key)
}

func (r failingCommitmentReader) Get(key []byte) ([]byte, error) {
	if r.getErr != nil {
		return nil, r.getErr
	}
	return r.reader.Get(key)
}

type failingNoCopyCommitmentReader struct {
	failingCommitmentReader
}

func (r failingNoCopyCommitmentReader) GetNoCopy(key []byte) ([]byte, error) {
	return r.Get(key)
}
