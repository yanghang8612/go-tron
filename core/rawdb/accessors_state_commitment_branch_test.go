package rawdb

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/ethereum/go-ethereum/ethdb"
)

type discardCommitmentWriter struct{}

func (discardCommitmentWriter) Put(_, _ []byte) error { return nil }
func (discardCommitmentWriter) Delete(_ []byte) error { return nil }

type keyPartsProbeWriter struct {
	putCalls    int
	deleteCalls int
	key         []byte
	value       []byte
}

func (w *keyPartsProbeWriter) Put(_, _ []byte) error {
	return fmt.Errorf("unexpected fallback Put")
}

func (w *keyPartsProbeWriter) Delete(_ []byte) error {
	return fmt.Errorf("unexpected fallback Delete")
}

func (w *keyPartsProbeWriter) PutKeyParts(first, second, value []byte) error {
	w.putCalls++
	w.key = append(append(w.key[:0], first...), second...)
	w.value = append(w.value[:0], value...)
	return nil
}

func (w *keyPartsProbeWriter) DeleteKeyParts(first, second []byte) error {
	w.deleteCalls++
	w.key = append(append(w.key[:0], first...), second...)
	return nil
}

func TestCommitmentBranchUsesSplitKeyWriter(t *testing.T) {
	w := new(keyPartsProbeWriter)
	prefix := []byte{1, 2, 3, 4}
	value := []byte{5, 6, 7}
	wantKey := commitmentBranchKey(prefix)
	if err := WriteCommitmentBranch(w, prefix, value); err != nil {
		t.Fatal(err)
	}
	if w.putCalls != 1 || !bytes.Equal(w.key, wantKey) || !bytes.Equal(w.value, value) {
		t.Fatalf("split Put = calls %d key %x value %x, want 1/%x/%x", w.putCalls, w.key, w.value, wantKey, value)
	}
	if err := DeleteCommitmentBranch(w, prefix); err != nil {
		t.Fatal(err)
	}
	if w.deleteCalls != 1 || !bytes.Equal(w.key, wantKey) {
		t.Fatalf("split Delete = calls %d key %x, want 1/%x", w.deleteCalls, w.key, wantKey)
	}
}

func BenchmarkCommitmentBranchKeyAllocation(b *testing.B) {
	w := discardCommitmentWriter{}
	value := bytes.Repeat([]byte{0xcd}, 256)
	for _, prefixLen := range []int{0, 8, 32, 64} {
		b.Run(fmt.Sprintf("prefix-%d", prefixLen), func(b *testing.B) {
			prefix := bytes.Repeat([]byte{0x0a}, prefixLen)
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				if err := WriteCommitmentBranch(w, prefix, value); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

type cachedNoCopyProbe struct {
	ethdb.KeyValueReader
	getCalls    int
	noCopyCalls int
	cachedCalls int
}

func (p *cachedNoCopyProbe) Get(key []byte) ([]byte, error) {
	p.getCalls++
	return p.KeyValueReader.Get(key)
}

func (p *cachedNoCopyProbe) GetNoCopy(key []byte) ([]byte, error) {
	p.noCopyCalls++
	return p.KeyValueReader.Get(key)
}

func (p *cachedNoCopyProbe) GetNoCopyCached(key []byte) ([]byte, error) {
	p.cachedCalls++
	return p.KeyValueReader.Get(key)
}

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

func TestReadCommitmentBranchNoCopy_PrefersCachedReader(t *testing.T) {
	db := NewMemoryDatabase()
	prefix := []byte{0x01, 0x02, 0x03}
	want := []byte{0xaa, 0xbb, 0xcc}
	if err := WriteCommitmentBranch(db, prefix, want); err != nil {
		t.Fatal(err)
	}
	probe := &cachedNoCopyProbe{KeyValueReader: db}
	got, ok, err := ReadCommitmentBranchNoCopy(probe, prefix)
	if err != nil || !ok || !bytes.Equal(got, want) {
		t.Fatalf("ReadCommitmentBranchNoCopy = (%x,%v,%v)", got, ok, err)
	}
	if probe.cachedCalls != 1 || probe.noCopyCalls != 0 || probe.getCalls != 0 {
		t.Fatalf("reader calls cached/noCopy/Get = %d/%d/%d, want 1/0/0",
			probe.cachedCalls, probe.noCopyCalls, probe.getCalls)
	}
}
