package rawdb

import (
	"bytes"
	"errors"
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

type ownedKeyPartsProbeWriter struct {
	keyPartsProbeWriter
	ownedCalls       int
	stringOwnedCalls int
	batchOwnedCalls  int
	batchCount       int
	batchKeys        [][]byte
	batchValues      [][]byte
}

func (w *ownedKeyPartsProbeWriter) PutKeyPartsOwnedValue(first, second, value []byte) error {
	w.ownedCalls++
	w.key = append(append(w.key[:0], first...), second...)
	w.value = value
	return nil
}

func (w *ownedKeyPartsProbeWriter) PutKeyPartsStringOwnedValue(first []byte, second string, value []byte) error {
	w.stringOwnedCalls++
	w.key = append(append(w.key[:0], first...), second...)
	w.value = value
	return nil
}

func (w *ownedKeyPartsProbeWriter) PutKeyPartsStringsOwnedValues(first []byte, seconds []string, values [][]byte) error {
	w.batchOwnedCalls++
	w.batchKeys = make([][]byte, len(seconds))
	for i, second := range seconds {
		w.batchKeys[i] = append(append([]byte(nil), first...), second...)
	}
	w.batchValues = values
	return nil
}

func (w *ownedKeyPartsProbeWriter) PutKeyPartsStringsOwnedValuesWithBatchCount(first []byte, seconds []string, values [][]byte, batchCount int) error {
	w.batchCount = batchCount
	return w.PutKeyPartsStringsOwnedValues(first, seconds, values)
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

func TestCommitmentBranchOwnedValueUsesTransferWriter(t *testing.T) {
	w := new(ownedKeyPartsProbeWriter)
	prefix := []byte{1, 2, 3, 4}
	value := []byte{5, 6, 7}
	wantKey := commitmentBranchKey(prefix)
	if !SupportsCommitmentBranchOwnedValue(w) {
		t.Fatal("owned split-key writer capability was not detected")
	}
	if err := WriteCommitmentBranchOwned(w, prefix, value); err != nil {
		t.Fatal(err)
	}
	if w.ownedCalls != 1 || w.putCalls != 0 || !bytes.Equal(w.key, wantKey) || !bytes.Equal(w.value, value) {
		t.Fatalf("owned Put = owned %d regular %d key %x value %x, want 1/0/%x/%x", w.ownedCalls, w.putCalls, w.key, w.value, wantKey, value)
	}
	if &w.value[0] != &value[0] {
		t.Fatal("owned commitment writer copied the transferred value")
	}
}

func TestCommitmentBranchOwnedStringUsesTransferWriter(t *testing.T) {
	w := new(ownedKeyPartsProbeWriter)
	prefix := string([]byte{1, 2, 3, 4})
	value := []byte{5, 6, 7}
	wantKey := commitmentBranchKey([]byte(prefix))
	if err := WriteCommitmentBranchOwnedString(w, prefix, value); err != nil {
		t.Fatal(err)
	}
	if w.stringOwnedCalls != 1 || w.ownedCalls != 0 || w.putCalls != 0 || !bytes.Equal(w.key, wantKey) || !bytes.Equal(w.value, value) {
		t.Fatalf("string-owned Put = string %d owned %d regular %d key %x value %x, want 1/0/0/%x/%x", w.stringOwnedCalls, w.ownedCalls, w.putCalls, w.key, w.value, wantKey, value)
	}
	if &w.value[0] != &value[0] {
		t.Fatal("string-owned commitment writer copied the transferred value")
	}
}

func TestCommitmentBranchesOwnedStringsUsesBatchTransferWriter(t *testing.T) {
	w := new(ownedKeyPartsProbeWriter)
	prefixes := []string{string([]byte{1, 2}), string([]byte{3, 4, 5})}
	values := [][]byte{{6, 7}, {8, 9, 10}}
	if err := WriteCommitmentBranchesOwnedStrings(w, prefixes, values); err != nil {
		t.Fatal(err)
	}
	if w.batchOwnedCalls != 1 || w.stringOwnedCalls != 0 || w.ownedCalls != 0 || w.putCalls != 0 {
		t.Fatalf("batch owned calls = batch %d string %d owned %d regular %d, want 1/0/0/0",
			w.batchOwnedCalls, w.stringOwnedCalls, w.ownedCalls, w.putCalls)
	}
	if w.batchCount != 1 {
		t.Fatalf("default batch count = %d, want 1", w.batchCount)
	}
	for i, prefix := range prefixes {
		wantKey := commitmentBranchKey([]byte(prefix))
		if !bytes.Equal(w.batchKeys[i], wantKey) || !bytes.Equal(w.batchValues[i], values[i]) {
			t.Fatalf("batch[%d] = key %x value %x, want %x/%x", i, w.batchKeys[i], w.batchValues[i], wantKey, values[i])
		}
		if &w.batchValues[i][0] != &values[i][0] {
			t.Fatalf("batch[%d] copied the transferred value", i)
		}
	}
	if err := WriteCommitmentBranchesOwnedStrings(w, prefixes, values[:1]); err == nil {
		t.Fatal("mismatched batch lengths were accepted")
	}
	if err := WriteCommitmentBranchesOwnedStringsWithBatchCount(w, prefixes, values, 7); err != nil {
		t.Fatal(err)
	}
	if w.batchCount != 7 {
		t.Fatalf("explicit batch count = %d, want 7", w.batchCount)
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

type splitCachedNoCopyProbe struct {
	*cachedNoCopyProbe
	splitCalls int
	first      []byte
	second     []byte
}

type splitCachedNoCopyViewProbe struct {
	*splitCachedNoCopyProbe
	stable    bool
	viewCalls int
	missing   bool
}

func (p *splitCachedNoCopyViewProbe) ViewNoCopyCachedKeyParts(first, second []byte, fn func([]byte, bool) error) (bool, error) {
	p.viewCalls++
	p.first = append(p.first[:0], first...)
	p.second = append(p.second[:0], second...)
	if p.missing {
		return false, nil
	}
	key := append(append(make([]byte, 0, len(first)+len(second)), first...), second...)
	value, err := p.KeyValueReader.Get(key)
	if err != nil {
		return false, err
	}
	return true, fn(value, p.stable)
}

func (p *splitCachedNoCopyProbe) GetNoCopyCachedKeyParts(first, second []byte) ([]byte, error) {
	p.splitCalls++
	p.first = append(p.first[:0], first...)
	p.second = append(p.second[:0], second...)
	key := append(append(make([]byte, 0, len(first)+len(second)), first...), second...)
	return p.KeyValueReader.Get(key)
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

func TestReadCommitmentBranchNoCopy_PrefersSplitCachedReader(t *testing.T) {
	db := NewMemoryDatabase()
	prefix := []byte{0x04, 0x05, 0x06}
	want := []byte{0xdd, 0xee, 0xff}
	if err := WriteCommitmentBranch(db, prefix, want); err != nil {
		t.Fatal(err)
	}
	probe := &splitCachedNoCopyProbe{
		cachedNoCopyProbe: &cachedNoCopyProbe{KeyValueReader: db},
	}
	got, ok, err := ReadCommitmentBranchNoCopy(probe, prefix)
	if err != nil || !ok || !bytes.Equal(got, want) {
		t.Fatalf("ReadCommitmentBranchNoCopy = (%x,%v,%v)", got, ok, err)
	}
	if probe.splitCalls != 1 || probe.cachedCalls != 0 || probe.noCopyCalls != 0 || probe.getCalls != 0 {
		t.Fatalf("reader calls split/cached/noCopy/Get = %d/%d/%d/%d, want 1/0/0/0",
			probe.splitCalls, probe.cachedCalls, probe.noCopyCalls, probe.getCalls)
	}
	if !bytes.Equal(probe.first, stateCommitmentBranchPrefix) || !bytes.Equal(probe.second, prefix) {
		t.Fatalf("split key parts = %x/%x, want %x/%x", probe.first, probe.second, stateCommitmentBranchPrefix, prefix)
	}
}

func TestViewCommitmentBranchNoCopy_PrefersSplitViewerAndPropagatesLifetime(t *testing.T) {
	db := NewMemoryDatabase()
	prefix := []byte{0x07, 0x08, 0x09}
	want := []byte{0xaa, 0xbb, 0xcc}
	if err := WriteCommitmentBranch(db, prefix, want); err != nil {
		t.Fatal(err)
	}
	probe := &splitCachedNoCopyViewProbe{
		splitCachedNoCopyProbe: &splitCachedNoCopyProbe{
			cachedNoCopyProbe: &cachedNoCopyProbe{KeyValueReader: db},
		},
		stable: false,
	}
	called := 0
	found, err := ViewCommitmentBranchNoCopy(probe, prefix, func(encoded []byte, stable bool) error {
		called++
		if !bytes.Equal(encoded, want) || stable {
			t.Fatalf("view callback = (%x, stable=%v), want (%x, false)", encoded, stable, want)
		}
		return nil
	})
	if err != nil || !found || called != 1 {
		t.Fatalf("ViewCommitmentBranchNoCopy = found %v called %d err %v, want true/1/nil", found, called, err)
	}
	if probe.viewCalls != 1 || probe.splitCalls != 0 || probe.cachedCalls != 0 || probe.getCalls != 0 {
		t.Fatalf("reader calls view/split/cached/noCopy/Get = %d/%d/%d/%d/%d, want 1/0/0/0/0",
			probe.viewCalls, probe.splitCalls, probe.cachedCalls, probe.noCopyCalls, probe.getCalls)
	}
	if !bytes.Equal(probe.first, stateCommitmentBranchPrefix) || !bytes.Equal(probe.second, prefix) {
		t.Fatalf("split key parts = %x/%x, want %x/%x", probe.first, probe.second, stateCommitmentBranchPrefix, prefix)
	}

	injected := errors.New("decode failed")
	if found, err := ViewCommitmentBranchNoCopy(probe, prefix, func([]byte, bool) error { return injected }); !found || !errors.Is(err, injected) {
		t.Fatalf("callback failure = found %v err %v, want true/%v", found, err, injected)
	}
	probe.missing = true
	called = 0
	if found, err := ViewCommitmentBranchNoCopy(probe, prefix, func([]byte, bool) error { called++; return nil }); err != nil || found || called != 0 {
		t.Fatalf("missing view = found %v called %d err %v, want false/0/nil", found, called, err)
	}
}
