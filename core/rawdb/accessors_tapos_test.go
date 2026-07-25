package rawdb

import (
	"bytes"
	"testing"

	"github.com/tronprotocol/go-tron/common"
)

type taposSplitReadProbe struct {
	value       []byte
	gets        int
	splitReads  int
	observedKey []byte
}

func (*taposSplitReadProbe) Has([]byte) (bool, error) { return true, nil }

func (p *taposSplitReadProbe) Get(key []byte) ([]byte, error) {
	p.gets++
	p.observedKey = append(p.observedKey[:0], key...)
	return p.value, nil
}

func (p *taposSplitReadProbe) GetNoCopyCachedKeyParts(first, second []byte) ([]byte, error) {
	p.splitReads++
	p.observedKey = append(p.observedKey[:0], first...)
	p.observedKey = append(p.observedKey, second...)
	return p.value, nil
}

func TestReadTaposRefNoCopyUsesSplitReader(t *testing.T) {
	backing := []byte{1, 2, 3, 4, 5, 6, 7, 8}
	probe := &taposSplitReadProbe{value: backing}
	ref := []byte{0x12, 0x34}

	got := ReadTaposRefNoCopy(probe, ref)
	if !bytes.Equal(got, backing) {
		t.Fatalf("TAPOS value = %x, want %x", got, backing)
	}
	if probe.splitReads != 1 || probe.gets != 0 {
		t.Fatalf("split/Get calls = %d/%d, want 1/0", probe.splitReads, probe.gets)
	}
	wantKey := append(append([]byte(nil), taposPrefix...), ref...)
	if !bytes.Equal(probe.observedKey, wantKey) {
		t.Fatalf("physical key = %x, want %x", probe.observedKey, wantKey)
	}
	if &got[0] != &backing[0] {
		t.Fatal("no-copy TAPOS read copied the reader value")
	}
}

func TestReadTaposRefNoCopyRejectsInvalidRows(t *testing.T) {
	probe := &taposSplitReadProbe{value: make([]byte, 8)}
	if got := ReadTaposRefNoCopy(probe, []byte{1}); got != nil {
		t.Fatalf("short ref read = %x, want nil", got)
	}
	if probe.splitReads != 0 || probe.gets != 0 {
		t.Fatalf("invalid ref reached reader: split/Get=%d/%d", probe.splitReads, probe.gets)
	}

	probe.value = make([]byte, 7)
	if got := ReadTaposRefNoCopy(probe, []byte{1, 2}); got != nil {
		t.Fatalf("short value read = %x, want nil", got)
	}
}

func TestReadTaposRefNoCopyFallbackRoundTrip(t *testing.T) {
	db := NewMemoryDatabase()
	var hash common.Hash
	copy(hash[8:16], []byte("hash-tail"))
	if err := WriteTaposRef(db, 0x1234, hash); err != nil {
		t.Fatal(err)
	}
	if got := ReadTaposRefNoCopy(db, []byte{0x12, 0x34}); !bytes.Equal(got, hash[8:16]) {
		t.Fatalf("fallback TAPOS read = %x, want %x", got, hash[8:16])
	}
}
