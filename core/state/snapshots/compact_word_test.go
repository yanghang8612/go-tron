package snapshots

import (
	"bytes"
	"encoding/binary"
	"testing"
)

func TestLatestBinaryCompactWordsExactAndCanonical(t *testing.T) {
	for leading := 0; leading <= 32; leading++ {
		word := make([]byte, 32)
		for i := leading; i < 32; i++ {
			word[i] = byte(i + 1)
		}
		encoded, frame, err := encodeLatestBinaryValue(word)
		if err != nil {
			t.Fatal(err)
		}
		if frame.compactWord != (leading > 0) || len(encoded) != 32-leading {
			t.Fatal("word not minimal", leading, frame, len(encoded))
		}
		got, err := decodeLatestBinaryValue(encoded, frame)
		if err != nil || !bytes.Equal(got, word) {
			t.Fatal(leading, got, err)
		}
		var buf bytes.Buffer
		if err := writeLatestBinaryEntry(&buf, 0, LatestEntry{Key: []byte("slot"), Value: word}); err != nil {
			t.Fatal(err)
		}
		record := buf.Bytes()
		if leading > 0 && binary.BigEndian.Uint32(record[4:8])&latestBinaryValueWordFlag == 0 {
			t.Fatal("missing word bit")
		}
		entry, rest, err := decodeLatestBinaryEntry(record, true)
		if err != nil || len(rest) != 0 || !bytes.Equal(entry.Value, word) {
			t.Fatal("framed roundtrip", leading, err)
		}
	}
	for _, bad := range []latestBinaryValueFrame{{compactWord: true, compressed: true}, {compactWord: true, encodedLen: 32}} {
		if err := validateLatestBinaryValueFrame(bad); err == nil {
			t.Fatal("malformed compact frame accepted")
		}
	}
	if _, err := decodeLatestBinaryValue([]byte{0, 1}, latestBinaryValueFrame{encodedLen: 2, compactWord: true}); err == nil {
		t.Fatal("leading zero accepted")
	}
	empty, frame, err := encodeLatestBinaryValue(nil)
	if err != nil || len(empty) != 0 || frame.compactWord {
		t.Fatal("empty value confused with zero word")
	}
}

func TestCompactManifestRejectsOldStateFormat(t *testing.T) {
	for _, mutate := range []func(*Manifest){func(m *Manifest) { m.Version = 2 }, func(m *Manifest) { m.CommitmentScheme = "state-flat-latest-muhash3072-v1" }, func(m *Manifest) { m.ElementVersion = 1 }} {
		m := NewManifest(0, 0, nil)
		mutate(m)
		if err := m.Validate(); err == nil {
			t.Fatal("old format accepted")
		}
	}
}

func TestLatestBinaryCompactUint64ExactAndCanonical(t *testing.T) {
	for leading := 0; leading <= 8; leading++ {
		word := make([]byte, 8)
		for i := leading; i < 8; i++ {
			word[i] = byte(i + 1)
		}
		encoded, frame, err := encodeLatestBinaryValue(word)
		if err != nil || frame.compactUint64 != (leading > 0) || len(encoded) != 8-leading {
			t.Fatal(leading, frame, encoded, err)
		}
		var buf bytes.Buffer
		if err := writeLatestBinaryEntry(&buf, 0, LatestEntry{Key: []byte("scalar"), Value: word}); err != nil {
			t.Fatal(err)
		}
		record := buf.Bytes()
		if leading > 0 && binary.BigEndian.Uint32(record[4:8])&latestBinaryValueUint64Flag == 0 {
			t.Fatal("missing uint64 flag")
		}
		entry, rest, err := decodeLatestBinaryEntry(record, true)
		if err != nil || len(rest) != 0 || !bytes.Equal(entry.Value, word) {
			t.Fatal("uint64 framed roundtrip", leading, err)
		}
	}
	for _, bad := range []latestBinaryValueFrame{{compactUint64: true, compactWord: true}, {compactUint64: true, compressed: true}, {compactUint64: true, encodedLen: 8}} {
		if err := validateLatestBinaryValueFrame(bad); err == nil {
			t.Fatal("invalid uint64 flags accepted")
		}
	}
	if _, err := decodeLatestBinaryValue([]byte{0, 1}, latestBinaryValueFrame{compactUint64: true, encodedLen: 2}); err == nil {
		t.Fatal("nonminimal uint64 accepted")
	}
}
