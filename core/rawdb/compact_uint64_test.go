package rawdb

import (
	"bytes"
	"encoding/binary"
	"testing"
)

func TestCompactUint64ExactPresenceAndCanonical(t *testing.T) {
	for width := 0; width <= 8; width++ {
		word := make([]byte, 8)
		if width > 0 {
			word[8-width] = 1
		}
		encoded := EncodeStateKVLatestValue(word)
		if StateKVLatestValueSize(word) != len(encoded) {
			t.Fatal("size")
		}
		decoded, err := DecodeStateKVLatestValue(encoded)
		if err != nil || !bytes.Equal(decoded, word) {
			t.Fatal(width, err)
		}
		if width < 8 && (encoded[0] != 3 || len(encoded) != width+1) {
			t.Fatal("width tag")
		}
	}
	for _, n := range []uint64{1 << 63, ^uint64(0)} {
		word := binary.BigEndian.AppendUint64(nil, n)
		decoded, err := DecodeStateKVLatestValue(EncodeStateKVLatestValue(word))
		if err != nil || !bytes.Equal(word, decoded) {
			t.Fatal("signed bits")
		}
	}
	if bytes.Equal(EncodeStateKVLatestValue(make([]byte, 8)), EncodeStateKVLatestValue(nil)) || bytes.Equal(EncodeStateKVLatestValue(make([]byte, 8)), EncodeStateKVLatestValue(make([]byte, 32))) {
		t.Fatal("presence/width conflation")
	}
	for _, bad := range [][]byte{{3, 0}, {3, 0, 1}, append([]byte{3}, make([]byte, 8)...), append([]byte{1}, make([]byte, 8)...)} {
		if ValidateStateKVLatestValue(bad) == nil {
			t.Fatal("malformed accepted")
		}
	}
}
