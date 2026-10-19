package rawdb

import (
	"bytes"
	"math/rand"
	"testing"

	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/state/kvdomains"
)

func TestCompactWordExactRoundTrip(t *testing.T) {
	rng := rand.New(rand.NewSource(319))
	for leading := 0; leading <= 32; leading++ {
		for j := 0; j < 20; j++ {
			word := make([]byte, 32)
			rng.Read(word[leading:])
			if leading < 32 {
				word[leading] |= 1
			}
			encoded := EncodeStateKVLatestValue(word)
			decoded, err := DecodeStateKVLatestValue(encoded)
			if err != nil || !bytes.Equal(decoded, word) {
				t.Fatalf("leading=%d word=%x got=%x err=%v", leading, word, decoded, err)
			}
			if len(encoded) != 33-leading || StateKVLatestValueSize(word) != len(encoded) {
				t.Fatal("nonminimal word", leading, len(encoded))
			}
			if !bytes.Equal(encoded, EncodeStateKVLatestValue(decoded)) {
				t.Fatal("noncanonical roundtrip")
			}
			decoded[0] ^= 1
			again, _ := DecodeStateKVLatestValue(encoded)
			if !bytes.Equal(again, word) {
				t.Fatal("decoded alias")
			}
		}
	}
	for _, value := range [][]byte{nil, {}, {0}, {1}, bytes.Repeat([]byte{0}, 31), bytes.Repeat([]byte{0}, 33)} {
		encoded := EncodeStateKVLatestValue(value)
		got, err := DecodeStateKVLatestValue(encoded)
		if err != nil || !bytes.Equal(got, value) {
			t.Fatal("bytes changed", value, got, err)
		}
	}
	if bytes.Equal(EncodeStateKVLatestValue(nil), EncodeStateKVLatestValue(make([]byte, 32))) {
		t.Fatal("empty/zero word aliased")
	}
}

func TestCompactRejectsMalformedWordsAndOldScheme(t *testing.T) {
	for _, bad := range [][]byte{nil, {4}, {3, 0}, {2, 0}, {2, 0, 1}, append([]byte{2}, bytes.Repeat([]byte{1}, 32)...), append([]byte{1}, make([]byte, 32)...)} {
		if _, err := DecodeStateKVLatestValue(bad); err == nil {
			t.Fatalf("accepted %x", bad)
		}
	}
	h := NewStateCommitmentHead()
	encoded, err := EncodeStateCommitmentHead(h)
	if err != nil {
		t.Fatal(err)
	}
	for _, old := range []byte{1, 2} {
		encoded[7] = old
		if _, err = DecodeStateCommitmentHead(encoded); err == nil {
			t.Fatalf("old HEAD %d accepted", old)
		}
	}
}

func TestCompactSourceExcludesForeignTrieHash(t *testing.T) {
	db := NewMemoryDatabase()
	defer db.Close()
	hashKey := append(bytes.Clone(stateKVLatestPrefix), make([]byte, common.HashLength-len(stateKVLatestPrefix))...)
	db.Put(hashKey, []byte("trie node"))
	if ClassifyPhysicalKeyString(string(hashKey)) != PhysicalKeyFamilyOther || inspectKeyspaceFor(hashKey).name != "legacy-trie-node" {
		t.Fatal("foreign hash misclassified as latest KV")
	}
	if err := InitializeStateCommitment(db); err != nil {
		t.Fatal(err)
	}
	var count int
	if err := IterateLatestDomainCommitmentSources(db, func(k, v []byte) (bool, error) { count++; return true, nil }); err != nil || count != 0 {
		t.Fatal("foreign row counted", count, err)
	}
	owner := common.Address{0x41, 1}
	slot := make([]byte, 32)
	word := make([]byte, 32)
	word[31] = 7
	if err := WriteStateKVLatest(db, owner, 0, kvdomains.ContractStorage, slot, word); err != nil {
		t.Fatal(err)
	}
	key := StateKVLatestCommitmentKey(owner, 0, kvdomains.ContractStorage, slot)
	if len(key) != 66 {
		t.Fatal("key not shortened", len(key))
	}
	got, ok, err := ReadStateKVLatest(db, owner, 0, kvdomains.ContractStorage, slot)
	if err != nil || !ok || !bytes.Equal(got, word) {
		t.Fatal("compact read", got, ok, err)
	}
	var rows int
	if err = IterateStateKVLatestRows(db, func(StateKVLatestRow) (bool, error) { rows++; return true, nil }); err != nil || rows != 1 {
		t.Fatal("rows", rows, err)
	}
}
