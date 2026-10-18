package rawdb

import (
	"bytes"
	"github.com/tronprotocol/go-tron/common"
	"testing"
)

func TestMuHashHeadStrictCodec(t *testing.T) {
	h := NewStateCommitmentHead()
	h.BlockNum = 7
	h.TxNum = 19
	h.BlockHash = common.Hash{1}
	h.ParentHash = common.Hash{2}
	encoded, err := EncodeStateCommitmentHead(h)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := DecodeStateCommitmentHead(encoded)
	if err != nil || decoded != h {
		t.Fatal("HEAD roundtrip", err)
	}
	for _, bad := range [][]byte{nil, encoded[:len(encoded)-1], append(bytes.Clone(encoded), 0), append([]byte("BADMAGIC"), encoded[8:]...)} {
		if _, err = DecodeStateCommitmentHead(bad); err == nil {
			t.Fatal("accepted malformed HEAD")
		}
	}
	h.State = [384]byte{}
	if _, err = EncodeStateCommitmentHead(h); err == nil {
		t.Fatal("accepted zero state")
	}
	h = NewStateCommitmentHead()
	h.Root[0] ^= 1
	if _, err = EncodeStateCommitmentHead(h); err == nil {
		t.Fatal("accepted state/digest mismatch")
	}
}
func TestMuHashDeltaOwnsBeforeAndChecksIntermediateChain(t *testing.T) {
	key := StateAccountLatestCommitmentKey(common.Address{0x41, 1})
	a, b, c := []byte("a"), []byte("b"), []byte("c")
	deltas, err := CoalesceStateCommitmentDeltas([]StateCommitmentDelta{{Key: key, BeforeExists: true, Before: a, AfterExists: true, After: b}, {Key: key, BeforeExists: true, Before: b, AfterExists: true, After: c}})
	if err != nil || len(deltas) != 1 || string(deltas[0].Before) != "a" || string(deltas[0].After) != "c" {
		t.Fatal("coalesce", err)
	}
	a[0] = 'X'
	b[0] = 'X'
	c[0] = 'X'
	key[0] = 'X'
	if string(deltas[0].Before) != "a" || string(deltas[0].After) != "c" {
		t.Fatal("delta borrowed caller storage")
	}
	key = StateAccountLatestCommitmentKey(common.Address{0x41, 2})
	if _, err = CoalesceStateCommitmentDeltas([]StateCommitmentDelta{{Key: key, AfterExists: true, After: []byte("b")}, {Key: key, BeforeExists: true, Before: []byte("wrong"), AfterExists: false}}); err == nil {
		t.Fatal("broken supplied intermediate chain accepted")
	}
	if _, err = StateCommitmentDeltasFromUpdates([]StateCommitmentUpdate{NewStateCommitmentPut(key, nil)}); err == nil {
		t.Fatal("after-only update silently assumed absent before")
	}
}
func TestStateGenesisRecipeStrictCodec(t *testing.T) {
	db := NewMemoryDatabase()
	defer db.Close()
	hash := common.Hash{9}
	payload := []byte(`{"Config":{}}`)
	if err := WriteStateGenesisRecipe(db, hash, payload); err != nil {
		t.Fatal(err)
	}
	got, data, present, err := ReadStateGenesisRecipe(db)
	if err != nil || !present || got != hash || !bytes.Equal(data, payload) {
		t.Fatal("recipe roundtrip", err)
	}
	raw, err := db.Get(stateGenesisRecipeKey)
	if err != nil {
		t.Fatal(err)
	}
	raw[len(raw)-1] ^= 1
	if err = db.Put(stateGenesisRecipeKey, raw); err != nil {
		t.Fatal(err)
	}
	if _, _, _, err = ReadStateGenesisRecipe(db); err == nil {
		t.Fatal("recipe corruption accepted")
	}
	if err = WriteStateGenesisRecipe(db, hash, make([]byte, MaxStateGenesisRecipeBytes+1)); err == nil {
		t.Fatal("oversized recipe accepted")
	}
}
