package rawdb

import (
	"bytes"
	"encoding/binary"
	"testing"

	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/state/kvdomains"
)

// This retains the original byte layout independently of the append helper.
func originalElementBytes(family byte, key, value []byte) []byte {
	out := append([]byte(nil), []byte("gtron/latest-domain/muhash3072/v1\x00")...)
	out = append(out, family)
	out = binary.BigEndian.AppendUint64(out, uint64(len(key)))
	out = append(out, key...)
	out = binary.BigEndian.AppendUint64(out, uint64(len(value)))
	return append(out, value...)
}

func TestMuHashElementAppendCodecAndOwnership(t *testing.T) {
	owner := common.Address{0x41, 1}
	keys := [][]byte{StateAccountLatestCommitmentKey(owner), StateKVGenerationCommitmentKey(owner), StateKVLatestCommitmentKey(owner, 7, kvdomains.ContractStorage, make([]byte, 32))}
	var scratch []byte
	for i, key := range keys {
		for _, value := range [][]byte{nil, {}, make([]byte, 32), bytes.Repeat([]byte{0xa3}, 1025)} {
			want := originalElementBytes(byte(i+1), key, value)
			encoded, err := EncodeStateCommitmentElement(key, value)
			if err != nil || !bytes.Equal(encoded, want) {
				t.Fatal("owned codec mismatch", err)
			}
			prefix := []byte("retained prefix")
			scratch = append(scratch[:0], prefix...)
			scratch, err = AppendStateCommitmentElement(scratch, key, value)
			if err != nil || !bytes.Equal(scratch[:len(prefix)], prefix) || !bytes.Equal(scratch[len(prefix):], want) {
				t.Fatal("append codec mismatch", err)
			}
			clear(scratch)
			if !bytes.Equal(encoded, want) {
				t.Fatal("Encode borrowed reusable destination")
			}
		}
	}
	key := StateAccountLatestCommitmentKey(owner)
	value := []byte("owned")
	encoded, err := EncodeStateCommitmentElement(key, value)
	if err != nil {
		t.Fatal(err)
	}
	want := bytes.Clone(encoded)
	clear(key)
	clear(value)
	if !bytes.Equal(encoded, want) {
		t.Fatal("Encode borrowed inputs")
	}
}

func TestMuHashElementAppendInvalidKeyDoesNotTouchDestination(t *testing.T) {
	backing := bytes.Repeat([]byte{0x73}, 2048)
	dst := backing[:17]
	want := bytes.Clone(backing)
	for _, bad := range [][]byte{nil, []byte("bad source"), append(StateAccountLatestCommitmentKey(common.Address{0x41, 1}), 0)} {
		if _, err := AppendStateCommitmentElement(dst, bad, []byte("must not write")); err == nil {
			t.Fatal("accepted invalid source key")
		}
		if len(dst) != 17 || !bytes.Equal(backing, want) {
			t.Fatal("invalid source modified destination/backing")
		}
	}
}
