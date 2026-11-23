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
	out := append([]byte(nil), []byte("gtron/latest-domain/muhash3072/compact-v3\x00")...)
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
			if i == 2 {
				value = EncodeStateKVLatestValue(value)
			}
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

func TestMuHashAppendMalformedValueDoesNotModifyDestination(t *testing.T) {
	backing := bytes.Repeat([]byte{0x73}, 2048)
	before := bytes.Clone(backing)
	key := StateKVLatestCommitmentKey(common.Address{0x41, 1}, 0, kvdomains.AccountAssetV2, []byte("x"))
	for _, value := range [][]byte{nil, {4}, {3, 0}, append([]byte{1}, make([]byte, 8)...)} {
		if _, err := AppendStateCommitmentElement(backing[:17], key, value); err == nil {
			t.Fatal("malformed wrapped scalar accepted")
		}
		if !bytes.Equal(backing, before) {
			t.Fatal("invalid value modified destination")
		}
	}
}

func TestStateCommitmentKVFamilyValidatesHeaderWithoutCopy(t *testing.T) {
	owner := common.Address{0x41, 0x44}
	logical := bytes.Repeat([]byte{0x66}, 64)
	key := StateKVLatestCommitmentKey(owner, 42, kvdomains.SystemDelegation, logical)
	if got := testing.AllocsPerRun(100, func() {
		if family, err := StateCommitmentElementFamily(key); family != StateCommitmentFamilyKVLatest || err != nil {
			t.Fatalf("family=%d err=%v", family, err)
		}
	}); got != 0 {
		t.Fatalf("classification allocated %.1f objects", got)
	}
	address, generation, domain, decoded, ok := DecodeStateKVLatestKey(key)
	if !ok || address != owner || generation != 42 || domain != kvdomains.SystemDelegation || !bytes.Equal(decoded, logical) {
		t.Fatalf("decode=%x/%d/%d/%x ok=%t", address, generation, domain, decoded, ok)
	}
	headerLen := len(key) - len(logical)
	for _, bad := range [][]byte{key[:headerLen-1], StateKVLatestCommitmentKey(owner, 42, kvdomains.KVDomain(0xffff), nil)} {
		if family, err := StateCommitmentElementFamily(bad); family != 0 || err == nil {
			t.Fatalf("bad key=%x family=%d err=%v", bad, family, err)
		}
		if _, _, _, _, ok := DecodeStateKVLatestKey(bad); ok {
			t.Fatalf("decoded invalid key %x", bad)
		}
	}
	if family, err := StateCommitmentElementFamily(key[:headerLen]); family != StateCommitmentFamilyKVLatest || err != nil {
		t.Fatalf("empty logical key family=%d err=%v", family, err)
	}
	if _, _, _, empty, ok := DecodeStateKVLatestKey(key[:headerLen]); !ok || empty != nil {
		t.Fatalf("empty logical key=%v ok=%t", empty, ok)
	}
	clear(key)
	if !bytes.Equal(decoded, logical) {
		t.Fatal("public decode borrowed caller storage")
	}
}

func TestCoalesceSortedCommitmentDeltasOwnsResults(t *testing.T) {
	owner := common.Address{0x41, 0x44}
	key := func(n byte) []byte {
		return StateKVLatestCommitmentKey(owner, 4, kvdomains.SystemDelegation, []byte{n})
	}
	input := []StateCommitmentDelta{
		{Key: key(1), BeforeExists: true, Before: []byte("before"), AfterExists: true, After: []byte("changed")},
		{Key: key(2), BeforeExists: true, Before: []byte("same"), AfterExists: true, After: []byte("same")},
		{Key: key(3), AfterExists: true, After: []byte("insert")},
	}
	got, err := CoalesceStateCommitmentDeltas(input)
	if err != nil || len(got) != 2 || !bytes.Equal(got[0].Key, key(1)) || !bytes.Equal(got[1].Key, key(3)) {
		t.Fatalf("sorted deltas=%+v err=%v", got, err)
	}
	for _, delta := range input {
		clear(delta.Key)
		clear(delta.Before)
		clear(delta.After)
	}
	if !bytes.Equal(got[0].Key, key(1)) || string(got[0].Before) != "before" || string(got[0].After) != "changed" || string(got[1].After) != "insert" {
		t.Fatalf("retained caller memory: %+v", got)
	}

	// An invalid key still fails even if its payload is a no-op.
	if _, err := CoalesceStateCommitmentDeltas([]StateCommitmentDelta{{Key: []byte("invalid")}}); err == nil {
		t.Fatal("invalid no-op skipped source-key validation")
	}
}

func TestCoalesceUnsortedCommitmentDeltasRetainsSameKeyOrder(t *testing.T) {
	owner := common.Address{0x41, 0x45}
	key := func(n byte) []byte {
		return StateKVLatestCommitmentKey(owner, 4, kvdomains.SystemDelegation, []byte{n})
	}
	input := []StateCommitmentDelta{
		{Key: key(3), AfterExists: true, After: []byte("old")},
		{Key: key(1), AfterExists: true, After: []byte("separate")},
		{Key: key(3), BeforeExists: true, Before: []byte("old"), AfterExists: true, After: []byte("new")},
	}
	got, err := CoalesceStateCommitmentDeltas(input)
	if err != nil || len(got) != 2 || !bytes.Equal(got[0].Key, key(1)) || !bytes.Equal(got[1].Key, key(3)) ||
		got[1].BeforeExists || string(got[1].After) != "new" {
		t.Fatalf("unordered deltas=%+v err=%v", got, err)
	}
	input[2].Before = []byte("wrong intermediate")
	input = append(input, StateCommitmentDelta{Key: []byte("invalid")})
	if _, err := CoalesceStateCommitmentDeltas(input); err == nil || err.Error() != "inconsistent commitment delta intermediate image" {
		t.Fatalf("unordered error order changed: %v", err)
	}
}
