package main

import (
	"bytes"
	"encoding/binary"
	"os"
	"path/filepath"
	"testing"

	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/state/kvdomains"
)

func bundleFixture(owners int) []row {
	var result []row
	for i := 0; i < owners; i++ {
		var owner common.Address
		owner[0] = 0x41
		binary.BigEndian.PutUint64(owner[13:], uint64(i))
		for j := 0; j < (i%24)+1; j++ {
			var token [4]byte
			binary.BigEndian.PutUint32(token[:], uint32(j*2))
			key := rawdb.StateKVLatestCommitmentKey(owner, 0, kvdomains.AccountAssetV2, token[:])
			var v []byte
			switch j % 4 {
			case 0:
				v = nil
			case 1:
				v = make([]byte, 8)
			case 2:
				v = make([]byte, 32)
			case 3:
				v = []byte{0x80, 0xff}
			}
			wrapped := rawdb.EncodeStateKVLatestValue(v)
			result = append(result, row{key: key, old: wrapped, candidate: wrapped})
		}
	}
	return result
}
func TestOwnerBundleExactBoundedRoundTrip(t *testing.T) {
	source := bundleFixture(500)
	for _, size := range []int{2, 8, 16} {
		packed, groups, err := packOwnerBundles(source, size)
		if err != nil || groups == 0 {
			t.Fatal(groups, err)
		}
		var restored []row
		for _, r := range packed {
			if bytes.HasPrefix(r.old, bundleMagic) {
				rows, err := decodeBundle(r.key, r.old)
				if err != nil {
					t.Fatal(err)
				}
				if len(rows) > size || len(r.old) > bundleMaxBytes {
					t.Fatal("unbounded bundle")
				}
				restored = append(restored, rows...)
			} else {
				restored = append(restored, r)
			}
		}
		if len(restored) != len(source) {
			t.Fatal("row count mismatch")
		}
		for i := range source {
			if !bytes.Equal(source[i].key, restored[i].key) || !bytes.Equal(source[i].old, restored[i].old) {
				t.Fatal("canonical row changed", i)
			}
		}
	}
}
func TestOwnerBundleRejectsMalformed(t *testing.T) {
	source := bundleFixture(2)
	key := source[1].key
	payload, err := encodeBundle(source[1:])
	if err != nil {
		t.Fatal(err)
	}
	for _, bad := range [][]byte{nil, bundleMagic, append(append([]byte(nil), bundleMagic...), 1), append(append([]byte(nil), bundleMagic...), 17), payload[:len(payload)-1], append(append([]byte(nil), payload...), 0)} {
		if _, err := decodeBundle(key, bad); err == nil {
			t.Fatalf("accepted malformed %x", bad)
		}
	}
	if _, err = encodeBundle(source[:2]); err == nil {
		t.Fatal("cross-owner bundle accepted")
	}
}
func TestOwnerBundleSSTPipeline(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "synthetic-assets.bin")
	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	for _, r := range bundleFixture(300) {
		var b [4]byte
		binary.BigEndian.PutUint32(b[:], uint32(len(r.key)))
		if _, err = f.Write(b[:]); err != nil {
			t.Fatal(err)
		}
		f.Write(r.key)
		binary.BigEndian.PutUint32(b[:], uint32(len(r.old)))
		f.Write(b[:])
		f.Write(r.old)
	}
	if err = f.Close(); err != nil {
		t.Fatal(err)
	}
	if err = runBundle(path, filepath.Join(dir, "result"), 10000, 1000, 2, 8); err != nil {
		t.Fatal(err)
	}
}
