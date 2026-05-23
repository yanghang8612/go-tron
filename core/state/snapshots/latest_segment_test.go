package snapshots

import (
	"bytes"
	"path/filepath"
	"testing"

	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/state/kvdomains"
)

func TestLatestSegmentBuildPublishAndRead(t *testing.T) {
	dir := t.TempDir()
	db := rawdb.NewMemoryDatabase()
	owner1 := common.BytesToAddress(append([]byte{common.AddressPrefixMainnet}, bytes.Repeat([]byte{0x11}, common.AccountIDLength)...))
	owner2 := common.BytesToAddress(append([]byte{common.AddressPrefixMainnet}, bytes.Repeat([]byte{0x22}, common.AccountIDLength)...))
	if err := rawdb.WriteStateKVLatest(db, owner2, 3, kvdomains.SystemReward, []byte("cycle/2"), []byte("ignored")); err != nil {
		t.Fatal(err)
	}
	if err := rawdb.WriteStateKVLatest(db, owner1, 7, kvdomains.SystemDynamicProperty, []byte("latest_block"), []byte("12")); err != nil {
		t.Fatal(err)
	}
	if err := rawdb.WriteStateKVLatest(db, owner2, 9, kvdomains.SystemDynamicProperty, []byte("next_maintenance"), []byte("42")); err != nil {
		t.Fatal(err)
	}

	ref, err := BuildLatestDomainSegmentFromDB(db, dir, kvdomains.SystemDynamicProperty, 1, 10, "latest/system-dp.json")
	if err != nil {
		t.Fatalf("build latest segment: %v", err)
	}
	if ref.Size == 0 || ref.Checksum == "" {
		t.Fatalf("segment metadata not filled: %+v", ref)
	}
	manifest := NewManifest(1, 10, []SegmentRef{ref})
	if err := PublishManifest(dir, manifest); err != nil {
		t.Fatalf("publish manifest: %v", err)
	}

	mgr, err := OpenManager(dir)
	if err != nil {
		t.Fatalf("open manager: %v", err)
	}
	key := AccountKVSnapshotKey(owner1, 7, []byte("latest_block"))
	got, ok, err := mgr.GetLatest(kvdomains.SystemDynamicProperty, key, 5)
	if err != nil || !ok || string(got) != "12" {
		t.Fatalf("GetLatest = %q ok:%v err:%v", got, ok, err)
	}
	if _, ok, err := mgr.GetLatest(kvdomains.SystemReward, key, 5); err != nil || ok {
		t.Fatalf("wrong domain hit ok:%v err:%v", ok, err)
	}

	var keys [][]byte
	prefix := owner2.AccountID().Bytes()
	if err := mgr.IterateLatestPrefix(kvdomains.SystemDynamicProperty, prefix, 5, func(key, value []byte) (bool, error) {
		keys = append(keys, key)
		if string(value) != "42" {
			t.Fatalf("prefix value = %q", value)
		}
		return true, nil
	}); err != nil {
		t.Fatalf("iterate prefix: %v", err)
	}
	if len(keys) != 1 {
		t.Fatalf("prefix keys = %d, want 1", len(keys))
	}
	if _, err := OpenLatestSegment(dir, SegmentRef{Domain: ref.Domain, Kind: ref.Kind, FromTxNum: ref.FromTxNum, ToTxNum: ref.ToTxNum, Path: ref.Path, Size: ref.Size, Checksum: "sha256:bad"}); err == nil {
		t.Fatal("bad checksum accepted")
	}
	if _, err := OpenLatestSegment(filepath.Join(dir, "missing"), ref); err == nil {
		t.Fatal("missing segment accepted")
	}
}
