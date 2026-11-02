package main

import (
	"bytes"
	"encoding/binary"
	"os"
	"path/filepath"
	"testing"

	"github.com/cockroachdb/pebble"
	ethrawdb "github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/state"
	"github.com/tronprotocol/go-tron/core/state/kvdomains"
	"github.com/tronprotocol/go-tron/core/types"
	corepb "github.com/tronprotocol/go-tron/proto/core"
)

func TestPermissionTemplatesAgainstProductionWriter(t *testing.T) {
	var owner common.Address
	owner[0] = 0x41
	owner[20] = 0x93
	disk := ethrawdb.NewMemoryDatabase()
	defer disk.Close()
	s, e := state.New(common.Hash{}, state.NewDatabase(disk))
	if e != nil {
		t.Fatal(e)
	}
	s.CreateAccount(owner, corepb.AccountType_Normal)
	for _, ops := range [][]byte{immutableOps[1], immutableOps[2], bytes.Repeat([]byte{0xa7}, 32)} {
		o := types.MakeDefaultOwnerPermission(owner)
		w := types.MakeDefaultWitnessPermission(owner)
		a := types.MakeDefaultActivePermission(owner, ops)
		s.SetPermissions(owner, o, w, []*corepb.Permission{a})
		for _, key := range [][]byte{{0}, {1}, {2, 0, 0, 0, 2}} {
			old, present, e := s.GetAccountKV(owner, kvdomains.AccountPermissionAux, key)
			if e != nil || !present {
				t.Fatal(present, e)
			}
			candidate, mode, _, e := compact(owner, key, old)
			if e != nil {
				t.Fatal(e)
			}
			restored, e := expand(owner, candidate)
			if e != nil || !bytes.Equal(old, restored) {
				t.Fatal(mode, e)
			}
			if len(candidate) >= len(old) {
				t.Fatal("template did not shrink", mode)
			}
			if key[0] == 0 && len(old) != 44 {
				t.Fatal("owner size", len(old))
			}
			if key[0] == 1 && len(old) != 46 {
				t.Fatal("witness size", len(old))
			}
			if key[0] == 2 && len(old) != 77 {
				t.Fatal("active size", len(old))
			}
		}
	}
	o := types.MakeDefaultOwnerPermission(owner)
	o.Threshold = 2
	o.PermissionName = "custom"
	s.SetPermissions(owner, o, nil, nil)
	old, _, _ := s.GetAccountKV(owner, kvdomains.AccountPermissionAux, []byte{0})
	candidate, mode, _, e := compact(owner, []byte{0}, old)
	if e != nil || mode != "self_only_custom" {
		t.Fatal(mode, e)
	}
	restored, e := expand(owner, candidate)
	if e != nil || !bytes.Equal(old, restored) {
		t.Fatal(e)
	}
	o.ProtoReflect().SetUnknown([]byte{0xa0, 0x06, 1})
	s.SetPermissions(owner, o, nil, nil)
	old, _, _ = s.GetAccountKV(owner, kvdomains.AccountPermissionAux, []byte{0})
	candidate, mode, _, e = compact(owner, []byte{0}, old)
	if e != nil || mode != "raw_unknown_fallback" || !bytes.Equal(candidate, old) {
		t.Fatal(mode, e)
	}
	for _, bad := range [][]byte{newMagic, append(append([]byte(nil), newMagic...), modeActiveID, 0xff), append(append([]byte(nil), newMagic...), modeOwner, 0), append(append([]byte(nil), newMagic...), modeSelf, 0)} {
		if _, e := expand(owner, bad); e == nil {
			t.Fatalf("accepted malformed %x", bad)
		}
	}
}
func TestPermissionShortPrefixAndRealSSTCursorSmoke(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "maintenance")
	source := filepath.Join(dir, "chaindata")
	if e := os.MkdirAll(source, 0755); e != nil {
		t.Fatal(e)
	}
	cache := pebble.NewCache(8 << 20)
	defer cache.Unref()
	db, e := pebble.Open(source, options(nil, cache, false))
	if e != nil {
		t.Fatal(e)
	}
	for i := 0; i < 300; i++ {
		var owner common.Address
		owner[0] = 0x41
		binary.BigEndian.PutUint64(owner[13:], uint64(i))
		for _, entry := range []struct {
			key  []byte
			mode byte
			ops  []byte
		}{{[]byte{0}, modeOwner, nil}, {[]byte{2, 0, 0, 0, 2}, modeActiveID, immutableOps[1]}} {
			key := rawdb.StateKVLatestCommitmentKey(owner, 0, kvdomains.AccountPermissionAux, entry.key)
			value := rawdb.EncodeStateKVLatestValue(template(owner, entry.mode, entry.ops))
			if e = db.Set(key, value, pebble.NoSync); e != nil {
				t.Fatal(e)
			}
		}
	}
	if e = db.Close(); e != nil {
		t.Fatal(e)
	}
	if e = run(source, filepath.Join(dir, "probe"), 200, 100, 0, 1000); e != nil {
		t.Fatal(e)
	}
}
