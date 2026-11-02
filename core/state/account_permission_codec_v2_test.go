package state

import (
	"bytes"
	"math"
	"testing"

	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/types"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
)

func permissionV2Fixtures(owner common.Address) []struct {
	name string
	key  []byte
	p    *corepb.Permission
	mode int
} {
	custom := types.MakeDefaultActivePermission(owner, bytes.Repeat([]byte{0xa5}, 32))
	custom.Id, custom.ParentId, custom.PermissionName, custom.Threshold = 9, math.MinInt32, "custom-日本", math.MinInt64
	custom.Keys = append(custom.Keys, nil, &corepb.Key{Address: []byte{0x41, 1}, Weight: math.MaxInt64},
		&corepb.Key{Address: owner.Bytes(), Weight: math.MinInt64}, &corepb.Key{Weight: 0})
	unknown := proto.Clone(custom).(*corepb.Permission)
	unknown.ProtoReflect().SetUnknown(protowire.AppendVarint(protowire.AppendTag(nil, 99, protowire.VarintType), 7))
	unknown.Keys[0].ProtoReflect().SetUnknown(protowire.AppendBytes(protowire.AppendTag(nil, 100, protowire.BytesType), owner[:]))
	other := types.MakeDefaultOwnerPermission(testAddr(0xd3))
	return []struct {
		name string
		key  []byte
		p    *corepb.Permission
		mode int
	}{
		{"owner", accountOwnerPermissionKey, types.MakeDefaultOwnerPermission(owner), int(permissionModeOwner)},
		{"witness", accountWitnessPermissionKey, types.MakeDefaultWitnessPermission(owner), int(permissionModeWitness)},
		{"active-legacy", accountActivePermissionKey(2), types.MakeDefaultActivePermission(owner, []byte(permissionOpsLegacy)), int(permissionModeActiveOpsID)},
		{"active-zero", accountActivePermissionKey(2), types.MakeDefaultActivePermission(owner, make([]byte, 32)), int(permissionModeActiveOpsID)},
		{"active-raw", accountActivePermissionKey(2), types.MakeDefaultActivePermission(owner, bytes.Repeat([]byte{0xa5}, 32)), int(permissionModeActiveOpsRaw)},
		{"custom-self", accountActivePermissionKey(9), custom, int(permissionModeSelf)},
		{"custom-unknown", accountActivePermissionKey(9), unknown, -1},
		{"custom-nonself", accountOwnerPermissionKey, other, -1},
	}
}

func TestAccountPermissionV2CanonicalRoundTripAndOwnership(t *testing.T) {
	owner := testAddr(0xd1)
	for _, f := range permissionV2Fixtures(owner) {
		t.Run(f.name, func(t *testing.T) {
			legacy, err := encodeAccountPermission(f.p)
			if err != nil {
				t.Fatal(err)
			}
			value, err := appendAccountPermissionForOwner(nil, owner, f.key, f.p)
			if err != nil {
				t.Fatal(err)
			}
			if f.mode < 0 {
				if !bytes.Equal(value, legacy) {
					t.Fatal("raw fallback changed canonical bytes")
				}
			} else if len(value) >= len(legacy) || value[4] != 2 || int(value[6]) != f.mode {
				t.Fatalf("wrong compact representation: legacy=%d new=%x", len(legacy), value)
			}
			decoded, _, err := decodeAccountPermissionRow(owner, f.key, value)
			if err != nil || !proto.Equal(decoded, f.p) {
				t.Fatalf("decode=%v, err=%v, want=%v", decoded, err, f.p)
			}
			canonical, err := encodeAccountPermission(decoded)
			if err != nil || !bytes.Equal(canonical, legacy) {
				t.Fatalf("canonical native mismatch: %x / %x (%v)", canonical, legacy, err)
			}
			again, _, err := decodeAccountPermissionRow(owner, f.key, value)
			if err != nil {
				t.Fatal(err)
			}
			for i := range value {
				value[i] ^= 0xff
			}
			if !proto.Equal(decoded, f.p) {
				t.Fatal("decoded permission aliases input")
			}
			decoded.Keys[0].Address[0] ^= 0xff
			if len(decoded.Operations) > 0 {
				decoded.Operations[0] ^= 0xff
			}
			if !proto.Equal(again, f.p) {
				t.Fatal("decoded permissions share mutable address or operations")
			}
		})
	}
}

func TestAccountPermissionV2StrictTemplates(t *testing.T) {
	owner := testAddr(0xd1)
	for _, mutate := range []struct {
		name string
		f    func(*corepb.Permission)
	}{
		{"type", func(p *corepb.Permission) { p.Type = corepb.Permission_Witness }},
		{"id", func(p *corepb.Permission) { p.Id = 3 }},
		{"name", func(p *corepb.Permission) { p.PermissionName += "x" }},
		{"threshold", func(p *corepb.Permission) { p.Threshold = 2 }},
		{"parent", func(p *corepb.Permission) { p.ParentId = 1 }},
		{"ops", func(p *corepb.Permission) { p.Operations = []byte{0} }},
		{"weight", func(p *corepb.Permission) { p.Keys[0].Weight = 2 }},
		{"duplicate", func(p *corepb.Permission) { p.Keys = append(p.Keys, p.Keys[0]) }},
		{"nil-key", func(p *corepb.Permission) { p.Keys[0] = nil }},
		{"address-length", func(p *corepb.Permission) { p.Keys[0].Address = p.Keys[0].Address[1:] }},
		{"key-unknown", func(p *corepb.Permission) { p.Keys[0].ProtoReflect().SetUnknown([]byte{0xa0, 6, 1}) }},
		{"unknown", func(p *corepb.Permission) { p.ProtoReflect().SetUnknown([]byte{0xa0, 6, 1}) }},
	} {
		t.Run(mutate.name, func(t *testing.T) {
			p := types.MakeDefaultOwnerPermission(owner)
			mutate.f(p)
			if mode := permissionTemplateMode(owner, accountOwnerPermissionKey, p); mode != permissionModeSelf {
				t.Fatalf("custom fields incorrectly matched template %d", mode)
			}
			value, err := appendAccountPermissionForOwner(nil, owner, accountOwnerPermissionKey, p)
			if err != nil {
				t.Fatal(err)
			}
			got, err := decodeAccountPermissionForOwner(owner, accountOwnerPermissionKey, value)
			if err != nil || !proto.Equal(got, p) {
				t.Fatalf("custom fields lost: got=%v want=%v err=%v", got, p, err)
			}
		})
	}
	for _, key := range [][]byte{accountWitnessPermissionKey, accountActivePermissionKey(2)} {
		p := types.MakeDefaultOwnerPermission(owner)
		if permissionTemplateMode(owner, key, p) != permissionModeSelf {
			t.Fatal("template ignored logical key")
		}
	}
}

func TestAccountPermissionV2RejectsMalformed(t *testing.T) {
	owner := testAddr(0xd1)
	for _, f := range permissionV2Fixtures(owner) {
		value, err := appendAccountPermissionForOwner(nil, owner, f.key, f.p)
		if err != nil {
			t.Fatal(err)
		}
		for cut := 0; cut < len(value); cut++ {
			if _, _, err := decodeAccountPermissionRow(owner, f.key, value[:cut]); err == nil {
				t.Fatalf("%s accepted truncation at %d", f.name, cut)
			}
		}
		if _, _, err := decodeAccountPermissionRow(owner, f.key, append(append([]byte(nil), value...), 0)); err == nil {
			t.Fatalf("%s accepted trailing data", f.name)
		}
	}
	ownerValue, _ := appendAccountPermissionForOwner(nil, owner, accountOwnerPermissionKey, types.MakeDefaultOwnerPermission(owner))
	for _, key := range [][]byte{nil, {3}, {2}, {0, 0}, {2, 0, 0, 0, 1}, accountActivePermissionKey(-1), accountWitnessPermissionKey, accountActivePermissionKey(2)} {
		if _, _, err := decodeAccountPermissionRow(owner, key, ownerValue); err == nil {
			t.Fatalf("accepted owner template under key %x", key)
		}
	}
	for _, mutate := range []func([]byte){
		func(v []byte) { v[4] = 3 }, func(v []byte) { v[5] = accountRowVote }, func(v []byte) { v[6] = 255 },
	} {
		v := append([]byte(nil), ownerValue...)
		mutate(v)
		if _, _, err := decodeAccountPermissionRow(owner, accountOwnerPermissionKey, v); err == nil {
			t.Fatal("accepted invalid permission version/kind/mode")
		}
	}
	activeValue, _ := appendAccountPermissionForOwner(nil, owner, accountActivePermissionKey(2), types.MakeDefaultActivePermission(owner, make([]byte, 32)))
	activeValue[7] = 0
	if _, _, err := decodeAccountPermissionRow(owner, accountActivePermissionKey(2), activeValue); err == nil {
		t.Fatal("accepted unknown immutable operations ID")
	}
	p := types.MakeDefaultActivePermission(owner, nil)
	p.Id = 3
	value, _ := appendAccountPermissionForOwner(nil, owner, accountActivePermissionKey(3), p)
	if _, _, err := decodeAccountPermissionRow(owner, accountActivePermissionKey(2), value); err == nil {
		t.Fatal("accepted custom active ID mismatch")
	}
}

func TestAccountPermissionV2TemplateAllocations(t *testing.T) {
	owner := testAddr(0xd1)
	for _, f := range permissionV2Fixtures(owner)[:5] {
		value, _ := appendAccountPermissionForOwner(nil, owner, f.key, f.p)
		allocs := testing.AllocsPerRun(100, func() {
			benchmarkDecodedAccountPermission, _ = decodeAccountPermissionForOwner(owner, f.key, value)
		})
		if allocs > 1 {
			t.Fatalf("%s template decode allocated %.1f times, want one aggregate", f.name, allocs)
		}
	}
}

func BenchmarkAccountPermissionV2Decode(b *testing.B) {
	owner := testAddr(0xd1)
	for _, f := range permissionV2Fixtures(owner) {
		legacy, _ := encodeAccountPermission(f.p)
		compact, _ := appendAccountPermissionForOwner(nil, owner, f.key, f.p)
		for _, arm := range []struct {
			name string
			v    []byte
		}{{"v1", legacy}, {"v2", compact}} {
			b.Run(f.name+"/"+arm.name, func(b *testing.B) {
				b.ReportAllocs()
				for b.Loop() {
					var err error
					benchmarkDecodedAccountPermission, err = decodeAccountPermissionForOwner(owner, f.key, arm.v)
					if err != nil {
						b.Fatal(err)
					}
				}
			})
		}
	}
}
