package state

import (
	"bytes"
	"errors"
	"fmt"
	"unicode/utf8"

	"github.com/tronprotocol/go-tron/common"
	corepb "github.com/tronprotocol/go-tron/proto/core"
)

// Permission V2 changes only the native row value. The physical permission key,
// explicit presence, and full-key Bloom membership remain unchanged. V1 is a
// raw fallback inside the V4 store, not an old-database compatibility mechanism.
const accountPermissionRowVersion byte = 2

const (
	permissionModeSelf byte = iota
	permissionModeOwner
	permissionModeWitness
	permissionModeActiveOpsID
	permissionModeActiveOpsRaw
)

// These IDs denote immutable byte strings, never the current dynamic property.
// An account retains exactly the operations bitmap it had when it was created.
const (
	permissionOpsLegacy = "\x7f\xff\x1f\xc0\x03\x3e\x00\x00" +
		"\x00\x00\x00\x00\x00\x00\x00\x00" +
		"\x00\x00\x00\x00\x00\x00\x00\x00" +
		"\x00\x00\x00\x00\x00\x00\x00\x00"
	permissionOpsEmpty = "\x00\x00\x00\x00\x00\x00\x00\x00" +
		"\x00\x00\x00\x00\x00\x00\x00\x00" +
		"\x00\x00\x00\x00\x00\x00\x00\x00" +
		"\x00\x00\x00\x00\x00\x00\x00\x00"
)

func permissionOpsID(ops []byte) byte {
	switch string(ops) {
	case permissionOpsLegacy:
		return 1
	case permissionOpsEmpty:
		return 2
	default:
		return 0
	}
}

func permissionOpsByID(id byte) (string, bool) {
	switch id {
	case 1:
		return permissionOpsLegacy, true
	case 2:
		return permissionOpsEmpty, true
	default:
		return "", false
	}
}

func permissionTemplateMode(owner common.Address, key []byte, p *corepb.Permission) byte {
	if p == nil || p.Threshold != 1 || p.ParentId != 0 || len(p.Keys) != 1 ||
		p.Keys[0] == nil || p.Keys[0].Weight != 1 || !bytes.Equal(p.Keys[0].Address, owner[:]) ||
		len(p.ProtoReflect().GetUnknown()) != 0 || len(p.Keys[0].ProtoReflect().GetUnknown()) != 0 {
		return permissionModeSelf
	}
	switch {
	case bytes.Equal(key, accountOwnerPermissionKey) && p.Type == corepb.Permission_Owner &&
		p.Id == 0 && p.PermissionName == "owner" && len(p.Operations) == 0:
		return permissionModeOwner
	case bytes.Equal(key, accountWitnessPermissionKey) && p.Type == corepb.Permission_Witness &&
		p.Id == 1 && p.PermissionName == "witness" && len(p.Operations) == 0:
		return permissionModeWitness
	case len(key) == 5 && key[0] == 2 && key[1] == 0 && key[2] == 0 && key[3] == 0 && key[4] == 2 &&
		p.Type == corepb.Permission_Active && p.Id == 2 && p.PermissionName == "active" && len(p.Operations) == 32:
		if permissionOpsID(p.Operations) != 0 {
			return permissionModeActiveOpsID
		}
		return permissionModeActiveOpsRaw
	default:
		return permissionModeSelf
	}
}

func appendAccountPermissionForOwner(dst []byte, owner common.Address, key []byte, p *corepb.Permission) ([]byte, error) {
	if p == nil {
		return nil, errors.New("cannot encode absent account permission")
	}
	if !accountPermissionRowSchemaOK {
		return nil, errors.New("Permission schema changed; update account row codec")
	}
	if !utf8.ValidString(p.PermissionName) {
		return nil, errors.New("account permission name is not valid UTF-8")
	}
	mode := permissionTemplateMode(owner, key, p)
	if mode == permissionModeSelf {
		self := false
		// Opaque future fields retain the existing raw representation. They are
		// never inspected for addresses or treated as a default template.
		unknown := len(p.ProtoReflect().GetUnknown()) != 0
		for _, k := range p.Keys {
			if k != nil {
				self = self || bytes.Equal(k.Address, owner[:])
				unknown = unknown || len(k.ProtoReflect().GetUnknown()) != 0
			}
		}
		if !self || unknown {
			return appendAccountPermission(dst, p)
		}
	}
	e := newAccountRowEncoderAppend(accountRowPermission, dst, 64)
	e.buf[4] = accountPermissionRowVersion
	e.u8(mode)
	switch mode {
	case permissionModeOwner, permissionModeWitness:
	case permissionModeActiveOpsID:
		e.u8(permissionOpsID(p.Operations))
	case permissionModeActiveOpsRaw:
		e.buf = append(e.buf, p.Operations...)
	case permissionModeSelf:
		appendAccountPermissionFields(e, p, owner[:])
	}
	return e.finish()
}

// One allocation owns the template protobuf, its key, address and bitmap. No
// decoded field aliases a shared dictionary, the input row, or another result.
type decodedPermissionTemplate struct {
	permission corepb.Permission
	key        corepb.Key
	keys       [1]*corepb.Key
	address    common.Address
	operations [32]byte
}

func decodeAccountPermissionForOwner(owner common.Address, key, value []byte) (*corepb.Permission, error) {
	if !isVersionedAccountRow(value) || len(value) < 6 || value[4] != accountPermissionRowVersion {
		return decodeAccountPermissionValue(value)
	}
	if value[5] != accountRowPermission {
		return nil, fmt.Errorf("account row kind %d, want %d", value[5], accountRowPermission)
	}
	if len(value) < 7 {
		return nil, errors.New("truncated account permission V2 mode")
	}
	d := accountRowDecoder{data: value, off: 7}
	mode := value[6]
	if mode == permissionModeSelf {
		return decodeAccountPermissionFields(&d, owner[:])
	}
	var typ corepb.Permission_PermissionType
	var id int32
	var name string
	switch mode {
	case permissionModeOwner:
		if !bytes.Equal(key, accountOwnerPermissionKey) {
			return nil, errors.New("owner permission template under non-owner key")
		}
		typ, id, name = corepb.Permission_Owner, 0, "owner"
	case permissionModeWitness:
		if !bytes.Equal(key, accountWitnessPermissionKey) {
			return nil, errors.New("witness permission template under non-witness key")
		}
		typ, id, name = corepb.Permission_Witness, 1, "witness"
	case permissionModeActiveOpsID, permissionModeActiveOpsRaw:
		if len(key) != 5 || key[0] != 2 || key[1] != 0 || key[2] != 0 || key[3] != 0 || key[4] != 2 {
			return nil, errors.New("active permission template under non-default-active key")
		}
		typ, id, name = corepb.Permission_Active, 2, "active"
	default:
		return nil, fmt.Errorf("unknown account permission V2 mode %d", mode)
	}
	var ops []byte
	var constantOps string
	if mode == permissionModeActiveOpsID {
		opsID, err := d.u8()
		if err != nil {
			return nil, err
		}
		var ok bool
		constantOps, ok = permissionOpsByID(opsID)
		if !ok {
			return nil, fmt.Errorf("unknown immutable permission operations id %d", opsID)
		}
	} else if mode == permissionModeActiveOpsRaw {
		var err error
		ops, err = d.take(32)
		if err != nil {
			return nil, err
		}
	}
	if err := d.done(); err != nil {
		return nil, err
	}
	r := &decodedPermissionTemplate{address: owner}
	r.permission = corepb.Permission{Type: typ, Id: id, PermissionName: name, Threshold: 1}
	r.key.Address, r.key.Weight = r.address[:], 1
	r.keys[0] = &r.key
	r.permission.Keys = r.keys[:]
	if mode == permissionModeActiveOpsID || mode == permissionModeActiveOpsRaw {
		if constantOps != "" {
			copy(r.operations[:], constantOps)
		} else {
			copy(r.operations[:], ops)
		}
		r.permission.Operations = r.operations[:]
	}
	return &r.permission, nil
}
