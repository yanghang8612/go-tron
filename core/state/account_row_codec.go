package state

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"unicode/utf8"

	corepb "github.com/tronprotocol/go-tron/proto/core"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// Account-local rows use an internal storage encoding instead of protobuf.
// The first byte (0x47) is an invalid protobuf wire tag, so accidentally stored
// protobuf data is rejected instead of being interpreted as rooted state.
var accountRowMagic = [...]byte{0x47, 0x54, 0x52, 0x41} // "GTRA"

const (
	accountRowVersion byte = 1

	accountRowPermission byte = 1
	accountRowVote       byte = 2
	accountRowFrozen     byte = 3
	accountRowUnfrozenV2 byte = 4
	accountRowResource   byte = 5
)

var (
	permissionEnumName = corepb.Permission_PermissionType(0).Descriptor().FullName()
	resourceEnumName   = corepb.ResourceCode(0).Descriptor().FullName()
	keyMessageName     = new(corepb.Key).ProtoReflect().Descriptor().FullName()
	frozenMessageName  = new(corepb.Account_Frozen).ProtoReflect().Descriptor().FullName()

	accountPermissionSchema = []accountFieldSchema{
		{1, protoreflect.EnumKind, false, false, false, permissionEnumName},
		{2, protoreflect.Int32Kind, false, false, false, ""},
		{3, protoreflect.StringKind, false, false, false, ""},
		{4, protoreflect.Int64Kind, false, false, false, ""},
		{5, protoreflect.Int32Kind, false, false, false, ""},
		{6, protoreflect.BytesKind, false, false, false, ""},
		{7, protoreflect.MessageKind, true, false, false, keyMessageName},
	}
	accountKeySchema = []accountFieldSchema{
		{1, protoreflect.BytesKind, false, false, false, ""},
		{2, protoreflect.Int64Kind, false, false, false, ""},
	}
	accountVoteSchema = []accountFieldSchema{
		{1, protoreflect.BytesKind, false, false, false, ""},
		{2, protoreflect.Int64Kind, false, false, false, ""},
	}
	accountFrozenSchema = []accountFieldSchema{
		{1, protoreflect.Int64Kind, false, false, false, ""},
		{2, protoreflect.Int64Kind, false, false, false, ""},
	}
	accountUnfrozenV2Schema = []accountFieldSchema{
		{1, protoreflect.EnumKind, false, false, false, resourceEnumName},
		{3, protoreflect.Int64Kind, false, false, false, ""},
		{4, protoreflect.Int64Kind, false, false, false, ""},
	}
	accountResourceSchema = []accountFieldSchema{
		{1, protoreflect.Int64Kind, false, false, false, ""},
		{2, protoreflect.MessageKind, false, false, true, frozenMessageName},
		{3, protoreflect.Int64Kind, false, false, false, ""},
		{4, protoreflect.Int64Kind, false, false, false, ""},
		{5, protoreflect.Int64Kind, false, false, false, ""},
		{6, protoreflect.Int64Kind, false, false, false, ""},
		{7, protoreflect.Int64Kind, false, false, false, ""},
		{8, protoreflect.Int64Kind, false, false, false, ""},
		{9, protoreflect.Int64Kind, false, false, false, ""},
		{10, protoreflect.Int64Kind, false, false, false, ""},
		{11, protoreflect.Int64Kind, false, false, false, ""},
		{12, protoreflect.BoolKind, false, false, false, ""},
	}

	accountPermissionRowSchemaOK = accountMessageSchemaMatches(new(corepb.Permission), accountPermissionSchema...) &&
		accountMessageSchemaMatches(new(corepb.Key), accountKeySchema...)
	accountVoteRowSchemaOK     = accountMessageSchemaMatches(new(corepb.Vote), accountVoteSchema...)
	accountFrozenRowSchemaOK   = accountMessageSchemaMatches(new(corepb.Account_Frozen), accountFrozenSchema...)
	accountUnfrozenV2SchemaOK  = accountMessageSchemaMatches(new(corepb.Account_UnFreezeV2), accountUnfrozenV2Schema...)
	accountResourceRowSchemaOK = accountMessageSchemaMatches(new(corepb.Account_AccountResource), accountResourceSchema...)
)

type accountFieldSchema struct {
	number      protoreflect.FieldNumber
	kind        protoreflect.Kind
	list        bool
	mapField    bool
	hasPresence bool
	typeName    protoreflect.FullName
}

func accountMessageSchemaMatches(message proto.Message, expected ...accountFieldSchema) bool {
	fields := message.ProtoReflect().Descriptor().Fields()
	if fields.Len() != len(expected) {
		return false
	}
	for _, want := range expected {
		field := fields.ByNumber(want.number)
		if field == nil || field.Kind() != want.kind || field.IsList() != want.list ||
			field.IsMap() != want.mapField || field.HasPresence() != want.hasPresence {
			return false
		}
		if want.typeName != "" {
			var got protoreflect.FullName
			switch field.Kind() {
			case protoreflect.MessageKind:
				got = field.Message().FullName()
			case protoreflect.EnumKind:
				got = field.Enum().FullName()
			default:
				return false
			}
			if got != want.typeName {
				return false
			}
		}
	}
	return true
}

type accountRowEncoder struct {
	buf []byte
}

func newAccountRowEncoderAppend(kind byte, dst []byte, capacity int) *accountRowEncoder {
	if capacity < 6 {
		capacity = 6
	}
	if cap(dst) < capacity {
		dst = make([]byte, 0, capacity)
	} else {
		dst = dst[:0]
	}
	e := &accountRowEncoder{buf: dst}
	e.buf = append(e.buf, accountRowMagic[:]...)
	e.buf = append(e.buf, accountRowVersion, kind)
	return e
}

func (e *accountRowEncoder) u8(v byte) { e.buf = append(e.buf, v) }
func (e *accountRowEncoder) uvarint(v uint64) {
	e.buf = binary.AppendUvarint(e.buf, v)
}
func (e *accountRowEncoder) i32(v int32) { e.i64(int64(v)) }
func (e *accountRowEncoder) i64(v int64) {
	e.uvarint(uint64(v<<1) ^ uint64(v>>63))
}
func (e *accountRowEncoder) bytes(v []byte) {
	e.uvarint(uint64(len(v)))
	e.buf = append(e.buf, v...)
}
func (e *accountRowEncoder) finish() ([]byte, error) {
	return e.buf, nil
}

type accountRowDecoder struct {
	data []byte
	off  int
}

func isVersionedAccountRow(data []byte) bool {
	return len(data) >= len(accountRowMagic) && bytes.Equal(data[:len(accountRowMagic)], accountRowMagic[:])
}

func newAccountRowDecoder(data []byte, kind byte) (*accountRowDecoder, error) {
	if !isVersionedAccountRow(data) {
		return nil, errors.New("non-native account rooted-state row")
	}
	if len(data) < 6 {
		return nil, errors.New("truncated account row header")
	}
	if data[4] != accountRowVersion {
		return nil, fmt.Errorf("unsupported account row version %d", data[4])
	}
	if data[5] != kind {
		return nil, fmt.Errorf("account row kind %d, want %d", data[5], kind)
	}
	return &accountRowDecoder{data: data, off: 6}, nil
}

func (d *accountRowDecoder) take(n int) ([]byte, error) {
	if n < 0 || d.off > len(d.data)-n {
		return nil, errors.New("truncated account row")
	}
	out := d.data[d.off : d.off+n]
	d.off += n
	return out, nil
}
func (d *accountRowDecoder) u8() (byte, error) {
	v, err := d.take(1)
	if err != nil {
		return 0, err
	}
	return v[0], nil
}
func (d *accountRowDecoder) uvarint() (uint64, error) {
	value, size := binary.Uvarint(d.data[d.off:])
	if size == 0 {
		return 0, errors.New("truncated account row varint")
	}
	if size < 0 {
		return 0, errors.New("overflowing account row varint")
	}
	canonical := 1
	for check := value; check >= 0x80; check >>= 7 {
		canonical++
	}
	if size != canonical {
		return 0, errors.New("non-canonical account row varint")
	}
	d.off += size
	return value, nil
}
func (d *accountRowDecoder) i32() (int32, error) {
	v, err := d.i64()
	if err != nil {
		return 0, err
	}
	if int64(int32(v)) != v {
		return 0, errors.New("account row int32 overflow")
	}
	return int32(v), nil
}
func (d *accountRowDecoder) i64() (int64, error) {
	v, err := d.uvarint()
	if err != nil {
		return 0, err
	}
	return int64(v>>1) ^ -int64(v&1), nil
}
func (d *accountRowDecoder) bytes() ([]byte, error) {
	n, err := d.uvarint()
	if err != nil {
		return nil, err
	}
	if n > uint64(len(d.data)-d.off) {
		return nil, errors.New("truncated account row byte field")
	}
	v, err := d.take(int(n))
	if err != nil {
		return nil, err
	}
	return append([]byte(nil), v...), nil
}
func (d *accountRowDecoder) done() error {
	if d.off != len(d.data) {
		return fmt.Errorf("account row has %d trailing bytes", len(d.data)-d.off)
	}
	return nil
}

func appendProtoUnknown(e *accountRowEncoder, message proto.Message) {
	if message == nil {
		e.bytes(nil)
		return
	}
	e.bytes(message.ProtoReflect().GetUnknown())
}

func restoreProtoUnknown(d *accountRowDecoder, message proto.Message) error {
	unknown, err := d.bytes()
	if err != nil {
		return err
	}
	message.ProtoReflect().SetUnknown(unknown)
	return nil
}

func encodeAccountFrozen(entry *corepb.Account_Frozen) ([]byte, error) {
	return appendAccountFrozen(nil, entry)
}

func appendAccountFrozen(dst []byte, entry *corepb.Account_Frozen) ([]byte, error) {
	if !accountFrozenRowSchemaOK {
		return nil, errors.New("Account_Frozen schema changed; update account row codec")
	}
	e := newAccountRowEncoderAppend(accountRowFrozen, dst, 16)
	e.i64(entry.GetFrozenBalance())
	e.i64(entry.GetExpireTime())
	appendProtoUnknown(e, entry)
	return e.finish()
}

func decodeAccountFrozen(value []byte) (*corepb.Account_Frozen, error) {
	d, err := newAccountRowDecoder(value, accountRowFrozen)
	if err != nil {
		return nil, err
	}
	balance, err := d.i64()
	if err != nil {
		return nil, err
	}
	expireTime, err := d.i64()
	if err != nil {
		return nil, err
	}
	entry := &corepb.Account_Frozen{FrozenBalance: balance, ExpireTime: expireTime}
	if err := restoreProtoUnknown(d, entry); err != nil {
		return nil, err
	}
	return entry, d.done()
}

func encodeAccountVote(vote *corepb.Vote) ([]byte, error) {
	return appendAccountVote(nil, vote)
}

func appendAccountVote(dst []byte, vote *corepb.Vote) ([]byte, error) {
	if !accountVoteRowSchemaOK {
		return nil, errors.New("Vote schema changed; update account row codec")
	}
	e := newAccountRowEncoderAppend(accountRowVote, dst, 32)
	e.bytes(vote.GetVoteAddress())
	e.i64(vote.GetVoteCount())
	appendProtoUnknown(e, vote)
	return e.finish()
}

func decodeAccountVote(value []byte) (*corepb.Vote, error) {
	d, err := newAccountRowDecoder(value, accountRowVote)
	if err != nil {
		return nil, err
	}
	address, err := d.bytes()
	if err != nil {
		return nil, err
	}
	count, err := d.i64()
	if err != nil {
		return nil, err
	}
	vote := &corepb.Vote{VoteAddress: address, VoteCount: count}
	if err := restoreProtoUnknown(d, vote); err != nil {
		return nil, err
	}
	return vote, d.done()
}

func encodeAccountUnfrozenV2(entry *corepb.Account_UnFreezeV2) ([]byte, error) {
	return appendAccountUnfrozenV2(nil, entry)
}

func appendAccountUnfrozenV2(dst []byte, entry *corepb.Account_UnFreezeV2) ([]byte, error) {
	if !accountUnfrozenV2SchemaOK {
		return nil, errors.New("Account_UnFreezeV2 schema changed; update account row codec")
	}
	e := newAccountRowEncoderAppend(accountRowUnfrozenV2, dst, 24)
	e.i32(int32(entry.GetType()))
	e.i64(entry.GetUnfreezeAmount())
	e.i64(entry.GetUnfreezeExpireTime())
	appendProtoUnknown(e, entry)
	return e.finish()
}

func decodeAccountUnfrozenV2(value []byte) (*corepb.Account_UnFreezeV2, error) {
	d, err := newAccountRowDecoder(value, accountRowUnfrozenV2)
	if err != nil {
		return nil, err
	}
	resource, err := d.i32()
	if err != nil {
		return nil, err
	}
	amount, err := d.i64()
	if err != nil {
		return nil, err
	}
	expireTime, err := d.i64()
	if err != nil {
		return nil, err
	}
	entry := &corepb.Account_UnFreezeV2{Type: corepb.ResourceCode(resource), UnfreezeAmount: amount, UnfreezeExpireTime: expireTime}
	if err := restoreProtoUnknown(d, entry); err != nil {
		return nil, err
	}
	return entry, d.done()
}

func encodeAccountResource(resource *corepb.Account_AccountResource) ([]byte, error) {
	return appendAccountResource(nil, resource)
}

func appendAccountResource(dst []byte, resource *corepb.Account_AccountResource) ([]byte, error) {
	if !accountResourceRowSchemaOK || !accountFrozenRowSchemaOK {
		return nil, errors.New("AccountResource schema changed; update account row codec")
	}
	e := newAccountRowEncoderAppend(accountRowResource, dst, 64)
	e.i64(resource.GetEnergyUsage())
	if resource.FrozenBalanceForEnergy == nil {
		e.u8(0)
	} else {
		e.u8(1)
		e.i64(resource.FrozenBalanceForEnergy.GetFrozenBalance())
		e.i64(resource.FrozenBalanceForEnergy.GetExpireTime())
		appendProtoUnknown(e, resource.FrozenBalanceForEnergy)
	}
	e.i64(resource.GetLatestConsumeTimeForEnergy())
	e.i64(resource.GetAcquiredDelegatedFrozenBalanceForEnergy())
	e.i64(resource.GetDelegatedFrozenBalanceForEnergy())
	e.i64(resource.GetStorageLimit())
	e.i64(resource.GetStorageUsage())
	e.i64(resource.GetLatestExchangeStorageTime())
	e.i64(resource.GetEnergyWindowSize())
	e.i64(resource.GetDelegatedFrozenV2BalanceForEnergy())
	e.i64(resource.GetAcquiredDelegatedFrozenV2BalanceForEnergy())
	if resource.GetEnergyWindowOptimized() {
		e.u8(1)
	} else {
		e.u8(0)
	}
	appendProtoUnknown(e, resource)
	return e.finish()
}

func decodeAccountResource(value []byte) (*corepb.Account_AccountResource, error) {
	d, err := newAccountRowDecoder(value, accountRowResource)
	if err != nil {
		return nil, err
	}
	resource := new(corepb.Account_AccountResource)
	if resource.EnergyUsage, err = d.i64(); err != nil {
		return nil, err
	}
	present, err := d.u8()
	if err != nil {
		return nil, err
	}
	if present > 1 {
		return nil, fmt.Errorf("invalid frozen-energy presence %d", present)
	}
	if present == 1 {
		frozen := new(corepb.Account_Frozen)
		if frozen.FrozenBalance, err = d.i64(); err != nil {
			return nil, err
		}
		if frozen.ExpireTime, err = d.i64(); err != nil {
			return nil, err
		}
		if err := restoreProtoUnknown(d, frozen); err != nil {
			return nil, err
		}
		resource.FrozenBalanceForEnergy = frozen
	}
	fields := []*int64{
		&resource.LatestConsumeTimeForEnergy,
		&resource.AcquiredDelegatedFrozenBalanceForEnergy,
		&resource.DelegatedFrozenBalanceForEnergy,
		&resource.StorageLimit,
		&resource.StorageUsage,
		&resource.LatestExchangeStorageTime,
		&resource.EnergyWindowSize,
		&resource.DelegatedFrozenV2BalanceForEnergy,
		&resource.AcquiredDelegatedFrozenV2BalanceForEnergy,
	}
	for _, field := range fields {
		if *field, err = d.i64(); err != nil {
			return nil, err
		}
	}
	optimized, err := d.u8()
	if err != nil {
		return nil, err
	}
	if optimized > 1 {
		return nil, fmt.Errorf("invalid energy-window boolean %d", optimized)
	}
	resource.EnergyWindowOptimized = optimized == 1
	if err := restoreProtoUnknown(d, resource); err != nil {
		return nil, err
	}
	return resource, d.done()
}

func encodeAccountPermission(permission *corepb.Permission) ([]byte, error) {
	return appendAccountPermission(nil, permission)
}

func appendAccountPermission(dst []byte, permission *corepb.Permission) ([]byte, error) {
	if !accountPermissionRowSchemaOK {
		return nil, errors.New("Permission schema changed; update account row codec")
	}
	if !utf8.ValidString(permission.GetPermissionName()) {
		return nil, errors.New("account permission name is not valid UTF-8")
	}
	e := newAccountRowEncoderAppend(accountRowPermission, dst, 64)
	e.i32(int32(permission.GetType()))
	e.i32(permission.GetId())
	e.bytes([]byte(permission.GetPermissionName()))
	e.i64(permission.GetThreshold())
	e.i32(permission.GetParentId())
	e.bytes(permission.GetOperations())
	e.uvarint(uint64(len(permission.Keys)))
	for _, key := range permission.Keys {
		if key == nil {
			e.u8(0)
			continue
		}
		e.u8(1)
		e.bytes(key.GetAddress())
		e.i64(key.GetWeight())
		appendProtoUnknown(e, key)
	}
	appendProtoUnknown(e, permission)
	return e.finish()
}

func decodeAccountPermissionValue(value []byte) (*corepb.Permission, error) {
	d, err := newAccountRowDecoder(value, accountRowPermission)
	if err != nil {
		return nil, err
	}
	typeValue, err := d.i32()
	if err != nil {
		return nil, err
	}
	id, err := d.i32()
	if err != nil {
		return nil, err
	}
	name, err := d.bytes()
	if err != nil {
		return nil, err
	}
	if !utf8.Valid(name) {
		return nil, errors.New("account permission name is not valid UTF-8")
	}
	threshold, err := d.i64()
	if err != nil {
		return nil, err
	}
	parentID, err := d.i32()
	if err != nil {
		return nil, err
	}
	operations, err := d.bytes()
	if err != nil {
		return nil, err
	}
	count, err := d.uvarint()
	if err != nil {
		return nil, err
	}
	// Every encoded key needs at least its one-byte presence marker. This also
	// bounds allocation when decoding corrupted input.
	if count > uint64(len(d.data)-d.off) {
		return nil, errors.New("account permission key count exceeds remaining row")
	}
	permission := &corepb.Permission{
		Type: corepb.Permission_PermissionType(typeValue), Id: id,
		PermissionName: string(name), Threshold: threshold, ParentId: parentID,
		Operations: operations, Keys: make([]*corepb.Key, 0, int(count)),
	}
	for i := uint64(0); i < count; i++ {
		present, err := d.u8()
		if err != nil {
			return nil, err
		}
		if present == 0 {
			permission.Keys = append(permission.Keys, nil)
			continue
		}
		if present != 1 {
			return nil, fmt.Errorf("invalid account permission key presence %d", present)
		}
		address, err := d.bytes()
		if err != nil {
			return nil, err
		}
		weight, err := d.i64()
		if err != nil {
			return nil, err
		}
		key := &corepb.Key{Address: address, Weight: weight}
		if err := restoreProtoUnknown(d, key); err != nil {
			return nil, err
		}
		permission.Keys = append(permission.Keys, key)
	}
	if err := restoreProtoUnknown(d, permission); err != nil {
		return nil, err
	}
	return permission, d.done()
}
