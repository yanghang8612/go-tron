package types

import (
	"errors"
	"sort"
	"sync"
	"unicode/utf8"

	corepb "github.com/tronprotocol/go-tron/proto/core"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/runtime/protoiface"
)

const accountKnownFieldCount = 43

// accountDirectMapLayoutOK protects the hand-specialized Account map encoder
// from silently omitting a future protobuf field. A schema change makes the
// fast path decline automatically until its field copy is updated.
var accountDirectMapLayoutOK = verifyAccountDirectMapLayout()

func verifyAccountDirectMapLayout() bool {
	fields := (&corepb.Account{}).ProtoReflect().Descriptor().Fields()
	if fields.Len() != accountKnownFieldCount {
		return false
	}
	for i := 0; i < fields.Len(); i++ {
		field := fields.Get(i)
		if !knownAccountField(field.Number()) {
			return false
		}
		if isAccountMapField(field.Number()) != field.IsMap() {
			return false
		}
		if field.IsMap() && (field.MapKey().Kind() != protoreflect.StringKind || field.MapValue().Kind() != protoreflect.Int64Kind) {
			return false
		}
	}
	return true
}

func knownAccountField(number protowire.Number) bool {
	switch number {
	case 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20,
		21, 22, 23, 24, 25, 26, 30, 31, 32, 33, 34, 35, 36, 37, 41, 42, 46, 47,
		56, 57, 58, 59, 60:
		return true
	default:
		return false
	}
}

func isAccountMapField(number protowire.Number) bool {
	switch number {
	case 6, 18, 20, 56, 58, 59:
		return true
	default:
		return false
	}
}

func accountHasMaps(pb *corepb.Account) bool {
	return len(pb.Asset)+len(pb.LatestAssetOperationTime)+len(pb.FreeAssetNetUsage)+
		len(pb.AssetV2)+len(pb.LatestAssetOperationTimeV2)+len(pb.FreeAssetNetUsageV2) > 0
}

// accountWithoutMaps makes a shallow, marshal-only Account view. Slice and
// nested-message fields are read synchronously and never mutated; the six map
// fields and unknown bytes are emitted separately by marshalAccountDirectMaps.
func accountWithoutMaps(pb *corepb.Account) *corepb.Account {
	return &corepb.Account{
		AccountName: pb.AccountName,
		Type:        pb.Type,
		Address:     pb.Address,
		Balance:     pb.Balance,
		Votes:       pb.Votes,
		Frozen:      pb.Frozen,
		NetUsage:    pb.NetUsage,
		AcquiredDelegatedFrozenBalanceForBandwidth: pb.AcquiredDelegatedFrozenBalanceForBandwidth,
		DelegatedFrozenBalanceForBandwidth:         pb.DelegatedFrozenBalanceForBandwidth,
		OldTronPower:                               pb.OldTronPower,
		TronPower:                                  pb.TronPower,
		AssetOptimized:                             pb.AssetOptimized,
		CreateTime:                                 pb.CreateTime,
		LatestOprationTime:                         pb.LatestOprationTime,
		Allowance:                                  pb.Allowance,
		LatestWithdrawTime:                         pb.LatestWithdrawTime,
		Code:                                       pb.Code,
		IsWitness:                                  pb.IsWitness,
		IsCommittee:                                pb.IsCommittee,
		FrozenSupply:                               pb.FrozenSupply,
		AssetIssuedName:                            pb.AssetIssuedName,
		AssetIssued_ID:                             pb.AssetIssued_ID,
		FreeNetUsage:                               pb.FreeNetUsage,
		LatestConsumeTime:                          pb.LatestConsumeTime,
		LatestConsumeFreeTime:                      pb.LatestConsumeFreeTime,
		AccountId:                                  pb.AccountId,
		NetWindowSize:                              pb.NetWindowSize,
		NetWindowOptimized:                         pb.NetWindowOptimized,
		AccountResource:                            pb.AccountResource,
		CodeHash:                                   pb.CodeHash,
		OwnerPermission:                            pb.OwnerPermission,
		WitnessPermission:                          pb.WitnessPermission,
		ActivePermission:                           pb.ActivePermission,
		FrozenV2:                                   pb.FrozenV2,
		UnfrozenV2:                                 pb.UnfrozenV2,
		DelegatedFrozenV2BalanceForBandwidth:       pb.DelegatedFrozenV2BalanceForBandwidth,
		AcquiredDelegatedFrozenV2BalanceForBandwidth: pb.AcquiredDelegatedFrozenV2BalanceForBandwidth,
	}
}

type accountMapField struct {
	number protowire.Number
	values map[string]int64
}

var accountMapKeysPool = sync.Pool{
	New: func() any {
		keys := make([]string, 0, 64)
		return &keys
	},
}

func appendAccountMap(dst []byte, number protowire.Number, values map[string]int64) ([]byte, error) {
	if len(values) == 0 {
		return dst, nil
	}
	keysPtr := accountMapKeysPool.Get().(*[]string)
	keys := (*keysPtr)[:0]
	if cap(keys) < len(values) {
		keys = make([]string, 0, len(values))
	}
	for key := range values {
		keys = append(keys, key)
	}
	defer func() {
		clear(keys)
		if cap(keys) <= 4096 {
			*keysPtr = keys[:0]
			accountMapKeysPool.Put(keysPtr)
		}
	}()
	sort.Strings(keys)
	for _, key := range keys {
		if !utf8.ValidString(key) {
			return nil, errors.New("string field contains invalid UTF-8")
		}
		value := values[key]
		entrySize := protowire.SizeTag(1) + protowire.SizeBytes(len(key)) +
			protowire.SizeTag(2) + protowire.SizeVarint(uint64(value))
		dst = protowire.AppendTag(dst, number, protowire.BytesType)
		dst = protowire.AppendVarint(dst, uint64(entrySize))
		dst = protowire.AppendTag(dst, 1, protowire.BytesType)
		dst = protowire.AppendString(dst, key)
		dst = protowire.AppendTag(dst, 2, protowire.VarintType)
		dst = protowire.AppendVarint(dst, uint64(value))
	}
	return dst, nil
}

func marshalMessageDeterministic(message protoreflect.Message, hint int) ([]byte, error) {
	methods := message.ProtoMethods()
	if methods == nil || methods.Marshal == nil || methods.Flags&protoiface.SupportMarshalDeterministic == 0 {
		return proto.MarshalOptions{Deterministic: true}.Marshal(message.Interface())
	}
	if hint < 256 {
		hint = 256
	}
	out, err := methods.Marshal(protoiface.MarshalInput{
		Message: message,
		Buf:     make([]byte, 0, hint),
		Flags:   protoiface.MarshalDeterministic,
	})
	if err != nil {
		return nil, err
	}
	if out.Buf == nil {
		return []byte{}, nil
	}
	return out.Buf, nil
}

// marshalAccountDirectMaps preserves protobuf-go's deterministic wire output
// while bypassing reflect.MapKeys/MapIndex for Account's six string→int64 maps.
func marshalAccountDirectMaps(pb *corepb.Account, hint int) ([]byte, error, bool) {
	if !accountDirectMapLayoutOK || !accountHasMaps(pb) {
		return nil, nil, false
	}
	base, err := marshalMessageDeterministic(accountWithoutMaps(pb).ProtoReflect(), 256)
	if err != nil {
		return nil, err, true
	}
	maps := [...]accountMapField{
		{number: 6, values: pb.Asset},
		{number: 18, values: pb.LatestAssetOperationTime},
		{number: 20, values: pb.FreeAssetNetUsage},
		{number: 56, values: pb.AssetV2},
		{number: 58, values: pb.LatestAssetOperationTimeV2},
		{number: 59, values: pb.FreeAssetNetUsageV2},
	}
	unknown := pb.ProtoReflect().GetUnknown()
	if hint < len(base)+len(unknown) {
		hint = len(base) + len(unknown)
	}
	out := make([]byte, 0, hint)
	mapIndex := 0
	for len(base) > 0 {
		number, wireType, tagSize := protowire.ConsumeTag(base)
		if tagSize < 0 {
			return nil, nil, false
		}
		valueSize := protowire.ConsumeFieldValue(number, wireType, base[tagSize:])
		if valueSize < 0 {
			return nil, nil, false
		}
		for mapIndex < len(maps) && maps[mapIndex].number < number {
			out, err = appendAccountMap(out, maps[mapIndex].number, maps[mapIndex].values)
			if err != nil {
				return nil, err, true
			}
			mapIndex++
		}
		fieldSize := tagSize + valueSize
		out = append(out, base[:fieldSize]...)
		base = base[fieldSize:]
	}
	for ; mapIndex < len(maps); mapIndex++ {
		out, err = appendAccountMap(out, maps[mapIndex].number, maps[mapIndex].values)
		if err != nil {
			return nil, err, true
		}
	}
	out = append(out, unknown...)
	return out, nil, true
}

// marshalAccountStorageCore encodes the Account fields retained in the v3
// account-latest envelope. The six TRC10 maps, Owner/Witness/Active permissions,
// votes, Stake V1/V2 fields, TRC10 frozen supply, and AccountResource live in
// account-local KV domains and are materialized only for full wire/API Account
// responses.
func marshalAccountStorageCore(pb *corepb.Account, hint int) ([]byte, error) {
	core := accountWithoutMaps(pb)
	core.OwnerPermission = nil
	core.WitnessPermission = nil
	core.ActivePermission = nil
	core.Votes = nil
	core.FrozenV2 = nil
	core.UnfrozenV2 = nil
	core.FrozenSupply = nil
	core.AccountResource = nil
	core.Frozen = nil
	core.TronPower = nil
	core.ProtoReflect().SetUnknown(pb.ProtoReflect().GetUnknown())
	return marshalMessageDeterministic(core.ProtoReflect(), hint)
}
