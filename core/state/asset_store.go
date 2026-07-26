package state

import (
	"encoding/binary"
	"errors"
	"unicode/utf8"

	"github.com/tronprotocol/go-tron/core/state/kvdomains"
	contractpb "github.com/tronprotocol/go-tron/proto/core/contract"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// TRC10 asset records are rooted into the reserved system account's SystemAsset
// KV so they rewind with the full state root, replacing the flat
// `ast-`/`astl-`/`astn-`/`asto-`/`asti-` rawdb buckets. The five java-tron
// stores they mirror coexist forever once AllowSameTokenName splits the
// name-space, so all five legs are rooted (the locked design decision):
//
//   - V2 (AssetIssueV2Store, `ast-`): the ID-keyed metadata bucket. Written for
//     EVERY issuance, pre- and post-fork (asset_issue.go writes it
//     unconditionally), so its token-id set is a superset of the legacy bucket.
//   - Legacy (AssetIssueStore, `astl-`): the pre-fork name-keyed metadata
//     bucket. Written only while !AllowSameTokenName, then frozen — but the
//     historical records must stay self-describing in every pre-fork root.
//   - Name index (`astn-`): token name -> token id, used by the legacy
//     name-uniqueness precheck and bandwidth/exchange name resolution.
//   - Owner index (`asto-`): issuer 21-byte address -> token id, enforcing
//     java-tron's one-asset-per-account rule.
//   - Issue time (`asti-`): token id -> block timestamp (ms) of issuance.
//
// All five share one domain (SystemAsset) but address disjoint key-spaces. A
// single-byte tag disambiguates them so a name can never collide with an id of
// the same bytes, mirroring the prior five-prefix split:
//
//	assetV2Tag        || u64-BE(tokenID)   (V2 metadata)
//	assetLegacyTag    || name              (legacy metadata)
//	assetNameIndexTag || name              (name -> id)
//	assetOwnerIndexTag|| owner-21B         (owner -> id)
//	assetIssueTimeTag || u64-BE(tokenID)   (issue time)
//
// The value encoding reuses the existing wire formats verbatim — proto.Marshal
// for the AssetIssueContract metadata, 8-byte big-endian for the id/time
// scalars — so a rooted record is byte-identical to what the flat bucket held;
// no new on-disk encoding lineage is introduced.
const (
	assetV2Tag         byte = 0x01
	assetLegacyTag     byte = 0x02
	assetNameIndexTag  byte = 0x03
	assetOwnerIndexTag byte = 0x04
	assetIssueTimeTag  byte = 0x05
)

const (
	assetIssueOwnerBytes = iota
	assetIssueNameBytes
	assetIssueAbbrBytes
	assetIssueDescriptionBytes
	assetIssueURLBytes
	assetIssueByteFieldCount
)

// Recent mainnet records in the sync hot path use at most 110 combined bytes
// across these fields. Unusually large historical descriptions or URLs fall
// back to an exact dynamic arena.
const assetIssueInlineByteArenaSize = 128

type decodedAssetIssue struct {
	contract  contractpb.AssetIssueContract
	byteArena [assetIssueInlineByteArenaSize]byte
}

var assetIssueByteArenaLayoutOK = verifyAssetIssueByteArenaLayout()

// verifyAssetIssueByteArenaLayout ties the allocation reserve below to the
// generated AssetIssueContract schema. A future protobuf regeneration that
// changes the relevant field shapes falls back to ordinary proto.Unmarshal.
func verifyAssetIssueByteArenaLayout() bool {
	fields := (&contractpb.AssetIssueContract{}).ProtoReflect().Descriptor().Fields()
	return fields.Len() == 19 &&
		assetIssueFieldShape(fields, 1, protoreflect.BytesKind, false) &&
		assetIssueFieldShape(fields, 2, protoreflect.BytesKind, false) &&
		assetIssueFieldShape(fields, 3, protoreflect.BytesKind, false) &&
		assetIssueFieldShape(fields, 4, protoreflect.Int64Kind, false) &&
		assetIssueFieldShape(fields, 5, protoreflect.MessageKind, true) &&
		assetIssueFieldShape(fields, 6, protoreflect.Int32Kind, false) &&
		assetIssueFieldShape(fields, 7, protoreflect.Int32Kind, false) &&
		assetIssueFieldShape(fields, 8, protoreflect.Int32Kind, false) &&
		assetIssueFieldShape(fields, 9, protoreflect.Int64Kind, false) &&
		assetIssueFieldShape(fields, 10, protoreflect.Int64Kind, false) &&
		assetIssueFieldShape(fields, 11, protoreflect.Int64Kind, false) &&
		assetIssueFieldShape(fields, 16, protoreflect.Int32Kind, false) &&
		assetIssueFieldShape(fields, 20, protoreflect.BytesKind, false) &&
		assetIssueFieldShape(fields, 21, protoreflect.BytesKind, false) &&
		assetIssueFieldShape(fields, 22, protoreflect.Int64Kind, false) &&
		assetIssueFieldShape(fields, 23, protoreflect.Int64Kind, false) &&
		assetIssueFieldShape(fields, 24, protoreflect.Int64Kind, false) &&
		assetIssueFieldShape(fields, 25, protoreflect.Int64Kind, false) &&
		assetIssueFieldShape(fields, 41, protoreflect.StringKind, false)
}

func assetIssueFieldShape(fields protoreflect.FieldDescriptors, number protoreflect.FieldNumber, kind protoreflect.Kind, list bool) bool {
	field := fields.ByNumber(number)
	return field != nil && field.Kind() == kind && field.IsList() == list
}

func assetIssueByteFieldIndex(number protowire.Number) int {
	switch number {
	case 1:
		return assetIssueOwnerBytes
	case 2:
		return assetIssueNameBytes
	case 3:
		return assetIssueAbbrBytes
	case 20:
		return assetIssueDescriptionBytes
	case 21:
		return assetIssueURLBytes
	default:
		return -1
	}
}

func setAssetIssueBytes(c *contractpb.AssetIssueContract, index int, field []byte) {
	switch index {
	case assetIssueOwnerBytes:
		c.OwnerAddress = field
	case assetIssueNameBytes:
		c.Name = field
	case assetIssueAbbrBytes:
		c.Abbr = field
	case assetIssueDescriptionBytes:
		c.Description = field
	case assetIssueURLBytes:
		c.Url = field
	}
}

// decodeAssetIssueArena decodes the scalar and nested fields directly while
// temporarily borrowing the five bytes fields from raw. Once the envelope is
// complete, their final (last-occurrence) values are copied into one owned
// arena. This retains protobuf ownership and duplicate-field semantics while
// replacing five small allocations with one. Unknown fields remain owned and
// receive the same tag canonicalization as the generated decoder. Malformed or
// group-bearing envelopes fall back to that decoder, retaining its recursion
// limits and exact errors on the cold path.
func decodeAssetIssueArena(raw []byte) (*contractpb.AssetIssueContract, error) {
	decoded := new(decodedAssetIssue)
	c := &decoded.contract
	var byteValues [assetIssueByteFieldCount][]byte
	var byteSeen [assetIssueByteFieldCount]bool
	var unknown []byte
	for data := raw; len(data) != 0; {
		number, wireType, tagSize := protowire.ConsumeTag(data)
		if tagSize < 0 || !number.IsValid() || wireType == protowire.StartGroupType || wireType == protowire.EndGroupType {
			return decodeAssetIssueGenerated(raw)
		}
		valueSize := protowire.ConsumeFieldValue(number, wireType, data[tagSize:])
		if valueSize < 0 {
			return decodeAssetIssueGenerated(raw)
		}
		fieldSize := tagSize + valueSize
		fieldData := data[:fieldSize]
		known := true
		switch {
		case wireType == protowire.BytesType && assetIssueByteFieldIndex(number) >= 0:
			value, _ := protowire.ConsumeBytes(fieldData[tagSize:])
			index := assetIssueByteFieldIndex(number)
			byteValues[index] = value
			byteSeen[index] = true
		case number == 4 && wireType == protowire.VarintType:
			value, _ := protowire.ConsumeVarint(fieldData[tagSize:])
			c.TotalSupply = int64(value)
		case number == 5 && wireType == protowire.BytesType:
			value, _ := protowire.ConsumeBytes(fieldData[tagSize:])
			frozen := &contractpb.AssetIssueContract_FrozenSupply{}
			if err := proto.Unmarshal(value, frozen); err != nil {
				return nil, err
			}
			c.FrozenSupply = append(c.FrozenSupply, frozen)
		case number == 6 && wireType == protowire.VarintType:
			value, _ := protowire.ConsumeVarint(fieldData[tagSize:])
			c.TrxNum = int32(value)
		case number == 7 && wireType == protowire.VarintType:
			value, _ := protowire.ConsumeVarint(fieldData[tagSize:])
			c.Precision = int32(value)
		case number == 8 && wireType == protowire.VarintType:
			value, _ := protowire.ConsumeVarint(fieldData[tagSize:])
			c.Num = int32(value)
		case number == 9 && wireType == protowire.VarintType:
			value, _ := protowire.ConsumeVarint(fieldData[tagSize:])
			c.StartTime = int64(value)
		case number == 10 && wireType == protowire.VarintType:
			value, _ := protowire.ConsumeVarint(fieldData[tagSize:])
			c.EndTime = int64(value)
		case number == 11 && wireType == protowire.VarintType:
			value, _ := protowire.ConsumeVarint(fieldData[tagSize:])
			c.Order = int64(value)
		case number == 16 && wireType == protowire.VarintType:
			value, _ := protowire.ConsumeVarint(fieldData[tagSize:])
			c.VoteScore = int32(value)
		case number == 22 && wireType == protowire.VarintType:
			value, _ := protowire.ConsumeVarint(fieldData[tagSize:])
			c.FreeAssetNetLimit = int64(value)
		case number == 23 && wireType == protowire.VarintType:
			value, _ := protowire.ConsumeVarint(fieldData[tagSize:])
			c.PublicFreeAssetNetLimit = int64(value)
		case number == 24 && wireType == protowire.VarintType:
			value, _ := protowire.ConsumeVarint(fieldData[tagSize:])
			c.PublicFreeAssetNetUsage = int64(value)
		case number == 25 && wireType == protowire.VarintType:
			value, _ := protowire.ConsumeVarint(fieldData[tagSize:])
			c.PublicLatestFreeNetTime = int64(value)
		case number == 41 && wireType == protowire.BytesType:
			value, _ := protowire.ConsumeBytes(fieldData[tagSize:])
			if !utf8.Valid(value) {
				return nil, errors.New("proto: field protocol.AssetIssueContract.id contains invalid UTF-8")
			}
			c.Id = string(value)
		default:
			known = false
		}
		if !known {
			unknown = protowire.AppendTag(unknown, number, wireType)
			unknown = append(unknown, fieldData[tagSize:]...)
		}
		data = data[fieldSize:]
	}
	if len(unknown) != 0 {
		c.ProtoReflect().SetUnknown(unknown)
	}
	total := 0
	for _, value := range byteValues {
		// The selected last occurrences are disjoint regions of raw.
		if len(value) > len(raw)-total {
			return nil, errors.New("asset issue byte fields exceed wire envelope")
		}
		total += len(value)
	}
	if total != 0 {
		var arena []byte
		if total <= len(decoded.byteArena) {
			arena = decoded.byteArena[:total]
		} else {
			arena = make([]byte, total)
		}
		offset := 0
		for index, value := range byteValues {
			start := offset
			offset += len(value)
			field := arena[start:offset:offset]
			copy(field, value)
			setAssetIssueBytes(c, index, field)
		}
	}
	for index, seen := range byteSeen {
		if seen && len(byteValues[index]) == 0 {
			setAssetIssueBytes(c, index, []byte{})
		}
	}
	return c, nil
}

func decodeAssetIssueGenerated(raw []byte) (*contractpb.AssetIssueContract, error) {
	c := &contractpb.AssetIssueContract{}
	if err := proto.Unmarshal(raw, c); err != nil {
		return nil, err
	}
	return c, nil
}

func decodeAssetIssue(raw []byte) (*contractpb.AssetIssueContract, error) {
	if assetIssueByteArenaLayoutOK {
		return decodeAssetIssueArena(raw)
	}
	return decodeAssetIssueGenerated(raw)
}

// assetIDKey builds a transient tag||u64-BE(id) logical key (V2 metadata,
// issue time). Account-KV reads consume the key synchronously and writes copy
// it into their owned composite key before returning.
func (s *StateDB) assetIDKey(tag byte, tokenID int64) []byte {
	k := s.assetIDKeyScratch[:]
	k[0] = tag
	binary.BigEndian.PutUint64(k[1:], uint64(tokenID))
	return k
}

// assetBytesKey builds a tag||raw-bytes logical key (legacy metadata by name,
// name index by name, owner index by address).
func assetBytesKey(tag byte, raw []byte) []byte {
	k := make([]byte, 1+len(raw))
	k[0] = tag
	copy(k[1:], raw)
	return k
}

// readAssetMeta resolves one AssetIssueContract leg, swallowing a KV error to
// nil to match the prior rawdb reader's defensive behavior (read sites treat
// nil as absent).
func (s *StateDB) readAssetMeta(key []byte) *contractpb.AssetIssueContract {
	raw, ok, err := s.systemKVGetForDecoding(kvdomains.SystemAsset, key)
	if err != nil || !ok || len(raw) == 0 {
		return nil
	}
	c, err := decodeAssetIssue(raw)
	if err != nil {
		return nil
	}
	return c
}

// writeAssetMeta stages one AssetIssueContract leg into the system-KV. The error
// is non-nil only for a proto marshal failure or an unregistered domain (a
// programmer error), since SystemAsset is registered at init.
func (s *StateDB) writeAssetMeta(key []byte, c *contractpb.AssetIssueContract) error {
	data, err := proto.Marshal(c)
	if err != nil {
		return err
	}
	return s.SystemKVPut(kvdomains.SystemAsset, key, data)
}

// ReadAssetIssue returns the rooted V2 (ID-keyed) AssetIssueContract for
// tokenID, or nil if absent. Mirrors java-tron AssetIssueV2Store.
func (s *StateDB) ReadAssetIssue(tokenID int64) *contractpb.AssetIssueContract {
	return s.readAssetMeta(s.assetIDKey(assetV2Tag, tokenID))
}

// WriteAssetIssue stages the V2 (ID-keyed) AssetIssueContract for tokenID.
func (s *StateDB) WriteAssetIssue(tokenID int64, c *contractpb.AssetIssueContract) error {
	return s.writeAssetMeta(s.assetIDKey(assetV2Tag, tokenID), c)
}

// ReadAssetIssueByName returns the rooted legacy (name-keyed) AssetIssueContract,
// or nil if absent. Mirrors java-tron's pre-AllowSameTokenName AssetIssueStore.
func (s *StateDB) ReadAssetIssueByName(name []byte) *contractpb.AssetIssueContract {
	return s.readAssetMeta(assetBytesKey(assetLegacyTag, name))
}

// WriteAssetIssueByName stages the legacy (name-keyed) AssetIssueContract.
func (s *StateDB) WriteAssetIssueByName(name []byte, c *contractpb.AssetIssueContract) error {
	return s.writeAssetMeta(assetBytesKey(assetLegacyTag, name), c)
}

// ReadAssetNameIndex returns the token id registered for name, and whether it
// exists. A KV error or short value reads as not-found, matching the prior
// rawdb reader.
func (s *StateDB) ReadAssetNameIndex(name []byte) (int64, bool) {
	raw, ok, err := s.systemKVGetForDecoding(kvdomains.SystemAsset, assetBytesKey(assetNameIndexTag, name))
	if err != nil || !ok || len(raw) < 8 {
		return 0, false
	}
	return int64(binary.BigEndian.Uint64(raw[:8])), true
}

// WriteAssetNameIndex stages a name -> token id mapping.
func (s *StateDB) WriteAssetNameIndex(name []byte, tokenID int64) error {
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, uint64(tokenID))
	return s.SystemKVPut(kvdomains.SystemAsset, assetBytesKey(assetNameIndexTag, name), buf)
}

// ReadAssetOwnerIndex returns the token id issued by ownerAddr (21-byte TRON
// address), and whether it exists.
func (s *StateDB) ReadAssetOwnerIndex(ownerAddr []byte) (int64, bool) {
	raw, ok, err := s.systemKVGetForDecoding(kvdomains.SystemAsset, assetBytesKey(assetOwnerIndexTag, ownerAddr))
	if err != nil || !ok || len(raw) < 8 {
		return 0, false
	}
	return int64(binary.BigEndian.Uint64(raw[:8])), true
}

// WriteAssetOwnerIndex stages an ownerAddr -> token id mapping, enforcing
// java-tron's one-asset-per-account rule at the storage layer.
func (s *StateDB) WriteAssetOwnerIndex(ownerAddr []byte, tokenID int64) error {
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, uint64(tokenID))
	return s.SystemKVPut(kvdomains.SystemAsset, assetBytesKey(assetOwnerIndexTag, ownerAddr), buf)
}

// ReadAssetIssueTime returns the issuance block timestamp (ms) for tokenID, or 0
// if absent.
func (s *StateDB) ReadAssetIssueTime(tokenID int64) int64 {
	raw, ok, err := s.systemKVGetForDecoding(kvdomains.SystemAsset, s.assetIDKey(assetIssueTimeTag, tokenID))
	if err != nil || !ok || len(raw) < 8 {
		return 0
	}
	return int64(binary.BigEndian.Uint64(raw[:8]))
}

// WriteAssetIssueTime stages the issuance block timestamp (ms) for tokenID.
func (s *StateDB) WriteAssetIssueTime(tokenID int64, issueTimeMs int64) error {
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, uint64(issueTimeMs))
	return s.SystemKVPut(kvdomains.SystemAsset, s.assetIDKey(assetIssueTimeTag, tokenID), buf)
}

// ListAssetsV2 enumerates the V2 (ID-keyed) bucket over ids
// firstTokenID..latestTokenID, skipping any id with no stored record. The
// caller supplies the bounds from the same rooted snapshot it enumerates
// against: firstTokenID is the genesis token_id_num + 1 (the first id ever
// assigned) and latestTokenID is the current token_id_num.
//
// The KV trie cannot be prefix-scanned (its keys are Keccak256(domain||key)
// hashes), so this walks the id range exactly as the flat ast- scan returned
// every record, but in id-ascending order. Because asset_issue.go writes a V2
// record for EVERY issuance — pre- and post-fork — the V2 id set is the
// authoritative superset of all assets ever created.
func (s *StateDB) ListAssetsV2(firstTokenID, latestTokenID int64) []*contractpb.AssetIssueContract {
	var out []*contractpb.AssetIssueContract
	for id := firstTokenID; id <= latestTokenID; id++ {
		if c := s.ReadAssetIssue(id); c != nil {
			out = append(out, c)
		}
	}
	return out
}

// ListAssetsLegacy enumerates the legacy (name-keyed) bucket over the same id
// range by resolving each V2 record's Name and probing the legacy leg with it.
// This works because the legacy and V2 buckets are written together while
// !AllowSameTokenName (and the legacy bucket is frozen afterward), so every
// legacy record's name is recoverable from its V2 twin's Name field.
func (s *StateDB) ListAssetsLegacy(firstTokenID, latestTokenID int64) []*contractpb.AssetIssueContract {
	var out []*contractpb.AssetIssueContract
	for id := firstTokenID; id <= latestTokenID; id++ {
		v2 := s.ReadAssetIssue(id)
		if v2 == nil {
			continue
		}
		if legacy := s.ReadAssetIssueByName(v2.Name); legacy != nil {
			out = append(out, legacy)
		}
	}
	return out
}
