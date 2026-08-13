package state

import (
	"encoding/binary"
	"errors"
	"fmt"
	"strconv"

	"github.com/tronprotocol/go-tron/core/state/kvdomains"
	contractpb "github.com/tronprotocol/go-tron/proto/core/contract"
)

// AssetBandwidthView is the fixed-size subset of AssetIssueContract consumed
// by BandwidthProcessor. It avoids materializing immutable issuance metadata
// on every TransferAsset transaction while retaining full native-row
// validation and the separately rooted mutable public-usage counters.
type AssetBandwidthView struct {
	TokenID                 int64
	Owner                   [21]byte
	FreeAssetNetLimit       int64
	PublicFreeAssetNetLimit int64
	PublicFreeAssetNetUsage int64
	PublicLatestFreeNetTime int64
	tokenIDValid            bool
}

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
// Metadata uses the versioned, hand-written hot-value codec. The two mutable
// public bandwidth counters are split into V2/legacy rows (tags 0x06/0x07), so
// bandwidth charging need not rewrite static issuance metadata. Rooted asset
// rows are native-only; protobuf wire values are rejected.
const (
	assetV2Tag              byte = 0x01
	assetLegacyTag          byte = 0x02
	assetNameIndexTag       byte = 0x03
	assetOwnerIndexTag      byte = 0x04
	assetIssueTimeTag       byte = 0x05
	assetV2BandwidthTag     byte = 0x06
	assetLegacyBandwidthTag byte = 0x07
)

// assetIDKey is the owned form used by asynchronous prefetch plans. The
// execution hot path uses StateDB.assetIDKey and its reusable scratch buffer.
func assetIDKey(tag byte, tokenID int64) []byte {
	k := make([]byte, 9)
	k[0] = tag
	binary.BigEndian.PutUint64(k[1:], uint64(tokenID))
	return k
}
func decodeAssetIssue(raw []byte) (*contractpb.AssetIssueContract, error) {
	return decodeAssetIssueNative(raw)
}

func (s *StateDB) readAssetBandwidthView(key []byte) (AssetBandwidthView, bool, error) {
	var out AssetBandwidthView
	raw, ok, err := s.systemKVGetForDecoding(kvdomains.SystemAsset, key)
	if err != nil {
		return out, false, s.recordStateError(fmt.Sprintf("read asset bandwidth view key=%x", key), err)
	}
	if !ok {
		return out, ok, err
	}
	view, err := decodeAssetBandwidthView(raw)
	if err != nil {
		return out, true, s.recordStateError(fmt.Sprintf("decode asset bandwidth view key=%x", key), err)
	}
	out.TokenID = view.tokenID
	out.tokenIDValid = view.tokenIDValid
	out.Owner = view.owner
	out.FreeAssetNetLimit = view.freeAssetNetLimit
	out.PublicFreeAssetNetLimit = view.publicFreeAssetNetLimit
	if hotKey := s.assetBandwidthKey(key); hotKey != nil {
		hotRaw, hotOK, hotErr := s.systemKVGetForDecoding(kvdomains.SystemAsset, hotKey)
		if hotErr != nil {
			return out, true, s.recordStateError(fmt.Sprintf("read asset bandwidth row key=%x", hotKey), hotErr)
		}
		if hotOK {
			out.PublicFreeAssetNetUsage, out.PublicLatestFreeNetTime, err = decodeAssetBandwidth(hotRaw)
			if err != nil {
				return out, true, s.recordStateError(fmt.Sprintf("decode asset bandwidth row key=%x", hotKey), err)
			}
		}
	}
	return out, true, nil
}

func (s *StateDB) ReadAssetBandwidthView(tokenID int64) (AssetBandwidthView, bool, error) {
	view, ok, err := s.readAssetBandwidthView(s.assetIDKey(assetV2Tag, tokenID))
	view.TokenID = tokenID
	return view, ok, err
}

func (s *StateDB) ReadAssetBandwidthViewByName(name []byte) (AssetBandwidthView, bool, error) {
	view, ok, err := s.readAssetBandwidthView(s.assetBytesKey(assetLegacyTag, name))
	if err != nil || !ok {
		return view, ok, err
	}
	if !view.tokenIDValid {
		return view, true, s.recordStateError(fmt.Sprintf("decode legacy asset issue name %q", string(name)), errors.New("invalid legacy asset ID"))
	}
	return view, true, nil
}

// decodeAssetIssueID extracts the numeric ID without materializing the message,
// but still validates the complete native row. A valid ID prefix must never
// turn truncated or otherwise non-canonical metadata into an existing asset.
func decodeAssetIssueID(raw []byte) (int64, error) {
	id, err := validateAssetIssueNativeID(raw)
	if err != nil {
		return 0, err
	}
	return strconv.ParseInt(string(id), 10, 64)
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

// assetBytesKey builds the execution-hot, transient form without allocation
// for protocol-valid asset names (at most 32 bytes) and owner addresses. The
// generic owned helper above remains for historical readers and oversized
// defensive inputs.
func (s *StateDB) assetBytesKey(tag byte, raw []byte) []byte {
	if len(raw)+1 > len(s.assetBytesKeyScratch) {
		return assetBytesKey(tag, raw)
	}
	k := s.assetBytesKeyScratch[:len(raw)+1]
	k[0] = tag
	copy(k[1:], raw)
	return k
}

// readAssetMeta resolves one AssetIssueContract leg. The public compatibility
// shape still returns nil on failure, but malformed or unreadable rooted state
// poisons the StateDB so consensus execution cannot treat corruption as absence.
func (s *StateDB) readAssetMeta(key []byte) *contractpb.AssetIssueContract {
	raw, ok, err := s.systemKVGetForDecoding(kvdomains.SystemAsset, key)
	if err != nil {
		s.recordStateError(fmt.Sprintf("read asset metadata key=%x", key), err)
		return nil
	}
	if !ok {
		return nil
	}
	if len(raw) == 0 {
		s.recordStateError(fmt.Sprintf("decode asset metadata key=%x", key), errors.New("empty value"))
		return nil
	}
	c, err := decodeAssetIssue(raw)
	if err != nil {
		s.recordStateError(fmt.Sprintf("decode asset metadata key=%x", key), err)
		return nil
	}
	if hotKey := s.assetBandwidthKey(key); hotKey != nil {
		hotRaw, hotOK, hotErr := s.systemKVGetForDecoding(kvdomains.SystemAsset, hotKey)
		if hotErr != nil {
			s.recordStateError(fmt.Sprintf("read asset bandwidth key=%x", hotKey), hotErr)
			return nil
		}
		if hotOK {
			usage, latest, decodeErr := decodeAssetBandwidth(hotRaw)
			if decodeErr != nil {
				s.recordStateError(fmt.Sprintf("decode asset bandwidth key=%x", hotKey), decodeErr)
				return nil
			}
			c.PublicFreeAssetNetUsage = usage
			c.PublicLatestFreeNetTime = latest
		}
	}
	return c
}

// writeAssetMeta stages one AssetIssueContract leg into the system-KV. The error
// is non-nil only for a codec failure or an unregistered domain (a
// programmer error), since SystemAsset is registered at init.
func (s *StateDB) writeAssetMeta(key []byte, c *contractpb.AssetIssueContract) error {
	data, err := encodeAssetIssue(c)
	if err != nil {
		return err
	}
	if err := s.SystemKVPut(kvdomains.SystemAsset, key, data); err != nil {
		return err
	}
	hotKey := s.assetBandwidthKey(key)
	if hotKey == nil {
		return nil
	}
	return s.SystemKVPut(kvdomains.SystemAsset, hotKey, encodeAssetBandwidth(c.PublicFreeAssetNetUsage, c.PublicLatestFreeNetTime))
}

func assetBandwidthKey(metadataKey []byte) []byte {
	if len(metadataKey) == 0 {
		return nil
	}
	hotTag := byte(0)
	switch metadataKey[0] {
	case assetV2Tag:
		hotTag = assetV2BandwidthTag
	case assetLegacyTag:
		hotTag = assetLegacyBandwidthTag
	default:
		return nil
	}
	key := append([]byte(nil), metadataKey...)
	key[0] = hotTag
	return key
}

// assetBandwidthKey builds the execution-hot transient form in StateDB-owned
// scratch. Metadata and bandwidth keys use separate scratch buffers, so a
// caller may derive the latter while the former is still borrowed.
func (s *StateDB) assetBandwidthKey(metadataKey []byte) []byte {
	if len(metadataKey) == 0 {
		return nil
	}
	if len(metadataKey) > len(s.assetBandwidthKeyScratch) {
		return assetBandwidthKey(metadataKey)
	}
	hotTag := byte(0)
	switch metadataKey[0] {
	case assetV2Tag:
		hotTag = assetV2BandwidthTag
	case assetLegacyTag:
		hotTag = assetLegacyBandwidthTag
	default:
		return nil
	}
	key := s.assetBandwidthKeyScratch[:len(metadataKey)]
	copy(key, metadataKey)
	key[0] = hotTag
	return key
}

func encodeAssetBandwidth(usage, latest int64) []byte {
	data := make([]byte, 17)
	data[0] = 1
	binary.BigEndian.PutUint64(data[1:9], uint64(usage))
	binary.BigEndian.PutUint64(data[9:17], uint64(latest))
	return data
}

func decodeAssetBandwidth(data []byte) (int64, int64, error) {
	if len(data) != 17 || data[0] != 1 {
		return 0, 0, fmt.Errorf("invalid asset bandwidth value length/version")
	}
	return int64(binary.BigEndian.Uint64(data[1:9])), int64(binary.BigEndian.Uint64(data[9:17])), nil
}

// WriteAssetIssueBandwidth updates only the two mutable public-bandwidth
// counters, without rewriting static issuance metadata.
func (s *StateDB) WriteAssetIssueBandwidth(tokenID, usage, latest int64) error {
	return s.SystemKVPut(kvdomains.SystemAsset, s.assetIDKey(assetV2BandwidthTag, tokenID), encodeAssetBandwidth(usage, latest))
}

// WriteAssetIssueBandwidthByName is the legacy name-keyed counterpart.
func (s *StateDB) WriteAssetIssueBandwidthByName(name []byte, usage, latest int64) error {
	return s.SystemKVPut(kvdomains.SystemAsset, s.assetBytesKey(assetLegacyBandwidthTag, name), encodeAssetBandwidth(usage, latest))
}

// ReadAssetIssue returns the rooted V2 (ID-keyed) AssetIssueContract for
// tokenID, or nil if absent. Mirrors java-tron AssetIssueV2Store.
func (s *StateDB) ReadAssetIssue(tokenID int64) *contractpb.AssetIssueContract {
	return s.readAssetMeta(s.assetIDKey(assetV2Tag, tokenID))
}

// HasAssetIssue reports whether the V2 metadata row exists and has a valid
// native layout without materializing it. java-tron's TransferAssetActuator
// validates this leg with AssetIssueV2Store.has() and consumes no metadata
// fields; the structural scan keeps that hot-path shape while failing closed on
// go-tron's internal rooted-state encoding.
func (s *StateDB) HasAssetIssue(tokenID int64) bool {
	raw, ok, err := s.systemKVGetForDecoding(kvdomains.SystemAsset, s.assetIDKey(assetV2Tag, tokenID))
	if err != nil {
		s.recordStateError(fmt.Sprintf("read asset issue id %d", tokenID), err)
		return false
	}
	if !ok {
		return false
	}
	if err := validateAssetIssueNative(raw); err != nil {
		s.recordStateError(fmt.Sprintf("decode asset issue id %d", tokenID), err)
		return false
	}
	return true
}

// WriteAssetIssue stages the V2 (ID-keyed) AssetIssueContract for tokenID.
func (s *StateDB) WriteAssetIssue(tokenID int64, c *contractpb.AssetIssueContract) error {
	return s.writeAssetMeta(s.assetIDKey(assetV2Tag, tokenID), c)
}

// ReadAssetIssueByName returns the rooted legacy (name-keyed) AssetIssueContract,
// or nil if absent. Mirrors java-tron's pre-AllowSameTokenName AssetIssueStore.
func (s *StateDB) ReadAssetIssueByName(name []byte) *contractpb.AssetIssueContract {
	return s.readAssetMeta(s.assetBytesKey(assetLegacyTag, name))
}

// HasAssetIssueByName probes the legacy metadata leg without decoding it.
// It is used only by the pre-AllowSameTokenName bandwidth mirror path, where
// a defensive name-index fallback must not create an orphan legacy hot row.
func (s *StateDB) HasAssetIssueByName(name []byte) bool {
	raw, ok, err := s.systemKVGetForDecoding(kvdomains.SystemAsset, s.assetBytesKey(assetLegacyTag, name))
	if err != nil {
		s.recordStateError(fmt.Sprintf("read legacy asset issue name %q", string(name)), err)
		return false
	}
	if !ok {
		return false
	}
	if err := validateAssetIssueNative(raw); err != nil {
		s.recordStateError(fmt.Sprintf("decode legacy asset issue name %q", string(name)), err)
		return false
	}
	return true
}

// ReadAssetIssueIDByName reads the numeric id from the legacy name-keyed row
// without materializing the full AssetIssueContract. It is the narrow read
// needed by pre-AllowSameTokenName balance routing.
func (s *StateDB) ReadAssetIssueIDByName(name []byte) (int64, bool, error) {
	raw, ok, err := s.systemKVGetForDecoding(kvdomains.SystemAsset, s.assetBytesKey(assetLegacyTag, name))
	if err != nil {
		return 0, false, s.recordStateError(fmt.Sprintf("read legacy asset issue name %q", string(name)), err)
	}
	if !ok {
		return 0, false, nil
	}
	if len(raw) == 0 {
		return 0, true, s.recordStateError(fmt.Sprintf("decode legacy asset issue name %q", string(name)), errors.New("empty value"))
	}
	id, decodeErr := decodeAssetIssueID(raw)
	if decodeErr != nil {
		return 0, true, s.recordStateError(fmt.Sprintf("decode legacy asset issue name %q", string(name)), decodeErr)
	}
	return id, true, nil
}

// WriteAssetIssueByName stages the legacy (name-keyed) AssetIssueContract.
func (s *StateDB) WriteAssetIssueByName(name []byte, c *contractpb.AssetIssueContract) error {
	return s.writeAssetMeta(s.assetBytesKey(assetLegacyTag, name), c)
}

// ReadAssetNameIndex returns the token id registered for name, and whether it
// exists. A KV error or value that is not exactly one int64 reads as not-found;
// accepting a valid prefix plus trailing bytes would create a second durable
// representation for the same rooted scalar.
func (s *StateDB) ReadAssetNameIndex(name []byte) (int64, bool) {
	id, ok, err := s.ReadAssetNameIndexStrict(name)
	if err != nil {
		s.recordStateError(fmt.Sprintf("read asset name index %q", string(name)), err)
		return 0, false
	}
	if !ok {
		return 0, false
	}
	return id, true
}

// WriteAssetNameIndex stages a name -> token id mapping.
func (s *StateDB) WriteAssetNameIndex(name []byte, tokenID int64) error {
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, uint64(tokenID))
	return s.SystemKVPut(kvdomains.SystemAsset, s.assetBytesKey(assetNameIndexTag, name), buf)
}

// ReadAssetOwnerIndex returns the token id issued by ownerAddr (21-byte TRON
// address), and whether it exists.
func (s *StateDB) ReadAssetOwnerIndex(ownerAddr []byte) (int64, bool) {
	id, ok, err := s.ReadAssetOwnerIndexStrict(ownerAddr)
	if err != nil {
		s.recordStateError(fmt.Sprintf("read asset owner index %x", ownerAddr), err)
		return 0, false
	}
	if !ok {
		return 0, false
	}
	return id, true
}

// WriteAssetOwnerIndex stages an ownerAddr -> token id mapping, enforcing
// java-tron's one-asset-per-account rule at the storage layer.
func (s *StateDB) WriteAssetOwnerIndex(ownerAddr []byte, tokenID int64) error {
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, uint64(tokenID))
	return s.SystemKVPut(kvdomains.SystemAsset, s.assetBytesKey(assetOwnerIndexTag, ownerAddr), buf)
}

// ReadAssetIssueTime returns the issuance block timestamp (ms) for tokenID, or 0
// if absent.
func (s *StateDB) ReadAssetIssueTime(tokenID int64) int64 {
	issueTime, ok, err := s.ReadAssetIssueTimeStrict(tokenID)
	if err != nil {
		s.recordStateError(fmt.Sprintf("read asset issue time %d", tokenID), err)
		return 0
	}
	if !ok {
		return 0
	}
	return issueTime
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
