package state

import (
	"encoding/binary"

	"github.com/tronprotocol/go-tron/core/state/kvdomains"
	contractpb "github.com/tronprotocol/go-tron/proto/core/contract"
	"google.golang.org/protobuf/proto"
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

// assetIDKey builds a tag||u64-BE(id) logical key (V2 metadata, issue time).
func assetIDKey(tag byte, tokenID int64) []byte {
	k := make([]byte, 1+8)
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
	c := &contractpb.AssetIssueContract{}
	if err := proto.Unmarshal(raw, c); err != nil {
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
	return s.readAssetMeta(assetIDKey(assetV2Tag, tokenID))
}

// WriteAssetIssue stages the V2 (ID-keyed) AssetIssueContract for tokenID.
func (s *StateDB) WriteAssetIssue(tokenID int64, c *contractpb.AssetIssueContract) error {
	return s.writeAssetMeta(assetIDKey(assetV2Tag, tokenID), c)
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
	raw, ok, err := s.systemKVGetForDecoding(kvdomains.SystemAsset, assetIDKey(assetIssueTimeTag, tokenID))
	if err != nil || !ok || len(raw) < 8 {
		return 0
	}
	return int64(binary.BigEndian.Uint64(raw[:8]))
}

// WriteAssetIssueTime stages the issuance block timestamp (ms) for tokenID.
func (s *StateDB) WriteAssetIssueTime(tokenID int64, issueTimeMs int64) error {
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, uint64(issueTimeMs))
	return s.SystemKVPut(kvdomains.SystemAsset, assetIDKey(assetIssueTimeTag, tokenID), buf)
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
