package state

import (
	"encoding/binary"
	"fmt"

	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/state/kvdomains"
	contractpb "github.com/tronprotocol/go-tron/proto/core/contract"
	"google.golang.org/protobuf/proto"
)

// Strict and historical asset accessors are kept separate from asset_store.go
// so the sync hot path can retain its specialized low-allocation decoder while
// API/archive paths surface malformed rows instead of folding them into misses.
func (s *StateDB) readAssetMetaStrict(key []byte, context string) (*contractpb.AssetIssueContract, bool, error) {
	raw, ok, err := s.SystemKVGet(kvdomains.SystemAsset, key)
	if err != nil || !ok {
		return nil, ok, err
	}
	c := new(contractpb.AssetIssueContract)
	if err := proto.Unmarshal(raw, c); err != nil {
		return nil, true, fmt.Errorf("decode %s: %w", context, err)
	}
	return c, true, nil
}

func (s *StateDB) ReadAssetIssueStrict(tokenID int64) (*contractpb.AssetIssueContract, bool, error) {
	return s.readAssetMetaStrict(assetIDKey(assetV2Tag, tokenID), fmt.Sprintf("asset issue id %d", tokenID))
}

func (s *StateDB) ReadAssetIssueByNameStrict(name []byte) (*contractpb.AssetIssueContract, bool, error) {
	return s.readAssetMetaStrict(assetBytesKey(assetLegacyTag, name), fmt.Sprintf("legacy asset issue name %q", string(name)))
}

func (s *StateDB) readAssetScalarStrict(key []byte, context string) (int64, bool, error) {
	raw, ok, err := s.SystemKVGet(kvdomains.SystemAsset, key)
	if err != nil || !ok {
		return 0, ok, err
	}
	if len(raw) < 8 {
		return 0, true, fmt.Errorf("decode %s: value length %d, want at least 8", context, len(raw))
	}
	return int64(binary.BigEndian.Uint64(raw[:8])), true, nil
}

func (s *StateDB) ReadAssetNameIndexStrict(name []byte) (int64, bool, error) {
	return s.readAssetScalarStrict(assetBytesKey(assetNameIndexTag, name), fmt.Sprintf("asset name index %q", string(name)))
}

func (s *StateDB) ReadAssetOwnerIndexStrict(ownerAddr []byte) (int64, bool, error) {
	return s.readAssetScalarStrict(assetBytesKey(assetOwnerIndexTag, ownerAddr), fmt.Sprintf("asset owner index %x", ownerAddr))
}

func (s *StateDB) ReadAssetIssueTimeStrict(tokenID int64) (int64, bool, error) {
	return s.readAssetScalarStrict(assetIDKey(assetIssueTimeTag, tokenID), fmt.Sprintf("asset issue time %d", tokenID))
}

func (s *StateDB) ListAssetsV2Strict(firstTokenID, latestTokenID int64) ([]*contractpb.AssetIssueContract, error) {
	var out []*contractpb.AssetIssueContract
	for id := firstTokenID; id <= latestTokenID; id++ {
		asset, ok, err := s.ReadAssetIssueStrict(id)
		if err != nil {
			return nil, err
		}
		if ok {
			out = append(out, asset)
		}
	}
	return out, nil
}

func (s *StateDB) ListAssetsLegacyStrict(firstTokenID, latestTokenID int64) ([]*contractpb.AssetIssueContract, error) {
	var out []*contractpb.AssetIssueContract
	for id := firstTokenID; id <= latestTokenID; id++ {
		v2, ok, err := s.ReadAssetIssueStrict(id)
		if err != nil {
			return nil, err
		}
		if !ok {
			continue
		}
		legacy, ok, err := s.ReadAssetIssueByNameStrict(v2.Name)
		if err != nil {
			return nil, err
		}
		if ok {
			out = append(out, legacy)
		}
	}
	return out, nil
}

func (r *PersistentHistoryReader) readAssetMetaAt(key []byte, blockNum uint64) (*contractpb.AssetIssueContract, error) {
	raw, ok, err := r.AccountKVAt(tcommon.SystemAccountAddress, kvdomains.SystemAsset, key, blockNum)
	if err != nil || !ok || len(raw) == 0 {
		return nil, err
	}
	c := new(contractpb.AssetIssueContract)
	if err := proto.Unmarshal(raw, c); err != nil {
		return nil, fmt.Errorf("decode asset metadata at block %d: %w", blockNum, err)
	}
	return c, nil
}

func (r *PersistentHistoryReader) AssetIssueAt(tokenID int64, blockNum uint64) (*contractpb.AssetIssueContract, error) {
	return r.readAssetMetaAt(assetIDKey(assetV2Tag, tokenID), blockNum)
}

func (r *PersistentHistoryReader) AssetIssueByNameAt(name []byte, blockNum uint64) (*contractpb.AssetIssueContract, error) {
	return r.readAssetMetaAt(assetBytesKey(assetLegacyTag, name), blockNum)
}

func (r *PersistentHistoryReader) AssetOwnerIndexAt(ownerAddr []byte, blockNum uint64) (int64, bool, error) {
	raw, ok, err := r.AccountKVAt(tcommon.SystemAccountAddress, kvdomains.SystemAsset, assetBytesKey(assetOwnerIndexTag, ownerAddr), blockNum)
	if err != nil || !ok || len(raw) == 0 {
		return 0, false, err
	}
	if len(raw) < 8 {
		return 0, false, fmt.Errorf("decode asset owner index at block %d: value length %d, want at least 8", blockNum, len(raw))
	}
	return int64(binary.BigEndian.Uint64(raw[:8])), true, nil
}

func (r *PersistentHistoryReader) ListAssetsV2At(firstTokenID, latestTokenID int64, blockNum uint64) ([]*contractpb.AssetIssueContract, error) {
	var out []*contractpb.AssetIssueContract
	for id := firstTokenID; id <= latestTokenID; id++ {
		asset, err := r.AssetIssueAt(id, blockNum)
		if err != nil {
			return nil, err
		}
		if asset != nil {
			out = append(out, asset)
		}
	}
	return out, nil
}

func (r *PersistentHistoryReader) ListAssetsLegacyAt(firstTokenID, latestTokenID int64, blockNum uint64) ([]*contractpb.AssetIssueContract, error) {
	var out []*contractpb.AssetIssueContract
	for id := firstTokenID; id <= latestTokenID; id++ {
		v2, err := r.AssetIssueAt(id, blockNum)
		if err != nil {
			return nil, err
		}
		if v2 == nil {
			continue
		}
		legacy, err := r.AssetIssueByNameAt(v2.Name, blockNum)
		if err != nil {
			return nil, err
		}
		if legacy != nil {
			out = append(out, legacy)
		}
	}
	return out, nil
}
