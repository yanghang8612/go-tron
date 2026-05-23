package domains

import (
	"encoding/binary"

	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/state/kvdomains"
)

const (
	// LatestKeyHeaderLen is owner20 || generation64 || domain16.
	LatestKeyHeaderLen = common.AccountIDLength + 8 + 2
)

// EncodeLatestKey builds the Phase-2 physical latest-state key shape. Phase 1
// exposes the encoding without making it authoritative yet.
func EncodeLatestKey(owner common.Address, generation uint64, domain kvdomains.KVDomain, logicalKey []byte) []byte {
	out := make([]byte, LatestKeyHeaderLen+len(logicalKey))
	accountID := owner.AccountID()
	copy(out[:common.AccountIDLength], accountID[:])
	binary.BigEndian.PutUint64(out[common.AccountIDLength:common.AccountIDLength+8], generation)
	binary.BigEndian.PutUint16(out[common.AccountIDLength+8:LatestKeyHeaderLen], uint16(domain))
	copy(out[LatestKeyHeaderLen:], logicalKey)
	return out
}

// DecodeLatestKey parses a key produced by EncodeLatestKey.
func DecodeLatestKey(key []byte) (common.AccountID, uint64, kvdomains.KVDomain, []byte, bool) {
	var owner common.AccountID
	if len(key) < LatestKeyHeaderLen {
		return owner, 0, 0, nil, false
	}
	copy(owner[:], key[:common.AccountIDLength])
	generation := binary.BigEndian.Uint64(key[common.AccountIDLength : common.AccountIDLength+8])
	domain := kvdomains.KVDomain(binary.BigEndian.Uint16(key[common.AccountIDLength+8 : LatestKeyHeaderLen]))
	logicalKey := append([]byte(nil), key[LatestKeyHeaderLen:]...)
	return owner, generation, domain, logicalKey, true
}

func logicalKey(owner common.Address, domain kvdomains.KVDomain, key []byte) string {
	return string(EncodeLatestKey(owner, 0, domain, key))
}
