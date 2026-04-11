package state

import (
	"encoding/binary"

	tcommon "github.com/tronprotocol/go-tron/common"
)

// trc10BalanceSlot returns the storage slot key for an account's TRC10 token balance.
// Key = keccak256("trc10_balance" || big_endian_uint64(tokenID))
func trc10BalanceSlot(tokenID int64) tcommon.Hash {
	buf := make([]byte, len("trc10_balance")+8)
	copy(buf, "trc10_balance")
	binary.BigEndian.PutUint64(buf[len("trc10_balance"):], uint64(tokenID))
	return tcommon.Keccak256(buf)
}

// trc10FrozenClaimedSlot returns the storage slot key indicating whether frozen_supply[index]
// has been claimed by the asset issuer.
// Key = keccak256("trc10_frozen_claimed" || big_endian_uint64(tokenID) || big_endian_uint32(index))
func trc10FrozenClaimedSlot(tokenID int64, index uint32) tcommon.Hash {
	buf := make([]byte, len("trc10_frozen_claimed")+8+4)
	copy(buf, "trc10_frozen_claimed")
	binary.BigEndian.PutUint64(buf[len("trc10_frozen_claimed"):], uint64(tokenID))
	binary.BigEndian.PutUint32(buf[len("trc10_frozen_claimed")+8:], index)
	return tcommon.Keccak256(buf)
}

// int64ToSlot encodes v as a 32-byte hash: value in the last 8 bytes (big-endian).
func int64ToSlot(v int64) tcommon.Hash {
	var h tcommon.Hash
	binary.BigEndian.PutUint64(h[24:], uint64(v))
	return h
}

// slotToInt64 decodes an int64 from a 32-byte hash (value in the last 8 bytes, big-endian).
func slotToInt64(h tcommon.Hash) int64 {
	return int64(binary.BigEndian.Uint64(h[24:]))
}
