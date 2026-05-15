package types

import (
	"github.com/tronprotocol/go-tron/common"
	corepb "github.com/tronprotocol/go-tron/proto/core"
)

// PermissionByID resolves a Transaction_Contract.Permission_id to the
// matching Permission record on `account`. id=0 → Owner, id=1 → Witness,
// id>=2 → ActivePermission entry whose Permission.Id field equals id (slice
// order is *not* significant — java-tron AccountCapsule.getPermissionById
// scans by id). Returns nil when no matching permission is set.
//
// Defaults are NOT materialized here: callers that need the implicit single-
// key Owner permission for an unmaterialized account should build it via
// MakeDefaultOwnerPermission. Keeping this function pure makes it safe to
// distinguish "permission exists but no key matched" from "no permission at
// all", which java-tron differentiates as PermissionNotFound vs
// SignatureFormatException.
func PermissionByID(account *Account, id int32) *corepb.Permission {
	if account == nil {
		return nil
	}
	switch id {
	case 0:
		return account.OwnerPermission()
	case 1:
		return account.WitnessPermission()
	}
	for _, p := range account.ActivePermission() {
		if p.Id == id {
			return p
		}
	}
	return nil
}

// KeyWeight returns the configured weight of `addr` within perm.Keys, or 0
// when the address isn't part of the permission. Java-tron forbids duplicate
// addresses inside a single permission's key list (AccountPermissionUpdate
// enforces this on write), so the first match is authoritative.
func KeyWeight(perm *corepb.Permission, addr common.Address) int64 {
	if perm == nil {
		return 0
	}
	for _, k := range perm.Keys {
		if len(k.Address) == len(addr) {
			if common.BytesToAddress(k.Address) == addr {
				return k.Weight
			}
		}
	}
	return 0
}

// OperationAllowed reports whether `perm` authorizes contractType under the
// 256-bit operations bitmask. Java-tron parity:
//   - Owner permission has no bitmask filter — any contract is allowed.
//   - Witness permission is only used to sign blocks, never tx envelopes,
//     so any contractType returns false (callers should not even reach
//     this with a Witness permission; the early Permission_id == 1 path
//     rejects upstream).
//   - Active permission consults the bitmask: bit (contractType) must be set.
//
// The bitmask is little-endian within each byte: bit i of byte b corresponds
// to contract type (8*b + i), matching java-tron's
// Commons.getPermission/setPermission convention.
func OperationAllowed(perm *corepb.Permission, contractType corepb.Transaction_Contract_ContractType) bool {
	if perm == nil {
		return false
	}
	switch perm.Type {
	case corepb.Permission_Owner:
		return true
	case corepb.Permission_Witness:
		return false
	}
	ct := int(contractType)
	if ct < 0 || ct >= 8*len(perm.Operations) {
		return false
	}
	return perm.Operations[ct/8]&(1<<uint(ct%8)) != 0
}
