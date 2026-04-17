package rawdb

import "encoding/binary"

var (
	headBlockKey      = []byte("LastBlock")
	headSolidBlockKey = []byte("LastSolidBlock")

	blockPrefix     = []byte("b-")
	blockHashPrefix = []byte("bh-")
	txPrefix        = []byte("tx-")
	txInfoPrefix    = []byte("ti-")
	txInfoBlockPrefix = []byte("tib-")
	accountPrefix   = []byte("a-")
	witnessPrefix   = []byte("w-")
	votesPrefix     = []byte("v-")
	proposalPrefix  = []byte("p-")
	codePrefix      = []byte("c-")
	contractPrefix  = []byte("ct-")
	storagePrefix   = []byte("s-")
	dynPropPrefix   = []byte("dp-")

	witnessScheduleKey = []byte("ws")

	activeWitnessesKey = []byte("ActiveWitnesses")
	witnessIndexKey    = []byte("WitnessIndex")

	proposalIndexKey = []byte("propi")

	delegationPrefix      = []byte("dr-")
	delegationIndexPrefix = []byte("dri-")
	brokeragePrefix       = []byte("wb-")

	assetPrefix          = []byte("ast-")   // tokenID big-endian 8B → AssetIssueContract proto bytes
	assetNamePrefix      = []byte("astn-")  // token name bytes → tokenID big-endian 8B
	assetOwnerPrefix     = []byte("asto-")  // owner address 21B → tokenID big-endian 8B
	assetIssueTimePrefix = []byte("asti-")  // tokenID big-endian 8B → issue timestamp ms big-endian 8B

	marketOrderPrefix        = []byte("mo-")
	marketAccountOrderPrefix = []byte("mao-")
	marketOrderBookPrefix    = []byte("mop-")
	marketPriceListPrefix    = []byte("mpl-")

	exchangePrefix = []byte("ex-")

	nullifierPrefix        = []byte("nf-")
	noteCommitmentPrefix   = []byte("nc-")
	noteCommitmentCountKey = []byte("nccount")

	// forkStatsPrefix stores per-block-version SR vote bitmaps.
	// Key:   forkStatsPrefix || big-endian int32 version
	// Value: byte slice of length == active witness count; index = slot,
	//        value == 0x01 (VERSION_UPGRADE) or 0x00 (VERSION_DOWNGRADE).
	// Mirrors java-tron DynamicPropertiesStore's FORK_VERSION_<v> keys.
	forkStatsPrefix = []byte("fv-")

	// delegRewardPrefix (dl-) maps to java-tron's DelegationStore (db name
	// "delegation"). Distinct from delegationPrefix ("dr-") which stores
	// Freeze-V2 resource delegation records. Holds per-cycle voter reward
	// pools, vote snapshots, brokerage rates, accumulated witness VI, and
	// voter beginCycle/endCycle cursors used by the reward v2 algorithm.
	delegRewardPrefix = []byte("dl-")

	// contractStatePrefix (cs-) maps to java-tron's ContractStateStore
	// (db name "contract-state"). Per-contract dynamic-energy state:
	// rolling energy usage, energy factor, and last-update cycle.
	contractStatePrefix = []byte("cs-")

	// accountAssetPrefix (aa-) maps to java-tron's AccountAssetStore
	// (db name "account-asset"). Per-(owner,tokenID) asset balance,
	// populated when allow_account_asset_optimization is active so the
	// main Account proto doesn't carry the whole asset map. Key:
	//   aa- || owner (21B) || tokenID (varint-style uint64 big-endian 8B)
	// Value: big-endian int64 balance (matches Java's Longs.toByteArray).
	accountAssetPrefix = []byte("aa-")
)

func blockKey(number uint64) []byte {
	k := make([]byte, len(blockPrefix)+8)
	copy(k, blockPrefix)
	binary.BigEndian.PutUint64(k[len(blockPrefix):], number)
	return k
}

func blockHashKey(hash []byte) []byte {
	return append(append([]byte{}, blockHashPrefix...), hash...)
}

func txKey(hash []byte) []byte {
	return append(append([]byte{}, txPrefix...), hash...)
}

func txInfoKey(hash []byte) []byte {
	return append(append([]byte{}, txInfoPrefix...), hash...)
}

func txInfoBlockKey(number uint64) []byte {
	k := make([]byte, len(txInfoBlockPrefix)+8)
	copy(k, txInfoBlockPrefix)
	binary.BigEndian.PutUint64(k[len(txInfoBlockPrefix):], number)
	return k
}

func accountKey(addr []byte) []byte {
	return append(append([]byte{}, accountPrefix...), addr...)
}

func witnessKey(addr []byte) []byte {
	return append(append([]byte{}, witnessPrefix...), addr...)
}

func dynPropKey(name string) []byte {
	return append(append([]byte{}, dynPropPrefix...), []byte(name)...)
}

func forkStatsKey(version int32) []byte {
	k := make([]byte, len(forkStatsPrefix)+4)
	copy(k, forkStatsPrefix)
	binary.BigEndian.PutUint32(k[len(forkStatsPrefix):], uint32(version))
	return k
}

func proposalKey(id int64) []byte {
	k := make([]byte, len(proposalPrefix)+8)
	copy(k, proposalPrefix)
	binary.BigEndian.PutUint64(k[len(proposalPrefix):], uint64(id))
	return k
}

func delegationKey(from, to []byte) []byte {
	k := make([]byte, len(delegationPrefix)+len(from)+len(to))
	copy(k, delegationPrefix)
	copy(k[len(delegationPrefix):], from)
	copy(k[len(delegationPrefix)+len(from):], to)
	return k
}

func delegationIndexKey(from []byte) []byte {
	return append(append([]byte{}, delegationIndexPrefix...), from...)
}

func brokerageKey(addr []byte) []byte {
	return append(append([]byte{}, brokeragePrefix...), addr...)
}

// delegRewardKey builds a per-cycle-per-witness key with the given suffix.
// Layout: dl- || big-endian int64 cycle || 0x2d '-' || addr || 0x2d '-' || suffix
// Matches java-tron DelegationStore's "{cycle}-{hex(address)}-{suffix}"
// string keys semantically (we use raw bytes rather than hex, but the
// cycle/address/suffix separation is preserved).
func delegRewardKey(cycle int64, addr []byte, suffix string) []byte {
	k := make([]byte, 0, len(delegRewardPrefix)+8+1+len(addr)+1+len(suffix))
	k = append(k, delegRewardPrefix...)
	var cb [8]byte
	binary.BigEndian.PutUint64(cb[:], uint64(cycle))
	k = append(k, cb[:]...)
	k = append(k, '-')
	k = append(k, addr...)
	k = append(k, '-')
	k = append(k, []byte(suffix)...)
	return k
}

// delegBeginCycleKey stores a voter's beginCycle cursor: dl- || addr.
// Mirrors java-tron DelegationStore.setBeginCycle(address, number).
func delegBeginCycleKey(addr []byte) []byte {
	k := make([]byte, 0, len(delegRewardPrefix)+len(addr))
	k = append(k, delegRewardPrefix...)
	return append(k, addr...)
}

// delegEndCycleKey stores a voter's endCycle cursor: dl- || "end-" || addr.
func delegEndCycleKey(addr []byte) []byte {
	k := make([]byte, 0, len(delegRewardPrefix)+4+len(addr))
	k = append(k, delegRewardPrefix...)
	k = append(k, []byte("end-")...)
	return append(k, addr...)
}

// contractStateKey builds the per-contract dynamic-energy state key.
func contractStateKey(addr []byte) []byte {
	return append(append([]byte{}, contractStatePrefix...), addr...)
}

// accountAssetKey builds an account-asset lookup key. Mirrors java-tron's
// AccountAssetStore key layout: owner address (21B) concatenated with the
// asset's tokenID, big-endian 8-byte. Value is the int64 balance.
func accountAssetKey(owner []byte, tokenID int64) []byte {
	k := make([]byte, 0, len(accountAssetPrefix)+len(owner)+8)
	k = append(k, accountAssetPrefix...)
	k = append(k, owner...)
	var tb [8]byte
	binary.BigEndian.PutUint64(tb[:], uint64(tokenID))
	return append(k, tb[:]...)
}

func assetKey(tokenID int64) []byte {
	k := make([]byte, len(assetPrefix)+8)
	copy(k, assetPrefix)
	binary.BigEndian.PutUint64(k[len(assetPrefix):], uint64(tokenID))
	return k
}

func assetNameKey(name []byte) []byte {
	return append(append([]byte{}, assetNamePrefix...), name...)
}

func assetOwnerKey(ownerAddr []byte) []byte {
	return append(append([]byte{}, assetOwnerPrefix...), ownerAddr...)
}

func assetIssueTimeKey(tokenID int64) []byte {
	k := make([]byte, len(assetIssueTimePrefix)+8)
	copy(k, assetIssueTimePrefix)
	binary.BigEndian.PutUint64(k[len(assetIssueTimePrefix):], uint64(tokenID))
	return k
}

func gcdInt64(a, b int64) int64 {
	for b != 0 {
		a, b = b, a%b
	}
	return a
}

// PriceKey normalizes a {sellQty, buyQty} pair by GCD and encodes as 16 bytes.
func PriceKey(sellQty, buyQty int64) [16]byte {
	g := gcdInt64(sellQty, buyQty)
	var k [16]byte
	binary.BigEndian.PutUint64(k[:8], uint64(sellQty/g))
	binary.BigEndian.PutUint64(k[8:], uint64(buyQty/g))
	return k
}

func marketOrderKey(orderID []byte) []byte {
	return append(append([]byte{}, marketOrderPrefix...), orderID...)
}

func marketAccountOrderKey(ownerAddr []byte) []byte {
	return append(append([]byte{}, marketAccountOrderPrefix...), ownerAddr...)
}

func marketOrderBookKey(sellTokenID, buyTokenID []byte, pk [16]byte) []byte {
	k := append(append([]byte{}, marketOrderBookPrefix...), sellTokenID...)
	k = append(k, '|')
	k = append(k, buyTokenID...)
	k = append(k, '|')
	return append(k, pk[:]...)
}

func marketPriceListKey(sellTokenID, buyTokenID []byte) []byte {
	k := append(append([]byte{}, marketPriceListPrefix...), sellTokenID...)
	k = append(k, '|')
	return append(k, buyTokenID...)
}

func exchangeKey(id int64) []byte {
	k := make([]byte, len(exchangePrefix)+8)
	copy(k, exchangePrefix)
	binary.BigEndian.PutUint64(k[len(exchangePrefix):], uint64(id))
	return k
}

func nullifierKey(nullifier []byte) []byte {
	return append(append([]byte{}, nullifierPrefix...), nullifier...)
}

func noteCommitmentKey(index int64) []byte {
	k := make([]byte, len(noteCommitmentPrefix)+8)
	copy(k, noteCommitmentPrefix)
	binary.BigEndian.PutUint64(k[len(noteCommitmentPrefix):], uint64(index))
	return k
}
