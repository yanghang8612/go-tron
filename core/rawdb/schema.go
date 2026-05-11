package rawdb

import (
	"encoding/binary"
	"strconv"
)

var (
	headBlockKey            = []byte("LastBlock")
	headSolidBlockKey       = []byte("LastSolidBlock")
	totalTransactionCountKey = []byte("total-tx-count")

	// genesisStateRootKey holds the post-genesis state root. java-tron does
	// not put account_state_root on the genesis block header (we mirror that
	// for cross-impl genesis-hash parity), so we persist it here for
	// applyBlock to bootstrap block #1's StateDB. See core.SetupGenesisBlock.
	genesisStateRootKey = []byte("genesis-state-root")

	// blockStateRootPrefix maps block hash → post-apply state root. Stored
	// alongside the block (out-of-band) so we never have to mutate the
	// block proto's `account_state_root` field — which would break wire-
	// format parity with java-tron blocks that arrived with that field
	// empty.
	blockStateRootPrefix = []byte("bsr-")

	blockPrefix     = []byte("b-")
	blockHashPrefix = []byte("bh-")
	txPrefix        = []byte("tx-")
	txInfoPrefix    = []byte("ti-")
	txInfoBlockPrefix = []byte("tib-")
	accountPrefix   = []byte("a-")
	witnessPrefix            = []byte("w-")
	witnessLatestBlockPrefix = []byte("wlb-") // per-witness latest produced block number
	votesPrefix              = []byte("v-")
	proposalPrefix  = []byte("p-")
	codePrefix      = []byte("c-")
	contractPrefix  = []byte("ct-")
	storagePrefix   = []byte("s-")
	dynPropPrefix   = []byte("dp-")

	// witnessScheduleKey is the head sentinel for witness-schedule state.
	// Kept for backwards compatibility with pre-M2 data; not written today.
	witnessScheduleKey = []byte("ws")

	// shuffledWitnessesKey maps to java-tron WitnessScheduleStore's
	// `current_shuffled_witnesses` key — the per-cycle shuffled block-
	// production order. go-tron doesn't wire this into block scheduling
	// yet (M3/M6 work) but the store is here for parity so capture and
	// future behaviour ports don't have to coin a prefix.
	// Value: 21*N bytes (concatenated addresses in shuffled order).
	shuffledWitnessesKey = []byte("ws-shuffled")

	// previousShuffledWitnessesKey stores the previous maintenance cycle's
	// shuffled witness list. Written at each maintenance boundary (before
	// overwriting shuffledWitnessesKey with the new list). Used by the PBFT
	// message handler to accept signatures from the prior epoch's SRs.
	// Value: 21*N bytes (same encoding as shuffledWitnessesKey).
	previousShuffledWitnessesKey = []byte("ws-prev-shuffled")

	activeWitnessesKey = []byte("ActiveWitnesses")
	witnessIndexKey    = []byte("WitnessIndex")

	// genesisWitnessesKey holds the immutable {address, initial_vote_count}
	// list from the chain's Genesis config. Required to faithfully port
	// java-tron's tryRemoveThePowerOfTheGr, which subtracts each GR's
	// *initial* vote count (not the current count after voting activity)
	// when REMOVE_THE_POWER_OF_THE_GR fires. Written once at genesis setup
	// and never mutated thereafter.
	// Value: 4-byte big-endian count || N * (21B address || 8B BE vote count)
	genesisWitnessesKey = []byte("GenesisWitnesses")

	proposalIndexKey = []byte("propi")

	delegationPrefix      = []byte("dr-")
	delegationIndexPrefix = []byte("dri-")
	brokeragePrefix       = []byte("wb-")

	assetPrefix          = []byte("ast-")   // tokenID big-endian 8B → AssetIssueContract proto bytes
	assetNamePrefix      = []byte("astn-")  // token name bytes → tokenID big-endian 8B
	assetOwnerPrefix     = []byte("asto-")  // owner address 21B → tokenID big-endian 8B
	assetIssueTimePrefix = []byte("asti-")  // tokenID big-endian 8B → issue timestamp ms big-endian 8B

	// marketOrderPrefix (mo-) maps to java-tron's MarketOrderStore
	// (db name "market-order"). Stores individual MarketOrder protos keyed
	// by orderID. Present in go-tron since initial market implementation.
	marketOrderPrefix = []byte("mo-")

	// marketAccountOrderPrefix (mao-) maps to java-tron's MarketAccountStore
	// (db name "market_account"). Per-owner list of active order IDs.
	// Covers the "market-account" store in the M2 schema gap list.
	marketAccountOrderPrefix = []byte("mao-")

	// marketOrderBookPrefix (mop-) maps to java-tron's
	// MarketPairPriceToOrderStore (db name "market_pair_price_to_order").
	// Stores MarketOrderIdList for each (sellToken, buyToken, price) triple.
	// Covers the "market-pair-price-to-order" store in the M2 schema gap list.
	marketOrderBookPrefix = []byte("mop-")

	// marketPriceListPrefix (mpl-) is a go-tron-specific cache that stores
	// the full computed MarketPriceList proto for a token pair. Java-tron
	// computes this dynamically from MarketPairToPriceStore + MarketPairPriceToOrderStore;
	// go-tron materializes it at write time. Used by market actuators.
	marketPriceListPrefix = []byte("mpl-")

	// marketPairToPricePrefix (mptop-) maps to java-tron's
	// MarketPairToPriceStore (db name "market_pair_to_price"). Stores the
	// int64 count of distinct prices for a (sellToken, buyToken) pair.
	// Key: mptop- || sellTokenID bytes || '|' || buyTokenID bytes
	// Value: big-endian int64 count.
	// Used for existence checks and RPC pagination (java-tron: getPriceNum).
	// Covers the "market-pair-to-price" store in the M2 schema gap list.
	marketPairToPricePrefix = []byte("mptop-")

	exchangePrefix = []byte("ex-")

	nullifierPrefix        = []byte("nf-")
	noteCommitmentPrefix   = []byte("nc-")
	noteCommitmentCountKey = []byte("nccount")

	// zkProofPrefix (zkp-) maps to java-tron's ZKProofStore (db "zkProof").
	// Stores a seen-flag for each ZK proof to prevent replay attacks on
	// shielded transactions (distinct from the nullifier used for double-spend).
	// Key:   zkp- || proof bytes (opaque, 192 bytes for Groth16)
	// Value: 0x01 (proof has been accepted once)
	// Gated on allow_shielded_transaction.
	zkProofPrefix = []byte("zkp-")

	// incrMerkleTreePrefix (imt-) maps to java-tron's
	// IncrementalMerkleTreeStore (db "IncrementalMerkleTree"). Stores the
	// full IncrementalMerkleTree proto for a given commitment-tree root
	// (anchor). The key is the 32-byte root hash; the value is the serialised
	// IncrementalMerkleTree proto. Used by shielded-transaction verification
	// to look up the tree state at any past anchor.
	// Key:   imt- || 32-byte commitment tree root
	// Value: proto-encoded shield_contract.IncrementalMerkleTree
	incrMerkleTreePrefix = []byte("imt-")

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

	// abiPrefix (abi-) maps to java-tron's AbiStore (db "abi"). When
	// allow_account_asset_optimization is active, contract ABIs are moved
	// OUT of the inline SmartContract proto and stored here so the main
	// contract store no longer carries ABI bytes on every read.
	// Key:   abi- || contract address 21B
	// Value: proto-encoded SmartContract_ABI
	// Note: go-tron currently stores ABI inline in SmartContract (via
	// contractPrefix "ct-"). This store exists for forward-compatibility
	// when the ABI extraction feature activates.
	abiPrefix = []byte("abi-")

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

	// accountIdIndexPrefix (aid-) maps to java-tron's
	// AccountIdIndexStore (db name "accountid-index"). Reverse lookup
	// from account_id (a user-chosen UTF-8 string) to the 21-byte
	// owner address. Written by SetAccountIdContract; read by its
	// uniqueness check and by the getaccountbyid RPC.
	// Key:   aid- || account_id bytes (utf-8, lowercased before insert)
	// Value: 21-byte owner address.
	accountIdIndexPrefix = []byte("aid-")

	// accountTracePrefix (at-) maps to java-tron's AccountTraceStore
	// (db name "account-trace"). Per-account historical balance
	// records, one per block height where the account changed. Gated
	// on CommonParameter.isHistoryBalanceLookup — dormant on mainnet
	// today, but ports live for parity + future opt-in auditing.
	// Key:   at- || owner (21B) || (blockNum XOR Long.MAX_VALUE) BE 8B
	// Value: proto-encoded AccountTrace ({balance: int64}).
	// XOR trick makes newer blocks sort first under lex ordering, so
	// a prefix iterator hands the latest trace back on first hit.
	accountTracePrefix = []byte("at-")

	// sectionBloomPrefix (sb-) maps to java-tron's SectionBloomStore
	// (db name "section-bloom"). Bloom filter per (section, bitIndex)
	// for eth_getLogs / log-filter acceleration. Dormant on mainnet
	// today (writer wired but not called); store is here for parity
	// and future M5.2 filter support.
	// Key:   sb- || (section * 1_000_000 + bitIndex) decimal ASCII
	// Value: opaque bytes (java side stores zlib-compressed BitSet).
	sectionBloomPrefix = []byte("sb-")

	// treeBlockIndexPrefix (tbi-) maps to java-tron's
	// TreeBlockIndexStore (db name "tree-block-index"). blockNum →
	// merkle root bytes, used for shielded-transaction proof
	// acceleration. Dormant on mainnet (zksnark feature flag).
	// Key:   tbi- || blockNum big-endian int64
	// Value: opaque bytes (merkle root identifier).
	treeBlockIndexPrefix = []byte("tbi-")

	// pbftSignDataPrefix (psd-) maps to java-tron's PbftSignDataStore
	// (db name "pbft-sign-data"). Holds PBFT quorum signatures for
	// committed blocks and election-cycle SR lists. Live on mainnet
	// once PBFT (proposal #50 ALLOW_PBFT) is active; essential for
	// validators and for RPC proof-of-finality lookups.
	// Key variants (mirroring DataType.BLOCK / DataType.SRL):
	//   psd- || "BLOCK" || blockNum decimal ASCII  → PBFTCommitResult
	//   psd- || "SRL"   || epoch    decimal ASCII  → PBFTCommitResult
	// Value: proto-encoded corepb.PBFTCommitResult { data, signature[] }.
	pbftSignDataPrefix = []byte("psd-")

	// txHistoryPrefix (ti-) maps to java-tron's TransactionHistoryStore
	// (db name "transactionHistoryStore"). Stores TransactionInfo protos
	// keyed by tx hash; gated on the transactionHistorySwitch config option.
	// NOTE: go-tron uses "ti-" as the prefix; the java-tron DB name
	// "transactionHistoryStore" maps to this prefix for parity purposes.
	// (txInfoPrefix above — same variable, documented here for completeness.)

	// txRetPrefix (tib-) maps to java-tron's TransactionRetStore
	// (db name "transactionRetStore"). Stores TransactionRet (list of
	// TransactionInfo) keyed by block number; also gated on the history
	// switch. NOTE: go-tron uses "tib-" as the prefix; java-tron's
	// "transactionRetStore" maps to this prefix for parity purposes.
	// (txInfoBlockPrefix above — same variable, documented here.)

	// balanceTracePrefix (btrace-) maps to java-tron's BalanceTraceStore
	// (db name "balance-trace"). Stores BlockBalanceTrace protos keyed by
	// block number (big-endian int64). Gated on isHistoryBalanceLookup;
	// dormant on mainnet today but live when balance-history audit is on.
	// Key:   btrace- || blockNum big-endian int64
	// Value: proto-encoded contract.BlockBalanceTrace.
	balanceTracePrefix = []byte("btrace-")

	// checkPointV2Prefix (cpv2-) maps to java-tron's CheckPointV2Store
	// (db path "check-point-v2"). A crash-recovery WAL: before committing
	// a batch of chainbase writes, java-tron writes the same rows here
	// with sync=true so they can be replayed on restart. In go-tron,
	// Pebble's native WAL provides equivalent crash safety; this prefix
	// is dormant (never written) and exists solely for schema completeness.
	checkPointV2Prefix = []byte("cpv2-")

	// rewardViPrefix (rvi-) maps to java-tron's RewardViStore (db name
	// "reward-vi"). Caches per-(cycle,witness) cumulative VI values for
	// the new reward algorithm; computed once by RewardViCalService and
	// then used for fast voter-reward lookups without re-scanning history.
	// Key: rvi- || cycle big-endian int64 || '-' || addr 21B || '-' || "vi"
	// Sentinel: rvi- || 0x00 → 0x01 (IS_DONE flag after initial migration)
	// Value: BigInteger bytes (two's-complement big-endian, minimum bytes).
	rewardViPrefix = []byte("rvi-")

	// drAccIdxPrefix (drax-) maps to java-tron's
	// DelegatedResourceAccountIndexStore (db name
	// "DelegatedResourceAccountIndex"). Holds forward and reverse
	// bindings for both Freeze-V1 and Freeze-V2 delegation pairs, one
	// record per (direction, anchor, counterparty) tuple — lets a
	// wallet answer "who delegated TO me" or "who have I delegated to"
	// via prefix scans without walking the primary DelegatedResource
	// store. The direction byte distinguishes V1/V2 × from/to:
	//   0x01 V1 from-anchor   (anchor=from, counterparty=to)
	//   0x02 V1 to-anchor     (anchor=to,   counterparty=from)
	//   0x03 V2 from-anchor   "
	//   0x04 V2 to-anchor     "
	// Key:   drax- || direction (1B) || anchor (21B) || counterparty (21B)
	// Value: proto-encoded DelegatedResourceAccountIndex with `Account`
	//        = counterparty and `Timestamp` = delegate/undelegate time.
	drAccIdxPrefix = []byte("drax-")
)

// drAccIdxDirection enumerates the four sub-indices of
// DelegatedResourceAccountIndexStore, matching java-tron's 0x01..0x04
// prefix bytes for V1/V2 × from/to-anchored lookups.
type drAccIdxDirection byte

const (
	DrAccIdxV1From drAccIdxDirection = 0x01 // V1: from-anchored  (anchor=from, counterparty=to)
	DrAccIdxV1To   drAccIdxDirection = 0x02 // V1: to-anchored    (anchor=to,   counterparty=from)
	DrAccIdxV2From drAccIdxDirection = 0x03 // V2: from-anchored
	DrAccIdxV2To   drAccIdxDirection = 0x04 // V2: to-anchored
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

func blockStateRootKey(hash []byte) []byte {
	return append(append([]byte{}, blockStateRootPrefix...), hash...)
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

func witnessLatestBlockKey(addr []byte) []byte {
	return append(append([]byte{}, witnessLatestBlockPrefix...), addr...)
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

// abiKey builds the per-contract ABI key.
func abiKey(contractAddr []byte) []byte {
	return append(append([]byte{}, abiPrefix...), contractAddr...)
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

// accountIdIndexKey builds the accountid-index key. Mirrors java-tron's
// AccountIdIndexStore — key is the raw account ID bytes (UTF-8).
func accountIdIndexKey(accountID []byte) []byte {
	return append(append([]byte{}, accountIdIndexPrefix...), accountID...)
}

// accountTraceKey builds an account-trace key: prefix || owner (21B) ||
// (blockNum XOR Long.MAX_VALUE) big-endian 8B. The XOR inverts numeric
// ordering so a lexicographic iterator walks newest-first, matching
// java-tron's AccountTraceStore key layout for recordBalanceWithBlock.
func accountTraceKey(owner []byte, blockNum int64) []byte {
	const longMax int64 = 0x7FFFFFFFFFFFFFFF
	xored := blockNum ^ longMax
	k := make([]byte, 0, len(accountTracePrefix)+len(owner)+8)
	k = append(k, accountTracePrefix...)
	k = append(k, owner...)
	var b [8]byte
	binary.BigEndian.PutUint64(b[:], uint64(xored))
	return append(k, b[:]...)
}

// sectionBloomKey builds the section-bloom key: java-tron encodes the
// (section, bitIndex) composite as a single decimal integer
// section*1_000_000 + bitIndex, then takes its ASCII bytes.
func sectionBloomKey(section, bitIndex uint64) []byte {
	composite := section*1_000_000 + bitIndex
	return append(append([]byte{}, sectionBloomPrefix...), []byte(strconv.FormatUint(composite, 10))...)
}

// treeBlockIndexKey builds the tree-block-index key: blockNum big-endian.
func treeBlockIndexKey(blockNum int64) []byte {
	k := make([]byte, len(treeBlockIndexPrefix)+8)
	copy(k, treeBlockIndexPrefix)
	binary.BigEndian.PutUint64(k[len(treeBlockIndexPrefix):], uint64(blockNum))
	return k
}

// pbftBlockSignKey builds the per-block PBFT sign-data key, matching
// java-tron: "BLOCK" + Long.toString(blockNum).
func pbftBlockSignKey(blockNum int64) []byte {
	return append(append([]byte{}, pbftSignDataPrefix...), []byte("BLOCK"+strconv.FormatInt(blockNum, 10))...)
}

// pbftSrSignKey builds the per-epoch SR list PBFT sign-data key, matching
// java-tron: "SRL" + Long.toString(epoch).
func pbftSrSignKey(epoch int64) []byte {
	return append(append([]byte{}, pbftSignDataPrefix...), []byte("SRL"+strconv.FormatInt(epoch, 10))...)
}

// balanceTraceKey builds the per-block balance-trace key: blockNum big-endian.
// Mirrors java-tron BalanceTraceStore.putBlockBalanceTrace (ByteArray.fromLong).
func balanceTraceKey(blockNum int64) []byte {
	k := make([]byte, len(balanceTracePrefix)+8)
	copy(k, balanceTracePrefix)
	binary.BigEndian.PutUint64(k[len(balanceTracePrefix):], uint64(blockNum))
	return k
}

// rewardViKey builds the per-(cycle, witness) VI key. Format:
//   rvi- || cycle big-endian 8B || '-' || addr
// The "-vi" suffix is omitted — all non-sentinel keys in this store are VI
// values, so the suffix adds no disambiguation value in go-tron's layout.
func rewardViKey(cycle int64, addr []byte) []byte {
	k := make([]byte, 0, len(rewardViPrefix)+8+1+len(addr))
	k = append(k, rewardViPrefix...)
	var cb [8]byte
	binary.BigEndian.PutUint64(cb[:], uint64(cycle))
	k = append(k, cb[:]...)
	k = append(k, '-')
	return append(k, addr...)
}

// rewardViIsDoneKey is the IS_DONE sentinel key.
func rewardViIsDoneKey() []byte {
	return append(append([]byte{}, rewardViPrefix...), 0x00)
}

// drAccIdxKey builds a DelegatedResourceAccountIndex key. Mirrors
// java-tron's Bytes.concat(PREFIX, anchor, counterparty) layout with
// the prefix byte selecting V1/V2 × from/to.
func drAccIdxKey(dir drAccIdxDirection, anchor, counterparty []byte) []byte {
	k := make([]byte, 0, len(drAccIdxPrefix)+1+len(anchor)+len(counterparty))
	k = append(k, drAccIdxPrefix...)
	k = append(k, byte(dir))
	k = append(k, anchor...)
	return append(k, counterparty...)
}

// drAccIdxAnchorPrefix returns the byte prefix that iterates every
// (counterparty, value) under a given direction+anchor — drax- ||
// direction || anchor.
func drAccIdxAnchorPrefix(dir drAccIdxDirection, anchor []byte) []byte {
	k := make([]byte, 0, len(drAccIdxPrefix)+1+len(anchor))
	k = append(k, drAccIdxPrefix...)
	k = append(k, byte(dir))
	return append(k, anchor...)
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

func marketPairToPriceKey(sellTokenID, buyTokenID []byte) []byte {
	k := append(append([]byte{}, marketPairToPricePrefix...), sellTokenID...)
	k = append(k, '|')
	return append(k, buyTokenID...)
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

func zkProofKey(proof []byte) []byte {
	return append(append([]byte{}, zkProofPrefix...), proof...)
}

func incrMerkleTreeKey(root []byte) []byte {
	return append(append([]byte{}, incrMerkleTreePrefix...), root...)
}

func noteCommitmentKey(index int64) []byte {
	k := make([]byte, len(noteCommitmentPrefix)+8)
	copy(k, noteCommitmentPrefix)
	binary.BigEndian.PutUint64(k[len(noteCommitmentPrefix):], uint64(index))
	return k
}
