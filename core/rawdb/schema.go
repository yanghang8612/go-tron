package rawdb

import (
	"encoding/binary"
	"strconv"

	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/state/kvdomains"
)

var (
	headBlockKey             = []byte("LastBlock")
	headSolidBlockKey        = []byte("LastSolidBlock")
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

	blockPrefix              = []byte("b-")
	blockHashPrefix          = []byte("bh-")
	txPrefix                 = []byte("tx-")
	txInfoPrefix             = []byte("ti-")
	txInfoBlockPrefix        = []byte("tib-")
	accountPrefix            = []byte("a-")
	witnessPrefix            = []byte("w-")
	witnessLatestBlockPrefix = []byte("wlb-") // per-witness latest produced block number
	codePrefix               = []byte("c-")
	contractPrefix           = []byte("ct-")
	storagePrefix            = []byte("s-")
	dynPropPrefix            = []byte("dp-")

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

	// genesisWitnessesKey holds the immutable {address, initial_vote_count}
	// list from the chain's Genesis config. Required to faithfully port
	// java-tron's tryRemoveThePowerOfTheGr, which subtracts each GR's
	// *initial* vote count (not the current count after voting activity)
	// when REMOVE_THE_POWER_OF_THE_GR fires. Written once at genesis setup
	// and never mutated thereafter.
	// Value: 4-byte big-endian count || N * (21B address || 8B BE vote count)
	genesisWitnessesKey = []byte("GenesisWitnesses")

	delegationPrefix      = []byte("dr-")
	delegationIndexPrefix = []byte("dri-")
	brokeragePrefix       = []byte("wb-")

	// TRC10 asset stores (formerly ast-/astl-/astn-/asto-/asti-) are rooted into
	// the reserved system account's SystemAsset KV; see core/state/asset_store.go.

	// Market (DEX) records (mo-, mao-, mop-, mptop-, mpl-) are no longer flat:
	// they are rooted into the reserved system account's SystemMarket KV (see
	// core/state/market_store.go) so the whole order book rewinds with the full
	// state root. The PriceKey helper below stays here because it is pure price
	// normalization shared by the market actuators and the rooted store.

	nullifierPrefix        = []byte("nf-")
	noteCommitmentPrefix   = []byte("nc-")
	noteCommitmentCountKey = []byte("nccount")

	// zkProofPrefix (zkp-) maps to java-tron's ZKProofStore (db "zkProof").
	// Stores the cached proof-verification result for each shielded transaction.
	// Key:   zkp- || transaction raw-data hash
	// Value: 0x01 (proof accepted) or 0x00 (proof rejected)
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

	// incrMerkleLastTreeKey / incrMerkleCurrentTreeKey mirror java-tron's
	// MerkleContainer "LAST_TREE" / "CURRENT_TREE" sentinels. They live in
	// the imt- namespace; the 13/16-byte sentinel keys cannot collide with
	// the 36-byte (imt- || 32-byte root) entries above.
	// - last:    best snapshot after the most recent block (anchor source)
	// - current: mutable working copy reset at block start, advanced per
	//            shielded receive, promoted to last on block success
	incrMerkleLastTreeKey    = []byte("imt-LAST_TREE")
	incrMerkleCurrentTreeKey = []byte("imt-CURRENT_TREE")

	// merkleTreeIndexPrefix maps block number → 32-byte merkle root.
	// Mirrors java-tron's MerkleTreeIndexStore. Used by wallet APIs to
	// reconstruct historical trees; not consulted on the spend-validation
	// hot path (that lookup is keyed by root via incrMerkleTreePrefix).
	// Key:   mti- || big-endian uint64 block number
	// Value: 32-byte commitment tree root
	merkleTreeIndexPrefix = []byte("mti-")

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

	// The account name index (java-tron AccountIndexStore) and account-id index
	// (AccountIdIndexStore) are no longer flat keys: they are rooted into the
	// system account's SystemAccountIndex KV (see core/state/account_index_store.go),
	// so they rewind with the full state root.

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

	// stateKVLatestPrefix is the Erigon-style physical latest-state index for
	// generic account KV. It is not the commitment itself; the account KV MPT
	// root in StateAccountV2 remains authoritative in this phase. The index is
	// a prefix-iterable mirror used for domain iteration and prefix deletion.
	//
	// Key:   state-kv-latest-v2- || owner AccountID20 || generation u64
	//        || domain u16 || logical key
	// Value: 0x01 || value, preserving empty-but-present values.
	stateKVLatestPrefix = []byte("state-kv-latest-v2-")

	// stateCodePrefix is the content-addressed TVM bytecode domain.
	// Account envelopes commit only the code hash; code bytes are immutable
	// payloads keyed by that hash.
	//
	// Key:   state-code-v1- || code_hash32
	// Value: contract bytecode
	stateCodePrefix = []byte("state-code-v1-")

	// stateTxRangePrefix records the first Erigon-style state transaction
	// numbering layer. The first implementation assigns one txNum per block:
	// begin_tx_num == end_tx_num == block number. Later phases can split a
	// block into per-transaction txNums without changing consumers that read
	// the range row first.
	//
	// Key:   state-tx-range-v1- || blockNum u64
	// Value: RLP(StateTxRange)
	stateTxRangePrefix = []byte("state-tx-range-v1-")

	// stateChangeSetPrefix records pre-values for latest-domain writes. Rows
	// are block-scoped and sequence-ordered in commit order so unwind code can
	// replay them backwards for a block.
	//
	// Key:   state-changeset-v1- || blockNum u64 || seq u64
	// Value: RLP(StateDomainChange)
	stateChangeSetPrefix = []byte("state-changeset-v1-")

	// stateChangeInversePrefix is the owner/domain/key inverse index for
	// StateDomainChange rows. It lets GetAsOf find blocks that touched one
	// logical domain key without scanning every block changeset.
	//
	// Key:   state-change-index-v1- || owner20 || generation u64
	//        || domain u16 || logical_key || blockNum u64
	// Value: empty
	stateChangeInversePrefix = []byte("state-change-index-v1-")

	// stateCommitmentPrefix stores transitional commitment checkpoints for the
	// Erigon-style domain state engine. The current checkpoints are debug
	// commitments over physical latest-domain tables, not yet the authoritative
	// internal full state root.
	//
	// Key:   state-commitment-v1- || blockNum u64
	// Value: RLP(StateCommitmentCheckpoint)
	stateCommitmentPrefix = []byte("state-commitment-v1-")

	// stateKVGenerationPrefix stores the latest physical generation observed
	// for an account. It lets a later recreate pick generation+1 without
	// scanning or deleting old latest rows from prior incarnations.
	//
	// Key:   state-kv-generation-v2- || owner AccountID20
	// Value: generation u64
	stateKVGenerationPrefix = []byte("state-kv-generation-v2-")

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

	// taposPrefix (tps-) maps to java-tron's RecentBlockStore (db name
	// "recent-block"). Holds the recent 8-byte block-hash tails used for
	// TAPOS (TransactionUtil.validateTapos). The key is the low 2 bytes of
	// a block number — a 65536-slot ring is naturally bounded because each
	// new block at the same lower-half number overwrites the previous
	// occupant. java-tron's RecentBlockStore stores exactly this layout.
	// Key:   tps- || refBlockBytes (2B = blockNum[6..7] big-endian)
	// Value: 8-byte hash tail (block.Hash()[8..16], matching
	//        TransactionCapsule.getRefBlockHash()).
	taposPrefix = []byte("tps-")

	// drAccIdxPrefix (drax-) maps to java-tron's
	// DelegatedResourceAccountIndexStore (db name
	// "DelegatedResourceAccountIndex"). Holds forward and reverse
	// bindings for both Freeze-V1 and Freeze-V2 delegation pairs, one
	// record per (direction, anchor, counterparty) tuple — lets a
	// wallet answer "who delegated TO me" or "who have I delegated to"
	// via prefix scans without walking the primary DelegatedResource
	// store. The direction byte distinguishes V1/V2 × from/to:
	//   0x00 legacy V1 aggregate (pre allow_delegate_optimization)
	//   0x01 V1 from-anchor   (anchor=from, counterparty=to)
	//   0x02 V1 to-anchor     (anchor=to,   counterparty=from)
	//   0x03 V2 from-anchor   "
	//   0x04 V2 to-anchor     "
	// Key:   aggregate: drax- || 0x00 || account
	//        directional: drax- || direction || anchor || counterparty
	// Value: aggregate stores Account + FromAccounts/ToAccounts;
	//        directional stores Account=counterparty + Timestamp.
	drAccIdxPrefix = []byte("drax-")

	// --- State History Index (SHI) ---
	//
	// The sh- family of prefixes implements gtron's archive-state query
	// support, modelled on go-ethereum's pathdb history. Each block
	// applied by the chain writes:
	//   1. one sh-m- row capturing per-block metadata (count, hash, ver)
	//   2. one sh-a- row per account whose state mutated, with the
	//      account's PRE-block proto/code/contract-meta as a blob
	//   3. one sh-s- row per TVM storage slot that mutated, value =
	//      32-byte pre-slot value
	//   4. one sh-i-a- row per (addr, blockNum) for fast "find latest
	//      change ≤ N for this addr" lookups
	//   5. one sh-i-s- row per (addr, slot, blockNum) for the same on
	//      storage slots
	//
	// Reads at block N reconstruct state by starting from HEAD and
	// rolling back deltas for blocks (N, HEAD]; the inverse index lets
	// callers skip blocks that didn't touch the queried key. The full
	// design and read-path algorithm live in
	// docs/superpowers/specs/2026-05-19-state-history-index-design.md.

	// shMetaPrefix (sh-m-) holds StateHistoryMeta protos per block.
	// Key:   sh-m- || big-endian uint64 blockNum
	// Value: proto-encoded historystate.StateHistoryMeta
	shMetaPrefix = []byte("sh-m-")

	// shAccountPrefix (sh-a-) holds AccountDelta blobs (one per touched
	// account per block).
	// Key:   sh-a- || big-endian uint64 blockNum || 21B addr
	// Value: proto-encoded historystate.AccountDelta
	shAccountPrefix = []byte("sh-a-")

	// shSlotPrefix (sh-s-) holds TVM storage slot pre-values (one per
	// touched (addr, slot) per block). Value is raw 32-byte pre-value;
	// no proto wrapper. An empty value byte slice would be ambiguous
	// with "absent", so we write a single 0x00 sentinel byte when the
	// pre-value is all-zero — readers normalise back to Hash{}.
	// Key:   sh-s- || big-endian uint64 blockNum || 21B addr || 32B slotkey
	// Value: 32B slot pre-value (or 1-byte 0x00 sentinel for the
	//        all-zero pre-value).
	shSlotPrefix = []byte("sh-s-")

	// shAddrInversePrefix (sh-i-a-) is the inverse index for account
	// deltas. Addresses come FIRST so a prefix scan finds "every block
	// that touched addr" without scanning the entire history; the
	// embedded blockNum lets callers SeekLT a target N.
	// Key:   sh-i-a- || 21B addr || big-endian uint64 blockNum
	// Value: empty (key-only marker)
	shAddrInversePrefix = []byte("sh-i-a-")

	// shSlotInversePrefix (sh-i-s-) is the inverse index for slot
	// deltas. Same layout shape as shAddrInversePrefix but with an
	// extra 32-byte slot segment.
	// Key:   sh-i-s- || 21B addr || 32B slotkey || big-endian uint64 blockNum
	// Value: empty (key-only marker)
	shSlotInversePrefix = []byte("sh-i-s-")

	// shConfigKey is the singleton HistoryConfig sentinel. Carries the
	// archive-vs-full mode, prune window, first/last available block
	// numbers, and schema version. Distinct prefix segment ("-cfg-")
	// makes collision with sh-a-/sh-m-/etc impossible regardless of
	// addr/blockNum content (length differs and segment differs).
	shConfigKey = []byte("sh-cfg-")

	// shBackfillCursorKey is the singleton resume cursor for the Slice 6
	// operator-recovery backfill tool. It holds the big-endian uint64 of
	// the last block whose history rows the backfill has re-derived, so a
	// `gtron history backfill --resume` picks up at cursor+1 after an
	// interrupt. Kept separate from HistoryConfig.FirstBlock (which the
	// live writer and the pruner own) so backfill progress can't be
	// confused with prune progress. Distinct "-bf-cursor-" segment avoids
	// collision with the sh-a-/sh-m-/sh-cfg- families.
	shBackfillCursorKey = []byte("sh-bf-cursor-")
)

// HistorySchemaVersion is the on-disk format version for the State
// History Index. Bump any time the wire format of StateHistoryMeta /
// AccountDelta / HistoryConfig or the key layout above changes. The
// HistoryConfig.schema_ver field is checked on startup against this
// constant — a mismatch refuses to launch with a "rebuild your archive"
// error per the spec.
const HistorySchemaVersion uint32 = 1

// DrAccIdxDirection enumerates the four sub-indices of
// DelegatedResourceAccountIndexStore, matching java-tron's 0x01..0x04
// prefix bytes for V1/V2 × from/to-anchored lookups.
type DrAccIdxDirection byte

const (
	DrAccIdxLegacy DrAccIdxDirection = 0x00 // V1 aggregate record before proposal #69
	DrAccIdxV1From DrAccIdxDirection = 0x01 // V1: from-anchored  (anchor=from, counterparty=to)
	DrAccIdxV1To   DrAccIdxDirection = 0x02 // V1: to-anchored    (anchor=to,   counterparty=from)
	DrAccIdxV2From DrAccIdxDirection = 0x03 // V2: from-anchored
	DrAccIdxV2To   DrAccIdxDirection = 0x04 // V2: to-anchored
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

func delegationKey(from, to []byte) []byte {
	k := make([]byte, len(delegationPrefix)+len(from)+len(to))
	copy(k, delegationPrefix)
	copy(k[len(delegationPrefix):], from)
	copy(k[len(delegationPrefix)+len(from):], to)
	return k
}

func delegationKeyV2(from, to []byte, locked bool) []byte {
	k := make([]byte, 0, len(delegationPrefix)+1+len(from)+len(to))
	k = append(k, delegationPrefix...)
	if locked {
		k = append(k, 0x02)
	} else {
		k = append(k, 0x01)
	}
	k = append(k, from...)
	return append(k, to...)
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

func stateKVLatestKey(owner common.Address, generation uint64, domain kvdomains.KVDomain, logicalKey []byte) []byte {
	return append(stateKVLatestDomainPrefix(owner, generation, domain), logicalKey...)
}

func stateKVLatestDomainPrefix(owner common.Address, generation uint64, domain kvdomains.KVDomain) []byte {
	accountID := owner.AccountID()
	k := make([]byte, 0, len(stateKVLatestPrefix)+common.AccountIDLength+8+2)
	k = append(k, stateKVLatestPrefix...)
	k = append(k, accountID[:]...)
	var buf [10]byte
	binary.BigEndian.PutUint64(buf[:8], generation)
	binary.BigEndian.PutUint16(buf[8:], uint16(domain))
	return append(k, buf[:]...)
}

func stateKVLatestLogicalPrefix(owner common.Address, generation uint64, domain kvdomains.KVDomain, logicalPrefix []byte) []byte {
	return append(stateKVLatestDomainPrefix(owner, generation, domain), logicalPrefix...)
}

func stateKVLatestOwnerPrefix(owner common.Address) []byte {
	accountID := owner.AccountID()
	k := make([]byte, 0, len(stateKVLatestPrefix)+common.AccountIDLength)
	k = append(k, stateKVLatestPrefix...)
	return append(k, accountID[:]...)
}

func stateKVGenerationKey(owner common.Address) []byte {
	accountID := owner.AccountID()
	k := make([]byte, 0, len(stateKVGenerationPrefix)+common.AccountIDLength)
	k = append(k, stateKVGenerationPrefix...)
	return append(k, accountID[:]...)
}

func stateCodeKey(hash common.Hash) []byte {
	k := make([]byte, 0, len(stateCodePrefix)+common.HashLength)
	k = append(k, stateCodePrefix...)
	return append(k, hash.Bytes()...)
}

func stateTxRangeKey(blockNum uint64) []byte {
	k := make([]byte, len(stateTxRangePrefix)+8)
	copy(k, stateTxRangePrefix)
	binary.BigEndian.PutUint64(k[len(stateTxRangePrefix):], blockNum)
	return k
}

func stateChangeSetKey(blockNum, seq uint64) []byte {
	k := make([]byte, len(stateChangeSetPrefix)+16)
	copy(k, stateChangeSetPrefix)
	binary.BigEndian.PutUint64(k[len(stateChangeSetPrefix):], blockNum)
	binary.BigEndian.PutUint64(k[len(stateChangeSetPrefix)+8:], seq)
	return k
}

func stateChangeSetBlockPrefix(blockNum uint64) []byte {
	k := make([]byte, len(stateChangeSetPrefix)+8)
	copy(k, stateChangeSetPrefix)
	binary.BigEndian.PutUint64(k[len(stateChangeSetPrefix):], blockNum)
	return k
}

func stateCommitmentCheckpointKey(blockNum uint64) []byte {
	k := make([]byte, len(stateCommitmentPrefix)+8)
	copy(k, stateCommitmentPrefix)
	binary.BigEndian.PutUint64(k[len(stateCommitmentPrefix):], blockNum)
	return k
}

func stateChangeInverseKey(owner common.Address, generation uint64, domain kvdomains.KVDomain, logicalKey []byte, blockNum uint64) []byte {
	k := stateChangeInverseKeyPrefix(owner, generation, domain, logicalKey)
	var b [8]byte
	binary.BigEndian.PutUint64(b[:], blockNum)
	return append(k, b[:]...)
}

func stateChangeInverseKeyPrefix(owner common.Address, generation uint64, domain kvdomains.KVDomain, logicalKey []byte) []byte {
	accountID := owner.AccountID()
	k := make([]byte, 0, len(stateChangeInversePrefix)+common.AccountIDLength+8+2+len(logicalKey))
	k = append(k, stateChangeInversePrefix...)
	k = append(k, accountID[:]...)
	var b [10]byte
	binary.BigEndian.PutUint64(b[:8], generation)
	binary.BigEndian.PutUint16(b[8:], uint16(domain))
	k = append(k, b[:]...)
	return append(k, logicalKey...)
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
//
//	rvi- || cycle big-endian 8B || '-' || addr
//
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
func drAccIdxKey(dir DrAccIdxDirection, anchor, counterparty []byte) []byte {
	k := make([]byte, 0, len(drAccIdxPrefix)+1+len(anchor)+len(counterparty))
	k = append(k, drAccIdxPrefix...)
	k = append(k, byte(dir))
	k = append(k, anchor...)
	return append(k, counterparty...)
}

func drAccIdxLegacyKey(account []byte) []byte {
	return drAccIdxKey(DrAccIdxLegacy, account, nil)
}

// drAccIdxAnchorPrefix returns the byte prefix that iterates every
// (counterparty, value) under a given direction+anchor — drax- ||
// direction || anchor.
func drAccIdxAnchorPrefix(dir DrAccIdxDirection, anchor []byte) []byte {
	k := make([]byte, 0, len(drAccIdxPrefix)+1+len(anchor))
	k = append(k, drAccIdxPrefix...)
	k = append(k, byte(dir))
	return append(k, anchor...)
}

// taposKey builds a RecentBlockStore key from the 2-byte refBlockBytes
// (low 16 bits of block number, big-endian).
func taposKey(refBlockBytes []byte) []byte {
	return append(append([]byte{}, taposPrefix...), refBlockBytes...)
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

func merkleTreeIndexKey(blockNum int64) []byte {
	k := make([]byte, len(merkleTreeIndexPrefix)+8)
	copy(k, merkleTreeIndexPrefix)
	binary.BigEndian.PutUint64(k[len(merkleTreeIndexPrefix):], uint64(blockNum))
	return k
}

// --- State History Index key builders ---

// historyMetaKey builds the sh-m- key: prefix || big-endian uint64 blockNum.
func historyMetaKey(blockNum uint64) []byte {
	k := make([]byte, len(shMetaPrefix)+8)
	copy(k, shMetaPrefix)
	binary.BigEndian.PutUint64(k[len(shMetaPrefix):], blockNum)
	return k
}

// historyAccountKey builds the sh-a- key: prefix || blockNum || 21B addr.
// blockNum-first ordering lets a per-block range-delete prune by big-endian
// blockNum range. addr lives at the tail.
func historyAccountKey(blockNum uint64, addr []byte) []byte {
	k := make([]byte, 0, len(shAccountPrefix)+8+len(addr))
	k = append(k, shAccountPrefix...)
	var nb [8]byte
	binary.BigEndian.PutUint64(nb[:], blockNum)
	k = append(k, nb[:]...)
	return append(k, addr...)
}

// historyAccountBlockPrefix returns the prefix that scopes a range scan to
// "every account delta at blockNum": sh-a- || blockNum. Useful for pruning
// or per-block diagnostics.
func historyAccountBlockPrefix(blockNum uint64) []byte {
	k := make([]byte, len(shAccountPrefix)+8)
	copy(k, shAccountPrefix)
	binary.BigEndian.PutUint64(k[len(shAccountPrefix):], blockNum)
	return k
}

// historySlotKey builds the sh-s- key: prefix || blockNum || 21B addr || 32B slot.
func historySlotKey(blockNum uint64, addr []byte, slot []byte) []byte {
	k := make([]byte, 0, len(shSlotPrefix)+8+len(addr)+len(slot))
	k = append(k, shSlotPrefix...)
	var nb [8]byte
	binary.BigEndian.PutUint64(nb[:], blockNum)
	k = append(k, nb[:]...)
	k = append(k, addr...)
	return append(k, slot...)
}

// historySlotBlockPrefix returns the prefix that scopes a range scan to
// "every slot delta at blockNum": sh-s- || blockNum. Counterpart to
// historyAccountBlockPrefix used by the pruner.
func historySlotBlockPrefix(blockNum uint64) []byte {
	k := make([]byte, len(shSlotPrefix)+8)
	copy(k, shSlotPrefix)
	binary.BigEndian.PutUint64(k[len(shSlotPrefix):], blockNum)
	return k
}

// historyAddrInverseKey builds the sh-i-a- key: prefix || 21B addr || blockNum.
// addr-first ordering lets a prefix scan find "every block that touched
// this addr" without scanning the entire history.
func historyAddrInverseKey(addr []byte, blockNum uint64) []byte {
	k := make([]byte, 0, len(shAddrInversePrefix)+len(addr)+8)
	k = append(k, shAddrInversePrefix...)
	k = append(k, addr...)
	var nb [8]byte
	binary.BigEndian.PutUint64(nb[:], blockNum)
	return append(k, nb[:]...)
}

// historyAddrInverseAddrPrefix returns the prefix that scopes a range scan to
// "every block-touch row for this addr": sh-i-a- || addr.
func historyAddrInverseAddrPrefix(addr []byte) []byte {
	k := make([]byte, 0, len(shAddrInversePrefix)+len(addr))
	k = append(k, shAddrInversePrefix...)
	return append(k, addr...)
}

// historySlotInverseKey builds the sh-i-s- key: prefix || 21B addr || 32B slot || blockNum.
func historySlotInverseKey(addr []byte, slot []byte, blockNum uint64) []byte {
	k := make([]byte, 0, len(shSlotInversePrefix)+len(addr)+len(slot)+8)
	k = append(k, shSlotInversePrefix...)
	k = append(k, addr...)
	k = append(k, slot...)
	var nb [8]byte
	binary.BigEndian.PutUint64(nb[:], blockNum)
	return append(k, nb[:]...)
}

// historySlotInverseSlotPrefix returns the prefix that scopes a range scan
// to "every block-touch row for this (addr, slot)": sh-i-s- || addr || slot.
func historySlotInverseSlotPrefix(addr []byte, slot []byte) []byte {
	k := make([]byte, 0, len(shSlotInversePrefix)+len(addr)+len(slot))
	k = append(k, shSlotInversePrefix...)
	k = append(k, addr...)
	return append(k, slot...)
}

// historyConfigKey returns the singleton HistoryConfig key.
func historyConfigKey() []byte {
	return append([]byte{}, shConfigKey...)
}

// historyBackfillCursorKey returns the singleton backfill resume-cursor key.
func historyBackfillCursorKey() []byte {
	return append([]byte{}, shBackfillCursorKey...)
}
