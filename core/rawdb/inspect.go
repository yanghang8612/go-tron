package rawdb

import (
	"bytes"
	"fmt"
	"sort"
	"time"

	"github.com/ethereum/go-ethereum/ethdb"
)

// KeyspaceStat accounts for the uncompressed key/value bytes in one logical
// rawdb keyspace. LogicalBytes excludes Pebble compression, indexes, filters,
// WAL files, obsolete tables, and LSM write amplification.
type KeyspaceStat struct {
	Name         string  `json:"name"`
	KeyPattern   string  `json:"key_pattern"`
	Rows         uint64  `json:"rows"`
	KeyBytes     uint64  `json:"key_bytes"`
	ValueBytes   uint64  `json:"value_bytes"`
	LogicalBytes uint64  `json:"logical_bytes"`
	Percent      float64 `json:"percent"`
}

// DatabaseInspection is a one-pass accounting of every live key returned by
// the database iterator.
type DatabaseInspection struct {
	Rows         uint64         `json:"rows"`
	KeyBytes     uint64         `json:"key_bytes"`
	ValueBytes   uint64         `json:"value_bytes"`
	LogicalBytes uint64         `json:"logical_bytes"`
	Keyspaces    []KeyspaceStat `json:"keyspaces"`
}

type InspectProgress struct {
	Rows         uint64
	LogicalBytes uint64
	Elapsed      time.Duration
}

type InspectOptions struct {
	ProgressInterval time.Duration
	Progress         func(InspectProgress)
}

type inspectKeyspace struct {
	name    string
	pattern string
	key     []byte
	prefix  []byte
}

var inspectSingletons = []inspectKeyspace{
	{name: "head-block", pattern: "=LastBlock", key: headBlockKey},
	{name: "head-solid-block", pattern: "=LastSolidBlock", key: headSolidBlockKey},
	{name: "total-transaction-count", pattern: "=total-tx-count", key: totalTransactionCountKey},
	{name: "history-prune-mode", pattern: "=history-prune-mode-v1", key: historyPruneModeKey},
	{name: "legacy-state-schema-version", pattern: "=state-schema-version", key: []byte("state-schema-version")},
	{name: "genesis-state-root", pattern: "=genesis-state-root", key: genesisStateRootKey},
	{name: "witness-schedule", pattern: "=ws", key: witnessScheduleKey},
	{name: "shuffled-witnesses", pattern: "=ws-shuffled", key: shuffledWitnessesKey},
	{name: "previous-shuffled-witnesses", pattern: "=ws-prev-shuffled", key: previousShuffledWitnessesKey},
	{name: "genesis-witnesses", pattern: "=GenesisWitnesses", key: genesisWitnessesKey},
	{name: "note-commitment-count", pattern: "=nccount", key: noteCommitmentCountKey},
	{name: "incremental-merkle-last-tree", pattern: "=imt-LAST_TREE", key: incrMerkleLastTreeKey},
	{name: "incremental-merkle-current-tree", pattern: "=imt-CURRENT_TREE", key: incrMerkleCurrentTreeKey},
	{name: "cycle-reward-pending", pattern: "=cycle-reward-pending-v1", key: cycleRewardPendingKey},
	{name: "state-commitment-engine-state", pattern: "=state-commitment-engine-state-v1", key: stateCommitmentEngineStateKey},
	{name: "latest-pbft-block", pattern: "=LATEST_PBFT_BLOCK_NUM", key: latestPbftBlockNumKey},
}

var inspectPrefixes = []inspectKeyspace{
	{name: "state-commitment-branch", pattern: "state-commitment-branch-v1-*", prefix: stateCommitmentBranchPrefix},
	{name: "state-commitment-domain", pattern: "state-commitment-domain-v1-*", prefix: stateCommitmentDomainPrefix},
	{name: "state-account-latest", pattern: "state-account-latest-v1-*", prefix: stateAccountLatestPrefix},
	{name: "legacy-state-account-latest-v2", pattern: "state-account-latest-v2-*", prefix: []byte("state-account-latest-v2-")},
	{name: "state-kv-generation", pattern: "state-kv-generation-v2-*", prefix: stateKVGenerationPrefix},
	{name: "state-change-index", pattern: "state-change-index-v2-*", prefix: stateChangeInversePrefix},
	{name: "state-changeset", pattern: "state-changeset-v2-*", prefix: stateChangeSetPrefix},
	{name: "state-kv-latest", pattern: "state-kv-latest-v2-*", prefix: stateKVLatestPrefix},
	{name: "sync-staged-block", pattern: "sync-staged-block-v1-*", prefix: syncStagedBlockPrefix},
	{name: "stage-progress", pattern: "stage-progress-v1-*", prefix: stageProgressPrefix},
	{name: "state-tx-range", pattern: "state-tx-range-v1-*", prefix: stateTxRangePrefix},
	{name: "state-code", pattern: "state-code-v1-*", prefix: stateCodePrefix},
	{name: "witness-latest-block", pattern: "wlb-*", prefix: witnessLatestBlockPrefix},
	{name: "block-number-hash", pattern: "bnh-*", prefix: blockNumberHashPrefix},
	{name: "block-state-root", pattern: "bsr-*", prefix: blockStateRootPrefix},
	{name: "balance-trace", pattern: "btrace-*", prefix: balanceTracePrefix},
	{name: "checkpoint-v2", pattern: "cpv2-*", prefix: checkPointV2Prefix},
	{name: "delegated-resource-account-index", pattern: "drax-*", prefix: drAccIdxPrefix},
	{name: "transaction-info-by-block", pattern: "tib-*", prefix: txInfoBlockPrefix},
	{name: "transaction-info", pattern: "ti-*", prefix: txInfoPrefix},
	{name: "transaction-index", pattern: "tx-*", prefix: txPrefix},
	{name: "block-hash", pattern: "bh-*", prefix: blockHashPrefix},
	{name: "block-body", pattern: "b-*", prefix: blockPrefix},
	{name: "account", pattern: "a-*", prefix: accountPrefix},
	{name: "witness", pattern: "w-*", prefix: witnessPrefix},
	{name: "code", pattern: "c-*", prefix: codePrefix},
	{name: "contract", pattern: "ct-*", prefix: contractPrefix},
	{name: "contract-storage", pattern: "s-*", prefix: storagePrefix},
	{name: "dynamic-property", pattern: "dp-*", prefix: dynPropPrefix},
	{name: "delegated-resource", pattern: "dr-*", prefix: delegationPrefix},
	{name: "delegation-index", pattern: "dri-*", prefix: delegationIndexPrefix},
	{name: "witness-brokerage", pattern: "wb-*", prefix: brokeragePrefix},
	{name: "nullifier", pattern: "nf-*", prefix: nullifierPrefix},
	{name: "note-commitment", pattern: "nc-*", prefix: noteCommitmentPrefix},
	{name: "zk-proof", pattern: "zkp-*", prefix: zkProofPrefix},
	{name: "incremental-merkle-tree", pattern: "imt-*", prefix: incrMerkleTreePrefix},
	{name: "merkle-tree-index", pattern: "mti-*", prefix: merkleTreeIndexPrefix},
	{name: "fork-version", pattern: "fv-*", prefix: forkStatsPrefix},
	{name: "delegation-reward", pattern: "dl-*", prefix: delegRewardPrefix},
	{name: "contract-abi", pattern: "abi-*", prefix: abiPrefix},
	{name: "contract-state", pattern: "cs-*", prefix: contractStatePrefix},
	{name: "account-asset", pattern: "aa-*", prefix: accountAssetPrefix},
	{name: "account-trace", pattern: "at-*", prefix: accountTracePrefix},
	{name: "section-bloom", pattern: "sb-*", prefix: sectionBloomPrefix},
	{name: "tree-block-index", pattern: "tbi-*", prefix: treeBlockIndexPrefix},
	{name: "pbft-sign-data", pattern: "psd-*", prefix: pbftSignDataPrefix},
	{name: "reward-vi", pattern: "rvi-*", prefix: rewardViPrefix},
	{name: "tapos-recent-block", pattern: "tps-*", prefix: taposPrefix},
}

var (
	inspectSingletonsByFirst = indexInspectKeyspaces(inspectSingletons, false)
	inspectPrefixesByFirst   = indexInspectKeyspaces(inspectPrefixes, true)
)

func InspectDatabase(db ethdb.Iteratee, opts InspectOptions) (DatabaseInspection, error) {
	if db == nil {
		return DatabaseInspection{}, fmt.Errorf("inspect database: nil database")
	}
	stats := make(map[string]*KeyspaceStat, len(inspectSingletons)+len(inspectPrefixes)+2)
	started := time.Now()
	nextProgress := started.Add(opts.ProgressInterval)
	it := db.NewIterator(nil, nil)
	defer it.Release()

	var report DatabaseInspection
	for it.Next() {
		key, value := it.Key(), it.Value()
		space := inspectKeyspaceFor(key)
		stat := stats[space.name]
		if stat == nil {
			stat = &KeyspaceStat{Name: space.name, KeyPattern: space.pattern}
			stats[space.name] = stat
		}
		keyBytes, valueBytes := uint64(len(key)), uint64(len(value))
		stat.Rows++
		stat.KeyBytes += keyBytes
		stat.ValueBytes += valueBytes
		stat.LogicalBytes += keyBytes + valueBytes
		report.Rows++
		report.KeyBytes += keyBytes
		report.ValueBytes += valueBytes
		report.LogicalBytes += keyBytes + valueBytes

		if opts.Progress != nil && opts.ProgressInterval > 0 && report.Rows%100000 == 0 {
			now := time.Now()
			if !now.Before(nextProgress) {
				opts.Progress(InspectProgress{Rows: report.Rows, LogicalBytes: report.LogicalBytes, Elapsed: now.Sub(started)})
				nextProgress = now.Add(opts.ProgressInterval)
			}
		}
	}
	if err := it.Error(); err != nil {
		return DatabaseInspection{}, fmt.Errorf("inspect database iterator: %w", err)
	}

	for _, stat := range stats {
		if report.LogicalBytes != 0 {
			stat.Percent = float64(stat.LogicalBytes) * 100 / float64(report.LogicalBytes)
		}
		report.Keyspaces = append(report.Keyspaces, *stat)
	}
	sort.Slice(report.Keyspaces, func(i, j int) bool {
		if report.Keyspaces[i].LogicalBytes == report.Keyspaces[j].LogicalBytes {
			return report.Keyspaces[i].Name < report.Keyspaces[j].Name
		}
		return report.Keyspaces[i].LogicalBytes > report.Keyspaces[j].LogicalBytes
	})
	return report, nil
}

func inspectKeyspaceFor(key []byte) inspectKeyspace {
	if len(key) == 0 {
		return inspectKeyspace{name: "unclassified", pattern: "<unknown>"}
	}
	for _, space := range inspectSingletonsByFirst[key[0]] {
		if bytes.Equal(key, space.key) {
			return space
		}
	}
	for _, space := range inspectPrefixesByFirst[key[0]] {
		if bytes.HasPrefix(key, space.prefix) {
			return space
		}
	}
	if len(key) == 32 {
		return inspectKeyspace{name: "legacy-trie-node", pattern: "<32-byte hash>"}
	}
	return inspectKeyspace{name: "unclassified", pattern: "<unknown>"}
}

func indexInspectKeyspaces(spaces []inspectKeyspace, prefixes bool) [256][]inspectKeyspace {
	var buckets [256][]inspectKeyspace
	for _, space := range spaces {
		key := space.key
		if prefixes {
			key = space.prefix
		}
		if len(key) > 0 {
			buckets[key[0]] = append(buckets[key[0]], space)
		}
	}
	if prefixes {
		for i := range buckets {
			sort.SliceStable(buckets[i], func(a, b int) bool {
				return len(buckets[i][a].prefix) > len(buckets[i][b].prefix)
			})
		}
	}
	return buckets
}
