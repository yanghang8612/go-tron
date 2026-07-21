# Nile lite database parity audit

Use an identical, explicit height on both implementations. `gtron` imports the
target block and then keeps sync paused, including broadcast-block imports:

```bash
build/bin/gtron --testnet --datadir /data/gtron-nile --sync.stop-at 12345678
```

Wait for `Sync stopped at configured height`, verify the wallet API reports the
target height, then stop gtron cleanly. The comparer opens Pebble and LevelDB
read-only, but both engines use exclusive process locks, so neither node may be
running while it executes.

Build and run:

```bash
make db-compare
build/bin/db-compare \
  --height 12345678 \
  --gtron /data/gtron-nile \
  --java /data/java-nile/output-directory \
  --json > db-compare-12345678.json
```

Progress is enabled by default and is written to stderr, so `--json` stdout
remains valid JSON when redirected. When stdout is redirected to a regular
file as in the example, that file is replaced with the latest complete report
snapshot on every progress event instead of remaining empty until completion.
The snapshot's `progress` object identifies the current store/stage, processed
rows, elapsed time, current store result, and total mismatches. The final
rewrite removes `progress` and leaves the normal final report schema. Pipes and
terminals still receive only one final JSON document.

The command logs database opening, the height guard, every store's
start/skip/completion summary, and a row count plus the current
equal/different/missing/invalid/mismatch totals for long-running scans every
five seconds. This includes each phase of
`storage-row`: building the temporary gtron index, comparing Java rows, and
checking gtron-only rows. Pass `--quiet` to suppress these progress logs.

The paths may point directly to `gtron/chaindata` and `database` instead. Exit
status is 0 for no mismatch, 1 for state differences, and 2 for an operational
error (including either head not exactly matching `--height`). `--max-diffs`
(default 10000) caps retained details globally without changing mismatch
counts. `--max-diffs-per-store` (default 100) additionally reserves an
independent sample budget for each store, so a large early mismatch such as
`delegation` cannot consume every detail before `contract-state` is reached.
Set it to 0 to disable the per-store cap. When `--json` is
redirected to a regular file, the file is refreshed during the run;
`--live-max-diffs` (default 1000) caps only those intermediate snapshots so a
large final diagnostic set is not copied, sorted, or re-marshaled every five
seconds. Live JSON serialization runs asynchronously and coalesces stale
snapshots, so a slow output disk cannot stall the database scan. Intermediate
samples are selected round-robin across stores. The final JSON still retains
up to `--max-diffs` details and up to `--max-diffs-per-store` for each store.

The comparer enumerates every LevelDB directory in the java-tron input before
it compares data. A directory must be classified as a supported state store,
an explicitly excluded non-state store, or an unsupported state store. Unknown
directories and present-but-unsupported state stores set
`state_coverage_complete=false`; the command exits 2 even when all rows that
were compared happen to match. This is the guard against a newer java-tron
silently adding state that the audit does not inspect.

The full supported state-store set is:

- account state: `account`, `account-asset`, `account-index`,
  `accountid-index`;
- witness/governance: `witness`, `witness_schedule`, `votes`, `proposal`,
  `properties`;
- assets and exchanges: `asset-issue`, `asset-issue-v2`, `exchange`,
  `exchange-v2`;
- contracts: `contract`, `contract-state`, `abi`, `code`, `storage-row`;
- delegation and reward: `DelegatedResource`,
  `DelegatedResourceAccountIndex`, `delegation`, `reward-vi`;
- market: `market_account`, `market_order`, `market_pair_to_price`,
  `market_pair_price_to_order`;
- shielded/TAPOS state and indexes: `nullifier`, `zkProof`,
  `IncrementalMerkleTree`, `tree-block-index`, `recent-block`.

Java `FORK_VERSION_*` and `FORK_CONTROLLER<version>` rows are checked against
gtron's rooted fork controller. Unknown dynamic-property rows are mismatches,
not skipped. The account comparison normalizes java-tron's account-asset
physical optimization because the moved balances are checked independently in
`account-asset`.

The `contract` adapter compares serialized metadata bytes first. Exact matches
avoid unmarshalling both `SmartContract` messages; rows whose encoding differs
still fall back to protobuf semantic comparison. Inline ABI is excluded from
that fallback because java-tron moves it to the independently compared `abi`
store. Likewise, java-tron's inline `SmartContract.code_hash` is compared with
go-tron's `StateAccountV2.CodeHash`; bytecode itself remains independently
checked in `code`. Equivalent logical state is therefore not reported merely
because the two clients place ABI or code-hash data differently. Contract rows
are validated by a bounded parallel worker pool; the very large `delegation`
reward-history store uses the same ordered parallel point-lookup model.
`--workers=0` selects up to eight workers from `GOMAXPROCS`; pass `--workers=N`
to select an explicit value up to 64. Java LevelDB iteration remains
sequential, while copied batches perform concurrent read-only gtron Pebble
lookups and are merged back in original key order.

`storage-row` is compared in both directions. The tool builds a temporary
Pebble index under the OS temporary directory so a Nile contract-storage dump
does not need to fit in memory; plan for temporary free disk roughly equal to
the live storage-row data size. Accounts are also scanned in both directions
by default. Stores absent from a particular lite package are reported as
`present=false` and do not make coverage incomplete.

Current java-tron builds may contain `accountTrie`, `account-asset-issue`,
`IncrementalMerkleVoucher`, `staker`, `staker-index`, or `tracker`. go-tron has
no equivalent state model for these stores yet. If
any is present, it is reported in `unsupported_state_stores` and the audit
cannot pass. Its row count is still emitted as `skipped` in the per-store
result, so the report shows whether the unsupported store is empty or carries
data. This is intentional: implementing or explicitly scoping that state comes
before claiming parity.

The following discovered stores are classified but excluded from the mutable
head-state result: chain data (`block`, `block-index`, `trans`), history/audit
data (`transactionHistoryStore`, `transactionRetStore`, `account-trace`,
`balance-trace`), derived indexes/finality metadata (`section-bloom`,
`pbft-sign-data`, `common`, `common-database`), node metadata (`peers`), runtime
caches (`recent-transaction`, `trans-cache`, `block_KDB`), and recovery WALs
(`checkpoint`, `check-point-v2`, `tmp`). The requested head
block is still compared through `block` + `block-index` as the height/content
guard.

The Java input must use LevelDB. A RocksDB lite package is detected as an open
error; convert it with java-tron's Toolkit or export a LevelDB lite package
before comparing.

## Rebuilding after parity fixes

The comparer never repairs either database. Fixes that change replay-time
writes (contract ABI/code hash, Stake 2.0 indexes, shielded trees, or the
reward-vi cache) do not retroactively rewrite an existing gtron datadir.
Rebuild from genesis before using a new report as proof of full-state parity;
rewinding only to the target height leaves earlier missing rows unchanged.
