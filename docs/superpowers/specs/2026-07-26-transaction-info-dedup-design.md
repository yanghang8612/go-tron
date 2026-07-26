# TransactionInfo hot/cold deduplication — design

**Status:** Implemented
**Date:** 2026-07-26
**Related:** [chain freezer design](./2026-05-19-chain-freezer-design.md)

## Evidence and problem

An offline mainnet inspection at height 13,761,515 measured:

- 745,824,140 `ti-*` rows, 468.59 GiB logical, 86.87% of live Pebble bytes;
- 745,824,140 `tx-*` locator rows, 29.87 GiB logical;
- ancient `tx_infos`, 186.71 GiB physical;
- only 1,788 hot `tib-*` rows, proving block-level freezing was current.

The original freezer design assumed the per-transaction `ti-*` payload was
small enough to remain hot. In reality it duplicates each TransactionInfo
already held inside the block-number-keyed TransactionRet (`tib-*` before
freezing, ancient `tx_infos` after freezing).

## Storage model

New block commits persist:

```text
tx-<txID>       -> block number (8 bytes)
tib-<blockNum>  -> TransactionRet containing every TransactionInfo in the block
```

The freezer moves `tib-*` to ancient `tx_infos` as before. New commits do not
write `ti-<txID>`.

No consensus or protobuf format changes. The schema prefix remains readable for
upgrade compatibility but is deprecated.

## Read algorithm

`ReadTransactionInfo(txID)`:

1. Read `tx-<txID>` to resolve the block number.
2. Read ancient `tx_infos[blockNum]`, falling back to hot `tib-<blockNum>`.
3. Scan TransactionRet's protobuf field 3 payloads.
4. Inspect nested field 1 (`TransactionInfo.id`) without unmarshalling.
5. Unmarshal and return only the matching TransactionInfo.
6. If the locator or block-level row is unavailable, fall back to legacy
   `ti-<txID>` during the rollout window.

The average inspected block contains about 54 transactions. Wire scanning is
bounded by one block and avoids allocating every non-matching receipt.

## Migration and rollback

Migration is explicit and offline:

```text
gtron db prune-tx-info --datadir <path> --yes [--compact]
```

The command writes a durable DeleteRange over `[ti-, ti.)` and verifies logical
absence. It does not compact by default because immediate reclamation can
rewrite hundreds of GiB. `--compact` is an explicit maintenance-window choice.

Rollback to an older binary is safe before pruning. After pruning, an older
binary cannot serve transaction-info-by-ID and must not be used with that
datadir. Chain consensus state is unaffected.

## Acceptance criteria

- New block metadata batches contain no `ti-*` rows.
- Transaction-info-by-ID works from both hot `tib-*` and ancient `tx_infos`.
- Legacy direct `ti-*` reads remain a final fallback before migration.
- The prune range preserves adjacent `tib-*` and `tx-*` rows.
- Reorg cleanup and full test suite remain green.
