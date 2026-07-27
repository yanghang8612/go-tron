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
tx-<txID>       -> marker | block number | transaction ordinal (8 bytes)
tib-<blockNum>  -> TransactionRet containing every TransactionInfo in the block
```

The freezer moves `tib-*` to ancient `tx_infos` as before. New commits do not
write `ti-<txID>`.

No consensus or protobuf format changes. The schema prefix remains readable for
upgrade compatibility but is deprecated.

When `tib-*` is frozen, the freezer removes nested `TransactionInfo.id` fields
only when the block transaction count exactly matches the info count. The ID is
the SHA-256 hash of the corresponding transaction's `raw_data`, already present
as the `tx-*` key and derivable from `bodies`; storing it again in `tx_infos`
costs about 34 incompressible protobuf bytes per transaction. All other known
and unknown fields retain their original wire bytes.

## Read algorithm

`ReadTransactionInfo(txID)`:

1. Read `tx-<txID>` to resolve the block number and ordinal. Legacy 8-byte
   block-only values remain readable.
2. Read ancient `tx_infos[blockNum]`, falling back to hot `tib-<blockNum>`.
3. For legacy rows, scan nested field 1 and return the ID match. For compact
   rows, select field 3 at the stored ordinal and restore `id` from the lookup
   key before returning it.
4. For a legacy block-only locator pointing to a compact row, derive the
   ordinal from the matching transaction in `bodies` once.
5. If the locator or block-level row is unavailable, fall back to legacy
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

Already-published V2 segments are compacted separately:

```text
gtron db compact-ancient-tx-info-v2 --datadir <path> --yes
```

The command upgrades each old `tx-*` locator in place without changing its
8-byte size, syncs that prerequisite, writes and verifies a replacement
`tx_infos` segment, atomically changes the manifest, then deletes the old
segment. A packed locator is also valid against the old receipt format, so a
crash before manifest publication is safe. Published manifests make the
operation resumable. Older binaries cannot read ID-less segments.

## Acceptance criteria

- New block metadata batches contain no `ti-*` rows.
- Transaction-info-by-ID works from both hot `tib-*` and ancient `tx_infos`.
- ID-less ancient rows reconstruct byte-equivalent Wallet API messages for
  both by-ID and by-block reads.
- New and upgraded reverse indexes remain 8 bytes and select compact rows by
  ordinal without hashing or unmarshalling the block.
- Legacy direct `ti-*` reads remain a final fallback before migration.
- The prune range preserves adjacent `tib-*` and `tx-*` rows.
- Reorg cleanup and full test suite remain green.
