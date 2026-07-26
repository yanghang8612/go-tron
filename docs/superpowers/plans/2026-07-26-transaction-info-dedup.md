# TransactionInfo hot/cold deduplication — implementation plan

**Spec:** [2026-07-26-transaction-info-dedup-design.md](../specs/2026-07-26-transaction-info-dedup-design.md)

## Tasks

- [x] Change block metadata writes to emit only `tx-*` and `tib-*`.
- [x] Resolve single TransactionInfo reads through the transaction locator and
      hot/ancient block receipt.
- [x] Wire-scan repeated TransactionInfo payloads and unmarshal only the match.
- [x] Preserve a legacy `ti-*` fallback during staged rollout.
- [x] Add an offline, confirmation-gated range prune command.
- [x] Make immediate physical compaction explicit rather than automatic.
- [x] Cover hot, ancient, legacy, malformed-wire, adjacent-prefix, CLI, and
      metadata-batch behavior in tests.
