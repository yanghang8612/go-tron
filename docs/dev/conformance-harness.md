# Conformance replay harness

> **Status:** PR-2 of M0″ — skeleton with the smoke range only.
> Real mainnet ranges, capture tooling, and cross-e2e are added in PR-3 / PR-4 / PR-5.

## Overview

The harness replays pre-captured java-tron mainnet blocks through go-tron's
`core.ProcessBlock` and flags state divergences at field granularity. Design:
[`../superpowers/specs/2026-04-17-m0-double-prime-conformance-replay-design.md`](../superpowers/specs/2026-04-17-m0-double-prime-conformance-replay-design.md).

## Layout

```
test/fixtures/mainnet-blocks/
    smoke/                          # synthetic 5-block range (PR-2)
    range-freeze-v2/                # real mainnet range (PR-4)
    range-maintenance/              # real mainnet range (PR-4)
    range-contract/                 # real mainnet range (PR-4)
```

Each range directory carries:

| File                         | What                                                          |
| ---                          | ---                                                           |
| `fixture.json`               | schema + java-tron jar sha + range metadata                   |
| `seed.json`                  | state at StartBlock-1 for the range's touched-address closure |
| `blocks.bin`                 | varint-prefixed Block protos                                  |
| `oracle.ndjson`              | per-block DigestB (+ optional DigestC diagnostic)             |
| `divergence-allowlist.json`  | known parity gaps whitelisted during bring-up                 |

## Running replay

```bash
# Default: only the smoke range.
make conformance-replay

# All real mainnet ranges (requires `git lfs pull` first).
RANGES="range-freeze-v2 range-maintenance range-contract" make conformance-replay

# Enforce allowlist-empty + stale-free (the M0" exit gate).
make conformance-replay-exit-gate
```

Exit codes:

- `0` — every range passed; allowlist clean if `--exit-gate`.
- `1` — replay passed but allowlist is non-empty / has stale entries (gate only).
- `2` — hard divergence (DigestB mismatch not covered by allowlist).
- `3` — harness error (missing fixture files, malformed seed/oracle).

## Regenerating the smoke range

Re-run only after intentional `ProcessBlock` semantics changes or fixture format bumps.

```bash
go run ./scripts/fixtures/cmd/gen-smoke
```

Output goes to `test/fixtures/mainnet-blocks/smoke/`; commit the resulting files.

## What's not here yet

- Capture from a live java-tron → PR-3 (`scripts/fixtures/capture_range.sh`).
- Three real mainnet ranges + allowlist catalog → PR-4.
- 1-gtron + 1-java-tron end-to-end → PR-5 (`scripts/system_test_cross.sh`).
- Full operator guide (troubleshooting, interpreting DigestC diffs, allowlist policy) → PR-5.
