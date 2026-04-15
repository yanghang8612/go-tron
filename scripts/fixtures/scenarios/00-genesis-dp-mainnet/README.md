# Scenario: 00-genesis-dp-mainnet

Observe java-tron's initial `DynamicPropertiesStore` state when booted with
the real TRON mainnet genesis block and no chain activity.

## What it does
- Starts a single-node java-tron with `net.type = mainnet` and the mainnet
  genesis block (27 GRs, 3 asset accounts).
- Does NOT configure a local witness → the chain stays at block 0.
- Dumps `/wallet/getchainparameters` → `test/fixtures/00-genesis-dp-mainnet/fixture.json`.

## Why
This fixture is the golden reference for M1.1 (DynamicProperties backfill).
Any discrepancy between `core/state/dynamic_properties.go` defaults and
java-tron's mainnet defaults shows up directly in a diff of this fixture
against go-tron's post-init DP map.

## Port use
- P2P TCP 19888
- HTTP 18090

Other fixtures pick different ports so sequential runs never collide.

## How to regenerate
```
./scripts/fixtures/run.sh 00-genesis-dp-mainnet
```

See `docs/dev/fixture-tooling.md` for prerequisites and troubleshooting.
