# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project

go-tron is a ground-up Go rewrite of [java-tron](https://github.com/tronprotocol/java-tron). The binary is `gtron`. The non-negotiable constraint is **full wire compatibility** with java-tron on mainnet/testnet: same protobuf messages, same P2P protocol (libp2p `io.github.tronprotocol/libp2p:2.2.7`), same consensus rules. When a go-tron implementation decision conflicts with java-tron behavior, java-tron is the source of truth.

Architecturally, it mirrors go-ethereum layering (`node/`, `core/`, `core/rawdb/`, `core/state/`, `p2p/`, etc.) but the domain model and actuator dispatch are TRON-specific.

## Commands

```bash
make gtron                # build → build/bin/gtron
make test                 # go test ./... -count=1 -timeout 300s
make lint                 # golangci-lint run ./...
make proto                # regenerate *.pb.go from proto/**/*.proto (needs protoc + protoc-gen-go)
make clean                # wipe build/ and go's build cache

go test ./core/... -run TestBlockChain_Insert -v       # single test
go test ./actuator -run TestTransfer -count=1          # single package
scripts/system_test.sh                                 # 2-node e2e: builds gtron+txsign, runs cross-node sync/tx/contract checks

# P2P integration tests against a real java-tron node are build-tagged:
JAVA_TRON_ADDR=127.0.0.1:18888 JAVA_TRON_NETWORK=0 \
  go test -tags=integration ./p2p/ -run TestJavaTron -v
```

See `docs/dev/java-tron-local.md` for how to stand up a local java-tron node for interop testing and `docs/dev/p2p-interop-status.md` for the running record of what wire-format details are validated (don't change the "cross-verified constants" listed there without re-running the interop tests).

## Architecture

### Request / block flow

1. `cmd/gtron/main.go` wires everything: opens Pebble, loads/sets up genesis, constructs `BlockChain`, `TxPool`, DPoS engine, `TronHandler`, `Server`, `BroadcastService`, `SyncService`, API/JSON-RPC servers, and (optionally) the block `producer`. All long-running components register as `node.Lifecycle`s on `node.Node`.
2. Peer traffic enters `p2p.Server` (TCP libp2p + UDP Kad discovery), is decoded by the varint frame codec, and dispatched by 1-byte type code. Codes `0xFB–0xFF` are libp2p control messages; `0x01–0x2F` are TRON application messages — see `docs/superpowers/specs/2026-04-12-tron-p2p-compatibility-design.md` for the layering.
3. App-layer messages reach `net/handler.go` (`TronHandler`), which feeds blocks/txs into `core.BlockChain` / `core/txpool` and drives `net/sync.go` for block sync.
4. `core.BlockChain.InsertBlock` runs `core.state_processor` over a `state.StateDB`. For each contract, `actuator.CreateActuator(tx)` returns the matching executor.
5. `producer` (witness mode) pulls from the txpool, builds a block via `core/block_builder.go`, signs with the DPoS engine, inserts it, and hands it to `BroadcastService`.

### Actuators (`actuator/`)

Each TRON contract type has its own `Actuator` implementing `Validate(ctx) / Execute(ctx)`. `actuator.go` is a single switch-based registry keyed on `corepb.Transaction_Contract_*`. Smart-contract txs route to `vm_actuator.go`, which drives the TVM. When adding a new contract type: port the Java `*Actuator.java`, register it in `CreateActuator`, and add golden-value tests against java-tron.

### TVM (`vm/`)

Forked from go-ethereum's EVM and then trimmed/renamed to TVM semantics: TRON-specific opcodes (`instructions_tron.go`), energy accounting (`energy.go`), and TRON precompiles at addresses `0x01000001+` (`precompile_tron.go`) alongside the stock Ethereum precompiles (`precompile_std.go`). Do not re-add Ethereum-only behavior that java-tron doesn't implement.

### State & storage

- `core/rawdb/schema.go` defines every DB key prefix — always add new accessors there, never hand-roll prefixes.
- `core/state/statedb.go` is the mutable state layer; `state/dynamic_properties.go` holds chain-global counters (latest solid block, witness schedule rollover, etc.) that java-tron keeps in a dedicated DB column.
- Storage backend is Pebble (`rawdb.NewPebbleDB`). `state.Database` wraps the kv store with trie-less access (TRON is not Merkle-trie-based like Ethereum).

### Consensus (`consensus/dpos/`)

27-SR DPoS with 6-hour maintenance cycles. `schedule.go` owns the slot → witness mapping; `maintenance.go` recomputes the active set at the cycle boundary and settles votes/rewards.

### Params & forks (`params/`, `core/forks/`)

`params/mainnet.go` and `params/nile.go` pin chain IDs, network IDs, and fork block numbers. `core/forks/forks.go` defines the fork gates that actuators and the VM check. Adding a fork: update the params file, add a gate predicate in `forks.go`, and gate behavior changes behind that predicate — never behind a raw block number comparison scattered in actuators.

SR software fork versions follow java-tron's quorum model: block producers write `params.BlockVersion` into `BlockHeader.raw.version`, and `core/forks/controller.go` tallies these per-version byte bitmaps (persisted via `core/rawdb/accessors_fork.go`) and returns `Pass(version)` once the HardForkTime + rate threshold is met. The complete AllowFlag → DP key → proposal ID map lives in `docs/dev/fork-gates.md`; snapshot audits under `docs/dev/fork-audit-<date>.md` are produced by `scripts/dev/fork_audit.sh`.

## Conventions

- Module path `github.com/tronprotocol/go-tron`, Go 1.25.
- Depend on `go-ethereum` only for primitives (`ethdb`, `rlp`, `crypto`) — not for chain logic.
- Proto files under `proto/` are imported verbatim from java-tron; regenerate with `make proto` after editing, never hand-edit `*.pb.go`.
- Integration tests that hit a running java-tron live behind `//go:build integration` so `make test` stays hermetic.
- Design docs for each phase of the rewrite live in `docs/superpowers/specs/` with matching implementation plans in `docs/superpowers/plans/` — read the spec before making cross-cutting changes.
