# JSON-RPC reflection framework — design

**Status:** Proposed
**Author:** yanghang8612
**Date:** 2026-05-19
**Inspiration:** [go-ethereum/rpc/](../../../../ethereum/go-ethereum/rpc/) (`server.go`, `service.go`, `subscription.go`)
**Related plan:** [2026-05-19-jsonrpc-reflection.md](../plans/2026-05-19-jsonrpc-reflection.md)

## Background

gtron has three RPC surfaces today:

- [`internal/jsonrpc/`](../../../internal/jsonrpc) — Ethereum-compatible
  JSON-RPC (eth_*, web3_*, net_*)
- [`internal/grpcapi/`](../../../internal/grpcapi) — gRPC mirror of
  java-tron's wallet API
- [`internal/tronapi/`](../../../internal/tronapi) — HTTP/JSON mirror of
  java-tron's HttpServlet API

Each surface is **hand-rolled**: every method is a dispatch-case in a big
switch, with manual argument unmarshalling, manual error mapping, manual
return serialization. Adding a new RPC is ~50-80 LOC across at least
three files (handler, registration, test).

go-ethereum solved this 10 years ago via the
[`rpc/`](../../../../ethereum/go-ethereum/rpc/) package:

```go
// Define an API as plain Go methods on a struct:
type EthAPI struct { backend Backend }

func (a *EthAPI) GetBalance(ctx context.Context, addr common.Address, block BlockNumber) (*big.Int, error) {
    // body
}

// Register it:
server.RegisterName("eth", &EthAPI{backend: be})
```

The server uses reflection at registration time to discover all exported
methods, build a typed dispatch table, and at runtime parses the
`"method":"eth_getBalance"` from JSON-RPC, unmarshals positional or named
parameters into the typed Go args, calls the method, marshals the
`(result, error)` return as JSON-RPC response. Subscriptions
(`eth_subscribe`) are handled by returning a `*rpc.Subscription` which
the server wires to the client's WebSocket session.

The same framework supports **HTTP**, **WebSocket**, and **IPC** with
identical method signatures.

This spec proposes porting that framework — or vendoring it directly,
since it's apache-2.0 / LGPL compatible — to gtron.

## Goals

- One reflection-based RPC framework backing all three surfaces
- Adding a method becomes: define a Go function with typed args/return,
  done
- Subscription support (used today by `eth_subscribe` events but
  hand-rolled per-surface)
- HTTP / WS / IPC unified codec layer
- Existing wire-level API behaviour preserved bit-for-bit

## Non-goals

- Do NOT change the **wire** format. `eth_getBalance` still takes
  `[address, blockNum]` positional args, returns the same hex string.
- Do NOT migrate the gRPC surface to JSON-RPC. gRPC is a separate
  protocol; its surface stays separate but can also use the framework
  internally (gRPC services map naturally to typed Go methods).
- Do NOT introduce HTTP-2 push or other transport innovation. The point
  is consolidation, not new wire features.

## Proposed approach

Two paths considered:

### Option A — Vendor geth's `rpc/` package (recommended)

geth's `rpc/` is ~3500 LOC of mature, well-tested code. It supports:

- HTTP server with batched requests
- WebSocket server with subscriptions
- Unix domain socket (IPC)
- Stdin/stdout JSON-RPC (for tooling)
- Reflection-based method discovery via `rpc.RegisterName("namespace", &apiStruct{})`
- Typed parameter unmarshalling with custom unmarshalers (e.g. `BlockNumber`
  parses `"latest"`, `"earliest"`, `"pending"`, hex, decimal)
- Subscription lifecycle, deduplication, backpressure
- Standardized error codes (JSON-RPC 2.0 spec + Ethereum's
  `application/-32000..-32099` range)

We vendor the package under `internal/rpc/` (gtron-renamed to avoid
import-path clash with go-ethereum which we may still depend on for
other primitives).

Drawbacks:

- TRON-specific request shapes (e.g. java-tron's `getNowBlock` returns
  the whole block proto, not a typed struct) need adapters. But these
  adapters are 5-line `func (...) (*BlockProto, error)` wrappers.
- Adds ~3500 LOC dependency surface. Mitigated by license compatibility
  + the framework's maturity.

### Option B — Build a minimal in-house reflection layer

Pros: smaller, gtron-specific.
Cons: re-inventing 10 years of geth's edge-case fixes (batch handling,
subscription cleanup on disconnect, error code mapping, BlockNumber
parsing, etc.). Multi-month investment for ~80% feature parity.

**Pick Option A.** The migration shape is the same regardless.

## Migration

### Phase 1: vendor + adapter

- Vendor `geth/rpc/` to `internal/rpc/`
- Update import paths in the vendored copy (`github.com/ethereum/go-ethereum/rpc`
  → `github.com/tronprotocol/go-tron/internal/rpc`)
- Verify `make test ./internal/rpc/...` green on vendored tests

### Phase 2: define one TRON namespace via the new framework

Pick `web3` (smallest surface): `web3_sha3`, `web3_clientVersion`.

```go
// internal/rpc/api/web3.go
type Web3API struct{}

func (a *Web3API) ClientVersion(ctx context.Context) (string, error) {
    return "gtron/" + version, nil
}

func (a *Web3API) Sha3(ctx context.Context, data hexutil.Bytes) (string, error) {
    h := crypto.Keccak256(data)
    return hexutil.Encode(h), nil
}
```

Register at server build time:

```go
server := rpc.NewServer()
server.RegisterName("web3", &Web3API{})
```

Wire test: `curl -d '{"method":"web3_clientVersion","jsonrpc":"2.0","id":1}'`
returns identical output to today.

### Phase 3: migrate `eth_*` namespace

Largest surface. Migrate in groups:

- **Account queries**: getBalance, getCode, getStorageAt, getTransactionCount
- **Block queries**: getBlockByNumber, getBlockByHash, blockNumber
- **Tx queries**: getTransactionByHash, getTransactionReceipt
- **Call/execute**: call, estimateGas, sendRawTransaction
- **Logs/filters**: getLogs, newFilter, getFilterChanges
- **Subscriptions**: subscribe / unsubscribe (newHeads, logs)

Each method has a today's-hand-rolled-code equivalent. Migration is:

1. Move the body into a typed method on a struct
2. Drop the hand-rolled marshal/unmarshal scaffolding
3. Add a test that drives the new dispatcher

Per-namespace migration is mergeable independently. After all `eth_*` is
migrated, delete the hand-rolled dispatcher in
`internal/jsonrpc/dispatch.go`.

### Phase 4: migrate `tronapi` (HTTP/JSON)

`internal/tronapi/` is the HTTP-servlet-shaped surface mimicking
java-tron's `wallet/*` HTTP API. It's not strictly JSON-RPC 2.0 (no
`method` field; URL paths are the method names). The reflection framework
needs a custom HTTP route adapter:

```go
http.Handle("/wallet/getnowblock", rpcHTTPAdapter(server, "tron_getNowBlock"))
http.Handle("/wallet/getaccount",   rpcHTTPAdapter(server, "tron_getAccount"))
// ... 50+ endpoints
```

`rpcHTTPAdapter` translates an HTTP form/JSON POST into a JSON-RPC call
internally; the underlying Go method has typed args either way.

### Phase 5: subscription cleanup

Today's `eth_subscribe` is hand-rolled in `internal/jsonrpc/sub.go`. The
new framework's `rpc.Subscription` does this with one method signature:

```go
func (a *EthAPI) NewHeads(ctx context.Context) (*rpc.Subscription, error) {
    sub := a.notifier.CreateSubscription()
    go func() {
        for ev := range a.headEvents {
            a.notifier.Notify(sub.ID, ev)
        }
    }()
    return sub, nil
}
```

WebSocket framing, client disconnect cleanup, ID generation — all
handled by the framework.

## Wire compatibility test

A "freeze" test captures pre-migration request/response pairs for ~50
representative API calls (one per major endpoint). Post-migration, the
same requests must produce byte-identical responses.

Tool: `scripts/dev/jsonrpc-diff.sh`:

```bash
# Replays a captured request set against two binaries, diffs responses.
./scripts/dev/jsonrpc-diff.sh old-binary new-binary fixtures/jsonrpc-corpus/
```

Acceptance: zero diffs for the corpus.

## Performance

geth's reflection framework dispatches with one reflective Call per
request. Benchmarks show <1µs per dispatch over the wire — negligible
against any actual chain DB read. Not a concern.

## Subscription scale

Today's surface supports maybe 50 concurrent subscriptions before custom
code starts straining. geth's framework regularly handles 10K+
subscriptions per node. Win.

## Acceptance criteria

- All three surfaces (eth_*, web3_*, net_*, tron's HTTP) backed by the
  unified framework
- Wire-format freeze tests pass byte-identical
- `internal/jsonrpc/dispatch.go` (the big switch) deleted
- Adding a new RPC method is ≤ 20 LOC (function definition + test)
- Subscription test: 1000 concurrent `newHeads` subscribers on a single
  node, no leaks, no missed notifications
- p99 dispatch overhead < 10 µs (well below any DB read)

## Risks

- Vendored code drift: geth's `rpc/` evolves; we pin a version + decide
  upgrade cadence in `docs/dev/rpc-vendor.md`. Probably once a year.
- Subtle response-format drift (number vs string, missing field) caught
  by the freeze tests. Spec lists this as the primary risk.

## Out of scope / future

- **GraphQL endpoint** — geth has `graphql/` package. Defer; not on user-
  asked feature list.
- **gRPC bridge** — gRPC handlers calling the same Go methods as JSON-RPC
  (via a thin adapter). Easy once Phase 2 lands.
- **RPC request tracing / audit log** — wire each request through a hook
  for ops observability. Doable post-vendor.
