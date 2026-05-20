# JSON-RPC reflection framework — plan

**Spec:** [2026-05-19-jsonrpc-reflection-design.md](../specs/2026-05-19-jsonrpc-reflection-design.md)

## Slice 1 — Vendor geth's rpc/ package

- [ ] Copy `github.com/ethereum/go-ethereum/rpc/` to
      `internal/rpc/` (entire directory tree)
- [ ] Rewrite import paths inside vendored files:
      `go-ethereum/rpc → go-tron/internal/rpc`
- [ ] Drop go-ethereum-specific tests that reference unrelated geth
      types; keep the framework-level tests (`server_test.go`,
      `service_test.go`, `subscription_test.go`)
- [ ] `make test ./internal/rpc/...` green
- [ ] Doc `docs/dev/rpc-vendor.md` recording: version pinned,
      modifications, upgrade procedure

## Slice 2 — Wire freeze tests

- [ ] `fixtures/jsonrpc-corpus/` — 50 captured request/response pairs
      covering every major method (eth_getBalance, getBlockByNumber,
      getLogs, subscribe newHeads, send raw tx, etc.)
- [ ] `scripts/dev/jsonrpc-diff.sh` — replays the corpus against two
      binaries and diffs
- [ ] CI gate: every PR runs the corpus against `master` binary +
      branch binary, diff must be empty

## Slice 3 — Migrate `web3` namespace (smallest)

- [ ] `internal/rpc/api/web3.go` — `Web3API` struct + 2 methods
- [ ] Register at server build (`internal/jsonrpc/server.go`)
- [ ] Drop the hand-rolled `web3_*` handlers from
      `internal/jsonrpc/dispatch.go`
- [ ] Freeze tests pass

## Slice 4 — Migrate `net_*` and `eth_*` (largest)

In sub-slices that can ship independently:

- [ ] 4a — `net_version`, `net_listening`, `net_peerCount`
- [ ] 4b — Account queries (getBalance, getCode, getStorageAt,
      getTransactionCount)
- [ ] 4c — Block queries (getBlockBy*, blockNumber, getHeaderBy*)
- [ ] 4d — Tx queries (getTransactionBy*, getTransactionReceipt)
- [ ] 4e — Call / estimateGas / sendRawTransaction
- [ ] 4f — Filters / Logs (getLogs, newFilter, getFilterChanges,
      uninstallFilter)
- [ ] 4g — Subscriptions (subscribe / unsubscribe)

After every sub-slice: freeze tests pass; new test in
`internal/rpc/api/eth_test.go`; old hand-rolled handler deleted.

## Slice 5 — Migrate TRON HTTP servlet API

- [ ] `internal/rpc/http_adapter.go` — `rpcHTTPAdapter(server, method)
      http.HandlerFunc` translates HTTP form/JSON POST → JSON-RPC call
- [ ] `internal/rpc/api/tron.go` — port every java-tron `/wallet/*`
      endpoint as a typed Go method
- [ ] Migrate `internal/tronapi/` route registration to use the
      adapter
- [ ] Freeze tests on TRON HTTP corpus (separate from JSON-RPC corpus)

## Slice 6 — Cleanup

- [ ] Delete `internal/jsonrpc/dispatch.go` (the big switch)
- [ ] Delete hand-rolled subscription manager
- [ ] Update `docs/dev/rpc.md` with the new pattern (one paragraph
      "to add a method, do X")
- [ ] Final freeze tests: zero diffs against pre-migration baseline

## Acceptance criteria

- [ ] Zero-diff freeze tests pass on full corpus
- [ ] `internal/jsonrpc/dispatch.go` deleted
- [ ] Adding a new RPC method demonstrably ≤ 20 LOC (one function +
      one test)
- [ ] 1000-concurrent-newHeads subscription test passes; no leaks
- [ ] `make test` green; `go test -race ./internal/rpc/...` clean
