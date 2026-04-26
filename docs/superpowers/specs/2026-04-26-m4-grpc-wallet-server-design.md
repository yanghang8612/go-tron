# M4 gRPC Wallet Server — Design

**Date**: 2026-04-26  
**Milestone**: M4  
**Status**: Active

## Goal

Expose TRON's gRPC `Wallet` service (`proto/api/api.proto`) from go-tron so that standard TRON SDKs (tronweb, tronpy) and `grpc_cli` can call it directly. The server is a thin protobuf adapter over the existing `tronapi.Backend` — no new domain logic.

## Proto compilation

`api/api.proto` imports `google/api/annotations.proto` (HTTP transcoding annotations from grpc-gateway). These are not needed at runtime but must be present for protoc to parse the file.

**Decision**: vendor `google/api/annotations.proto` and `google/api/http.proto` under `proto/google/api/`. These files are Apache-2.0 from the Google APIs repository and are standard well-known protos. This avoids GOPATH coupling and makes `make proto` hermetic.

**Makefile addition** (added alongside existing `--go_out`):
```makefile
--proto_path=. \
--proto_path=.. \          # for proto/google/api resolution from cd proto
--go-grpc_out=. \
--go-grpc_opt=paths=source_relative \
```

Output: `proto/api/api.pb.go` + `proto/api/api_grpc.pb.go`.

## Package layout

```
internal/grpcapi/
  server.go        # WalletServer implementation + lifecycle wiring
  server_test.go   # bufconn-based tests
```

`internal/tronapi/` (HTTP) is unchanged. Both packages import `tronapi.Backend`.

## WalletServer design

`grpcapi.Server` holds a `tronapi.Backend` and embeds `apipb.UnimplementedWalletServer` so all ~200 methods return `codes.Unimplemented` by default. Implemented methods override only what they need.

```go
type Server struct {
    apipb.UnimplementedWalletServer
    backend tronapi.Backend
    grpc    *grpc.Server
}
```

Lifecycle: `Start(addr string) error` / `Stop()` — matches `node.Lifecycle` pattern.

## Wire type mapping

gRPC handlers receive and return proto types directly (no Go intermediary structs). Key mappings:

| RPC | Input | Backend call | Return |
|-----|-------|--------------|--------|
| `GetNowBlock` | `*apipb.EmptyMessage` | `CurrentBlock()` | `*corepb.Block` via `block.Proto()` |
| `GetBlockByNum` | `*apipb.NumberMessage` | `GetBlockByNumber(n)` | `*corepb.Block` |
| `GetAccount` | `*corepb.Account` (Address field) | `GetAccount(addr)` | `*corepb.Account` |
| `GetTransactionById` | `*apipb.BytesMessage` | `GetTransactionByID(hash)` | `*corepb.Transaction` |
| `GetChainParameters` | `*apipb.EmptyMessage` | `GetChainParameters()` | `*apipb.ChainParameters` |

Error mapping: `err != nil` → `codes.Internal`; not found → `codes.NotFound`; bad input → `codes.InvalidArgument`.

## PR sequencing

**PR-A0 (this PR)**: Foundation
- Vendor `google/api` protos
- Makefile: add grpc generation
- Generated `api.pb.go` + `api_grpc.pb.go` committed
- `internal/grpcapi/server.go`: scaffold + 5 read RPCs above
- `cmd/gtron/main.go`: `--grpc.port` flag + lifecycle registration
- `internal/grpcapi/server_test.go`: bufconn tests for the 5 RPCs
- All existing tests still green

**PR-A1**: Remaining read-only block/account RPCs (~20 methods)  
**PR-A2**: Read-only tx/contract/resource RPCs (~30 methods)  
**PR-B**: Transaction building RPCs (~50 methods)  
**PR-C**: Broadcast + EstimateEnergy + signing helpers  
**PR-D**: WalletSolidity (read from solid state) — deferred to M8  
**PR-E**: Monitor/Node service

## java-tron reference

java-tron's `WalletImplBase` lives in `framework/src/main/java/org/tron/core/services/rpc/`. Each handler method maps 1:1 to a `*Servlet` class in the same package. Field semantics verified by cross-referencing with the HTTP handler in `internal/tronapi/api.go`.

## Testing

- `server_test.go` uses `google.golang.org/grpc/test/bufconn` for in-process transport — no real port needed.
- Each PR adds tests for the RPCs it implements.
- Integration smoke: `grpc_cli call localhost:50051 wallet.Wallet/GetNowBlock` documented in `docs/dev/grpc-smoke.md`.
