# Vendored go-ethereum `rpc` package (`internal/rpc/`)

This package is a near-verbatim copy of go-ethereum's reflection-based JSON-RPC
server framework. It is **Slice 1** of the "jsonrpc-reflection" effort: go-tron
will eventually replace its hand-rolled JSON-RPC dispatch switch with this
framework. This slice **only vendors the framework** — nothing in the rest of
the repo is wired to it yet and no behavior elsewhere changes.

## Pinned version

- **Source repo:** `github.com/ethereum/go-ethereum`
- **Version:** `v1.17.2` (the version go-tron already requires in `go.mod`)
- **Source path (local checkout used for the copy):**
  `/Users/asuka/Projects/ethereum/go-ethereum/rpc/`
- **Package name:** kept as `package rpc`; import path is now
  `github.com/tronprotocol/go-tron/internal/rpc`.

## Vendored files

Source `.go` files (copied unchanged except the telemetry patch in `handler.go`):

```
client.go            doc.go        http.go        ipc_wasip1.go  server.go      stdio.go
client_opt.go        endpoints.go  inproc.go      ipc_windows.go service.go     subscription.go
context_headers.go   errors.go     ipc.go         json.go        types.go
handler.go           ipc_js.go     ipc_unix.go    metrics.go     websocket.go
```

Framework tests + shared helper + test data:

```
server_test.go  subscription_test.go  types_test.go  testservice_test.go  testdata/
```

## Files dropped (NOT copied)

| File | Reason |
| --- | --- |
| `client_example_test.go` | Example/integration-flavored; references the old `go-ethereum/rpc` import path. |
| `client_test.go` | Client end-to-end test; pulls geth-specific machinery and unrelated deps. |
| `client_opt_test.go` | References the old `go-ethereum/rpc` import path; client-option integration test. |
| `http_test.go` | HTTP server/client integration test; geth-integration-flavored. |
| `websocket_test.go` | WebSocket integration test; geth-integration-flavored. |
| `tracing_test.go` | OpenTelemetry tracing test; exercises `setTracerProvider` + propagation, which we no-op'd out (see telemetry patch). |

`service_test.go` was listed in the vendoring instructions as a kept test but
**does not exist** in the v1.17.2 source tree, so there was nothing to copy.

None of the *kept* tests had to be dropped: `server_test.go`,
`subscription_test.go`, `types_test.go`, and `testservice_test.go` are all
`package rpc` internal tests with no references to `internal/telemetry`,
`tracerProvider`, or the dropped helper files.

## Modifications applied

### 1. `internal/telemetry` patch (the required one)

Go forbids importing another module's `internal/` package, so
`github.com/ethereum/go-ethereum/internal/telemetry` cannot compile from
go-tron. It was used **only in `handler.go`** for OpenTelemetry request
tracing/labels (spans + attributes) — pure instrumentation with no effect on
JSON-RPC semantics. It was patched out as follows.

**Import block — removed the one forbidden line:**

```go
// BEFORE
	"github.com/ethereum/go-ethereum/internal/telemetry"
	"github.com/ethereum/go-ethereum/log"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"

// AFTER
	"github.com/ethereum/go-ethereum/log"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"
```

(`go.opentelemetry.io/otel` and `.../otel/trace` are third-party modules that
resolve fine — they are still used by the retained `tracer()` method and the
`tracerProvider` struct field, so those imports stay.)

**`handleCallMsg` span bookkeeping — collapsed to the underlying plain calls:**

```go
// BEFORE
	// Start root span for the request.
	rpcInfo := telemetry.RPCInfo{
		System:    "jsonrpc",
		Service:   service,
		Method:    method,
		RequestID: string(msg.ID),
	}
	attrib := []telemetry.Attribute{
		telemetry.BoolAttribute("rpc.batch", cp.isBatch),
	}
	ctx, spanEnd := telemetry.StartServerSpan(cp.ctx, h.tracer(), rpcInfo, attrib...)
	defer spanEnd(nil) // don't propagate errors to parent spans

	// Start tracing span before parsing arguments.
	_, _, pSpanEnd := telemetry.StartSpanWithTracer(ctx, h.tracer(), "rpc.parsePositionalArguments")
	args, pErr := parsePositionalArguments(msg.Params, callb.argTypes)
	pSpanEnd(&pErr)
	if pErr != nil {
		return msg.errorResponse(&invalidParamsError{pErr.Error()})
	}
	start := time.Now()

	// Start tracing span before running the method.
	rctx, _, rSpanEnd := telemetry.StartSpanWithTracer(ctx, h.tracer(), "rpc.runMethod")
	answer := h.runMethod(rctx, msg, callb, args)
	var rErr error
	if answer.Error != nil {
		rErr = errors.New(answer.Error.Message)
	}
	rSpanEnd(&rErr)

// AFTER
	// NOTE(go-tron): OpenTelemetry request tracing removed during vendoring.
	// ... (comment) ...
	args, pErr := parsePositionalArguments(msg.Params, callb.argTypes)
	if pErr != nil {
		return msg.errorResponse(&invalidParamsError{pErr.Error()})
	}
	start := time.Now()

	answer := h.runMethod(cp.ctx, msg, callb, args)
```

**`runMethod` signature/body — dropped the variadic attributes and the encode span:**

```go
// BEFORE
func (h *handler) runMethod(ctx context.Context, msg *jsonrpcMessage, callb *callback, args []reflect.Value, attributes ...telemetry.Attribute) *jsonrpcMessage {
	result, err := callb.call(ctx, msg.Method, args)
	if err != nil {
		return msg.errorResponse(err)
	}
	_, _, spanEnd := telemetry.StartSpanWithTracer(ctx, h.tracer(), "rpc.encodeJSONResponse", attributes...)
	response := msg.response(result)
	if response.Error != nil {
		err = errors.New(response.Error.Message)
	}
	spanEnd(&err)
	return response
}

// AFTER
func (h *handler) runMethod(ctx context.Context, msg *jsonrpcMessage, callb *callback, args []reflect.Value) *jsonrpcMessage {
	result, err := callb.call(ctx, msg.Method, args)
	if err != nil {
		return msg.errorResponse(err)
	}
	return msg.response(result)
}
```

All three `runMethod` call sites (`handleCallMsg`, `handleSubscribe`,
unsubscribe) passed zero attributes, so dropping the variadic is safe.

### 2. Unused `service` / `method` locals (consequence of patch #1)

`h.reg.callback(msg.Method)` returns `(callb, service, method)`. The `service`
and `method` names were consumed *only* by the removed `telemetry.RPCInfo`
struct, so they became unused locals. Discarded with `_`:

```go
// BEFORE
	callb, service, method := h.reg.callback(msg.Method)
// AFTER
	callb, _, _ := h.reg.callback(msg.Method)
```

(`methodNotFoundError{method: msg.Method}` uses the struct field, not the
discarded local, so the not-found path is unchanged.)

### 3. `go.mod` / `go.sum` dependency promotion (no code change)

`internal/rpc` is the first first-party package to *directly* import a handful
of modules that go-ethereum previously pulled in transitively. They were present
in `go.sum` (or pinned by go-ethereum's `go.mod` via MVS) but absent from
go-tron's `go.mod` require block, so the build failed with "updates to go.mod
needed". They were added with **targeted `go get` at versions already resolved
by MVS** (NOT `go mod tidy`, which would scan the whole repo and risk churning
unrelated WIP deps):

```
go get go.opentelemetry.io/otel@v1.40.0 go.opentelemetry.io/otel/trace@v1.40.0
go get github.com/deckarep/golang-set/v2@v2.6.0
```

Resulting `go.mod` additions (all marked `// indirect` by `go get`; the
annotation is cosmetic and does not affect the build — only `go mod tidy` would
reclassify `otel`/`otel/trace` to direct, and we intentionally skipped tidy):

```
github.com/deckarep/golang-set/v2 v2.6.0   // websocket.go
github.com/go-logr/logr v1.4.3             // transitive (otel)
github.com/go-logr/stdr v1.2.2             // transitive (otel)
go.opentelemetry.io/auto/sdk v1.2.1        // transitive (otel)
go.opentelemetry.io/otel v1.40.0           // handler.go, http.go
go.opentelemetry.io/otel/metric v1.40.0    // transitive (otel)
go.opentelemetry.io/otel/trace v1.40.0     // handler.go, server.go
```

No other code modifications were required. Imports of go-ethereum `log`,
`metrics`, `common`, `common/hexutil`, and `p2p/netutil` resolve as-is and were
left untouched.

## Build / test (scoped to this package only)

Run **only** these — the rest of the repo may carry unrelated uncommitted WIP,
so never `go build ./...` / `go test ./...`:

```bash
go build ./internal/rpc/...
go vet ./internal/rpc/...
go test ./internal/rpc/... -count=1
go test -race ./internal/rpc/... -count=1
```

All four are green: build PASS, vet clean, 13 tests (plus subtests) PASS, race PASS.

## Upgrade procedure

To re-sync against a newer go-ethereum tag:

1. Bump `github.com/ethereum/go-ethereum` in `go.mod` to the target version
   (`go get github.com/ethereum/go-ethereum@vX.Y.Z`).
2. Re-copy the `.go` files and `testdata/` listed under **Vendored files**
   above from `<geth>/rpc/` into `internal/rpc/`, keeping `package rpc`. Do NOT
   copy the files under **Files dropped**. (Check whether upstream added/removed
   any source or test files and adjust the lists here accordingly.)
3. Re-apply the **`internal/telemetry` patch** (modification #1) and the
   resulting **unused-locals fix** (#2) to `handler.go`. The upstream tracing
   code may have shifted; re-locate every `telemetry.*` reference
   (`grep -n telemetry internal/rpc/*.go`), strip the forbidden import, and
   replace each call with its no-op equivalent. If upstream moved tracing into
   additional files, patch those too and update this doc.
4. If any *kept* file/test still references
   `github.com/ethereum/go-ethereum/rpc`, rewrite it to
   `github.com/tronprotocol/go-tron/internal/rpc`.
5. Promote any newly-required transitive deps with targeted `go get @<version>`
   (modification #3) — pin to the versions MVS already resolves; avoid
   `go mod tidy` so unrelated WIP deps aren't churned.
6. Re-run the four scoped commands above until all are green.
