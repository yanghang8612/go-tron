# gtron observability implementation plan

## Phase 0 — security containment

- [ ] Remove or deny the public Nginx `/debug/` and `/debug-nile/` routes.
- [ ] Confirm pprof remains reachable only through direct loopback access or an
  operator-controlled SSH tunnel.
- [ ] Verify all public pprof and debug-metrics probes return 404 or an
  authenticated gateway response.

This phase requires deployment-host access. Repository changes cannot by
themselves close an already deployed Nginx route.

## Phase 1 — monitoring loop

- [x] Add opt-in `--metrics`, `--metrics.addr` and `--metrics.port` flags.
- [x] Add a dedicated Prometheus-only lifecycle server.
- [x] Export existing RPC, DB and Go runtime registry metrics.
- [x] Add chain height/solidification, peers, txpool and sync gauges.
- [x] Add Prometheus scrape jobs and initial alerts.
- [x] Add Grafana provisioning and an overview dashboard.
- [ ] Deploy mainnet on loopback port 6071 and Nile on 6072.
- [ ] Exercise alerts against a controlled Nile restart/stall.

## Phase 2 — logging lifecycle

- [x] Add configurable terminal/JSON/logfmt file rotation (100 MiB, 3 backups,
  28 days, compressed by default).
- [x] Force active log-file permissions to `0600`.
- [x] Add an independent file verbosity while retaining module overrides.
- [x] Close the rotating writer during CLI teardown.
- [x] Log redacted startup arguments.
- [ ] Validate rotation and retention during a Nile soak run.

## Phase 3 — deep diagnostics

- [ ] Add SIGUSR1-triggered heap and goroutine artifact capture.
- [ ] Add opt-in near-OOM heap capture with a configurable threshold, cooldown
  and disk budget.
- [ ] Add pprof labels to sync import, block execution and Pebble maintenance
  goroutines.
- [ ] Evaluate Pyroscope-compatible continuous profiling behind an opt-in
  flag.
- [ ] Add an incident runbook covering CPU, heap, mutex, block and runtime trace
  collection.

## Verification gates

- [x] Unit tests for exporter isolation, metric snapshots, flag validation,
  file permissions, independent log levels and argument redaction.
- [x] `go test ./... -count=1 -timeout 300s`.
- [x] `go vet ./...`, gofmt, `git diff --check`, dashboard JSON and YAML parse.
- [ ] `golangci-lint run ./...` (tool is not installed in the current local
  environment).
- [ ] Deployment smoke test and public exposure probe.
