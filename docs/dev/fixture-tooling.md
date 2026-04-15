# Fixture extraction tooling

Pulls golden state snapshots from a local java-tron for use as test
oracles by M1 unit tests. Design: `docs/superpowers/specs/2026-04-15-fixture-extraction-design.md`.

## Prerequisites

1. **java-tron** — follow [java-tron-local.md](./java-tron-local.md) to build
   `FullNode.jar` for your platform. On Apple Silicon, the jar must be
   rebuilt with ARM64 Java 17 or RocksDB JNI will fail to load:
   ```bash
   cd /path/to/java-tron
   export JAVA_HOME=$(/usr/libexec/java_home -v 17)
   ./gradlew build -x test -x lint -x checkstyleMain -x checkstyleTest
   ```
2. **jq** (`brew install jq` / `apt-get install jq`).
3. **Java 17 ARM64** available (Apple Silicon). Pass as the `JAVA` env var
   to the scripts:
   ```bash
   export JAVA=/path/to/corretto-17/Contents/Home/bin/java
   ```
4. **FullNode.jar** at the default path
   `/Users/asuka/Projects/tron/java-tron/build/libs/FullNode.jar`, or set
   `FULLNODE_JAR=/custom/path/FullNode.jar`.

## Usage

```bash
# List available scenarios
./scripts/fixtures/run.sh list

# Run a single scenario; output lands at test/fixtures/<name>/fixture.json
./scripts/fixtures/run.sh 00-genesis-dp-mainnet

# Run every scenario
./scripts/fixtures/run.sh all
```

Or via Make:

```bash
make fixtures-list
make fixtures        # runs all scenarios
```

Each run uses a unique `/tmp/fixture-tron-<pid>-<scenario>/` workdir which
is wiped after. Nothing persists outside of `test/fixtures/`.

## Adding a scenario

1. `cp -r scripts/fixtures/scenarios/00-genesis-dp-mainnet scripts/fixtures/scenarios/<new-name>`
2. Edit `<new-name>/config.conf`:
   - Pick unused ports (HTTP `fullNodePort`, TCP `listen.port`).
   - Keep `needSyncCheck = false` if the chain should stay at block 0.
   - Add `localwitness` only if the scenario must produce blocks.
3. Edit `<new-name>/setup.sh` and `run.sh` to build/broadcast/wait for
   any needed transactions. Use `lib/api.sh` helpers.
4. Edit `<new-name>/dump.sh` to call `dump_fixture` with the sections the
   scenario cares about. DP is always cheap to include; accounts/receipts
   require broadcasting specific addresses/txids to pass in.
5. Edit `<new-name>/README.md` — what it tests, why.
6. `./scripts/fixtures/run.sh <new-name>` — the resulting `fixture.json`
   is what you commit alongside the scenario files.

## Fixture format

See spec §3.3 for the full schema. Fields of interest:

- `schema` — integer, currently `1`. Loader rejects anything else.
- `javaTron.jarSha256` — sha256 of the FullNode.jar that produced this
  fixture. A changed hash on re-extraction means the reference moved; the
  diff should be reviewed manually.
- `javaTron.configSha256` — sha256 of the scenario's `config.conf`.
  Changes to the config invalidate the fixture and force a re-run.
- `dynamicProperties` — map from java-tron DP key to int64 value.
  Normalised from `/wallet/getchainparameters`: missing `value` fields
  (booleans default 0) are filled in as `0`.

All numeric fields are strict int64; any value not representable as int64
is rejected at load time (`internal/testutil/fixture/fixture.go`).

## Consuming fixtures in Go tests

```go
import "github.com/tronprotocol/go-tron/internal/testutil/fixture"

func TestDefaults(t *testing.T) {
    fix := fixture.Load(t, "00-genesis-dp-mainnet")
    for key, want := range fix.DynamicProperties {
        got := myComponent.Get(key)
        if got != want {
            t.Errorf("dp[%s]: got %d, want %d", key, got, want)
        }
    }
}
```

Tests iterate over whatever keys the fixture contains — do **not** hard-code
a key list. That way, a new java-tron release adding a DP key shows up as
a naturally-surfaced test failure rather than silent drift.

## Regenerating fixtures

Regenerate when:
- The java-tron jar is rebuilt (arch change, version bump, or local patch).
- A scenario's `config.conf` is modified.
- Schema v1 is extended in a backward-compatible way.

Do **not** regenerate just because a test fails — if fixture and go-tron
disagree, the default assumption is that go-tron is wrong. Treat the
fixture as the specification.

## CI policy

Fixture extraction does **not** run in CI — the java-tron jar is too
heavy and environment-sensitive. CI only runs the Go consumers of the
committed fixtures (`go test ./...`).
