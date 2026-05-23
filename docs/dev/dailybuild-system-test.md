# java-tron dailyBuild interop harness

This note records the local dailyBuild setup used to verify that gtron can join
a java-tron private chain, receive blocks over P2P, and apply the same state
transitions while the java-tron `system-test` dailyBuild suite broadcasts
transactions.

The source of truth for the runnable workflow is:

- `scripts/system_test_stress.sh`
- `test/fixtures/system-test/java-tron.conf`
- `test/fixtures/system-test/genesis.json`

## Topology

The harness runs three local components:

- `system-test`: Gradle/TestNG client that broadcasts transactions through
  java-tron gRPC/HTTP.
- `java-tron`: single witness process. It holds all 27 active witness private
  keys, produces every block, and is the only transaction ingress.
- `gtron`: full node, no witness key. It connects to java-tron through P2P and
  validates every block produced by java-tron.

Ports are intentionally off mainnet defaults:

- java-tron P2P `28888`, HTTP `28090`, gRPC `50081`, solidity HTTP `28091`,
  solidity gRPC `50091`, JSON-RPC `50575`.
- gtron P2P `29999`, HTTP `28190`, gRPC `50171`, JSON-RPC `28546`.

The watchdog polls `/wallet/getnowblock` on java-tron and gtron every 10s. It
fails the run if gtron stays more than `DRIFT_LIMIT` blocks behind for
`DRIFT_STREAK_LIMIT` consecutive polls.

## Prerequisites

Build java-tron first:

```bash
cd /Users/asuka/Projects/tron/java-tron
./gradlew clean build -x test
```

The default jar path expected by the harness is:

```text
/Users/asuka/Projects/tron/java-tron/build/libs/FullNode.jar
```

The default system-test checkout is:

```text
/Users/asuka/Projects/tron/system-test
```

The harness expects:

- A JDK matching the bundled `rocksdbjni` architecture in `FullNode.jar`. The
  arm64 java-tron jar runs with JDK 17:
  `/Users/asuka/Library/Java/JavaVirtualMachines/corretto-17.0.18/Contents/Home`
  The local x86_64 jar needs an x86_64 JDK, for example:
  `/Users/asuka/Library/Java/JavaVirtualMachines/corretto-1.8.0_442/Contents/Home`
- JDK 8 for Gradle 6.3 in system-test:
  `/Users/asuka/Library/Java/JavaVirtualMachines/azul-1.8.0_482/Contents/Home`
- TRON solc fork:
  `/Users/asuka/.local/opt/tron-solc/tv_0.8.26/solc`

Override any path with environment variables:

```bash
JAVA_TRON_JAR=/path/to/FullNode.jar \
SYSTEM_TEST_DIR=/path/to/system-test \
JT_JAVA_HOME=/path/to/jdk17 \
GRADLE_JAVA_HOME=/path/to/jdk8 \
TRON_SOLC=/path/to/tron-solc \
scripts/system_test_stress.sh --stage=C
```

## Running

From the go-tron repo root:

```bash
make gtron
scripts/system_test_stress.sh --stage=C
```

Useful modes:

```bash
scripts/system_test_stress.sh --stage=A
scripts/system_test_stress.sh --stage=B
scripts/system_test_stress.sh --stage=C
scripts/system_test_stress.sh --stage=C --keep-alive
scripts/system_test_stress.sh --stage=C --drift-limit=20
```

Stages:

- `A`: genesis BlockID parity plus a 60s sync watch.
- `B`: stage A plus transfer package smoke tests.
- `C`: stage A plus Gradle `:testcase:dailyBuild`.

`--keep-alive` leaves java-tron and gtron running after the script exits and
writes pid files under `/tmp/system-test-stress`. Stop them manually when done:

```bash
for p in $(cat /tmp/system-test-stress/java.pid /tmp/system-test-stress/gtron.pid); do
  kill "$p" 2>/dev/null || true
done
```

## What the harness stages

The script copies the system-test checkout to `/tmp/system-test-stress/system-test`
and only patches that sandbox. The user's checkout is not modified.

The staged system-test config is rewritten to point at the private java-tron
node. The script also normalizes local dailyBuild assumptions that otherwise
belong to TRON's CI environment:

- `maxFeeLimit = 15000000000`
- active permission operations bitmap:
  `7fff1fc0037e0100000000000000000000000000000000000000000000000000`
- no-op Slack hook
- TRON solc `--experimental-via-ir` for the local tv solc binary
- dailyBuild XML excludes for tests that require unavailable infra
- serial dailyBuild block neutralized

The dailyBuild governance parameters are seeded at genesis in both fixtures:

- `getEnergyFee = 420`
- `getMaxCreateAccountTxSize = 1500`
- `getMaxFeeLimit = 15000000000`

The script still keeps the proposal bootstrap as a fallback: if the java-tron
jar does not support `genesis.block.dynamicProperties`, or the runtime values
do not match, it creates and approves one proposal before dailyBuild starts. The
fixture uses `maintenance_time_interval = 300000` and
`proposal_expire_time = 300000`, so that fallback executes in minutes instead
of the mainnet 6-hour maintenance cycle.

## Fixture notes

`java-tron.conf` and `genesis.json` must stay in lockstep. A genesis mismatch is
a hard failure because P2P sync cannot proceed if block 0 differs.

Important fixture properties:

- `p2p_version = 333`
- `chain_id = 9999`
- `block_num_for_energy_limit = 0`, so energy-limit behavior is active from
  genesis for this private chain.
- `energy_fee = 420`, `max_create_account_tx_size = 1500`, and
  `max_fee_limit = 15000000000` are genesis-seeded dailyBuild governance
  parameters. They must match `genesis.block.dynamicProperties` in
  `java-tron.conf`.
- 27 `mainWitness.keyNN` accounts are active witnesses and have corresponding
  private keys in java-tron's `localwitness` list.
- 5 `witness.keyN` accounts are registered as lower-vote witnesses and also
  included in `localwitness`; dailyBuild vote/proposal tests can target them
  without stalling block production if one enters the active set.
- `foundationAccount.key1`, `foundationAccount.key2`, all 27 main witnesses,
  and the 5 witness accounts are funded.
- Full current TVM/resource/market feature gates are enabled at genesis.

## Known sandbox patches

These are local-harness patches, not gtron consensus behavior:

- Disable packages needing unavailable external infrastructure, including
  grpcurl, zentoken, longexecutiontime, manual, freezeV2, selected operation
  update tests, and shielded TRC20 proof-command tests.
- Make `Opcode.test03Coinbase` assert against the configured witness set
  instead of hard-coded CI witness addresses.
- Align a small set of stale expected energy constants in current system-test.
- Disable stale or local-harness-unstable methods that fail against java-tron
  itself, including duplicate FreezeBalanceV2 broadcasts in
  `ExtCodeHashTest005.test03GetInvalidAddressCodeHash` and
  `ExtCodeHashTest005.test04GetNormalAddressCodeHash`.
- Randomize the temporary FreezeBalanceV2 amount used by
  `PublicMethed.delegateResourceForReceiver` so parallel dailyBuild cases do
  not produce identical freeze txids before delegating resources.
- Top up `batchValidateSignContract011`'s execution account before its
  high-cost negative-signature cases, avoiding a local account-depletion
  cascade that otherwise fails inside java-tron system-test helpers.

If a future run reports a TestNG failure while gtron remains fully synced and
`gtron.log` has no `failed to insert`, inspect whether the failure reproduces
against java-tron alone before treating it as a go-tron divergence.

## Success criteria

A useful passing run has all of the following:

- `gradle-C.log` ends with `BUILD SUCCESSFUL`.
- Test XML has `failures=0` and `errors=0`.
- `watchdog.log` shows gtron at the same height as java-tron through the run.
- `gtron.log` has no `failed to insert`, `panic`, `fatal`, or `ERROR`.

The last known green run was on 2026-05-23:

```text
tests=876 failures=0 errors=0 skipped=39
BUILD SUCCESSFUL in 25m 25s
watchdog tail: java=497 gtron=497 diff=0
receipt parity: compared_blocks=499 compared_tx_infos=2053 mismatched_tx_infos=0 head_lag_blocks=0
```

Useful inspection commands:

```bash
grep -n "tests completed\|BUILD SUCCESSFUL\|BUILD FAILED" /tmp/system-test-stress/gradle-C.log
grep -n "> .* FAILED\|java.lang.AssertionError\|java.lang.NullPointerException" /tmp/system-test-stress/gradle-C.log
grep -n "failed to insert\|panic\|fatal\|ERROR" /tmp/system-test-stress/gtron.log
tail -n 80 /tmp/system-test-stress/watchdog.log
python3 - <<'PY'
from pathlib import Path
import xml.etree.ElementTree as ET
root = Path('/tmp/system-test-stress/system-test/testcase/build/test-results/dailyBuild')
tests = failures = errors = skipped = 0
for p in root.glob('TEST-*.xml'):
    t = ET.parse(p).getroot()
    tests += int(t.attrib.get('tests', 0))
    failures += int(t.attrib.get('failures', 0))
    errors += int(t.attrib.get('errors', 0))
    skipped += int(t.attrib.get('skipped', 0))
print(f'tests={tests} failures={failures} errors={errors} skipped={skipped}')
PY
```
