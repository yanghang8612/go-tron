# DynamicProperties defaults audit — 2026-05-15

Triggered by the "mainnet-biased DP defaults" architectural smell noted while
fixing the Nile `proposal_expire_time` bug (2026-05-11). Goal: sweep gtron's
DP bootstrap values against java-tron ground truth and find every key where
the genesis-seeded value diverges.

## Sources

| source | role |
|---|---|
| `core/state/dynamic_properties.go::defaultProps` | gtron's 121-key bare default map |
| `params/mainnet.go::DefaultMainnetGenesis().DynamicProperties` | 14-key mainnet genesis override (applied over `defaultProps` in `core/genesis.go:87`) |
| `params/nile.go::DefaultNileGenesis().DynamicProperties` | 15-key Nile genesis override |
| `test/fixtures/00-genesis-dp-mainnet/fixture.json` | **java-tron ground truth** — 76 DP getters captured from a real java-tron node at block 0 |
| java-tron `chainbase/.../store/DynamicPropertiesStore.java` | constructor hardcoded inits + `CommonParameter`-driven inits |
| java-tron `config.conf` / `config-nile.conf` (`nile/master` + genesis-era `6e5fc849d3`) | committee/block config sections |

## Finding 1 — the original scan target is empirically empty

The premise was "config-nile.conf sets values that differ from java-tron's
hardcoded defaults, and those leak through gtron's mainnet-biased defaults."

Checked: mainnet `config.conf` and `config-nile.conf` `committee {}` sections
are **byte-identical** — both set only `allowCreationOfContracts = 0` and
`allowAdaptiveEnergy = 0`; every other committee key is commented out in both.
The genesis-era config-nile.conf (`6e5fc849d3`) is the same. The two configs
differ **only** in `block {}`:

| key | mainnet | Nile |
|---|---|---|
| `maintenanceTimeInterval` | 21600000 | 600000 |
| `proposalExpireTime` | 259200000 | 600000 |

Both are **already** overridden in `params/nile.go`. Every `CommonParameter`-driven
DP key in the `DynamicPropertiesStore` constructor falls through to the same
`CommonParameter` default on both chains because neither config sets them.

**Conclusion: there is no further config-driven Nile-vs-mainnet divergence
surface.** The memory's predicted "more keys leak on Nile" set is empty.

## Finding 2 — `defaultProps` itself is near-perfect

Diffed all 121 `defaultProps` keys against the real java-tron genesis fixture
(76 keys) and java-tron source inits:

- **0** keys where `defaultProps` disagrees with java-tron's hardcoded /
  config-default value.
- 1 extra key gtron carries that java-tron doesn't serialize at genesis:
  `next_proposal_id` (gtron `defaultProps` = 1; java-tron writes
  `LATEST_PROPOSAL_NUM` lazily, absent at genesis). Introduced by commit
  `42c597f`. **Secondary finding — needs separate triage**: this is a
  serialization-shape difference that can drift the conformance DigestC at
  genesis. Not part of the Nile-defaults scope; tracked here so it isn't lost.
- `state_flag` is a gtron-internal key mirroring java-tron's `stateFlag`;
  expected, not a divergence.

## Finding 3 — `params/mainnet.go` overrides 3 keys with WRONG genesis values

`params/mainnet.go` carries a 14-key `DynamicProperties` map. 11 keys exactly
restate the (correct) `defaultProps` value — redundant but harmless. **3 keys
override `defaultProps` with the *current-era* mainnet value instead of the
*genesis* value:**

| key | params/mainnet.go | java-tron genesis (fixture + source) | gtron `defaultProps` |
|---|---|---|---|
| `witness_pay_per_block` | **16000000** | **32000000** (`saveWitnessPayPerBlock(32000000L)`) | 32000000 ✓ |
| `max_cpu_time_of_one_tx` | **80** | **50** (`saveMaxCpuTimeOfOneTx(50L)`) | 50 ✓ |
| `unfreeze_delay_days` | **14** | **0** (`CommonParameter.unfreezeDelayDays = 0L`) | 0 ✓ |

Note `witness_127_pay_per_block` is a **separate** key and *is* 16000000 at
genesis — the override likely confused the two. These overrides were present
in the initial commit `8291ecf` with no rationale comment; they look copied
from a live `getchainparameters` snapshot. They are **masked** because no test
replays mainnet from genesis through `DefaultMainnetGenesis()` — the M0″ smoke
range loads a hand-built seed at block 1M, and the M1.1 fixture test checks
`defaultProps`, not the params-applied result.

## Finding 4 — `params/nile.go` has the same 3 wrong keys

`params/nile.go`'s 15-key map: 2 keys are legitimately Nile-specific
(`maintenance_time_interval` = 600000, `proposal_expire_time` = 600000 — see
Finding 1), 10 keys redundantly restate `defaultProps`, and the **same 3 keys**
(`witness_pay_per_block` = 16000000, `max_cpu_time_of_one_tx` = 80,
`unfreeze_delay_days` = 14) are wrong.

Nile genesis ran the same java-tron code, and the genesis-era `config-nile.conf`
does not override these 3 keys → Nile genesis values are identical to
mainnet's: 32000000 / 50 / 0. The Nile soak shows `blockID` MATCH despite this
because TRON block headers carry no state root — a wrong `witness_pay_per_block`
silently corrupts witness allowance balances without surfacing as DIVERGE.

**Open verification (network-blocked in this environment):** query
`nile.trongrid.io/wallet/getchainparameters` + Nile proposal history to learn
*when/if* a proposal later changed these on Nile-live. This does not change the
genesis-seed conclusion — it only tells us how many blocks gtron stays diverged
before a proposal replay (if any) converges it.

## Recommended fix

Both params files should seed only genuinely chain-specific genesis state and
fall through to `defaultProps` for everything else:

- `params/mainnet.go`: the entire `DynamicProperties` map can be dropped —
  mainnet genesis == `defaultProps`. (Or, minimally: fix the 3 wrong keys.)
- `params/nile.go`: shrink the map to the 2 chain-specific keys
  (`maintenance_time_interval`, `proposal_expire_time`); drop the 13
  redundant/wrong entries.

This is consensus-affecting genesis state. After the change: re-run the M1.1
fixture test, and a from-genesis Nile soak restart is the real validation
(the 3 keys affect balances/timing, not block hashes, so watch state-level
parity, not the blockID monitor).
