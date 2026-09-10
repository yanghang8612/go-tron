# Busy history resource observation

The deployed resource admission path can build bounded state history while the
importer is busy below the busy watermark. A resource refusal currently waits
for an ordinary maintenance invocation to refresh the load budget. The prior
online capture included a roughly 375-second publication pause; resource
recovery/hysteresis was visible during that pause. This motivates reducing the
observation delay, without treating every pause as a resource-only failure.

## Contract

- Arm a lightweight observation only after an otherwise eligible busy-history
  opportunity is refused by its existing storage/CPU/runtime/memory budget.
- Recheck at most once every five seconds. Refresh bounded observations without
  catalog preflight, history reads, manifest work, pruning, freezing, density
  updates, lease acquisition, or full maintenance accounting.
- Preserve the current complete-work recovery and failure deadlines. Observe
  their latest values, including later outer completion, before requesting one
  coalesced ordinary pass. That pass revalidates all admission conditions.
- Serialize budget mutation with normal passes, invalidate stale observations
  before lifecycle preflight, and prevent old completion callbacks from
  canceling newer work. Cancel on context/stop, full-pass failure, or departure
  from deep sync. Support both the integrated lifecycle and standalone runner.
- Preserve byte formats, canonical checks, pruning windows, cache budgets,
  transaction/block caps, and all storage/free-space/shared-gate protections.

## Verification

Use controlled time to cover recovery and renewed pressure on the same sample
trace, repeated/stale samples, absolute deadline extension, concurrent
cancellation, pending outer completion, timer coalescing, shutdown, and no
additional heavy work on unready observations. Faster observation may also
detect pressure sooner; it does not guarantee higher throughput.

Run focused/race tests, the hermetic Go suite, and uncapped incremental lint.
Build with the server's native Go 1.25.5 toolchain and existing Sapling static
library, including complete snapshots/pruning tests and a Sapling probe.

## Release and measurement

Develop and push directly on master as requested. Use a pinned isolated native
build and the existing guarded activation/rollback flow; keep the server's
working checkout and unrelated Java/Nile services intact. Compare bounded
before/after captures with exact process identity, preserve failed requests,
and report full-window and later-window frontier/debt trends separately.

Expose lightweight observation checks, coalesced wakeups, and last observation
duration. These distinguish resource observation from a complete pass; they do
not alone prove successful admission or publication. Complete canonical and
historical read checks outside the stable sampling window.
