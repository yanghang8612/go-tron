# MuHash latest-state snapshots

The local latest-state commitment scheme is `state-flat-latest-muhash3072-compact-v2`.
It does not replace Java/TRON's account-state trie root or any wire/consensus field.
This release starts from genesis in a fresh database. Older state databases and
snapshot manifests are rejected; there is no migration or compatibility mode.
The current compact representation is documented in [compact-state-format-v2.md](compact-state-format-v2.md).

## Snapshot cuts

Manifest version 3 identifies the element codec and commitment scheme. Only the
current HEAD and actual snapshot cuts retain the complete normalized 384-byte
MuHash state; historical per-block checkpoints retain identity and digest only.
Every physical account-latest, generation and KV-latest row is included, including
old-generation KV rows. `SourceRows` records the three physical family counts.

A latest build acquires one owned pinned view of the committed current cut N/T,
including immutable block-buffer layers. It does not force un-solid layers to
disk. Source families, complete HEAD, root and execution checkpoint use that same
view. The view lease stays alive through materialization, then closes. Publication
waits for solid height >= N and verifies N's canonical hash without chasing a new
head or rebuilding its source. The 40,000-block idle cadence remains; deep sync
continues history freezing without full latest-state snapshots.

`LatestStateTxNum` is the actual latest cut T. It is independent of historical
`VisibleTxEnd`: later history-only publication and merging preserve T and its
source/HEAD/execution datasets. Restore must never infer current state from the
maximum history transaction number.

The required execution checkpoint uses schema 1 and binds HEAD block hash and
transaction number. It includes the exact current block transaction allocator
range (required for a nonzero history cut), independent of history coverage,
and records the four derived execution properties (number,
timestamp, hash, solid height) and an explicit present/absent current-cycle pending
reward accumulator using its existing raw format. These are execution metadata,
not additional MuHash elements. Missing metadata is an error, including a missing
record where pending rewards happen to be empty.

## Explicit offline restore

Use `snapshot restore` only with an exclusive, fresh state target. The three
source prefixes, HEAD, latest root and current chain anchor must initially be
empty. A residual restore marker is rejected and is never automatically resumed.
Existing chain/freezer data is accepted only after the explicit restore checks.

The signed catalog and payload checksums authenticate the selected manifest.
Before the first payload batch, restore writes and syncs the restore-in-progress
marker. Startup and initialization refuse a marked database. Bulk source/history
batches can then populate the offline database, but valid HEAD/root/current
anchors, derived properties and cycle pending metadata remain unpublished.
Restore recomputes all three physical source families and checks normalized
MuHash state, digest and row counts against the complete snapshot HEAD.

Before publication, the CLI restores the selected manifest's chain freezer and
indexes, verifies genesis wire identity, verifies the cut block/hash/internal
root against HEAD and its lightweight checkpoint, and checks the execution
number/timestamp/hash against that block. It rebuilds TAPOS in ascending order
from the final 65,536 blocks at or before the cut. History beyond the latest cut
never enters that ring. A snapshot without sufficient chain bodies is rejected;
a state-only payload cannot become a runnable node through this CLI.

Nonempty Java account-state trie roots are explicitly unsupported for snapshot
restore because these datasets do not transport Java trie nodes. A zero root or
Ethereum empty-trie root is accepted. This restriction does not apply to normal
fresh-genesis synchronization and does not change consensus.

One final batch publishes complete HEAD/root/current identity, derived execution
properties, cycle pending metadata, genesis metadata and canonical progress, and
deletes the marker; a final sync establishes durability. Payload import itself is
not one atomic batch. Any prepublication failure leaves an unstartable marked
target. If final batch write or sync has an uncertain outcome, stop and reopen to
inspect marker/HEAD/current identity before deciding whether publication reached
disk. Do not retry bulk import into that target or infer failure from an error.

## Cache and deployment boundary

`--state.latest.cache` independently retains the generic flat latest/SLOAD read
cache. Its default is 128 MiB, the old 512 MiB base budget's non-branch quarter.
Zero disables it. Branch base/tail/trunk/partition/prefetch settings are removed;
obsolete `state.commitment.cache*` and `history.commitment-base-mode` flags fail.

The first mainnet release crosses an incompatible local state scheme. Before
pushing a deployable master, pause the automatic deploy timer and acquire its
existing start lock. Confirm current PID/executable, actual mainnet DB paths,
symlinks/mounts, unit argv and external HISTORY_/DIRECT_ guard bindings. Stop only
mainnet and remove only the explicitly verified mainnet database/obsolete guard
metadata after approval. Preserve Nile, Java, parameters, keys and configuration.
Remove retired flags from the installed unit and reload systemd before release.

The release helper's explicit `--fresh-state-no-rollback` mode requires an
already stopped service (`inactive` or `failed`, MainPID 0). If startup or health
fails, it stops the service and retains the new configuration for repair; it
never starts a pre-MuHash binary against the new database. Ordinary subsequent
same-scheme releases retain the existing rollback behavior. This flag does not
automatically delete any database and is not forwarded by the periodic start
script. Actual deployment paths/guard cleanup require fresh server evidence.
