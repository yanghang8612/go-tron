# Mainnet auto-deploy restored (2026-09-22)

The live auto-deploy units are `gtron-deploy.timer` (one-minute poll) and its
oneshot `gtron-deploy.service`, which runs `/data/gtron/start.sh` as `java-tron`.
The timer was enabled but inactive: both deploy units had a drop-in condition
against `/var/lib/gtron-offline-maintenance.hold`. That global hold remains in
place for the separate Nile protections. We removed only the condition line
from the two **deploy** drop-ins, reloaded systemd, and started the timer.

The former script could build a new `build/bin/gtron` while `gtron.service`
continued to execute its pinned, root-owned release. The installed script now
calls `/usr/local/libexec/gtron-mainnet-release.py` to verify the effective
mainnet executable and its guard marker. For a future new source revision, it
publishes a root-owned release, updates the guarded systemd path and marker,
checks the process executable and Wallet API, and rolls back on failure. The
mainnet service flags, reader/disk guards, and Nile service configuration were
not changed. Both installed files are root:root mode 0755; their SHA-256 values
are `409c8504b4d3e3c7e2006b78db00d1dddc7c2739eb8fc6628a2c43c01effd49c`
(`start.sh`) and `4bace6594b5c6164c956b71a7a1b5637e637c4ee71eb0ba38c741a7a6372a55c`
(`gtron-mainnet-release.py`). Local helper tests (11 cases), `bash -n`, and
Python compilation passed; the staged files passed syntax checks with the
server's Python 3.6.8 before installation.
The live run verified the already deployed revision only. Switching to a new
binary and rollback were covered by local tests; no restart was triggered to
exercise those paths on the server.

The first timer execution at 06:48:47 UTC failed fetching Git because 83 loose
objects under `.git/objects` were root:root mode 0400 and unreadable to the
`java-tron` unit. Root `git fsck --full` passed; there was no object-content
corruption. We saved the owner/mode inventory, changed only those 83 objects'
owner to java-tron (retaining mode 0400), then passed
`git fsck --full --no-dangling` and the script's exact fetch command as
java-tron. No Git clone, worktree cleanup, submodule cleanup, database
operation, or gtron binary deployment was performed.

| Live check | Result |
| --- | --- |
| 06:54:00-06:54:01 UTC timer-triggered service | Fetched `origin/master` at `250ac539b675f018dfb3b393d7031a503a36468e`; helper returned `verified: true` for the already running pinned release; journal says `gtron.service already runs verified ...`; systemd `Result=success`, `ExecMainStatus=0`. |
| Deployment state | `/data/gtron/deployed-master.rev` automatically aligned to `250ac539b675f018dfb3b393d7031a503a36468e`. |
| Timer after run | `active (waiting)`; a later check showed the next poll at 06:58:00 UTC after a 06:57:00 UTC poll, with service `Result=success` and `ExecMainStatus=0`. |
| Nodes and guards | Mainnet active, PID 17902 unchanged, Wallet API height 17,148,054; Nile inactive. `gtron-mainnet-space-guard.timer` active (waiting); global maintenance hold still present. |

The running binary is still the previously pinned release at
`/data/gtron/releases/20260921-cache-recycle-cold-fallback/gtron` (SHA-256
`eab4d65689451949f210618c7953dd4efd91c4c3358a203e3008c123023dacca`).
Local optimization commit `94e29781` remains unpushed and was not deployed. Backups of the
old script, state, deploy drop-ins, mainnet guard drop-ins and marker, plus the
Git owner/mode and fsck records, are on the server under
`/data/gtron/auto-deploy-restore-20260922/`.
