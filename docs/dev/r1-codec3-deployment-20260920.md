# R1 codec3 production deployment (2026-09-20)

- Service: `gtron.service` on `ip-172-12-16-193.us-east-2.compute.internal`
- Activated: `2026-09-20 04:17:53 UTC`
- Source commit: `1e6f5c2565cedb581d1c3401da07c8d37520117e`
- Native binary: `/data/gtron/releases/20260920-r1-codec3/gtron`
- SHA-256: `b1e263e71d16097e9a7bc19f9fa2378f603697c16c7b2a21acbb314e52593321`
- Initial PID: `8480`; `/proc/8480/exe` resolved to the native binary and had the same SHA-256.
- Existing data directory, ports, memory limits, sync flags, and history parameters were preserved. The two reader guards, final `ExecStart`, global shared-history marker, and release R1 marker were rebound to the new binary/source identity. Container magic remains `GTHREF01`; marker versions remain unchanged.
- No database reset, migration, or A/B benchmark was performed.

Post-start HTTP checks recorded head `15,025,010` at `04:18:42 UTC` and `15,029,750` at `04:19:21 UTC`, an increase of 4,740 blocks in 39 seconds. The second metrics snapshot reported cold published block `13,048,460`, cold lag `1,912,993`, cold errors `1`, and `sync/history_backlog/lag=0`.

The single cold error was logged at `12:18:17.291 +0800` as `snapshots: history reference container budget exceeded` for blocks `13,048,461..13,049,492`. The lifecycle logged recovery at `12:19:00.392 +0800` after 43 seconds. Recent service logs contained no panic, fatal error, corruption, or codec-read failure. A post-recovery cold publication was not yet observed in this short validation window.

The final instantaneous check at `04:22:00 UTC` reported head `15,047,359`, cold published block `13,048,460`, cold lag `1,912,993`, cold errors `1`, and `state/lifecycle/failure_active=0`. The head continued advancing, but the cold publication watermark had not advanced by the end of validation, so codec3 cold-output publication was not yet proven online.

Raw HTTP evidence is under `build/benchmarks/20260920-r1-codec3-deploy/` (ignored build output).
