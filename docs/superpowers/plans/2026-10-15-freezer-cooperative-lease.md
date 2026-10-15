# Direct V2 cooperative lease implementation

1. Serialize public Runner entrypoints with nonblocking workMu before gate entry.
2. Expose opt-in direct-only raw checkpoint hooks; split preparation windows,
   drain compression, and pause only with no background source/transform work.
3. Release tail/write locks for handoff while retaining migration ownership;
   validate mutation epoch/layout on resume, preserve atomic segment publication.
4. Rotate shared leases with fresh history contention and observed handoff or a
   bounded ordinary retry opportunity; preserve recovery, promotion and pressure.
5. Emit bounded owner/hold/rejection and job/phase/yield/wait observations.
6. Verify correctness, real lock interactions, fault/cancel/retry and race;
   benchmark fixed input full migrations. Preserve unrelated WIP and live settings.
7. Freeze exact paths/hashes for independent Astra audit. Commit, push and normal
   guarded automatic deployment only after root authorization; observe a natural
   full job and handoff, then archive resources and remove this round's tools.
