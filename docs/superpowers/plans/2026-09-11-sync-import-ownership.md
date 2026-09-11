# Sync import ownership implementation plan

1. Reproduce the loss of exact-hash ownership between decoded-buffer removal and canonical settlement on the current master baseline.
2. Preserve drain-owned hashes through the commit barrier; cover inventory, scheduling, receipt, reset/recovery and failure lifetimes without retaining block payloads.
3. Independently assess a small commitment branch-codec candidate with fixed-input benchmarks and byte/root compatibility tests; keep only a supported improvement.
4. Review both diffs, run affected tests/race, full repository tests and two-node system checks on the frozen source.
5. Push reviewed changes directly to master, prepare an isolated pinned native release with rollback, activate under existing guards, and sample live progress and cache growth.
6. Record evidence, remaining bottlenecks and measurement limits; keep archival debt within the user's accepted same-order-of-magnitude criterion.
