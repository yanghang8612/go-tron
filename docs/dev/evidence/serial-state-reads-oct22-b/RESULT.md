# B v1: NO-GO (sealed)

Production SHA256: `1ebc4ba9480a7abf90ab13fdf7f51041c5830faec13af24d489785abf37133f9`.
Actual parent 9030ce7 and candidate both pass the byte-identical independent
materialized-map oracle, focused race tests, and complete blockbuffer tests.
Astra read-only audit independently matched all four frozen hashes and both
exports (only predecessor.go differs); no new definite semantic defect found.

The fixed five samples per arm failed the original performance gate. Cold
8/128 MiB improves 88.44%/84.12%, full sorts 16/32 -> 0, but hot-stable
regresses 24.70%/132.05%. Churn improves 98.06%/99.78% but candidate spread
0.463/0.580 exceeds 0.30. The 8 MiB small-prefix and 128 MiB generic controls
also regress 11.97%/11.03%. See benchmark-summary.json for all raw samples.

No samples were added or removed. This version is excluded from release.
All logs, manifests, commands, binaries and frozen exports are retained. Root
authorized a separate v2 design and independent new round; it does not replace
or relax this failed round.
