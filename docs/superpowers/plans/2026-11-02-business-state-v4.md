# Business-state V4 implementation

1. Work from the preserved research branch; keep the production source/binary and
   immutable 53,016,002 copy unchanged. Freeze a joint format/behavior design.
2. In parallel, implement owner-aware permission values, typed Votes reward state,
   and an independent hash-bound event catchup frontier. Keep file ownership split.
3. Root changes HEAD/element/delta domains and snapshot schema, adds V3 rejection
   and coherent source-fold/restore/continuation tests, and reconciles consumers.
4. Run changed package tests, targeted race/vet, executable build; fix actual
   failures, then run the relevant combined suites once. Review cross-layer edges.
5. Compare real fixed-input sizes and production-policy reads. Server testing uses
   isolated research binaries and disposable targets, never the running service.
   Keep all source changes and evidence reviewable before any release decision.
