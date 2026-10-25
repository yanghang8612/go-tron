# Automatic record columns policy implementation and acceptance

1. Isolate from the frozen ongoing V5 fixed-copy experiment. Record the base
   commit and retain the default `auto` behavior and all numeric overrides.
2. Share the automatic ordinary-format resolution between full V6 build and
   merge; preserve the existing bounded CDC observer and source-header scan.
3. Admit explicit `auto-columns` in runtime flags and standalone diagnostics.
   Keep unresolved stream and migration compatibility semantics conservative.
4. Verify the large opaque-Prev regression, ordinary small records, effective
   repaired/order-fallback rows, borrowed/pipeline builds and mixed 2/3/5 actual
   compaction. Compare with independent canonical/digest oracles, including
   strict companion authentication and actual restore behavior.
5. Run relevant complete packages, race, vet and build; save exact source and
   logs. Keep this separate from the native experiment's frozen identity.
6. Before adoption, complete that experiment's full space/SHA/MuHash/restore/
   HTTP/publication gates, then validate this policy natively and measure its
   representative writer behavior. No rollout in this phase.
