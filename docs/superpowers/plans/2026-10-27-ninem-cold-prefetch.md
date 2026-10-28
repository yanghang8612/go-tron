# 9M cold-prefetch implementation and validation

1. Record executable identity, runtime options, unprofiled/reference HTTP values,
   an idle profile and in-request profiles/stacks. Separate deployed changes from
   the existing unpublished small-system and maintenance-policy candidate.
2. Capture one complete manifest's immutable cold files with bounded retries for
   retirement races. Preserve exact canonical source identities; do not copy live
   Pebble WAL/SST metadata as an alleged consistent state snapshot.
3. Add descriptor-local framed header reuse with cancellation/Close/fresh-open
   corruption tests and concurrent-reader coverage.
4. Add selected broad-route views with complete physical and structural checks,
   bounded retention, conservative unselected-key handling and cache isolation.
5. Seek authenticated small namespaces during sidecar construction, while strict
   import independently scans complete dictionaries. Compare selective extraction
   against the full walk across source versions and generations.
6. Use native, isolated, resource-limited processes for same-catalog query/CPU/I/O
   comparisons, strict full-coverage verification and actual history restoration.
   Bound private cold-cache copying by verified size and available disk space;
   hash copied files, prove inode isolation, and verify per-file mincore residency.
7. Preserve raw outputs, exact source/binary hashes, baseline identity and failures.
   Independently verify returned rows and measurements locally, record acceptance
   limits, and leave production untouched.
