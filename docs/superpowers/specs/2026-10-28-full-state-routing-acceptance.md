# Full-state acceptance of bounded cold-history routing

The user authorized one short production stop to capture a consistent complete
input and immediately resume the original executable at its existing height.
That stop/start is complete. No further production restart, release, parameter
change, reset or database deletion is authorized by this acceptance workflow.

Use the captured chaindata, split history, cold state catalog and ancient tables
as one immutable execution cut. Record the executable SHA256, original argv,
canonical block/hash, state HEAD, allocator TxNum, state root, input inode/copy
inventory and recovery receipt. Copies of mutable metadata and lock files must
have independent inodes; only immutable data files may share inodes.

Construct the baseline from the deployed source revision and the candidate from
the previously tested product-file SHA256 map. The HTTP host must be identical
test-only glue around the real chain backend and JSON-RPC handler, with import,
publication and pruning absent. Both versions use the same cache and CPU/memory
limits. Bind each process to the complete independently verified source cut.

Independently rescan all canonical history dictionaries to verify both auxiliary
routes. Compare all canonical manifest references before and after route building.
Neither missing coverage nor a Bloom positive may answer a state lookup.

Recompute all three physical state-source families, including obsolete generation
rows, into the complete normalized MuHash state and digest. Latest-state export
must include every source family plus the complete cold/hot bytecode union, which
MuHash alone does not authenticate. Bind export identity to actual genesis and
cut blocks and the canonical internal state root. Use a fresh signed catalog and
a new persistent restore target, then close/reopen and independently recompute
source row counts, logical bytes, row SHA256, normalized MuHash and execution
checkpoint. Verify all account code references. Latest-only restore does not
claim historical import or a runnable CLI bootstrap.

Compare complete loopback HTTP eth_call at known transition heights, 5M, 9M,
35M and the captured tail/latest. Preserve fixed known answers and require
version parity; storage0 cross-checks share the history reader and are not an
independent proof that previously omitted metadata history exists.

Separate warm repeats from private-file zero-residency/new-process first HTTP
requests. Before a cold process starts, prove every discovered data-file inode
is private, immutable and has zero resident pages. Never drop global caches.
Monitor the actual timed process's opened file set with directory IN_OPEN watches
installed before launch. Reject overflow, watch invalidation, topology changes
or any opened data path missing from that case's eviction proof. Lock exemptions
are exact, precreated, empty private files. Join the host before draining the
monitor. A separate strace reproduction is an additional audit, not a substitute
for actual timed-process coverage. Startup may warm data before HTTP dispatch;
do not call the result “all data cold at request dispatch.”

Keep profiling outside admitted timing samples. Report sample counts and ranges,
resource limits, indirect shared-host load, correctness scope and remaining
bottlenecks. A successful latest-state MuHash does not repair the historical
SetContract before-image omission; repairing old missing history requires replay.
