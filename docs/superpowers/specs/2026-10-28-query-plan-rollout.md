# Query-plan mainnet deployment

The user explicitly authorized master publication and production deployment.
Merge current master, preserving its retained V11 reader/adoption support. Keep
existing data, formats, rollback releases, flags, memory budgets and ports. Do
not invoke adoption, database reset or migration commands.

Validate merged local packages, focused race/vet and Sapling build. Validate the
same merged source with native Sapling focused checks, main binary build and
fixed-cut full HTTP smoke before publishing master. The existing start.sh and
root-owned release helper remain the deployment path, with their normal full
Sapling gate before restart. Inspect their live identity/configuration first.

Record pre/post executable source/hash, service/argv/data identity, advancing
height, known historic balances/transaction and full 9M HTTP call results. Keep
first/repeat live latency separate from the private cold-history page tests; do
not evict production caches. Verify route coverage, posting/cold frontiers and
recent integrity errors. If read coverage remains incomplete, report the live
result accurately and continue the already authorized bounded bootstrap; do not
force a full-library rebuild or alter persistent state to match the benchmark.
No full-state MuHash recomputation or whole-node restore is repeated here.
