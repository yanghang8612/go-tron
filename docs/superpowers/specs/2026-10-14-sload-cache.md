# SLOAD read allocation reduction

The October 14 execution sample reports 20.01% SLOAD object-cache misses in the
23M range versus 7.23% in an earlier range. These are logical misses, not SSD
misses. The existing path already reaches the canonical blockbuffer cache via
commitScopeLatestReader, the physical latest adapter and rawdb's structured read.
The oversized-map observer reloaded only 134 of 13,824 exact sampled keys; this
does not justify changing the 4096-slot cross-block cache limit.

Allocation profiling found that readTransactionVersionedAccountKV constructs an
owned string from every logical key before the lower helper discovers that no
versioned retry reader is bound. Ordinary execution therefore paid a 32-byte
allocation for each cold SLOAD's physical row key. Return the same absent-version
result before constructing the string when StateDB or its reader is nil.

When a reader is enabled, retain the existing owned key, transaction index,
post-image, tombstone and miss behavior. Retained reader keys must remain valid
when callers reuse their input scratch. Public KV value ownership, dirty and
pending precedence, storage existence, account generation, snapshots/revert,
version rebinding, rewind, cache admission and close lifetimes are unchanged.
No cache is added and no increased cache-hit ratio is claimed.

A strict scoped scalar decoding experiment removed an additional intermediate
value copy but added callback overhead. Isolated ablation did not establish a
CPU benefit beyond the nil-reader guard, so that implementation is excluded.
The experiment and raw results remain under ignored build/benchmarks/20261014-sload.

Compare the final change with an isolated git archive of ebe93b33 using the same
fixed-input benchmark. Cover complete blocks with repeated reads, writes,
snapshot/revert, scope pending reads, cross-block object-cache replacement,
commitment and physical flush, plus a fixed storage trace with present, durable
zero and absent rows. Validate enabled-reader key ownership and tombstones,
disabled-reader zero allocation, and existing storage/versioned lifecycle tests.
Report local timing and allocation results separately from production throughput;
this change does not reduce the number of persistent-layer reads.

A follow-up against c1d68990 reuses the object already resolved by the same cold
SLOAD while deriving its Java storage row and checking generic KV overlays. The
outer SLOAD still records account existence and contract metadata dependencies;
the decoding helper retains logical KV/generation tracking, dirty tombstones,
fresh-generation handling, versioned-reader precedence and the existing borrowed
latest-read path. A separate empty-map guard avoids constructing composite keys
when no dirty entries exist. No object is retained beyond the current read, and
no new cache or borrowed-value lifetime is introduced.

The fixed 100-block lifecycle test compares the complete root sequence with the
c1d68990 reference, includes present/missing/durable-zero slots, dirty writes,
snapshot/revert, scope-pending reads, object-cache replacement and physical flush.
It also counts contract-storage point calls into the durable store. Baseline and
candidate both make 2036 calls without a base cache and 549 with the same 1 MiB
cache: this follow-up reduces CPU work, not durable-read count. A dependency test
checks the cold path's existence, metadata, generation, slot and logical-row read
set and retained row-key ownership after scratch reuse.

The new production profile places most cold-SLOAD CPU in Pebble Get. Predictive
state read-ahead warms account envelopes, metadata/code and predictable auxiliary
rows; it cannot infer arbitrary TVM storage slots from a transaction. The latest
online oversized-cache sample reloaded 232 of 27,520 sampled released keys, so
raising the 4096-slot object-cache bound remains unsupported. Physical-latest
cache admission/capacity and per-Get costs need separate fixed-key evidence.

The direct hot/history router is on every durable cold-SLOAD path. Its byte-key
classifier now compares the existing schema prefixes directly, avoiding an
owned string conversion; the separate string-key entry point remains unchanged.
Cursor construction also omits the unused engine cursor when the complete
prefix range belongs to one partition. Every prefix beneath a history-family
prefix belongs to history. A prefix that is an ancestor of any history family
or the exact stage-progress singleton remains dual; all remaining prefixes are
hot. The exact singleton prefix remains dual because its extensions are hot.
Both snapshots are still acquired together under the mutation lock and both are
closed, preserving the pinned cut and all existing dual-cursor cleanup errors.

Boundary validation exposed an existing ResetAccountKV bug on isolated c1d68990:
reset left warm/dirty TVM storage in the old namespace, and revert could retain a
new generation's missing-runtime/default-layout cache or lose pending metadata.
The reset journal now owns detached storage/origin maps and a cloned metadata
pre-image. Reads and writes after reset use separate maps; revert releases those
new-generation maps, restores the previous maps and pending metadata, and
invalidates derived runtime/layout state. Detached maps never enter the pools
while the journal owns them, and the non-recyclable reset entry cannot clear a
restored map when removed. Nested resets, pending metadata ownership, cached and
cold storage, dirty SSTORE, commit and reopen are covered without manually
clearing caches across reset/revert. This is a correctness repair for the reset
API; no non-test ResetAccountKV caller was found and no performance gain from it
is claimed.

Real dual-Pebble cursor tests cover all ancestors of the stage singleton, the
singleton itself and its NUL/suffix hot extensions with wrong-engine shadow
values. Live writes after snapshot acquisition cannot change the observed cut.
Cold SLOAD tests cover initially unloaded version 0/1 metadata with non-default
creation hashes, exact logical metadata/storage dependencies, metadata changes
and revert, generation resets and versioned reader rebinding. Existing backend
out-of-prefix behavior remains unchanged: the iterator reports its bounds error
through View and Close, while the bounded-Get cursor reports an absent row.
