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
