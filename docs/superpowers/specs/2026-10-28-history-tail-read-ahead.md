# Bounded history tail reads

Keep the deployed node and master unchanged. Compare with cda883ba on the same
H45,836,827 fixed input and routing coverage. Prioritize complete HTTP eth_call,
return values and process CPU/allocation/I/O counters. Do not repeat full MuHash
or export/restore. Fresh process is not OS-cold: use private-inode cache eviction
where the existing facility permits it, otherwise label the limitation.

Uncovered sources currently perform independent small reads serially. Add a
bounded ordered window of exact-key binary source lookups, with process-wide
I/O admission, request cancellation and bounded retained results. Routing stays
on the request goroutine; workers only read immutable exact history/accessor
pairs. Consume results in manifest order. If earlier results resolve part of a
speculative lookup, retry only the still-required keys before exposing an error.
Oversized results fall back to the existing sequential callback path. Join all
workers and close descriptors before returning; never retain cross-request
readers, buffers or absence proofs.

Coalesce the initial accessor magic and header on the same descriptor, and
anchor final-page reads at EOF so descending adjacent footers share one read.
Keep existing CRC, dictionary binding, range and identity checks unchanged.

The exact-key lookup context also marks only its own Linux descriptors as
FADV_RANDOM. Suppress unrelated kernel readahead on these independent point
files, without changing sequential scan descriptors or evicting any pages.
Compare the hint separately on the same private cold-history input; other
platforms retain normal file reads and a failed optional hint is nonfatal.

Tests cover ordered errors/results, cancellation/join, result budget fallback,
mixed owners/domains and corrupt tail/footer bytes. Benchmark incomplete and
complete routing separately; do not count a routing coverage change as reader
speedup, nor claim unmeasured OS-cold improvement.
