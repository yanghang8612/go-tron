# Owned buffers for latest-domain MuHash commits

Baseline: `bc939af29656839de71c1716b440100a5df7cab0`. This change is a same-database
upgrade: scheme, canonical state and element codecs, local roots, Java consensus
roots, fork/unwind/snapshot behavior and publication cuts remain unchanged.
Root/Astra approved the ownership design and the pre-screen product-scratch
amendment; release still requires the frozen performance gates and final review.

## Accumulator ownership

One accumulator exclusively owns numerator, denominator, mapping, product and
reduction high/low storage. SHA256 and ChaCha20 still consume the complete
canonical element, expand exactly 384 little-endian bytes, and reject mapped
zero. Mapping reverses a local fixed-width stream and sets an owned `big.Int`;
FromState similarly converts a local fixed-width buffer directly into owned
numerator storage while retaining strict invalid-state rejection.

Multiplication writes into a distinct product scratch, uses the existing bounded
reduction, then swaps field/product storage as an internal ownership move. No
other pointer to either field is retained across this move. This is not general
permission to shallow-copy accumulators or `big.Int` values. All nonempty
backing ranges remain disjoint. Go 1.25.5 `nat.mul` explicitly discards receiver
storage when it aliases a multiword input; the distinct receiver avoids that
recurring allocation after scratch warmup.

Clone deep-copies numerator/denominator and starts mapping/product/reduction
scratch at zero. Finalize remains observational, uses its own local temporaries,
and still performs inverse, canonical 384-byte serialization and SHA256 for
every block, including empty changes. A fresh FromState accumulator is already
independently owned; Apply need not clone it a second time.

Apply owns one element buffer for its complete synchronous invocation. The new
append codec validates the physical key family before touching its destination,
requires nonoverlapping destination and input storage, and emits exactly the
old domain separator/family/full key and value lengths/bytes. The existing Encode
API continues to return independently owned storage. There is no global cache,
cross-block scratch pool or shared mutable buffer.

## Captured before images and validated deltas

First physical capture still clones the raw before value and keeps exact
presence; the map's owned key string already identifies its row, so an unused
second key copy in the map value is unnecessary. Assembling an update borrows
this immutable owned before buffer. Its slice header extends the lifetime past
collector reset/clear/release and delayed worker completion. Collector cleanup
never overwrites or pools the backing before bytes. Absent before is nil;
present empty and physically encoded zero remain present.

Unknown KV before still reads the actual same-cut latest source. Borrowing a
pending/cache row lasts only until immediate presence encoding and first-capture
ownership are complete. The internal borrow helper distinguishes commitment
capture from SLOAD decoding so capture does not increment execution's storage
pending-resolution counter. Existing public read ownership and SLOAD counters
remain unchanged. RMW reuse retains its existing physical-before semantics.

The Updates entry validates known-before and coalesces into owned canonical
deltas before reading HEAD, then calls a private application path that avoids
repeating this work. The public Deltas entry retains HEAD/root validation before
coalescing, ownership copies, intermediate-chain checks, retry identity checks
and original error order. No public caller can select the trusted private path.
Source writes, HEAD, current root and canonical identity still publish through
the same bound layer and durable cut.

## Frozen verification

`build/benchmarks/20261019-commit-before-muhash/screen-protocol-v2.json` freezes
baseline/C1/combined C1+C2, nine selectors, fifteen balanced variant runs, five
samples per variant at 300ms, exact environment/native library and acceptance
thresholds before timed sampling. Full Apply Updates chains use legal storage
keys and presence envelopes; four Pebble lifecycle cases include real capture,
commit and flush. Complete physical-source root/state oracles run after timing;
all Apply/Finalize/source/HEAD writes and fixture flushes remain timed. Primitive
controls and untimed fixed lifecycle Get counts prevent interpreting allocation
changes as measured SSD savings. No repeated search or extra samples are allowed.

Correctness includes official/Python mapping vectors and canonical reload,
independent generic modular arithmetic over continuous Insert/Remove/alternating
scratch rotations, disjoint backing, warm parent/clone interleaving, observational
Finalize, invalid-state rejection without mutation, append bytes/ownership/error
atomicity, missing/empty/physical-zero before capture, delayed A/B folds after
collector reuse, borrowed-buffer overwrite, and unchanged capture/SLOAD counters.
A mapped-zero cryptographic preimage is not reasonably constructible; no test
hook or alternate production mapping is introduced to fabricate one.

Relevant clean-export normal and focused race tests precede one exact Linux
Go 1.25.5 CGO Sapling full-repository test/build with the existing native archive.
Protected unrelated working changes remain byte-identical and excluded. Final
Root/Astra review and server deployment gates precede master push/deployment.
