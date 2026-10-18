# Bounded MuHash3072 reduction and signature-path evaluation

Baseline 2ca9c6f. The previous fixed 4.0M-height profile assigns 55.26 CPU-s
to RecoverSigners and 36.32 CPU-s to ApplyMuHashDeltas. The fresh fixed
4.33M–4.37M baseline has 52.90/270.90 CPU-s recovery (19.53%) and
36.54 CPU-s MuHash commitment (13.49%); its 300-second window averages
129.737 blocks/s and 8080.146 transactions/s. These are stage-dependent
observations, not a promised online gain. Offline focus shows54.52CPU-s of native
recovery already under runSigJob, while the entire ordered ValidateTxEnvelope
path is only0.88CPU-s. Existing per-Transaction sync.Once, immutable block
wrappers and bounded prewarming already share recovery with ordered validation.
No repeated-recovery or scheduling-stall evidence justifies a new global cache,
worker pool or native implementation here. Signature permission checks, Java
normalization/infinity/high-S behavior and first-error order remain unchanged;
this investigation must not be reported as a signature throughput improvement.

The MuHash profile instead identifies generic big.Int.Mod15.46CPU-s versus
mapElement8.35, fromLE2.58 and Finalize1.32. Use only math/big operations to
exploit p=B-c, B=2^3072 and c=1103717. For0<=x<B^2, one fold computes
x_low+c*x_high <(c+1)B. The second fold yields y<B+c^2<2p, so at most one
subtraction of p returns the canonical remainder. No unbounded reduction loop
or hand-written limb implementation is needed.

The unexported helper accepts only the bounded products of canonical
numerator/denominator and mapped element, or numerator and modular inverse.
Insert/Remove own high/low big.Int scratch in their exclusively owned
Accumulator. Split high/low completely before overwriting the receiver; the
mask, correction and modulus are immutable globals. Clone deep-copies num/den
and starts with independent zero scratch. Finalize remains observational and
uses local owned scratch. Mapping384LE has value<B, so compare/subtract once
replaces its generic Mod. Mapping zero and public invalid states still fail
closed; strict HEAD/state codecs never normalize an invalid stored field.
No source/HEAD publication, root, encoding, schema, cache or parallelism changes.

First benchmark old code on fixed actual inputs: primitive full block
FromState/Clone/remove/insert/Finalize, and complete ApplyMuHashDeltas chains
with legal32-byte storage slots/presence envelopes. Repeat identical parameters
on the candidate, reporting allocations and all controls rather than reduction
microbenchmarks alone. Independently compare reduction against big.Int.Mod at
0,p±1,B±1,B^2-1,(p-1)^2 and seeded random bounded integers. Retain official and
Python vector/random/reload tests, invalid state rejection, parent ownership,
concurrent independent clones and full source/HEAD/root unwind/reopen tests.
Root and Astra have approved this bounded math/ownership design. Release still
requires the exact combined cold-maintenance tree's checks and final audit.
