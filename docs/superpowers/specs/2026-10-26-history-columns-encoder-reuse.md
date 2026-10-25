# Reuse record-column encoder scratch without changing encoded bytes

The completed V5 fixed-copy experiment saves space but publication is 70–84%
slower than ordinary V2. The measured extra allocation is 1.23–1.32 GB per
bounded real merge. Do not adopt that writer or change production state.

Preserve all four candidates (plain and three key encodings), strict smaller
selection, 128 KiB boundaries, physical V5 bytes and the independent decoder.
Build common TxNum/length/flag/literal columns once, end-align the varying key
prefix before the shared tail, and reuse two compression destinations per
serial writer. Keep the existing stateless implementation as an independent
byte oracle. Scratch owns all copied bytes; no borrowed old-value data survives
a call. Reset clears annotations and retains only bounded scratch capacity.

Require exact transformed and compressed byte equality across all modes,
partial records, extreme integers, invalid layouts followed by recovery, reused
flags, stream flushes and Reset. Measure allocations and retained capacity.
Then repeat the three actual fixed-copy publication pairs outside other heavy
jobs, with canonical/companion/physical output proofs. New binary identity and
full fixed-copy MuHash, HTTP warm/fresh/private-cold and recovery acceptance are
still required before claiming readiness. No push, deploy, restart or deletion.
