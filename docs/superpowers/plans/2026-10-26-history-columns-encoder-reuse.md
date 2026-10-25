# Record-column encoder allocation follow-up

1. Keep the frozen 4d69f072 policy/native baseline and f64ed787 fixed-copy
   candidate intact. Implement writer-owned scratch in this working tree.
2. Compare transformed bytes in changing mode order and final stored bytes
   with the unchanged stateless oracle. Cover extremes, partial records,
   flag reuse, invalid layout recovery, stream ownership and Reset.
3. Record bounded retained capacity, allocation counts and same-input
   page benchmarks. Run focused tests, five affected packages, race, vet and
   the Sapling build; freeze the source identity before native work.
4. Build a new private native source/binary set. Recheck actual fixed-copy
   publication on all three retained real pairs, alternating original and
   optimized encoders with all preparation/validation outside timed regions.
   Require identical physical output SHA, canonical bytes and companions.
5. Rerun correctness and full HTTP acceptance with the actual new binary.
   Preserve regressions and limits. Retain originals and exact space receipts.
   No push, deployment, production restart, deletion or budget changes.
