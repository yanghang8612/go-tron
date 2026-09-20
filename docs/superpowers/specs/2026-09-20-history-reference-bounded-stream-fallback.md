# Bounded R1 busy-leaf merge fallback

## Scope

This change applies only to busy leaf consolidation for state-domain history. The existing R1 reference merge remains the preferred output. When its conservative directory/span estimate rejects an otherwise eligible group, the selector may explicitly choose the existing compressed V6/V7 output path.

The fallback does not relax an R1 limit. It admits at most 512 MiB of physical input, 2 GiB of logical input, 8,000,000 records, and 16 sources, including when a caller leaves a corresponding configuration field unset. Non-busy aligned compaction retains its existing R1 admission policy.

## Streaming and validation

An R1 V6 record is copied from its logical `ReaderAt` using its 21-byte frame header and a fixed 256 KiB buffer. The path validates the frame length, key id, boolean flag, source bounds, output offset, transaction range, block-range hydration, record order, posting collection, and tx index while copying. It never allocates `Prev` at record length. Cancellation is checked between buffer reads.

The completed V6/V7 trio retains the existing trusted-output validation gates for the segment structure, dictionary, accessor, posting, and index. This change does not add a second full semantic scan before publication. Manifest replacement and source retirement retain the existing atomic integration and cleanup behavior. A failed or canceled build leaves the source manifest entries in place; tests compare the fallback's complete logical and companion output with the established merge oracle.

Mixed compressed/R1 inputs preserve CDC output when any compressed input already uses CDC3; otherwise the fallback writes the current Zstd V6/V7 format.

## Limits

This is a bounded re-encode of selected busy leaves. It does not migrate the existing R1 inventory, change foreground publication, or address higher-level non-busy merges. It removes the permanent R1 output-estimate barrier for eligible busy groups, but it does not attribute or eliminate every source of storage amplification.
