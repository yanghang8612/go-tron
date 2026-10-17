# Bounded SST buffering and decompression error cleanup

## Scope

`objstorageprovider.newFileBufferedWritable` uses a fixed 64 KiB buffer instead
of bufio's 4 KiB default. SST data blocks near 4 KiB plus their trailer often
exhaust the old buffer, causing repeated small writes into `syncingFile`.
Coalescing these writes reduces calls through the file writer. Local SST creation
and the provider's local copy fallback use this object. WALs use the separate
record log writer.

This changes neither encoded SST bytes, compression, LSM geometry, cache budget,
consensus, nor persistent formats. It does not reduce logical compaction write
amplification or establish fewer block-device writes. Whether lower writer CPU
improves synchronization depends on the running node's other bottlenecks.

## Durability and bounded cost

Finish still flushes, syncs the file, then closes it, preserving first-error
precedence and closure after errors. Buffering can delay a write error until
Flush/Finish, as permitted by the existing Writable contract. Abort discards the
unflushed residual; closing an underlying syncingFile can sync an already-written
prefix. Abort is not a guarantee that no sync occurs.

The configured BytesPerSync threshold, 4 KiB offset rounding, and 1 MiB tail rule
are unchanged. Larger writes can move the offsets and times at which periodic
SyncTo starts. Finish still syncs the entire completed file.

Each active writable now reserves an additional 60 KiB. Eight buffers occupy
524,288 bytes rather than 32,768 bytes, an increase of 480 KiB. Many tiny SSTs
pay the allocation cost even when no underlying write call is saved. This fixed
cost is deliberate; there is no new pool, adaptive growth, or memory limit change.

## Independent reader ownership correction

`Reader.readBlock` allocates both compressed and decoded storage before decoding.
If a valid compressed-length header is followed by an invalid body, the error
branch must release both. Previously it released only compressed storage,
leaking a manually allocated decoded value or retaining an in-use pool slot.
The added decoded release changes no success-path work or error classification.
This is a resource correction, with no claimed healthy-read speed improvement.

## Verification and measured limits

`vfs_writable_buffer_test.go` covers Finish/Abort order and resource closure,
buffered and direct partial writes, short writes with nil errors, Flush/SyncTo/
Sync/Close failures, first-error precedence, the actual written length at each
periodic SyncTo, and the sum of eight live buffer sizes. Heap samples are auxiliary;
`bufio.Writer.Size` supplies the direct buffer-size measurement.

`decompression_error_test.go` supplies checksum-valid Snappy with a valid decoded
length and an invalid body. Repeated failures must return corruption, admit no
cache entry, and release pool slots. Healthy decoded values, an actual cache hit,
checksum failure, and invalid length headers cover neighboring ownership paths.
An isolated diagnostic forwarding real CGO calloc/free observed 128 failed reads:
the old reader retained 128 allocations (528,384 bytes); the correction retained
none. Diagnostic allocation instrumentation is not production code.

A fixed local Go 1.25.5 / CGO / GOMAXPROCS=2 three-pair AB/BA/AB screening included
complete Flush/Finish/Sync/Close. Real overlapping compaction reduced underlying
Write calls from 9,668 to 637 with identical returned write bytes. Total SST writes
were 39,557,206 bytes: 38,345,910 compaction output plus 1,211,296 flush output.
All selected rows, logical digest, final table shape and zero final debt agreed.
Whole-call CPU decreased 18.8–28.8%; wall decreased 3.9–12.8%. These runs used
an instrumented VFS; fewer observer calls can contribute to measured CPU savings.
Release validation therefore also requires the same fixed compaction workload
with unwrapped `vfs.Default`, and reports absent per-I/O observations as null.

The local 256-tiny-SST control saved no Write calls and allocated about 15.7 MB
more in total, consistent with 60 KiB per table. Timing was variable, so it does
not establish a tiny-table speed improvement. Only this tiny control compares
complete file-image hashes; the compaction oracle compares logical contents,
shape, read/write bytes and debt. The compaction fixture uses default Snappy across levels; the tiny-SST fixture
uses NoCompression. Production L0–L5 currently use no compression. These measurements do not
predict production throughput or a reduction in SST/device write amplification.

Tests are scoped to the new targeted cases and affected application packages.
The vendored sstable directory did not include the upstream reader test suite;
a targeted package PASS must not be described as running that full upstream suite.
