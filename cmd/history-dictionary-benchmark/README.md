# History dictionary zstd benchmark

Read-only diagnostic for the V6/V7 `.kv` accessor's 128-key dictionary blocks. It never opens Pebble, reads postings, changes cold files, or configures production compression.

```sh
go build -o build/bin/history-dictionary-benchmark ./cmd/history-dictionary-benchmark
build/bin/history-dictionary-benchmark -blocks 32 -repeat 200 /exact/file1.kv /exact/file2.kv > dictionary-benchmark.json
```

Pass 2–4 published accessor paths from different sizes/domains when sampling the server. Samples are evenly spaced through each dictionary, including the first and last block. Only the header, sampled directory offsets/entries, and sampled block bytes are read. Defaults cap total physical reads at 32 MiB and total decoded sample bytes at 64 MiB; `ReadBytes`/`ReadCalls` in the output report actual reads. A sampling error exits without emitting a partial report. Increase `-repeat` for steadier CPU measurements; it causes no additional file reads.

The report compares zstd `fastest,default,better`, compressed/raw ratios, raw fallback when compression loses, and encode/decode process user+system CPU. Encoder concurrency is fixed to one; decoder concurrency is zero, matching the current reader. Encode reuses destination capacity; decode allocates a destination like the reader. Timings exclude disk I/O, CRC, and key parsing. They measure codec cost, not complete query latency. Ratios describe the sample, not a whole-store guarantee.

Optional: `-extract` emits only a bounded sample JSON bundle; `-sample bundle.json` benchmarks that bundle locally without access to the original `.kv` files.

**Compatibility finding:** the reader already supports zstd when `StoredRaw` is unset, but also rejects `block.rawLen > header.keyDataLen`. Compressing the entire dictionary of a one-block file therefore fails; small multi-block files can fail too. A compatible writer strategy is to keep each file's largest decoded block raw and compress other blocks only when shorter. This guarantees total stored keyData is at least every raw block's length. `keep_largest_sample_block_raw_ratio` applies this strategy to the sampled blocks; the real largest block must be identified during the existing complete writer pass. `fully_sampled_unsafe_compressed_files` identifies this failure when all blocks of a sampled file were included. No production writer changes are included.

The reader separately bounds raw length by `keyCount*(65535+2*MaxVarintLen64+2*MaxVarintLen32)` (about 8.004 MiB for 128 keys) and verifies raw CRC after decode. Rewriting accessors would also require new section offsets, header/directory CRCs, file metadata/hash, and manifest references; changing only the stored flag is insufficient.

```sh
go test -race ./cmd/history-dictionary-benchmark -count=1
```

Tests use synthetic framing data, including a 1 GiB sparse postings section from which sampling reads only about 6 KiB. They do not claim a mainnet compression ratio.
