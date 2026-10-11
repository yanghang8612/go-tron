# Targeted cold key-block compression, 2026-10-11

Local changes only; no deployment or publication. Existing Zstd block readers,
CRC checks, dictionary identities and independently seekable key blocks remain
unchanged. No new historical format compatibility path is introduced.

## Selection

Event lookup/sidecar blocks retain front coding. A block with at least 32 keys
is a compression candidate only if its first posting CRC occurs at least 16
times. This detects the common sidecar case where many singleton topic keys
point to the same segment. It is only a hint; CRC collisions do not affect
correctness. Candidate blocks use existing `cbCodec`; compressed bytes are
stored only if they save at least 10%. Random topic main-lookup blocks skip
compression without allocation. This deliberately misses some compressible
blocks whose first key is uncommon.

V7 state dictionary blocks independently use the same 10% actual savings gate.
Every block larger than all preceding blocks stays raw, ensuring physical
aggregate `keyDataLen >= max(rawLen)` for the existing reader's bound. Equal
sized subsequent blocks can compress. This avoids a second dictionary pass,
reader changes, and unbounded buffering. Single-block dictionaries stay raw.
The older in-memory V6 encoder is unchanged. The real V7 builder test with
2,048 storage keys produces 16 blocks, 12 compressed: dictionary 13,552 →
9,181 bytes (32.25% saved), followed by point lookup of every key and exhaustive
accessor validation.

## Same-input measurements

Apple M1 Max, workspace Go 1.27.2, hot memory, 200ms benchmark windows.
Synthetic fixtures use fixed seeds and the actual production key-block encoder;
these are shape experiments, not mainnet distribution estimates. Every codec
fixture is decoded and compared byte for byte before timing.

| Input | Raw bytes | Stored bytes | Result |
| --- | ---: | ---: | --- |
| Event lookup random topic, distinct posting CRC | 5,184 | 5,198 if attempted | reject; adaptive keeps 5,184 |
| Event lookup address-shaped, distinct CRC | 3,657 | 3,671 if attempted | reject |
| Event sidecar random topic, singleton same-segment posting | 5,195 | 4,476 | 13.84% saved |
| V7 dictionary account/slot shape | 7,384 | 3,612 | 51.08% saved |
| V7 dictionary random storage keys | 7,391 | 7,405 if attempted | reject; keep raw |

Three adaptive event benchmark runs: random-block selection 59.2–59.7 ns,
0 B/0 allocations; sidecar compression 60.7–73.3 us, 5,376 B/1 allocation;
sidecar decode 3.35–3.57 us, 5,376 B/1 allocation. Raw-block clone baseline
0.43–0.47 us with the same 5,376 B allocation. These isolate incremental codec
cost, excluding disk I/O and key parsing. Old writer did no codec work.
Dictionary account/slot encode/decode 46.7/3.95 us; random dictionary compression
attempt 4.47 us. Reader memory remains bounded to one decoded block; compressed
reads allocate stored bytes in addition to decoded bytes as before.

The existing 270-log full builder regression's sidecar shrinks 1,970 → 842 B
(57.26%); its 11,284-byte main file checksum remains identical. Both one-worker
and four-worker builds retain exact output/semantic assertions.

The server's earlier samples reported dictionary 24% and postings 11% Zstd
savings, but those section samples are not complete random-read units. Postings
already use independently seekable delta-varint frames. Compressing the whole
posting stream would defeat those bounds, and adding another framing format is
outside this small change. Transaction hashes showed no benefit and remain raw.
Production V7 posting-list encoder experiments (128 postings, fixed seed):
sparse/random deltas 1,009 → 1,024 B (reject; 2.06 us attempted encode);
regular hot-key deltas 389 → 31 B (0.69 us attempted encode). The latter is
highly compressible but would require a new per-frame flag/length layout and
reader handling; the existing codec option does not cover postings. It is
deferred rather than applying whole-stream compression to all distributions.
No whole-store savings or online throughput claim is made from these fixtures.

Commands:

```sh
go test ./core/state/snapshots -run 'TestEvent(Log|Lookup)' -count=1
go test -race ./core/state/snapshots -run 'TestEvent(Log|Lookup)' -count=1 -timeout=180s
go test ./core/state/snapshots -run '^TestHistoryDictionary' -count=1 -v
go test ./core/state/snapshots -run '^$' -bench '^BenchmarkEventLookupAdaptive$' -benchtime=200ms -count=3
go test ./core/state/snapshots -run '^$' -bench '^BenchmarkHistoryDictionaryCompression$' -benchtime=200ms
```

Raw outputs: `build/benchmarks/20261011-targeted-key-compression/`.

Validation: full snapshots package passed (117.857 s); event tests and their
race run passed. The V7 builder regression additionally checks every point key
and the complete accessor.

Final combined regression was also run with explicit Go 1.25.5, matching the
deployed node: core subpackages, actuator and CLI all passed, including the full
snapshots package (204.697 s). The Sapling-enabled production build passed.
See `build/benchmarks/20261011-migration-performance/final-go125-tests.log`
and `final-go125-build.log`. No production files were rewritten.
