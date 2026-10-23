# Reuse the direct Ancient V2 receipt decode

The fixed five-minute ef25 production sample at height 31,923,824--31,936,606
sustained 42.61 blocks/s and 7,078 transactions/s. Two 60-second CPU profiles
showed 3.75--3.82 average cores, while a separate 60-second allocation delta
sampled 53.41 GB allocated. Direct V2 preparation workers accounted for 30.38 GB
(56.89%); strict receipt validation and external-log compaction decoded the
same TransactionRet independently. This is background allocation that may
interfere with import through GC, not proof of importer critical-path time.

Keep VM and transfer execution serial. Reuse one exclusively owned decoded
TransactionRet across the direct freezer's strict validation and external-log
codec. The new rawdb entry point takes the already-decoded block's transactions,
the immutable original receipt and body bytes, and the existing externalization
flag. It performs every former strict check before compaction. Non-direct
append validation and the existing codec remain the independent reference.

Typed transaction hashes still validate canonical coverage. Separately derive
wire raw_data hashes for the existing identity-stripping rule: these hashes are
not interchangeable for exceptional protobuf encodings. Only when the wire
stripper reports an actual removal may the owned decoded message's ID fields
be cleared. Keep original/compacted wire bytes as the exact fallback for
logless, tiny, malformed or otherwise exceptional codec inputs. Mutate no
input bytes and retain unknown fields, duplicate-field semantics, genesis
exemptions, error behavior and the external-log envelope format.

Test against the complete old decode/validate/compact composition, including
malformed nested data, duplicate IDs/raw_data/scalars, nonminimal wire values,
unknown fields, maps, zero/default/negative block numbers, coverage mismatches,
and legacy block fallback. V2 must never publish on a strict validation error;
verify parallel preparation, complete migration/reopen/hydration and race
regressions. Benchmark both receipt preparation and complete three-table
migration with a fixed corpus, allocations, output size and errors. This does
not alter cache budgets, compression levels, publication/reclaim, transaction
indexing, state roots, consensus or storage formats. Linux native verification,
Astra review and demonstrated improvement precede master push/deployment.
