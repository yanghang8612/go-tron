# Lazy storage-family proofs for first cold historical calls

Continue the first-cold-query objective on the existing fixed H45,836,827 copy.
No master publication, production restart, database reset or production cache
eviction is authorized. Prioritize measured HTTP performance and exact returns.

The current broad route covers 25,600 of 32,036 fixed-copy history pairs.
ContractStorage queries without a later match must visit its 6,436 unknown pairs.
The observed 20M request reads about 640 MB in 95,000 calls despite complete
metadata routing. Before attributing this entirely to storage, run a diagnostic
source archive that records batch keys/candidates/physical reads. Sample distinct
storage prefixes, target prefix membership and target slot membership separately:
an inactive slot does not imply an inactive owner/generation.

Prototype a separate optional storage-history-routing kind. Bind each filter to
the exact authenticated history/accessor pair ID and owner/generation/domain
prefix. Complete authenticated dictionary iteration precedes publication; unknown,
unsupported or oversized sources never yield partial absence proofs. Collect a
bounded array of prefix hashes before choosing the Bloom size. Adjacent sorted
prefixes deduplicate slots. Use eight double-hash probes, a power-of-two size of
at least sixteen bits per distinct prefix, at least 64 bytes for nonempty sources,
and at most 65,536 hashes / 128 KiB per source. An empty complete source has an
explicit zero filter. Bound each page to 256 sources and 1 MiB including headers.

Authenticate a whole small page against its manifest checksum, then cache only
that page for the current batch. Consult it lazily immediately before submitting
a canonical source read, after existing broad negative proofs. Do not load pages
inside candidate planning: early matches must not touch future pages. Positive,
missing, stale, malformed or unreadable filters follow the existing canonical
path. Cancellation propagates. No cache from another request proves absence.

Repairing an overlapping page must carry every still-current proof, including
those before the cursor, with both row and byte limits. Defer oversized repairs
without shrinking coverage. Publication must compare overlapping family and
canonical pair inputs under the existing publication lock; ordinary read leases,
retirement and signed-generation protection remain. Register the new kind and
strict dictionary-based checker, without changing canonical state or MuHash.

First validate the prototype through explicit builds on private inputs. Runtime
maintenance admission follows measured build cost and HTTP benefit, and must not
enable the previously rejected 3.4 GB account-table rebuild scheduling. The whole
goal remains open until actual first-cold performance is demonstrated; prototype
tests or warm sub-second calls alone are insufficient.
