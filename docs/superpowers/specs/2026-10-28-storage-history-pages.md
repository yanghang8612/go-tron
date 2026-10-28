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

## Runtime admission after the measured prototype

The fixed-copy prototype built all 6,436 uncovered pairs into 421 pages / 10.1 MB
in 704 seconds under a 128 MiB / two-second soft budget. It reduced complete 20M
HTTP reads from 640 MB / 94,858 read syscalls to 93 MB / 2,013. Authenticate the
existing broad v2 source table once per Runner and exact broad ref identity;
retain only IDs and bounds as scheduling hints so covered sources are not rebuilt.
Never use these cached hints to answer a query or write an absence proof.

Keep the storage scan cursor and backoff in Runner-owned state, not a global
directory cursor. Hash canonical pair identities plus relevant storage/broad refs
within the previous scan horizon before honoring backoff. Replacement/retirement
inside that horizon cancels deferral and rewinds. A new tail beyond the horizon
must not defeat repeated no-output backoff. Advance a publication cursor only
after guarded success. A completed unsupported batch may advance the scheduling
scan cursor without claiming coverage; hard errors and conflicts do not advance.
No-output retries back off from one minute to thirty minutes. Fully scanned
catalogs idle thirty seconds. Progress remains subject to the existing shared
heavy gate and busy recovery duty budget.

Rotate small, metadata and storage auxiliary opportunities. Do not admit the old
3.4 GB account rebuild as another fair family. After three auxiliary publications,
or three admitted auxiliary work slices (including storage reads without output),
reserve the next admitted pass for one canonical compaction attempt before any
auxiliary/broad/account work. Bound that reservation to the existing busy leaf,
input/logical/record/source limits even when idle, and consume it when there is
no eligible merge. This prevents permanent auxiliary backlog from starving the
canonical merge. A storage slice that has read dictionaries or authenticated a
broad hint ends this maintenance opportunity even without output; it cannot fall
through into another expensive broad/account task on the same lease. New
cold-history publication deadlines remain independent. Count reported non-cancel
family failures toward the reserved opportunity and advance the family rotation;
a permanently broken optional object must not block every later family/merge.
Retained storage-page boundaries constrain a new page even when the intervening
canonical source is skipped by the broad admission hint. Storage's idle timeout
is not a wall-clock promise: global recovery, load and gate admission still apply.

Validate exact broad-ID reuse (including a hole inside its range), directory and
source replacement invalidation, cursor retry on publication conflict, budget
yield and unsupported advancement, idle/backoff without source reads, and real
Runner fairness with an actual canonical merge. On a private fixed copy remove
only a few storage pages, repair through the runtime publication path, check
canonical refs/coverage, and repeat full HTTP returns/performance without another
whole-state MuHash or full restore scan. No production restart/publication.
