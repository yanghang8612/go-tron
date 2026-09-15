# Reference history implementation and in-place rollout

Implements [the reference-container design](../specs/2026-09-15-history-reference-container.md). The user explicitly authorized development on master, deployment, offline inspection, and in-place migration without a database copy. Genesis resync is an accepted recovery path; it is not needed merely to adopt this format.

1. Add an independently readable, bounded reference container whose virtual stream is exactly V6 history, with existing V7 companions.
2. Export authenticated hot-pack chunk spans from one pinned view, retaining whole-pack SHA, repair overlays, canonical range checks, and callback ownership. Pipeline pack authentication only, within the existing 256 MiB decoded budget; keep one ordered consumer.
3. Build a complete trio using a bounded metadata spool and unique owned chunks. Buffer directory I/O without changing on-disk bytes or reducing authentication.
4. Make compaction preserve references whenever an R1 input is selected, including mixed old/new inputs. Verify full logical history and physical companion equivalence against the old writer. Keep the writer opt-in.
5. Implement in-place migration in the original snapshot directory. Hold the original Pebble exclusive lock, reencode one trio, verify equality, journal source/target manifest identities, atomically publish, and reclaim only that trio's unleased old files. Resume partial publication/reclamation from the small journal. Dry-run must not create files. No full database copy.
6. Validate Go/race ownership and crash/error paths, then native Sapling build and fixed-input A/B/B/A. A slower experimental version must not be deployed as a performance fix.
7. Before the first production R1 publication, pin every service restart guard to a verified reference-capable binary. Run a bounded in-place migration and measure actual wall time, work space, and reclaimed files. Keep chain/stage progress identical during reencoding. Deploy the opt-in writer only after build/merge correctness and native performance gates pass.
8. Record actual online head/cold/backlog rates and physical sizes. Estimate whole-history migration from active trios, decoded bytes, and measured conversion; distinguish it from copying chaindata and from resyncing the chain.

At 07:38 UTC the active history trios totalled 657,510,817,313 bytes and no retired files or published download leases remained. Metadata/reference bounds are format admission limits; an oversized historical segment requires splitting or continued old-format reading, never silent truncation.
