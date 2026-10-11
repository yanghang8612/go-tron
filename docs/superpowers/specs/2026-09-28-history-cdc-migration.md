# Offline V2 to CDC V3 history migration

The existing V2/V6/V7 history inventory is rewritten without changing any logical
history byte. R1 stays disabled. The node and automatic deployment remain stopped;
the CLI holds the real datadir's exclusive Pebble lock throughout publication and
source retirement. The existing R1 migration lock also excludes another migration.

A new `db migrate-history-cdc` entry selects complete trios within inclusive tx
bounds, newest first. Default is a read-only plan; `--yes` executes. The default
batch is one trio. `--max-trios=0` attempts the full selected inventory, and a size
rejection consumes a batch slot. A measured history file reduction of at least 5%
is required by default. Ordinary small-value segments can remain V2 indefinitely.

Source admission: only V2/V6 with a complete checksummed trio; at most 2 GiB logical,
20 million records and 256 MiB per companion. The work estimate includes output,
scratch, companions, verifier ETL and slack; a live filesystem reserve is checked
before every trio. The CLI's default reserve is 64 GiB. No logical full-stream copy
is retained in memory; the codec has one encoder worker and bounded dictionary.

For each admitted source: fully verify the source trio, stream into production CDC
V3, reject insufficient gain, compare every decoded byte, install content-addressed
history and unchanged companion bytes under the corresponding new stem, verify
the new trio, recheck source identities, and persist the small recovery journal.
Publication reuses the existing fsync + manifest atomic rename + directory fsync
protocol. Recheck the new trio before unlinking exactly the old unleased identities.
Chain identity, visibility, progress, unrelated active refs and unrelated retired
refs are preserved. No global GC, mode change, hot pruning or lifecycle runs.

CDC journals use `.history-cdc-migration.json`. An unfinished other-format journal
is refused; metadata GC defers for either journal. Resume before publication
reproves the conversion; resume after publication verifies the new trio even if
some old files have already been unlinked. Conflicting manifests or output files
fail closed. Immutable installed outputs are retained on uncertain publication;
only the command's private scratch directory is removed automatically.

Validation covers exact source/destination history digest, index queries, mixed
V2/V3 inventory, newest-first selection, size rejection, offline lock and disk
admission, interruptions before/after publication and partial retirement, corrupt
source/new companions, leases, cross-target journals and unchanged progress. A
small production pilot must pass before broadening the selected range.
