# Frozen storage experiments

The dated tools here are evidence for the source version recorded in each
experiment receipt. They intentionally preserve V3 full-Account values and old
permission bytes. They are not V4 product packages or compatibility adapters.
The nested module keeps `go test ./...` and `make test` scoped to product code
without editing the frozen experiment sources or weakening V4 write guards.

Reproduce the original experiments at source commit
`fdc81c29e4d10ae703b29d88095f8ec1bd436110`, using their recorded instructions.
Named-source programs may also be built from the repository root with explicit
file arguments, as in the original receipts. Do not run old writers against V4
databases. New product-code fixed-cohort tests live in `core/state` and
`core/state/snapshots`; they are part of the normal product test graph and require
explicit environment variables before accessing any captured input files.
