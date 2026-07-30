package snapshots

import "github.com/tronprotocol/go-tron/core/rawdb/etl"

// RestoreETLOptions configures sorted ETL scratch space used by snapshot
// restore paths. Zero values preserve the collector defaults.
type RestoreETLOptions struct {
	TempDir     string
	BufferLimit int
	BatchSize   int
}

func (opts RestoreETLOptions) collectorOptions() etl.Options {
	return etl.Options{
		TempDir:     opts.TempDir,
		BufferLimit: opts.BufferLimit,
		BatchSize:   opts.BatchSize,
	}
}
