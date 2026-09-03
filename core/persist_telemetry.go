package core

// PersistStats describes the block-local metadata batch that is durably
// committed during canonical publication. It intentionally excludes
// blockbuffer layer flushes: those are asynchronous, may coalesce layers from
// several blocks, and can complete after this block's ApplyStats is published.
// Process-wide flush work remains available through the monotonic
// blockbuffer/flush/layers and blockbuffer/flush/output/bytes metrics.
type PersistStats struct {
	// MetadataRecords is the number of key/value records in the successful
	// metadata batch. It includes the block body and indexes, state root,
	// TAPOS reference, per-block receipt payload, optional transaction lookup
	// rows, and optional balance-trace rows.
	MetadataRecords uint64

	// MetadataBytes is the batch's logical key-plus-value byte count reported
	// by ethdb.Batch.ValueSize. It is not encoded Pebble batch size, WAL bytes,
	// SST bytes, or compaction write amplification.
	MetadataBytes uint64

	// TransactionLookupRows is the subset of MetadataRecords holding tx-hash
	// to block-number mappings. Bulk sync and cold-index-covered blocks omit
	// these rebuildable rows.
	TransactionLookupRows uint64

	// TraceAccounts counts AccountTrace rows in the metadata batch. The single
	// per-block BlockBalanceTrace row, when present, is included only in
	// MetadataRecords.
	TraceAccounts uint64
}

// Add folds one block-local persistence observation into an aggregate.
func (s *PersistStats) Add(other PersistStats) {
	if s == nil {
		return
	}
	s.MetadataRecords += other.MetadataRecords
	s.MetadataBytes += other.MetadataBytes
	s.TransactionLookupRows += other.TransactionLookupRows
	s.TraceAccounts += other.TraceAccounts
}
