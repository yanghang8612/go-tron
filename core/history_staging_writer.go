package core

import (
	"github.com/tronprotocol/go-tron/core/blockbuffer"
	"github.com/tronprotocol/go-tron/core/rawdb"
)

// historyStagingRecordingWriter observes the exact changeset bytes as the
// canonical publisher writes them. Embedding Buffer preserves its atomic
// shared-chunk capability; only Put is intercepted. The receipt is written to
// the same block layer after FlushFinal, without a second storage read.
type historyStagingRecordingWriter struct {
	*blockbuffer.Buffer
	hasher *rawdb.HistoryStagingBlockHasher
}

func (w *historyStagingRecordingWriter) Put(key, value []byte) error {
	if _, err := w.hasher.MaybeAdd(key, value); err != nil {
		return err
	}
	return w.Buffer.Put(key, value)
}
