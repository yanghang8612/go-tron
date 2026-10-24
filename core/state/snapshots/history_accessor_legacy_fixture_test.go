package snapshots

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/tronprotocol/go-tron/core/rawdb/etl"
)

// Legacy-reader tests must continue exercising actual V7 bytes after the current
// builder advances. Rebuild only the accessor, preserving the history dictionary.
func buildStateDomainChangeV7SegmentsForTest(t testing.TB, db ethdb.Iteratee, dir string, from, to uint64, path string) ([]SegmentRef, error) {
	t.Helper()
	refs, err := BuildStateDomainChangeHistorySegmentsFromDB(db, dir, from, to, path)
	if err != nil {
		return nil, err
	}
	for i, ref := range refs {
		if ref.Kind != SegmentAccessor {
			continue
		}
		f, err := os.Open(filepath.Join(dir, ref.Path))
		if err != nil {
			return nil, err
		}
		st, err := f.Stat()
		if err != nil {
			f.Close()
			return nil, err
		}
		entries, err := readStateDomainChangeBinaryAccessorV7Debug(f, uint64(st.Size()))
		f.Close()
		if err != nil {
			return nil, err
		}
		b, err := newStateDomainChangeV6Build(etl.Options{TempDir: t.TempDir()}, dir, ref.Path+".legacy")
		if err != nil {
			return nil, err
		}
		for _, entry := range entries {
			if err = b.CollectLogicalKey(entry.key); err != nil {
				break
			}
		}
		if err == nil {
			err = b.FinishDictionary()
		}
		if err == nil {
			for _, entry := range entries {
				var id uint32
				id, err = b.KeyID(entry.key)
				if err == nil {
					err = b.CollectPosting(id, entry.txNum, entry.offset, entry.recordIndex)
				}
				if err != nil {
					break
				}
			}
		}
		if err == nil {
			refs[i], _, err = b.buildAccessorContextVersion(context.Background(), dir, ref, uint64(len(entries)), stateDomainChangeBinaryVersionV7)
		}
		b.Close()
		if err != nil {
			return nil, err
		}
	}
	return refs, nil
}
