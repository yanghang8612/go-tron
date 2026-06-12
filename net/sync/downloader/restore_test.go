package downloader

import (
	"errors"
	"testing"

	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
)

func TestRestoreStagedBodiesConsumesBufferedThenRows(t *testing.T) {
	h2 := tcommon.Hash{0x02}
	h3 := tcommon.Hash{0x03}
	buffer := map[uint64]BufferedBlock{
		2: {Num: 2, Hash: h2},
	}
	hashes := map[tcommon.Hash]struct{}{h2: {}}
	path := NewBlockPath()
	path[2] = h2
	rows := []rawdb.SyncStagedBlockRow{{Number: 3, Hash: h3, Raw: []byte{0x03}}}

	got := RestoreStagedBodies(2, 2, 1, buffer, hashes, &path, stagedRows(rows, nil))
	if got.NeedPruneTail {
		t.Fatalf("NeedPruneTail = true, want false: %+v", got)
	}
	if got.Restored != 2 || got.TargetHead != 3 || got.NextExpected != 4 {
		t.Fatalf("result = %+v, want restored 2 target 3 next 4", got)
	}
	if !got.HaveLastRestored || got.LastRestoredNum != 3 || got.LastRestoredHash != h3 {
		t.Fatalf("last restored = %+v, want block3", got)
	}
	if _, ok := buffer[3]; !ok {
		t.Fatal("row 3 was not restored into buffer")
	}
	if _, ok := hashes[h3]; !ok {
		t.Fatal("hash 3 was not registered")
	}
	if path[3] != h3 {
		t.Fatalf("path[3] = %x, want %x", path[3], h3)
	}
}

func TestRestoreStagedBodiesRequestsPruneAtGap(t *testing.T) {
	h2 := tcommon.Hash{0x02}
	h4 := tcommon.Hash{0x04}
	buffer := map[uint64]BufferedBlock{}
	hashes := map[tcommon.Hash]struct{}{}
	path := NewBlockPath()
	rows := []rawdb.SyncStagedBlockRow{
		{Number: 2, Hash: h2, Raw: []byte{0x02}},
		{Number: 4, Hash: h4, Raw: []byte{0x04}},
	}

	got := RestoreStagedBodies(2, 10, 1, buffer, hashes, &path, stagedRows(rows, nil))
	if !got.NeedPruneTail || got.PruneFrom != 3 {
		t.Fatalf("prune = %v from %d, want true from 3", got.NeedPruneTail, got.PruneFrom)
	}
	if got.Restored != 1 || got.TargetHead != 2 || got.NextExpected != 3 {
		t.Fatalf("result = %+v, want restored 1 target 2 next 3", got)
	}
	if !got.HaveLastRestored || got.LastRestoredNum != 2 || got.LastRestoredHash != h2 {
		t.Fatalf("last restored = %+v, want block2", got)
	}
	if _, ok := buffer[4]; ok {
		t.Fatal("gapped row 4 was restored")
	}
}

func TestRestoreStagedBodiesStopsAtPathConflict(t *testing.T) {
	h2 := tcommon.Hash{0x02}
	conflict := tcommon.Hash{0xff}
	buffer := map[uint64]BufferedBlock{}
	hashes := map[tcommon.Hash]struct{}{}
	path := NewBlockPath()
	path[2] = conflict

	got := RestoreStagedBodies(2, 10, 1, buffer, hashes, &path, stagedRows([]rawdb.SyncStagedBlockRow{
		{Number: 2, Hash: h2, Raw: []byte{0x02}},
	}, nil))
	if !got.NeedPruneTail || got.PruneFrom != 2 {
		t.Fatalf("prune = %v from %d, want true from 2", got.NeedPruneTail, got.PruneFrom)
	}
	if got.Restored != 0 || got.HaveLastRestored {
		t.Fatalf("result = %+v, want no restored row", got)
	}
	if _, ok := buffer[2]; ok {
		t.Fatal("conflicting row was restored")
	}
	if path[2] != conflict {
		t.Fatal("conflicting path reservation was overwritten")
	}
}

func TestRestoreStagedBodiesReadErrorRequestsPruneAtExpected(t *testing.T) {
	readErr := errors.New("read rows")
	h2 := tcommon.Hash{0x02}
	buffer := map[uint64]BufferedBlock{}
	hashes := map[tcommon.Hash]struct{}{}
	path := NewBlockPath()

	got := RestoreStagedBodies(2, 10, 1, buffer, hashes, &path, stagedRows([]rawdb.SyncStagedBlockRow{
		{Number: 2, Hash: h2, Raw: []byte{0x02}},
	}, readErr))
	if !errors.Is(got.ReadError, readErr) {
		t.Fatalf("ReadError = %v, want %v", got.ReadError, readErr)
	}
	if !got.NeedPruneTail || got.PruneFrom != 3 || got.NextExpected != 3 {
		t.Fatalf("result = %+v, want prune from expected 3", got)
	}
	if got.Restored != 1 || got.LastRestoredNum != 2 {
		t.Fatalf("result = %+v, want restored block2", got)
	}
}

func stagedRows(rows []rawdb.SyncStagedBlockRow, err error) StagedBodyIterator {
	return func(start uint64, fn func(rawdb.SyncStagedBlockRow) (bool, error)) error {
		for _, row := range rows {
			if row.Number < start {
				continue
			}
			cont, callErr := fn(row)
			if callErr != nil {
				return callErr
			}
			if !cont {
				return nil
			}
		}
		return err
	}
}
