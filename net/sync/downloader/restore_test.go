package downloader

import (
	"errors"
	"reflect"
	"testing"

	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/types"
)

func TestRestoreStagedBodiesConsumesBufferedThenRows(t *testing.T) {
	h2 := tcommon.Hash{0x02}
	block3 := testBufferedBlock(3)
	h3 := block3.Hash()
	buffer := map[uint64]BufferedBlock{
		2: {Num: 2, Hash: h2},
	}
	hashes := map[tcommon.Hash]struct{}{h2: {}}
	path := NewBlockPath()
	path[2] = h2
	rows := []rawdb.SyncStagedBlockRow{stagedRestoreRow(t, block3)}

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
	block2 := testBufferedBlock(2)
	block4 := testBufferedBlock(4)
	h2 := block2.Hash()
	buffer := map[uint64]BufferedBlock{}
	hashes := map[tcommon.Hash]struct{}{}
	path := NewBlockPath()
	rows := []rawdb.SyncStagedBlockRow{
		stagedRestoreRow(t, block2),
		stagedRestoreRow(t, block4),
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

func TestRestoreStagedBodiesBridgesPersistedGapWithBufferedBlock(t *testing.T) {
	block2 := testBufferedBlock(2)
	block3 := testBufferedBlock(3)
	block4 := testBufferedBlock(4)
	h3 := block3.Hash()
	buffer := map[uint64]BufferedBlock{
		block3.Number(): {Num: block3.Number(), Hash: h3},
	}
	hashes := map[tcommon.Hash]struct{}{h3: {}}
	path := NewBlockPath()
	path[block3.Number()] = h3
	rows := []rawdb.SyncStagedBlockRow{
		stagedRestoreRow(t, block2),
		stagedRestoreRow(t, block4),
	}

	got := RestoreStagedBodies(block2.Number(), 10, 1, buffer, hashes, &path, stagedRows(rows, nil))
	if !got.NeedPruneTail || got.PruneFrom != block4.Number()+1 {
		t.Fatalf("prune = %v from %d, want tail prune from block5 after bridging buffered gap: %+v", got.NeedPruneTail, got.PruneFrom, got)
	}
	if got.Restored != 3 || got.TargetHead != block4.Number() || got.NextExpected != block4.Number()+1 {
		t.Fatalf("result = %+v, want restored block2..4", got)
	}
	if !got.HaveLastRestored || got.LastRestoredNum != block4.Number() || got.LastRestoredHash != block4.Hash() {
		t.Fatalf("last restored = %+v, want block4", got)
	}
	for _, block := range []*types.Block{block2, block3, block4} {
		buffered, ok := buffer[block.Number()]
		if !ok || buffered.Hash != block.Hash() {
			t.Fatalf("buffer[%d] = %+v ok=%v, want restored hash %x", block.Number(), buffered, ok, block.Hash())
		}
		if _, ok := hashes[block.Hash()]; !ok {
			t.Fatalf("hash %x for block %d was not registered", block.Hash(), block.Number())
		}
		if path[block.Number()] != block.Hash() {
			t.Fatalf("path[%d] = %x, want %x", block.Number(), path[block.Number()], block.Hash())
		}
	}
}

func TestRestoreStagedBodiesStopsAtPathConflict(t *testing.T) {
	block2 := testBufferedBlock(2)
	conflict := tcommon.Hash{0xff}
	buffer := map[uint64]BufferedBlock{}
	hashes := map[tcommon.Hash]struct{}{}
	path := NewBlockPath()
	path[2] = conflict

	got := RestoreStagedBodies(2, 10, 1, buffer, hashes, &path, stagedRows([]rawdb.SyncStagedBlockRow{
		stagedRestoreRow(t, block2),
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
	block2 := testBufferedBlock(2)
	buffer := map[uint64]BufferedBlock{}
	hashes := map[tcommon.Hash]struct{}{}
	path := NewBlockPath()

	got := RestoreStagedBodies(2, 10, 1, buffer, hashes, &path, stagedRows([]rawdb.SyncStagedBlockRow{
		stagedRestoreRow(t, block2),
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

func TestRestoreStagedBodiesPrunesInvalidRawAtRow(t *testing.T) {
	block2 := testBufferedBlock(2)
	block3 := testBufferedBlock(3)
	rows := []rawdb.SyncStagedBlockRow{
		stagedRestoreRow(t, block2),
		{Number: block3.Number(), Hash: block3.Hash(), Raw: []byte{0x03}},
	}
	buffer := map[uint64]BufferedBlock{}
	hashes := map[tcommon.Hash]struct{}{}
	path := NewBlockPath()

	got := RestoreStagedBodies(2, 10, 1, buffer, hashes, &path, stagedRows(rows, nil))
	if got.InvalidError == nil {
		t.Fatalf("InvalidError = nil, want decode error: %+v", got)
	}
	if got.InvalidRow.Number != block3.Number() || got.InvalidRow.Hash != block3.Hash() {
		t.Fatalf("InvalidRow = %+v, want block3 row", got.InvalidRow)
	}
	if !got.NeedPruneTail || got.PruneFrom != block3.Number() || got.NextExpected != block3.Number() {
		t.Fatalf("restore result = %+v, want prune from invalid block3", got)
	}
	if got.Restored != 1 || got.LastRestoredNum != block2.Number() || got.LastRestoredHash != block2.Hash() {
		t.Fatalf("restore result = %+v, want only block2 restored", got)
	}
	if _, ok := buffer[block2.Number()]; !ok {
		t.Fatal("valid prefix block2 was not restored")
	}
	if _, ok := buffer[block3.Number()]; ok {
		t.Fatal("invalid block3 was restored")
	}
}

func TestRestoreStagedBodiesPrunesMetadataMismatchAtRow(t *testing.T) {
	block2 := testBufferedBlock(2)
	block3 := testBufferedBlock(3)
	raw3, err := block3.Marshal()
	if err != nil {
		t.Fatalf("marshal block3: %v", err)
	}
	wrongHash := tcommon.Hash{0xee}
	rows := []rawdb.SyncStagedBlockRow{
		stagedRestoreRow(t, block2),
		{Number: block3.Number(), Hash: wrongHash, Raw: raw3},
	}
	buffer := map[uint64]BufferedBlock{}
	hashes := map[tcommon.Hash]struct{}{}
	path := NewBlockPath()

	got := RestoreStagedBodies(2, 10, 1, buffer, hashes, &path, stagedRows(rows, nil))
	var mismatch *BufferedBlockMetadataMismatchError
	if !errors.As(got.InvalidError, &mismatch) {
		t.Fatalf("InvalidError = %T %[1]v, want metadata mismatch", got.InvalidError)
	}
	if mismatch.ExpectedNum != block3.Number() || mismatch.ExpectedHash != wrongHash || mismatch.GotHash != block3.Hash() {
		t.Fatalf("mismatch = %+v, want staged wrong hash and decoded block3", mismatch)
	}
	if !got.NeedPruneTail || got.PruneFrom != block3.Number() || got.Restored != 1 {
		t.Fatalf("restore result = %+v, want restored prefix and prune from block3", got)
	}
	if got.InvalidRow.Number != block3.Number() || got.InvalidRow.Hash != wrongHash || !reflect.DeepEqual(got.InvalidRow.Raw, raw3) {
		t.Fatalf("invalid row = %+v, want staged block3 mismatch", got.InvalidRow)
	}
	if _, ok := buffer[block3.Number()]; ok {
		t.Fatal("metadata-mismatched block3 was restored")
	}
	if _, ok := hashes[wrongHash]; ok {
		t.Fatal("metadata-mismatched hash was registered")
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

func stagedRestoreRow(t *testing.T, block *types.Block) rawdb.SyncStagedBlockRow {
	t.Helper()
	raw, err := block.Marshal()
	if err != nil {
		t.Fatalf("marshal block %d: %v", block.Number(), err)
	}
	return rawdb.SyncStagedBlockRow{Number: block.Number(), Hash: block.Hash(), Raw: raw}
}
