package downloader

import (
	"bytes"
	"errors"
	"testing"
	"time"

	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core"
	"github.com/tronprotocol/go-tron/core/types"
	corepb "github.com/tronprotocol/go-tron/proto/core"
)

func TestRawBlockBytesCopiesWirePayload(t *testing.T) {
	block := testBufferedBlock(1)
	raw, err := block.Marshal()
	if err != nil {
		t.Fatalf("marshal block: %v", err)
	}
	got := RawBlockBytes(block, raw)
	if !bytes.Equal(got, raw) {
		t.Fatalf("raw copy differs from source")
	}
	raw[0] ^= 0xff
	if bytes.Equal(got, raw) {
		t.Fatal("raw copy aliases source slice")
	}
}

func TestRawBlockBytesRemarshalsWhenWirePayloadMissing(t *testing.T) {
	block := testBufferedBlock(2)
	got := RawBlockBytes(block, nil)
	decoded, err := types.UnmarshalBlock(got)
	if err != nil {
		t.Fatalf("decode remarshal bytes: %v", err)
	}
	if decoded.Hash() != block.Hash() || decoded.Number() != block.Number() {
		t.Fatalf("decoded block = #%d %x, want #%d %x", decoded.Number(), decoded.Hash(), block.Number(), block.Hash())
	}
}

func TestBufferedBatchDecodeBlocksKeepsPrefixOnError(t *testing.T) {
	block1 := testBufferedBlock(1)
	block3 := testBufferedBlock(3)
	raw1, err := block1.Marshal()
	if err != nil {
		t.Fatalf("marshal block1: %v", err)
	}
	raw3, err := block3.Marshal()
	if err != nil {
		t.Fatalf("marshal block3: %v", err)
	}
	batch := BufferedBatch{Buffered: []BufferedBlock{
		{Raw: raw1, Hash: block1.Hash(), Num: block1.Number()},
		{Raw: []byte{0x01, 0x02}, Hash: tcommon.Hash{0xee}, Num: 2},
		{Raw: raw3, Hash: block3.Hash(), Num: block3.Number()},
	}}

	dropped, err := batch.DecodeBlocks()
	if err == nil {
		t.Fatal("DecodeBlocks succeeded, want decode error")
	}
	if dropped.Num != 2 || dropped.Hash != (tcommon.Hash{0xee}) {
		t.Fatalf("dropped = #%d %x, want #2 ee", dropped.Num, dropped.Hash)
	}
	if len(batch.Blocks) != 1 || batch.Blocks[0].Hash() != block1.Hash() {
		t.Fatalf("decoded prefix = %d blocks, want only block1", len(batch.Blocks))
	}
}

func TestDecodeBufferedBatchAction(t *testing.T) {
	block1 := testBufferedBlock(1)
	block2 := testBufferedBlock(2)
	raw1, err := block1.Marshal()
	if err != nil {
		t.Fatalf("marshal block1: %v", err)
	}
	raw2, err := block2.Marshal()
	if err != nil {
		t.Fatalf("marshal block2: %v", err)
	}

	full := BufferedBatch{Buffered: []BufferedBlock{
		{Raw: raw1, Hash: block1.Hash(), Num: block1.Number()},
		{Raw: raw2, Hash: block2.Hash(), Num: block2.Number()},
	}}
	if got := DecodeBufferedBatch(&full); got.Action != BufferedBatchDecodeImport || got.Err != nil || len(full.Blocks) != 2 {
		t.Fatalf("full decode = %+v blocks=%d, want import without error", got, len(full.Blocks))
	}

	firstBad := BufferedBatch{Buffered: []BufferedBlock{
		{Raw: []byte{0x01}, Hash: tcommon.Hash{0xee}, Num: 1},
		{Raw: raw2, Hash: block2.Hash(), Num: block2.Number()},
	}}
	if got := DecodeBufferedBatch(&firstBad); got.Action != BufferedBatchDecodeContinue || got.Err == nil || len(firstBad.Blocks) != 0 || got.Dropped.Num != 1 {
		t.Fatalf("first-bad decode = %+v blocks=%d, want continue with dropped #1", got, len(firstBad.Blocks))
	}

	prefix := BufferedBatch{Buffered: []BufferedBlock{
		{Raw: raw1, Hash: block1.Hash(), Num: block1.Number()},
		{Raw: []byte{0x02}, Hash: tcommon.Hash{0xdd}, Num: 2},
	}}
	if got := DecodeBufferedBatch(&prefix); got.Action != BufferedBatchDecodeImport || got.Err == nil || len(prefix.Blocks) != 1 || got.Dropped.Num != 2 {
		t.Fatalf("prefix decode = %+v blocks=%d, want import prefix with dropped #2", got, len(prefix.Blocks))
	}
}

func TestSummarizeAppliedBatch(t *testing.T) {
	block1 := testBufferedBlock(1)
	block2 := testBufferedBlock(2)
	block2.Proto().Transactions = append(block2.Proto().Transactions, &corepb.Transaction{Signature: [][]byte{{0x02, 0x02}}})
	batch := BufferedBatch{
		Blocks: []*types.Block{block1, block2},
		Buffered: []BufferedBlock{
			{Hash: block1.Hash(), Num: block1.Number()},
			{Hash: block2.Hash(), Num: block2.Number()},
		},
	}

	got := SummarizeAppliedBatch(batch, 2)
	if !got.OK || got.Applied != 2 || got.TxCount != 3 || !got.HasStage {
		t.Fatalf("summary = %+v, want applied 2 txs 3 with stage", got)
	}
	if got.Last.Num != block2.Number() || got.Last.Hash != block2.Hash() {
		t.Fatalf("last = #%d %x, want block2 #%d %x", got.Last.Num, got.Last.Hash, block2.Number(), block2.Hash())
	}
}

func TestSummarizeAppliedBatchRejectsInvalidApplied(t *testing.T) {
	batch := BufferedBatch{Buffered: []BufferedBlock{{Num: 1}}}
	for _, applied := range []int{0, -1, 2} {
		if got := SummarizeAppliedBatch(batch, applied); got.OK {
			t.Fatalf("applied %d summary = %+v, want not ok", applied, got)
		}
	}
}

func TestSummarizeAppliedBatchCountsOnlyDecodedBlocks(t *testing.T) {
	block1 := testBufferedBlock(1)
	batch := BufferedBatch{
		Blocks: []*types.Block{block1},
		Buffered: []BufferedBlock{
			{Hash: block1.Hash(), Num: block1.Number()},
			{Hash: tcommon.Hash{0x02}, Num: 2},
		},
	}

	got := SummarizeAppliedBatch(batch, 2)
	if !got.OK || got.Applied != 2 || got.TxCount != 1 {
		t.Fatalf("summary = %+v, want applied 2 with tx count 1", got)
	}
	if got.Last.Num != 2 || !got.HasStage {
		t.Fatalf("last/stage = #%d %v, want block 2 stage", got.Last.Num, got.HasStage)
	}
}

func TestAppliedStagedBlockDeletes(t *testing.T) {
	first := BufferedBlock{Hash: tcommon.Hash{0x01}, Num: 1}
	second := BufferedBlock{Hash: tcommon.Hash{0x02}, Num: 2}
	third := BufferedBlock{Hash: tcommon.Hash{0x03}, Num: 3}

	got := AppliedStagedBlockDeletes(BufferedBatch{Buffered: []BufferedBlock{first, second, third}}, 2)
	if len(got) != 2 {
		t.Fatalf("deletes = %d, want 2", len(got))
	}
	if got[0].Number != 1 || got[0].Hash != first.Hash || got[1].Number != 2 || got[1].Hash != second.Hash {
		t.Fatalf("deletes = %+v, want first two buffered blocks", got)
	}
}

func TestAppliedStagedBlockDeletesClampsInvalidApplied(t *testing.T) {
	first := BufferedBlock{Hash: tcommon.Hash{0x01}, Num: 1}

	if got := AppliedStagedBlockDeletes(BufferedBatch{Buffered: []BufferedBlock{first}}, 0); got != nil {
		t.Fatalf("zero applied deletes = %+v, want nil", got)
	}
	got := AppliedStagedBlockDeletes(BufferedBatch{Buffered: []BufferedBlock{first}}, 3)
	if len(got) != 1 || got[0].Number != first.Num || got[0].Hash != first.Hash {
		t.Fatalf("clamped deletes = %+v, want first buffered block", got)
	}
}

func TestResolveImportFailureUsesRangeErrorIndex(t *testing.T) {
	first := BufferedBlock{Hash: tcommon.Hash{0x01}, Num: 1}
	second := BufferedBlock{Hash: tcommon.Hash{0x02}, Num: 2}
	err := &core.InsertBlocksError{Index: 1, BlockNumber: 2, Err: errors.New("bad block")}

	got := ResolveImportFailure(BufferedBatch{Buffered: []BufferedBlock{first, second}}, err)
	if !got.OK || got.Applied != 1 || got.FailedIndex != 1 || got.Failed.Num != 2 || got.FailedNum != 2 {
		t.Fatalf("resolution = %+v, want failed block2 with applied prefix 1", got)
	}
}

func TestResolveImportFailureFallsBackToFirstBlock(t *testing.T) {
	first := BufferedBlock{Hash: tcommon.Hash{0x01}, Num: 1}
	second := BufferedBlock{Hash: tcommon.Hash{0x02}, Num: 2}

	got := ResolveImportFailure(BufferedBatch{Buffered: []BufferedBlock{first, second}}, errors.New("plain insert failure"))
	if !got.OK || got.Applied != 0 || got.FailedIndex != 0 || got.Failed.Num != 1 || got.FailedNum != 1 {
		t.Fatalf("resolution = %+v, want failed first block", got)
	}
}

func TestResolveImportFailureFallsBackToRangeBlockNumber(t *testing.T) {
	err := &core.InsertBlocksError{Index: 0, BlockNumber: 99, Err: errors.New("bad block")}

	got := ResolveImportFailure(BufferedBatch{Buffered: []BufferedBlock{{Hash: tcommon.Hash{0x01}}}}, err)
	if !got.OK || got.FailedNum != 99 {
		t.Fatalf("resolution = %+v, want fallback block number 99", got)
	}
}

func TestResolveImportFailureRejectsNilOrEmpty(t *testing.T) {
	if got := ResolveImportFailure(BufferedBatch{}, errors.New("bad block")); got.OK {
		t.Fatalf("empty batch resolution = %+v, want not ok", got)
	}
	if got := ResolveImportFailure(BufferedBatch{Buffered: []BufferedBlock{{Num: 1}}}, nil); got.OK {
		t.Fatalf("nil error resolution = %+v, want not ok", got)
	}
}

func TestPlanImportOutcome(t *testing.T) {
	first := BufferedBlock{Hash: tcommon.Hash{0x01}, Num: 1}
	second := BufferedBlock{Hash: tcommon.Hash{0x02}, Num: 2}
	batch := BufferedBatch{
		Blocks:   []*types.Block{testBufferedBlock(1), testBufferedBlock(2)},
		Buffered: []BufferedBlock{first, second},
	}

	ok := PlanImportOutcome(batch, nil)
	if ok.Applied != 2 || !ok.RecordApplied || ok.Pause || ok.StopDrain {
		t.Fatalf("success outcome = %+v, want record full batch without pause", ok)
	}

	rangeErr := &core.InsertBlocksError{Index: 1, BlockNumber: 2, Err: errors.New("bad block")}
	partial := PlanImportOutcome(batch, rangeErr)
	if partial.Applied != 1 || !partial.RecordApplied || !partial.Pause || !partial.StopDrain || partial.PauseNum != 2 {
		t.Fatalf("partial outcome = %+v, want record prefix and pause at block2", partial)
	}

	firstErr := PlanImportOutcome(batch, errors.New("plain insert failure"))
	if firstErr.Applied != 0 || firstErr.RecordApplied || !firstErr.Pause || !firstErr.StopDrain || firstErr.PauseNum != 1 {
		t.Fatalf("first failure outcome = %+v, want pause at block1 without record", firstErr)
	}

	unmapped := PlanImportOutcome(BufferedBatch{}, errors.New("bad block"))
	if unmapped.RecordApplied || !unmapped.Pause || !unmapped.StopDrain || unmapped.PauseNum != 0 {
		t.Fatalf("unmapped outcome = %+v, want generic pause", unmapped)
	}
}

func TestStagedBodyDrainLimit(t *testing.T) {
	tests := []struct {
		name          string
		next          uint64
		max           int
		readyLimit    uint64
		hasReadyLimit bool
		wantLimit     int
		wantOK        bool
	}{
		{name: "no ready frontier uses max", next: 10, max: 32, wantLimit: 32, wantOK: true},
		{name: "ready frontier behind next stops drain", next: 10, max: 32, readyLimit: 9, hasReadyLimit: true},
		{name: "ready frontier clamps partial span", next: 10, max: 32, readyLimit: 12, hasReadyLimit: true, wantLimit: 3, wantOK: true},
		{name: "ready frontier beyond max keeps max", next: 10, max: 32, readyLimit: 99, hasReadyLimit: true, wantLimit: 32, wantOK: true},
		{name: "nonpositive max stops drain", next: 10, max: 0},
	}
	for _, tt := range tests {
		got, ok := StagedBodyDrainLimit(tt.next, tt.max, tt.readyLimit, tt.hasReadyLimit)
		if got != tt.wantLimit || ok != tt.wantOK {
			t.Fatalf("%s: limit=%d ok=%v, want %d %v", tt.name, got, ok, tt.wantLimit, tt.wantOK)
		}
	}
}

func TestPlanStagedBodyDrain(t *testing.T) {
	tests := []struct {
		name  string
		next  uint64
		max   int
		ready StagedBodyReadyLimit
		want  StagedBodyDrainPlan
	}{
		{
			name:  "missing ready uses max",
			next:  10,
			max:   32,
			ready: StagedBodyReadyLimit{Status: StagedBodyReadyLimitMissing},
			want:  StagedBodyDrainPlan{RestoreLimit: 32, CanDrain: true},
		},
		{
			name:  "valid ready clamps chunk",
			next:  10,
			max:   32,
			ready: StagedBodyReadyLimit{Status: StagedBodyReadyLimitValid, Limit: 12},
			want:  StagedBodyDrainPlan{RestoreLimit: 3, CanDrain: true, ReadyLimit: 12, HasReadyLimit: true},
		},
		{
			name:  "valid ready beyond max keeps max",
			next:  10,
			max:   32,
			ready: StagedBodyReadyLimit{Status: StagedBodyReadyLimitValid, Limit: 99},
			want:  StagedBodyDrainPlan{RestoreLimit: 32, CanDrain: true, ReadyLimit: 99, HasReadyLimit: true},
		},
		{
			name:  "stale ready requests refresh and uses max",
			next:  10,
			max:   32,
			ready: StagedBodyReadyLimit{Status: StagedBodyReadyLimitStale, Limit: 9},
			want:  StagedBodyDrainPlan{RestoreLimit: 32, CanDrain: true, RefreshReady: true},
		},
		{
			name:  "invalid ready still uses max",
			next:  10,
			max:   32,
			ready: StagedBodyReadyLimit{Status: StagedBodyReadyLimitHashMismatch, Limit: 12},
			want:  StagedBodyDrainPlan{RestoreLimit: 32, CanDrain: true},
		},
		{
			name:  "nonpositive max stops drain",
			next:  10,
			max:   0,
			ready: StagedBodyReadyLimit{Status: StagedBodyReadyLimitValid, Limit: 12},
			want:  StagedBodyDrainPlan{ReadyLimit: 12, HasReadyLimit: true},
		},
	}
	for _, tt := range tests {
		if got := PlanStagedBodyDrain(tt.next, tt.max, tt.ready); got != tt.want {
			t.Fatalf("%s: plan = %+v, want %+v", tt.name, got, tt.want)
		}
	}
}

func TestPopBufferedBatchReleasesReservationsAndKeepsGap(t *testing.T) {
	now := time.Unix(100, 0)
	waitStart := now.Add(-3 * time.Second)
	var wait BufferWaitTracker
	wait.Begin(4, waitStart)

	h4 := tcommon.Hash{0x04}
	h5 := tcommon.Hash{0x05}
	h7 := tcommon.Hash{0x07}
	buffer := map[uint64]BufferedBlock{
		4: {Num: 4, Hash: h4},
		5: {Num: 5, Hash: h5},
		7: {Num: 7, Hash: h7},
	}
	bufferedHashes := map[tcommon.Hash]struct{}{
		h4: {},
		h5: {},
		h7: {},
	}
	path := BlockPath{
		4: h4,
		5: h5,
		7: h7,
	}

	batch := PopBufferedBatch(buffer, bufferedHashes, path, &wait, 4, 4, now)
	if len(batch.Buffered) != 2 {
		t.Fatalf("popped %d blocks, want 2", len(batch.Buffered))
	}
	if batch.Buffered[0].Num != 4 || batch.Buffered[1].Num != 5 {
		t.Fatalf("popped nums = %d,%d; want 4,5", batch.Buffered[0].Num, batch.Buffered[1].Num)
	}
	if len(batch.BufferWaits) != 2 || batch.BufferWaits[0] != 3*time.Second || batch.BufferWaits[1] != 0 {
		t.Fatalf("waits = %v, want [3s 0s]", batch.BufferWaits)
	}
	if _, ok := buffer[4]; ok {
		t.Fatal("block 4 still buffered")
	}
	if _, ok := buffer[5]; ok {
		t.Fatal("block 5 still buffered")
	}
	if _, ok := buffer[7]; !ok {
		t.Fatal("gap tail block 7 was removed")
	}
	if _, ok := bufferedHashes[h4]; ok {
		t.Fatal("hash 4 still reserved")
	}
	if _, ok := bufferedHashes[h5]; ok {
		t.Fatal("hash 5 still reserved")
	}
	if _, ok := bufferedHashes[h7]; !ok {
		t.Fatal("gap tail hash 7 was removed")
	}
	if _, ok := path[4]; ok {
		t.Fatal("path reservation 4 still present")
	}
	if _, ok := path[5]; ok {
		t.Fatal("path reservation 5 still present")
	}
	if _, ok := path[7]; !ok {
		t.Fatal("gap tail path reservation 7 was removed")
	}
}

func testBufferedBlock(num int64) *types.Block {
	return types.NewBlockFromPB(&corepb.Block{
		BlockHeader: &corepb.BlockHeader{
			RawData:          &corepb.BlockHeaderRaw{Number: num, Timestamp: num * 3000},
			WitnessSignature: make([]byte, 65),
		},
		Transactions: []*corepb.Transaction{
			{Signature: [][]byte{{byte(num)}}},
		},
	})
}
