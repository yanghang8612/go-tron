package rawdb

import (
	"bytes"
	"testing"

	"github.com/tronprotocol/go-tron/common"
)

type stageProgressOwnedWriterProbe struct {
	putCalled bool
	key       string
	value     []byte
}

func (p *stageProgressOwnedWriterProbe) Put([]byte, []byte) error {
	p.putCalled = true
	return nil
}

func (*stageProgressOwnedWriterProbe) Delete([]byte) error { return nil }

func (p *stageProgressOwnedWriterProbe) PutStringOwnedValue(key string, value []byte) error {
	p.key = key
	p.value = value
	return nil
}

func TestStageProgressReadWriteIterateDelete(t *testing.T) {
	db := NewMemoryDatabase()
	if _, ok, err := ReadStageProgress(db, StageExecution); err != nil || ok {
		t.Fatalf("empty stage progress ok=%v err=%v", ok, err)
	}
	if err := WriteStageProgress(db, StageExecution, 42); err != nil {
		t.Fatalf("write execution progress: %v", err)
	}
	if err := WriteStageProgress(db, StageCommitment, 41); err != nil {
		t.Fatalf("write commitment progress: %v", err)
	}
	executionHash := common.Hash{0x2a}
	if err := WriteStageProgressWithHash(db, StageExecution, 42, executionHash); err != nil {
		t.Fatalf("write hash-bound execution progress: %v", err)
	}
	if got, ok, err := ReadStageProgress(db, StageExecution); err != nil || !ok || got != 42 {
		t.Fatalf("read execution progress = %d ok=%v err=%v", got, ok, err)
	}
	if row, ok, err := ReadStageProgressRow(db, StageExecution); err != nil || !ok || row.BlockNum != 42 || !row.HasBlockHash || row.BlockHash != executionHash {
		t.Fatalf("read execution progress row = %+v ok=%v err=%v, want hash-bound 42", row, ok, err)
	}
	var got []StageProgress
	if err := IterateStageProgress(db, func(progress StageProgress) (bool, error) {
		got = append(got, progress)
		return true, nil
	}); err != nil {
		t.Fatalf("iterate stage progress: %v", err)
	}
	if len(got) != 2 || got[0].Stage != StageCommitment || got[0].BlockNum != 41 || got[0].HasBlockHash ||
		got[1].Stage != StageExecution || got[1].BlockNum != 42 || !got[1].HasBlockHash || got[1].BlockHash != executionHash {
		t.Fatalf("stage progress rows = %+v", got)
	}
	if err := DeleteStageProgress(db, StageExecution); err != nil {
		t.Fatalf("delete execution progress: %v", err)
	}
	if _, ok, err := ReadStageProgress(db, StageExecution); err != nil || ok {
		t.Fatalf("deleted stage progress ok=%v err=%v", ok, err)
	}
}

func TestCanonicalStageProgressWriteAndRewind(t *testing.T) {
	db := NewMemoryDatabase()
	hash12 := common.Hash{0x12}
	if err := WriteCanonicalStageProgressWithHash(db, 12, hash12); err != nil {
		t.Fatalf("write canonical progress: %v", err)
	}
	for _, stage := range CanonicalExecutionStages() {
		if row, ok, err := ReadStageProgressRow(db, stage); err != nil || !ok || row.BlockNum != 12 || !row.HasBlockHash || row.BlockHash != hash12 {
			t.Fatalf("%s progress after write = %+v ok=%v err=%v, want 12 hash", stage, row, ok, err)
		}
	}
	hash7 := common.Hash{0x07}
	if err := RewindCanonicalStageProgressWithHash(db, 7, hash7); err != nil {
		t.Fatalf("rewind canonical progress: %v", err)
	}
	for _, stage := range CanonicalExecutionStages() {
		if row, ok, err := ReadStageProgressRow(db, stage); err != nil || !ok || row.BlockNum != 7 || !row.HasBlockHash || row.BlockHash != hash7 {
			t.Fatalf("%s progress after rewind = %+v ok=%v err=%v, want 7 hash", stage, row, ok, err)
		}
	}
}

func TestWriteStageProgressTransfersCanonicalKeyAndValue(t *testing.T) {
	probe := new(stageProgressOwnedWriterProbe)
	hash := common.Hash{0x42}
	if err := WriteStageProgressWithHash(probe, StageExecution, 123, hash); err != nil {
		t.Fatal(err)
	}
	if probe.putCalled {
		t.Fatal("stage progress used defensive Put instead of owned string write")
	}
	if probe.key != stageExecutionProgressKeyString {
		t.Fatalf("stage key = %q, want %q", probe.key, stageExecutionProgressKeyString)
	}
	want := encodeStageProgress(123, hash, true)
	if !bytes.Equal(probe.value, want) {
		t.Fatalf("stage value = %x, want %x", probe.value, want)
	}
}

var benchmarkStageProgressKey []byte

func BenchmarkCanonicalStageProgressKey(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		benchmarkStageProgressKey = stageProgressKey(StageExecution)
	}
}

func BenchmarkWriteCanonicalStageProgressWithHash(b *testing.B) {
	probe := new(stageProgressOwnedWriterProbe)
	hash := common.Hash{0x42}
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if err := WriteStageProgressWithHash(probe, StageExecution, uint64(i), hash); err != nil {
			b.Fatal(err)
		}
	}
}
