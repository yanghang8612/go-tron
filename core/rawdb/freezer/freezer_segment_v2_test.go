package freezer

import (
	"bytes"
	"container/list"
	"encoding/binary"
	"os"
	"path/filepath"
	"testing"
)

func TestV2SegmentRoundTripAndChecksum(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "bodies", v2SegmentName(100, 150))
	records := make(map[uint64][]byte)
	for number := uint64(100); number < 250; number++ {
		var suffix [8]byte
		binary.BigEndian.PutUint64(suffix[:], number)
		records[number] = append(bytes.Repeat([]byte("repeated-v2-record"), int(number%11)), suffix[:]...)
	}
	read := func(number uint64) ([]byte, error) { return records[number], nil }
	if err := writeV2Segment(path, 100, 150, 16, read); err != nil {
		t.Fatalf("writeV2Segment: %v", err)
	}
	reader, err := openV2Segment(path, "bodies")
	if err != nil {
		t.Fatalf("openV2Segment: %v", err)
	}
	store := newTestV2Store(t, "bodies", reader)
	for number := uint64(100); number < 250; number++ {
		got, err := store.read("bodies", number)
		if err != nil {
			t.Fatalf("read %d: %v", number, err)
		}
		if !bytes.Equal(got, records[number]) {
			t.Fatalf("record %d mismatch", number)
		}
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	reader, err = openV2Segment(path, "bodies")
	if err != nil {
		t.Fatal(err)
	}
	first := reader.frames[0]
	if err := reader.Close(); err != nil {
		t.Fatal(err)
	}
	file, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	one := []byte{0}
	if _, err := file.ReadAt(one, int64(first.compressedStart)); err != nil {
		file.Close()
		t.Fatal(err)
	}
	one[0] ^= 0xff
	if _, err := file.WriteAt(one, int64(first.compressedStart)); err != nil {
		file.Close()
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	reader, err = openV2Segment(path, "bodies")
	if err != nil {
		t.Fatal(err)
	}
	store = newTestV2Store(t, "bodies", reader)
	if _, err := store.read("bodies", 100); err == nil {
		t.Fatal("corrupt frame passed checksum validation")
	}
	_ = store.Close()
}

func TestFreezerMigrateV2RoundTripResumeAndAppend(t *testing.T) {
	dir := t.TempDir()
	tables := map[string]TableConfig{
		"bodies":      {Prunable: true},
		"tx_infos":    {Prunable: true},
		"state_roots": {NoSnappy: true},
	}
	want := make(map[string][][]byte)
	for kind := range tables {
		want[kind] = make([][]byte, 300)
	}
	freezer, err := NewFreezer(dir, "", false, 2049, tables)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := freezer.ModifyAncients(func(op AncientWriteOp) error {
		for number := uint64(0); number < 300; number++ {
			var suffix [8]byte
			binary.BigEndian.PutUint64(suffix[:], number)
			for _, kind := range []string{"bodies", "tx_infos", "state_roots"} {
				value := append(bytes.Repeat([]byte(kind+"-repeated-"), 20+int(number%7)), suffix[:]...)
				want[kind][number] = append([]byte(nil), value...)
				if err := op.AppendRaw(kind, number, value); err != nil {
					return err
				}
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := freezer.Sync(); err != nil {
		t.Fatal(err)
	}
	result, err := freezer.MigrateV2(V2MigrationOptions{
		Tables:        []string{"bodies", "tx_infos"},
		SegmentBlocks: 64,
		FrameBlocks:   8,
		MaxSegments:   2,
	})
	if err != nil {
		t.Fatalf("first migration: %v", err)
	}
	if result.End != 128 || result.Segments != 2 || freezer.v2.coverage != 128 {
		t.Fatalf("first result = %+v, coverage=%d", result, freezer.v2.coverage)
	}
	rangeBodies, err := freezer.AncientRange("bodies", 120, 16, 0)
	if err != nil || len(rangeBodies) != 16 {
		t.Fatalf("cross-tier range len=%d err=%v", len(rangeBodies), err)
	}
	for i, body := range rangeBodies {
		if !bytes.Equal(body, want["bodies"][120+i]) {
			t.Fatalf("cross-tier range item %d mismatch", i)
		}
	}
	rangeRoots, err := freezer.AncientRange("state_roots", 0, 16, 0)
	if err != nil || len(rangeRoots) != 16 {
		t.Fatalf("V1-only range len=%d err=%v", len(rangeRoots), err)
	}
	for _, kind := range []string{"bodies", "tx_infos", "state_roots"} {
		for number := uint64(0); number < 300; number++ {
			got, err := freezer.Ancient(kind, number)
			if err != nil {
				t.Fatalf("read %s[%d]: %v", kind, number, err)
			}
			if !bytes.Equal(got, want[kind][number]) {
				t.Fatalf("read %s[%d] mismatch", kind, number)
			}
		}
	}
	if err := freezer.Close(); err != nil {
		t.Fatal(err)
	}

	freezer, err = NewFreezer(dir, "", false, 2049, tables)
	if err != nil {
		t.Fatalf("reopen mixed freezer: %v", err)
	}
	result, err = freezer.MigrateV2(V2MigrationOptions{
		Tables:        []string{"bodies", "tx_infos"},
		SegmentBlocks: 64,
		FrameBlocks:   8,
	})
	if err != nil {
		t.Fatalf("resume migration: %v", err)
	}
	if result.End != 256 || result.Segments != 2 || freezer.v2.coverage != 256 {
		t.Fatalf("resume result = %+v, coverage=%d", result, freezer.v2.coverage)
	}
	if _, err := freezer.ModifyAncients(func(op AncientWriteOp) error {
		for kind := range tables {
			if err := op.AppendRaw(kind, 300, []byte("appended-"+kind)); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatalf("append after migration: %v", err)
	}
	for _, kind := range []string{"bodies", "tx_infos"} {
		got, err := freezer.Ancient(kind, 300)
		if err != nil || string(got) != "appended-"+kind {
			t.Fatalf("appended %s = %q, err=%v", kind, got, err)
		}
	}
	if err := freezer.Close(); err != nil {
		t.Fatal(err)
	}
}

func newTestV2Store(t *testing.T, kind string, reader *v2SegmentReader) *v2Store {
	t.Helper()
	decoder, err := newV2Decoder()
	if err != nil {
		t.Fatal(err)
	}
	return &v2Store{
		segments:   map[string][]*v2SegmentReader{kind: {reader}},
		decoder:    decoder,
		cacheList:  list.New(),
		cacheItems: make(map[v2FrameKey]*list.Element),
	}
}
