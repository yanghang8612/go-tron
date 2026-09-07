package snapshots

import (
	"bytes"
	"encoding/binary"
	"hash/crc32"
	"testing"

	"github.com/klauspost/compress/zstd"
	"github.com/tronprotocol/go-tron/internal/historychunk"
)

func cdcOneAnchorFixture(frame []byte, logical uint64) []byte {
	out := make([]byte, compressedBlockHeaderSize)
	copy(out[:8], compressedBlockMagic)
	binary.BigEndian.PutUint32(out[8:12], compressedBlockCDCVersion)
	binary.BigEndian.PutUint32(out[12:16], historychunk.MaxSize)
	out = append(out, frame...)
	tableOff := len(out)
	var entry [cdcEntrySize]byte
	putCDCEntry(entry[:], cdcEntry{physical: 48, stored: uint64(len(frame)), anchor: cdcAnchor})
	out = append(out, entry[:]...)
	out = binary.BigEndian.AppendUint32(out, crc32.ChecksumIEEE(entry[:]))
	sparse := make([]byte, 8)
	out = append(out, sparse...)
	out = binary.BigEndian.AppendUint32(out, crc32.ChecksumIEEE(sparse))
	var footer [cdcFooterSize]byte
	copy(footer[:8], compressedBlockCDCEndMagic)
	binary.BigEndian.PutUint64(footer[8:16], 1)
	binary.BigEndian.PutUint64(footer[16:24], logical)
	binary.BigEndian.PutUint64(footer[24:32], uint64(tableOff))
	binary.BigEndian.PutUint64(footer[32:40], cdcEntrySize+4)
	binary.BigEndian.PutUint32(footer[40:44], crc32.ChecksumIEEE(footer[:40]))
	return append(out, footer[:]...)
}

func TestCDCDecoderRejectsKnownAndUnknownFCSBombs(t *testing.T) {
	raw := bytes.Repeat([]byte{7}, 1<<20)
	for _, streaming := range []bool{false, true} {
		var dst bytes.Buffer
		enc, err := zstd.NewWriter(&dst, zstd.WithEncoderConcurrency(1), zstd.WithWindowSize(historychunk.MaxSize), zstd.WithSingleSegment(false))
		if err != nil {
			t.Fatal(err)
		}
		var frame []byte
		if streaming {
			if _, err := enc.Write(raw); err != nil {
				t.Fatal(err)
			}
			if err := enc.Close(); err != nil {
				t.Fatal(err)
			}
			frame = dst.Bytes()
		} else {
			frame = enc.EncodeAll(raw, nil)
			enc.Close()
		}
		var h zstd.Header
		if err := h.Decode(frame); err != nil {
			t.Fatal(err)
		}
		if h.HasFCS == streaming || len(frame) > cdcMaxEncodedChunk {
			t.Fatalf("bad bomb fixture: streaming=%v header=%+v len=%d", streaming, h, len(frame))
		}
		// Metadata claims a 17-byte chunk. Reused cache storage is deliberately
		// much larger: the decoder must receive capacity 17, not 128KiB.
		data := cdcOneAnchorFixture(frame, 17)
		r, err := openCDCReader(bytes.NewReader(data), uint64(len(data)), data[:48])
		if err != nil {
			t.Fatal(err)
		}
		r.cache = []cdcDecodedPage{{anchor: 99, bytes: make([]byte, historychunk.MaxSize)}, {anchor: 98, bytes: make([]byte, historychunk.MaxSize)}}
		if _, err := r.ReadAt(make([]byte, 17), 0); err == nil {
			t.Fatalf("streaming=%v bomb decoded", streaming)
		}
		for _, cached := range r.cache {
			if cached.anchor == 98 {
				t.Fatal("recycled buffer still cached after failed decode")
			}
		}
	}
}

func TestCDCMetadataRejectsHugeSparseAllocationBeforeRead(t *testing.T) {
	data := cdcOneAnchorFixture([]byte{1}, 1)
	footer := data[len(data)-cdcFooterSize:]
	binary.BigEndian.PutUint64(footer[8:16], uint64(cdcMaxSparseBytes/8+1)*cdcEntriesPerPage)
	binary.BigEndian.PutUint64(footer[16:24], uint64(1)<<50)
	binary.BigEndian.PutUint32(footer[40:44], crc32.ChecksumIEEE(footer[:40]))
	source := &cdcCountReader{src: bytes.NewReader(data)}
	if _, err := openCDCReader(source, uint64(len(data)), data[:48]); err == nil {
		t.Fatal("oversized sparse metadata accepted")
	}
	if source.requests != 1 || source.bytes != cdcFooterSize {
		t.Fatalf("unexpected read before cap rejection: %+v", source)
	}
}
