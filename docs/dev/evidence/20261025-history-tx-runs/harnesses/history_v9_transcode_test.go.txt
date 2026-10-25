package snapshots

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"reflect"
	"testing"
)

type accessorV9TranscodeAudit struct {
	Keys, Records, Singletons, MultiLists, MultiRows            uint64
	SourceBytes, CandidateBytes, RemovedOffsetBytes             uint64
	LocatorDataBytes, LocatorDirectoryBytes, LocatorFooterBytes uint64
	SourcePostingBytes, CandidatePostingBytes, SparseBytes      uint64
	DictionaryDigest                                            [32]byte
}

// Bounded offline experiment helper only; never run on node startup.
func transcodeHistoryAccessorV9ForTest(data []byte) ([]byte, accessorV9TranscodeAudit, error) {
	var audit accessorV9TranscodeAudit
	if len(data) > 128<<20 {
		return nil, audit, fmt.Errorf("offline V9 input exceeds 128 MiB")
	}
	r := bytes.NewReader(data)
	h, err := decodeStateDomainChangeBinaryAccessorKeyHeader(r, uint64(len(data)))
	if err != nil {
		return nil, audit, err
	}
	if h.version != 8 || h.recordCount > 16<<20 {
		return nil, audit, fmt.Errorf("offline V9 requires bounded V8 input")
	}
	blocks, err := stateDomainChangeBinaryAccessorV6ReadBlockDirectory(r, h)
	if err != nil {
		return nil, audit, err
	}
	offsets := make([]uint64, h.recordCount)
	seen := make([]byte, (h.recordCount+7)/8)
	var keyData, posts bytes.Buffer
	var targetBlocks []stateDomainChangeBinaryAccessorV6Block
	largest := 0
	var expectedSourceOff uint64
	for bi, b := range blocks {
		records, err := stateDomainChangeBinaryAccessorV6ReadBlock(r, uint64(len(data)), h, b, uint32(bi*128))
		if err != nil {
			return nil, audit, err
		}
		keys := make([]stateDomainChangeBinaryAccessorV6Key, len(records))
		for i, record := range records {
			k := stateDomainChangeBinaryAccessorV6Key{key: record.key, keyID: record.keyID, postingCount: record.postings, postingOff: uint64(posts.Len()), inlinePosting: record.inlinePosting}
			frames, err := stateDomainChangeBinaryAccessorV7PostingFrames(r, h, record)
			if err != nil {
				return nil, audit, err
			}
			rows := make([]stateDomainChangeBinaryAccessorV6Posting, 0, record.postings)
			for _, frame := range frames {
				ps, err := stateDomainChangeBinaryAccessorV7ReadFrame(r, h, record, frame)
				if err != nil {
					return nil, audit, err
				}
				rows = append(rows, ps...)
			}
			if len(rows) != int(record.postings) {
				return nil, audit, fmt.Errorf("source posting count mismatch")
			}
			for _, row := range rows {
				id := uint64(row.recordIndex)
				if id >= h.recordCount || seen[id/8]&(1<<(id%8)) != 0 {
					return nil, audit, fmt.Errorf("source ordinal duplicate/out of bounds")
				}
				offsets[id] = row.offset
				seen[id/8] |= 1 << (id % 8)
				audit.Records++
			}
			audit.Keys++
			if record.inline {
				if record.postings != 1 {
					return nil, audit, fmt.Errorf("bad source singleton")
				}
				audit.Singletons++
			} else {
				if record.postingOff != expectedSourceOff {
					return nil, audit, fmt.Errorf("noncontiguous source postings")
				}
				original, err := stateDomainChangeBinaryAccessorV7PostingListLength(r, h, record)
				if err != nil {
					return nil, audit, err
				}
				expectedSourceOff += original
				encoded, err := encodeStateDomainChangeBinaryAccessorPostingListVersion(h.fromTxNum, rows, 9)
				if err != nil {
					return nil, audit, err
				}
				if uint64(len(encoded)) >= original {
					return nil, audit, fmt.Errorf("V9 multi frame did not omit offsets")
				}
				posts.Write(encoded)
				audit.RemovedOffsetBytes += original - uint64(len(encoded))
				audit.MultiLists++
				audit.MultiRows += uint64(len(rows))
			}
			keys[i] = k
		}
		raw, err := encodeStateDomainChangeBinaryAccessorV8KeyBlock(keys, h.fromTxNum)
		if err != nil {
			return nil, audit, err
		}
		stored, flags, err := compressHistoryDictionaryKeyBlock(raw, &largest)
		if err != nil {
			return nil, audit, err
		}
		targetBlocks = append(targetBlocks, stateDomainChangeBinaryAccessorV6Block{firstKey: append([]byte(nil), keys[0].key...), dataOff: uint64(keyData.Len()), dataLen: flags, rawLen: uint32(len(raw)), checksum: crc32.ChecksumIEEE(raw), keyCount: uint32(len(keys))})
		keyData.Write(stored)
	}
	if audit.Keys != h.keyCount || audit.Records != h.recordCount || expectedSourceOff != h.postingLen {
		return nil, audit, fmt.Errorf("source coverage/count mismatch")
	}
	for i, off := range offsets {
		if i > 0 && off <= offsets[i-1] {
			return nil, audit, fmt.Errorf("source ordinal offsets not ordered")
		}
	}
	sparse := encodeHistoryAccessorSparseDirectory(targetBlocks)
	dirLen := uint64(len(targetBlocks))*8 + uint64(len(sparse))
	for _, b := range targetBlocks {
		dirLen += 2 + uint64(len(b.firstKey)) + stateDomainChangeBinaryAccessorV6BlockTail
	}
	out := bytes.NewBuffer(make([]byte, 0, 120+int(dirLen)+keyData.Len()+posts.Len()))
	header := append([]byte(nil), data[:120]...)
	binary.BigEndian.PutUint32(header[8:12], 9)
	binary.BigEndian.PutUint64(header[56:64], dirLen)
	binary.BigEndian.PutUint64(header[64:72], uint64(keyData.Len()))
	binary.BigEndian.PutUint64(header[104:112], uint64(posts.Len()))
	binary.BigEndian.PutUint32(header[116:120], uint32(len(sparse)))
	binary.BigEndian.PutUint32(header[112:116], stateDomainChangeAccessorHeaderCRC(header, 9))
	out.Write(header)
	off := uint64(120 + len(targetBlocks)*8)
	for _, b := range targetBlocks {
		binary.Write(out, binary.BigEndian, off)
		off += 2 + uint64(len(b.firstKey)) + stateDomainChangeBinaryAccessorV6BlockTail
	}
	keyBase := uint64(120) + dirLen
	for _, b := range targetBlocks {
		binary.Write(out, binary.BigEndian, uint16(len(b.firstKey)))
		out.Write(b.firstKey)
		var tail [28]byte
		binary.BigEndian.PutUint64(tail[:8], keyBase+b.dataOff)
		binary.BigEndian.PutUint32(tail[8:12], b.dataLen)
		binary.BigEndian.PutUint32(tail[12:16], b.rawLen)
		binary.BigEndian.PutUint32(tail[16:20], b.checksum)
		binary.BigEndian.PutUint32(tail[20:24], b.keyCount)
		binary.BigEndian.PutUint32(tail[24:28], crc32.Update(crc32.ChecksumIEEE(b.firstKey), crc32.IEEETable, tail[:24]))
		out.Write(tail[:])
	}
	out.Write(sparse)
	out.Write(keyData.Bytes())
	out.Write(posts.Bytes())

	// Serialize exactly the production page format without disk scratch in this
	// bounded test transform; byte equality with the streaming builder is tested.
	var locatorData, locatorDirectory bytes.Buffer
	for start := 0; start < len(offsets); start += 128 {
		end := min(start+128, len(offsets))
		raw := make([]byte, 6, historyOffsetPageMaxBytes)
		if err := putStateDomainChangeBinaryAccessorV5Offset(raw[:6], offsets[start]); err != nil {
			return nil, audit, err
		}
		for i := start + 1; i < end; i++ {
			raw = binary.AppendUvarint(raw, offsets[i]-offsets[i-1])
		}
		var entry [12]byte
		if err := putStateDomainChangeBinaryAccessorV5Offset(entry[:6], uint64(locatorData.Len())); err != nil {
			return nil, audit, err
		}
		binary.BigEndian.PutUint16(entry[6:8], uint16(len(raw)))
		binary.BigEndian.PutUint32(entry[8:], historyOffsetCRC(uint64(start/128), entry[:], raw))
		locatorData.Write(raw)
		locatorDirectory.Write(entry[:])
	}
	out.Write(locatorData.Bytes())
	out.Write(locatorDirectory.Bytes())
	var footer [32]byte
	copy(footer[:8], historyOffsetMagic[:])
	binary.BigEndian.PutUint64(footer[8:16], h.recordCount)
	binary.BigEndian.PutUint64(footer[16:24], uint64(locatorData.Len()))
	binary.BigEndian.PutUint32(footer[24:28], 128)
	binary.BigEndian.PutUint32(footer[28:], crc32.ChecksumIEEE(footer[:28]))
	out.Write(footer[:])
	candidate := out.Bytes()
	nr := bytes.NewReader(candidate)
	nh, err := decodeStateDomainChangeBinaryAccessorKeyHeader(nr, uint64(len(candidate)))
	if err != nil {
		return nil, audit, err
	}
	if nh.dictionaryDigest != h.dictionaryDigest {
		return nil, audit, fmt.Errorf("dictionary identity changed")
	}
	if err := checkHistoryAccessorOffsets(nr, nh); err != nil {
		return nil, audit, err
	}
	resolver := newHistoryAccessorOffsetResolver(nr, nh)
	for id, want := range offsets {
		got, err := resolver.Offset(uint32(id))
		if err != nil || got != want {
			return nil, audit, fmt.Errorf("locator differs at ordinal %d: %v", id, err)
		}
	}
	nb, err := stateDomainChangeBinaryAccessorV6ReadBlockDirectory(nr, nh)
	if err != nil {
		return nil, audit, err
	}
	for bi, b := range blocks {
		source, err := stateDomainChangeBinaryAccessorV6ReadBlock(r, uint64(len(data)), h, b, uint32(bi*128))
		if err != nil {
			return nil, audit, err
		}
		target, err := stateDomainChangeBinaryAccessorV6ReadBlock(nr, uint64(len(candidate)), nh, nb[bi], uint32(bi*128))
		if err != nil {
			return nil, audit, err
		}
		for i, a := range source {
			z := target[i]
			if !bytes.Equal(a.key, z.key) || a.keyID != z.keyID || a.postings != z.postings || a.inline != z.inline || a.inline && a.inlinePosting != z.inlinePosting {
				return nil, audit, fmt.Errorf("key/inline identity changed")
			}
			af, err := stateDomainChangeBinaryAccessorV7PostingFrames(r, h, a)
			if err != nil {
				return nil, audit, err
			}
			zf, err := stateDomainChangeBinaryAccessorV7PostingFrames(nr, nh, z)
			if err != nil || len(af) != len(zf) {
				return nil, audit, fmt.Errorf("frame count changed")
			}
			for j, f := range af {
				before, err := stateDomainChangeBinaryAccessorV7ReadFrame(r, h, a, f)
				if err != nil {
					return nil, audit, err
				}
				after, _, err := readHistoryAccessorRawFrame(nr, nh, z, zf[j])
				if err != nil || len(after) != len(before) {
					return nil, audit, fmt.Errorf("frame changed: %v", err)
				}
				for k, v := range after {
					if !z.inline {
						if uint64(v.recordIndex) >= h.recordCount {
							return nil, audit, fmt.Errorf("target ordinal out of range")
						}
						v.offset = offsets[v.recordIndex]
					}
					if v != before[k] {
						return nil, audit, fmt.Errorf("posting tuple changed")
					}
				}
			}
		}
	}
	audit.SourceBytes = uint64(len(data))
	audit.CandidateBytes = uint64(len(candidate))
	audit.LocatorDataBytes = uint64(locatorData.Len())
	audit.LocatorDirectoryBytes = uint64(locatorDirectory.Len())
	audit.LocatorFooterBytes = 32
	audit.SourcePostingBytes = h.postingLen
	audit.CandidatePostingBytes = nh.postingLen
	audit.SparseBytes = uint64(len(sparse))
	audit.DictionaryDigest = h.dictionaryDigest
	return candidate, audit, nil
}

func TestStateDomainChangeV9OfflineTransformMatchesStreamingBuilder(t *testing.T) {
	old, _ := buildAccessorVersionFixture(t, 8, 4200)
	want, _ := buildAccessorVersionFixture(t, 9, 4200)
	got, audit, err := transcodeHistoryAccessorV9ForTest(old)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, want) {
		t.Fatal("offline/streaming V9 bytes differ")
	}
	a, err := readStateDomainChangeBinaryAccessorV7Debug(bytes.NewReader(old), uint64(len(old)))
	if err != nil {
		t.Fatal(err)
	}
	b, err := readStateDomainChangeBinaryAccessorV7Debug(bytes.NewReader(got), uint64(len(got)))
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(a, b) {
		t.Fatal("complete historical tuples differ")
	}
	t.Logf("audit %+v", audit)
}
