package snapshots

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"reflect"
	"testing"
)

type accessorV8TranscodeAudit struct {
	Keys, Records, Singletons, MultiLists, MultiBytesCompared uint64
	SourceBytes, CandidateBytes, SparseBytes                  uint64
	DictionaryDigest                                          [32]byte
}

// Offline experiment helper, never invoked by node startup. The corpus caller
// authenticates the source's manifest checksum before this bounded transform.
func transcodeHistoryAccessorV8ForTest(data []byte) ([]byte, accessorV8TranscodeAudit, error) {
	var audit accessorV8TranscodeAudit
	if len(data) > 128<<20 {
		return nil, audit, fmt.Errorf("offline accessor exceeds 128 MiB budget")
	}
	r := bytes.NewReader(data)
	h, err := decodeStateDomainChangeBinaryAccessorV7Header(r, uint64(len(data)))
	if err != nil {
		return nil, audit, err
	}
	if h.version != stateDomainChangeBinaryVersionV7 {
		return nil, audit, fmt.Errorf("offline transform requires V7 input")
	}
	blocks, err := stateDomainChangeBinaryAccessorV6ReadBlockDirectory(r, h)
	if err != nil {
		return nil, audit, err
	}
	var keyData, posts bytes.Buffer
	var targetBlocks []stateDomainChangeBinaryAccessorV6Block
	var sourceOff uint64
	largest := 0
	dictionary := sha256.New()
	var length [4]byte
	for bi, b := range blocks {
		records, err := stateDomainChangeBinaryAccessorV6ReadBlock(r, uint64(len(data)), h, b, uint32(bi*128))
		if err != nil {
			return nil, audit, err
		}
		keys := make([]stateDomainChangeBinaryAccessorV6Key, len(records))
		for i, record := range records {
			if record.postingOff != sourceOff {
				return nil, audit, fmt.Errorf("noncontiguous source postings")
			}
			binary.BigEndian.PutUint32(length[:], uint32(len(record.key)))
			dictionary.Write(length[:])
			dictionary.Write(record.key)
			k := stateDomainChangeBinaryAccessorV6Key{key: record.key, keyID: record.keyID, postingOff: uint64(posts.Len()), postingCount: record.postings}
			if record.postings == 1 {
				var scratch [34]byte
				raw, n, err := stateDomainChangeBinaryAccessorV7ReadSingleFrameData(r, h, record, 1, scratch[:])
				if err != nil {
					return nil, audit, err
				}
				p, err := decodeStateDomainChangeBinaryAccessorV7Frame(raw, h.fromTxNum, 1)
				if err != nil {
					return nil, audit, err
				}
				k.inlinePosting = p[0]
				sourceOff += n
				audit.Singletons++
			} else {
				n, err := stateDomainChangeBinaryAccessorV7PostingListLength(r, h, record)
				if err != nil {
					return nil, audit, err
				}
				base := h.headerSize + h.blockDirLen + h.keyDataLen + record.postingOff
				if n > uint64(len(data))-base {
					return nil, audit, fmt.Errorf("source posting span outside file")
				}
				frames, err := stateDomainChangeBinaryAccessorV7PostingFrames(r, h, record)
				if err != nil {
					return nil, audit, err
				}
				if len(frames) > 1 {
					for _, f := range frames {
						raw := data[base+uint64(f.dataOff) : base+uint64(f.dataOff)+uint64(f.dataLen)]
						if crc32.ChecksumIEEE(raw) != f.crc {
							return nil, audit, fmt.Errorf("source multi frame CRC mismatch")
						}
					}
				}
				posts.Write(data[base : base+n])
				sourceOff += n
				audit.MultiLists++
				audit.MultiBytesCompared += n
			}
			audit.Keys++
			audit.Records += uint64(record.postings)
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
	if audit.Keys != h.keyCount || audit.Records != h.recordCount || sourceOff != h.postingLen || !bytes.Equal(dictionary.Sum(nil), h.dictionaryDigest[:]) {
		return nil, audit, fmt.Errorf("source dictionary/count mismatch")
	}
	sparse := encodeHistoryAccessorSparseDirectory(targetBlocks)
	dirLen := uint64(len(targetBlocks))*8 + uint64(len(sparse))
	for _, b := range targetBlocks {
		dirLen += 2 + uint64(len(b.firstKey)) + stateDomainChangeBinaryAccessorV6BlockTail
	}
	out := bytes.NewBuffer(make([]byte, 0, 120+int(dirLen)+keyData.Len()+posts.Len()))
	header := append([]byte(nil), data[:120]...)
	binary.BigEndian.PutUint32(header[8:12], 8)
	binary.BigEndian.PutUint64(header[56:64], dirLen)
	binary.BigEndian.PutUint64(header[64:72], uint64(keyData.Len()))
	binary.BigEndian.PutUint64(header[104:112], uint64(posts.Len()))
	binary.BigEndian.PutUint32(header[116:120], uint32(len(sparse)))
	binary.BigEndian.PutUint32(header[112:116], stateDomainChangeAccessorHeaderCRC(header, 8))
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
	candidate := out.Bytes()
	// Independently reopen every resulting key page through the production reader.
	nr := bytes.NewReader(candidate)
	nh, err := decodeStateDomainChangeBinaryAccessorKeyHeader(nr, uint64(len(candidate)))
	if err != nil {
		return nil, audit, err
	}
	nb, err := stateDomainChangeBinaryAccessorV6ReadBlockDirectory(nr, nh)
	if err != nil {
		return nil, audit, err
	}
	if err := verifyHistoryAccessorSparseDirectory(nr, nh, nb); err != nil {
		return nil, audit, err
	}
	for bi, b := range blocks {
		old, err := stateDomainChangeBinaryAccessorV6ReadBlock(r, uint64(len(data)), h, b, uint32(bi*128))
		if err != nil {
			return nil, audit, err
		}
		got, err := stateDomainChangeBinaryAccessorV6ReadBlock(nr, uint64(len(candidate)), nh, nb[bi], uint32(bi*128))
		if err != nil {
			return nil, audit, err
		}
		for i, a := range old {
			z := got[i]
			if !bytes.Equal(a.key, z.key) || a.keyID != z.keyID || a.postings != z.postings {
				return nil, audit, fmt.Errorf("target dictionary identity differs")
			}
			if a.postings == 1 {
				var scratch [34]byte
				raw, _, err := stateDomainChangeBinaryAccessorV7ReadSingleFrameData(r, h, a, 1, scratch[:])
				if err != nil {
					return nil, audit, err
				}
				p, err := decodeStateDomainChangeBinaryAccessorV7Frame(raw, h.fromTxNum, 1)
				if err != nil || !z.inline || p[0] != z.inlinePosting {
					return nil, audit, fmt.Errorf("inline tuple differs: %v", err)
				}
			} else {
				n, err := stateDomainChangeBinaryAccessorV7PostingListLength(r, h, a)
				if err != nil {
					return nil, audit, err
				}
				aoff := h.headerSize + h.blockDirLen + h.keyDataLen + a.postingOff
				zoff := nh.headerSize + nh.blockDirLen + nh.keyDataLen + z.postingOff
				if z.inline || !bytes.Equal(data[aoff:aoff+n], candidate[zoff:zoff+n]) {
					return nil, audit, fmt.Errorf("multi posting bytes differ")
				}
			}
		}
	}
	audit.SourceBytes = uint64(len(data))
	audit.CandidateBytes = uint64(len(candidate))
	audit.SparseBytes = uint64(len(sparse))
	audit.DictionaryDigest = h.dictionaryDigest
	return candidate, audit, nil
}
func TestStateDomainChangeV8OfflineTransformMatchesStreamingBuilder(t *testing.T) {
	old, _ := buildAccessorVersionFixture(t, 7, 4200)
	want, _ := buildAccessorVersionFixture(t, 8, 4200)
	got, audit, err := transcodeHistoryAccessorV8ForTest(old)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, want) {
		t.Fatal("offline/streaming physical encodings differ")
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
		t.Fatal("complete historical record enumeration differs")
	}
	if err := checkStateDomainChangeBinaryAccessorV7(bytes.NewReader(got), uint64(len(got))); err != nil {
		t.Fatal(err)
	}
	t.Logf("audit %+v", audit)
}
