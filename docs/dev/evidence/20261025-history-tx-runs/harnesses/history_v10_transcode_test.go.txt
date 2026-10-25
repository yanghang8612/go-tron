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

type accessorV10TranscodeAudit struct {
	Keys, Records, Owners, CompactPages, PlainPages                uint64
	SourceBytes, CandidateBytes, SourceKeyBytes, CandidateKeyBytes uint64
	OwnerDataBytes, OwnerDirectoryBytes, OwnerFooterBytes          uint64
	PostingBytes, OffsetBytes, DirectoryBytes                      uint64
	AccountKeys, AssetKeys, CrossFlatOwners                        uint64
	Version                                                        uint32
	DictionaryDigest                                               [32]byte
}

// Bounded test-only transformation. Posting and locator sections are copied
// byte-for-byte; every expanded key/ID and inline/posting metadata is compared.
func transcodeHistoryAccessorV10ForTest(data []byte) ([]byte, accessorV10TranscodeAudit, error) {
	var audit accessorV10TranscodeAudit
	if len(data) > 128<<20 {
		return nil, audit, fmt.Errorf("offline V10 input exceeds 128 MiB")
	}
	r := bytes.NewReader(data)
	h, err := decodeStateDomainChangeBinaryAccessorKeyHeader(r, uint64(len(data)))
	if err != nil {
		return nil, audit, err
	}
	if h.version != 9 || h.keyCount > 1<<20 {
		return nil, audit, fmt.Errorf("offline V10 requires bounded V9 input")
	}
	blocks, err := stateDomainChangeBinaryAccessorV6ReadBlockDirectory(r, h)
	if err != nil {
		return nil, audit, err
	}
	seen := map[[20]byte]byte{}
	eligible := h.keyCount >= 128
	for bi, b := range blocks {
		records, err := stateDomainChangeBinaryAccessorV6ReadBlock(r, uint64(len(data)), h, b, uint32(bi*128))
		if err != nil {
			return nil, audit, err
		}
		for _, record := range records {
			if len(record.key) < 21 {
				eligible = false
				continue
			}
			var owner [20]byte
			copy(owner[:], record.key[1:21])
			seen[owner] |= 1 << record.key[0]
			if record.key[0] == 1 {
				audit.AccountKeys++
			}
			if len(record.key) >= 31 && record.key[0] == 2 && binary.BigEndian.Uint16(record.key[29:31]) == 515 {
				audit.AssetKeys++
			}
		}
	}
	for _, mask := range seen {
		if mask&2 != 0 && mask&4 != 0 {
			audit.CrossFlatOwners++
		}
	}
	var table *historyOwnerTable
	if eligible {
		owners := make([][20]byte, 0, len(seen))
		for owner := range seen {
			owners = append(owners, owner)
		}
		table, err = makeHistoryOwnerTable(owners)
		if err != nil {
			return nil, audit, err
		}
	}
	audit.Owners = uint64(len(seen))
	var keys bytes.Buffer
	var targetBlocks []stateDomainChangeBinaryAccessorV6Block
	largest := 0
	for bi, b := range blocks {
		records, err := stateDomainChangeBinaryAccessorV6ReadBlock(r, uint64(len(data)), h, b, uint32(bi*128))
		if err != nil {
			return nil, audit, err
		}
		list := make([]stateDomainChangeBinaryAccessorV6Key, len(records))
		for i, rec := range records {
			list[i] = stateDomainChangeBinaryAccessorV6Key{key: rec.key, keyID: rec.keyID, postingCount: rec.postings, postingOff: rec.postingOff, inlinePosting: rec.inlinePosting}
			audit.Records += uint64(rec.postings)
		}
		audit.Keys += uint64(len(records))
		if table == nil {
			continue
		}
		raw, stored, flags, err := encodeHistoryOwnerKeyBlock(list, h.fromTxNum, table, &largest)
		if err != nil {
			return nil, audit, err
		}
		if raw[0] == 1 {
			audit.CompactPages++
		} else {
			audit.PlainPages++
		}
		targetBlocks = append(targetBlocks, stateDomainChangeBinaryAccessorV6Block{firstKey: append([]byte(nil), b.firstKey...), dataOff: uint64(keys.Len()), dataLen: flags, rawLen: uint32(len(raw)), checksum: crc32.ChecksumIEEE(raw), keyCount: b.keyCount})
		keys.Write(stored)
	}
	if audit.Keys != h.keyCount || audit.Records != h.recordCount {
		return nil, audit, fmt.Errorf("source key/record count mismatch")
	}
	audit.SourceBytes = uint64(len(data))
	audit.SourceKeyBytes = h.keyDataLen
	audit.PostingBytes = h.postingLen
	audit.OffsetBytes = uint64(len(data)) - h.offsetDataBase
	audit.DirectoryBytes = h.blockDirLen
	audit.DictionaryDigest = h.dictionaryDigest
	if table == nil || uint64(keys.Len())+table.size() >= h.keyDataLen {
		audit.CandidateBytes = audit.SourceBytes
		audit.CandidateKeyBytes = h.keyDataLen
		audit.Version = 9
		audit.PlainPages = h.blockCount
		audit.CompactPages = 0
		return append([]byte(nil), data...), audit, nil
	}
	sparse := encodeHistoryAccessorSparseDirectory(targetBlocks)
	dirLen := uint64(len(targetBlocks))*8 + uint64(len(sparse))
	for _, b := range targetBlocks {
		dirLen += 2 + uint64(len(b.firstKey)) + stateDomainChangeBinaryAccessorV6BlockTail
	}
	if dirLen != h.blockDirLen {
		return nil, audit, fmt.Errorf("directory identity changed")
	}
	out := bytes.NewBuffer(make([]byte, 0, len(data)))
	header := append([]byte(nil), data[:120]...)
	binary.BigEndian.PutUint32(header[8:12], 10)
	binary.BigEndian.PutUint64(header[64:72], uint64(keys.Len()))
	binary.BigEndian.PutUint32(header[112:116], stateDomainChangeAccessorHeaderCRC(header, 10))
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
		binary.BigEndian.PutUint32(tail[24:], crc32.Update(crc32.ChecksumIEEE(b.firstKey), crc32.IEEETable, tail[:24]))
		out.Write(tail[:])
	}
	out.Write(sparse)
	out.Write(keys.Bytes())
	// Key-data relocation leaves both complete V9 sections and their CRCs intact.
	postingStart := h.headerSize + h.blockDirLen + h.keyDataLen
	out.Write(data[postingStart:])
	if err := table.appendTo(out, uint64(out.Len())); err != nil {
		return nil, audit, err
	}
	candidate := out.Bytes()
	nr := bytes.NewReader(candidate)
	nh, err := decodeStateDomainChangeBinaryAccessorKeyHeader(nr, uint64(len(candidate)))
	if err != nil {
		return nil, audit, err
	}
	if nh.dictionaryDigest != h.dictionaryDigest {
		return nil, audit, fmt.Errorf("dictionary identity changed")
	}
	if err := checkHistoryOwnerDictionary(nr, nh); err != nil {
		return nil, audit, err
	}
	nb, err := stateDomainChangeBinaryAccessorV6ReadBlockDirectory(nr, nh)
	if err != nil {
		return nil, audit, err
	}
	digest := sha256.New()
	for bi, b := range blocks {
		source, err := stateDomainChangeBinaryAccessorV6ReadBlock(r, uint64(len(data)), h, b, uint32(bi*128))
		if err != nil {
			return nil, audit, err
		}
		target, err := stateDomainChangeBinaryAccessorV6ReadBlock(nr, uint64(len(candidate)), nh, nb[bi], uint32(bi*128))
		if err != nil {
			return nil, audit, err
		}
		if len(source) != len(target) {
			return nil, audit, fmt.Errorf("key-page count changed")
		}
		for i, a := range source {
			z := target[i]
			if !bytes.Equal(a.key, z.key) || a.keyID != z.keyID || a.postings != z.postings || a.postingOff != z.postingOff || a.inline != z.inline || a.inlinePosting != z.inlinePosting {
				return nil, audit, fmt.Errorf("expanded key/ID/metadata changed at page %d", bi)
			}
		}
		for _, rec := range target {
			var length [4]byte
			binary.BigEndian.PutUint32(length[:], uint32(len(rec.key)))
			digest.Write(length[:])
			digest.Write(rec.key)
		}
	}
	// The production dictionary commitment uses fixed uint32 lengths.
	if !bytes.Equal(digest.Sum(nil), h.dictionaryDigest[:]) {
		return nil, audit, fmt.Errorf("expanded dictionary digest mismatch")
	}
	candidatePosting := nh.headerSize + nh.blockDirLen + nh.keyDataLen
	if !bytes.Equal(data[postingStart:], candidate[candidatePosting:nh.owners.prefixSize]) {
		return nil, audit, fmt.Errorf("posting/locator bytes changed")
	}
	audit.CandidateBytes = uint64(len(candidate))
	audit.CandidateKeyBytes = nh.keyDataLen
	audit.OwnerDataBytes = uint64(len(table.data))
	audit.OwnerDirectoryBytes = uint64(len(table.directory))
	audit.OwnerFooterBytes = 64
	audit.Version = 10
	return candidate, audit, nil
}

func TestHistoryOwnerOfflineTransformMatchesBuilder(t *testing.T) {
	old, _ := buildOwnerAccessorFixture(t, 9, 4200, true)
	want, _ := buildOwnerAccessorFixture(t, 10, 4200, true)
	got, audit, err := transcodeHistoryAccessorV10ForTest(old)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("streaming and offline bytes differ: %d %d %+v", len(got), len(want), audit)
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
		t.Fatal("historical tuples differ")
	}
	t.Logf("audit %+v", audit)
}
