package snapshots

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"testing"
)

type accessorV11TranscodeAudit struct {
	Keys, Records, Singletons, MultiLists, MultiRows                   uint64
	SourceBytes, CandidateBytes, SourcePostingBytes, PostingBytes      uint64
	SourceKeyBytes, CandidateKeyBytes, DirectoryBytes                  uint64
	OffsetBytes, OwnerDataBytes, OwnerDirectoryBytes, OwnerFooterBytes uint64
	TxRuns, TxDataBytes, TxDirectoryBytes, TxFooterBytes               uint64
	Version                                                            uint32
	DictionaryDigest                                                   [32]byte
}

// Offline bounded transformation. Every source ordinal is assigned exactly
// once; the chronological transaction vector is independently checked against
// every candidate posting. This is not an online database migration command.
func transcodeHistoryAccessorV11ForTest(data []byte, scratchDir string) ([]byte, accessorV11TranscodeAudit, error) {
	var a accessorV11TranscodeAudit
	fail := func(e error) ([]byte, accessorV11TranscodeAudit, error) { return nil, a, e }
	if len(data) > 128<<20 {
		return fail(fmt.Errorf("offline V11 input cap"))
	}
	r := bytes.NewReader(data)
	h, e := decodeStateDomainChangeBinaryAccessorKeyHeader(r, uint64(len(data)))
	if e != nil {
		return fail(e)
	}
	if h.version != 9 && h.version != 10 || h.recordCount > 32<<20 || h.keyCount > 1<<20 {
		return fail(fmt.Errorf("requires bounded V9/V10 input"))
	}
	a.SourceBytes = uint64(len(data))
	a.SourceKeyBytes = h.keyDataLen
	a.SourcePostingBytes = h.postingLen
	a.Keys = h.keyCount
	a.DictionaryDigest = h.dictionaryDigest
	a.DirectoryBytes = h.blockDirLen
	bs, e := stateDomainChangeBinaryAccessorV6ReadBlockDirectory(r, h)
	if e != nil {
		return fail(e)
	}
	txs := make([]uint64, h.recordCount)
	seen := make([]byte, (h.recordCount+7)/8)
	readPage := func(bi int) ([]stateDomainChangeBinaryAccessorV6Record, error) {
		return stateDomainChangeBinaryAccessorV6ReadBlock(r, uint64(len(data)), h, bs[bi], uint32(bi*128))
	}
	visit := func(rec stateDomainChangeBinaryAccessorV6Record, fn func([]stateDomainChangeBinaryAccessorV6Posting) error) error {
		fs, e := stateDomainChangeBinaryAccessorV7PostingFrames(r, h, rec)
		if e != nil {
			return e
		}
		var count uint64
		for _, f := range fs {
			ps, _, e := readHistoryAccessorRawFrame(r, h, rec, f)
			if e != nil {
				return e
			}
			count += uint64(len(ps))
			if e = fn(ps); e != nil {
				return e
			}
		}
		if count != uint64(rec.postings) {
			return fmt.Errorf("source list count")
		}
		return nil
	}
	for bi := range bs {
		rs, e := readPage(bi)
		if e != nil {
			return fail(e)
		}
		for _, rec := range rs {
			e = visit(rec, func(ps []stateDomainChangeBinaryAccessorV6Posting) error {
				for _, p := range ps {
					i := uint64(p.recordIndex)
					if i >= h.recordCount || seen[i/8]&(1<<(i%8)) != 0 {
						return fmt.Errorf("ordinal duplicate/outside table")
					}
					seen[i/8] |= 1 << (i % 8)
					txs[i] = p.txNum
					a.Records++
				}
				return nil
			})
			if e != nil {
				return fail(e)
			}
			if rec.inline {
				a.Singletons++
			} else {
				a.MultiLists++
				a.MultiRows += uint64(rec.postings)
			}
		}
	}
	if a.Records != h.recordCount {
		return fail(fmt.Errorf("source missing ordinal"))
	}
	tx, e := newHistoryTxBuilder(scratchDir, "offline")
	if e != nil {
		return fail(e)
	}
	defer tx.Close()
	for i, v := range txs {
		if seen[i/8]&(1<<uint(i%8)) == 0 || v < h.fromTxNum || v > h.toTxNum {
			return fail(fmt.Errorf("source ordinal/range"))
		}
		if e = tx.Add(v, uint32(i)); e != nil {
			return fail(e)
		}
	}
	if e = tx.Finish(); e != nil {
		return fail(e)
	}
	if tx.tooLarge || a.MultiLists == 0 {
		a.CandidateBytes = a.SourceBytes
		a.CandidateKeyBytes = h.keyDataLen
		a.PostingBytes = h.postingLen
		a.OffsetBytes = h.offsetDirectory + ceilDiv(h.recordCount, historyOffsetPageRows)*historyOffsetEntrySize + historyOffsetFooterSize - h.offsetDataBase
		a.Version = h.version
		if h.owners != nil {
			a.OwnerDataBytes = h.owners.dataLen
			a.OwnerDirectoryBytes = h.owners.dirLen
			a.OwnerFooterBytes = 64
		}
		return append([]byte(nil), data...), a, nil
	}
	var owners *historyOwnerTable
	if h.owners != nil {
		var values [][20]byte
		for page := uint64(0); page < ceilDiv(h.owners.count, historyOwnerPageRows); page++ {
			base := h.owners.prefixSize + h.owners.dataLen + page*historyOwnerEntrySize
			entry := data[base : base+18]
			off, n, _, err := historyOwnerPageBounds(h.owners, page, entry)
			if err != nil {
				return fail(err)
			}
			vs, err := decodeHistoryOwnerPage(h.owners, page, entry, data[h.owners.prefixSize+off:h.owners.prefixSize+off+n])
			if err != nil {
				return fail(err)
			}
			values = append(values, vs...)
		}
		owners, e = makeHistoryOwnerTable(values)
		if e != nil {
			return fail(e)
		}
	}
	var posts, keys, plain bytes.Buffer
	var target, plainBlocks []stateDomainChangeBinaryAccessorV6Block
	largest, plainLargest := 0, 0
	var sourceEnd uint64
	for bi := range bs {
		rs, e := readPage(bi)
		if e != nil {
			return fail(e)
		}
		ks := make([]stateDomainChangeBinaryAccessorV6Key, len(rs))
		for i, rec := range rs {
			ks[i] = stateDomainChangeBinaryAccessorV6Key{key: rec.key, keyID: rec.keyID, postingCount: rec.postings, postingOff: uint64(posts.Len()), inlinePosting: rec.inlinePosting}
			if rec.inline {
				continue
			}
			if rec.postingOff != sourceEnd {
				return fail(fmt.Errorf("source posting stream gap"))
			}
			fs, e := stateDomainChangeBinaryAccessorV7PostingFrames(r, h, rec)
			if e != nil {
				return fail(e)
			}
			var rows []stateDomainChangeBinaryAccessorV6Posting
			var length uint64
			for _, f := range fs {
				ps, end, e := readHistoryAccessorRawFrame(r, h, rec, f)
				if e != nil {
					return fail(e)
				}
				length = end
				for j := range ps {
					ps[j].offset = uint64(ps[j].recordIndex) + 1
				}
				rows = append(rows, ps...)
			}
			sourceEnd += length
			encoded, e := encodeStateDomainChangeBinaryAccessorPostingListVersion(h.fromTxNum, rows, 11)
			if e != nil {
				return fail(e)
			}
			posts.Write(encoded)
		}
		raw, e := encodeStateDomainChangeBinaryAccessorV8KeyBlock(ks, h.fromTxNum)
		if e != nil {
			return fail(e)
		}
		packed, flags, e := compressHistoryDictionaryKeyBlock(raw, &plainLargest)
		if e != nil {
			return fail(e)
		}
		pb := stateDomainChangeBinaryAccessorV6Block{firstKey: bs[bi].firstKey, dataOff: uint64(plain.Len()), dataLen: flags, rawLen: uint32(len(raw)), checksum: crc32.ChecksumIEEE(raw), keyCount: uint32(len(ks))}
		plainBlocks = append(plainBlocks, pb)
		plain.Write(packed)
		if owners != nil {
			raw, packed, flags, e = encodeHistoryOwnerKeyBlock(ks, h.fromTxNum, owners, &largest)
			if e != nil {
				return fail(e)
			}
			target = append(target, stateDomainChangeBinaryAccessorV6Block{firstKey: bs[bi].firstKey, dataOff: uint64(keys.Len()), dataLen: flags, rawLen: uint32(len(raw)), checksum: crc32.ChecksumIEEE(raw), keyCount: uint32(len(ks))})
			keys.Write(packed)
		}
	}
	if sourceEnd != h.postingLen {
		return fail(fmt.Errorf("source posting coverage"))
	}
	if owners == nil || uint64(keys.Len())+owners.size() >= uint64(plain.Len()) {
		owners = nil
		keys = plain
		target = plainBlocks
	}
	sparse := encodeHistoryAccessorSparseDirectory(target)
	dirLen := uint64(len(target))*8 + uint64(len(sparse))
	for _, b := range target {
		dirLen += 2 + uint64(len(b.firstKey)) + 28
	}
	if dirLen != h.blockDirLen {
		return fail(fmt.Errorf("directory identity"))
	}
	out := bytes.NewBuffer(make([]byte, 0, len(data)))
	header := append([]byte(nil), data[:120]...)
	binary.BigEndian.PutUint32(header[8:12], 11)
	binary.BigEndian.PutUint64(header[64:72], uint64(keys.Len()))
	binary.BigEndian.PutUint64(header[104:112], uint64(posts.Len()))
	binary.BigEndian.PutUint32(header[112:116], stateDomainChangeAccessorHeaderCRC(header, 11))
	out.Write(header)
	off := uint64(120 + len(target)*8)
	for _, b := range target {
		binary.Write(out, binary.BigEndian, off)
		off += 2 + uint64(len(b.firstKey)) + 28
	}
	keyBase := uint64(120) + dirLen
	for _, b := range target {
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
	out.Write(posts.Bytes())
	locatorEnd := h.offsetDirectory + ceilDiv(h.recordCount, historyOffsetPageRows)*historyOffsetEntrySize + historyOffsetFooterSize
	out.Write(data[h.offsetDataBase:locatorEnd])
	a.OffsetBytes = locatorEnd - h.offsetDataBase
	if owners != nil {
		if e = owners.appendTo(out, uint64(out.Len())); e != nil {
			return fail(e)
		}
		a.OwnerDataBytes = uint64(len(owners.data))
		a.OwnerDirectoryBytes = uint64(len(owners.directory))
		a.OwnerFooterBytes = 64
	}
	if e = tx.AppendTo(out, uint64(out.Len()), owners != nil); e != nil {
		return fail(e)
	}
	candidate := out.Bytes()
	nr := bytes.NewReader(candidate)
	nh, e := decodeStateDomainChangeBinaryAccessorKeyHeader(nr, uint64(len(candidate)))
	if e != nil {
		return fail(e)
	}
	if nh.dictionaryDigest != h.dictionaryDigest {
		return fail(fmt.Errorf("dictionary identity changed"))
	}
	if e = nh.txRuns.Check(); e != nil {
		return fail(e)
	}
	for page := range nh.txRuns.pages {
		runs, err := nh.txRuns.page(page)
		if err != nil {
			return fail(err)
		}
		for _, run := range runs {
			for j := uint64(run.first); j < uint64(run.first)+uint64(run.count); j++ {
				if txs[j] != run.tx {
					return fail(fmt.Errorf("transaction table differs from independent ordinal vector"))
				}
			}
		}
	}
	nb, e := stateDomainChangeBinaryAccessorV6ReadBlockDirectory(nr, nh)
	if e != nil {
		return fail(e)
	}
	for bi := range bs {
		source, e := readPage(bi)
		if e != nil {
			return fail(e)
		}
		target, e := stateDomainChangeBinaryAccessorV6ReadBlock(nr, uint64(len(candidate)), nh, nb[bi], uint32(bi*128))
		if e != nil {
			return fail(e)
		}
		if len(source) != len(target) {
			return fail(fmt.Errorf("key page size"))
		}
		for i, z := range target {
			s := source[i]
			if !bytes.Equal(z.key, s.key) || z.keyID != s.keyID || z.postings != s.postings || z.inline != s.inline || z.inline && z.inlinePosting != s.inlinePosting {
				return fail(fmt.Errorf("key identity"))
			}
			sf, e := stateDomainChangeBinaryAccessorV7PostingFrames(r, h, s)
			if e != nil {
				return fail(e)
			}
			zf, e := stateDomainChangeBinaryAccessorV7PostingFrames(nr, nh, z)
			if e != nil {
				return fail(e)
			}
			if len(sf) != len(zf) {
				return fail(fmt.Errorf("frame count"))
			}
			for fi, f := range zf {
				sp, _, e := readHistoryAccessorRawFrame(r, h, s, sf[fi])
				if e != nil {
					return fail(e)
				}
				zp, _, e := readHistoryAccessorOrdinalFrame(nr, nh, z, f)
				if e != nil {
					return fail(e)
				}
				if len(sp) != len(zp) {
					return fail(fmt.Errorf("frame rows"))
				}
				for j, p := range zp {
					if sp[j].recordIndex != p.recordIndex || sp[j].txNum != txs[p.recordIndex] {
						return fail(fmt.Errorf("posting identity"))
					}
				}
			}
		}
	}
	if !bytes.Equal(data[h.offsetDataBase:locatorEnd], candidate[nh.offsetDataBase:nh.offsetDirectory+ceilDiv(nh.recordCount, historyOffsetPageRows)*historyOffsetEntrySize+historyOffsetFooterSize]) {
		return fail(fmt.Errorf("locator changed"))
	}
	a.Version = 11
	a.CandidateBytes = uint64(len(candidate))
	a.CandidateKeyBytes = nh.keyDataLen
	a.PostingBytes = nh.postingLen
	a.TxRuns = tx.runs
	a.TxDataBytes = tx.dataLen
	a.TxDirectoryBytes = uint64(len(tx.pages)) * 32
	a.TxFooterBytes = 64
	return candidate, a, nil
}
func TestHistoryTxRunsOfflineTransformMatchesBuilder(t *testing.T) {
	for _, repeated := range []bool{false, true} {
		t.Run(fmt.Sprint(repeated), func(t *testing.T) {
			old, _ := buildOwnerAccessorFixture(t, 10, 4200, repeated)
			want, _ := buildOwnerAccessorFixture(t, 11, 4200, repeated)
			got, a, e := transcodeHistoryAccessorV11ForTest(old, t.TempDir())
			if e != nil {
				t.Fatal(e)
			}
			if !bytes.Equal(got, want) {
				t.Fatalf("streamed and offline differ %d %d %+v", len(got), len(want), a)
			}
			t.Logf("%+v", a)
		})
	}
}
