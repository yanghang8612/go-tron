package snapshots

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"io"
	"math"
	"sort"
)

const stateDomainChangeBinaryAccessorVersionV10 = uint32(10)
const historyOwnerPageRows = uint64(128)
const historyOwnerEntrySize = uint64(18)
const historyOwnerFooterSize = uint64(64)
const historyOwnerLimit = 1 << 20
const historyOwnerKeyPageLimit = 4
const historyOwnerReadBudget = uint64(64 << 10)
const historyOwnerPageMaxRaw = historyOwnerPageRows * 21

var historyOwnerMagic = [8]byte{'G', 'T', 'O', 'W', 'N', 'R', '1', '0'}

type historyOwnerHeader struct {
	count, prefixSize, dataLen, dirLen, fileSize uint64
}

func decodeHistoryOwnerFooter(r io.ReaderAt, size uint64) (*historyOwnerHeader, error) {
	if size < historyOwnerFooterSize {
		return nil, io.ErrUnexpectedEOF
	}
	var raw [historyOwnerFooterSize]byte
	if _, err := r.ReadAt(raw[:], int64(size-historyOwnerFooterSize)); err != nil {
		return nil, err
	}
	if !bytes.Equal(raw[:8], historyOwnerMagic[:]) || binary.BigEndian.Uint32(raw[40:44]) != uint32(historyOwnerPageRows) || binary.BigEndian.Uint32(raw[44:48]) != uint32(historyOwnerEntrySize) || !bytes.Equal(raw[48:60], make([]byte, 12)) || binary.BigEndian.Uint32(raw[60:]) != crc32.ChecksumIEEE(raw[:60]) {
		return nil, errors.New("snapshots: invalid V10 owner footer")
	}
	h := &historyOwnerHeader{count: binary.BigEndian.Uint64(raw[8:16]), prefixSize: binary.BigEndian.Uint64(raw[16:24]), dataLen: binary.BigEndian.Uint64(raw[24:32]), dirLen: binary.BigEndian.Uint64(raw[32:40]), fileSize: size}
	if h.count == 0 || h.count > historyOwnerLimit || h.prefixSize < 120+historyOffsetFooterSize || h.prefixSize > size-historyOwnerFooterSize || h.dirLen != ceilDiv(h.count, historyOwnerPageRows)*historyOwnerEntrySize {
		return nil, errors.New("snapshots: invalid V10 owner counts/prefix")
	}
	remaining := size - historyOwnerFooterSize - h.prefixSize
	if h.dataLen > remaining || remaining-h.dataLen != h.dirLen {
		return nil, errors.New("snapshots: invalid V10 owner section length")
	}
	return h, nil
}

func historyOwnerPageCRC(page uint64, entry, stored []byte) uint32 {
	var index [8]byte
	binary.BigEndian.PutUint64(index[:], page)
	sum := crc32.Update(crc32.ChecksumIEEE(index[:]), crc32.IEEETable, entry[:14])
	return crc32.Update(sum, crc32.IEEETable, stored)
}

func historyOwnerPageBounds(h *historyOwnerHeader, page uint64, entry []byte) (uint64, uint64, uint32, error) {
	if h == nil || page >= ceilDiv(h.count, historyOwnerPageRows) || len(entry) != int(historyOwnerEntrySize) {
		return 0, 0, 0, errors.New("snapshots: owner page outside dictionary")
	}
	off, err := stateDomainChangeBinaryAccessorV5Offset(entry[:6])
	flags := binary.BigEndian.Uint32(entry[6:10])
	n := uint64(flags &^ stateDomainChangeBinaryAccessorV6StoredRaw)
	rawLen := uint64(binary.BigEndian.Uint32(entry[10:14]))
	count := min(historyOwnerPageRows, h.count-page*historyOwnerPageRows)
	if err != nil || off > h.dataLen || n == 0 || n > historyOwnerPageMaxRaw || n > h.dataLen-off || rawLen < count || rawLen > count*21 || flags&stateDomainChangeBinaryAccessorV6StoredRaw != 0 && n != rawLen {
		return 0, 0, 0, errors.New("snapshots: invalid V10 owner page bounds")
	}
	return off, n, flags, nil
}

func decodeHistoryOwnerPage(h *historyOwnerHeader, page uint64, entry, stored []byte) ([][20]byte, error) {
	_, n, flags, err := historyOwnerPageBounds(h, page, entry)
	if err != nil || n != uint64(len(stored)) || historyOwnerPageCRC(page, entry, stored) != binary.BigEndian.Uint32(entry[14:]) {
		return nil, errors.New("snapshots: V10 owner page checksum/bounds mismatch")
	}
	raw := stored
	if flags&stateDomainChangeBinaryAccessorV6StoredRaw == 0 {
		_, decoder, err := cbCodec()
		if err != nil {
			return nil, err
		}
		raw, err = decoder.DecodeAll(stored, make([]byte, 0, binary.BigEndian.Uint32(entry[10:14])))
		if err != nil {
			return nil, err
		}
	}
	if len(raw) != int(binary.BigEndian.Uint32(entry[10:14])) {
		return nil, errors.New("snapshots: V10 owner decoded length mismatch")
	}
	count := min(historyOwnerPageRows, h.count-page*historyOwnerPageRows)
	owners := make([][20]byte, count)
	pos := 0
	for i := range owners {
		if pos >= len(raw) {
			return nil, io.ErrUnexpectedEOF
		}
		prefix := int(raw[pos])
		pos++
		if prefix > 20 || i == 0 && prefix != 0 || len(raw)-pos < 20-prefix {
			return nil, errors.New("snapshots: invalid V10 owner prefix")
		}
		if i > 0 {
			copy(owners[i][:prefix], owners[i-1][:prefix])
		}
		copy(owners[i][prefix:], raw[pos:pos+20-prefix])
		pos += 20 - prefix
		if i > 0 && bytes.Compare(owners[i-1][:], owners[i][:]) >= 0 {
			return nil, errors.New("snapshots: V10 owners not ordered")
		}
	}
	if pos != len(raw) {
		return nil, errors.New("snapshots: V10 owner page trailing data")
	}
	return owners, nil
}

// At most four dictionary pages per referenced key page. Directory and adjacent
// payload reads are combined, with a hard bound independent of file size.
func resolveHistoryOwnerKeys(r io.ReaderAt, h stateDomainChangeBinaryAccessorV6Header, records []stateDomainChangeBinaryAccessorV6Record) error {
	if h.owners == nil {
		return errors.New("snapshots: missing V10 owner dictionary")
	}
	pages := make([]uint64, 0, historyOwnerKeyPageLimit)
	for _, rec := range records {
		if len(rec.key) < 5 {
			return errors.New("snapshots: truncated V10 owner reference")
		}
		id := uint64(binary.BigEndian.Uint32(rec.key[1:5]))
		if id >= h.owners.count || len(rec.key)+16 > math.MaxUint16 {
			return errors.New("snapshots: V10 owner reference outside dictionary")
		}
		page := id / historyOwnerPageRows
		if !containsHistoryOwnerPage(pages, page) {
			pages = append(pages, page)
		}
	}
	if len(pages) > historyOwnerKeyPageLimit {
		return errors.New("snapshots: V10 key page exceeds owner read budget")
	}
	sort.Slice(pages, func(i, j int) bool { return pages[i] < pages[j] })
	entries := make(map[uint64][]byte, len(pages))
	for start := 0; start < len(pages); {
		end := start + 1
		// Merge only nearby entries: sparse owners must not turn two 18-byte
		// reads into a 64 KiB scan of unrelated directory entries.
		for end < len(pages) && pages[end]-pages[end-1] <= 4 && (pages[end]-pages[start]+1)*historyOwnerEntrySize <= historyOwnerReadBudget {
			end++
		}
		raw := make([]byte, (pages[end-1]-pages[start]+1)*historyOwnerEntrySize)
		if _, err := r.ReadAt(raw, int64(h.owners.prefixSize+h.owners.dataLen+pages[start]*historyOwnerEntrySize)); err != nil {
			return err
		}
		for _, page := range pages[start:end] {
			off := (page - pages[start]) * historyOwnerEntrySize
			entries[page] = raw[off : off+historyOwnerEntrySize]
		}
		start = end
	}
	decoded := make(map[uint64][][20]byte, len(pages))
	for start := 0; start < len(pages); {
		first := pages[start]
		off, n, _, err := historyOwnerPageBounds(h.owners, first, entries[first])
		if err != nil {
			return err
		}
		end, length := start+1, n
		for end < len(pages) && pages[end] == pages[end-1]+1 {
			nextOff, nextLen, _, err := historyOwnerPageBounds(h.owners, pages[end], entries[pages[end]])
			if err != nil {
				return err
			}
			if nextOff != off+length || length+nextLen > historyOwnerReadBudget {
				break
			}
			length += nextLen
			end++
		}
		stored := make([]byte, length)
		if _, err := r.ReadAt(stored, int64(h.owners.prefixSize+off)); err != nil {
			return err
		}
		for _, page := range pages[start:end] {
			at, size, _, err := historyOwnerPageBounds(h.owners, page, entries[page])
			if err != nil {
				return err
			}
			owners, err := decodeHistoryOwnerPage(h.owners, page, entries[page], stored[at-off:at-off+size])
			if err != nil {
				return err
			}
			decoded[page] = owners
		}
		start = end
	}
	for i := range records {
		encoded := records[i].key
		id := uint64(binary.BigEndian.Uint32(encoded[1:5]))
		owner := decoded[id/historyOwnerPageRows][id%historyOwnerPageRows]
		key := make([]byte, len(encoded)+16)
		key[0] = encoded[0]
		copy(key[1:21], owner[:])
		copy(key[21:], encoded[5:])
		records[i].key = key
	}
	return nil
}

func containsHistoryOwnerPage(pages []uint64, page uint64) bool {
	for _, p := range pages {
		if p == page {
			return true
		}
	}
	return false
}

type historyOwnerTable struct {
	ids             map[[20]byte]uint32
	data, directory []byte
}

func (b *stateDomainChangeV6Build) buildHistoryOwnerTable(ctx context.Context) (*historyOwnerTable, error) {
	if b.keyCount < 128 {
		return nil, nil
	}
	seen := make(map[[20]byte]struct{})
	for bi := range b.blocks {
		if err := contextError(ctx); err != nil {
			return nil, err
		}
		keys, err := b.dictionaryBlock(bi)
		if err != nil {
			return nil, err
		}
		for _, key := range keys {
			if len(key) < 21 {
				return nil, nil // Generic fixture/legacy keys keep their original representation.
			}
			var owner [20]byte
			copy(owner[:], key[1:21])
			seen[owner] = struct{}{}
			if len(seen) > historyOwnerLimit {
				return nil, nil
			}
		}
	}
	owners := make([][20]byte, 0, len(seen))
	for owner := range seen {
		owners = append(owners, owner)
	}
	return makeHistoryOwnerTableContext(ctx, owners)
}

func makeHistoryOwnerTable(owners [][20]byte) (*historyOwnerTable, error) {
	return makeHistoryOwnerTableContext(context.Background(), owners)
}

func makeHistoryOwnerTableContext(ctx context.Context, owners [][20]byte) (*historyOwnerTable, error) {
	if err := contextError(ctx); err != nil {
		return nil, err
	}
	if len(owners) == 0 || len(owners) > historyOwnerLimit {
		return nil, errors.New("snapshots: invalid owner dictionary size")
	}
	sort.Slice(owners, func(i, j int) bool { return bytes.Compare(owners[i][:], owners[j][:]) < 0 })
	t := &historyOwnerTable{ids: make(map[[20]byte]uint32, len(owners))}
	largest := 0
	for start := 0; start < len(owners); start += int(historyOwnerPageRows) {
		if err := contextError(ctx); err != nil {
			return nil, err
		}
		end := min(start+int(historyOwnerPageRows), len(owners))
		var raw, previous []byte
		for i := start; i < end; i++ {
			if i > 0 && bytes.Compare(owners[i-1][:], owners[i][:]) >= 0 {
				return nil, errors.New("snapshots: repeated owner dictionary input")
			}
			t.ids[owners[i]] = uint32(i)
			owner := owners[i][:]
			prefix := commonPrefixLength(previous, owner)
			raw = append(raw, byte(prefix))
			raw = append(raw, owner[prefix:]...)
			previous = owner
		}
		stored, flags, err := compressHistoryDictionaryKeyBlock(raw, &largest)
		if err != nil {
			return nil, err
		}
		var entry [historyOwnerEntrySize]byte
		if err := putStateDomainChangeBinaryAccessorV5Offset(entry[:6], uint64(len(t.data))); err != nil {
			return nil, err
		}
		binary.BigEndian.PutUint32(entry[6:10], flags)
		binary.BigEndian.PutUint32(entry[10:14], uint32(len(raw)))
		binary.BigEndian.PutUint32(entry[14:], historyOwnerPageCRC(uint64(start)/historyOwnerPageRows, entry[:], stored))
		t.directory = append(t.directory, entry[:]...)
		t.data = append(t.data, stored...)
	}
	return t, nil
}

func (t *historyOwnerTable) size() uint64 {
	return uint64(len(t.data)+len(t.directory)) + historyOwnerFooterSize
}

func (t *historyOwnerTable) appendTo(w io.Writer, prefixSize uint64) error {
	if _, err := w.Write(t.data); err != nil {
		return err
	}
	if _, err := w.Write(t.directory); err != nil {
		return err
	}
	var footer [historyOwnerFooterSize]byte
	copy(footer[:8], historyOwnerMagic[:])
	binary.BigEndian.PutUint64(footer[8:16], uint64(len(t.ids)))
	binary.BigEndian.PutUint64(footer[16:24], prefixSize)
	binary.BigEndian.PutUint64(footer[24:32], uint64(len(t.data)))
	binary.BigEndian.PutUint64(footer[32:40], uint64(len(t.directory)))
	binary.BigEndian.PutUint32(footer[40:44], uint32(historyOwnerPageRows))
	binary.BigEndian.PutUint32(footer[44:48], uint32(historyOwnerEntrySize))
	binary.BigEndian.PutUint32(footer[60:], crc32.ChecksumIEEE(footer[:60]))
	_, err := w.Write(footer[:])
	return err
}

func encodeHistoryOwnerKeyBlock(keys []stateDomainChangeBinaryAccessorV6Key, from uint64, table *historyOwnerTable, largest *int) ([]byte, []byte, uint32, error) {
	legacy, err := encodeStateDomainChangeBinaryAccessorV8KeyBlock(keys, from)
	if err != nil {
		return nil, nil, 0, err
	}
	raw := append([]byte{0}, legacy...)
	trialLargest := *largest
	stored, flags, err := compressHistoryDictionaryKeyBlock(raw, &trialLargest)
	if err != nil {
		return nil, nil, 0, err
	}
	compact := make([]stateDomainChangeBinaryAccessorV6Key, len(keys))
	pages := make([]uint64, 0, historyOwnerKeyPageLimit)
	for i, k := range keys {
		if len(k.key) < 21 {
			*largest = trialLargest
			return raw, stored, flags, nil
		}
		var owner [20]byte
		copy(owner[:], k.key[1:21])
		id, ok := table.ids[owner]
		if !ok {
			return nil, nil, 0, errors.New("snapshots: owner missing during key page build")
		}
		page := uint64(id) / historyOwnerPageRows
		if !containsHistoryOwnerPage(pages, page) {
			pages = append(pages, page)
		}
		compact[i] = k
		compact[i].key = make([]byte, len(k.key)-16)
		compact[i].key[0] = k.key[0]
		binary.BigEndian.PutUint32(compact[i].key[1:5], id)
		copy(compact[i].key[5:], k.key[21:])
	}
	if len(pages) <= historyOwnerKeyPageLimit {
		candidate, err := encodeStateDomainChangeBinaryAccessorV8KeyBlock(compact, from)
		if err != nil {
			return nil, nil, 0, err
		}
		candidate = append([]byte{1}, candidate...)
		nextLargest := *largest
		packed, packedFlags, err := compressHistoryDictionaryKeyBlock(candidate, &nextLargest)
		if err != nil {
			return nil, nil, 0, err
		}
		if len(packed) < len(stored) {
			raw, stored, flags, trialLargest = candidate, packed, packedFlags, nextLargest
		}
	}
	*largest = trialLargest
	return raw, stored, flags, nil
}

func checkHistoryOwnerDictionary(r io.ReaderAt, h stateDomainChangeBinaryAccessorV6Header) error {
	if h.owners == nil {
		return nil
	}
	var previous [20]byte
	var next uint64
	for page := uint64(0); page < ceilDiv(h.owners.count, historyOwnerPageRows); page++ {
		var entry [historyOwnerEntrySize]byte
		if _, err := r.ReadAt(entry[:], int64(h.owners.prefixSize+h.owners.dataLen+page*historyOwnerEntrySize)); err != nil {
			return err
		}
		off, n, _, err := historyOwnerPageBounds(h.owners, page, entry[:])
		if err != nil {
			return err
		}
		if off != next {
			return errors.New("snapshots: V10 owner page coverage mismatch")
		}
		stored := make([]byte, n)
		if _, err := r.ReadAt(stored, int64(h.owners.prefixSize+off)); err != nil {
			return err
		}
		owners, err := decodeHistoryOwnerPage(h.owners, page, entry[:], stored)
		if err != nil {
			return err
		}
		if page > 0 && bytes.Compare(previous[:], owners[0][:]) >= 0 {
			return errors.New("snapshots: V10 owner dictionary order mismatch")
		}
		previous, next = owners[len(owners)-1], off+n
	}
	if next != h.owners.dataLen {
		return errors.New("snapshots: V10 owner dictionary trailing data")
	}
	return nil
}
