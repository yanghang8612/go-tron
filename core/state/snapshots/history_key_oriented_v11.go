package snapshots

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"io"
	"math"
	"os"
	"sort"
	"sync"
)

const stateDomainChangeBinaryAccessorVersionV11 = uint32(11)
const historyTxPageRuns = uint64(256)
const historyTxEntrySize = uint64(32)
const historyTxFooterSize = uint64(64)
const historyTxDirectoryLimit = 1 << 20
const historyTxPageMaxBytes = int(historyTxPageRuns) * 15
const historyTxDecodedCacheLimit = 2 << 20

var historyTxMagic = [8]byte{'G', 'T', 'T', 'R', 'U', 'N', '1', '1'}

type historyTxRun struct {
	tx           uint64
	first, count uint32
}
type historyTxPage struct {
	firstTx      uint64
	first, rows  uint32
	off          uint64
	length, runs uint16
	crc          uint32
}
type historyTxTable struct {
	r                                                io.ReaderAt
	prefix, dataLen, runCount, recordCount, from, to uint64
	owners                                           bool
	pages                                            []historyTxPage
	mu                                               sync.Mutex
	cache                                            map[int][]historyTxRun
	order                                            []int
	cacheBytes                                       int
}

func historyTxPageCRC(index uint64, entry, raw []byte) uint32 {
	var b [8]byte
	binary.BigEndian.PutUint64(b[:], index)
	sum := crc32.Update(crc32.ChecksumIEEE(b[:]), crc32.IEEETable, entry[:28])
	return crc32.Update(sum, crc32.IEEETable, raw)
}
func encodeHistoryTxEntry(p historyTxPage) ([32]byte, error) {
	var b [32]byte
	binary.BigEndian.PutUint64(b[:8], p.firstTx)
	binary.BigEndian.PutUint32(b[8:12], p.first)
	binary.BigEndian.PutUint32(b[12:16], p.rows)
	if err := putStateDomainChangeBinaryAccessorV5Offset(b[16:22], p.off); err != nil {
		return b, err
	}
	binary.BigEndian.PutUint16(b[22:24], p.length)
	binary.BigEndian.PutUint16(b[24:26], p.runs)
	binary.BigEndian.PutUint32(b[28:], p.crc)
	return b, nil
}

// The whole bounded routing directory is authenticated before binary search.
// A corrupt unvisited first-Tx value must not silently turn a hit into a miss.
func decodeHistoryTxFooter(r io.ReaderAt, size uint64, h stateDomainChangeBinaryAccessorV6Header) (*historyTxTable, error) {
	if size < historyTxFooterSize || size > math.MaxInt64 {
		return nil, io.ErrUnexpectedEOF
	}
	var b [64]byte
	if _, err := r.ReadAt(b[:], int64(size-64)); err != nil {
		return nil, err
	}
	if !bytes.Equal(b[:8], historyTxMagic[:]) || binary.BigEndian.Uint32(b[40:44]) != uint32(historyTxPageRuns) || binary.BigEndian.Uint32(b[44:48]) != uint32(historyTxEntrySize) || binary.BigEndian.Uint32(b[48:52]) > 1 || binary.BigEndian.Uint32(b[56:60]) != 0 || binary.BigEndian.Uint32(b[60:]) != crc32.ChecksumIEEE(b[:60]) {
		return nil, errors.New("snapshots: invalid V11 transaction footer")
	}
	t := &historyTxTable{r: r, prefix: binary.BigEndian.Uint64(b[8:16]), dataLen: binary.BigEndian.Uint64(b[16:24]), runCount: binary.BigEndian.Uint64(b[32:40]), recordCount: h.recordCount, from: h.fromTxNum, to: h.toTxNum, owners: binary.BigEndian.Uint32(b[48:52]) == 1, cache: make(map[int][]historyTxRun)}
	dirLen := binary.BigEndian.Uint64(b[24:32])
	if t.prefix < 120+historyOffsetFooterSize || t.prefix > size-64 || t.runCount > h.recordCount || (t.runCount == 0) != (h.recordCount == 0) || dirLen > historyTxDirectoryLimit || dirLen != ceilDiv(t.runCount, historyTxPageRuns)*32 {
		return nil, errors.New("snapshots: invalid V11 transaction counts")
	}
	remain := size - 64 - t.prefix
	if t.dataLen > remain || remain-t.dataLen != dirLen {
		return nil, errors.New("snapshots: invalid V11 transaction section length")
	}
	raw := make([]byte, int(dirLen))
	if _, err := r.ReadAt(raw, int64(t.prefix+t.dataLen)); err != nil {
		return nil, err
	}
	if crc32.ChecksumIEEE(raw) != binary.BigEndian.Uint32(b[52:56]) {
		return nil, errors.New("snapshots: V11 transaction directory checksum mismatch")
	}
	var rows, off, runs uint64
	for i := uint64(0); i < dirLen/32; i++ {
		e := raw[i*32 : (i+1)*32]
		at, err := stateDomainChangeBinaryAccessorV5Offset(e[16:22])
		p := historyTxPage{firstTx: binary.BigEndian.Uint64(e[:8]), first: binary.BigEndian.Uint32(e[8:12]), rows: binary.BigEndian.Uint32(e[12:16]), off: at, length: binary.BigEndian.Uint16(e[22:24]), runs: binary.BigEndian.Uint16(e[24:26]), crc: binary.BigEndian.Uint32(e[28:])}
		want := min(historyTxPageRuns, t.runCount-runs)
		if err != nil || e[26] != 0 || e[27] != 0 || p.firstTx < t.from || p.firstTx > t.to || i > 0 && p.firstTx <= t.pages[len(t.pages)-1].firstTx || uint64(p.first) != rows || p.rows == 0 || uint64(p.rows) > h.recordCount-rows || p.off != off || p.length == 0 || int(p.length) > historyTxPageMaxBytes || uint64(p.length) > t.dataLen-off || uint64(p.runs) != want || uint64(p.rows) < want {
			return nil, errors.New("snapshots: invalid V11 transaction directory entry")
		}
		t.pages = append(t.pages, p)
		rows += uint64(p.rows)
		off += uint64(p.length)
		runs += uint64(p.runs)
	}
	if rows != h.recordCount || runs != t.runCount || off != t.dataLen {
		return nil, errors.New("snapshots: V11 transaction directory coverage mismatch")
	}
	return t, nil
}
func (t *historyTxTable) page(index int) ([]historyTxRun, error) {
	if t == nil || index < 0 || index >= len(t.pages) {
		return nil, errors.New("snapshots: transaction page outside table")
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if v, ok := t.cache[index]; ok {
		return v, nil
	}
	p := t.pages[index]
	raw := make([]byte, int(p.length))
	if _, err := t.r.ReadAt(raw, int64(t.prefix+p.off)); err != nil {
		return nil, err
	}
	entry, err := encodeHistoryTxEntry(p)
	if err != nil {
		return nil, err
	}
	if historyTxPageCRC(uint64(index), entry[:], raw) != p.crc {
		return nil, errors.New("snapshots: V11 transaction page checksum mismatch")
	}
	runs := make([]historyTxRun, 0, p.runs)
	pos := 0
	tx := p.firstTx
	first := uint64(p.first)
	read := func() (uint64, error) {
		if pos >= len(raw) {
			return 0, io.ErrUnexpectedEOF
		}
		v, n := binary.Uvarint(raw[pos:])
		if n <= 0 {
			return 0, errors.New("snapshots: invalid transaction varint")
		}
		pos += n
		return v, nil
	}
	for i := uint16(0); i < p.runs; i++ {
		delta, err := read()
		if err != nil {
			return nil, err
		}
		count, err := read()
		if err != nil {
			return nil, err
		}
		if i == 0 && delta != 0 || i > 0 && delta == 0 || delta > math.MaxUint64-tx || count == 0 || count > math.MaxUint32 || first > t.recordCount || count > t.recordCount-first {
			return nil, errors.New("snapshots: invalid transaction run")
		}
		tx += delta
		if tx > t.to || index+1 < len(t.pages) && tx >= t.pages[index+1].firstTx {
			return nil, errors.New("snapshots: transaction run outside page bounds")
		}
		runs = append(runs, historyTxRun{tx: tx, first: uint32(first), count: uint32(count)})
		first += count
	}
	if pos != len(raw) || first != uint64(p.first)+uint64(p.rows) {
		return nil, errors.New("snapshots: transaction page row coverage mismatch")
	}
	cost := len(runs) * 16
	for t.cacheBytes+cost > historyTxDecodedCacheLimit && len(t.order) > 0 {
		old := t.order[0]
		t.order = t.order[1:]
		t.cacheBytes -= len(t.cache[old]) * 16
		delete(t.cache, old)
	}
	t.cache[index] = runs
	t.order = append(t.order, index)
	t.cacheBytes += cost
	return runs, nil
}
func (t *historyTxTable) TxNum(ordinal uint32) (uint64, error) {
	if t == nil || uint64(ordinal) >= t.recordCount {
		return 0, errors.New("snapshots: transaction ordinal outside table")
	}
	i := sort.Search(len(t.pages), func(i int) bool { return t.pages[i].first > ordinal }) - 1
	rows, err := t.page(i)
	if err != nil {
		return 0, err
	}
	j := sort.Search(len(rows), func(j int) bool { return uint64(rows[j].first)+uint64(rows[j].count) > uint64(ordinal) })
	if j == len(rows) || rows[j].first > ordinal {
		return 0, errors.New("snapshots: uncovered transaction ordinal")
	}
	return rows[j].tx, nil
}
func (t *historyTxTable) bound(tx uint64, strict bool) (uint64, error) {
	if t == nil {
		return 0, errors.New("snapshots: missing transaction table")
	}
	if len(t.pages) == 0 {
		return 0, nil
	}
	i := sort.Search(len(t.pages), func(i int) bool { return t.pages[i].firstTx > tx }) - 1
	if i < 0 {
		return 0, nil
	}
	runs, err := t.page(i)
	if err != nil {
		return 0, err
	}
	j := sort.Search(len(runs), func(j int) bool {
		if strict {
			return runs[j].tx > tx
		}
		return runs[j].tx >= tx
	})
	if j < len(runs) {
		return uint64(runs[j].first), nil
	}
	return uint64(t.pages[i].first) + uint64(t.pages[i].rows), nil
}
func (t *historyTxTable) Bounds(from, to uint64) (uint64, uint64, error) {
	if from > to {
		return 0, 0, errors.New("snapshots: inverted transaction range")
	}
	a, err := t.bound(from, false)
	if err != nil {
		return 0, 0, err
	}
	b, err := t.bound(to, true)
	return a, b, err
}
func (t *historyTxTable) Check() error {
	for i := range t.pages {
		if _, err := t.page(i); err != nil {
			return err
		}
	}
	return nil
}

// Chronological build scratch; an oversized routing directory disables V11
// before posting ETL is consumed, retaining the ordinary V10 build path.
type historyTxBuilder struct {
	file                     *os.File
	writer                   *bufio.Writer
	name                     string
	pages                    []historyTxPage
	current                  []historyTxRun
	rows, runs, dataLen      uint64
	lastTx                   uint64
	pending                  historyTxRun
	have, finished, tooLarge bool
}

func newHistoryTxBuilder(dir, base string) (*historyTxBuilder, error) {
	f, n, err := createStateDomainChangeBinaryTempFileInDir(dir, base+".tx-runs")
	if err != nil {
		return nil, err
	}
	return &historyTxBuilder{file: f, name: n, writer: bufio.NewWriterSize(f, 64<<10)}, nil
}
func (b *historyTxBuilder) Add(tx uint64, ordinal uint32) error {
	if b == nil || b.finished || uint64(ordinal) != b.rows || b.rows == math.MaxUint32 || b.have && tx < b.lastTx {
		return errors.New("snapshots: invalid chronological transaction run")
	}
	if !b.have {
		b.pending = historyTxRun{tx: tx, first: ordinal, count: 1}
		b.have = true
	} else if tx == b.lastTx {
		b.pending.count++
	} else {
		if err := b.finishRun(); err != nil {
			return err
		}
		b.pending = historyTxRun{tx: tx, first: ordinal, count: 1}
	}
	b.rows++
	b.lastTx = tx
	return nil
}
func (b *historyTxBuilder) finishRun() error {
	b.runs++
	if b.tooLarge {
		return nil
	}
	b.current = append(b.current, b.pending)
	if len(b.current) == int(historyTxPageRuns) {
		return b.flush()
	}
	return nil
}
func (b *historyTxBuilder) flush() error {
	if len(b.current) == 0 {
		return nil
	}
	if (len(b.pages)+1)*32 > historyTxDirectoryLimit {
		b.tooLarge = true
		b.current = nil
		return nil
	}
	p := historyTxPage{firstTx: b.current[0].tx, first: b.current[0].first, off: b.dataLen, runs: uint16(len(b.current))}
	var raw []byte
	previous := p.firstTx
	for _, r := range b.current {
		raw = binary.AppendUvarint(raw, r.tx-previous)
		raw = binary.AppendUvarint(raw, uint64(r.count))
		previous = r.tx
		p.rows += r.count
	}
	p.length = uint16(len(raw))
	entry, err := encodeHistoryTxEntry(p)
	if err != nil {
		return err
	}
	p.crc = historyTxPageCRC(uint64(len(b.pages)), entry[:], raw)
	if _, err := b.writer.Write(raw); err != nil {
		return err
	}
	b.dataLen += uint64(len(raw))
	b.pages = append(b.pages, p)
	b.current = b.current[:0]
	return nil
}
func (b *historyTxBuilder) Finish() error {
	if b == nil {
		return errors.New("snapshots: nil transaction builder")
	}
	if b.finished {
		return nil
	}
	if b.have {
		if err := b.finishRun(); err != nil {
			return err
		}
	}
	if err := b.flush(); err != nil {
		return err
	}
	if err := b.writer.Flush(); err != nil {
		return err
	}
	b.finished = true
	return nil
}
func (b *historyTxBuilder) AppendTo(w io.Writer, prefix uint64, owners bool) error {
	if err := b.Finish(); err != nil {
		return err
	}
	if b.tooLarge {
		return errors.New("snapshots: transaction directory exceeds bound")
	}
	if _, err := b.file.Seek(0, io.SeekStart); err != nil {
		return err
	}
	if _, err := io.Copy(w, b.file); err != nil {
		return err
	}
	var directory []byte
	for _, p := range b.pages {
		e, err := encodeHistoryTxEntry(p)
		if err != nil {
			return err
		}
		directory = append(directory, e[:]...)
	}
	if _, err := w.Write(directory); err != nil {
		return err
	}
	var footer [64]byte
	copy(footer[:8], historyTxMagic[:])
	binary.BigEndian.PutUint64(footer[8:16], prefix)
	binary.BigEndian.PutUint64(footer[16:24], b.dataLen)
	binary.BigEndian.PutUint64(footer[24:32], uint64(len(directory)))
	binary.BigEndian.PutUint64(footer[32:40], b.runs)
	binary.BigEndian.PutUint32(footer[40:44], uint32(historyTxPageRuns))
	binary.BigEndian.PutUint32(footer[44:48], 32)
	if owners {
		binary.BigEndian.PutUint32(footer[48:52], 1)
	}
	binary.BigEndian.PutUint32(footer[52:56], crc32.ChecksumIEEE(directory))
	binary.BigEndian.PutUint32(footer[60:], crc32.ChecksumIEEE(footer[:60]))
	_, err := w.Write(footer[:])
	return err
}
func (b *historyTxBuilder) Close() error {
	if b == nil || b.file == nil {
		return nil
	}
	err := errors.Join(b.file.Close(), os.Remove(b.name))
	b.file = nil
	b.name = ""
	return err
}

// A batch/prefix iterator shares one resolver, so its transaction interval is
// translated only once even when many logical keys land in the same segment.
func (r *historyAccessorOffsetResolver) TxBounds(from, to uint64) (uint64, uint64, error) {
	if r.boundsHave && r.boundsFrom == from && r.boundsTo == to {
		return r.boundsLower, r.boundsUpper, nil
	}
	lower, upper, err := r.h.txRuns.Bounds(from, to)
	if err != nil {
		return 0, 0, err
	}
	r.boundsHave = true
	r.boundsFrom = from
	r.boundsTo = to
	r.boundsLower = lower
	r.boundsUpper = upper
	return lower, upper, nil
}

// Joint publication validation reads the authoritative .seg in ordinal order.
// Include inline singleton ordinals: range pruning also depends on their runs.
type historyTxSequentialCursor struct {
	table     *historyTxTable
	page, run int
	rows      []historyTxRun
	next      uint64
}

func (c *historyTxSequentialCursor) Next(ordinal uint64) (uint64, error) {
	if c.table == nil || ordinal != c.next || ordinal >= c.table.recordCount {
		return 0, errors.New("snapshots: invalid sequential transaction ordinal")
	}
	for c.rows == nil || c.run == len(c.rows) || ordinal >= uint64(c.rows[c.run].first)+uint64(c.rows[c.run].count) {
		if c.rows != nil && c.run < len(c.rows) {
			c.run++
			continue
		}
		var err error
		c.rows, err = c.table.page(c.page)
		if err != nil {
			return 0, err
		}
		c.page++
		c.run = 0
	}
	r := c.rows[c.run]
	if ordinal < uint64(r.first) {
		return 0, errors.New("snapshots: sequential transaction coverage gap")
	}
	c.next++
	return r.tx, nil
}
