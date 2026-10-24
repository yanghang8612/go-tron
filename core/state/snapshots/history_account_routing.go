package snapshots

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"sync"

	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/rawdb/etl"
)

const (
	accountRoutingHeaderBytes         = 64
	accountRoutingPageBytes           = 4096
	accountRoutingKeyBytes            = 21
	accountRoutingRecordBytes         = accountRoutingKeyBytes + 8
	accountRoutingDirectoryEntryBytes = accountRoutingKeyBytes + sha256.Size
	accountRoutingPageRecords         = accountRoutingPageBytes / accountRoutingRecordBytes
	accountRoutingDirectoryEntries    = accountRoutingPageBytes / accountRoutingDirectoryEntryBytes
	accountRoutingMaxRows             = uint64(1 << 30)
	accountRoutingMaxMetadataBytes    = 16 << 20
	accountRoutingMaxFileBytes        = uint64(64 << 30)
	accountRoutingNewDictionaryKeys   = uint64(8_000_000)
)

var accountRoutingMagic = [8]byte{'G', 'T', 'A', 'C', 'R', 'T', '0', '1'}
var errAccountRoutingCorrupt = errors.New("snapshots: corrupt account history routing index")

type accountRoutingHeader struct {
	sources                            uint32
	rows, pages, directories, from, to uint64
}

func (h accountRoutingHeader) metadataBytes() uint64 {
	return accountRoutingHeaderBytes + uint64(h.sources)*32 + h.directories*accountRoutingDirectoryEntryBytes
}
func (h accountRoutingHeader) fileBytes() uint64 {
	return h.metadataBytes() + (h.directories+h.pages)*accountRoutingPageBytes
}

type accountRoutingMetadata struct {
	header  accountRoutingHeader
	raw     []byte
	covered map[[32]byte]struct{}
}

var sharedAccountRoutingMetadata struct {
	sync.Mutex
	digest [32]byte
	value  *accountRoutingMetadata
}

type accountRoutingCachedPage struct {
	index uint64
	raw   []byte
}
type accountHistoryRoutingReader struct {
	file               *os.File
	metadata           *accountRoutingMetadata
	directories, pages []accountRoutingCachedPage
}

func accountRoutingKey(key []byte) bool {
	return len(key) == accountRoutingKeyBytes && (key[0] == byte(rawdb.StateFlatDomainAccountLatest) || key[0] == byte(rawdb.StateFlatDomainKVGeneration))
}
func accountRoutingQuery(q StateHistoryQuery) bool {
	return q.FlatDomain == rawdb.StateFlatDomainAccountLatest || q.FlatDomain == rawdb.StateFlatDomainKVGeneration
}

func accountRoutingRef(manifest *Manifest, from, to uint64) (SegmentRef, bool) {
	for _, ref := range manifest.Segments {
		if ref.Kind == SegmentAccountHistoryRouting && ref.ToTxNum >= from && ref.FromTxNum <= to {
			return ref, true
		}
	}
	return SegmentRef{}, false
}

func openAccountHistoryRouting(ctx context.Context, dir string, ref SegmentRef) (reader *accountHistoryRoutingReader, err error) {
	if err = contextError(ctx); err != nil {
		return nil, err
	}
	if ref.Kind != SegmentAccountHistoryRouting || ref.normalizedDataset() != SegmentDatasetStateDomainChange || ref.Size < accountRoutingHeaderBytes || ref.Size > accountRoutingMaxFileBytes || ref.LookupRoot == "" {
		return nil, errAccountRoutingCorrupt
	}
	if err = validateSegmentRef(ref); err != nil {
		return nil, err
	}
	f, err := os.Open(filepath.Join(dir, ref.Path))
	if err != nil {
		return nil, err
	}
	defer func() {
		if err != nil {
			f.Close()
		}
	}()
	stat, err := f.Stat()
	if err != nil {
		return nil, err
	}
	if stat.Size() != int64(ref.Size) {
		return nil, errAccountRoutingCorrupt
	}
	var header [accountRoutingHeaderBytes]byte
	r := contextReaderAt{ctx: ctx, r: f}
	if _, err = r.ReadAt(header[:], 0); err != nil {
		return nil, err
	}
	h, err := decodeAccountRoutingHeader(header[:], ref)
	if err != nil {
		return nil, err
	}
	raw := make([]byte, h.metadataBytes())
	if _, err = r.ReadAt(raw, 0); err != nil {
		return nil, err
	}
	digest := sha256.Sum256(raw)
	if fmt.Sprintf("sha256:%x", digest) != ref.LookupRoot {
		return nil, errAccountRoutingCorrupt
	}
	if err = contextError(ctx); err != nil {
		return nil, err
	}
	sharedAccountRoutingMetadata.Lock()
	meta := sharedAccountRoutingMetadata.value
	if sharedAccountRoutingMetadata.digest != digest {
		meta = nil
	}
	sharedAccountRoutingMetadata.Unlock()
	if meta == nil {
		meta, err = decodeAccountRoutingMetadata(raw, h)
		if err != nil {
			return nil, err
		}
		// Only immutable metadata is retained. Current metadata bytes are read
		// and hashed again on every open; data/inner-directory pages are local.
		if len(raw)+len(meta.covered)*128 <= systemHistoryRoutingMaxBytes {
			sharedAccountRoutingMetadata.Lock()
			sharedAccountRoutingMetadata.digest = digest
			sharedAccountRoutingMetadata.value = meta
			sharedAccountRoutingMetadata.Unlock()
		}
	}
	return &accountHistoryRoutingReader{file: f, metadata: meta}, nil
}

func decodeAccountRoutingHeader(raw []byte, ref SegmentRef) (h accountRoutingHeader, err error) {
	if len(raw) != accountRoutingHeaderBytes || !bytes.Equal(raw[:8], accountRoutingMagic[:]) || binary.BigEndian.Uint32(raw[8:12]) != 1 || !bytes.Equal(raw[56:64], make([]byte, 8)) {
		return h, errAccountRoutingCorrupt
	}
	h = accountRoutingHeader{sources: binary.BigEndian.Uint32(raw[12:16]), rows: binary.BigEndian.Uint64(raw[16:24]), pages: binary.BigEndian.Uint64(raw[24:32]), directories: binary.BigEndian.Uint64(raw[32:40]), from: binary.BigEndian.Uint64(raw[40:48]), to: binary.BigEndian.Uint64(raw[48:56])}
	if h.sources == 0 || h.sources > systemHistoryRoutingMaxSources || h.rows > accountRoutingMaxRows || h.pages != ceilDiv(h.rows, accountRoutingPageRecords) || h.directories != ceilDiv(h.pages, accountRoutingDirectoryEntries) || h.metadataBytes() > accountRoutingMaxMetadataBytes || h.fileBytes() > accountRoutingMaxFileBytes || h.fileBytes() != ref.Size || h.rows != ref.Rows || h.from != ref.FromTxNum || h.to != ref.ToTxNum || h.to < h.from {
		return h, errAccountRoutingCorrupt
	}
	return h, nil
}

func decodeAccountRoutingMetadata(raw []byte, h accountRoutingHeader) (*accountRoutingMetadata, error) {
	if uint64(len(raw)) != h.metadataBytes() {
		return nil, errAccountRoutingCorrupt
	}
	m := &accountRoutingMetadata{header: h, raw: raw, covered: make(map[[32]byte]struct{}, h.sources)}
	for i := uint32(0); i < h.sources; i++ {
		var id [32]byte
		copy(id[:], raw[accountRoutingHeaderBytes+int(i)*32:])
		if _, exists := m.covered[id]; exists {
			return nil, errAccountRoutingCorrupt
		}
		m.covered[id] = struct{}{}
	}
	var previous []byte
	for i := uint64(0); i < h.directories; i++ {
		entry := m.rootEntry(i)
		if !accountRoutingKey(entry[:21]) || previous != nil && bytes.Compare(previous, entry[:21]) >= 0 {
			return nil, errAccountRoutingCorrupt
		}
		previous = entry[:21]
	}
	return m, nil
}
func (m *accountRoutingMetadata) rootEntry(i uint64) []byte {
	off := accountRoutingHeaderBytes + uint64(m.header.sources)*32 + i*accountRoutingDirectoryEntryBytes
	return m.raw[off : off+accountRoutingDirectoryEntryBytes]
}

func (r *accountHistoryRoutingReader) Close() error {
	if r == nil || r.file == nil {
		return nil
	}
	err := r.file.Close()
	r.file = nil
	r.pages = nil
	r.directories = nil
	return err
}
func cacheAccountRoutingPage(cache *[]accountRoutingCachedPage, index uint64, raw []byte) {
	if len(*cache) == 8 {
		copy(*cache, (*cache)[1:])
		*cache = (*cache)[:7]
	}
	*cache = append(*cache, accountRoutingCachedPage{index, raw})
}
func findAccountRoutingPage(cache []accountRoutingCachedPage, index uint64) []byte {
	for _, p := range cache {
		if p.index == index {
			return p.raw
		}
	}
	return nil
}

func (r *accountHistoryRoutingReader) directory(ctx context.Context, index uint64) ([]byte, error) {
	if err := contextError(ctx); err != nil {
		return nil, err
	}
	h := r.metadata.header
	if index >= h.directories {
		return nil, errAccountRoutingCorrupt
	}
	if page := findAccountRoutingPage(r.directories, index); page != nil {
		return page, nil
	}
	page := make([]byte, accountRoutingPageBytes)
	off := h.metadataBytes() + index*accountRoutingPageBytes
	if _, err := (contextReaderAt{ctx: ctx, r: r.file}).ReadAt(page, int64(off)); err != nil {
		return nil, err
	}
	expected := r.metadata.rootEntry(index)
	sum := sha256.Sum256(page)
	if !bytes.Equal(sum[:], expected[21:]) || !bytes.Equal(page[:21], expected[:21]) {
		return nil, errAccountRoutingCorrupt
	}
	n := min(uint64(accountRoutingDirectoryEntries), h.pages-index*accountRoutingDirectoryEntries)
	var previous []byte
	for i := uint64(0); i < n; i++ {
		key := page[i*accountRoutingDirectoryEntryBytes : i*accountRoutingDirectoryEntryBytes+21]
		if !accountRoutingKey(key) || previous != nil && bytes.Compare(previous, key) >= 0 {
			return nil, errAccountRoutingCorrupt
		}
		previous = key
	}
	if !allZeroRoutingBytes(page[n*accountRoutingDirectoryEntryBytes:]) || index+1 < h.directories && bytes.Compare(previous, r.metadata.rootEntry(index + 1)[:21]) >= 0 {
		return nil, errAccountRoutingCorrupt
	}
	cacheAccountRoutingPage(&r.directories, index, page)
	return page, nil
}
func allZeroRoutingBytes(raw []byte) bool {
	for _, b := range raw {
		if b != 0 {
			return false
		}
	}
	return true
}

func (r *accountHistoryRoutingReader) page(ctx context.Context, index uint64) ([]byte, error) {
	if err := contextError(ctx); err != nil {
		return nil, err
	}
	h := r.metadata.header
	if index >= h.pages {
		return nil, errAccountRoutingCorrupt
	}
	if page := findAccountRoutingPage(r.pages, index); page != nil {
		return page, nil
	}
	dir, err := r.directory(ctx, index/accountRoutingDirectoryEntries)
	if err != nil {
		return nil, err
	}
	pos := (index % accountRoutingDirectoryEntries) * accountRoutingDirectoryEntryBytes
	entry := dir[pos : pos+accountRoutingDirectoryEntryBytes]
	page := make([]byte, accountRoutingPageBytes)
	off := h.metadataBytes() + h.directories*accountRoutingPageBytes + index*accountRoutingPageBytes
	if _, err := (contextReaderAt{ctx: ctx, r: r.file}).ReadAt(page, int64(off)); err != nil {
		return nil, err
	}
	sum := sha256.Sum256(page)
	if !bytes.Equal(sum[:], entry[21:]) || !bytes.Equal(page[:21], entry[:21]) {
		return nil, errAccountRoutingCorrupt
	}
	n := min(uint64(accountRoutingPageRecords), h.rows-index*accountRoutingPageRecords)
	var previous []byte
	for i := uint64(0); i < n; i++ {
		record := page[i*accountRoutingRecordBytes : (i+1)*accountRoutingRecordBytes]
		if !accountRoutingKey(record[:21]) || previous != nil && bytes.Compare(previous, record[:21]) >= 0 || binary.BigEndian.Uint64(record[21:]) > h.to {
			return nil, errAccountRoutingCorrupt
		}
		previous = record[:21]
	}
	if !allZeroRoutingBytes(page[n*accountRoutingRecordBytes:]) {
		return nil, errAccountRoutingCorrupt
	}
	if index+1 < h.pages {
		var next []byte
		if (index+1)%accountRoutingDirectoryEntries == 0 {
			next = r.metadata.rootEntry((index + 1) / accountRoutingDirectoryEntries)[:21]
		} else {
			next = dir[pos+accountRoutingDirectoryEntryBytes : pos+accountRoutingDirectoryEntryBytes+21]
		}
		if bytes.Compare(previous, next) >= 0 {
			return nil, errAccountRoutingCorrupt
		}
	}
	cacheAccountRoutingPage(&r.pages, index, page)
	return page, nil
}

func (r *accountHistoryRoutingReader) lookup(ctx context.Context, key []byte) (uint64, bool, error) {
	if !accountRoutingKey(key) {
		return 0, false, errAccountRoutingCorrupt
	}
	if err := contextError(ctx); err != nil {
		return 0, false, err
	}
	h := r.metadata.header
	if h.rows == 0 {
		return 0, false, nil
	}
	pos := sort.Search(int(h.directories), func(i int) bool { return bytes.Compare(r.metadata.rootEntry(uint64(i))[:21], key) > 0 })
	if pos == 0 {
		return 0, false, nil
	}
	dirIndex := uint64(pos - 1)
	dir, err := r.directory(ctx, dirIndex)
	if err != nil {
		return 0, false, err
	}
	n := int(min(uint64(accountRoutingDirectoryEntries), h.pages-dirIndex*accountRoutingDirectoryEntries))
	pos = sort.Search(n, func(i int) bool {
		return bytes.Compare(dir[i*accountRoutingDirectoryEntryBytes:i*accountRoutingDirectoryEntryBytes+21], key) > 0
	})
	if pos == 0 {
		return 0, false, errAccountRoutingCorrupt
	}
	pageIndex := dirIndex*accountRoutingDirectoryEntries + uint64(pos-1)
	page, err := r.page(ctx, pageIndex)
	if err != nil {
		return 0, false, err
	}
	n = int(min(uint64(accountRoutingPageRecords), h.rows-pageIndex*accountRoutingPageRecords))
	pos = sort.Search(n, func(i int) bool {
		return bytes.Compare(page[i*accountRoutingRecordBytes:i*accountRoutingRecordBytes+21], key) >= 0
	})
	if pos == n || !bytes.Equal(page[pos*accountRoutingRecordBytes:pos*accountRoutingRecordBytes+21], key) {
		return 0, false, nil
	}
	return binary.BigEndian.Uint64(page[pos*accountRoutingRecordBytes+21 : pos*accountRoutingRecordBytes+29]), true, nil
}

type accountRoutingIterator struct {
	reader *accountHistoryRoutingReader
	row    uint64
}

func (i *accountRoutingIterator) next(ctx context.Context) (key [21]byte, bound uint64, ok bool, err error) {
	if i.row >= i.reader.metadata.header.rows {
		return key, 0, false, nil
	}
	page, err := i.reader.page(ctx, i.row/accountRoutingPageRecords)
	if err != nil {
		return key, 0, false, err
	}
	off := (i.row % accountRoutingPageRecords) * accountRoutingRecordBytes
	copy(key[:], page[off:off+21])
	bound = binary.BigEndian.Uint64(page[off+21 : off+29])
	i.row++
	return key, bound, true, nil
}

type accountRoutingPageWriter struct {
	data, directories     *bufio.Writer
	page, directory       [accountRoutingPageBytes]byte
	rows, pages, dirPages uint64
	dirEntries            int
	roots                 []byte
	previous              [21]byte
	havePrevious          bool
	maxTx                 uint64
}

func (w *accountRoutingPageWriter) put(key []byte, bound uint64) error {
	if !accountRoutingKey(key) || w.rows >= accountRoutingMaxRows || w.havePrevious && bytes.Compare(w.previous[:], key) >= 0 {
		return errAccountRoutingCorrupt
	}
	pos := (w.rows % accountRoutingPageRecords) * accountRoutingRecordBytes
	copy(w.page[pos:pos+21], key)
	binary.BigEndian.PutUint64(w.page[pos+21:pos+29], min(bound, w.maxTx))
	copy(w.previous[:], key)
	w.havePrevious = true
	w.rows++
	if w.rows%accountRoutingPageRecords == 0 {
		return w.flushPage()
	}
	return nil
}
func (w *accountRoutingPageWriter) flushPage() error {
	if _, err := w.data.Write(w.page[:]); err != nil {
		return err
	}
	sum := sha256.Sum256(w.page[:])
	off := w.dirEntries * accountRoutingDirectoryEntryBytes
	copy(w.directory[off:off+21], w.page[:21])
	copy(w.directory[off+21:off+53], sum[:])
	w.dirEntries++
	w.pages++
	clear(w.page[:])
	if w.dirEntries == accountRoutingDirectoryEntries {
		return w.flushDirectory()
	}
	return nil
}
func (w *accountRoutingPageWriter) flushDirectory() error {
	if _, err := w.directories.Write(w.directory[:]); err != nil {
		return err
	}
	sum := sha256.Sum256(w.directory[:])
	w.roots = append(w.roots, w.directory[:21]...)
	w.roots = append(w.roots, sum[:]...)
	w.dirPages++
	w.dirEntries = 0
	clear(w.directory[:])
	return nil
}
func (w *accountRoutingPageWriter) finish() error {
	if w.rows%accountRoutingPageRecords != 0 {
		if err := w.flushPage(); err != nil {
			return err
		}
	}
	if w.dirEntries > 0 {
		if err := w.flushDirectory(); err != nil {
			return err
		}
	}
	return errors.Join(w.data.Flush(), w.directories.Flush())
}

// The delta is externally sorted by (logical key, source upper bound). Old
// rows are already sorted, so merge them in one stream instead of sorting the
// whole growing account table on every refresh.
type accountRoutingMerge struct {
	ctx              context.Context
	old              *accountRoutingIterator
	oldKey           [21]byte
	oldBound         uint64
	oldOK, oldLoaded bool
	pending          [21]byte
	pendingBound     uint64
	havePending      bool
	emit             func([]byte, uint64) error
}

func (m *accountRoutingMerge) advance() error {
	m.oldLoaded = true
	if m.old == nil {
		m.oldOK = false
		return nil
	}
	var err error
	m.oldKey, m.oldBound, m.oldOK, err = m.old.next(m.ctx)
	return err
}
func (m *accountRoutingMerge) flush() error {
	if !m.havePending {
		return nil
	}
	if !m.oldLoaded {
		if err := m.advance(); err != nil {
			return err
		}
	}
	for m.oldOK && bytes.Compare(m.oldKey[:], m.pending[:]) < 0 {
		if err := m.emit(m.oldKey[:], m.oldBound); err != nil {
			return err
		}
		if err := m.advance(); err != nil {
			return err
		}
	}
	bound := m.pendingBound
	if m.oldOK && m.oldKey == m.pending {
		bound = max(bound, m.oldBound)
		if err := m.advance(); err != nil {
			return err
		}
	}
	if err := m.emit(m.pending[:], bound); err != nil {
		return err
	}
	m.havePending = false
	return nil
}
func (m *accountRoutingMerge) Put(key, value []byte) error {
	if len(key) != 29 || len(value) != 0 || !accountRoutingKey(key[:21]) {
		return errAccountRoutingCorrupt
	}
	var logical [21]byte
	copy(logical[:], key[:21])
	bound := binary.BigEndian.Uint64(key[21:])
	if m.havePending && logical != m.pending {
		if bytes.Compare(m.pending[:], logical[:]) >= 0 {
			return errAccountRoutingCorrupt
		}
		if err := m.flush(); err != nil {
			return err
		}
	}
	if !m.havePending {
		m.pending = logical
		m.pendingBound = bound
		m.havePending = true
	} else {
		m.pendingBound = max(m.pendingBound, bound)
	}
	return nil
}
func (*accountRoutingMerge) Delete([]byte) error { return errAccountRoutingCorrupt }
func (m *accountRoutingMerge) finish() error {
	if err := m.flush(); err != nil {
		return err
	}
	if !m.oldLoaded {
		if err := m.advance(); err != nil {
			return err
		}
	}
	for m.oldOK {
		if err := m.emit(m.oldKey[:], m.oldBound); err != nil {
			return err
		}
		if err := m.advance(); err != nil {
			return err
		}
	}
	return contextError(m.ctx)
}

func accountRoutingDictionaryCost(dir string, ref SegmentRef) (uint64, error) {
	f, err := os.Open(filepath.Join(dir, ref.Path))
	if err != nil {
		return 0, err
	}
	defer f.Close()
	stat, err := f.Stat()
	if err != nil {
		return 0, err
	}
	if stat.Size() < 0 || uint64(stat.Size()) != ref.Size {
		return 0, errAccountRoutingCorrupt
	}
	if ref.Size > systemHistoryRoutingMaxAccessorBytes {
		return 0, errSystemHistoryRoutingBudget
	}
	var raw [12]byte
	if _, err := f.ReadAt(raw[:], 0); err != nil {
		return 0, err
	}
	version := binary.BigEndian.Uint32(raw[8:])
	if !isStateDomainChangeKeyAccessor(version) {
		return 0, errSystemHistoryRoutingBudget
	}
	h, err := decodeStateDomainChangeBinaryAccessorKeyHeader(f, ref.Size)
	return h.keyCount, err
}

// BuildAccountHistoryRoutingContext writes a self-contained auxiliary object.
// It never changes canonical state/history, advances frontiers, or publishes.
func BuildAccountHistoryRoutingContext(ctx context.Context, dir string, manifest *Manifest, maxNew int) (ref SegmentRef, built bool, err error) {
	if err = contextError(ctx); err != nil {
		return ref, false, err
	}
	if manifest == nil || maxNew < 1 {
		return ref, false, nil
	}
	maxNew = min(maxNew, systemHistoryRoutingMaxNewSources)
	var old *accountHistoryRoutingReader
	if oldRef, ok := accountRoutingRef(manifest, 0, ^uint64(0)); ok {
		old, err = openAccountHistoryRouting(ctx, dir, oldRef)
		if err != nil {
			return ref, false, err
		}
		defer old.Close()
	}
	collector, err := etl.NewCollector(etl.Options{TempDir: filepath.Join(dir, "etl"), BufferLimit: 32 << 20})
	if err != nil {
		return ref, false, err
	}
	defer collector.Close()
	manifest = manifestLookupView(manifest)
	cfg, _ := DefaultDomainRegistry().Dataset(SegmentDatasetStateDomainChange)
	var ids [][32]byte
	from, to := ^uint64(0), uint64(0)
	attempted, added := 0, 0
	remaining := accountRoutingNewDictionaryKeys
	for _, history := range manifest.Segments {
		if err := contextError(ctx); err != nil {
			return ref, false, err
		}
		if history.Kind != SegmentHistory || history.normalizedDataset() != SegmentDatasetStateDomainChange || !isStateDomainChangeBinarySegmentPath(history.Path) {
			continue
		}
		accessor, ok := cfg.HistoryAccessorRef(manifest, history)
		if !ok {
			return ref, false, errAccountRoutingCorrupt
		}
		id := systemHistoryRoutingPairID(history, accessor)
		covered := false
		if old != nil {
			_, covered = old.metadata.covered[id]
		}
		if !covered {
			if attempted >= maxNew {
				continue
			}
			attempted++
			cost, e := accountRoutingDictionaryCost(dir, accessor)
			if errors.Is(e, errSystemHistoryRoutingBudget) {
				continue
			}
			if e != nil {
				return ref, false, e
			}
			if cost > remaining {
				continue
			}
			e = iterateHistoryRoutingSourceKeys(ctx, dir, history, accessor, func(key []byte) error {
				if !accountRoutingKey(key) {
					return nil
				}
				return collector.PutEncoded(29, 0, func(out, _ []byte) { copy(out, key); binary.BigEndian.PutUint64(out[21:], history.ToTxNum) })
			})
			if errors.Is(e, errSystemHistoryRoutingBudget) {
				continue
			}
			if e != nil {
				return ref, false, e
			}
			remaining -= cost
			added++
		}
		ids = append(ids, id)
		if len(ids) > systemHistoryRoutingMaxSources {
			return ref, false, nil
		}
		from = min(from, history.FromTxNum)
		to = max(to, history.ToTxNum)
	}
	if added == 0 {
		return ref, false, nil
	}
	if err = os.MkdirAll(filepath.Join(dir, "history"), 0755); err != nil {
		return ref, false, err
	}
	data, err := os.CreateTemp(filepath.Join(dir, "history"), "account-routing-data-*.tmp")
	if err != nil {
		return ref, false, err
	}
	defer func() { data.Close(); os.Remove(data.Name()) }()
	directory, err := os.CreateTemp(filepath.Join(dir, "history"), "account-routing-directory-*.tmp")
	if err != nil {
		return ref, false, err
	}
	defer func() { directory.Close(); os.Remove(directory.Name()) }()
	w := &accountRoutingPageWriter{data: bufio.NewWriterSize(data, 1<<20), directories: bufio.NewWriterSize(directory, 64<<10), maxTx: to}
	merge := &accountRoutingMerge{ctx: ctx, emit: w.put}
	if old != nil {
		merge.old = &accountRoutingIterator{reader: old}
	}
	if _, err = loadHistoryETLContext(ctx, collector, merge); err != nil {
		return ref, false, err
	}
	if err = merge.finish(); err != nil {
		return ref, false, err
	}
	if err = w.finish(); err != nil {
		return ref, false, err
	}
	h := accountRoutingHeader{sources: uint32(len(ids)), rows: w.rows, pages: w.pages, directories: w.dirPages, from: from, to: to}
	if h.metadataBytes() > accountRoutingMaxMetadataBytes || h.fileBytes() > accountRoutingMaxFileBytes {
		return ref, false, nil
	}
	metadata := make([]byte, accountRoutingHeaderBytes)
	copy(metadata, accountRoutingMagic[:])
	binary.BigEndian.PutUint32(metadata[8:], 1)
	binary.BigEndian.PutUint32(metadata[12:], h.sources)
	binary.BigEndian.PutUint64(metadata[16:], h.rows)
	binary.BigEndian.PutUint64(metadata[24:], h.pages)
	binary.BigEndian.PutUint64(metadata[32:], h.directories)
	binary.BigEndian.PutUint64(metadata[40:], h.from)
	binary.BigEndian.PutUint64(metadata[48:], h.to)
	for _, id := range ids {
		metadata = append(metadata, id[:]...)
	}
	metadata = append(metadata, w.roots...)
	root := sha256.Sum256(metadata)
	ref = SegmentRef{Dataset: SegmentDatasetStateDomainChange, Kind: SegmentAccountHistoryRouting, Rows: h.rows, FromTxNum: from, ToTxNum: to, Size: h.fileBytes(), LookupRoot: fmt.Sprintf("sha256:%x", root)}
	if _, err = decodeAccountRoutingHeader(metadata[:accountRoutingHeaderBytes], ref); err != nil {
		return SegmentRef{}, false, err
	}
	if _, err = decodeAccountRoutingMetadata(metadata, h); err != nil {
		return SegmentRef{}, false, err
	}
	output, err := os.CreateTemp(filepath.Join(dir, "history"), "account-routing-output-*.tmp")
	if err != nil {
		return ref, false, err
	}
	name := output.Name()
	defer func() { output.Close(); os.Remove(name) }()
	hash := sha256.New()
	dst := contextWriter{ctx: ctx, w: io.MultiWriter(output, hash)}
	if _, err = dst.Write(metadata); err != nil {
		return ref, false, err
	}
	for _, input := range []*os.File{directory, data} {
		if _, err = input.Seek(0, io.SeekStart); err != nil {
			return ref, false, err
		}
		if _, err = io.Copy(dst, contextReader{ctx: ctx, r: input}); err != nil {
			return ref, false, err
		}
	}
	if err = output.Sync(); err != nil {
		return ref, false, err
	}
	if err = output.Close(); err != nil {
		return ref, false, err
	}
	if err = contextError(ctx); err != nil {
		return ref, false, err
	}
	ref.Checksum = fmt.Sprintf("sha256:%x", hash.Sum(nil))
	ref.Path = fmt.Sprintf("history/account-routing-%d-%d-%x.route", from, to, hash.Sum(nil))
	if err = os.Rename(name, filepath.Join(dir, ref.Path)); err != nil {
		return ref, false, err
	}
	if err = syncSnapshotDir(filepath.Join(dir, "history")); err != nil {
		return ref, false, err
	}
	return ref, true, nil
}

func checkAccountHistoryRoutingContext(ctx context.Context, dir string, manifest *Manifest, ref SegmentRef) error {
	r, err := openAccountHistoryRouting(ctx, dir, ref)
	if err != nil {
		return err
	}
	defer r.Close()
	// Whole-file authentication remains mandatory for strict snapshot import.
	if _, err := r.file.Seek(0, io.SeekStart); err != nil {
		return err
	}
	hash := sha256.New()
	if _, err := io.Copy(hash, contextReader{ctx: ctx, r: r.file}); err != nil {
		return err
	}
	if fmt.Sprintf("sha256:%x", hash.Sum(nil)) != ref.Checksum {
		return errAccountRoutingCorrupt
	}
	collector, err := etl.NewCollector(etl.Options{TempDir: filepath.Join(dir, "etl"), BufferLimit: 32 << 20})
	if err != nil {
		return err
	}
	defer collector.Close()
	manifest = manifestLookupView(manifest)
	cfg, _ := DefaultDomainRegistry().Dataset(SegmentDatasetStateDomainChange)
	for _, history := range manifest.Segments {
		if history.Kind != SegmentHistory || history.normalizedDataset() != SegmentDatasetStateDomainChange {
			continue
		}
		accessor, ok := cfg.HistoryAccessorRef(manifest, history)
		if !ok {
			continue
		}
		if _, covered := r.metadata.covered[systemHistoryRoutingPairID(history, accessor)]; !covered {
			continue
		}
		if err := iterateHistoryRoutingSourceKeys(ctx, dir, history, accessor, func(key []byte) error {
			if !accountRoutingKey(key) {
				return nil
			}
			return collector.PutEncoded(29, 0, func(out, _ []byte) { copy(out, key); binary.BigEndian.PutUint64(out[21:], history.ToTxNum) })
		}); err != nil {
			return err
		}
	}
	it := &accountRoutingIterator{reader: r}
	var current [21]byte
	var bound uint64
	var present bool
	advance := func() error { var e error; current, bound, present, e = it.next(ctx); return e }
	if err := advance(); err != nil {
		return err
	}
	checker := &accountRoutingMerge{ctx: ctx, emit: func(key []byte, want uint64) error {
		for present && bytes.Compare(current[:], key) < 0 {
			if err := advance(); err != nil {
				return err
			}
		}
		if !present || !bytes.Equal(current[:], key) || bound < want {
			return errors.New("snapshots: account history routing omitted or underestimated a covered key")
		}
		return nil
	}}
	if _, err := loadHistoryETLContext(ctx, collector, checker); err != nil {
		return err
	}
	if err := checker.finish(); err != nil {
		return err
	}
	for present {
		if err := advance(); err != nil {
			return err
		}
	}
	return contextError(ctx)
}

func CheckAccountHistoryRouting(dir string, ref SegmentRef) error {
	manifest, err := LoadProductionManifest(dir)
	if err != nil {
		return err
	}
	return checkAccountHistoryRoutingContext(context.Background(), dir, manifest, ref)
}

func refreshAccountHistoryRoutingContext(ctx context.Context, dir string, busy bool) (bool, error) {
	if !historyReadLeaseGate.TryRLock() {
		return false, nil
	}
	defer historyReadLeaseGate.RUnlock()
	manifest, err := loadProductionManifestReadOnly(dir)
	if os.IsNotExist(err) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	manifest = manifestLookupView(manifest)
	var old *accountHistoryRoutingReader
	if ref, ok := accountRoutingRef(manifest, 0, ^uint64(0)); ok {
		old, err = openAccountHistoryRouting(ctx, dir, ref)
		if err != nil {
			return false, err
		}
		defer old.Close()
	}
	count, uncovered := 0, 0
	cfg, _ := DefaultDomainRegistry().Dataset(SegmentDatasetStateDomainChange)
	for _, history := range manifest.Segments {
		if history.Kind != SegmentHistory || history.normalizedDataset() != SegmentDatasetStateDomainChange {
			continue
		}
		count++
		accessor, ok := cfg.HistoryAccessorRef(manifest, history)
		if !ok {
			continue
		}
		if old == nil {
			uncovered++
		} else if _, ok := old.metadata.covered[systemHistoryRoutingPairID(history, accessor)]; !ok {
			uncovered++
		}
	}
	if count < systemHistoryRoutingMaxNewSources && old == nil || uncovered == 0 || busy && uncovered < systemHistoryRoutingMaxNewSources {
		return false, nil
	}
	ref, built, err := BuildAccountHistoryRoutingContext(ctx, dir, manifest, systemHistoryRoutingMaxNewSources)
	if err != nil || !built {
		return false, err
	}
	if err := contextError(ctx); err != nil {
		return false, err
	}
	_, err = NewAggregator(dir).Integrate(manifest.VisibleTxStart, manifest.VisibleTxEnd, []SegmentRef{ref})
	return err == nil, err
}
