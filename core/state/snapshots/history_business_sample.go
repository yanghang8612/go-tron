package snapshots

// Read-only, header-only diagnostics. This file does not register a format,
// publish a manifest or hydrate previous values. Sampling is over nonempty
// indexed transactions, not uniform over bytes, keys or block time.
import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"math"
	"math/rand"
	"os"
	"path/filepath"
	"sort"

	"github.com/klauspost/compress/zstd"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/state/kvdomains"
)

type HistoryBusinessSampleOptions struct {
	Seed                uint64 `json:"seed"`
	Windows             uint64 `json:"windows"`
	TxEntriesPerWindow  uint64 `json:"tx_entries_per_window"`
	MaxRecords          uint64 `json:"max_records"`
	MaxRecordsPerWindow uint64 `json:"max_records_per_window"`
	MaxReadBytes        uint64 `json:"max_read_bytes"`
	MaxMetadataBytes    uint64 `json:"max_metadata_bytes"`
	MaxChunkBytes       uint64 `json:"max_chunk_bytes"`
}
type HistoryBusinessSampleStats struct {
	Records          uint64 `json:"records"`
	Present          uint64 `json:"present"`
	EmptyPresent     uint64 `json:"empty_present"`
	PreviousBytes    uint64 `json:"previous_bytes"`
	MaxPreviousBytes uint64 `json:"max_previous_bytes"`
}
type HistoryBusinessSampleKey struct {
	KeyID        uint32 `json:"key_id"`
	Category     string `json:"category"`
	OwnerHex     string `json:"owner_hex"`
	Generation   uint64 `json:"generation"`
	KeyBytes     int    `json:"key_bytes"`
	KeyPrefixHex string `json:"key_prefix_hex"`
	KeySHA256    string `json:"key_sha256"`
	HistoryBusinessSampleStats
}
type HistoryBusinessSampleWindow struct {
	IndexStart   uint64                                 `json:"index_start"`
	IndexEntries uint64                                 `json:"index_entries"`
	FirstTxNum   uint64                                 `json:"first_tx_num"`
	LastTxNum    uint64                                 `json:"last_tx_num"`
	Records      uint64                                 `json:"records"`
	Complete     bool                                   `json:"complete"`
	StopReason   string                                 `json:"stop_reason,omitempty"`
	Categories   map[string]*HistoryBusinessSampleStats `json:"categories"`
}
type HistoryBusinessSampleReport struct {
	Options             HistoryBusinessSampleOptions           `json:"options"`
	Sources             []SegmentRef                           `json:"sources"`
	SourceRecords       uint64                                 `json:"source_records"`
	SourceKeys          uint64                                 `json:"source_keys"`
	IndexEntries        uint64                                 `json:"index_entries"`
	LogicalHistoryBytes uint64                                 `json:"logical_history_bytes"`
	ChargedReadBytes    uint64                                 `json:"charged_read_bytes"`
	Records             uint64                                 `json:"sampled_records"`
	FileStatsStable     bool                                   `json:"file_stats_stable"`
	Windows             []HistoryBusinessSampleWindow          `json:"windows"`
	Categories          map[string]*HistoryBusinessSampleStats `json:"categories"`
	TopKeys             []*HistoryBusinessSampleKey            `json:"top_keys"`
	Limitations         []string                               `json:"limitations"`
}

func (o HistoryBusinessSampleOptions) defaults() (HistoryBusinessSampleOptions, error) {
	if o.Windows == 0 {
		o.Windows = 8
	}
	if o.TxEntriesPerWindow == 0 {
		o.TxEntriesPerWindow = 8
	}
	if o.MaxRecords == 0 {
		o.MaxRecords = 4096
	}
	if o.MaxRecordsPerWindow == 0 {
		o.MaxRecordsPerWindow = 512
	}
	if o.MaxReadBytes == 0 {
		o.MaxReadBytes = 256 << 20
	}
	if o.MaxMetadataBytes == 0 {
		o.MaxMetadataBytes = 32 << 20
	}
	if o.MaxChunkBytes == 0 {
		o.MaxChunkBytes = 32 << 20
	}
	if o.Windows > 64 || o.TxEntriesPerWindow > 256 || o.MaxRecords > 100000 || o.MaxRecordsPerWindow > 100000 || o.MaxReadBytes > 4<<30 || o.MaxMetadataBytes > 32<<20 || o.MaxChunkBytes > 32<<20 {
		return o, errors.New("snapshots: business sample options exceed hard limits")
	}
	return o, nil
}

// SampleHistoryBusiness reads only selected V6 frame headers and key metadata.
// ReadBytes includes metadata and compressed cache misses, not storage-device
// traffic. The input must be an immutable existing trio. No full SHA is run.
func SampleHistoryBusiness(ctx context.Context, dir string, historyRef, indexRef, accessorRef SegmentRef, opts HistoryBusinessSampleOptions) (report HistoryBusinessSampleReport, err error) {
	opts, err = opts.defaults()
	if err != nil {
		return report, err
	}
	report.Options = opts
	report.Sources = []SegmentRef{historyRef, indexRef, accessorRef}
	report.Categories = map[string]*HistoryBusinessSampleStats{}
	budget := &businessSampleBudget{ctx: ctx, limit: opts.MaxReadBytes}
	defer func() { report.ChargedReadBytes = budget.used }()
	for i, ref := range report.Sources {
		if ref.Dataset != SegmentDatasetStateDomainChange || ref.Kind != []SegmentKind{SegmentHistory, SegmentInverted, SegmentAccessor}[i] || ref.FromTxNum != historyRef.FromTxNum || ref.ToTxNum != historyRef.ToTxNum || ref.effectiveAggregationSteps() != historyRef.effectiveAggregationSteps() || !filepath.IsLocal(ref.Path) {
			return report, errors.New("snapshots: mismatched business sample trio")
		}
	}
	history, err := openBusinessSampleFile(dir, historyRef, budget, opts, true)
	if err != nil {
		return report, err
	}
	defer history.Close()
	index, err := openBusinessSampleFile(dir, indexRef, budget, opts, false)
	if err != nil {
		return report, err
	}
	defer index.Close()
	accessor, err := openBusinessSampleFile(dir, accessorRef, budget, opts, false)
	if err != nil {
		return report, err
	}
	defer accessor.Close()
	hh, err := readStateDomainChangeBinaryHeaderAt(history, stateDomainChangeBinarySegmentMagic)
	if err != nil {
		return report, err
	}
	ih, err := readStateDomainChangeBinaryHeaderAt(index, stateDomainChangeBinaryIndexMagic)
	if err != nil {
		return report, err
	}
	ah, err := decodeStateDomainChangeBinaryAccessorV7Header(accessor, accessor.logical)
	if err != nil {
		return report, err
	}
	if hh.version != stateDomainChangeBinaryVersionV6 || ih.version != stateDomainChangeBinaryIndexCurrentVersion || hh.fromTxNum != historyRef.FromTxNum || hh.toTxNum != historyRef.ToTxNum || ih.fromTxNum != hh.fromTxNum || ih.toTxNum != hh.toTxNum || ah.fromTxNum != hh.fromTxNum || ah.toTxNum != hh.toTxNum || ah.recordCount != hh.count || ih.count > hh.count {
		return report, errors.New("snapshots: sample requires matching V6 history and V7 companions")
	}
	digest, err := readStateDomainChangeBinaryV6DictionaryCommitment(history)
	if err != nil {
		return report, err
	}
	if digest != ah.dictionaryDigest {
		return report, errors.New("snapshots: sample dictionary commitment mismatch")
	}
	_, recordStart, err := stateDomainChangeBinaryTxRangeTableBoundsAt(history, history.logical, historyRef, hh)
	if err != nil {
		return report, err
	}
	if recordStart > history.logical || hh.count > (history.logical-recordStart)/21 {
		return report, errors.New("snapshots: sample record count exceeds logical body")
	}
	report.SourceRecords, report.SourceKeys, report.IndexEntries, report.LogicalHistoryBytes = hh.count, ah.keyCount, ih.count, history.logical
	idx, err := openBusinessSampleIndex(index, ih, opts)
	if err != nil {
		return report, err
	}
	keys := map[uint32]*HistoryBusinessSampleKey{}
	var cacheBlock uint64 = math.MaxUint64
	var cache []stateDomainChangeBinaryAccessorV6Record
	for _, selection := range businessSampleWindows(ih.count, opts) {
		w := HistoryBusinessSampleWindow{IndexStart: selection[0], IndexEntries: selection[1], Complete: true, Categories: map[string]*HistoryBusinessSampleStats{}}
		var previous stateDomainChangeBinaryTxOffset
		for n := uint64(0); n < selection[1]; n++ {
			if report.Records >= opts.MaxRecords || w.Records >= opts.MaxRecordsPerWindow {
				w.Complete = false
				w.StopReason = "record limit"
				break
			}
			entry, readErr := businessSampleIndexAt(idx, index, selection[0]+n)
			if readErr != nil {
				return report, readErr
			}
			if entry.txNum < hh.fromTxNum || entry.txNum > hh.toTxNum || entry.offset < recordStart || entry.offset >= history.logical || entry.recordIndex >= hh.count || entry.count == 0 || entry.count > hh.count-entry.recordIndex || entry.count > (history.logical-entry.offset)/21 || n > 0 && (entry.txNum <= previous.txNum || entry.recordIndex != previous.recordIndex+previous.count) {
				return report, errors.New("snapshots: invalid sampled index entry")
			}
			if n == 0 {
				w.FirstTxNum = entry.txNum
			}
			w.LastTxNum = entry.txNum
			offset := entry.offset
			for row := uint64(0); row < entry.count; row++ {
				if report.Records >= opts.MaxRecords || w.Records >= opts.MaxRecordsPerWindow {
					w.Complete = false
					w.StopReason = "record limit inside transaction"
					break
				}
				keyID, present, length, next, readErr := readBusinessSampleRecordHeader(history, offset, history.logical, entry.txNum)
				if readErr != nil {
					return report, readErr
				}
				offset = next
				if uint64(keyID) >= ah.keyCount {
					return report, errors.New("snapshots: sampled keyID outside dictionary")
				}
				key := keys[keyID]
				if key == nil {
					blockID := uint64(keyID / stateDomainChangeBinaryAccessorV6BlockKeys)
					if blockID != cacheBlock {
						block, e := stateDomainChangeBinaryAccessorV6ReadBlockDirectoryEntry(accessor, ah, blockID)
						if e != nil {
							return report, e
						}
						if block.rawLen > 64<<10 || block.dataLen&^stateDomainChangeBinaryAccessorV6StoredRaw > 64<<10 {
							return report, errors.New("snapshots: sampled dictionary block exceeds 64KiB cap")
						}
						cache, e = stateDomainChangeBinaryAccessorV6ReadBlock(accessor, accessor.logical, ah, block, uint32(blockID*stateDomainChangeBinaryAccessorV6BlockKeys))
						if e != nil {
							return report, e
						}
						cacheBlock = blockID
					}
					within := int(keyID % stateDomainChangeBinaryAccessorV6BlockKeys)
					if within >= len(cache) {
						return report, errors.New("snapshots: sample key outside dictionary block")
					}
					var change rawdb.StateDomainChange
					if err := decodeStateDomainChangeBinaryAccessorKey(cache[within].key, &change); err != nil {
						return report, err
					}
					sum := sha256.Sum256(change.Key)
					key = &HistoryBusinessSampleKey{KeyID: keyID, Category: StateHistoryBusinessCategory(&change), OwnerHex: hex.EncodeToString(change.Owner[:]), Generation: change.Generation, KeyBytes: len(change.Key), KeyPrefixHex: hex.EncodeToString(change.Key[:min(64, len(change.Key))]), KeySHA256: hex.EncodeToString(sum[:])}
					keys[keyID] = key
				}
				for _, m := range []map[string]*HistoryBusinessSampleStats{report.Categories, w.Categories} {
					if m[key.Category] == nil {
						m[key.Category] = new(HistoryBusinessSampleStats)
					}
					businessSampleAdd(m[key.Category], present, length)
				}
				businessSampleAdd(&key.HistoryBusinessSampleStats, present, length)
				report.Records++
				w.Records++
			}
			if !w.Complete {
				break
			}
			if selection[0]+n+1 < ih.count {
				next, e := businessSampleIndexAt(idx, index, selection[0]+n+1)
				if e != nil {
					return report, e
				}
				if next.offset != offset || next.recordIndex != entry.recordIndex+entry.count {
					return report, errors.New("snapshots: sampled record run does not reach next transaction")
				}
			} else if offset != history.logical {
				return report, errors.New("snapshots: sampled final record run does not reach EOF")
			}
			previous = entry
		}
		report.Windows = append(report.Windows, w)
		if report.Records >= opts.MaxRecords {
			break
		}
	}
	for _, key := range keys {
		report.TopKeys = append(report.TopKeys, key)
	}
	sort.Slice(report.TopKeys, func(i, j int) bool {
		a, b := report.TopKeys[i], report.TopKeys[j]
		if a.PreviousBytes == b.PreviousBytes {
			return a.KeyID < b.KeyID
		}
		return a.PreviousBytes > b.PreviousBytes
	})
	if len(report.TopKeys) > 40 {
		report.TopKeys = report.TopKeys[:40]
	}
	for _, f := range []*businessSampleFile{history, index, accessor} {
		if err := f.stable(); err != nil {
			return report, err
		}
	}
	report.FileStatsStable = true
	report.Limitations = []string{"Header-only length sample: values are skipped and not hashed or decoded; no whole-file SHA or chain authentication.", "Seeded, disjoint strata of nonempty indexed transactions. No byte-weighted, key-uniform or full-chain estimate is implied.", "PreviousBytes is logical previous-image length, not physical compressed bytes. Categories and maxima describe sampled records only.", "Record limits can truncate a window or transaction; inspect Complete/StopReason. Oversized values are counted by header, not silently excluded.", "ChargedReadBytes counts file read requests and compressed cache misses, not storage-device I/O. Metadata and per-chunk caps apply before allocation; deadlines are cooperative between reads.", "KeyID is segment-local; key prefix output is capped at 64 bytes. File stat stability is not a cryptographic immutability proof."}
	return report, nil
}

func businessSampleWindows(count uint64, o HistoryBusinessSampleOptions) [][2]uint64 {
	n := min(o.Windows, count)
	out := make([][2]uint64, 0, n)
	rng := rand.New(rand.NewSource(int64(o.Seed)))
	for i := uint64(0); i < n; i++ {
		lo, hi := count*i/n, count*(i+1)/n
		width := min(o.TxEntriesPerWindow, hi-lo)
		start := lo + uint64(rng.Int63n(int64(hi-lo-width+1)))
		out = append(out, [2]uint64{start, width})
	}
	return out
}
func businessSampleAdd(s *HistoryBusinessSampleStats, present bool, n uint64) {
	s.Records++
	s.PreviousBytes += n
	s.MaxPreviousBytes = max(s.MaxPreviousBytes, n)
	if present {
		s.Present++
		if n == 0 {
			s.EmptyPresent++
		}
	}
}

func readBusinessSampleRecordHeader(r io.ReaderAt, offset, size, tx uint64) (uint32, bool, uint64, uint64, error) {
	if offset > math.MaxInt64 || offset > size || size-offset < 21 {
		return 0, false, 0, 0, io.ErrUnexpectedEOF
	}
	var b [21]byte
	if _, err := r.ReadAt(b[:], int64(offset)); err != nil {
		return 0, false, 0, 0, err
	}
	n := uint64(binary.BigEndian.Uint32(b[17:21]))
	frame := uint64(binary.BigEndian.Uint32(b[:4]))
	if frame != 17+n || n > size-offset-21 || b[16] > 1 || b[16] == 0 && n != 0 || binary.BigEndian.Uint64(b[8:16]) != tx {
		return 0, false, 0, 0, errors.New("snapshots: invalid sampled V6 record header")
	}
	return binary.BigEndian.Uint32(b[4:8]), b[16] == 1, n, offset + 21 + n, nil
}

// StateHistoryBusinessCategory classifies logical state keys, not value codecs.
func StateHistoryBusinessCategory(c *rawdb.StateDomainChange) string {
	if c == nil {
		return "nil"
	}
	base := c.FlatDomain.String()
	if c.FlatDomain != rawdb.StateFlatDomainKVLatest {
		return base
	}
	name := kvdomains.Name(c.Domain)
	if name == "" {
		name = fmt.Sprint(uint16(c.Domain))
	}
	base += "/" + name
	if c.Domain != kvdomains.SystemDelegation {
		return base
	}
	k := c.Key
	sub := "unknown"
	switch {
	case bytes.HasPrefix(k, []byte("dri-")):
		sub = "dri-malformed"
		if len(k) == 25 {
			sub = "dri-address-list"
		}
	case bytes.HasPrefix(k, []byte("drax-")):
		sub = "drax-malformed"
		if len(k) == 27 && k[5] == 0 {
			sub = "drax-0-aggregate"
		}
		if len(k) == 48 && k[5] >= 1 && k[5] <= 4 {
			sub = fmt.Sprintf("drax-%d-directional", k[5])
		}
	case bytes.HasPrefix(k, []byte("dr-")):
		sub = "dr-malformed"
		if len(k) == 45 {
			sub = "dr-v1-resource"
		}
		if len(k) == 46 && k[3] >= 1 && k[3] <= 2 {
			sub = fmt.Sprintf("dr-v2-bucket-%d", k[3])
		}
	}
	return base + "/" + sub
}

type businessSampleBudget struct {
	ctx         context.Context
	limit, used uint64
}

func (b *businessSampleBudget) charge(n uint64) error {
	if err := b.ctx.Err(); err != nil {
		return err
	}
	if n > b.limit-b.used {
		return errors.New("snapshots: business sample read budget exhausted")
	}
	b.used += n
	return nil
}

type businessSampleFile struct {
	file       *os.File
	path       string
	stat       os.FileInfo
	logical    uint64
	compressed *compressedBlockReader
	decoder    *zstd.Decoder
	budget     *businessSampleBudget
}

func (f *businessSampleFile) Close() error {
	if f.decoder != nil {
		f.decoder.Close()
	}
	return f.file.Close()
}
func (f *businessSampleFile) stable() error {
	now, e := f.file.Stat()
	if e != nil {
		return e
	}
	named, e := os.Stat(f.path)
	if e != nil {
		return e
	}
	if !os.SameFile(f.stat, now) || !os.SameFile(f.stat, named) || f.stat.Size() != now.Size() || f.stat.ModTime() != now.ModTime() {
		return errors.New("snapshots: sampled file changed")
	}
	return nil
}
func (f *businessSampleFile) ReadAt(p []byte, off int64) (int, error) {
	if off < 0 {
		return 0, errors.New("snapshots: negative sample read offset")
	}
	cost := uint64(len(p))
	if f.compressed != nil {
		cr := f.compressed
		cost = 0
		end := uint64(off) + uint64(len(p))
		if end < uint64(off) || end > f.logical {
			return 0, io.ErrUnexpectedEOF
		}
		if len(p) > 0 {
			first, last := cr.findBlock(uint64(off)), cr.findBlock(end-1)
			if first < 0 {
				return 0, io.ErrUnexpectedEOF
			}
			// Mirror MRU updates, including eviction within a read crossing more
			// than two chunks. This private reader is used by one goroutine only.
			cached := make([]int, 0, cr.cacheLimit)
			for _, entry := range cr.cache {
				cached = append(cached, entry.idx)
			}
			for i := first; i <= last; i++ {
				found := -1
				for j, entry := range cached {
					if entry == i {
						found = j
						break
					}
				}
				if found < 0 {
					cost += cr.table[i].compressedLen
					if len(cached) < cr.cacheLimit {
						cached = append(cached, 0)
					}
					found = len(cached) - 1
				}
				copy(cached[1:found+1], cached[:found])
				cached[0] = i
			}
		}
	}
	if err := f.budget.charge(cost); err != nil {
		return 0, err
	}
	if f.compressed != nil {
		return f.compressed.ReadAt(p, off)
	}
	return f.file.ReadAt(p, off)
}

func openBusinessSampleFile(dir string, ref SegmentRef, budget *businessSampleBudget, o HistoryBusinessSampleOptions, allowCompressed bool) (out *businessSampleFile, err error) {
	file, err := os.Open(filepath.Join(dir, ref.Path))
	if err != nil {
		return nil, err
	}
	ok := false
	defer func() {
		if !ok {
			file.Close()
		}
	}()
	stat, err := file.Stat()
	if err != nil {
		return nil, err
	}
	if !stat.Mode().IsRegular() || stat.Size() < 8 || ref.Size != 0 && uint64(stat.Size()) != ref.Size {
		return nil, errors.New("snapshots: invalid sample file size/type")
	}
	out = &businessSampleFile{file: file, path: filepath.Join(dir, ref.Path), stat: stat, logical: uint64(stat.Size()), budget: budget}
	var magic [8]byte
	if _, err = out.ReadAt(magic[:], 0); err != nil {
		return nil, err
	}
	if string(magic[:]) != compressedBlockMagic {
		ok = true
		return out, nil
	}
	if !allowCompressed {
		return nil, errors.New("snapshots: sample requires raw V7 index/accessor")
	}
	var h [48]byte
	if _, err = out.ReadAt(h[:], 0); err != nil {
		return nil, err
	}
	var info compressedBlockFooterInfo
	dataOff := uint64(48)
	dataEnd := uint64(stat.Size())
	footer := false
	switch binary.BigEndian.Uint32(h[8:12]) {
	case compressedBlockVersion:
		info = compressedBlockFooterInfo{blockSize: int(binary.BigEndian.Uint32(h[12:16])), recCount: binary.BigEndian.Uint64(h[16:24]), blockCount: binary.BigEndian.Uint64(h[24:32]), uncSize: binary.BigEndian.Uint64(h[32:40]), tableOff: 48}
		if info.blockCount > o.MaxMetadataBytes/28 {
			return nil, errors.New("snapshots: sample compressed metadata cap exceeded")
		}
		info.tableLen = info.blockCount * 28
		dataOff = 48 + info.tableLen
		if binary.BigEndian.Uint64(h[40:48]) != dataOff || dataOff > uint64(stat.Size()) {
			return nil, errors.New("snapshots: invalid sample compressed table bounds")
		}
	case compressedBlockFooterVersion:
		info, err = readCompressedBlockFooterInfo(out, uint64(stat.Size()), h[:])
		if err != nil {
			return nil, err
		}
		footer = true
		dataEnd = info.tableOff
	default:
		return nil, errors.New("snapshots: unsupported sample compression version")
	}
	if info.tableLen > o.MaxMetadataBytes || info.uncSize > math.MaxInt64 || info.blockSize <= 0 || uint64(info.blockSize) > o.MaxChunkBytes {
		return nil, errors.New("snapshots: sample compressed layout exceeds cap")
	}
	table := make([]cbBlock, info.blockCount)
	reader := bufio.NewReaderSize(io.NewSectionReader(out, int64(info.tableOff), int64(info.tableLen)), 32<<10)
	var b [28]byte
	for i := range table {
		if _, err = io.ReadFull(reader, b[:]); err != nil {
			return nil, err
		}
		table[i] = cbBlock{uncompressedStart: binary.BigEndian.Uint64(b[:8]), compressedStart: binary.BigEndian.Uint64(b[8:16]), compressedLen: binary.BigEndian.Uint64(b[16:24]), records: binary.BigEndian.Uint32(b[24:])}
		if table[i].compressedLen > o.MaxChunkBytes {
			return nil, errors.New("snapshots: sample compressed chunk cap exceeded")
		}
	}
	if err = validateCompressedBlockPhysicalTable(table, info.recCount, info.uncSize, dataOff, dataEnd, footer); err != nil {
		return nil, err
	}
	for i := range table {
		n, e := compressedBlockExpectedLen(table, info.uncSize, i)
		if e != nil {
			return nil, e
		}
		if n > o.MaxChunkBytes {
			return nil, errors.New("snapshots: sample decoded chunk cap exceeded")
		}
	}
	dec, err := zstd.NewReader(nil, zstd.WithDecoderConcurrency(1), zstd.WithDecodeAllCapLimit(true), zstd.WithDecoderMaxMemory(o.MaxChunkBytes))
	if err != nil {
		return nil, err
	}
	out.decoder = dec
	out.logical = info.uncSize
	out.compressed = &compressedBlockReader{f: file, dec: dec, blockSize: info.blockSize, recCount: info.recCount, uncSize: info.uncSize, dataOff: dataOff, fileSize: uint64(stat.Size()), table: table, cacheLimit: 2}
	ok = true
	return out, nil
}

func openBusinessSampleIndex(file *businessSampleFile, header stateDomainChangeBinaryHeader, o HistoryBusinessSampleOptions) (*stateDomainChangeBinaryIndexV7Reader, error) {
	var raw [80]byte
	if _, err := file.ReadAt(raw[:], 0); err != nil {
		return nil, err
	}
	frames, dirLen := binary.BigEndian.Uint64(raw[40:48]), binary.BigEndian.Uint64(raw[48:56])
	if dirLen > o.MaxMetadataBytes || frames > o.MaxMetadataBytes/32 || dirLen != frames*32 || frames != ceilDiv(header.count, 256) {
		return nil, errors.New("snapshots: sample index metadata cap exceeded")
	}
	// The existing opener reads exactly its 80-byte header and 32-byte entries.
	if err := file.budget.charge(80 + dirLen); err != nil {
		return nil, err
	}
	r, err := openStateDomainChangeBinaryIndexV7Reader(file.file, file.logical, header)
	if err != nil {
		return nil, err
	}
	for _, f := range r.frames {
		if f.dataLen > 4*binary.MaxVarintLen64*256 {
			return nil, errors.New("snapshots: sample index frame exceeds encoding cap")
		}
	}
	return r, nil
}
func businessSampleIndexAt(r *stateDomainChangeBinaryIndexV7Reader, file *businessSampleFile, index uint64) (stateDomainChangeBinaryTxOffset, error) {
	if index >= r.header.count {
		return stateDomainChangeBinaryTxOffset{}, io.EOF
	}
	frame := index / 256
	if !r.cacheValid || r.cacheIndex != frame {
		if err := file.budget.charge(uint64(r.frames[frame].dataLen)); err != nil {
			return stateDomainChangeBinaryTxOffset{}, err
		}
	} else if err := file.budget.ctx.Err(); err != nil {
		return stateDomainChangeBinaryTxOffset{}, err
	}
	return r.entryAt(index)
}
