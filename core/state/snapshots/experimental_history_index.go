package snapshots

// This file is an isolated format experiment. No production reader, registry,
// builder, manifest or pruning path recognizes its magic. It indexes transaction
// numbers, not account/storage byte keys, and retains every input transaction.

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"math"
	"math/bits"
	"sort"
)

const experimentalHistoryIndexMagic = "gtexidx1"
const experimentalHistoryIndexHeader = 64
const experimentalHistoryIndexDirectoryEntry = 24

// ExperimentalHistoryIndexEntry has exactly the logical fields of a V7 tx
// index entry. Record ordinals must be contiguous: the following ordinal equals
// RecordOrdinal+Count. This proven redundancy is absent from the new payload.
type ExperimentalHistoryIndexEntry struct {
	TxNum         uint64 `json:"tx_num"`
	Offset        uint64 `json:"offset"`
	RecordOrdinal uint64 `json:"record_ordinal"`
	Count         uint64 `json:"count"`
}

type ExperimentalHistoryIndexOptions struct {
	RestartEntries uint32 `json:"restart_entries"` // 1..1024; zero defaults to 256
	SparseOffsets  bool   `json:"sparse_offsets"`
}

type experimentalHistoryIndexFrame struct {
	firstTx uint64
	offset  uint64
	length  uint32
	crc     uint32
}

// ExperimentalHistoryIndex owns immutable serialized bytes. Open validates all
// encoded frames once; individual reads recheck frame CRCs. There is no mutable
// query cache, so concurrent callers do not share a lock or decoded frame.
type ExperimentalHistoryIndex struct {
	data    []byte
	frames  []experimentalHistoryIndexFrame
	count   uint64
	restart uint32
	sparse  bool
}

func experimentalHistoryIndexOptions(opts ExperimentalHistoryIndexOptions) (ExperimentalHistoryIndexOptions, error) {
	if opts.RestartEntries == 0 {
		opts.RestartEntries = 256
	}
	if opts.RestartEntries > 1024 {
		return opts, errors.New("experimental history index: restart exceeds 1024 entries")
	}
	return opts, nil
}

// EncodeExperimentalHistoryIndex serializes actual bytes, not a projected size.
// Each frame stores absolute offset/ordinal once, tx gaps and counts, and (in
// dense mode) offset gaps. A column chooses the shortest of constant, unsigned
// varints, and fixed-bit packed integers. No compression library is required.
func EncodeExperimentalHistoryIndex(ctx context.Context, entries []ExperimentalHistoryIndexEntry, opts ExperimentalHistoryIndexOptions) ([]byte, error) {
	opts, err := experimentalHistoryIndexOptions(opts)
	if err != nil {
		return nil, err
	}
	frameCount := (len(entries) + int(opts.RestartEntries) - 1) / int(opts.RestartEntries)
	if frameCount > (math.MaxInt-experimentalHistoryIndexHeader)/experimentalHistoryIndexDirectoryEntry {
		return nil, errors.New("experimental history index: directory size overflow")
	}
	dataStart := experimentalHistoryIndexHeader + frameCount*experimentalHistoryIndexDirectoryEntry
	out := make([]byte, dataStart)
	for i, entry := range entries {
		if err := contextError(ctx); err != nil {
			return nil, err
		}
		if entry.Count == 0 || entry.Count > math.MaxUint64-entry.RecordOrdinal {
			return nil, errors.New("experimental history index: zero count or ordinal overflow")
		}
		if i > 0 {
			prev := entries[i-1]
			if entry.TxNum <= prev.TxNum || entry.Offset <= prev.Offset || entry.RecordOrdinal != prev.RecordOrdinal+prev.Count {
				return nil, errors.New("experimental history index: unordered or non-contiguous entries")
			}
		}
	}
	for f := 0; f < frameCount; f++ {
		if err := contextError(ctx); err != nil {
			return nil, err
		}
		start := f * int(opts.RestartEntries)
		rows := entries[start:min(start+int(opts.RestartEntries), len(entries))]
		tx, counts, offsets := make([]uint64, len(rows)-1), make([]uint64, len(rows)), make([]uint64, len(rows)-1)
		for i, entry := range rows {
			counts[i] = entry.Count
			if i > 0 {
				tx[i-1], offsets[i-1] = entry.TxNum-rows[i-1].TxNum, entry.Offset-rows[i-1].Offset
			}
		}
		payload := binary.AppendUvarint(nil, rows[0].Offset)
		payload = binary.AppendUvarint(payload, rows[0].RecordOrdinal)
		payload = append(payload, experimentalEncodeColumn(tx)...)
		payload = append(payload, experimentalEncodeColumn(counts)...)
		if !opts.SparseOffsets {
			payload = append(payload, experimentalEncodeColumn(offsets)...)
		}
		dir := out[experimentalHistoryIndexHeader+f*experimentalHistoryIndexDirectoryEntry:][:experimentalHistoryIndexDirectoryEntry]
		binary.BigEndian.PutUint64(dir[:8], rows[0].TxNum)
		binary.BigEndian.PutUint64(dir[8:16], uint64(len(out)))
		binary.BigEndian.PutUint32(dir[16:20], uint32(len(payload)))
		binary.BigEndian.PutUint32(dir[20:24], crc32.ChecksumIEEE(payload))
		out = append(out, payload...)
	}
	copy(out[:8], experimentalHistoryIndexMagic)
	binary.BigEndian.PutUint32(out[8:12], 1)
	if opts.SparseOffsets {
		binary.BigEndian.PutUint32(out[12:16], 1)
	}
	binary.BigEndian.PutUint64(out[16:24], uint64(len(entries)))
	binary.BigEndian.PutUint32(out[24:28], opts.RestartEntries)
	binary.BigEndian.PutUint64(out[32:40], uint64(frameCount))
	binary.BigEndian.PutUint64(out[40:48], uint64(dataStart))
	binary.BigEndian.PutUint64(out[48:56], uint64(len(out)))
	binary.BigEndian.PutUint32(out[56:60], crc32.ChecksumIEEE(out[:56]))
	binary.BigEndian.PutUint32(out[60:64], crc32.ChecksumIEEE(out[64:dataStart]))
	return out, contextError(ctx)
}

func experimentalEncodeColumn(values []uint64) []byte {
	constant, maxValue := true, uint64(0)
	variable := []byte{1}
	for i, v := range values {
		variable = binary.AppendUvarint(variable, v)
		maxValue = max(maxValue, v)
		if i > 0 && v != values[0] {
			constant = false
		}
	}
	if constant {
		value := uint64(0)
		if len(values) != 0 {
			value = values[0]
		}
		return binary.AppendUvarint([]byte{0}, value)
	}
	width := bits.Len64(maxValue)
	packed := make([]byte, 2+(len(values)*width+7)/8)
	packed[0], packed[1] = 2, byte(width)
	if len(packed) >= len(variable) {
		return variable
	}
	bitPos := 0
	for _, value := range values {
		for left := width; left > 0; {
			take := min(8-bitPos%8, left)
			packed[2+bitPos/8] |= byte(value&((1<<take)-1)) << (bitPos % 8)
			value >>= take
			left, bitPos = left-take, bitPos+take
		}
	}
	return packed
}

func experimentalReadUvarint(data []byte, pos *int) (uint64, error) {
	if *pos >= len(data) {
		return 0, io.ErrUnexpectedEOF
	}
	value, n := binary.Uvarint(data[*pos:])
	if n <= 0 || n > 1 && data[*pos+n-1] == 0 {
		return 0, errors.New("experimental history index: invalid or noncanonical varint")
	}
	*pos += n
	return value, nil
}

func experimentalDecodeColumn(data []byte, pos *int, count int) ([]uint64, error) {
	if *pos >= len(data) {
		return nil, io.ErrUnexpectedEOF
	}
	mode := data[*pos]
	*pos++
	out := make([]uint64, count)
	switch mode {
	case 0:
		value, err := experimentalReadUvarint(data, pos)
		if err != nil || count == 0 && value != 0 {
			return nil, fmt.Errorf("experimental history index: invalid constant column: %v", err)
		}
		for i := range out {
			out[i] = value
		}
	case 1:
		for i := range out {
			value, err := experimentalReadUvarint(data, pos)
			if err != nil {
				return nil, err
			}
			out[i] = value
		}
	case 2:
		if *pos >= len(data) {
			return nil, io.ErrUnexpectedEOF
		}
		width := int(data[*pos])
		*pos++
		if width == 0 || width > 64 {
			return nil, errors.New("experimental history index: invalid bit width")
		}
		byteLen := (count*width + 7) / 8
		if byteLen > len(data)-*pos {
			return nil, io.ErrUnexpectedEOF
		}
		packed := data[*pos : *pos+byteLen]
		*pos += byteLen
		bitPos := 0
		for i := range out {
			for left := width; left > 0; {
				take := min(8-bitPos%8, left)
				piece := uint64(packed[bitPos/8]>>(bitPos%8)) & ((1 << take) - 1)
				out[i] |= piece << (width - left)
				left, bitPos = left-take, bitPos+take
			}
		}
		if bitPos%8 != 0 && packed[len(packed)-1]>>(bitPos%8) != 0 {
			return nil, errors.New("experimental history index: nonzero bit padding")
		}
	default:
		return nil, errors.New("experimental history index: unknown column encoding")
	}
	return out, nil
}

func OpenExperimentalHistoryIndex(ctx context.Context, serialized []byte) (*ExperimentalHistoryIndex, error) {
	if len(serialized) < experimentalHistoryIndexHeader {
		return nil, io.ErrUnexpectedEOF
	}
	data := append([]byte(nil), serialized...)
	if !bytes.Equal(data[:8], []byte(experimentalHistoryIndexMagic)) || binary.BigEndian.Uint32(data[8:12]) != 1 || binary.BigEndian.Uint32(data[12:16]) > 1 || binary.BigEndian.Uint32(data[28:32]) != 0 || binary.BigEndian.Uint32(data[56:60]) != crc32.ChecksumIEEE(data[:56]) {
		return nil, errors.New("experimental history index: invalid header")
	}
	count, restart := binary.BigEndian.Uint64(data[16:24]), binary.BigEndian.Uint32(data[24:28])
	if restart == 0 || restart > 1024 {
		return nil, errors.New("experimental history index: invalid restart count")
	}
	frameCount := count / uint64(restart)
	if count%uint64(restart) != 0 {
		frameCount++
	}
	if frameCount > uint64((len(data)-experimentalHistoryIndexHeader)/experimentalHistoryIndexDirectoryEntry) {
		return nil, errors.New("experimental history index: directory exceeds file")
	}
	dataStart := experimentalHistoryIndexHeader + frameCount*experimentalHistoryIndexDirectoryEntry
	if binary.BigEndian.Uint64(data[32:40]) != frameCount || binary.BigEndian.Uint64(data[40:48]) != dataStart || binary.BigEndian.Uint64(data[48:56]) != uint64(len(data)) || binary.BigEndian.Uint32(data[60:64]) != crc32.ChecksumIEEE(data[experimentalHistoryIndexHeader:dataStart]) {
		return nil, errors.New("experimental history index: invalid directory layout")
	}
	r := &ExperimentalHistoryIndex{data: data, count: count, restart: restart, sparse: binary.BigEndian.Uint32(data[12:16]) == 1}
	end := dataStart
	for f := uint64(0); f < frameCount; f++ {
		dir := data[experimentalHistoryIndexHeader+f*experimentalHistoryIndexDirectoryEntry:][:experimentalHistoryIndexDirectoryEntry]
		frame := experimentalHistoryIndexFrame{firstTx: binary.BigEndian.Uint64(dir[:8]), offset: binary.BigEndian.Uint64(dir[8:16]), length: binary.BigEndian.Uint32(dir[16:20]), crc: binary.BigEndian.Uint32(dir[20:24])}
		if frame.offset != end || uint64(frame.length) > uint64(len(data))-end {
			return nil, errors.New("experimental history index: frame outside file")
		}
		r.frames = append(r.frames, frame)
		end += uint64(frame.length)
	}
	if end != uint64(len(data)) {
		return nil, errors.New("experimental history index: trailing bytes")
	}
	var previous ExperimentalHistoryIndexEntry
	var previousBase uint64
	for f := range r.frames {
		if err := contextError(ctx); err != nil {
			return nil, err
		}
		rows, err := r.decodeFrame(f)
		if err != nil {
			return nil, err
		}
		if f > 0 && (rows[0].TxNum <= previous.TxNum || rows[0].RecordOrdinal != previous.RecordOrdinal+previous.Count || rows[0].Offset <= previousBase || !r.sparse && rows[0].Offset <= previous.Offset) {
			return nil, errors.New("experimental history index: discontinuous frame boundary")
		}
		previous, previousBase = rows[len(rows)-1], rows[0].Offset
	}
	return r, contextError(ctx)
}

func (r *ExperimentalHistoryIndex) decodeFrame(f int) ([]ExperimentalHistoryIndexEntry, error) {
	frame := r.frames[f]
	data := r.data[frame.offset : frame.offset+uint64(frame.length)]
	if crc32.ChecksumIEEE(data) != frame.crc {
		return nil, errors.New("experimental history index: frame checksum mismatch")
	}
	pos := 0
	offset, err := experimentalReadUvarint(data, &pos)
	if err != nil {
		return nil, err
	}
	ordinal, err := experimentalReadUvarint(data, &pos)
	if err != nil {
		return nil, err
	}
	n := int(min(uint64(r.restart), r.count-uint64(f)*uint64(r.restart)))
	tx, err := experimentalDecodeColumn(data, &pos, n-1)
	if err != nil {
		return nil, err
	}
	counts, err := experimentalDecodeColumn(data, &pos, n)
	if err != nil {
		return nil, err
	}
	var offsets []uint64
	if !r.sparse {
		offsets, err = experimentalDecodeColumn(data, &pos, n-1)
		if err != nil {
			return nil, err
		}
	}
	if pos != len(data) {
		return nil, errors.New("experimental history index: frame trailing bytes")
	}
	rows := make([]ExperimentalHistoryIndexEntry, n)
	currentTx := frame.firstTx
	for i := range rows {
		if i > 0 {
			if tx[i-1] == 0 || tx[i-1] > math.MaxUint64-currentTx {
				return nil, errors.New("experimental history index: transaction delta overflow or duplicate")
			}
			currentTx += tx[i-1]
			ordinal += counts[i-1] // prior row checked the sum
			if !r.sparse {
				if offsets[i-1] == 0 || offsets[i-1] > math.MaxUint64-offset {
					return nil, errors.New("experimental history index: offset delta overflow or duplicate")
				}
				offset += offsets[i-1]
			}
		}
		if counts[i] == 0 || counts[i] > math.MaxUint64-ordinal {
			return nil, errors.New("experimental history index: record ordinal overflow or zero count")
		}
		rows[i] = ExperimentalHistoryIndexEntry{TxNum: currentTx, Offset: offset, RecordOrdinal: ordinal, Count: counts[i]}
	}
	return rows, nil
}

// ExperimentalHistoryRecordSource is only required by SparseOffsets. It must
// expose the logical V6 segment (the existing reader transparently decompresses
// physical chunks). MaxScanRecords bounds all record-header reads per operation,
// counting both skipped records and selected-frame validation; zero means 1M.
type ExperimentalHistoryRecordSource struct {
	Reader         io.ReaderAt
	LogicalSize    uint64
	MaxScanRecords uint64
}

type ExperimentalHistoryLookupStats struct {
	FramesDecoded         uint64 `json:"frames_decoded"`
	RecordsSkipped        uint64 `json:"records_skipped"`
	RecordFramesValidated uint64 `json:"record_frames_validated"`
	LogicalBytesSkipped   uint64 `json:"logical_bytes_skipped"`
	HeaderBytesRead       uint64 `json:"header_bytes_read"`
}

// Reject a selected pointer that cannot name a complete V6 frame, including a
// restart hit that skips no records. This is structural validation, not value
// authentication; the caller still binds source identity separately.
func experimentalValidateRecordOffset(ctx context.Context, source ExperimentalHistoryRecordSource, offset uint64, stats *ExperimentalHistoryLookupStats) error {
	if err := contextError(ctx); err != nil {
		return err
	}
	if err := experimentalRecordReadBudget(source, stats, 1); err != nil {
		return err
	}
	if source.Reader == nil || offset > math.MaxInt64 || offset > source.LogicalSize || source.LogicalSize-offset < 4 {
		return errors.New("experimental history index: selected record outside source")
	}
	var header [4]byte
	if _, err := source.Reader.ReadAt(header[:], int64(offset)); err != nil {
		return err
	}
	length := uint64(binary.BigEndian.Uint32(header[:]))
	if length < 17 || length > source.LogicalSize-offset-4 {
		return errors.New("experimental history index: selected V6 record frame is incomplete")
	}
	stats.RecordFramesValidated++
	stats.HeaderBytesRead += 4
	return nil
}

func experimentalSkipRecords(ctx context.Context, source ExperimentalHistoryRecordSource, offset, count uint64, stats *ExperimentalHistoryLookupStats) (uint64, error) {
	if err := experimentalRecordReadBudget(source, stats, count); err != nil {
		return 0, err
	}
	var header [4]byte
	for i := uint64(0); i < count; i++ {
		if err := contextError(ctx); err != nil {
			return 0, err
		}
		if offset > math.MaxInt64 || offset > source.LogicalSize || source.LogicalSize-offset < 4 {
			return 0, io.ErrUnexpectedEOF
		}
		if _, err := source.Reader.ReadAt(header[:], int64(offset)); err != nil {
			return 0, err
		}
		length := uint64(binary.BigEndian.Uint32(header[:]))
		if length < 17 || length > source.LogicalSize-offset-4 {
			return 0, errors.New("experimental history index: invalid V6 record frame length")
		}
		offset += length + 4
		stats.RecordsSkipped++
		stats.LogicalBytesSkipped += length + 4
		stats.HeaderBytesRead += 4
	}
	return offset, nil
}

func experimentalRecordReadBudget(source ExperimentalHistoryRecordSource, stats *ExperimentalHistoryLookupStats, count uint64) error {
	limit := source.MaxScanRecords
	if limit == 0 {
		limit = 1_000_000
	}
	if source.Reader == nil || stats.RecordFramesValidated > limit || stats.RecordsSkipped > limit-stats.RecordFramesValidated || count > limit-stats.RecordFramesValidated-stats.RecordsSkipped {
		return errors.New("experimental history index: missing source or sparse record-read budget exceeded")
	}
	return nil
}

// LowerBound returns the first entry with TxNum >= target. Exact excludes the
// following transaction when the requested tx has no changes. Lookup decodes at
// most two restart frames; sparse mode additionally walks record lengths.
func (r *ExperimentalHistoryIndex) LowerBound(ctx context.Context, target uint64, source ExperimentalHistoryRecordSource) (ExperimentalHistoryIndexEntry, bool, ExperimentalHistoryLookupStats, error) {
	return r.lookup(ctx, target, false, source)
}

func (r *ExperimentalHistoryIndex) Exact(ctx context.Context, target uint64, source ExperimentalHistoryRecordSource) (ExperimentalHistoryIndexEntry, bool, ExperimentalHistoryLookupStats, error) {
	return r.lookup(ctx, target, true, source)
}

func (r *ExperimentalHistoryIndex) lookup(ctx context.Context, target uint64, exact bool, source ExperimentalHistoryRecordSource) (ExperimentalHistoryIndexEntry, bool, ExperimentalHistoryLookupStats, error) {
	var stats ExperimentalHistoryLookupStats
	var zero ExperimentalHistoryIndexEntry
	if err := contextError(ctx); err != nil {
		return zero, false, stats, err
	}
	if r == nil || len(r.frames) == 0 {
		return zero, false, stats, nil
	}
	f := sort.Search(len(r.frames), func(i int) bool { return r.frames[i].firstTx > target }) - 1
	if f < 0 {
		f = 0
	}
	for ; f < len(r.frames); f++ {
		rows, err := r.decodeFrame(f)
		stats.FramesDecoded++
		if err != nil {
			return zero, false, stats, err
		}
		i := sort.Search(len(rows), func(i int) bool { return rows[i].TxNum >= target })
		if i == len(rows) {
			continue
		}
		row := rows[i]
		if exact && row.TxNum != target {
			return zero, false, stats, nil
		}
		if r.sparse {
			if i != 0 {
				row.Offset, err = experimentalSkipRecords(ctx, source, rows[0].Offset, row.RecordOrdinal-rows[0].RecordOrdinal, &stats)
				if err != nil {
					return zero, false, stats, err
				}
			}
			if err := experimentalValidateRecordOffset(ctx, source, row.Offset, &stats); err != nil {
				return zero, false, stats, err
			}
			if row.Count > (source.LogicalSize-row.Offset)/21 {
				return zero, false, stats, errors.New("experimental history index: tx count cannot fit remaining V6 source")
			}
		}
		return row, true, stats, nil
	}
	return zero, false, stats, nil
}

// Scan visits every entry once. Sparse mode walks source record lengths once
// within each frame; unlike repeated point queries, this is linear work. The
// scan budget applies to the entire operation, not separately to each frame.
func (r *ExperimentalHistoryIndex) Scan(ctx context.Context, source ExperimentalHistoryRecordSource, visit func(ExperimentalHistoryIndexEntry) error) (ExperimentalHistoryLookupStats, error) {
	var stats ExperimentalHistoryLookupStats
	if visit == nil {
		return stats, errors.New("experimental history index: nil scan visitor")
	}
	if r == nil {
		return stats, nil
	}
	for f := range r.frames {
		if err := contextError(ctx); err != nil {
			return stats, err
		}
		rows, err := r.decodeFrame(f)
		stats.FramesDecoded++
		if err != nil {
			return stats, err
		}
		for i := range rows {
			if r.sparse && i > 0 {
				rows[i].Offset, err = experimentalSkipRecords(ctx, source, rows[i-1].Offset, rows[i-1].Count, &stats)
				if err != nil {
					return stats, err
				}
			}
			if r.sparse {
				if err := experimentalValidateRecordOffset(ctx, source, rows[i].Offset, &stats); err != nil {
					return stats, err
				}
				if rows[i].Count > (source.LogicalSize-rows[i].Offset)/21 {
					return stats, errors.New("experimental history index: tx count cannot fit remaining V6 source")
				}
			}
			if err := visit(rows[i]); err != nil {
				return stats, err
			}
		}
	}
	return stats, contextError(ctx)
}
