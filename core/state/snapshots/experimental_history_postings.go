package snapshots

// Experimental accessor posting format. Key dictionaries remain outside this
// experiment: the caller supplies the existing key, fromTx and posting count,
// just as V7 does. No existing .kv reader accepts these markers.

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"math"
	"sort"
)

const experimentalPostingFrameEntries = 128
const experimentalPostingDirectoryBytes = 16

type ExperimentalHistoryPosting struct {
	TxNum         uint64 `json:"tx_num"`
	Offset        uint64 `json:"offset"`
	RecordOrdinal uint64 `json:"record_ordinal"`
}

type ExperimentalHistoryPostings struct {
	data     []byte
	fromTx   uint64
	count    uint64
	indirect bool
	frames   []experimentalHistoryIndexFrame
}

// experimentalPostingChecksum binds the payload to its unchanged external key
// metadata, including an empty byte key. CRC detects corruption, not forgery.
func experimentalPostingChecksum(key []byte, fromTx, count uint64, data []byte) uint32 {
	var bounds [16]byte
	binary.BigEndian.PutUint64(bounds[:8], fromTx)
	binary.BigEndian.PutUint64(bounds[8:], count)
	sum := crc32.ChecksumIEEE(key)
	sum = crc32.Update(sum, crc32.IEEETable, bounds[:])
	return crc32.Update(sum, crc32.IEEETable, data)
}

// EncodeExperimentalHistoryPostings retains duplicates of TxNum (multiple
// changes to a key in one transaction), but requires increasing record ordinals
// and offsets. Indirect mode omits every offset and uses a shared record locator.
// Both modes choose adaptive columns or raw delta varints per 128-posting frame.
func EncodeExperimentalHistoryPostings(ctx context.Context, key []byte, fromTx uint64, rows []ExperimentalHistoryPosting, indirect bool) ([]byte, error) {
	for i, row := range rows {
		if err := contextError(ctx); err != nil {
			return nil, err
		}
		if row.TxNum < fromTx || i > 0 && (row.TxNum < rows[i-1].TxNum || row.Offset <= rows[i-1].Offset || row.RecordOrdinal <= rows[i-1].RecordOrdinal) {
			return nil, errors.New("experimental postings: unordered posting")
		}
	}
	marker := byte(0xc0)
	if indirect {
		marker++
	}
	frames := (len(rows) + experimentalPostingFrameEntries - 1) / experimentalPostingFrameEntries
	if frames > (math.MaxInt-1)/experimentalPostingDirectoryBytes {
		return nil, errors.New("experimental postings: directory overflow")
	}
	var out []byte
	if frames > 1 {
		out = make([]byte, 1+frames*experimentalPostingDirectoryBytes)
		out[0] = marker
	} else {
		out = []byte{marker}
	}
	for f := 0; f < frames; f++ {
		if err := contextError(ctx); err != nil {
			return nil, err
		}
		start := f * experimentalPostingFrameEntries
		part := rows[start:min(start+experimentalPostingFrameEntries, len(rows))]
		encoded, raw := experimentalEncodePostingFrame(fromTx, part, indirect)
		if frames == 1 {
			if raw {
				out[0] |= 2
			}
		} else {
			frameMarker := byte(0)
			if raw {
				frameMarker = 1
			}
			encoded = append([]byte{frameMarker}, encoded...)
			dir := out[1+f*experimentalPostingDirectoryBytes:][:experimentalPostingDirectoryBytes]
			binary.BigEndian.PutUint64(dir[:8], part[0].TxNum)
			binary.BigEndian.PutUint32(dir[8:12], uint32(len(encoded)))
			binary.BigEndian.PutUint32(dir[12:16], crc32.ChecksumIEEE(encoded))
		}
		out = append(out, encoded...)
	}
	return binary.BigEndian.AppendUint32(out, experimentalPostingChecksum(key, fromTx, uint64(len(rows)), out)), contextError(ctx)
}

func experimentalEncodePostingFrame(fromTx uint64, rows []ExperimentalHistoryPosting, indirect bool) ([]byte, bool) {
	tx, ord, off := make([]uint64, len(rows)-1), make([]uint64, len(rows)-1), make([]uint64, len(rows)-1)
	base := binary.AppendUvarint(nil, rows[0].TxNum-fromTx)
	base = binary.AppendUvarint(base, rows[0].RecordOrdinal)
	if !indirect {
		base = binary.AppendUvarint(base, rows[0].Offset)
	}
	raw := append([]byte(nil), base...)
	for i := 1; i < len(rows); i++ {
		tx[i-1] = rows[i].TxNum - rows[i-1].TxNum
		ord[i-1] = rows[i].RecordOrdinal - rows[i-1].RecordOrdinal
		off[i-1] = rows[i].Offset - rows[i-1].Offset
		raw = binary.AppendUvarint(raw, tx[i-1])
		raw = binary.AppendUvarint(raw, ord[i-1])
		if !indirect {
			raw = binary.AppendUvarint(raw, off[i-1])
		}
	}
	adaptive := append([]byte(nil), base...)
	adaptive = append(adaptive, experimentalEncodeColumn(tx)...)
	adaptive = append(adaptive, experimentalEncodeColumn(ord)...)
	if !indirect {
		adaptive = append(adaptive, experimentalEncodeColumn(off)...)
	}
	if len(raw) <= len(adaptive) {
		return raw, true
	}
	return adaptive, false
}

func OpenExperimentalHistoryPostings(ctx context.Context, key []byte, fromTx, count uint64, serialized []byte) (*ExperimentalHistoryPostings, error) {
	if len(serialized) < 5 || serialized[0]&0xfc != 0xc0 {
		return nil, errors.New("experimental postings: invalid marker or short list")
	}
	// A single frame cannot contain arbitrary trailing gigabytes. Check before
	// copying and before narrowing its length into the frame's uint32 field.
	if count <= experimentalPostingFrameEntries && !experimentalSinglePostingBodyFits(count, uint64(len(serialized)-5)) {
		return nil, errors.New("experimental postings: single frame exceeds encoding bound")
	}
	data := append([]byte(nil), serialized...)
	if binary.BigEndian.Uint32(data[len(data)-4:]) != experimentalPostingChecksum(key, fromTx, count, data[:len(data)-4]) {
		return nil, errors.New("experimental postings: checksum or external key/range/count mismatch")
	}
	r := &ExperimentalHistoryPostings{data: data, fromTx: fromTx, count: count, indirect: data[0]&1 != 0}
	frameCount := count / experimentalPostingFrameEntries
	if count%experimentalPostingFrameEntries != 0 {
		frameCount++
	}
	if frameCount == 0 {
		if len(data) != 5 || data[0]&2 != 0 {
			return nil, errors.New("experimental postings: invalid empty list")
		}
		return r, contextError(ctx)
	}
	end := uint64(1)
	if frameCount == 1 {
		r.frames = []experimentalHistoryIndexFrame{{offset: 1, length: uint32(len(data) - 5)}}
		end = uint64(len(data) - 4)
	} else {
		if data[0]&2 != 0 || frameCount > uint64((len(data)-5)/experimentalPostingDirectoryBytes) {
			return nil, errors.New("experimental postings: invalid frame directory")
		}
		end += frameCount * experimentalPostingDirectoryBytes
		for f := uint64(0); f < frameCount; f++ {
			dir := data[1+f*experimentalPostingDirectoryBytes:][:experimentalPostingDirectoryBytes]
			frame := experimentalHistoryIndexFrame{firstTx: binary.BigEndian.Uint64(dir[:8]), offset: end, length: binary.BigEndian.Uint32(dir[8:12]), crc: binary.BigEndian.Uint32(dir[12:16])}
			if frame.length == 0 || uint64(frame.length) > uint64(len(data)-4)-end {
				return nil, errors.New("experimental postings: frame exceeds list")
			}
			r.frames = append(r.frames, frame)
			end += uint64(frame.length)
		}
	}
	if end != uint64(len(data)-4) {
		return nil, errors.New("experimental postings: trailing bytes")
	}
	var prev ExperimentalHistoryPosting
	for f := range r.frames {
		if err := contextError(ctx); err != nil {
			return nil, err
		}
		rows, err := r.decodeFrame(f)
		if err != nil {
			return nil, err
		}
		if len(r.frames) > 1 && r.frames[f].firstTx != rows[0].TxNum {
			return nil, errors.New("experimental postings: incorrect directory transaction")
		}
		r.frames[f].firstTx = rows[0].TxNum
		if f > 0 && (rows[0].TxNum < prev.TxNum || rows[0].RecordOrdinal <= prev.RecordOrdinal || !r.indirect && rows[0].Offset <= prev.Offset) {
			return nil, errors.New("experimental postings: unordered frame boundary")
		}
		prev = rows[len(rows)-1]
	}
	return r, contextError(ctx)
}

func experimentalSinglePostingBodyFits(count, bytes uint64) bool {
	if count == 0 {
		return bytes == 0
	}
	// Three uint64 columns: a <=10-byte base, up to count-1 maximal
	// varints, and column selectors/constant encoding. Bit packing is smaller.
	return count <= experimentalPostingFrameEntries && bytes <= 3*(count*10+12)
}

func (r *ExperimentalHistoryPostings) decodeFrame(f int) ([]ExperimentalHistoryPosting, error) {
	frame := r.frames[f]
	data := r.data[frame.offset : frame.offset+uint64(frame.length)]
	raw := r.data[0]&2 != 0
	if len(r.frames) > 1 {
		if crc32.ChecksumIEEE(data) != frame.crc || len(data) == 0 || data[0] > 1 {
			return nil, errors.New("experimental postings: corrupt frame")
		}
		raw, data = data[0] == 1, data[1:]
	}
	pos := 0
	a, err := experimentalReadUvarint(data, &pos)
	if err != nil || a > math.MaxUint64-r.fromTx {
		return nil, fmt.Errorf("experimental postings: invalid first transaction: %v", err)
	}
	ordinal, err := experimentalReadUvarint(data, &pos)
	if err != nil {
		return nil, err
	}
	offset := uint64(0)
	if !r.indirect {
		offset, err = experimentalReadUvarint(data, &pos)
		if err != nil {
			return nil, err
		}
	}
	n := int(min(uint64(experimentalPostingFrameEntries), r.count-uint64(f)*experimentalPostingFrameEntries))
	var tx, ord, off []uint64
	if raw {
		tx, ord, off = make([]uint64, n-1), make([]uint64, n-1), make([]uint64, n-1)
		for i := range tx {
			if tx[i], err = experimentalReadUvarint(data, &pos); err != nil {
				return nil, err
			}
			if ord[i], err = experimentalReadUvarint(data, &pos); err != nil {
				return nil, err
			}
			if !r.indirect {
				if off[i], err = experimentalReadUvarint(data, &pos); err != nil {
					return nil, err
				}
			}
		}
	} else {
		if tx, err = experimentalDecodeColumn(data, &pos, n-1); err != nil {
			return nil, err
		}
		if ord, err = experimentalDecodeColumn(data, &pos, n-1); err != nil {
			return nil, err
		}
		if !r.indirect {
			if off, err = experimentalDecodeColumn(data, &pos, n-1); err != nil {
				return nil, err
			}
		}
	}
	if pos != len(data) {
		return nil, errors.New("experimental postings: trailing frame data")
	}
	rows := make([]ExperimentalHistoryPosting, n)
	txNum := r.fromTx + a
	for i := range rows {
		if i > 0 {
			if tx[i-1] > math.MaxUint64-txNum || ord[i-1] == 0 || ord[i-1] > math.MaxUint64-ordinal || !r.indirect && (off[i-1] == 0 || off[i-1] > math.MaxUint64-offset) {
				return nil, errors.New("experimental postings: overflowing or unordered delta")
			}
			txNum += tx[i-1]
			ordinal += ord[i-1]
			if !r.indirect {
				offset += off[i-1]
			}
		}
		rows[i] = ExperimentalHistoryPosting{TxNum: txNum, Offset: offset, RecordOrdinal: ordinal}
	}
	return rows, nil
}

// LowerBound preserves the first occurrence even when the same transaction
// spans multiple frames. Only the selected posting's offset is resolved.
func (r *ExperimentalHistoryPostings) LowerBound(ctx context.Context, target uint64, locator *ExperimentalHistoryLocator, source ExperimentalHistoryRecordSource) (ExperimentalHistoryPosting, bool, ExperimentalHistoryLookupStats, error) {
	return r.lookup(ctx, target, false, locator, source)
}

func (r *ExperimentalHistoryPostings) Exact(ctx context.Context, target uint64, locator *ExperimentalHistoryLocator, source ExperimentalHistoryRecordSource) (ExperimentalHistoryPosting, bool, ExperimentalHistoryLookupStats, error) {
	return r.lookup(ctx, target, true, locator, source)
}

func (r *ExperimentalHistoryPostings) lookup(ctx context.Context, target uint64, exact bool, locator *ExperimentalHistoryLocator, source ExperimentalHistoryRecordSource) (ExperimentalHistoryPosting, bool, ExperimentalHistoryLookupStats, error) {
	var zero ExperimentalHistoryPosting
	var stats ExperimentalHistoryLookupStats
	if err := contextError(ctx); err != nil {
		return zero, false, stats, err
	}
	if r == nil || len(r.frames) == 0 {
		return zero, false, stats, nil
	}
	// Search >=, then include the preceding frame: duplicates can start there
	// and continue across any number of subsequent equal-firstTx frames.
	f := sort.Search(len(r.frames), func(i int) bool { return r.frames[i].firstTx >= target })
	if f > 0 {
		f--
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
		if r.indirect {
			row.Offset, err = locator.resolve(ctx, row.RecordOrdinal, source, &stats)
			if err != nil {
				return zero, false, stats, err
			}
		}
		return row, true, stats, nil
	}
	return zero, false, stats, nil
}

// Scan resolves indirect offsets monotonically, starting from the nearest
// global restart each time. It never scans an unbounded gap between key hits.
func (r *ExperimentalHistoryPostings) Scan(ctx context.Context, locator *ExperimentalHistoryLocator, source ExperimentalHistoryRecordSource, visit func(ExperimentalHistoryPosting) error) (ExperimentalHistoryLookupStats, error) {
	var stats ExperimentalHistoryLookupStats
	if r == nil || visit == nil {
		return stats, errors.New("experimental postings: nil reader or visitor")
	}
	var previous ExperimentalHistoryPosting
	previousPresent := false
	for f := range r.frames {
		if err := contextError(ctx); err != nil {
			return stats, err
		}
		rows, err := r.decodeFrame(f)
		stats.FramesDecoded++
		if err != nil {
			return stats, err
		}
		for _, row := range rows {
			if r.indirect {
				if locator == nil || row.RecordOrdinal >= locator.count {
					return stats, errors.New("experimental postings: missing record locator or ordinal outside segment")
				}
				if previousPresent && previous.RecordOrdinal/uint64(locator.stride) == row.RecordOrdinal/uint64(locator.stride) {
					row.Offset, err = experimentalSkipRecords(ctx, source, previous.Offset, row.RecordOrdinal-previous.RecordOrdinal, &stats)
					if err == nil {
						err = experimentalValidateRecordOffset(ctx, source, row.Offset, &stats)
					}
				} else {
					row.Offset, err = locator.resolve(ctx, row.RecordOrdinal, source, &stats)
				}
				if err != nil {
					return stats, err
				}
			}
			if err := visit(row); err != nil {
				return stats, err
			}
			previous, previousPresent = row, true
		}
	}
	return stats, contextError(ctx)
}

// ExperimentalHistoryLocator is one shared sparse table per history segment.
// Serialized cost is exactly 48+8*ceil(recordCount/stride) bytes. Its stride is
// in global records, so offset recovery always skips <stride records regardless
// of how infrequently a key changes. This trades bytes for extra .seg reads.
type ExperimentalHistoryLocator struct {
	count       uint64
	stride      uint32
	logicalSize uint64
	offsets     []uint64
}

func BuildExperimentalHistoryLocator(ctx context.Context, source ExperimentalHistoryRecordSource, firstOffset, recordCount uint64, stride uint32) ([]byte, ExperimentalHistoryLookupStats, error) {
	var stats ExperimentalHistoryLookupStats
	if stride == 0 || stride > 1024 {
		return nil, stats, errors.New("experimental locator: stride must be 1..1024")
	}
	samples := recordCount / uint64(stride)
	if recordCount%uint64(stride) != 0 {
		samples++
	}
	if samples > uint64((math.MaxInt-48)/8) {
		return nil, stats, errors.New("experimental locator: sample table overflow")
	}
	// Check the scan limit before allocating or reading. The default is 1M.
	limit := source.MaxScanRecords
	if limit == 0 {
		limit = 1_000_000
	}
	if recordCount > limit {
		return nil, stats, errors.New("experimental locator: source record budget exceeded")
	}
	out := make([]byte, 44, 48+samples*8)
	copy(out[:8], "gtexloc1")
	binary.BigEndian.PutUint64(out[8:16], recordCount)
	binary.BigEndian.PutUint32(out[16:20], stride)
	binary.BigEndian.PutUint64(out[24:32], source.LogicalSize)
	binary.BigEndian.PutUint64(out[32:40], samples)
	offset := firstOffset
	for ordinal := uint64(0); ordinal < recordCount; ordinal++ {
		if ordinal%uint64(stride) == 0 {
			out = binary.BigEndian.AppendUint64(out, offset)
		}
		var err error
		offset, err = experimentalSkipRecords(ctx, source, offset, 1, &stats)
		if err != nil {
			return nil, stats, err
		}
	}
	if offset != source.LogicalSize {
		return nil, stats, errors.New("experimental locator: record walk did not end at logical EOF")
	}
	return binary.BigEndian.AppendUint32(out, crc32.ChecksumIEEE(out)), stats, contextError(ctx)
}

func OpenExperimentalHistoryLocator(data []byte) (*ExperimentalHistoryLocator, error) {
	if len(data) < 48 || !bytes.Equal(data[:8], []byte("gtexloc1")) || binary.BigEndian.Uint32(data[20:24]) != 0 || binary.BigEndian.Uint32(data[40:44]) != 0 || crc32.ChecksumIEEE(data[:len(data)-4]) != binary.BigEndian.Uint32(data[len(data)-4:]) {
		return nil, errors.New("experimental locator: invalid header/checksum")
	}
	r := &ExperimentalHistoryLocator{count: binary.BigEndian.Uint64(data[8:16]), stride: binary.BigEndian.Uint32(data[16:20]), logicalSize: binary.BigEndian.Uint64(data[24:32])}
	if r.stride == 0 || r.stride > 1024 {
		return nil, errors.New("experimental locator: invalid stride")
	}
	samples := r.count / uint64(r.stride)
	if r.count%uint64(r.stride) != 0 {
		samples++
	}
	if samples != binary.BigEndian.Uint64(data[32:40]) || samples > uint64((len(data)-48)/8) || 48+samples*8 != uint64(len(data)) {
		return nil, errors.New("experimental locator: invalid table size")
	}
	for i := uint64(0); i < samples; i++ {
		offset := binary.BigEndian.Uint64(data[44+i*8:])
		if offset > math.MaxInt64 || offset >= r.logicalSize || i > 0 && offset <= r.offsets[i-1] {
			return nil, errors.New("experimental locator: invalid or unordered sample offset")
		}
		r.offsets = append(r.offsets, offset)
	}
	return r, nil
}

func (r *ExperimentalHistoryLocator) resolve(ctx context.Context, ordinal uint64, source ExperimentalHistoryRecordSource, stats *ExperimentalHistoryLookupStats) (uint64, error) {
	if r == nil || ordinal >= r.count || source.Reader == nil || source.LogicalSize != r.logicalSize {
		return 0, errors.New("experimental locator: missing/mismatched source or ordinal outside segment")
	}
	f := ordinal / uint64(r.stride)
	offset, err := experimentalSkipRecords(ctx, source, r.offsets[f], ordinal%uint64(r.stride), stats)
	if err == nil {
		err = experimentalValidateRecordOffset(ctx, source, offset, stats)
	}
	return offset, err
}
