package snapshots

// Offline repair changes immutable history files only. These helpers never
// publish a manifest, modify a route, or authorize deletion of either source.

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"reflect"
	"sort"

	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/rawdb/etl"
)

type OfflineHistoryRepairSlice struct {
	SourceRefs []SegmentRef `json:"source_refs"`
	FromTxNum  uint64       `json:"from_tx_num"`
	ToTxNum    uint64       `json:"to_tx_num"`
	path       string
}

type OfflineHistoryRepairPlan struct {
	FromTxNum        uint64 `json:"from_tx_num"`
	ToTxNum          uint64 `json:"to_tx_num"`
	ReplaceFromTxNum uint64 `json:"replace_from_tx_num"`
	ReplaceToTxNum   uint64 `json:"replace_to_tx_num"`
	// Complete trios, each in history/index/accessor order.
	SourceRefs []SegmentRef               `json:"source_refs"`
	Left       *OfflineHistoryRepairSlice `json:"left,omitempty"`
	Right      *OfflineHistoryRepairSlice `json:"right,omitempty"`
}

// PlanOfflineHistoryRepair selects whole overlapping active trios. Only the
// middle interval may be rebuilt from TARGET; boundary slices must be copied
// from their exact old immutable sources and authenticated independently.
func PlanOfflineHistoryRepair(manifest *Manifest, fromTx, toTx uint64) (*OfflineHistoryRepairPlan, error) {
	if manifest == nil || toTx < fromTx {
		return nil, errors.New("snapshots: invalid offline repair interval")
	}
	if err := manifest.Validate(); err != nil {
		return nil, err
	}
	cfg, _ := DefaultDomainRegistry().Dataset(SegmentDatasetStateDomainChange)
	var histories []SegmentRef
	for _, ref := range manifest.Segments {
		if ref.NormalizedDataset() == SegmentDatasetStateDomainChange && ref.Kind == SegmentHistory && ref.FromTxNum <= toTx && ref.ToTxNum >= fromTx {
			histories = append(histories, ref)
		}
	}
	sort.Slice(histories, func(i, j int) bool { return histories[i].FromTxNum < histories[j].FromTxNum })
	if len(histories) == 0 || histories[0].FromTxNum > fromTx || histories[len(histories)-1].ToTxNum < toTx {
		return nil, errors.New("snapshots: repair interval is not covered by active cold history")
	}
	p := &OfflineHistoryRepairPlan{FromTxNum: fromTx, ToTxNum: toTx, ReplaceFromTxNum: histories[0].FromTxNum, ReplaceToTxNum: histories[len(histories)-1].ToTxNum}
	for i, h := range histories {
		if i > 0 && (histories[i-1].ToTxNum == math.MaxUint64 || h.FromTxNum != histories[i-1].ToTxNum+1) {
			return nil, errors.New("snapshots: repair sources are not contiguous")
		}
		idx, ok := cfg.HistoryIndexRef(manifest, h)
		if !ok {
			return nil, errors.New("snapshots: repair source index missing")
		}
		acc, ok := cfg.HistoryAccessorRef(manifest, h)
		if !ok {
			return nil, errors.New("snapshots: repair source accessor missing")
		}
		trio := []SegmentRef{h, idx, acc}
		if _, _, _, _, err := historyReferenceTranscodeIdentity(trio); err != nil {
			return nil, err
		}
		p.SourceRefs = append(p.SourceRefs, trio...)
		if i == 0 && h.FromTxNum < fromTx {
			p.Left = &OfflineHistoryRepairSlice{SourceRefs: append([]SegmentRef(nil), trio...), FromTxNum: h.FromTxNum, ToTxNum: fromTx - 1}
		}
		if i == len(histories)-1 && h.ToTxNum > toTx {
			p.Right = &OfflineHistoryRepairSlice{SourceRefs: append([]SegmentRef(nil), trio...), FromTxNum: toTx + 1, ToTxNum: h.ToTxNum}
		}
	}
	return p, nil
}

// PrepareOfflineHistoryRepairManifest creates, but never publishes, one exact
// replacement. It preserves every unrelated ref and all progress/chain/reset
// fields. Retired files are retained even after a successful later rebind.
func PrepareOfflineHistoryRepairManifest(manifest *Manifest, plan *OfflineHistoryRepairPlan, replacements []SegmentRef, publishedUnix int64) (*Manifest, error) {
	if manifest == nil || plan == nil || publishedUnix <= 0 || manifest.Generation == math.MaxUint64 {
		return nil, errors.New("snapshots: invalid offline repair publication")
	}
	expected, err := PlanOfflineHistoryRepair(manifest, plan.FromTxNum, plan.ToTxNum)
	if err != nil {
		return nil, err
	}
	if !reflect.DeepEqual(expected, plan) {
		return nil, errors.New("snapshots: offline repair plan differs from current manifest")
	}
	if len(replacements) == 0 || len(replacements)%3 != 0 {
		return nil, errors.New("snapshots: incomplete repair replacement trios")
	}
	byRange := make(map[[2]uint64][]SegmentRef)
	for _, ref := range replacements {
		if ref.NormalizedDataset() != SegmentDatasetStateDomainChange || (ref.Kind != SegmentHistory && ref.Kind != SegmentInverted && ref.Kind != SegmentAccessor) || ref.FromTxNum < plan.ReplaceFromTxNum || ref.ToTxNum > plan.ReplaceToTxNum {
			return nil, errors.New("snapshots: replacement outside repair history family or interval")
		}
		k := [2]uint64{ref.FromTxNum, ref.ToTxNum}
		byRange[k] = append(byRange[k], ref)
	}
	var intervals [][2]uint64
	for k, trio := range byRange {
		if len(trio) != 3 {
			return nil, errors.New("snapshots: incomplete repair replacement trio")
		}
		if _, _, _, _, err := historyReferenceTranscodeIdentity(trio); err != nil {
			return nil, err
		}
		intervals = append(intervals, k)
	}
	sort.Slice(intervals, func(i, j int) bool { return intervals[i][0] < intervals[j][0] })
	next := plan.ReplaceFromTxNum
	for i, k := range intervals {
		if k[0] != next || k[1] < k[0] {
			return nil, errors.New("snapshots: repair replacement has gap or overlap")
		}
		if i < len(intervals)-1 {
			if k[1] == math.MaxUint64 {
				return nil, errors.New("snapshots: repair interval overflow")
			}
			next = k[1] + 1
		}
	}
	if intervals[len(intervals)-1][1] != plan.ReplaceToTxNum {
		return nil, errors.New("snapshots: repair replacement does not preserve whole boundary trios")
	}
	old := make(map[string]SegmentRef, len(plan.SourceRefs))
	for _, ref := range plan.SourceRefs {
		old[ref.Path] = ref
	}
	for _, ref := range replacements {
		if _, exists := old[ref.Path]; exists {
			return nil, errors.New("snapshots: replacement reuses an old immutable path")
		}
	}
	nextManifest := cloneManifest(manifest)
	nextManifest.Segments = nil
	for _, ref := range manifest.Segments {
		if oldRef, found := old[ref.Path]; found {
			if oldRef != ref {
				return nil, errors.New("snapshots: conflicting repair source")
			}
			nextManifest.Retired = append(nextManifest.Retired, ref)
		} else {
			nextManifest.Segments = append(nextManifest.Segments, ref)
		}
	}
	nextManifest.Segments = append(nextManifest.Segments, replacements...)
	nextManifest.Retired = dedupeSegmentRefs(nextManifest.Retired)
	nextManifest.Generation++
	nextManifest.PublishedUnix = publishedUnix
	normalizeChainIdentity(nextManifest.Chain)
	sortSegments(nextManifest.Segments)
	sortSegments(nextManifest.Retired)
	if err := nextManifest.Validate(); err != nil {
		return nil, err
	}
	return nextManifest, nil
}

// CopyStateHistoryReferenceTrioRangeContext produces one self-contained R1/V7
// trio. Full source identities and companions are authenticated first; the
// output must have the identical ordered row/Prev and block-range digest for
// the selected interval. This proves copying, not correctness of old history.
func CopyStateHistoryReferenceTrioRangeContext(ctx context.Context, dir string, sourceRefs []SegmentRef, fromTx, toTx uint64, relPath string, opts etl.Options) ([]SegmentRef, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	h, idx, acc, cfg, err := historyReferenceTranscodeIdentity(sourceRefs)
	if err != nil {
		return nil, err
	}
	if fromTx < h.FromTxNum || toTx > h.ToTxNum || toTx < fromTx || !isStateDomainChangeBinarySegmentPath(relPath) {
		return nil, errors.New("snapshots: invalid offline cold copy range or path")
	}
	if relPath == h.Path || cfg.HistoryIndexPathFor(relPath) == idx.Path || cfg.HistoryAccessorPathFor(relPath) == acc.Path {
		return nil, errors.New("snapshots: offline copy would overwrite its source")
	}
	selection := historyCompactionSelection{fromTxNum: h.FromTxNum, toTxNum: h.ToTxNum, aggregationSteps: 1, candidates: []historyCompactionCandidate{{history: h, companions: []SegmentRef{idx, acc}}}}
	trio := [3]SegmentRef{h, idx, acc}
	var states [3]historyStagingFileState
	for i, ref := range trio {
		states[i], err = historyStagingFileFingerprint(dir, ref)
		if err != nil {
			return nil, err
		}
	}
	source, err := offlineHistoryRepairAuthenticatedSource(ctx, dir, trio)
	if err != nil {
		return nil, err
	}
	sources := []stateDomainChangeBinaryCompactionSource{source}
	if err := historyStagingCheckFileStates(dir, trio, states); err != nil {
		return nil, err
	}
	if err := checkOfflineHistoryRepairBlockBoundary(ctx, dir, sources, fromTx, toTx); err != nil {
		return nil, err
	}
	before, err := offlineHistoryRepairRangeDigest(ctx, dir, sources, fromTx, toTx)
	if err != nil {
		return nil, err
	}
	projection := &OfflineHistoryRepairSlice{FromTxNum: fromTx, ToTxNum: toTx, path: relPath}
	refs, err := compactStateDomainChangeReferenceHistoryProjectionContext(ctx, dir, cfg, selection, sources, nil, projection, opts)
	if err != nil {
		return nil, err
	}
	nh, ni, na, _, err := historyReferenceTranscodeIdentity(refs)
	if err != nil {
		return nil, err
	}
	newTrio := [3]SegmentRef{nh, ni, na}
	var newStates [3]historyStagingFileState
	for i, ref := range newTrio {
		newStates[i], err = historyStagingFileFingerprint(dir, ref)
		if err != nil {
			return nil, err
		}
	}
	newSource, err := offlineHistoryRepairAuthenticatedSource(ctx, dir, newTrio)
	if err != nil {
		return nil, err
	}
	after, err := offlineHistoryRepairRangeDigest(ctx, dir, []stateDomainChangeBinaryCompactionSource{newSource}, fromTx, toTx)
	if err != nil {
		return nil, err
	}
	if before != after {
		return nil, errors.New("snapshots: offline cold range copy changed ordered rows, Prev, or block ranges")
	}
	if err := historyStagingCheckFileStates(dir, newTrio, newStates); err != nil {
		return nil, err
	}
	// Last checks also bind the earlier digest to the same immutable input.
	if err := historyStagingCheckFileStates(dir, trio, states); err != nil {
		return nil, err
	}
	return refs, ctx.Err()
}

// Use the ordinary cold prover's strong fingerprint cache, so publication
// authentication can reuse this one complete trio audit without another SHA.
func offlineHistoryRepairAuthenticatedSource(ctx context.Context, dir string, trio [3]SegmentRef) (source stateDomainChangeBinaryCompactionSource, err error) {
	p, err := NewHistoryStagingColdProver(dir, NewManifest(trio[0].FromTxNum, trio[0].ToTxNum, trio[:]))
	if err != nil {
		return source, err
	}
	defer p.Close()
	id, err := historyStagingTrioID(trio)
	if err != nil {
		return source, err
	}
	if err = p.authenticate(ctx, trio, id); err != nil {
		return source, err
	}
	reader, header, size, err := openStateDomainChangeBinarySegmentReader(dir, trio[0])
	if err != nil {
		return source, err
	}
	defer func() { err = errors.Join(err, reader.Close()) }()
	count, offset, err := stateDomainChangeBinaryTxRangeTableBoundsAt(reader, size, trio[0], header)
	if err != nil {
		return source, err
	}
	if header.version != stateDomainChangeBinaryVersionV6 {
		return source, errors.New("snapshots: offline cold copy requires V6 logical records")
	}
	return stateDomainChangeBinaryCompactionSource{history: trio[0], accessor: trio[2], segmentHeader: header, segmentSize: size, txRangeCount: count, recordOffset: offset}, nil
}

func checkOfflineHistoryRepairBlockBoundary(ctx context.Context, dir string, sources []stateDomainChangeBinaryCompactionSource, fromTx, toTx uint64) error {
	var first, last *rawdb.StateTxRange
	err := iterateMergedStateDomainChangeBinaryCompactionTxRanges(ctx, dir, sources, func(row *rawdb.StateTxRange) error {
		if row.EndTxNum < fromTx || row.BeginTxNum > toTx {
			return nil
		}
		if first == nil {
			first = cloneStateTxRangeForSegment(row)
		}
		last = cloneStateTxRangeForSegment(row)
		return nil
	})
	if err != nil {
		return err
	}
	if first == nil || first.BeginTxNum != fromTx || last.EndTxNum != toTx {
		return errors.New("snapshots: offline cold copy must preserve complete canonical blocks")
	}
	return nil
}

func writeOfflineHistoryRepairTxRanges(ctx context.Context, dir string, dst stateDomainChangeBinaryWriteAtWriter, sources []stateDomainChangeBinaryCompactionSource, fromTx, toTx uint64) (uint64, error) {
	if err := writeStateDomainChangeBinaryTxRangeCount(dst, 0); err != nil {
		return 0, err
	}
	var count uint64
	err := iterateMergedStateDomainChangeBinaryCompactionTxRanges(ctx, dir, sources, func(row *rawdb.StateTxRange) error {
		if row.EndTxNum < fromTx || row.BeginTxNum > toTx {
			return nil
		}
		var raw [stateDomainChangeBinaryTxRangeSize]byte
		if err := putStateDomainChangeBinaryTxRangeEntry(&raw, row); err != nil {
			return err
		}
		if _, err := dst.Write(raw[:]); err != nil {
			return err
		}
		count++
		return nil
	})
	if err != nil {
		return 0, err
	}
	if count == 0 {
		return 0, errors.New("snapshots: offline copy has no canonical block range")
	}
	var raw [8]byte
	binary.BigEndian.PutUint64(raw[:], count)
	_, err = dst.WriteAt(raw[:], int64(stateDomainChangeBinaryTxRangeTableStart(stateDomainChangeBinaryVersionV6)))
	return count, err
}

// The streaming digest deliberately uses the same per-Tx ordinal/Prev proof
// as staging equivalence. It does not retain a row set or an owning Prev image.
func offlineHistoryRepairRangeDigest(ctx context.Context, dir string, sources []stateDomainChangeBinaryCompactionSource, fromTx, toTx uint64) ([32]byte, error) {
	hash := sha256.New()
	hash.Write([]byte("gtron-offline-history-copy-v1\x00"))
	err := iterateMergedStateDomainChangeBinaryCompactionTxRanges(ctx, dir, sources, func(row *rawdb.StateTxRange) error {
		if row.EndTxNum < fromTx || row.BeginTxNum > toTx {
			return nil
		}
		var raw [stateDomainChangeBinaryTxRangeSize]byte
		if err := putStateDomainChangeBinaryTxRangeEntry(&raw, row); err != nil {
			return err
		}
		hash.Write(raw[:])
		return nil
	})
	if err != nil {
		return [32]byte{}, err
	}
	hash.Write([]byte("\x00rows\x00"))
	var rows, previousTx, ordinal uint64
	var have bool
	for _, source := range sources {
		err = func() (err error) {
			reader, header, size, err := openStateDomainChangeBinarySegmentSequentialReader(dir, source.history)
			if err != nil {
				return err
			}
			defer func() { err = errors.Join(err, reader.Close()) }()
			if header.version != stateDomainChangeBinaryVersionV6 {
				return errors.New("snapshots: offline cold copy requires V6 logical records")
			}
			contextual, ok := reader.(*stateDomainChangeHistoryReader)
			if !ok {
				return errors.New("snapshots: offline cold copy lacks contextual key reader")
			}
			offset := source.recordOffset
			for i := uint64(0); i < header.count; i++ {
				if err := ctx.Err(); err != nil {
					return err
				}
				var frame [21]byte
				if offset > size || size-offset < 21 {
					return io.ErrUnexpectedEOF
				}
				if _, err := reader.ReadAt(frame[:], int64(offset)); err != nil {
					return err
				}
				payload := uint64(binary.BigEndian.Uint32(frame[:4]))
				prev := uint64(binary.BigEndian.Uint32(frame[17:]))
				if payload != 17+prev || payload > size-offset-4 || frame[16] > 1 {
					return errors.New("snapshots: malformed offline copy V6 record")
				}
				row := &rawdb.StateDomainChange{TxNum: binary.BigEndian.Uint64(frame[8:16]), PrevExists: frame[16] == 1}
				if row.TxNum >= fromTx && row.TxNum <= toTx {
					key, e := contextual.v6Key(binary.BigEndian.Uint32(frame[4:8]))
					if e != nil {
						return e
					}
					if e = decodeStateDomainChangeBinaryAccessorKey(key, row); e != nil {
						return e
					}
					txRange, e := contextual.txRangeForTxNum(row.TxNum)
					if e != nil {
						return e
					}
					if e = hydrateStateDomainChangeBinaryRecordV5FromRange(txRange, i, row); e != nil {
						return e
					}
					if have && row.TxNum < previousTx {
						return errStateDomainChangeHistoryRecordsNotOrdered
					}
					if !have || row.TxNum != previousTx {
						ordinal = 0
					} else {
						ordinal++
					}
					previousTx, have = row.TxNum, true
					digest, e := historyStagingRecordDigestStream(row, ordinal, prev, func(dst io.Writer) error {
						_, e := io.Copy(dst, io.NewSectionReader(contextReaderAt{ctx: ctx, r: reader}, int64(offset+21), int64(prev)))
						return e
					})
					if e != nil {
						return e
					}
					hash.Write(digest[:])
					rows++
				}
				offset += 4 + payload
			}
			if offset != size {
				return fmt.Errorf("snapshots: offline copy source has trailing bytes")
			}
			return ctx.Err()
		}()
		if err != nil {
			return [32]byte{}, err
		}
	}
	var count [8]byte
	binary.BigEndian.PutUint64(count[:], rows)
	hash.Write(count[:])
	var result [32]byte
	copy(result[:], hash.Sum(nil))
	return result, ctx.Err()
}
