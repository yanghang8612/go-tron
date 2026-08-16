package snapshots

import (
	"context"
	"encoding/binary"
	"fmt"
	"math"
	"sort"

	"github.com/klauspost/compress/zstd"
	"github.com/tronprotocol/go-tron/common"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	"google.golang.org/protobuf/proto"
)

const (
	eventLogCandidateHeaderBytes       = uint64(128)
	eventLogCandidateSidecarHeader     = uint64(64)
	eventLogCandidateRowFrameRows      = uint64(256)
	eventLogCandidateRowFrameDirBytes  = uint64(24)
	eventLogCandidatePayloadDirBytes   = uint64(24)
	eventLogCandidateLookupFrameBytes  = uint64(4096)
	eventLogCandidateLookupFrameDir    = uint64(24)
	eventLogCandidateLookupDirMetadata = uint64(24)
)

// EventLogSpaceInspectOptions controls the read-only event-log space profiler.
// SampleSegments == 0 scans every active event-log segment from the one
// production manifest loaded at startup. A positive value selects that many
// evenly-spaced segments without reloading or following manifest updates.
type EventLogSpaceInspectOptions struct {
	SampleSegments uint64
	Context        context.Context
}

type EventLogPhysicalBytes struct {
	MainSegment             uint64 `json:"mainSegment"`
	Header                  uint64 `json:"header"`
	FixedRowIndex           uint64 `json:"fixedRowIndex"`
	ProtobufPayload         uint64 `json:"protobufPayload"`
	EmbeddedAddressPostings uint64 `json:"embeddedAddressPostings"`
	EmbeddedTopicPostings   uint64 `json:"embeddedTopicPostings"`
	ExternalSidecar         uint64 `json:"externalSidecar"`
	Total                   uint64 `json:"total"`
}

type EventLogValueDistribution struct {
	Count uint64 `json:"count"`
	Bytes uint64 `json:"bytes,omitempty"`
	Min   uint64 `json:"min"`
	P50   uint64 `json:"p50"`
	P95   uint64 `json:"p95"`
	P99   uint64 `json:"p99"`
	Max   uint64 `json:"max"`
}

type EventLogDuplicateStats struct {
	Rows                uint64 `json:"rows"`
	DistinctBlockHashes uint64 `json:"distinctBlockHashes"`
	RepeatedBlockHashes uint64 `json:"repeatedBlockHashes"`
	ZeroBlockHashes     uint64 `json:"zeroBlockHashes"`
	DistinctTxHashes    uint64 `json:"distinctTxHashes"`
	RepeatedTxHashes    uint64 `json:"repeatedTxHashes"`
	ZeroTxHashes        uint64 `json:"zeroTxHashes"`
	DistinctAddresses   uint64 `json:"distinctAddresses"`
	RepeatedAddresses   uint64 `json:"repeatedAddresses"`
}

type EventLogLookupCandidateStats struct {
	Keys              uint64 `json:"keys"`
	Postings          uint64 `json:"postings"`
	EncodedBytes      uint64 `json:"encodedBytes"`
	DirectoryBytes    uint64 `json:"directoryBytes"`
	FrameDirectory    uint64 `json:"frameDirectoryBytes"`
	PostingBytes      uint64 `json:"postingBytes"`
	Frames            uint64 `json:"frames"`
	MaxFrameBytes     uint64 `json:"maxFrameBytes"`
	MaxFramesPerKey   uint64 `json:"maxFramesPerKey"`
	MaxPostingsPerKey uint64 `json:"maxPostingsPerKey"`
}

type EventLogCandidateLayout struct {
	PayloadBlockSize       uint64                       `json:"payloadBlockSize"`
	BlockHashSource        string                       `json:"blockHashSource"`
	HeaderBytes            uint64                       `json:"headerBytes"`
	RowFrameDirectoryBytes uint64                       `json:"rowFrameDirectoryBytes"`
	RowDeltaVarintBytes    uint64                       `json:"rowDeltaVarintBytes"`
	BlockDictionaryBytes   uint64                       `json:"blockDictionaryBytes"`
	TxDictionaryBytes      uint64                       `json:"txDictionaryBytes"`
	AddressDictionaryBytes uint64                       `json:"addressDictionaryBytes"`
	PayloadDirectoryBytes  uint64                       `json:"payloadDirectoryBytes"`
	PayloadCompressedBytes uint64                       `json:"payloadCompressedBytes"`
	AddressPostings        EventLogLookupCandidateStats `json:"addressPostings"`
	TopicPostings          EventLogLookupCandidateStats `json:"topicPostings"`
	ExternalSidecarBytes   uint64                       `json:"externalSidecarBytes"`
	EstimatedPhysicalBytes uint64                       `json:"estimatedPhysicalBytes"`
	ComparedPhysicalBytes  uint64                       `json:"comparedPhysicalBytes"`
	SavingsBytes           uint64                       `json:"savingsBytes"`
	SavingsMilli           int64                        `json:"savingsMilli"`
	PayloadFrames          uint64                       `json:"payloadFrames"`
	MaxPointReadBytes      uint64                       `json:"maxPointReadBytes"`
	MaxPointDecompress     uint64                       `json:"maxPointDecompressBytes"`
	MaxSingleKeyLookupRead uint64                       `json:"maxSingleKeyLookupReadBytes"`
	ChainFreezerPointReads uint64                       `json:"chainFreezerPointReads"`
	SegmentModel           string                       `json:"segmentModel"`
}

type EventLogMergeProjection struct {
	MergeFactor                 uint64 `json:"mergeFactor"`
	ProjectedEventLogFiles      uint64 `json:"projectedEventLogFiles"`
	ProjectedExternalIndexFiles uint64 `json:"projectedExternalIndexFiles"`
	SampleAddressPostings       uint64 `json:"sampleAddressPostings"`
	SampleTopicPostings         uint64 `json:"sampleTopicPostings"`
	SampleSidecarBytes          uint64 `json:"sampleSidecarBytes"`
	RepresentativeOnly          bool   `json:"representativeOnly"`
}

type EventLogSegmentSpaceStats struct {
	Path         string                    `json:"path"`
	Version      uint32                    `json:"version"`
	FromBlock    uint64                    `json:"fromBlock"`
	ToBlock      uint64                    `json:"toBlock"`
	Rows         uint64                    `json:"rows"`
	Physical     EventLogPhysicalBytes     `json:"physical"`
	PayloadSizes EventLogValueDistribution `json:"payloadSizes"`
	TopicCounts  EventLogValueDistribution `json:"topicCounts"`
	Duplicates   EventLogDuplicateStats    `json:"duplicates"`
}

type EventLogSpaceInspection struct {
	ManifestGeneration   uint64                      `json:"manifestGeneration"`
	ManifestPath         string                      `json:"manifestPath"`
	ManifestLoadMode     string                      `json:"manifestLoadMode"`
	ActiveEventSegments  uint64                      `json:"activeEventSegments"`
	ActiveIndexSegments  uint64                      `json:"activeIndexSegments"`
	SampledEventSegments uint64                      `json:"sampledEventSegments"`
	SampledIndexSegments uint64                      `json:"sampledIndexSegments"`
	SampledAll           bool                        `json:"sampledAll"`
	ManifestPhysical     EventLogPhysicalBytes       `json:"manifestPhysical"`
	SamplePhysical       EventLogPhysicalBytes       `json:"samplePhysical"`
	PayloadSizes         EventLogValueDistribution   `json:"payloadSizes"`
	TopicCounts          EventLogValueDistribution   `json:"topicCounts"`
	Duplicates           EventLogDuplicateStats      `json:"duplicates"`
	Segments             []EventLogSegmentSpaceStats `json:"segments"`
	Candidates           []EventLogCandidateLayout   `json:"candidates"`
	Merge                []EventLogMergeProjection   `json:"merge"`
	Limitations          []string                    `json:"limitations"`
}

type eventLogExactDistribution struct {
	counts map[uint64]uint64
	count  uint64
	bytes  uint64
}

func (d *eventLogExactDistribution) add(value uint64) {
	if d.counts == nil {
		d.counts = make(map[uint64]uint64)
	}
	d.counts[value]++
	d.count++
	d.bytes += value
}

func (d *eventLogExactDistribution) merge(other eventLogExactDistribution) {
	if d.counts == nil {
		d.counts = make(map[uint64]uint64)
	}
	for value, count := range other.counts {
		d.counts[value] += count
	}
	d.count += other.count
	d.bytes += other.bytes
}

func (d eventLogExactDistribution) summary() EventLogValueDistribution {
	out := EventLogValueDistribution{Count: d.count, Bytes: d.bytes}
	if d.count == 0 {
		return out
	}
	keys := make([]uint64, 0, len(d.counts))
	for value := range d.counts {
		keys = append(keys, value)
	}
	sort.Slice(keys, func(i, j int) bool { return keys[i] < keys[j] })
	out.Min, out.Max = keys[0], keys[len(keys)-1]
	out.P50 = d.quantile(keys, 50)
	out.P95 = d.quantile(keys, 95)
	out.P99 = d.quantile(keys, 99)
	return out
}

func (d eventLogExactDistribution) quantile(keys []uint64, percentile uint64) uint64 {
	if d.count == 0 {
		return 0
	}
	target := (d.count*percentile + 99) / 100
	var seen uint64
	for _, value := range keys {
		seen += d.counts[value]
		if seen >= target {
			return value
		}
	}
	return keys[len(keys)-1]
}

type eventLogPayloadCompression struct {
	target        uint64
	encoder       *zstd.Encoder
	buffer        []byte
	frames        uint64
	compressed    uint64
	maxRawFrame   uint64
	maxCompressed uint64
}

func newEventLogPayloadCompression(target uint64) (*eventLogPayloadCompression, error) {
	encoder, err := zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.SpeedDefault), zstd.WithEncoderConcurrency(1))
	if err != nil {
		return nil, err
	}
	return &eventLogPayloadCompression{target: target, encoder: encoder}, nil
}

func (p *eventLogPayloadCompression) add(raw []byte) {
	if len(p.buffer) > 0 && uint64(len(p.buffer)+len(raw)) > p.target {
		p.flush()
	}
	p.buffer = append(p.buffer, raw...)
}

func (p *eventLogPayloadCompression) flush() {
	if len(p.buffer) == 0 {
		return
	}
	encoded := p.encoder.EncodeAll(p.buffer, nil)
	p.frames++
	p.compressed += uint64(len(encoded))
	p.maxRawFrame = max(p.maxRawFrame, uint64(len(p.buffer)))
	p.maxCompressed = max(p.maxCompressed, uint64(len(encoded)))
	p.buffer = p.buffer[:0]
}

func (p *eventLogPayloadCompression) close() {
	p.flush()
	p.encoder.Close()
}

type eventLogCandidateAccumulator struct {
	rowBytes               uint64
	rowFrames              uint64
	maxRowFrame            uint64
	rowFrameBytes          uint64
	rowsInFrame            uint64
	prevBlock              uint64
	prevTx                 uint64
	prevLog                uint64
	prevPayloadLen         uint64
	lastTxHash             common.Hash
	haveTxHash             bool
	currentTxID            uint64
	txDictionaryEntries    uint64
	lastDictionaryBlock    uint64
	haveDictionaryBlock    bool
	blockDictionaryBytes   uint64
	blockDictionaryEntries uint64
	addressIDs             map[common.Address]uint64
	addressPostings        map[string]*eventLogCandidatePostingState
	topicPostings          map[string]*eventLogCandidatePostingState
	currentSegment         uint64
	payloads               []*eventLogPayloadCompression
}

var eventLogCandidateMergeFactors = [...]uint64{1, 2, 4, 8, 16, 32}

type eventLogPostingStream struct {
	postings        uint64
	postingBytes    uint64
	frames          uint64
	maxFrameBytes   uint64
	currentFrame    uint64
	previousPosting uint64
	havePrevious    bool
}

func (s *eventLogPostingStream) add(posting uint64) {
	delta := posting
	if s.havePrevious {
		delta = posting - s.previousPosting
	}
	encoded := uvarintBytes(delta)
	if s.currentFrame > 0 && s.currentFrame+encoded > eventLogCandidateLookupFrameBytes {
		s.postingBytes += s.currentFrame
		s.maxFrameBytes = max(s.maxFrameBytes, s.currentFrame)
		s.frames++
		s.currentFrame = 0
	}
	s.currentFrame += encoded
	s.postings++
	s.previousPosting = posting
	s.havePrevious = true
}

func (s eventLogPostingStream) summary() (postings, bytes, frames, maxFrame uint64) {
	postings, bytes, frames, maxFrame = s.postings, s.postingBytes, s.frames, s.maxFrameBytes
	if s.currentFrame > 0 {
		bytes += s.currentFrame
		frames++
		maxFrame = max(maxFrame, s.currentFrame)
	}
	return
}

type eventLogCandidatePostingState struct {
	rows           eventLogPostingStream
	merged         [len(eventLogCandidateMergeFactors)]eventLogPostingStream
	lastMergeGroup [len(eventLogCandidateMergeFactors)]uint64
	haveMergeGroup [len(eventLogCandidateMergeFactors)]bool
}

func (s *eventLogCandidatePostingState) add(row, segment uint64) {
	s.rows.add(row)
	for i, factor := range eventLogCandidateMergeFactors {
		group := segment / factor
		if s.haveMergeGroup[i] && s.lastMergeGroup[i] == group {
			continue
		}
		s.merged[i].add(group)
		s.lastMergeGroup[i] = group
		s.haveMergeGroup[i] = true
	}
}

func newEventLogCandidateAccumulator() (*eventLogCandidateAccumulator, error) {
	a := &eventLogCandidateAccumulator{
		addressIDs:      make(map[common.Address]uint64),
		addressPostings: make(map[string]*eventLogCandidatePostingState),
		topicPostings:   make(map[string]*eventLogCandidatePostingState),
	}
	for _, size := range []uint64{16 << 10, 32 << 10, 64 << 10} {
		payload, err := newEventLogPayloadCompression(size)
		if err != nil {
			a.close()
			return nil, err
		}
		a.payloads = append(a.payloads, payload)
	}
	return a, nil
}

func (a *eventLogCandidateAccumulator) close() {
	for _, payload := range a.payloads {
		payload.close()
	}
}

func (a *eventLogCandidateAccumulator) startSegment(segment uint64) {
	a.currentSegment = segment
}

func (a *eventLogCandidateAccumulator) add(rowIndex uint64, entry eventLogIndexEntry, log *corepb.TransactionInfo_Log) error {
	if !a.haveTxHash || entry.txHash != a.lastTxHash {
		a.currentTxID = a.txDictionaryEntries
		a.txDictionaryEntries++
		a.lastTxHash = entry.txHash
		a.haveTxHash = true
	}
	txID := a.currentTxID
	addressID, ok := a.addressIDs[entry.address]
	if !ok {
		addressID = uint64(len(a.addressIDs))
		a.addressIDs[entry.address] = addressID
	}
	if !a.haveDictionaryBlock || entry.blockNum != a.lastDictionaryBlock {
		delta := entry.blockNum
		if a.haveDictionaryBlock {
			delta = entry.blockNum - a.lastDictionaryBlock
		}
		a.blockDictionaryBytes += common.HashLength + uvarintBytes(delta)
		a.blockDictionaryEntries++
		a.lastDictionaryBlock = entry.blockNum
		a.haveDictionaryBlock = true
	}

	if a.rowsInFrame == 0 {
		a.rowFrames++
		a.prevBlock, a.prevTx, a.prevLog = 0, 0, 0
		a.prevPayloadLen = 0
	}
	rowBytes := uvarintBytes(entry.blockNum - a.prevBlock)
	if entry.blockNum == a.prevBlock {
		rowBytes += uvarintBytes(entry.txIndex - a.prevTx)
		if entry.logIndex >= a.prevLog {
			rowBytes += uvarintBytes(entry.logIndex - a.prevLog)
		} else {
			rowBytes += uvarintBytes(entry.logIndex)
		}
	} else {
		rowBytes += uvarintBytes(entry.txIndex)
		rowBytes += uvarintBytes(entry.logIndex)
	}
	rowBytes += uvarintBytes(txID)
	rowBytes += uvarintBytes(addressID)
	// The frame directory checkpoints the absolute payload offset. Each row
	// only needs the preceding payload length to derive its own offset.
	rowBytes += uvarintBytes(a.prevPayloadLen)

	normalized := proto.Clone(log).(*corepb.TransactionInfo_Log)
	normalized.Address = nil
	raw, err := proto.Marshal(normalized)
	if err != nil {
		return err
	}
	rowBytes += uvarintBytes(uint64(len(raw)))
	for _, payload := range a.payloads {
		payload.add(raw)
	}
	a.prevPayloadLen = uint64(len(raw))
	a.prevBlock, a.prevTx, a.prevLog = entry.blockNum, entry.txIndex, entry.logIndex
	a.rowBytes += rowBytes
	a.rowFrameBytes += rowBytes
	a.rowsInFrame++
	if a.rowsInFrame == eventLogCandidateRowFrameRows {
		a.maxRowFrame = max(a.maxRowFrame, a.rowFrameBytes)
		a.rowsInFrame, a.rowFrameBytes = 0, 0
	}

	addressKey := string(eventLogAddressLookupKey(entry.address))
	addressState := a.addressPostings[addressKey]
	if addressState == nil {
		addressState = new(eventLogCandidatePostingState)
		a.addressPostings[addressKey] = addressState
	}
	addressState.add(rowIndex, a.currentSegment)
	for position, rawTopic := range log.GetTopics() {
		var topic common.Hash
		copy(topic[:], rawTopic)
		key := string(eventLogTopicLookupKey(uint64(position), topic))
		topicState := a.topicPostings[key]
		if topicState == nil {
			topicState = new(eventLogCandidatePostingState)
			a.topicPostings[key] = topicState
		}
		topicState.add(rowIndex, a.currentSegment)
	}
	return nil
}

func (a *eventLogCandidateAccumulator) finish() {
	if a.rowsInFrame > 0 {
		a.maxRowFrame = max(a.maxRowFrame, a.rowFrameBytes)
		a.rowsInFrame, a.rowFrameBytes = 0, 0
	}
	a.close()
}

func uvarintBytes(value uint64) uint64 {
	var raw [binary.MaxVarintLen64]byte
	return uint64(binary.PutUvarint(raw[:], value))
}

func eventLogCandidateLookup(postings map[string]*eventLogCandidatePostingState, keySize uint64, mergeIndex int) EventLogLookupCandidateStats {
	stats := EventLogLookupCandidateStats{Keys: uint64(len(postings))}
	stats.DirectoryBytes = 8 + stats.Keys*(keySize+eventLogCandidateLookupDirMetadata)
	for _, state := range postings {
		stream := state.rows
		if mergeIndex >= 0 {
			stream = state.merged[mergeIndex]
		}
		count, bytes, frames, maxFrame := stream.summary()
		stats.Postings += count
		stats.PostingBytes += bytes
		stats.Frames += frames
		stats.MaxFrameBytes = max(stats.MaxFrameBytes, maxFrame)
		stats.MaxFramesPerKey = max(stats.MaxFramesPerKey, frames)
		stats.MaxPostingsPerKey = max(stats.MaxPostingsPerKey, count)
	}
	stats.FrameDirectory = stats.Frames * eventLogCandidateLookupFrameDir
	stats.EncodedBytes = stats.DirectoryBytes + stats.FrameDirectory + stats.PostingBytes
	return stats
}

// InspectEventLogSpace reads immutable files referenced by one production
// manifest snapshot. It never opens chaindata, creates a Manager, or reloads
// manifest.json while scanning.
func InspectEventLogSpace(dir string, opts EventLogSpaceInspectOptions) (*EventLogSpaceInspection, error) {
	manifest, err := LoadProductionManifest(dir)
	if err != nil {
		return nil, err
	}
	return InspectEventLogSpaceFromManifest(dir, manifest, opts)
}

// InspectEventLogSpaceFromManifest scans exactly the supplied production
// generation. Callers that already pinned a manifest can use this entry point
// without another filesystem manifest read.
func InspectEventLogSpaceFromManifest(dir string, manifest *Manifest, opts EventLogSpaceInspectOptions) (*EventLogSpaceInspection, error) {
	if manifest == nil {
		return nil, fmt.Errorf("snapshots: nil event log inspect manifest")
	}
	manifest = cloneManifest(manifest)
	if err := manifest.ValidateProduction(); err != nil {
		return nil, err
	}
	ctx := opts.Context
	if ctx == nil {
		ctx = context.Background()
	}
	eventRefs := eventLogRefs(manifest)
	indexRefs := eventLogIndexRefs(manifest)
	selected := selectEvenEventLogRefs(eventRefs, opts.SampleSegments)
	out := &EventLogSpaceInspection{
		ManifestGeneration:   manifest.Generation,
		ManifestPath:         ManifestFile,
		ManifestLoadMode:     "single-production-manifest-no-follow",
		ActiveEventSegments:  uint64(len(eventRefs)),
		ActiveIndexSegments:  uint64(len(indexRefs)),
		SampledEventSegments: uint64(len(selected)),
		SampledAll:           len(selected) == len(eventRefs),
	}
	for _, ref := range eventRefs {
		out.ManifestPhysical.MainSegment += ref.Size
		out.ManifestPhysical.Total += ref.Size
	}
	for _, ref := range indexRefs {
		out.ManifestPhysical.ExternalSidecar += ref.Size
		out.ManifestPhysical.Total += ref.Size
	}

	selectedIndexes := selectEventLogIndexRefs(indexRefs, eventRefs, selected)
	selectedSidecarBytes, err := inspectEventLogSidecarPhysicalBytes(dir, manifest.Generation, selectedIndexes)
	if err != nil {
		return nil, err
	}
	out.SamplePhysical.ExternalSidecar = selectedSidecarBytes
	out.SamplePhysical.Total += selectedSidecarBytes
	out.SampledIndexSegments = uint64(len(selectedIndexes))
	if len(selected) == 0 {
		out.Limitations = append(out.Limitations, "production manifest has no active event-log segments")
		return out, nil
	}

	acc, err := newEventLogCandidateAccumulator()
	if err != nil {
		return nil, err
	}
	finished := false
	defer func() {
		if !finished {
			acc.close()
		}
	}()
	var payloadDist, topicDist eventLogExactDistribution
	var globalRow uint64
	var previousBlockHash, previousTxHash common.Hash
	var haveBlockHash, haveTxHash bool
	distinctAddresses := make(map[common.Address]struct{})
	for segmentIndex, ref := range selected {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		acc.startSegment(uint64(segmentIndex))
		seg, err := OpenEventLogSegment(dir, ref)
		if err != nil {
			return nil, err
		}
		if ref.Size != 0 && seg.size != ref.Size {
			_ = seg.Close()
			return nil, fmt.Errorf("snapshots: inspect generation %d event log %q size %d, want manifest size %d", manifest.Generation, ref.Path, seg.size, ref.Size)
		}
		stats, segmentPayload, segmentTopics, err := inspectEventLogSegment(ctx, seg, acc, &globalRow, &previousBlockHash, &haveBlockHash, &previousTxHash, &haveTxHash, distinctAddresses)
		closeErr := seg.Close()
		if err != nil {
			return nil, err
		}
		if closeErr != nil {
			return nil, closeErr
		}
		out.Segments = append(out.Segments, stats)
		payloadDist.merge(segmentPayload)
		topicDist.merge(segmentTopics)
		out.SamplePhysical.Header += stats.Physical.Header
		out.SamplePhysical.MainSegment += stats.Physical.Total
		out.SamplePhysical.FixedRowIndex += stats.Physical.FixedRowIndex
		out.SamplePhysical.ProtobufPayload += stats.Physical.ProtobufPayload
		out.SamplePhysical.EmbeddedAddressPostings += stats.Physical.EmbeddedAddressPostings
		out.SamplePhysical.EmbeddedTopicPostings += stats.Physical.EmbeddedTopicPostings
		out.SamplePhysical.Total += stats.Physical.Total
	}
	acc.finish()
	finished = true
	out.PayloadSizes = payloadDist.summary()
	out.TopicCounts = topicDist.summary()
	out.Duplicates = aggregateEventLogDuplicateStats(out.Segments)
	out.Duplicates.DistinctBlockHashes = acc.blockDictionaryEntries
	out.Duplicates.RepeatedBlockHashes = out.Duplicates.Rows - out.Duplicates.DistinctBlockHashes
	out.Duplicates.DistinctTxHashes = acc.txDictionaryEntries
	out.Duplicates.RepeatedTxHashes = out.Duplicates.Rows - out.Duplicates.DistinctTxHashes
	out.Duplicates.DistinctAddresses = uint64(len(distinctAddresses))
	out.Duplicates.RepeatedAddresses = out.Duplicates.Rows - out.Duplicates.DistinctAddresses

	addressCandidate := eventLogCandidateLookup(acc.addressPostings, eventLogAddressLookupKeySize, -1)
	topicCandidate := eventLogCandidateLookup(acc.topicPostings, eventLogTopicLookupKeySize, -1)
	sidecarCandidateBytes := candidateMergedEventLogSidecarBytes(uint64(len(acc.addressPostings)), uint64(len(acc.topicPostings)))
	for _, payload := range acc.payloads {
		for _, blockHashSource := range []string{"chain-freezer", "segment-dictionary"} {
			candidate := EventLogCandidateLayout{
				PayloadBlockSize:       payload.target,
				BlockHashSource:        blockHashSource,
				HeaderBytes:            eventLogCandidateHeaderBytes,
				RowFrameDirectoryBytes: acc.rowFrames * eventLogCandidateRowFrameDirBytes,
				RowDeltaVarintBytes:    acc.rowBytes,
				TxDictionaryBytes:      acc.txDictionaryEntries * common.HashLength,
				// Embedded address lookup keys are the address dictionary; row IDs
				// reference that directory, so no second 21-byte key copy is charged.
				AddressDictionaryBytes: 0,
				PayloadDirectoryBytes:  payload.frames * eventLogCandidatePayloadDirBytes,
				PayloadCompressedBytes: payload.compressed,
				AddressPostings:        addressCandidate,
				TopicPostings:          topicCandidate,
				ExternalSidecarBytes:   sidecarCandidateBytes,
				ComparedPhysicalBytes:  out.SamplePhysical.Total,
				PayloadFrames:          payload.frames,
				MaxPointDecompress:     payload.maxRawFrame,
				SegmentModel:           "selected-segments-merged-into-one",
			}
			if blockHashSource == "segment-dictionary" {
				candidate.BlockDictionaryBytes = acc.blockDictionaryBytes
			} else {
				candidate.ChainFreezerPointReads = 1
			}
			candidate.EstimatedPhysicalBytes = candidate.HeaderBytes + candidate.RowFrameDirectoryBytes + candidate.RowDeltaVarintBytes +
				candidate.BlockDictionaryBytes + candidate.TxDictionaryBytes + candidate.AddressDictionaryBytes +
				candidate.PayloadDirectoryBytes + candidate.PayloadCompressedBytes + candidate.AddressPostings.EncodedBytes +
				candidate.TopicPostings.EncodedBytes + candidate.ExternalSidecarBytes
			if candidate.ComparedPhysicalBytes >= candidate.EstimatedPhysicalBytes {
				candidate.SavingsBytes = candidate.ComparedPhysicalBytes - candidate.EstimatedPhysicalBytes
			}
			candidate.SavingsMilli = checkedCandidateSavings(candidate.ComparedPhysicalBytes, candidate.EstimatedPhysicalBytes)
			candidate.MaxPointReadBytes = eventLogCandidateRowFrameDirBytes + acc.maxRowFrame + common.HashLength + common.AddressLength +
				eventLogCandidatePayloadDirBytes + payload.maxCompressed
			if blockHashSource == "segment-dictionary" {
				candidate.MaxPointReadBytes += common.HashLength + binary.MaxVarintLen64
			}
			candidate.MaxSingleKeyLookupRead = max(singleKeyLookupReadBound(candidate.AddressPostings, eventLogAddressLookupKeySize), singleKeyLookupReadBound(candidate.TopicPostings, eventLogTopicLookupKeySize))
			out.Candidates = append(out.Candidates, candidate)
		}
	}
	out.Merge = projectEventLogMerges(uint64(len(eventRefs)), uint64(len(indexRefs)), uint64(len(selected)), acc.addressPostings, acc.topicPostings)
	if !out.SampledAll {
		out.Limitations = append(out.Limitations,
			"candidate byte totals and merge posting projections cover evenly sampled whole segments, not a full-chain extrapolation",
			"manifestPhysical is exact from manifest sizes, while component and distribution totals are sample-only")
	}
	out.Limitations = append(out.Limitations,
		"chain-freezer block-hash candidates exclude bytes and latency paid by the freezer lookup itself",
		"single-key lookup read bounds exclude an unbounded number of OR filter keys and filesystem block amplification",
		"candidate layouts merge all selected segments into one; non-contiguous samples are comparative models, not deployable merge ranges",
		"candidate V3 numbers are simulations, not a committed or reader-compatible on-disk format")
	return out, nil
}

func inspectEventLogSegment(ctx context.Context, seg *EventLogSegment, acc *eventLogCandidateAccumulator, globalRow *uint64, previousBlockHash *common.Hash, haveBlockHash *bool, previousTxHash *common.Hash, haveTxHash *bool, distinctAddresses map[common.Address]struct{}) (EventLogSegmentSpaceStats, eventLogExactDistribution, eventLogExactDistribution, error) {
	var payloadDist, topicDist eventLogExactDistribution
	header := seg.header
	if header.version == EventLogSegmentV4Version {
		return inspectEventLogV3Space(ctx, seg, acc, globalRow, previousBlockHash, haveBlockHash, previousTxHash, haveTxHash, distinctAddresses)
	}
	stats := EventLogSegmentSpaceStats{
		Path:      seg.ref.Path,
		Version:   header.version,
		FromBlock: header.fromBlock,
		ToBlock:   header.toBlock,
		Rows:      header.rowCount,
		Physical: EventLogPhysicalBytes{
			MainSegment:             seg.size,
			Header:                  header.headerSize,
			FixedRowIndex:           header.rowCount * eventLogIndexEntrySize,
			ProtobufPayload:         eventLogPayloadEnd(header, seg.size) - header.payloadOffset,
			EmbeddedAddressPostings: header.addressIndexLength,
			EmbeddedTopicPostings:   header.topicIndexLength,
			Total:                   seg.size,
		},
	}
	stats.Duplicates.Rows = header.rowCount
	segmentAddresses := make(map[common.Address]struct{})
	for i := uint64(0); i < header.rowCount; i++ {
		if i&4095 == 0 {
			if err := ctx.Err(); err != nil {
				return EventLogSegmentSpaceStats{}, payloadDist, topicDist, err
			}
		}
		entry, err := readEventLogIndexEntryAt(seg.file, eventLogIndexEntryOffset(header, i))
		if err != nil {
			return EventLogSegmentSpaceStats{}, payloadDist, topicDist, err
		}
		raw, err := seg.readLogPayload(entry)
		if err != nil {
			return EventLogSegmentSpaceStats{}, payloadDist, topicDist, err
		}
		var log corepb.TransactionInfo_Log
		if err := proto.Unmarshal(raw, &log); err != nil {
			return EventLogSegmentSpaceStats{}, payloadDist, topicDist, fmt.Errorf("snapshots: inspect event log %q row %d: %w", seg.ref.Path, i, err)
		}
		if err := validateEventLogPayload(entry, &log, "event log space inspect"); err != nil {
			return EventLogSegmentSpaceStats{}, payloadDist, topicDist, err
		}
		payloadDist.add(entry.length)
		topicDist.add(uint64(len(log.GetTopics())))
		if entry.blockHash == (common.Hash{}) {
			stats.Duplicates.ZeroBlockHashes++
		}
		if !*haveBlockHash || entry.blockHash != *previousBlockHash {
			stats.Duplicates.DistinctBlockHashes++
			*previousBlockHash = entry.blockHash
			*haveBlockHash = true
		}
		if entry.txHash == (common.Hash{}) {
			stats.Duplicates.ZeroTxHashes++
		}
		if !*haveTxHash || entry.txHash != *previousTxHash {
			stats.Duplicates.DistinctTxHashes++
			*previousTxHash = entry.txHash
			*haveTxHash = true
		}
		distinctAddresses[entry.address] = struct{}{}
		segmentAddresses[entry.address] = struct{}{}
		if err := acc.add(*globalRow, entry, &log); err != nil {
			return EventLogSegmentSpaceStats{}, payloadDist, topicDist, err
		}
		*globalRow++
	}
	stats.Duplicates.RepeatedBlockHashes = stats.Duplicates.Rows - stats.Duplicates.DistinctBlockHashes
	stats.Duplicates.RepeatedTxHashes = stats.Duplicates.Rows - stats.Duplicates.DistinctTxHashes
	stats.Duplicates.DistinctAddresses = uint64(len(segmentAddresses))
	stats.Duplicates.RepeatedAddresses = stats.Duplicates.Rows - stats.Duplicates.DistinctAddresses
	stats.PayloadSizes = payloadDist.summary()
	stats.TopicCounts = topicDist.summary()
	return stats, payloadDist, topicDist, nil
}

func inspectEventLogV3Space(ctx context.Context, seg *EventLogSegment, acc *eventLogCandidateAccumulator, globalRow *uint64, previousBlockHash *common.Hash, haveBlockHash *bool, previousTxHash *common.Hash, haveTxHash *bool, distinctAddresses map[common.Address]struct{}) (EventLogSegmentSpaceStats, eventLogExactDistribution, eventLogExactDistribution, error) {
	h := seg.header.v3
	var payloadDist, topicDist eventLogExactDistribution
	stats := EventLogSegmentSpaceStats{
		Path: seg.ref.Path, Version: EventLogSegmentV4Version, FromBlock: h.fromBlock, ToBlock: h.toBlock, Rows: h.rowCount,
		Physical: EventLogPhysicalBytes{
			MainSegment: seg.size, Header: eventLogV3HeaderSize,
			FixedRowIndex:           h.blockDictLength + h.txDictLength + h.rowDirLength + h.rowDataLength,
			ProtobufPayload:         h.payloadDirLength + h.payloadDataLength,
			EmbeddedAddressPostings: h.addressIndexLength,
			EmbeddedTopicPostings:   h.topicIndexLength,
			Total:                   seg.size,
		},
	}
	stats.Duplicates.Rows = h.rowCount
	segmentAddresses := make(map[common.Address]struct{})
	for i := uint64(0); i < h.rowCount; i++ {
		if i&4095 == 0 {
			if err := ctx.Err(); err != nil {
				return EventLogSegmentSpaceStats{}, payloadDist, topicDist, err
			}
		}
		encoded, err := seg.readEventLogV3Row(i)
		if err != nil {
			return EventLogSegmentSpaceStats{}, payloadDist, topicDist, err
		}
		row, err := seg.materializeEventLogV3(encoded)
		if err != nil {
			return EventLogSegmentSpaceStats{}, payloadDist, topicDist, err
		}
		stripped := proto.Clone(row.Log).(*corepb.TransactionInfo_Log)
		stripped.Address = nil
		raw, err := proto.Marshal(stripped)
		if err != nil {
			return EventLogSegmentSpaceStats{}, payloadDist, topicDist, err
		}
		entry := eventLogIndexEntry{blockNum: row.BlockNum, txIndex: row.TxIndex, logIndex: row.LogIndex, txHash: row.TxHash, blockHash: row.BlockHash, address: row.Address, length: uint64(len(raw))}
		payloadDist.add(entry.length)
		topicDist.add(uint64(len(row.Log.GetTopics())))
		if row.BlockHash == (common.Hash{}) {
			stats.Duplicates.ZeroBlockHashes++
		}
		if !*haveBlockHash || row.BlockHash != *previousBlockHash {
			stats.Duplicates.DistinctBlockHashes++
			*previousBlockHash = row.BlockHash
			*haveBlockHash = true
		}
		if row.TxHash == (common.Hash{}) {
			stats.Duplicates.ZeroTxHashes++
		}
		if !*haveTxHash || row.TxHash != *previousTxHash {
			stats.Duplicates.DistinctTxHashes++
			*previousTxHash = row.TxHash
			*haveTxHash = true
		}
		distinctAddresses[row.Address] = struct{}{}
		segmentAddresses[row.Address] = struct{}{}
		if err := acc.add(*globalRow, entry, row.Log); err != nil {
			return EventLogSegmentSpaceStats{}, payloadDist, topicDist, err
		}
		*globalRow++
	}
	stats.Duplicates.RepeatedBlockHashes = stats.Duplicates.Rows - stats.Duplicates.DistinctBlockHashes
	stats.Duplicates.RepeatedTxHashes = stats.Duplicates.Rows - stats.Duplicates.DistinctTxHashes
	stats.Duplicates.DistinctAddresses = uint64(len(segmentAddresses))
	stats.Duplicates.RepeatedAddresses = stats.Duplicates.Rows - stats.Duplicates.DistinctAddresses
	stats.PayloadSizes, stats.TopicCounts = payloadDist.summary(), topicDist.summary()
	return stats, payloadDist, topicDist, nil
}

func aggregateEventLogDuplicateStats(segments []EventLogSegmentSpaceStats) EventLogDuplicateStats {
	var out EventLogDuplicateStats
	for _, segment := range segments {
		out.Rows += segment.Duplicates.Rows
		out.DistinctBlockHashes += segment.Duplicates.DistinctBlockHashes
		out.ZeroBlockHashes += segment.Duplicates.ZeroBlockHashes
		out.DistinctTxHashes += segment.Duplicates.DistinctTxHashes
		out.ZeroTxHashes += segment.Duplicates.ZeroTxHashes
	}
	out.RepeatedBlockHashes = out.Rows - out.DistinctBlockHashes
	out.RepeatedTxHashes = out.Rows - out.DistinctTxHashes
	return out
}

func selectEvenEventLogRefs(refs []SegmentRef, count uint64) []SegmentRef {
	if count == 0 || count >= uint64(len(refs)) {
		return append([]SegmentRef(nil), refs...)
	}
	if count == 1 {
		return []SegmentRef{refs[len(refs)/2]}
	}
	out := make([]SegmentRef, 0, count)
	for i := uint64(0); i < count; i++ {
		index := i * uint64(len(refs)-1) / (count - 1)
		out = append(out, refs[index])
	}
	return out
}

func selectEventLogIndexRefs(indexRefs, allEvents, selected []SegmentRef) []SegmentRef {
	selectedPaths := make(map[string]struct{}, len(selected))
	for _, ref := range selected {
		selectedPaths[ref.Path] = struct{}{}
	}
	var out []SegmentRef
	for _, indexRef := range indexRefs {
		coveredEvents := 0
		allSelected := true
		for _, eventRef := range allEvents {
			if indexRef.ToTxNum < eventRef.FromTxNum || indexRef.FromTxNum > eventRef.ToTxNum {
				continue
			}
			coveredEvents++
			if _, ok := selectedPaths[eventRef.Path]; !ok {
				allSelected = false
				break
			}
		}
		if coveredEvents > 0 && allSelected {
			out = append(out, indexRef)
		}
	}
	return out
}

func inspectEventLogSidecarPhysicalBytes(dir string, generation uint64, refs []SegmentRef) (uint64, error) {
	var total uint64
	for _, ref := range refs {
		seg, err := OpenEventLogIndexSegment(dir, ref)
		if err != nil {
			return 0, fmt.Errorf("snapshots: inspect generation %d event log index %q: %w", generation, ref.Path, err)
		}
		if ref.Size != 0 && seg.size != ref.Size {
			_ = seg.Close()
			return 0, fmt.Errorf("snapshots: inspect generation %d event log index %q size %d, want manifest size %d", generation, ref.Path, seg.size, ref.Size)
		}
		total += seg.size
		if err := seg.Close(); err != nil {
			return 0, err
		}
	}
	return total, nil
}

func candidateMergedEventLogSidecarBytes(addressKeys, topicKeys uint64) uint64 {
	return eventLogCandidateSidecarHeader + eventLogSinglePostingLookup(addressKeys, eventLogAddressLookupKeySize).EncodedBytes + eventLogSinglePostingLookup(topicKeys, eventLogTopicLookupKeySize).EncodedBytes
}

func eventLogSinglePostingLookup(keys, keySize uint64) EventLogLookupCandidateStats {
	stats := EventLogLookupCandidateStats{
		Keys:              keys,
		Postings:          keys,
		DirectoryBytes:    8 + keys*(keySize+eventLogCandidateLookupDirMetadata),
		FrameDirectory:    keys * eventLogCandidateLookupFrameDir,
		PostingBytes:      keys,
		Frames:            keys,
		MaxPostingsPerKey: min(keys, uint64(1)),
		MaxFramesPerKey:   min(keys, uint64(1)),
		MaxFrameBytes:     min(keys, uint64(1)),
	}
	stats.EncodedBytes = stats.DirectoryBytes + stats.FrameDirectory + stats.PostingBytes
	return stats
}

func projectEventLogMerges(activeEvents, activeIndexes, selectedEvents uint64, addressPostings, topicPostings map[string]*eventLogCandidatePostingState) []EventLogMergeProjection {
	var out []EventLogMergeProjection
	for mergeIndex, factor := range eventLogCandidateMergeFactors {
		addressStats := eventLogCandidateLookup(addressPostings, eventLogAddressLookupKeySize, mergeIndex)
		topicStats := eventLogCandidateLookup(topicPostings, eventLogTopicLookupKeySize, mergeIndex)
		out = append(out, EventLogMergeProjection{
			MergeFactor:                 factor,
			ProjectedEventLogFiles:      ceilDiv(activeEvents, factor),
			ProjectedExternalIndexFiles: ceilDiv(activeIndexes, factor),
			SampleAddressPostings:       addressStats.Postings,
			SampleTopicPostings:         topicStats.Postings,
			SampleSidecarBytes:          eventLogCandidateSidecarHeader + addressStats.EncodedBytes + topicStats.EncodedBytes,
			RepresentativeOnly:          selectedEvents != activeEvents,
		})
	}
	return out
}

func ceilDiv(value, divisor uint64) uint64 {
	if value == 0 {
		return 0
	}
	return 1 + (value-1)/divisor
}

func checkedCandidateSavings(current, candidate uint64) int64 {
	if current == 0 {
		return 0
	}
	if candidate <= current {
		return int64(milliRatio(current-candidate, current))
	}
	over := candidate - current
	ratio := milliRatio(over, current)
	if ratio > uint64(math.MaxInt64) {
		return math.MinInt64
	}
	return -int64(ratio)
}

func milliRatio(value, total uint64) uint64 {
	if total == 0 {
		return 0
	}
	whole := value / total
	if whole > math.MaxUint64/1000 {
		return math.MaxUint64
	}
	base := whole * 1000
	remainder := value % total
	if remainder > math.MaxUint64/1000 {
		return math.MaxUint64
	}
	return base + remainder*1000/total
}

func singleKeyLookupReadBound(stats EventLogLookupCandidateStats, keySize uint64) uint64 {
	var probes uint64 = 1
	for keys := stats.Keys; keys > 1; keys = (keys + 1) / 2 {
		probes++
	}
	return probes*(keySize+eventLogCandidateLookupDirMetadata) + stats.MaxFramesPerKey*eventLogCandidateLookupFrameDir + stats.MaxFramesPerKey*eventLogCandidateLookupFrameBytes
}
