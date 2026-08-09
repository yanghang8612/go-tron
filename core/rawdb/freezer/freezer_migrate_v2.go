package freezer

import (
	"bytes"
	"container/list"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// V2MigrationOptions controls conversion of prunable V1 tables into immutable
// Zstd segments. Online installs each published segment before reclaiming its
// V1 source; the offline command keeps the lower-overhead final reload path.
// SegmentBlocks bounds temporary disk use and FrameBlocks controls random-read
// amplification. Mainnet measurement selected 64 records per frame and 65,536
// records per segment.
type V2MigrationOptions struct {
	Tables        []string
	SegmentBlocks uint64
	FrameBlocks   uint32
	MaxSegments   uint64
	KeepV1        bool
	Online        bool
	Context       context.Context
	Progress      func(V2MigrationProgress)
	// Transform optionally rewrites a row before compression. body is the
	// corresponding V1 bodies row when kind == "tx_infos", and nil for other
	// tables. It must be deterministic because verification invokes it again.
	Transform func(kind string, number uint64, data, body []byte) ([]byte, error)
}

type V2MigrationProgress struct {
	Stage         string
	Table         string
	Segment       uint64
	Start         uint64
	End           uint64
	Rows          uint64
	Head          uint64
	Elapsed       time.Duration
	PhysicalBytes uint64
}

type V2MigrationResult struct {
	Start               uint64
	End                 uint64
	Head                uint64
	Segments            uint64
	FrameBlocks         uint32
	SegmentBlocks       uint64
	KeptV1              bool
	PhysicalBytesBefore uint64
	PhysicalBytesAfter  uint64
	Elapsed             time.Duration
}

func (f *Freezer) MigrateV2(options V2MigrationOptions) (V2MigrationResult, error) {
	var result V2MigrationResult
	f.v2Migrate.Lock()
	defer f.v2Migrate.Unlock()
	f.tailMutation.Lock()
	defer f.tailMutation.Unlock()
	if f.readonly {
		return result, errReadOnly
	}
	if len(options.Tables) == 0 {
		return result, fmt.Errorf("ancient V2: no tables selected")
	}
	// The current freezer persists one virtual tail for the complete table set.
	// Reclaiming only a subset would hide an unconverted table (notably
	// state_roots) at the same boundary. Require a complete segment family so
	// publication followed by TruncateTail cannot create a cross-table gap.
	if len(options.Tables) != len(f.tables) {
		return result, fmt.Errorf("ancient V2: all %d freezer tables must be selected before V1 reclamation", len(f.tables))
	}
	if options.SegmentBlocks == 0 {
		options.SegmentBlocks = v2DefaultSegments
	}
	if options.FrameBlocks == 0 {
		options.FrameBlocks = v2DefaultFrames
	}
	if options.Context == nil {
		options.Context = context.Background()
	}
	if options.SegmentBlocks%uint64(options.FrameBlocks) != 0 {
		return result, fmt.Errorf("ancient V2: segment blocks %d must be divisible by frame blocks %d", options.SegmentBlocks, options.FrameBlocks)
	}
	for _, kind := range options.Tables {
		table := f.tables[kind]
		if table == nil {
			return result, fmt.Errorf("ancient V2: unknown table %s", kind)
		}
		if !table.config.prunable {
			return result, fmt.Errorf("ancient V2: table %s is not prunable", kind)
		}
	}
	for kind := range f.tables {
		found := false
		for _, selected := range options.Tables {
			if selected == kind {
				found = true
				break
			}
		}
		if !found {
			return result, fmt.Errorf("ancient V2: freezer table %s is not selected", kind)
		}
	}

	started := time.Now()
	start := f.V2Coverage()
	result.Start = start
	result.FrameBlocks = options.FrameBlocks
	result.SegmentBlocks = options.SegmentBlocks
	result.KeptV1 = options.KeepV1
	result.PhysicalBytesBefore = f.selectedPhysicalBytes(options.Tables)

	// A crash after publishing a manifest but before V1 tail reclamation leaves
	// safe duplicate data. Reconcile it before writing the next segment.
	if !options.KeepV1 && f.tail.Load() < start {
		if _, err := f.truncateTailLocked(start); err != nil {
			return result, fmt.Errorf("ancient V2 reconcile V1 tail: %w", err)
		}
		if _, err := f.PruneTailFiles(); err != nil {
			return result, fmt.Errorf("ancient V2 reconcile V1 files: %w", err)
		}
	}
	if f.tail.Load() > start {
		return result, fmt.Errorf("%w: coverage %d is behind V1 tail %d", ErrV2SourcePruned, start, f.tail.Load())
	}

	head := f.head.Load()
	for _, kind := range options.Tables {
		if count := f.tables[kind].items.Load(); count < head {
			head = count
		}
	}
	result.Head = head
	target := head / options.SegmentBlocks * options.SegmentBlocks
	if start > target || start%options.SegmentBlocks != 0 {
		return result, fmt.Errorf("ancient V2 coverage %d is not aligned to target %d", start, target)
	}
	base := filepath.Join(f.datadir, "v2")
	removeOrphanV2Temps(base)
	for start < target && (options.MaxSegments == 0 || result.Segments < options.MaxSegments) {
		if err := options.Context.Err(); err != nil {
			return result, err
		}
		segmentStarted := time.Now()
		count := options.SegmentBlocks
		manifest := v2Manifest{
			Version:     v2Version,
			Start:       start,
			Count:       count,
			FrameBlocks: options.FrameBlocks,
			Tables:      make(map[string]string, len(options.Tables)),
		}
		if options.Transform != nil {
			manifest.TxInfoIDsCompacted = true
		}
		var unpublished []string
		cleanupUnpublished := func() {
			for _, path := range unpublished {
				_ = os.Remove(path)
			}
		}
		for _, kind := range options.Tables {
			name := v2SegmentName(start, count)
			path := filepath.Join(base, kind, name)
			table := f.tables[kind]
			lastProgress := time.Now()
			readV1 := func(number uint64) ([]byte, error) {
				data, err := table.Retrieve(number)
				if err != nil {
					return nil, fmt.Errorf("read V1 %s[%d]: %w", kind, number, err)
				}
				return data, nil
			}
			readForWrite := func(number uint64) ([]byte, error) {
				if err := options.Context.Err(); err != nil {
					return nil, err
				}
				data, err := readV1(number)
				if err != nil {
					return nil, err
				}
				data, err = f.transformV2MigrationRecord(options, kind, number, data)
				if err != nil {
					return nil, err
				}
				if options.Progress != nil && time.Since(lastProgress) >= 30*time.Second {
					options.Progress(V2MigrationProgress{
						Stage:   "writing",
						Table:   kind,
						Segment: result.Segments + 1,
						Start:   start,
						End:     start + count,
						Rows:    number - start + 1,
						Head:    head,
						Elapsed: time.Since(segmentStarted),
					})
					lastProgress = time.Now()
				}
				return data, nil
			}
			unpublished = append(unpublished, path)
			if err := writeV2Segment(path, start, count, options.FrameBlocks, readForWrite); err != nil {
				cleanupUnpublished()
				return result, fmt.Errorf("write ancient V2 %s segment %d: %w", kind, start, err)
			}
			readExpected := func(number uint64) ([]byte, error) {
				data, err := readV1(number)
				if err != nil {
					return nil, err
				}
				return f.transformV2MigrationRecord(options, kind, number, data)
			}
			if err := verifyV2Segment(options.Context, path, kind, start, count, readExpected); err != nil {
				cleanupUnpublished()
				return result, err
			}
			manifest.Tables[kind] = name
		}
		if err := publishV2Manifest(base, manifest); err != nil {
			return result, fmt.Errorf("publish ancient V2 segment %d: %w", start, err)
		}
		end := start + count
		if options.Online {
			newStore, err := openV2Store(f.datadir)
			if err != nil {
				return result, fmt.Errorf("reload ancient V2 after segment %d: %w", start, err)
			}
			f.replaceV2Store(newStore)
		}
		if !options.KeepV1 {
			if _, err := f.truncateTailLocked(end); err != nil {
				return result, fmt.Errorf("reclaim V1 through %d: %w", end, err)
			}
			if _, err := f.PruneTailFiles(); err != nil {
				return result, fmt.Errorf("reclaim V1 files through %d: %w", end, err)
			}
		}
		start = end
		result.End = end
		result.Segments++
		if options.Progress != nil {
			options.Progress(V2MigrationProgress{
				Stage:         "complete",
				Segment:       result.Segments,
				Start:         manifest.Start,
				End:           end,
				Head:          head,
				Elapsed:       time.Since(segmentStarted),
				PhysicalBytes: f.selectedPhysicalBytes(options.Tables),
			})
		}
	}
	if result.End == 0 {
		result.End = result.Start
	}
	if !options.Online {
		newStore, err := openV2Store(f.datadir)
		if err != nil {
			return result, fmt.Errorf("reload ancient V2: %w", err)
		}
		f.replaceV2Store(newStore)
	}
	result.PhysicalBytesAfter = f.selectedPhysicalBytes(options.Tables)
	result.Elapsed = time.Since(started)
	return result, nil
}

func (f *Freezer) transformV2MigrationRecord(options V2MigrationOptions, kind string, number uint64, data []byte) ([]byte, error) {
	if options.Transform == nil {
		return data, nil
	}
	var body []byte
	if kind == "tx_infos" {
		table := f.tables["bodies"]
		if table == nil {
			return nil, fmt.Errorf("ancient V2: bodies table required to transform tx_infos")
		}
		var err error
		body, err = table.Retrieve(number)
		if err != nil {
			return nil, fmt.Errorf("read V1 bodies[%d] for transform: %w", number, err)
		}
	}
	return options.Transform(kind, number, data, body)
}

func (f *Freezer) selectedPhysicalBytes(tables []string) uint64 {
	var total uint64
	for _, kind := range tables {
		if table := f.tables[kind]; table != nil {
			if size, err := table.size(); err == nil {
				total += size
			}
		}
		f.v2Mu.RLock()
		if f.v2 != nil {
			total += f.v2.size(kind)
		}
		f.v2Mu.RUnlock()
	}
	return total
}

func (f *Freezer) replaceV2Store(store *v2Store) {
	f.v2Mu.Lock()
	oldStore := f.v2
	f.v2 = store
	f.v2Mu.Unlock()
	if oldStore != nil {
		_ = oldStore.Close()
	}
}

func verifyV2Segment(ctx context.Context, path, kind string, start, count uint64, readV1 func(uint64) ([]byte, error)) error {
	reader, err := openV2Segment(path, kind)
	if err != nil {
		return fmt.Errorf("open new ancient V2 segment %s: %w", path, err)
	}
	decoder, err := newV2Decoder()
	if err != nil {
		reader.Close()
		return err
	}
	store := &v2Store{
		segments:          map[string][]*v2SegmentReader{kind: {reader}},
		decoder:           decoder,
		cacheList:         list.New(),
		cacheItems:        make(map[v2FrameKey]*list.Element),
		cacheLoads:        make(map[v2FrameKey]*v2FrameLoad),
		cacheLimit:        v2DefaultCacheBytes,
		compressedBuffers: make(chan *v2CompressedBuffer, v2DecoderConcurrency),
	}
	defer store.Close()
	for frameIndex, frame := range reader.frames {
		if err := ctx.Err(); err != nil {
			return err
		}
		decoded, err := store.readFrame(reader, frameIndex)
		if err != nil {
			return fmt.Errorf("verify ancient V2 %s frame %d: %w", kind, frameIndex, err)
		}
		if len(decoded.records) != int(frame.records) {
			return fmt.Errorf("verify ancient V2 %s frame %d: record count %d, want %d", kind, frameIndex, len(decoded.records), frame.records)
		}
	}
	checks := []uint64{start, start + count/2, start + count - 1}
	for _, number := range checks {
		want, err := readV1(number)
		if err != nil {
			return err
		}
		got, err := store.read(kind, number)
		if err != nil {
			return fmt.Errorf("verify ancient V2 %s[%d]: %w", kind, number, err)
		}
		if !bytes.Equal(got, want) {
			return fmt.Errorf("verify ancient V2 %s[%d]: byte mismatch", kind, number)
		}
	}
	return nil
}

func removeOrphanV2Temps(base string) {
	_ = filepath.WalkDir(base, func(path string, entry os.DirEntry, err error) error {
		if err == nil && !entry.IsDir() && strings.HasSuffix(entry.Name(), ".tmp") {
			_ = os.Remove(path)
		}
		return nil
	})
}
