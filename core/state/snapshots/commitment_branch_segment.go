package snapshots

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"hash"
	"io"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"

	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/tronprotocol/go-tron/core/pointread"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/state/kvdomains"
)

// SegmentDatasetCommitmentBranch labels the staged commitment engine's
// branch-row snapshot family. It streams the dedicated
// state-commitment-branch-v1- keyspace (hex-trie prefix -> encoded BranchData),
// which the legacy CommitmentNode family (tree/node/ logical keys, 32-byte hash
// values) cannot represent. Fresh production builds use the common immutable
// binary latest family (.seg + .lidx + .bt); the JSON reader remains only for
// previously produced/manual bootstrap fixtures.
const SegmentDatasetCommitmentBranch SegmentDataset = "commitment-branch"

// CommitmentBranchSegmentVersion is the on-disk version of a branch segment.
const CommitmentBranchSegmentVersion = 1

// commitmentBranchEntry is one persisted branch row. Encoded is the opaque
// BranchData.Encode() value; the snapshot layer never decodes it.
type commitmentBranchEntry struct {
	Prefix  []byte `json:"prefix"`
	Encoded []byte `json:"encoded"`
}

// CommitmentBranchSegment is an opened, validated branch segment ready for
// streaming iteration. It retains only the segment location; branch rows stay
// on disk until Iterate consumes them.
type CommitmentBranchSegment struct {
	ref    SegmentRef
	path   string
	binary bool
}

// CommitmentBranchPointView owns one open segment descriptor plus a packed
// resident copy of its sparse B-tree. The B-tree file itself is read once and
// closed during OpenCommitmentBranchSnapshot. Lookups use one segment ReadAt,
// so all 16 ordered commitment lanes may share the view without cursor locks or
// per-block descriptor churn.
type CommitmentBranchPointView struct {
	mu            sync.RWMutex
	txNum         uint64
	segment       *os.File
	segmentHeader latestBinaryHeader
	btreeHeader   latestBinaryBTreeHeader

	// The sparse B-tree has one entry per 128 segment rows. Keeping those
	// entries resident turns every hot lookup from O(log N) tiny ReadAt calls
	// plus up to 128 per-row reads into one in-memory floor search and one
	// contiguous segment ReadAt. Keys share keyArena, so the steady resident
	// footprint is one entry descriptor plus the sparse key bytes per block.
	index    []latestBinaryBTreeEntry
	keyArena []byte

	// scratchPool is deliberately bounded by both concurrency and total retained
	// bytes. The commitment fold normally has 16 lanes; ordinary ~70 KiB blocks
	// retain all 32 slots, while an unusually large valid block lowers the slot
	// count instead of pinning hundreds of MiB after a transient burst.
	maxBlockBytes int
	scratchPool   chan []byte
}

const (
	commitmentBranchPointScratchPoolSize = 32
	// BranchData rows are normally below 1 KiB, so a 128-row block is around
	// 64-128 KiB. Rejecting an implausibly large block prevents a malformed or
	// incompatible snapshot from turning one lookup into an unbounded allocation.
	commitmentBranchPointMaxBlockBytes           = 8 << 20
	commitmentBranchPointMaxRetainedScratchBytes = 32 << 20
	// The sparse B-tree is roughly one 45-byte record per 128 branch rows.
	// 256 MiB therefore covers branch baselines far beyond the current chain
	// while bounding the one sequential temporary allocation during open.
	commitmentBranchPointMaxIndexFileBytes = 256 << 20
)

var errCommitmentBranchPointViewClosed = errors.New("snapshots: closed commitment branch point view")

type commitmentBranchBinaryIterator struct {
	file   *os.File
	header latestBinaryHeader
	index  uint64
	prev   []byte
	prefix []byte
	value  []byte
	err    error
}

var _ pointread.CommitmentBranchSnapshotView = (*CommitmentBranchPointView)(nil)

func (v *CommitmentBranchPointView) Get(prefix []byte) ([]byte, bool, error) {
	if v == nil {
		return nil, false, errCommitmentBranchPointViewClosed
	}
	v.mu.RLock()
	defer v.mu.RUnlock()
	if v.segment == nil || len(v.index) == 0 || v.maxBlockBytes <= 0 || v.scratchPool == nil {
		return nil, false, errCommitmentBranchPointViewClosed
	}

	var keyStack [65]byte // one schema byte plus the 64-nibble commitment path
	var key []byte
	if len(prefix)+1 <= len(keyStack) {
		key = keyStack[:len(prefix)+1]
	} else {
		key = make([]byte, len(prefix)+1)
	}
	copy(key[1:], prefix)

	index := sort.Search(len(v.index), func(i int) bool {
		return bytes.Compare(v.index[i].key, key) > 0
	})
	if index == 0 {
		return nil, false, nil
	}
	entry := v.index[index-1]
	end := v.segmentHeader.fileSize
	if index < len(v.index) {
		end = v.index[index].segmentOffset
	}
	if end < entry.segmentOffset || end-entry.segmentOffset > uint64(v.maxBlockBytes) {
		return nil, false, errors.New("snapshots: invalid commitment branch resident block span")
	}

	scratch := v.borrowScratch()
	defer v.returnScratch(scratch)
	block := scratch[:int(end-entry.segmentOffset)]
	if _, err := v.segment.ReadAt(block, int64(entry.segmentOffset)); err != nil {
		return nil, false, err
	}
	return readCommitmentBranchValueFromBlock(block, v.segmentHeader, entry.ordinal, v.btreeHeader.blockSize, key)
}

func (v *CommitmentBranchPointView) SnapshotTxNum() uint64 {
	if v == nil {
		return 0
	}
	v.mu.RLock()
	defer v.mu.RUnlock()
	return v.txNum
}

func (v *CommitmentBranchPointView) Close() error {
	if v == nil {
		return nil
	}
	v.mu.Lock()
	defer v.mu.Unlock()
	var first error
	if v.segment != nil {
		first = v.segment.Close()
		v.segment = nil
	}
	v.index = nil
	v.keyArena = nil
	v.maxBlockBytes = 0
	v.scratchPool = nil
	return first
}

func (v *CommitmentBranchPointView) borrowScratch() []byte {
	select {
	case scratch := <-v.scratchPool:
		return scratch[:v.maxBlockBytes]
	default:
		return make([]byte, v.maxBlockBytes)
	}
}

func (v *CommitmentBranchPointView) returnScratch(scratch []byte) {
	if cap(scratch) < v.maxBlockBytes {
		return
	}
	scratch = scratch[:v.maxBlockBytes]
	select {
	case v.scratchPool <- scratch:
	default:
	}
}

func commitmentBranchPointScratchPoolCapacity(blockBytes int) int {
	if blockBytes <= 0 {
		return 0
	}
	byBytes := commitmentBranchPointMaxRetainedScratchBytes / blockBytes
	if byBytes < 1 {
		byBytes = 1
	}
	if byBytes > commitmentBranchPointScratchPoolSize {
		byBytes = commitmentBranchPointScratchPoolSize
	}
	return byBytes
}

func readCommitmentBranchValueFromBlock(block []byte, header latestBinaryHeader, firstOrdinal, blockSize uint64, key []byte) ([]byte, bool, error) {
	offset := 0
	limit := header.count - firstOrdinal
	if limit > blockSize {
		limit = blockSize
	}
	for ordinal := uint64(0); ordinal < limit; ordinal++ {
		if len(block)-offset < 8 {
			return nil, false, io.ErrUnexpectedEOF
		}
		keyLen := int(binary.BigEndian.Uint32(block[offset : offset+4]))
		frame, err := latestBinaryValueFrameFromStoredLength(binary.BigEndian.Uint32(block[offset+4:offset+8]), header.compressedValues)
		if err != nil {
			return nil, false, err
		}
		if err := validateLatestBinaryValueFrame(frame); err != nil {
			return nil, false, err
		}
		keyStart := offset + 8
		valueStart := keyStart + keyLen
		next := valueStart + int(frame.encodedLen)
		if keyStart < 0 || valueStart < keyStart || next < valueStart || next > len(block) {
			return nil, false, io.ErrUnexpectedEOF
		}
		entryKey := block[keyStart:valueStart]
		cmp := bytes.Compare(entryKey, key)
		if cmp == 0 {
			value, err := decodeLatestBinaryValue(block[valueStart:next], frame)
			if err != nil {
				return nil, false, err
			}
			if err := validateLatestBinaryEntry(header.dataset, entryKey, value); err != nil {
				return nil, false, err
			}
			// Uncompressed values alias the pooled block. Preserve Get's owned
			// result contract before returning the scratch buffer to the pool.
			if !frame.compressed {
				value = append([]byte(nil), value...)
			}
			return value, true, nil
		}
		if cmp > 0 {
			return nil, false, nil
		}
		offset = next
	}
	return nil, false, nil
}

func newCommitmentBranchPointView(txNum uint64, segment *os.File, segmentHeader latestBinaryHeader, btree *os.File, btreeHeader latestBinaryBTreeHeader) (*CommitmentBranchPointView, error) {
	if segment == nil || btree == nil {
		return nil, errors.New("snapshots: nil commitment branch point descriptor")
	}
	if btreeHeader.count == 0 || btreeHeader.blockSize == 0 {
		return nil, errors.New("snapshots: empty commitment branch resident index")
	}
	expectedEntries := segmentHeader.count / btreeHeader.blockSize
	if segmentHeader.count%btreeHeader.blockSize != 0 {
		expectedEntries++
	}
	if btreeHeader.count != expectedEntries {
		return nil, fmt.Errorf("snapshots: commitment branch resident index count %d, want %d", btreeHeader.count, expectedEntries)
	}
	entries, totalKeyBytes, err := readCommitmentBranchResidentIndexFile(btree, btreeHeader, segmentHeader)
	if err != nil {
		return nil, err
	}

	keyArena := make([]byte, totalKeyBytes)
	keyOffset := 0
	maxBlockBytes := 0
	for i := range entries {
		copy(keyArena[keyOffset:], entries[i].key)
		entries[i].key = keyArena[keyOffset : keyOffset+len(entries[i].key)]
		keyOffset += len(entries[i].key)
		end := segmentHeader.fileSize
		if i+1 < len(entries) {
			end = entries[i+1].segmentOffset
		}
		if end <= entries[i].segmentOffset || end-entries[i].segmentOffset > commitmentBranchPointMaxBlockBytes {
			return nil, fmt.Errorf("snapshots: commitment branch resident block span %d exceeds limit %d", end-entries[i].segmentOffset, commitmentBranchPointMaxBlockBytes)
		}
		span := int(end - entries[i].segmentOffset)
		if span > maxBlockBytes {
			maxBlockBytes = span
		}
	}
	return &CommitmentBranchPointView{
		txNum:         txNum,
		segment:       segment,
		segmentHeader: segmentHeader,
		btreeHeader:   btreeHeader,
		index:         entries,
		keyArena:      keyArena,
		maxBlockBytes: maxBlockBytes,
		scratchPool:   make(chan []byte, commitmentBranchPointScratchPoolCapacity(maxBlockBytes)),
	}, nil
}

// readCommitmentBranchResidentIndexFile loads the complete sparse B-tree with
// one contiguous ReadAt, then validates and packs its keys. The previous open
// path called readLatestBinaryBTreeEntryAt per sparse row, which meant at least
// three preads per entry (offset, header, key) and scaled to millions of syscalls
// for a large base before the first block could execute.
func readCommitmentBranchResidentIndexFile(btree *os.File, btreeHeader latestBinaryBTreeHeader, segmentHeader latestBinaryHeader) ([]latestBinaryBTreeEntry, int, error) {
	if btreeHeader.fileSize > commitmentBranchPointMaxIndexFileBytes || btreeHeader.fileSize > uint64(math.MaxInt) {
		return nil, 0, fmt.Errorf("snapshots: commitment branch sparse index file %d exceeds limit %d", btreeHeader.fileSize, commitmentBranchPointMaxIndexFileBytes)
	}
	if btreeHeader.count > uint64(math.MaxInt) {
		return nil, 0, errors.New("snapshots: commitment branch resident index is too large")
	}
	offsetBytes := btreeHeader.count * 8
	if btreeHeader.count != 0 && offsetBytes/8 != btreeHeader.count {
		return nil, 0, errors.New("snapshots: commitment branch resident offset table overflow")
	}
	payloadStart := uint64(latestBinaryBTreeHeaderSize) + offsetBytes
	if payloadStart < latestBinaryBTreeHeaderSize || payloadStart > btreeHeader.fileSize {
		return nil, 0, errors.New("snapshots: commitment branch resident offset table exceeds index file")
	}

	data := make([]byte, int(btreeHeader.fileSize))
	n, err := btree.ReadAt(data, 0)
	if err != nil && !(errors.Is(err, io.EOF) && n == len(data)) {
		return nil, 0, err
	}
	if n != len(data) {
		return nil, 0, io.ErrUnexpectedEOF
	}
	entries := make([]latestBinaryBTreeEntry, int(btreeHeader.count))
	totalKeyBytes := 0
	for i := range entries {
		offsetPos := latestBinaryBTreeHeaderSize + i*8
		offset := binary.BigEndian.Uint64(data[offsetPos : offsetPos+8])
		if offset < payloadStart || offset > btreeHeader.fileSize-20 {
			return nil, 0, errors.New("snapshots: commitment branch resident entry offset outside index payload")
		}
		head := data[int(offset) : int(offset)+20]
		keyLen := uint64(binary.BigEndian.Uint32(head[:4]))
		keyStart := offset + 20
		keyEnd := keyStart + keyLen
		if keyEnd < keyStart || keyEnd > btreeHeader.fileSize {
			return nil, 0, errors.New("snapshots: commitment branch resident key outside index payload")
		}
		entry := latestBinaryBTreeEntry{
			key:           data[int(keyStart):int(keyEnd)],
			ordinal:       binary.BigEndian.Uint64(head[4:12]),
			segmentOffset: binary.BigEndian.Uint64(head[12:20]),
		}
		if entry.ordinal != uint64(i)*btreeHeader.blockSize {
			return nil, 0, errors.New("snapshots: commitment branch resident index ordinal gap")
		}
		if i == 0 {
			if entry.ordinal != 0 || entry.segmentOffset < latestBinaryHeaderSize {
				return nil, 0, errors.New("snapshots: invalid first commitment branch resident index entry")
			}
		} else {
			prev := entries[i-1]
			if bytes.Compare(prev.key, entry.key) >= 0 || prev.segmentOffset >= entry.segmentOffset {
				return nil, 0, errors.New("snapshots: unsorted commitment branch resident index")
			}
		}
		if entry.ordinal >= segmentHeader.count || entry.segmentOffset >= segmentHeader.fileSize {
			return nil, 0, errors.New("snapshots: commitment branch resident index entry outside segment")
		}
		if len(entry.key) > math.MaxInt-totalKeyBytes {
			return nil, 0, errors.New("snapshots: commitment branch resident index key arena overflow")
		}
		totalKeyBytes += len(entry.key)
		entries[i] = entry
	}
	return entries, totalKeyBytes, nil
}

// BuildCommitmentBranchSegmentFromDB streams every state-commitment-branch-v1-
// row from db into a branch segment file at dir/relPath and returns its
// SegmentRef. Rows are written sorted by prefix for a deterministic file.
func BuildCommitmentBranchSegmentFromDB(db ethdb.Iteratee, dir, relPath string, fromTxNum, toTxNum uint64) (SegmentRef, error) {
	if db == nil {
		return SegmentRef{}, errors.New("snapshots: nil database")
	}
	if err := validateBranchSegmentPath(relPath); err != nil {
		return SegmentRef{}, err
	}
	if toTxNum < fromTxNum {
		return SegmentRef{}, fmt.Errorf("snapshots: branch segment range [%d,%d] is inverted", fromTxNum, toTxNum)
	}
	if isLatestBinarySegmentPath(relPath) {
		return SegmentRef{}, errors.New("snapshots: binary commitment branches require BuildCommitmentBranchSegmentFilesFromDB")
	}
	ref, _, err := writeCommitmentBranchSegmentFromDB(db, dir, relPath, fromTxNum, toTxNum, false)
	return ref, err
}

// BuildCommitmentBranchSegmentFilesFromDB writes the Erigon-style immutable
// branch baseline: one sorted binary value segment, an ordinal accessor, and a
// sparse B-tree. The B-tree is the point-read seam required before hot branch
// rows can become a bounded delta over immutable state instead of a complete
// Pebble-owned latest keyspace.
func BuildCommitmentBranchSegmentFilesFromDB(db ethdb.Iteratee, dir, relPath string, fromTxNum, toTxNum uint64) (SegmentRef, SegmentRef, SegmentRef, error) {
	if db == nil {
		return SegmentRef{}, SegmentRef{}, SegmentRef{}, errors.New("snapshots: nil database")
	}
	if !isLatestBinarySegmentPath(relPath) {
		return SegmentRef{}, SegmentRef{}, SegmentRef{}, fmt.Errorf("snapshots: binary branch segment path %q must end in .seg", relPath)
	}
	if toTxNum < fromTxNum {
		return SegmentRef{}, SegmentRef{}, SegmentRef{}, fmt.Errorf("snapshots: branch segment range [%d,%d] is inverted", fromTxNum, toTxNum)
	}
	ref, accessor, btree, _, err := writeCommitmentBranchBinarySegmentFilesFromDB(db, dir, relPath, fromTxNum, toTxNum, false)
	return ref, accessor, btree, err
}

func writeCommitmentBranchBinarySegmentFilesFromDB(db ethdb.Iteratee, dir, relPath string, fromTxNum, toTxNum uint64, skipEmpty bool) (SegmentRef, SegmentRef, SegmentRef, bool, error) {
	return writeCommitmentBranchBinarySegmentFilesFromDBContext(context.Background(), db, dir, relPath, fromTxNum, toTxNum, skipEmpty)
}

func writeCommitmentBranchBinarySegmentFilesFromDBContext(ctx context.Context, db ethdb.Iteratee, dir, relPath string, fromTxNum, toTxNum uint64, skipEmpty bool) (SegmentRef, SegmentRef, SegmentRef, bool, error) {
	return writeCommitmentBranchBinarySegmentFilesFromIteratorContext(ctx, dir, relPath, fromTxNum, toTxNum, skipEmpty, func(yield func(prefix, encoded []byte) error) error {
		return rawdb.IterateCommitmentBranches(db, func(prefix, encoded []byte) (bool, error) {
			if err := contextError(ctx); err != nil {
				return false, err
			}
			if err := yield(prefix, encoded); err != nil {
				return false, err
			}
			return true, nil
		})
	})
}

type commitmentBranchRowIterator func(func(prefix, encoded []byte) error) error

func writeCommitmentBranchBinarySegmentFilesFromIterator(dir, relPath string, fromTxNum, toTxNum uint64, skipEmpty bool, rowsIter commitmentBranchRowIterator) (SegmentRef, SegmentRef, SegmentRef, bool, error) {
	return writeCommitmentBranchBinarySegmentFilesFromIteratorContext(context.Background(), dir, relPath, fromTxNum, toTxNum, skipEmpty, rowsIter)
}

func writeCommitmentBranchBinarySegmentFilesFromIteratorContext(ctx context.Context, dir, relPath string, fromTxNum, toTxNum uint64, skipEmpty bool, rowsIter commitmentBranchRowIterator) (SegmentRef, SegmentRef, SegmentRef, bool, error) {
	ref := SegmentRef{
		Dataset:   SegmentDatasetCommitmentBranch,
		Kind:      SegmentLatest,
		FromTxNum: fromTxNum,
		ToTxNum:   toTxNum,
		Path:      filepath.ToSlash(relPath),
	}
	var rows uint64
	iter := func(yield func(LatestEntry) error) error {
		return rowsIter(func(prefix, encoded []byte) error {
			if err := contextError(ctx); err != nil {
				return err
			}
			rows++
			key := encodeCommitmentBranchSnapshotKey(prefix)
			return yield(LatestEntry{Key: key, Value: encoded})
		})
	}
	segment, accessor, btree, err := writeLatestBinarySegmentWithCompanionsContext(ctx, dir, ref, iter, true)
	if err != nil {
		return SegmentRef{}, SegmentRef{}, SegmentRef{}, false, err
	}
	if rows != 0 || !skipEmpty {
		return segment, accessor, btree, rows != 0, nil
	}
	// The streaming writer learns that the keyspace is empty only after its one
	// source scan. Do not publish empty branch families; remove the three newly
	// built content-addressed files just as the legacy writer removed its temp.
	for _, built := range []SegmentRef{segment, accessor, btree} {
		if built.Path != "" {
			if err := os.Remove(filepath.Join(dir, built.Path)); err != nil && !errors.Is(err, os.ErrNotExist) {
				return SegmentRef{}, SegmentRef{}, SegmentRef{}, false, err
			}
		}
	}
	return SegmentRef{}, SegmentRef{}, SegmentRef{}, false, nil
}

func openCommitmentBranchBinaryIterator(dir string, ref SegmentRef) (*commitmentBranchBinaryIterator, error) {
	if ref.NormalizedDataset() != SegmentDatasetCommitmentBranch || ref.Kind != SegmentLatest || !isLatestBinarySegmentPath(ref.Path) {
		return nil, fmt.Errorf("snapshots: commitment branch merge requires a binary latest segment, got %+v", ref)
	}
	path := filepath.Join(dir, ref.Path)
	file, header, err := openLatestBinaryReader(path, ref)
	if err != nil {
		return nil, err
	}
	if header.dataset != SegmentDatasetCommitmentBranch || header.kind != SegmentLatest {
		_ = file.Close()
		return nil, fmt.Errorf("snapshots: commitment branch merge segment %q has dataset/kind %s/%s", ref.Path, header.dataset, header.kind)
	}
	return &commitmentBranchBinaryIterator{file: file, header: header}, nil
}

func (it *commitmentBranchBinaryIterator) Next() bool {
	if it == nil || it.file == nil || it.err != nil || it.index >= it.header.count {
		return false
	}
	key, frame, err := readLatestBinaryEntryKey(it.file, it.header.fileSize, it.header.compressedValues)
	if err != nil {
		it.err = fmt.Errorf("snapshots: read commitment branch merge key %d: %w", it.index, err)
		return false
	}
	if len(it.prev) > 0 && bytes.Compare(it.prev, key) >= 0 {
		it.err = errors.New("snapshots: commitment branch merge input is not strictly sorted")
		return false
	}
	value, err := readLatestBinaryValueBytes(it.file, frame, it.header.fileSize)
	if err != nil {
		it.err = fmt.Errorf("snapshots: read commitment branch merge value %d: %w", it.index, err)
		return false
	}
	if err := validateLatestBinaryEntry(it.header.dataset, key, value); err != nil {
		it.err = fmt.Errorf("snapshots: validate commitment branch merge entry %d: %w", it.index, err)
		return false
	}
	prefix, err := decodeCommitmentBranchSnapshotKey(key)
	if err != nil {
		it.err = err
		return false
	}
	it.prev = key
	it.prefix = prefix
	it.value = value
	it.index++
	return true
}

func (it *commitmentBranchBinaryIterator) Key() []byte { return it.prefix }

func (it *commitmentBranchBinaryIterator) Value() []byte { return it.value }

func (it *commitmentBranchBinaryIterator) Error() error {
	if it == nil {
		return nil
	}
	return it.err
}

func (it *commitmentBranchBinaryIterator) Close() error {
	if it == nil || it.file == nil {
		return nil
	}
	err := it.file.Close()
	it.file = nil
	return err
}

// Binary latest keys may not be empty because accessors and B-trees use an
// empty key as an invalid/sentinel value. Prefixing every nibble path with zero
// gives the root a one-byte key and preserves the exact lexicographic order of
// all branch prefixes.
func encodeCommitmentBranchSnapshotKey(prefix []byte) []byte {
	key := make([]byte, len(prefix)+1)
	copy(key[1:], prefix)
	return key
}

func decodeCommitmentBranchSnapshotKey(key []byte) ([]byte, error) {
	if len(key) == 0 || key[0] != 0 {
		return nil, fmt.Errorf("snapshots: invalid commitment branch key %x", key)
	}
	for _, nibble := range key[1:] {
		if nibble >= 16 {
			return nil, fmt.Errorf("snapshots: invalid commitment branch nibble %x in key %x", nibble, key)
		}
	}
	return key[1:], nil
}

// writeCommitmentBranchSegmentFromDB streams a branch segment. When skipEmpty
// is true it removes the temporary output and returns hasRows=false for an
// empty keyspace, letting production latest builds avoid a separate pre-scan.
func writeCommitmentBranchSegmentFromDB(db ethdb.Iteratee, dir, relPath string, fromTxNum, toTxNum uint64, skipEmpty bool) (SegmentRef, bool, error) {
	abs := filepath.Join(dir, relPath)
	if err := os.MkdirAll(filepath.Dir(abs), 0o755); err != nil {
		return SegmentRef{}, false, err
	}
	tmp, err := os.CreateTemp(filepath.Dir(abs), "."+filepath.Base(abs)+".*.tmp")
	if err != nil {
		return SegmentRef{}, false, err
	}
	tmpName := tmp.Name()
	defer os.Remove(tmpName)

	hash := sha256.New()
	counter := &countingWriter{w: io.MultiWriter(tmp, hash)}
	writer := bufio.NewWriterSize(counter, 1<<20)
	if _, err := fmt.Fprintf(writer, `{"version":%d,"dataset":%q,"fromTxNum":%d,"toTxNum":%d,"entries":[`, CommitmentBranchSegmentVersion, SegmentDatasetCommitmentBranch, fromTxNum, toTxNum); err != nil {
		_ = tmp.Close()
		return SegmentRef{}, false, err
	}
	first := true
	if err := rawdb.IterateCommitmentBranches(db, func(prefix, encoded []byte) (bool, error) {
		if !first {
			if err := writer.WriteByte(','); err != nil {
				return false, err
			}
		}
		first = false
		entry, err := json.Marshal(commitmentBranchEntry{Prefix: prefix, Encoded: encoded})
		if err != nil {
			return false, err
		}
		if _, err := writer.Write(entry); err != nil {
			return false, err
		}
		return true, nil
	}); err != nil {
		_ = tmp.Close()
		return SegmentRef{}, false, err
	}
	if first && skipEmpty {
		if err := tmp.Close(); err != nil {
			return SegmentRef{}, false, err
		}
		return SegmentRef{}, false, nil
	}
	if _, err := writer.WriteString(`]}`); err != nil {
		_ = tmp.Close()
		return SegmentRef{}, false, err
	}
	if err := writer.Flush(); err != nil {
		_ = tmp.Close()
		return SegmentRef{}, false, err
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return SegmentRef{}, false, err
	}
	if err := tmp.Close(); err != nil {
		return SegmentRef{}, false, err
	}

	ref := SegmentRef{
		Dataset:   SegmentDatasetCommitmentBranch,
		Kind:      SegmentLatest,
		FromTxNum: fromTxNum,
		ToTxNum:   toTxNum,
		Path:      filepath.ToSlash(relPath),
		Size:      counter.n,
		Checksum:  "sha256:" + hex.EncodeToString(hash.Sum(nil)),
	}
	ref.Path = contentAddressedSnapshotPath(ref.Path, ref.Checksum)
	finalAbs := filepath.Join(dir, ref.Path)
	if err := os.MkdirAll(filepath.Dir(finalAbs), 0o755); err != nil {
		return SegmentRef{}, false, err
	}
	if err := os.Rename(tmpName, finalAbs); err != nil {
		return SegmentRef{}, false, err
	}
	return ref, true, nil
}

// OpenCommitmentBranchSegment validates the branch segment at dir/ref.Path.
// The returned handle keeps no entries in memory; Iterate opens and streams the
// verified segment file when its rows are needed.
func OpenCommitmentBranchSegment(dir string, ref SegmentRef) (*CommitmentBranchSegment, error) {
	if ref.Dataset != SegmentDatasetCommitmentBranch {
		return nil, fmt.Errorf("snapshots: segment %q dataset %q, want %q", ref.Path, ref.Dataset, SegmentDatasetCommitmentBranch)
	}
	if err := validateBranchSegmentPath(ref.Path); err != nil {
		return nil, err
	}
	path := filepath.Join(dir, ref.Path)
	if isLatestBinarySegmentPath(ref.Path) {
		if err := checkLatestBinarySegment(dir, ref); err != nil {
			return nil, err
		}
		return &CommitmentBranchSegment{ref: ref, path: path, binary: true}, nil
	}
	if err := streamCommitmentBranchSegment(path, ref, true, nil); err != nil {
		return nil, err
	}
	return &CommitmentBranchSegment{ref: ref, path: path}, nil
}

// Iterate calls fn with each (prefix, encoded) branch row in the segment.
func (s *CommitmentBranchSegment) Iterate(fn func(prefix, encoded []byte) (bool, error)) error {
	if s == nil || s.path == "" {
		return nil
	}
	if s.binary {
		return iterateLatestBinaryPrefix(s.path, s.ref, nil, func(key, encoded []byte) (bool, error) {
			prefix, err := decodeCommitmentBranchSnapshotKey(key)
			if err != nil {
				return false, err
			}
			return fn(prefix, encoded)
		})
	}
	return streamCommitmentBranchSegment(s.path, s.ref, false, fn)
}

// streamCommitmentBranchSegment parses the branch document one field and one
// entry at a time. Open calls it with verifyChecksum before handing the segment
// to a caller, so Restore cannot persist rows from a corrupt snapshot. Iterate
// then performs a second, bounded-memory pass over its immutable file.
func streamCommitmentBranchSegment(path string, ref SegmentRef, verifyChecksum bool, fn func(prefix, encoded []byte) (bool, error)) error {
	file, err := os.Open(path)
	if err != nil {
		return err
	}
	defer file.Close()
	stat, err := file.Stat()
	if err != nil {
		return err
	}
	if ref.Size != 0 && uint64(stat.Size()) != ref.Size {
		return fmt.Errorf("snapshots: branch segment %q size %d, want %d", ref.Path, stat.Size(), ref.Size)
	}

	reader := io.Reader(file)
	var checksum hash.Hash
	if verifyChecksum && ref.Checksum != "" {
		checksum = sha256.New()
		reader = io.TeeReader(reader, checksum)
	}
	decoder := json.NewDecoder(reader)
	start, err := decoder.Token()
	if err != nil {
		return fmt.Errorf("snapshots: decode branch segment %q: %w", ref.Path, err)
	}
	if delim, ok := start.(json.Delim); !ok || delim != '{' {
		return fmt.Errorf("snapshots: branch segment %q must contain an object", ref.Path)
	}

	var version uint32
	var dataset SegmentDataset
	var fromTxNum, toTxNum uint64
	for decoder.More() {
		field, err := decoder.Token()
		if err != nil {
			return fmt.Errorf("snapshots: decode branch segment %q field: %w", ref.Path, err)
		}
		name, ok := field.(string)
		if !ok {
			return fmt.Errorf("snapshots: branch segment %q contains a non-string field", ref.Path)
		}
		switch name {
		case "version":
			err = decoder.Decode(&version)
		case "dataset":
			err = decoder.Decode(&dataset)
		case "fromTxNum":
			err = decoder.Decode(&fromTxNum)
		case "toTxNum":
			err = decoder.Decode(&toTxNum)
		case "entries":
			err = streamCommitmentBranchEntries(decoder, fn)
		default:
			var ignored json.RawMessage
			err = decoder.Decode(&ignored)
		}
		if err != nil {
			return fmt.Errorf("snapshots: decode branch segment %q field %q: %w", ref.Path, name, err)
		}
	}
	end, err := decoder.Token()
	if err != nil {
		return fmt.Errorf("snapshots: decode branch segment %q end: %w", ref.Path, err)
	}
	if delim, ok := end.(json.Delim); !ok || delim != '}' {
		return fmt.Errorf("snapshots: branch segment %q has an invalid object terminator", ref.Path)
	}
	var trailing json.RawMessage
	if err := decoder.Decode(&trailing); err != io.EOF {
		if err == nil {
			return fmt.Errorf("snapshots: branch segment %q has trailing JSON data", ref.Path)
		}
		return fmt.Errorf("snapshots: decode branch segment %q trailing data: %w", ref.Path, err)
	}
	if _, err := io.Copy(io.Discard, reader); err != nil {
		return err
	}
	if checksum != nil {
		want := "sha256:" + hex.EncodeToString(checksum.Sum(nil))
		if !strings.EqualFold(ref.Checksum, want) {
			return fmt.Errorf("snapshots: branch segment %q checksum %s, want %s", ref.Path, want, ref.Checksum)
		}
	}
	if version != CommitmentBranchSegmentVersion {
		return fmt.Errorf("snapshots: unsupported branch segment version %d", version)
	}
	if dataset != SegmentDatasetCommitmentBranch {
		return fmt.Errorf("snapshots: branch segment %q dataset %q", ref.Path, dataset)
	}
	if fromTxNum != ref.FromTxNum || toTxNum != ref.ToTxNum {
		return fmt.Errorf("snapshots: branch segment %q metadata does not match manifest", ref.Path)
	}
	return nil
}

func streamCommitmentBranchEntries(decoder *json.Decoder, fn func(prefix, encoded []byte) (bool, error)) error {
	start, err := decoder.Token()
	if err != nil {
		return err
	}
	if start == nil {
		return nil
	}
	if delim, ok := start.(json.Delim); !ok || delim != '[' {
		return errors.New("entries must be an array")
	}
	callFn := fn != nil
	for decoder.More() {
		var entry commitmentBranchEntry
		if err := decoder.Decode(&entry); err != nil {
			return err
		}
		if !callFn {
			continue
		}
		cont, err := fn(entry.Prefix, entry.Encoded)
		if err != nil {
			return err
		}
		if !cont {
			// The caller asked to stop receiving rows, but the surrounding
			// document still has to be consumed so the parser remains aligned.
			callFn = false
		}
	}
	end, err := decoder.Token()
	if err != nil {
		return err
	}
	if delim, ok := end.(json.Delim); !ok || delim != ']' {
		return errors.New("entries array has an invalid terminator")
	}
	return nil
}

func (s *CommitmentBranchSegment) Restore(db ethdb.KeyValueWriter) error {
	if db == nil {
		return errors.New("snapshots: nil database")
	}
	return s.Iterate(func(prefix, encoded []byte) (bool, error) {
		return true, rawdb.WriteCommitmentBranch(db, prefix, encoded)
	})
}

// CommitmentBranchSource adapts the cold-snapshot layer to the staged engine's
// restore seam. It embeds *Manager for the snapshot root (GetCommitmentRoot) and
// the legacy node iterator (so it also satisfies the engine-agnostic
// CommitmentSnapshotSource), and serves the staged branch rows directly from a
// branch segment file. It thus satisfies both domains.CommitmentSnapshotSource
// and domains.CommitmentBranchSnapshotSource WITHOUT this package importing
// domains (which would be an import cycle via the domains test package).
type CommitmentBranchSource struct {
	*Manager
	dir       string
	branchRef SegmentRef
}

// NewCommitmentBranchSource builds a CommitmentBranchSource. mgr supplies the
// snapshot root; branchRef locates the branch segment file under dir.
func NewCommitmentBranchSource(mgr *Manager, dir string, branchRef SegmentRef) *CommitmentBranchSource {
	return &CommitmentBranchSource{Manager: mgr, dir: dir, branchRef: branchRef}
}

// IterateCommitmentBranches streams the snapshotted branch rows when txNum falls
// within the branch segment's visible range, else yields nothing. The txNum gate
// mirrors the latest-segment selection rule so a restore request for a tx range
// the snapshot does not cover declines cleanly (the staged store then falls
// through to Rebuild).
func (s *CommitmentBranchSource) IterateCommitmentBranches(txNum uint64, fn func(prefix, encoded []byte) (bool, error)) error {
	if s == nil || s.branchRef.Path == "" {
		return nil
	}
	if txNum < s.branchRef.FromTxNum || txNum > s.branchRef.ToTxNum {
		return nil
	}
	seg, err := OpenCommitmentBranchSegment(s.dir, s.branchRef)
	if err != nil {
		return err
	}
	return seg.Iterate(fn)
}

// buildCommitmentBranchLatest is the registry LatestSnapshotBuilder adapter for
// the CommitmentBranch family. It returns no ref (publishes nothing) when the
// branch keyspace is empty, mirroring Runner.onePass's "no rows, return early"
// without first walking a large branch keyspace just to detect that it exists.
func buildCommitmentBranchLatest(db AggregatorDB, dir string, _ kvdomains.KVDomain, fromTxNum, toTxNum uint64, relPath string) ([]SegmentRef, error) {
	return buildCommitmentBranchLatestContext(context.Background(), db, dir, 0, fromTxNum, toTxNum, relPath)
}

func buildCommitmentBranchLatestContext(ctx context.Context, db AggregatorDB, dir string, _ kvdomains.KVDomain, fromTxNum, toTxNum uint64, relPath string) ([]SegmentRef, error) {
	if err := contextError(ctx); err != nil {
		return nil, err
	}
	base, based, err := rawdb.ReadCommitmentBranchBase(db)
	if err != nil {
		return nil, err
	}
	rotation, rotating, err := rawdb.ReadCommitmentBranchRotation(db)
	if err != nil {
		return nil, err
	}
	if based && rotating {
		if rotation.Generation != base.Generation+1 || rotation.Generation == 0 {
			return nil, fmt.Errorf("snapshots: rotation generation %d does not follow base %d", rotation.Generation, base.Generation)
		}
		published, err := commitmentBranchRotationAlreadyPublished(dir, rotation)
		if err != nil {
			return nil, err
		}
		if published {
			// Completion can be deferred until the boundary is solidified or the
			// process can restart after manifest publication. Retain the already
			// merged family instead of requiring its retired input base again.
			return nil, nil
		}
		return buildMergedCommitmentBranchLatestContext(ctx, db, dir, base, fromTxNum, toTxNum, relPath)
	}
	if based {
		// With no active rotation the current immutable baseline remains the
		// authoritative branch family. Aggregator.Integrate retains its refs.
		return nil, nil
	}
	ref, accessor, btree, hasRows, err := writeCommitmentBranchBinarySegmentFilesFromDBContext(ctx, db, dir, relPath, fromTxNum, toTxNum, true)
	if err != nil {
		return nil, err
	}
	if !hasRows {
		return nil, nil
	}
	return []SegmentRef{ref, accessor, btree}, nil
}

func commitmentBranchRotationAlreadyPublished(dir string, rotation rawdb.CommitmentBranchRotation) (bool, error) {
	mgr, err := OpenManager(dir)
	if err != nil {
		return false, fmt.Errorf("snapshots: open commitment branch rotation manifest: %w", err)
	}
	ref, ok := commitmentBranchRefAtOrBefore(mgr.Manifest(), rotation.SnapshotTxNum)
	if !ok || ref.ToTxNum != rotation.SnapshotTxNum {
		return false, nil
	}
	root, ok, err := mgr.GetCommitmentRoot(rotation.SnapshotTxNum)
	if err != nil {
		return false, err
	}
	if !ok || root != rotation.Root {
		return false, fmt.Errorf("snapshots: published commitment branch rotation root mismatch at tx %d", rotation.SnapshotTxNum)
	}
	return true, nil
}

func buildMergedCommitmentBranchLatest(db AggregatorDB, dir string, base rawdb.CommitmentBranchBase, fromTxNum, toTxNum uint64, relPath string) ([]SegmentRef, error) {
	return buildMergedCommitmentBranchLatestContext(context.Background(), db, dir, base, fromTxNum, toTxNum, relPath)
}

func buildMergedCommitmentBranchLatestContext(ctx context.Context, db AggregatorDB, dir string, base rawdb.CommitmentBranchBase, fromTxNum, toTxNum uint64, relPath string) ([]SegmentRef, error) {
	if err := contextError(ctx); err != nil {
		return nil, err
	}
	mgr, err := OpenManager(dir)
	if err != nil {
		return nil, fmt.Errorf("snapshots: open prior commitment branch manifest: %w", err)
	}
	ref, ok := commitmentBranchRefAtOrBefore(mgr.Manifest(), base.SnapshotTxNum)
	if !ok || ref.ToTxNum != base.SnapshotTxNum {
		return nil, fmt.Errorf("snapshots: prior commitment branch base missing at tx %d", base.SnapshotTxNum)
	}
	cold, err := openCommitmentBranchBinaryIterator(dir, ref)
	if err != nil {
		return nil, err
	}
	defer cold.Close()
	deltaSpace, err := rawdb.NewCommitmentBranchDeltaKeyspace(base.Generation)
	if err != nil {
		return nil, err
	}
	delta := deltaSpace.NewIterator(db)
	defer delta.Release()

	segment, accessor, btree, hasRows, err := writeCommitmentBranchBinarySegmentFilesFromIteratorContext(
		ctx, dir, relPath, fromTxNum, toTxNum, true,
		func(yield func(prefix, encoded []byte) error) error {
			coldOK := cold.Next()
			deltaOK := delta.Next()
			for coldOK || deltaOK {
				if err := contextError(ctx); err != nil {
					return err
				}
				switch {
				case !deltaOK:
					if err := yield(cold.Key(), cold.Value()); err != nil {
						return err
					}
					coldOK = cold.Next()
				case !coldOK:
					if len(delta.Value()) != 0 {
						if err := yield(delta.Key(), delta.Value()); err != nil {
							return err
						}
					}
					deltaOK = delta.Next()
				default:
					cmp := bytes.Compare(cold.Key(), delta.Key())
					switch {
					case cmp < 0:
						if err := yield(cold.Key(), cold.Value()); err != nil {
							return err
						}
						coldOK = cold.Next()
					case cmp > 0:
						if len(delta.Value()) != 0 {
							if err := yield(delta.Key(), delta.Value()); err != nil {
								return err
							}
						}
						deltaOK = delta.Next()
					default:
						// The frozen delta is newer than the immutable base. Its
						// empty value is a tombstone and therefore yields no row.
						if len(delta.Value()) != 0 {
							if err := yield(delta.Key(), delta.Value()); err != nil {
								return err
							}
						}
						coldOK = cold.Next()
						deltaOK = delta.Next()
					}
				}
			}
			if err := cold.Error(); err != nil {
				return err
			}
			return delta.Error()
		},
	)
	if err != nil {
		return nil, err
	}
	if !hasRows {
		return nil, errors.New("snapshots: merged commitment branch baseline is empty")
	}
	return []SegmentRef{segment, accessor, btree}, nil
}

// checkCommitmentBranchSegment validates a published branch segment without
// materializing its branch rows — the registry CheckLatest hook for the family.
func checkCommitmentBranchSegment(dir string, ref SegmentRef) error {
	_, err := OpenCommitmentBranchSegment(dir, ref)
	return err
}

func validateBranchSegmentPath(path string) error {
	if path == "" || filepath.IsAbs(path) || filepath.Clean(path) != path || path == "." || hasParentDir(path) {
		return fmt.Errorf("snapshots: invalid relative branch segment path %q", path)
	}
	return nil
}
