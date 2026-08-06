package etl

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math/bits"
	"os"
	"path/filepath"
	"slices"
	"sync"

	"github.com/ethereum/go-ethereum/ethdb"
)

const (
	defaultBufferLimit = 64 * 1024 * 1024
	defaultBatchSize   = ethdb.IdealBatchSize
	runFileMagic       = "gtronetl1"
	runEntryHeaderSize = 17
	runIOBufferSize    = 1 << 20
	// Keep at most one default-buffer-sized entry array in each pool item.
	// Larger one-off collectors are released to the garbage collector rather
	// than pinning an unexpectedly large backing array for future work.
	collectorRowsPoolMaxCapacity  = 1 << 20
	collectorOrderPoolMaxCapacity = 1 << 20
)

var (
	emptyRunReader            = bytes.NewReader(nil)
	ErrCollectorClosed        = errors.New("etl: collector closed")
	ErrCollectorLoaded        = errors.New("etl: collector already loaded")
	ErrLoadInterrupted        = errors.New("etl: load interrupted")
	collectorRowsPool         = sync.Pool{New: func() any { return new([]entry) }}
	collectorOrderPool        = sync.Pool{New: func() any { return new([]uint32) }}
	collectorOrderScratchPool = sync.Pool{New: func() any { return new([]uint32) }}
	collectorRadixRangePool   = sync.Pool{New: func() any { return new([]entryOrderRange) }}
	collectorRunReaderPool    = sync.Pool{New: func() any { return bufio.NewReaderSize(emptyRunReader, runIOBufferSize) }}
	collectorRunWriterPool    = sync.Pool{New: func() any { return bufio.NewWriterSize(io.Discard, runIOBufferSize) }}
)

const (
	radixEntryOrderMin                 = 128
	collectorRadixRangePoolMaxCapacity = 1 << 16
)

// Options configures a Collector. TempDir is a parent directory; Collector
// creates and owns a private child directory below it.
type Options struct {
	TempDir     string
	BufferLimit int
	BatchSize   int
}

// Stats describes the work performed by a collector load.
type Stats struct {
	Collected      uint64
	InputPuts      uint64
	InputDeletes   uint64
	InputBytes     uint64
	SpilledRuns    int
	Applied        uint64
	AppliedPuts    uint64
	AppliedDeletes uint64
	BatchWrites    uint64
}

// Collector gathers unordered key/value operations, sorts them by key, collapses
// duplicate keys by latest input order, and loads the final stream into a DB.
// It is intended for large snapshot restore/backfill/index builds where
// key-ordered writes reduce Pebble write amplification.
type Collector struct {
	opts        Options
	dir         string
	rows        []entry
	rowBuffer   *[]entry
	arena       *collectorByteArena
	bufferBytes int
	runFiles    []string
	seq         uint64
	stats       Stats
	loaded      bool
	closed      bool
}

type opKind uint8

const (
	opPut opKind = iota + 1
	opDelete
)

type entry struct {
	key   []byte
	value []byte
	op    opKind
	seq   uint64
}

// NewCollector creates a collector with a private temporary work directory.
func NewCollector(opts Options) (*Collector, error) {
	if opts.BufferLimit <= 0 {
		opts.BufferLimit = defaultBufferLimit
	}
	if opts.BatchSize <= 0 {
		opts.BatchSize = defaultBatchSize
	}
	parent := opts.TempDir
	if parent == "" {
		parent = os.TempDir()
	}
	if err := os.MkdirAll(parent, 0o755); err != nil {
		return nil, err
	}
	dir, err := os.MkdirTemp(parent, "gtron-etl-*")
	if err != nil {
		return nil, err
	}
	return &Collector{opts: opts, dir: dir}, nil
}

// TempDir returns the private work directory. It is removed by Close.
func (c *Collector) TempDir() string {
	if c == nil {
		return ""
	}
	return c.dir
}

// Put records a key/value write. The key and value are copied.
func (c *Collector) Put(key, value []byte) error {
	return c.append(entry{
		key:   cloneBytes(key),
		value: cloneBytes(value),
		op:    opPut,
	})
}

// PutOwned records a key/value write by taking ownership of both slices. The
// caller must not modify either slice after a successful call. It is intended
// for collation code that has just allocated its final derived key/value and
// would otherwise pay an immediate redundant copy into the bounded collector.
func (c *Collector) PutOwned(key, value []byte) error {
	return c.append(entry{
		key:   key,
		value: value,
		op:    opPut,
	})
}

// PutEncoded reserves stable collector-owned storage and invokes encode to
// populate the key and value directly. The callback-scoped slices must not be
// retained or resized. This avoids one heap object per derived field while
// preserving PutOwned for callers which already own their final byte slices.
func (c *Collector) PutEncoded(keyLen, valueLen int, encode func(key, value []byte)) error {
	if encode == nil {
		return errors.New("etl: nil PutEncoded callback")
	}
	if err := c.validateAppend(); err != nil {
		return err
	}
	if keyLen < 0 || valueLen < 0 || keyLen > int(^uint(0)>>1)-valueLen {
		return fmt.Errorf("etl: invalid encoded key/value sizes %d/%d", keyLen, valueLen)
	}
	storage := c.allocEncoded(keyLen + valueLen)
	key := storage[:keyLen:keyLen]
	value := storage[keyLen : keyLen+valueLen : keyLen+valueLen]
	encode(key, value)
	return c.append(entry{key: key, value: value, op: opPut})
}

// PutEncodedKeyAsValue is PutEncoded for fixed records whose sort key and
// output value are byte-identical. A single arena view is stored in both entry
// fields, retaining the existing PutOwned(key, key) memory behavior.
func (c *Collector) PutEncodedKeyAsValue(size int, encode func(key []byte)) error {
	if encode == nil {
		return errors.New("etl: nil PutEncodedKeyAsValue callback")
	}
	if err := c.validateAppend(); err != nil {
		return err
	}
	if size < 0 {
		return fmt.Errorf("etl: invalid encoded key/value size %d", size)
	}
	key := c.allocEncoded(size)
	encode(key)
	return c.append(entry{key: key, value: key, op: opPut})
}

// Delete records a key deletion. The key is copied.
func (c *Collector) Delete(key []byte) error {
	return c.append(entry{
		key: cloneBytes(key),
		op:  opDelete,
	})
}

// Load writes the collected final state to writer in key order. Load can be
// called once; call Close afterwards to remove temporary run files.
func (c *Collector) Load(writer ethdb.KeyValueWriter) (Stats, error) {
	return c.LoadInterruptible(writer, nil)
}

// LoadInterruptible is Load with a cooperative stop check. A stopped load
// keeps the collector retryable and never marks it loaded. The target may have
// received earlier completed batches, matching the collector's existing
// crash-recovery contract; callers publish their stage watermark only after a
// successful return and rerun idempotently otherwise.
func (c *Collector) LoadInterruptible(writer ethdb.KeyValueWriter, interrupted func() bool) (Stats, error) {
	if writer == nil {
		return c.stats, errors.New("etl: nil writer")
	}
	if c.closed {
		return c.stats, ErrCollectorClosed
	}
	if c.loaded {
		return c.stats, ErrCollectorLoaded
	}
	if interrupted != nil && interrupted() {
		return c.stats, ErrLoadInterrupted
	}
	inMemory := len(c.runFiles) == 0
	if !inMemory {
		if err := c.spillBuffer(); err != nil {
			return c.stats, err
		}
	}
	if interrupted != nil && interrupted() {
		return c.stats, ErrLoadInterrupted
	}
	loadStartStats := c.stats
	applier := newApplier(writer, c.opts.BatchSize)
	defer applier.close()

	var loadErr error
	if inMemory {
		loadErr = c.loadRows(applier, interrupted)
	} else {
		loadErr = c.mergeRuns(applier, interrupted)
	}
	if loadErr != nil {
		if errors.Is(loadErr, ErrLoadInterrupted) {
			c.stats = loadStartStats
		}
		return c.stats, loadErr
	}
	if interrupted != nil && interrupted() {
		c.stats = loadStartStats
		return c.stats, ErrLoadInterrupted
	}
	if err := applier.flush(); err != nil {
		return c.stats, err
	}
	c.stats.BatchWrites += applier.batchWrites
	c.releaseRows()
	c.loaded = true
	return c.stats, nil
}

// Close removes any temporary run files and rejects future collector use.
func (c *Collector) Close() error {
	if c == nil || c.closed {
		return nil
	}
	c.closed = true
	c.releaseRows()
	c.runFiles = nil
	if c.dir == "" {
		return nil
	}
	return os.RemoveAll(c.dir)
}

func (c *Collector) append(e entry) error {
	if err := c.validateAppend(); err != nil {
		return err
	}
	c.seq++
	e.seq = c.seq
	c.ensureRows()
	c.rows = append(c.rows, e)
	size := len(e.key) + len(e.value) + runEntryHeaderSize
	c.bufferBytes += size
	c.stats.Collected++
	c.stats.InputBytes += uint64(size)
	switch e.op {
	case opPut:
		c.stats.InputPuts++
	case opDelete:
		c.stats.InputDeletes++
	default:
		return fmt.Errorf("etl: unknown operation %d", e.op)
	}
	if c.bufferBytes >= c.opts.BufferLimit {
		return c.spillBuffer()
	}
	return nil
}

func (c *Collector) spillBuffer() error {
	if len(c.rows) == 0 {
		return nil
	}
	order, err := sortedEntryOrder(c.rows)
	if err != nil {
		return err
	}
	defer releaseEntryOrder(&order)
	final := filepath.Join(c.dir, fmt.Sprintf("run-%06d.dat", len(c.runFiles)))
	tmp, err := os.CreateTemp(c.dir, ".run-*.tmp")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	ok := false
	defer func() {
		if !ok {
			_ = os.Remove(tmpName)
		}
	}()

	w := collectorRunWriterPool.Get().(*bufio.Writer)
	w.Reset(tmp)
	defer func() {
		w.Reset(io.Discard)
		collectorRunWriterPool.Put(w)
	}()
	if _, err := w.WriteString(runFileMagic); err != nil {
		_ = tmp.Close()
		return err
	}
	for _, row := range *order {
		e := c.rows[row]
		if err := writeRunEntry(w, e); err != nil {
			_ = tmp.Close()
			return err
		}
	}
	if err := w.Flush(); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmpName, final); err != nil {
		return err
	}
	ok = true
	c.runFiles = append(c.runFiles, final)
	clear(c.rows)
	c.rows = c.rows[:0]
	if c.arena != nil {
		c.arena.reset()
	}
	c.bufferBytes = 0
	c.stats.SpilledRuns++
	return nil
}

// ensureRows lazily borrows the sortable entry metadata buffer. Collectors
// which are created but never used allocate no row storage.
func (c *Collector) ensureRows() {
	if c.rowBuffer != nil {
		return
	}
	c.rowBuffer = collectorRowsPool.Get().(*[]entry)
	c.rows = (*c.rowBuffer)[:0]
}

func (c *Collector) validateAppend() error {
	if c.closed {
		return ErrCollectorClosed
	}
	if c.loaded {
		return ErrCollectorLoaded
	}
	if c.seq == ^uint64(0) {
		return errors.New("etl: sequence overflow")
	}
	return nil
}

func (c *Collector) allocEncoded(size int) []byte {
	if c.arena == nil {
		c.arena = collectorArenaPool.Get().(*collectorByteArena)
		c.arena.reset()
	}
	storage := c.arena.alloc(size)
	clear(storage)
	return storage
}

// releaseRows drops key/value references before returning reasonably-sized
// metadata storage to the process-wide pool. The slice capacity is retained
// across collectors, mirroring Erigon's sortable-buffer lifecycle.
func (c *Collector) releaseRows() {
	if c.rowBuffer == nil {
		c.rows = nil
		c.bufferBytes = 0
		c.releaseArena()
		return
	}
	clear(c.rows)
	buffer := c.rowBuffer
	*buffer = c.rows[:0]
	c.rows = nil
	c.rowBuffer = nil
	c.bufferBytes = 0
	if cap(*buffer) <= collectorRowsPoolMaxCapacity {
		collectorRowsPool.Put(buffer)
	}
	c.releaseArena()
}

func (c *Collector) releaseArena() {
	if c.arena == nil {
		return
	}
	arena := c.arena
	c.arena = nil
	arena.reset()
	if arena.capacity <= collectorArenaPoolMaxCapacity {
		collectorArenaPool.Put(arena)
	}
}

func (c *Collector) mergeRuns(applier *applier, interrupted func() bool) error {
	readers := make([]*runReader, 0, len(c.runFiles))
	for i, path := range c.runFiles {
		rr, err := openRunReader(path, i)
		if err != nil {
			closeRunReaders(readers)
			return err
		}
		readers = append(readers, rr)
	}
	defer closeRunReaders(readers)

	for _, rr := range readers {
		if err := rr.next(); err != nil {
			return err
		}
	}
	// Advancing one run changes one tournament leaf and costs one comparison
	// per level. The former heap pop plus push traversed the same levels twice.
	tournament := newRunTournament(readers)

	var (
		haveGroup   bool
		groupKey    []byte
		winnerValue []byte
		winnerOp    opKind
		winnerSeq   uint64
	)
	applyGroup := func() error {
		if !haveGroup {
			return nil
		}
		if winnerOp == opDelete {
			if err := applier.delete(groupKey); err != nil {
				return err
			}
			c.stats.AppliedDeletes++
		} else {
			if err := applier.put(groupKey, winnerValue); err != nil {
				return err
			}
			c.stats.AppliedPuts++
		}
		c.stats.Applied++
		return nil
	}
	setWinner := func(e entry) {
		// Readers reuse their key/value buffers on next(), so retain only the
		// current duplicate group's eventual output in two stable buffers.
		groupKey = append(groupKey[:0], e.key...)
		winnerValue = append(winnerValue[:0], e.value...)
		winnerOp = e.op
		winnerSeq = e.seq
	}
	updateWinner := func(e entry) {
		winnerValue = append(winnerValue[:0], e.value...)
		winnerOp = e.op
		winnerSeq = e.seq
	}

	var merged uint64
	for tournament.winner() != nil {
		if merged&1023 == 0 && interrupted != nil && interrupted() {
			return ErrLoadInterrupted
		}
		merged++
		rr := tournament.winner()
		e := rr.current
		if !haveGroup {
			haveGroup = true
			setWinner(e)
		} else if bytes.Equal(groupKey, e.key) {
			if e.seq > winnerSeq {
				updateWinner(e)
			}
		} else {
			if err := applyGroup(); err != nil {
				return err
			}
			setWinner(e)
		}
		if err := rr.next(); err != nil {
			return err
		}
		tournament.update(rr.index)
	}
	return applyGroup()
}

// loadRows is the Erigon-style final-buffer fast path: when collection never
// exceeded its memory bound, sort and apply that buffer directly instead of
// writing a one-run temporary file and immediately reading it back. Entries
// remain owned by the collector until Load succeeds so an interrupted attempt
// can be retried with the same idempotent contract as the disk merge path.
func (c *Collector) loadRows(applier *applier, interrupted func() bool) error {
	if len(c.rows) == 0 {
		return nil
	}
	order, err := sortedEntryOrder(c.rows)
	if err != nil {
		return err
	}
	defer releaseEntryOrder(&order)
	var groups uint64
	for start := 0; start < len(*order); {
		if groups&1023 == 0 && interrupted != nil && interrupted() {
			return ErrLoadInterrupted
		}
		end := start + 1
		for end < len(*order) && bytes.Equal(c.rows[(*order)[start]].key, c.rows[(*order)[end]].key) {
			end++
		}
		winner := c.rows[(*order)[end-1]]
		if winner.op == opDelete {
			if err := applier.delete(winner.key); err != nil {
				return err
			}
			c.stats.AppliedDeletes++
		} else {
			if err := applier.put(winner.key, winner.value); err != nil {
				return err
			}
			c.stats.AppliedPuts++
		}
		c.stats.Applied++
		groups++
		start = end
	}
	return nil
}

// sortedEntryOrder keeps the 64-byte entry metadata stationary and moves
// compact row numbers. Large buffers use a stable MSD radix pass over the key
// bytes, avoiding repeated bytes.Compare calls on the long shared prefixes
// used by snapshot accessors. Equal-key rows retain sequence ordering so the
// collector's latest-operation collapse contract is unchanged.
func sortedEntryOrder(entries []entry) (*[]uint32, error) {
	if uint64(len(entries)) > uint64(^uint32(0)) {
		return nil, fmt.Errorf("etl: entry count %d exceeds compact sort order", len(entries))
	}
	order := collectorOrderPool.Get().(*[]uint32)
	if cap(*order) < len(entries) {
		*order = make([]uint32, len(entries))
	} else {
		*order = (*order)[:len(entries)]
	}
	for i := range entries {
		(*order)[i] = uint32(i)
	}
	if len(entries) < radixEntryOrderMin {
		sortEntryOrderComparison(*order, entries)
		return order, nil
	}
	scratch := collectorOrderScratchPool.Get().(*[]uint32)
	if cap(*scratch) < len(entries) {
		*scratch = make([]uint32, len(entries))
	} else {
		*scratch = (*scratch)[:len(entries)]
	}
	radixSortEntryOrder(*order, *scratch, entries)
	releaseEntryOrderBuffer(scratch, collectorOrderScratchPool.Put)
	return order, nil
}

func sortEntryOrderComparison(order []uint32, entries []entry) {
	slices.SortFunc(order, func(left, right uint32) int {
		a, b := &entries[left], &entries[right]
		if cmp := bytes.Compare(a.key, b.key); cmp != 0 {
			return cmp
		}
		if a.seq < b.seq {
			return -1
		}
		if a.seq > b.seq {
			return 1
		}
		return 0
	})
}

type entryOrderRange struct {
	lo    int
	hi    int
	depth int
}

// radixSortEntryOrder performs a stable lexicographic MSD radix sort. Bucket
// zero represents end-of-key and therefore sorts before every byte value,
// preserving bytes.Compare's prefix ordering. Small partitions fall back to
// pdqsort; all-equal terminal partitions need only order their sequence.
func radixSortEntryOrder(order, scratch []uint32, entries []entry) {
	stackBuffer := collectorRadixRangePool.Get().(*[]entryOrderRange)
	stack := append((*stackBuffer)[:0], entryOrderRange{hi: len(order)})
	defer func() {
		*stackBuffer = stack[:0]
		if cap(*stackBuffer) <= collectorRadixRangePoolMaxCapacity {
			collectorRadixRangePool.Put(stackBuffer)
		}
	}()
	for len(stack) > 0 {
		last := len(stack) - 1
		current := stack[last]
		stack = stack[:last]
		for {
			length := current.hi - current.lo
			if length < 2 {
				break
			}
			if length < radixEntryOrderMin {
				sortEntryOrderComparison(order[current.lo:current.hi], entries)
				break
			}
			var counts [257]int
			nonEmpty := 0
			onlyBucket := 0
			for _, row := range order[current.lo:current.hi] {
				key := entries[row].key
				bucket := 0
				if current.depth < len(key) {
					bucket = int(key[current.depth]) + 1
				}
				if counts[bucket] == 0 {
					nonEmpty++
					onlyBucket = bucket
				}
				counts[bucket]++
			}
			if nonEmpty == 1 {
				if onlyBucket == 0 {
					slices.SortFunc(order[current.lo:current.hi], func(left, right uint32) int {
						a, b := entries[left].seq, entries[right].seq
						if a < b {
							return -1
						}
						if a > b {
							return 1
						}
						return 0
					})
					break
				}
				current.depth = sharedEntryKeyPrefixDepth(order[current.lo:current.hi], entries, current.depth+1)
				continue
			}

			var starts [257]int
			next := current.lo
			for bucket, count := range counts {
				starts[bucket] = next
				next += count
			}
			positions := starts
			for _, row := range order[current.lo:current.hi] {
				key := entries[row].key
				bucket := 0
				if current.depth < len(key) {
					bucket = int(key[current.depth]) + 1
				}
				scratch[positions[bucket]] = row
				positions[bucket]++
			}
			copy(order[current.lo:current.hi], scratch[current.lo:current.hi])

			if counts[0] > 1 {
				lo, hi := starts[0], starts[0]+counts[0]
				slices.SortFunc(order[lo:hi], func(left, right uint32) int {
					a, b := entries[left].seq, entries[right].seq
					if a < b {
						return -1
					}
					if a > b {
						return 1
					}
					return 0
				})
			}
			for bucket := 256; bucket >= 1; bucket-- {
				if counts[bucket] > 1 {
					stack = append(stack, entryOrderRange{
						lo:    starts[bucket],
						hi:    starts[bucket] + counts[bucket],
						depth: current.depth + 1,
					})
				}
			}
			break
		}
	}
}

// sharedEntryKeyPrefixDepth skips a range's common continuation after the
// radix pass has proved that every key contains the same byte at start-1. A
// last/middle-row probe keeps the common one-byte case O(1); only a candidate
// longer prefix triggers the full range scan. Eight-byte comparisons avoid
// revisiting long encoded accessor prefixes one byte and one full pass at a
// time.
func sharedEntryKeyPrefixDepth(order []uint32, entries []entry, start int) int {
	if len(order) < 2 {
		return start
	}
	reference := entries[order[0]].key
	depth := commonKeyPrefixDepth(reference, entries[order[len(order)-1]].key, start, len(reference))
	if depth == start {
		return start
	}
	depth = commonKeyPrefixDepth(reference, entries[order[len(order)/2]].key, start, depth)
	if depth == start {
		return start
	}
	for _, row := range order[1:] {
		depth = commonKeyPrefixDepth(reference, entries[row].key, start, depth)
		if depth == start {
			return start
		}
	}
	return depth
}

func commonKeyPrefixDepth(left, right []byte, start, limit int) int {
	limit = min(limit, len(right))
	index := start
	for index+8 <= limit {
		difference := binary.LittleEndian.Uint64(left[index:index+8]) ^ binary.LittleEndian.Uint64(right[index:index+8])
		if difference != 0 {
			return index + bits.TrailingZeros64(difference)/8
		}
		index += 8
	}
	for index < limit && left[index] == right[index] {
		index++
	}
	return index
}

func releaseEntryOrder(order **[]uint32) {
	if order == nil || *order == nil {
		return
	}
	buffer := *order
	*order = nil
	releaseEntryOrderBuffer(buffer, collectorOrderPool.Put)
}

func releaseEntryOrderBuffer(buffer *[]uint32, put func(any)) {
	*buffer = (*buffer)[:0]
	if cap(*buffer) <= collectorOrderPoolMaxCapacity {
		put(buffer)
	}
}

func writeRunEntry(w io.Writer, e entry) error {
	if uint64(len(e.key)) > uint64(^uint32(0)) || uint64(len(e.value)) > uint64(^uint32(0)) {
		return errors.New("etl: key or value too large")
	}
	var header [runEntryHeaderSize]byte
	header[0] = byte(e.op)
	binary.BigEndian.PutUint64(header[1:9], e.seq)
	binary.BigEndian.PutUint32(header[9:13], uint32(len(e.key)))
	binary.BigEndian.PutUint32(header[13:17], uint32(len(e.value)))
	if _, err := w.Write(header[:]); err != nil {
		return err
	}
	if _, err := w.Write(e.key); err != nil {
		return err
	}
	_, err := w.Write(e.value)
	return err
}

func readRunEntryReuse(r *bufio.Reader, keyBuffer, valueBuffer *[]byte) (entry, error) {
	header, err := r.Peek(runEntryHeaderSize)
	if err != nil {
		if errors.Is(err, io.EOF) && len(header) == 0 {
			return entry{}, io.EOF
		}
		if errors.Is(err, io.EOF) {
			return entry{}, io.ErrUnexpectedEOF
		}
		return entry{}, err
	}
	e := entry{
		op:  opKind(header[0]),
		seq: binary.BigEndian.Uint64(header[1:9]),
	}
	if e.op != opPut && e.op != opDelete {
		return entry{}, fmt.Errorf("etl: invalid operation %d", e.op)
	}
	keyLen := binary.BigEndian.Uint32(header[9:13])
	valueLen := binary.BigEndian.Uint32(header[13:17])
	if _, err := r.Discard(runEntryHeaderSize); err != nil {
		return entry{}, err
	}
	*keyBuffer = resizeRunEntryBuffer(*keyBuffer, int(keyLen))
	e.key = *keyBuffer
	if _, err := io.ReadFull(r, e.key); err != nil {
		return entry{}, err
	}
	*valueBuffer = resizeRunEntryBuffer(*valueBuffer, int(valueLen))
	e.value = *valueBuffer
	if _, err := io.ReadFull(r, e.value); err != nil {
		return entry{}, err
	}
	return e, nil
}

func resizeRunEntryBuffer(buffer []byte, size int) []byte {
	if cap(buffer) < size {
		return make([]byte, size)
	}
	return buffer[:size]
}

type runReader struct {
	index    int
	file     *os.File
	reader   *bufio.Reader
	current  entry
	prefix   [2]uint64
	keyBuf   []byte
	valueBuf []byte
	has      bool
}

func openRunReader(path string, index int) (*runReader, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	rr := &runReader{
		index: index,
		file:  file,
	}
	rr.reader = collectorRunReaderPool.Get().(*bufio.Reader)
	rr.reader.Reset(file)
	magic := make([]byte, len(runFileMagic))
	if _, err := io.ReadFull(rr.reader, magic); err != nil {
		releaseRunReader(rr.reader)
		_ = file.Close()
		return nil, err
	}
	if string(magic) != runFileMagic {
		releaseRunReader(rr.reader)
		_ = file.Close()
		return nil, fmt.Errorf("etl: invalid run file %q", path)
	}
	return rr, nil
}

func (r *runReader) next() error {
	e, err := readRunEntryReuse(r.reader, &r.keyBuf, &r.valueBuf)
	if errors.Is(err, io.EOF) {
		r.has = false
		return nil
	}
	if err != nil {
		return err
	}
	r.current = e
	r.prefix = entryKeyPrefix(e.key)
	r.has = true
	return nil
}

func closeRunReaders(readers []*runReader) {
	for _, rr := range readers {
		if rr == nil {
			continue
		}
		if rr.reader != nil {
			releaseRunReader(rr.reader)
			rr.reader = nil
		}
		if rr.file != nil {
			_ = rr.file.Close()
			rr.file = nil
		}
	}
}

func releaseRunReader(reader *bufio.Reader) {
	reader.Reset(emptyRunReader)
	collectorRunReaderPool.Put(reader)
}

// runTournament is a compact winner tree over sorted spill runs. Cached
// big-endian key prefixes resolve the common case without calling bytes.Compare;
// the full comparator remains authoritative for shared or short prefixes.
type runTournament struct {
	readers  []*runReader
	leafBase int
	tree     []int
}

func newRunTournament(readers []*runReader) *runTournament {
	leafBase := 1
	for leafBase < len(readers) {
		leafBase <<= 1
	}
	t := &runTournament{
		readers:  readers,
		leafBase: leafBase,
		tree:     make([]int, leafBase*2),
	}
	for i := range t.tree {
		t.tree[i] = -1
	}
	for i, rr := range readers {
		if rr.has {
			t.tree[leafBase+i] = i
		}
	}
	for node := leafBase - 1; node > 0; node-- {
		t.tree[node] = t.pick(t.tree[node*2], t.tree[node*2+1])
	}
	return t
}

func (t *runTournament) winner() *runReader {
	if t == nil || len(t.tree) < 2 || t.tree[1] < 0 {
		return nil
	}
	return t.readers[t.tree[1]]
}

func (t *runTournament) update(readerIndex int) {
	leaf := t.leafBase + readerIndex
	if t.readers[readerIndex].has {
		t.tree[leaf] = readerIndex
	} else {
		t.tree[leaf] = -1
	}
	for node := leaf >> 1; node > 0; node >>= 1 {
		t.tree[node] = t.pick(t.tree[node*2], t.tree[node*2+1])
	}
}

func (t *runTournament) pick(leftIndex, rightIndex int) int {
	if leftIndex < 0 {
		return rightIndex
	}
	if rightIndex < 0 {
		return leftIndex
	}
	left, right := t.readers[leftIndex], t.readers[rightIndex]
	if left.prefix[0] != right.prefix[0] {
		if left.prefix[0] < right.prefix[0] {
			return leftIndex
		}
		return rightIndex
	}
	if left.prefix[1] != right.prefix[1] {
		if left.prefix[1] < right.prefix[1] {
			return leftIndex
		}
		return rightIndex
	}
	if cmp := bytes.Compare(left.current.key, right.current.key); cmp != 0 {
		if cmp < 0 {
			return leftIndex
		}
		return rightIndex
	}
	if left.current.seq != right.current.seq {
		if left.current.seq < right.current.seq {
			return leftIndex
		}
		return rightIndex
	}
	if left.index < right.index {
		return leftIndex
	}
	return rightIndex
}

func entryKeyPrefix(key []byte) (prefix [2]uint64) {
	limit := min(len(key), 16)
	for i := 0; i < limit; i++ {
		prefix[i>>3] |= uint64(key[i]) << (56 - uint(i&7)*8)
	}
	return prefix
}

type applier struct {
	writer      ethdb.KeyValueWriter
	batch       ethdb.Batch
	batchSize   int
	pendingSize int
	pendingOps  int
	batchWrites uint64
}

func newApplier(writer ethdb.KeyValueWriter, batchSize int) *applier {
	a := &applier{writer: writer, batchSize: batchSize}
	if a.batchSize <= 0 {
		a.batchSize = defaultBatchSize
	}
	if batcher, ok := writer.(ethdb.Batcher); ok {
		a.batch = batcher.NewBatchWithSize(a.batchSize)
	}
	return a
}

func (a *applier) put(key, value []byte) error {
	if a.batch == nil {
		return a.writer.Put(key, value)
	}
	if err := a.batch.Put(key, value); err != nil {
		return err
	}
	return a.account(len(key) + len(value))
}

func (a *applier) delete(key []byte) error {
	if a.batch == nil {
		return a.writer.Delete(key)
	}
	if err := a.batch.Delete(key); err != nil {
		return err
	}
	return a.account(len(key))
}

func (a *applier) account(size int) error {
	a.pendingOps++
	a.pendingSize += size
	if a.pendingSize >= a.batchSize {
		return a.flush()
	}
	return nil
}

func (a *applier) flush() error {
	if a.batch == nil || a.pendingOps == 0 {
		return nil
	}
	if err := a.batch.Write(); err != nil {
		return err
	}
	a.batchWrites++
	a.batch.Reset()
	a.pendingOps = 0
	a.pendingSize = 0
	return nil
}

func (a *applier) close() {
	if a.batch != nil {
		a.batch.Close()
	}
}

func cloneBytes(b []byte) []byte {
	if b == nil {
		return nil
	}
	out := make([]byte, len(b))
	copy(out, b)
	return out
}
