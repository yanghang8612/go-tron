package etl

import (
	"bufio"
	"bytes"
	"container/heap"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"

	"github.com/ethereum/go-ethereum/ethdb"
)

const (
	defaultBufferLimit = 64 * 1024 * 1024
	defaultBatchSize   = ethdb.IdealBatchSize
	runFileMagic       = "gtronetl1"
	runEntryHeaderSize = 17
)

var (
	ErrCollectorClosed = errors.New("etl: collector closed")
	ErrCollectorLoaded = errors.New("etl: collector already loaded")
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
	if writer == nil {
		return c.stats, errors.New("etl: nil writer")
	}
	if c.closed {
		return c.stats, ErrCollectorClosed
	}
	if c.loaded {
		return c.stats, ErrCollectorLoaded
	}
	if err := c.spillBuffer(); err != nil {
		return c.stats, err
	}
	applier := newApplier(writer, c.opts.BatchSize)
	defer applier.close()

	if err := c.mergeRuns(applier); err != nil {
		return c.stats, err
	}
	if err := applier.flush(); err != nil {
		return c.stats, err
	}
	c.stats.BatchWrites += applier.batchWrites
	c.loaded = true
	return c.stats, nil
}

// Close removes any temporary run files and rejects future collector use.
func (c *Collector) Close() error {
	if c == nil || c.closed {
		return nil
	}
	c.closed = true
	c.rows = nil
	c.runFiles = nil
	if c.dir == "" {
		return nil
	}
	return os.RemoveAll(c.dir)
}

func (c *Collector) append(e entry) error {
	if c.closed {
		return ErrCollectorClosed
	}
	if c.loaded {
		return ErrCollectorLoaded
	}
	if c.seq == ^uint64(0) {
		return errors.New("etl: sequence overflow")
	}
	c.seq++
	e.seq = c.seq
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
	sortEntries(c.rows)
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

	w := bufio.NewWriterSize(tmp, 1<<20)
	if _, err := w.WriteString(runFileMagic); err != nil {
		_ = tmp.Close()
		return err
	}
	for _, e := range c.rows {
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
	c.rows = nil
	c.bufferBytes = 0
	c.stats.SpilledRuns++
	return nil
}

func (c *Collector) mergeRuns(applier *applier) error {
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

	h := make(runHeap, 0, len(readers))
	for _, rr := range readers {
		if err := rr.next(); err != nil {
			return err
		}
		if rr.has {
			heap.Push(&h, rr)
		}
	}

	var (
		haveGroup bool
		groupKey  []byte
		winner    entry
	)
	applyGroup := func() error {
		if !haveGroup {
			return nil
		}
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
		return nil
	}

	for h.Len() > 0 {
		rr := heap.Pop(&h).(*runReader)
		e := rr.current
		if !haveGroup {
			haveGroup = true
			groupKey = e.key
			winner = e
		} else if bytes.Equal(groupKey, e.key) {
			if e.seq > winner.seq {
				winner = e
			}
		} else {
			if err := applyGroup(); err != nil {
				return err
			}
			groupKey = e.key
			winner = e
		}
		if err := rr.next(); err != nil {
			return err
		}
		if rr.has {
			heap.Push(&h, rr)
		}
	}
	return applyGroup()
}

func sortEntries(entries []entry) {
	sort.SliceStable(entries, func(i, j int) bool {
		if cmp := bytes.Compare(entries[i].key, entries[j].key); cmp != 0 {
			return cmp < 0
		}
		return entries[i].seq < entries[j].seq
	})
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

func readRunEntry(r io.Reader) (entry, error) {
	var header [runEntryHeaderSize]byte
	if _, err := io.ReadFull(r, header[:]); err != nil {
		if errors.Is(err, io.EOF) {
			return entry{}, io.EOF
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
	e.key = make([]byte, keyLen)
	if _, err := io.ReadFull(r, e.key); err != nil {
		return entry{}, err
	}
	e.value = make([]byte, valueLen)
	if _, err := io.ReadFull(r, e.value); err != nil {
		return entry{}, err
	}
	return e, nil
}

type runReader struct {
	index   int
	file    *os.File
	reader  *bufio.Reader
	current entry
	has     bool
}

func openRunReader(path string, index int) (*runReader, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	rr := &runReader{
		index:  index,
		file:   file,
		reader: bufio.NewReaderSize(file, 1<<20),
	}
	magic := make([]byte, len(runFileMagic))
	if _, err := io.ReadFull(rr.reader, magic); err != nil {
		_ = file.Close()
		return nil, err
	}
	if string(magic) != runFileMagic {
		_ = file.Close()
		return nil, fmt.Errorf("etl: invalid run file %q", path)
	}
	return rr, nil
}

func (r *runReader) next() error {
	e, err := readRunEntry(r.reader)
	if errors.Is(err, io.EOF) {
		r.has = false
		return nil
	}
	if err != nil {
		return err
	}
	r.current = e
	r.has = true
	return nil
}

func closeRunReaders(readers []*runReader) {
	for _, rr := range readers {
		if rr != nil && rr.file != nil {
			_ = rr.file.Close()
		}
	}
}

type runHeap []*runReader

func (h runHeap) Len() int { return len(h) }

func (h runHeap) Less(i, j int) bool {
	left, right := h[i].current, h[j].current
	if cmp := bytes.Compare(left.key, right.key); cmp != 0 {
		return cmp < 0
	}
	if left.seq != right.seq {
		return left.seq < right.seq
	}
	return h[i].index < h[j].index
}

func (h runHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }

func (h *runHeap) Push(x any) {
	*h = append(*h, x.(*runReader))
}

func (h *runHeap) Pop() any {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[:n-1]
	return x
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
