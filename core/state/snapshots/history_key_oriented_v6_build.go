package snapshots

import (
	"bufio"
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"math"
	"os"
	"path/filepath"
	"sort"

	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/rawdb/etl"
)

const stateDomainChangeV6DictionaryCacheBlocks = 64
const stateDomainChangeV6KeyTableMaxBytes = 512 << 20

type stateDomainChangeV6DictionaryBlock struct {
	firstKey []byte
	offset   uint64
	length   uint32
	count    uint32
}

type stateDomainChangeV6Build struct {
	opts             etl.Options
	keys             *etl.Collector
	postings         *etl.Collector
	dictionary       *os.File
	dictName         string
	blocks           []stateDomainChangeV6DictionaryBlock
	keyCount         uint32
	keyStats         etl.Stats
	cache            map[int][][]byte
	cacheOrder       []int
	keyScratch       []byte
	keyTable         []byte
	keyMask          uint64
	dictionaryDigest [sha256.Size]byte
}

func newStateDomainChangeV6Build(opts etl.Options, dir, base string) (*stateDomainChangeV6Build, error) {
	if opts.TempDir == "" {
		opts.TempDir = filepath.Join(dir, "etl")
	}
	keys, err := etl.NewCollector(opts)
	if err != nil {
		return nil, fmt.Errorf("snapshots: create V6 key ETL: %w", err)
	}
	postings, err := etl.NewCollector(opts)
	if err != nil {
		_ = keys.Close()
		return nil, fmt.Errorf("snapshots: create V6 posting ETL: %w", err)
	}
	dictionaryDir := filepath.Dir(filepath.Join(dir, base))
	if err := os.MkdirAll(dictionaryDir, 0o755); err != nil {
		_ = keys.Close()
		_ = postings.Close()
		return nil, err
	}
	dictionary, dictName, err := createStateDomainChangeBinaryTempFileInDir(dictionaryDir, filepath.Base(base)+".v6-dictionary")
	if err != nil {
		_ = keys.Close()
		_ = postings.Close()
		return nil, err
	}
	return &stateDomainChangeV6Build{opts: opts, keys: keys, postings: postings, dictionary: dictionary, dictName: dictName, cache: make(map[int][][]byte)}, nil
}

func (b *stateDomainChangeV6Build) Close() {
	if b == nil {
		return
	}
	if b.keys != nil {
		_ = b.keys.Close()
	}
	if b.postings != nil {
		_ = b.postings.Close()
	}
	if b.dictionary != nil {
		_ = b.dictionary.Close()
	}
	if b.dictName != "" {
		_ = os.Remove(b.dictName)
	}
}

func (b *stateDomainChangeV6Build) CollectKey(change *rawdb.StateDomainChange) error {
	if b == nil || b.keys == nil || change == nil {
		return errors.New("snapshots: nil V6 key collector/change")
	}
	b.keyScratch = appendStateDomainChangeBinaryAccessorLookupKey(b.keyScratch[:0], change.FlatDomain, change.Owner, change.Generation, change.Domain, change.Key)
	if len(b.keyScratch) > math.MaxUint16 {
		return fmt.Errorf("snapshots: V6 logical key length %d exceeds uint16", len(b.keyScratch))
	}
	return b.keys.Put(b.keyScratch, nil)
}

type stateDomainChangeV6DictionaryWriter struct {
	build    *stateDomainChangeV6Build
	block    [][]byte
	previous []byte
}

func (w *stateDomainChangeV6DictionaryWriter) Put(key, _ []byte) error {
	if w == nil || w.build == nil || w.build.dictionary == nil || len(key) == 0 {
		return errors.New("snapshots: invalid V6 dictionary key")
	}
	if w.previous != nil && bytes.Compare(w.previous, key) >= 0 {
		return errors.New("snapshots: V6 dictionary keys are not strictly ordered")
	}
	if w.build.keyCount == math.MaxUint32 {
		return errors.New("snapshots: V6 dictionary exceeds uint32 key IDs")
	}
	w.block = append(w.block, append([]byte(nil), key...))
	w.previous = append(w.previous[:0], key...)
	w.build.keyCount++
	if len(w.block) == stateDomainChangeBinaryAccessorV6BlockKeys {
		return w.flush()
	}
	return nil
}
func (*stateDomainChangeV6DictionaryWriter) Delete([]byte) error {
	return errors.New("snapshots: V6 dictionary delete")
}
func (w *stateDomainChangeV6DictionaryWriter) flush() error {
	if len(w.block) == 0 {
		return nil
	}
	off, err := w.build.dictionary.Seek(0, io.SeekCurrent)
	if err != nil {
		return err
	}
	raw := encodeStateDomainChangeV6DictionaryBlock(w.block)
	if _, err := w.build.dictionary.Write(raw); err != nil {
		return err
	}
	w.build.blocks = append(w.build.blocks, stateDomainChangeV6DictionaryBlock{firstKey: append([]byte(nil), w.block[0]...), offset: uint64(off), length: uint32(len(raw)), count: uint32(len(w.block))})
	w.block = w.block[:0]
	return nil
}

func encodeStateDomainChangeV6DictionaryBlock(keys [][]byte) []byte {
	var raw []byte
	var previous []byte
	for _, key := range keys {
		prefix := commonPrefixLength(previous, key)
		raw = binary.AppendUvarint(raw, uint64(prefix))
		raw = binary.AppendUvarint(raw, uint64(len(key)-prefix))
		raw = append(raw, key[prefix:]...)
		previous = key
	}
	return raw
}

func decodeStateDomainChangeV6DictionaryBlock(raw []byte, count uint32) ([][]byte, error) {
	reader := bytes.NewReader(raw)
	keys := make([][]byte, 0, count)
	var previous []byte
	for i := uint32(0); i < count; i++ {
		prefix, err := binary.ReadUvarint(reader)
		if err != nil || prefix > uint64(len(previous)) {
			return nil, errors.New("snapshots: invalid V6 dictionary prefix")
		}
		suffix, err := binary.ReadUvarint(reader)
		if err != nil || suffix > uint64(reader.Len()) {
			return nil, errors.New("snapshots: invalid V6 dictionary suffix")
		}
		key := make([]byte, prefix+suffix)
		copy(key, previous[:prefix])
		if _, err := io.ReadFull(reader, key[prefix:]); err != nil {
			return nil, err
		}
		if i > 0 && bytes.Compare(previous, key) >= 0 {
			return nil, errors.New("snapshots: unordered V6 dictionary block")
		}
		keys = append(keys, key)
		previous = key
	}
	if reader.Len() != 0 {
		return nil, errors.New("snapshots: V6 dictionary block trailing bytes")
	}
	return keys, nil
}

func (b *stateDomainChangeV6Build) FinishDictionary() error {
	if b == nil || b.keys == nil {
		return errors.New("snapshots: nil V6 dictionary build")
	}
	writer := &stateDomainChangeV6DictionaryWriter{build: b}
	stats, err := b.keys.Load(writer)
	if err != nil {
		return err
	}
	if err := writer.flush(); err != nil {
		return err
	}
	if err := b.dictionary.Sync(); err != nil {
		return err
	}
	b.keyStats = stats
	_ = b.keys.Close()
	b.keys = nil
	return b.buildKeyTable()
}

func (b *stateDomainChangeV6Build) iterateDictionaryKeys(fn func(uint32, []byte) error) error {
	for blockIndex := range b.blocks {
		keys, err := b.dictionaryBlock(blockIndex)
		if err != nil {
			return err
		}
		for i, key := range keys {
			if err := fn(uint32(blockIndex*stateDomainChangeBinaryAccessorV6BlockKeys+i), key); err != nil {
				return err
			}
		}
	}
	return nil
}

func (b *stateDomainChangeV6Build) buildKeyTable() error {
	buckets := uint64(1)
	for buckets*7 < uint64(b.keyCount)*10 {
		buckets <<= 1
	}
	useTable := b.keyCount != 0 && buckets <= uint64(stateDomainChangeV6KeyTableMaxBytes/16)
	if useTable {
		b.keyTable = make([]byte, buckets*16)
		b.keyMask = buckets - 1
	}
	digest := sha256.New()
	var length [4]byte
	err := b.iterateDictionaryKeys(func(keyID uint32, key []byte) error {
		binary.BigEndian.PutUint32(length[:], uint32(len(key)))
		_, _ = digest.Write(length[:])
		_, _ = digest.Write(key)
		if !useTable {
			return nil
		}
		token := sha256.Sum256(key)
		slot := binary.LittleEndian.Uint64(token[:8]) & b.keyMask
		for probes := uint64(0); probes <= b.keyMask; probes++ {
			off := slot * 16
			id := binary.BigEndian.Uint32(b.keyTable[off+12 : off+16])
			if id == 0 {
				copy(b.keyTable[off:off+12], token[:12])
				binary.BigEndian.PutUint32(b.keyTable[off+12:off+16], keyID+1)
				return nil
			}
			if bytes.Equal(b.keyTable[off:off+12], token[:12]) {
				return errors.New("snapshots: V6 logical-key fingerprint collision")
			}
			slot = (slot + 1) & b.keyMask
		}
		return errors.New("snapshots: V6 logical-key table is full")
	})
	if err != nil {
		return err
	}
	copy(b.dictionaryDigest[:], digest.Sum(nil))
	return nil
}

func (b *stateDomainChangeV6Build) ResetPostings() error {
	if b == nil {
		return errors.New("snapshots: nil V6 posting reset")
	}
	if b.postings != nil {
		_ = b.postings.Close()
	}
	postings, err := etl.NewCollector(b.opts)
	if err != nil {
		return fmt.Errorf("snapshots: recreate V6 posting ETL: %w", err)
	}
	b.postings = postings
	return nil
}

func (b *stateDomainChangeV6Build) dictionaryBlock(index int) ([][]byte, error) {
	if keys, ok := b.cache[index]; ok {
		return keys, nil
	}
	if index < 0 || index >= len(b.blocks) {
		return nil, errors.New("snapshots: V6 dictionary block outside range")
	}
	block := b.blocks[index]
	raw := make([]byte, block.length)
	if _, err := b.dictionary.ReadAt(raw, int64(block.offset)); err != nil {
		return nil, err
	}
	keys, err := decodeStateDomainChangeV6DictionaryBlock(raw, block.count)
	if err != nil {
		return nil, err
	}
	if !bytes.Equal(keys[0], block.firstKey) {
		return nil, errors.New("snapshots: V6 dictionary first key mismatch")
	}
	if len(b.cacheOrder) >= stateDomainChangeV6DictionaryCacheBlocks {
		delete(b.cache, b.cacheOrder[0])
		b.cacheOrder = b.cacheOrder[1:]
	}
	b.cache[index] = keys
	b.cacheOrder = append(b.cacheOrder, index)
	return keys, nil
}

func (b *stateDomainChangeV6Build) KeyID(key []byte) (uint32, error) {
	if b == nil {
		return 0, errors.New("snapshots: nil V6 dictionary")
	}
	if len(b.keyTable) != 0 {
		token := sha256.Sum256(key)
		slot := binary.LittleEndian.Uint64(token[:8]) & b.keyMask
		for probes := uint64(0); probes <= b.keyMask; probes++ {
			off := slot * 16
			id := binary.BigEndian.Uint32(b.keyTable[off+12 : off+16])
			if id == 0 {
				break
			}
			if bytes.Equal(b.keyTable[off:off+12], token[:12]) {
				return id - 1, nil
			}
			slot = (slot + 1) & b.keyMask
		}
		return 0, fmt.Errorf("snapshots: V6 key %x not found in fingerprint table", key)
	}
	if len(b.blocks) == 0 {
		return 0, errors.New("snapshots: empty V6 dictionary")
	}
	blockIndex := sort.Search(len(b.blocks), func(i int) bool { return bytes.Compare(b.blocks[i].firstKey, key) > 0 }) - 1
	if blockIndex < 0 {
		return 0, errors.New("snapshots: V6 key not found")
	}
	keys, err := b.dictionaryBlock(blockIndex)
	if err != nil {
		return 0, err
	}
	within := sort.Search(len(keys), func(i int) bool { return bytes.Compare(keys[i], key) >= 0 })
	if within == len(keys) || !bytes.Equal(keys[within], key) {
		return 0, fmt.Errorf("snapshots: V6 key %x not found", key)
	}
	return uint32(blockIndex*stateDomainChangeBinaryAccessorV6BlockKeys + within), nil
}

func (b *stateDomainChangeV6Build) CollectPosting(keyID uint32, txNum, offset, recordIndex uint64) error {
	if b == nil || b.postings == nil || recordIndex > math.MaxUint32 || offset > stateDomainChangeBinaryAccessorV5MaxOffset {
		return errors.New("snapshots: invalid V6 posting")
	}
	var key [8]byte
	binary.BigEndian.PutUint32(key[:4], keyID)
	binary.BigEndian.PutUint32(key[4:], uint32(recordIndex))
	var value [stateDomainChangeBinaryAccessorV6PostingSize]byte
	binary.BigEndian.PutUint64(value[:8], txNum)
	_ = putStateDomainChangeBinaryAccessorV5Offset(value[8:14], offset)
	binary.BigEndian.PutUint32(value[14:18], uint32(recordIndex))
	return b.postings.Put(key[:], value[:])
}

type stateDomainChangeV6PostingWriter struct {
	postings *bufio.Writer
	meta     *bufio.Writer
	expected uint32
	keyID    uint32
	haveKey  bool
	offset   uint64
	count    uint32
	rows     uint64
}

func (w *stateDomainChangeV6PostingWriter) Put(key, value []byte) error {
	if len(key) != 8 || len(value) != stateDomainChangeBinaryAccessorV6PostingSize {
		return errors.New("snapshots: invalid V6 posting ETL row")
	}
	keyID := binary.BigEndian.Uint32(key[:4])
	if keyID >= w.expected {
		return errors.New("snapshots: V6 posting key ID outside dictionary")
	}
	if !w.haveKey {
		if keyID != 0 {
			return errors.New("snapshots: V6 postings do not start at key zero")
		}
		w.keyID, w.haveKey = keyID, true
	} else if keyID != w.keyID {
		if keyID != w.keyID+1 {
			return errors.New("snapshots: V6 posting key ID gap")
		}
		if err := w.flushMeta(); err != nil {
			return err
		}
		w.keyID = keyID
	}
	if _, err := w.postings.Write(value); err != nil {
		return err
	}
	w.count++
	w.rows++
	return nil
}
func (*stateDomainChangeV6PostingWriter) Delete([]byte) error {
	return errors.New("snapshots: V6 posting delete")
}
func (w *stateDomainChangeV6PostingWriter) flushMeta() error {
	var raw [12]byte
	binary.BigEndian.PutUint64(raw[:8], w.offset)
	binary.BigEndian.PutUint32(raw[8:12], w.count)
	if _, err := w.meta.Write(raw[:]); err != nil {
		return err
	}
	w.offset += uint64(w.count) * stateDomainChangeBinaryAccessorV6PostingSize
	w.count = 0
	return nil
}
func (w *stateDomainChangeV6PostingWriter) Finish() error {
	if w.expected == 0 {
		if w.haveKey {
			return errors.New("snapshots: V6 empty dictionary has postings")
		}
		return nil
	}
	if !w.haveKey || w.keyID+1 != w.expected {
		return errors.New("snapshots: V6 postings do not cover dictionary")
	}
	if err := w.flushMeta(); err != nil {
		return err
	}
	if err := w.postings.Flush(); err != nil {
		return err
	}
	return w.meta.Flush()
}

func (b *stateDomainChangeV6Build) BuildAccessor(dir string, ref SegmentRef, recordCount uint64) (SegmentRef, etl.Stats, error) {
	if b == nil || b.postings == nil || uint64(b.keyCount) > recordCount {
		return SegmentRef{}, etl.Stats{}, errors.New("snapshots: invalid V6 accessor build")
	}
	abs := filepath.Join(dir, ref.Path)
	if err := os.MkdirAll(filepath.Dir(abs), 0o755); err != nil {
		return SegmentRef{}, etl.Stats{}, err
	}
	postingFile, postingName, err := createStateDomainChangeBinaryTempFileInDir(filepath.Dir(abs), filepath.Base(abs)+".postings")
	if err != nil {
		return SegmentRef{}, etl.Stats{}, err
	}
	defer func() { _ = postingFile.Close(); _ = os.Remove(postingName) }()
	metaFile, metaName, err := createStateDomainChangeBinaryTempFileInDir(filepath.Dir(abs), filepath.Base(abs)+".meta")
	if err != nil {
		return SegmentRef{}, etl.Stats{}, err
	}
	defer func() { _ = metaFile.Close(); _ = os.Remove(metaName) }()
	writer := &stateDomainChangeV6PostingWriter{postings: bufio.NewWriterSize(postingFile, 1<<20), meta: bufio.NewWriterSize(metaFile, 1<<20), expected: b.keyCount}
	postingStats, err := b.postings.Load(writer)
	if err != nil {
		return SegmentRef{}, postingStats, err
	}
	if err := writer.Finish(); err != nil {
		return SegmentRef{}, postingStats, err
	}
	if writer.rows != recordCount {
		return SegmentRef{}, postingStats, fmt.Errorf("snapshots: V6 postings wrote %d records, want %d", writer.rows, recordCount)
	}
	_ = b.postings.Close()
	b.postings = nil
	keyData, keyDataName, err := createStateDomainChangeBinaryTempFileInDir(filepath.Dir(abs), filepath.Base(abs)+".keys")
	if err != nil {
		return SegmentRef{}, postingStats, err
	}
	defer func() { _ = keyData.Close(); _ = os.Remove(keyDataName) }()
	var blocks []stateDomainChangeBinaryAccessorV6Block
	for blockIndex := range b.blocks {
		keys, err := b.dictionaryBlock(blockIndex)
		if err != nil {
			return SegmentRef{}, postingStats, err
		}
		records := make([]stateDomainChangeBinaryAccessorV6Key, len(keys))
		for i, key := range keys {
			keyID := uint32(blockIndex*stateDomainChangeBinaryAccessorV6BlockKeys + i)
			var raw [12]byte
			if _, err := metaFile.ReadAt(raw[:], int64(keyID)*12); err != nil {
				return SegmentRef{}, postingStats, err
			}
			records[i] = stateDomainChangeBinaryAccessorV6Key{key: key, keyID: keyID, postingOff: binary.BigEndian.Uint64(raw[:8]), postingCount: binary.BigEndian.Uint32(raw[8:12])}
		}
		raw, err := encodeStateDomainChangeBinaryAccessorV6KeyBlock(records)
		if err != nil {
			return SegmentRef{}, postingStats, err
		}
		off, err := keyData.Seek(0, io.SeekCurrent)
		if err != nil {
			return SegmentRef{}, postingStats, err
		}
		if _, err := keyData.Write(raw); err != nil {
			return SegmentRef{}, postingStats, err
		}
		blocks = append(blocks, stateDomainChangeBinaryAccessorV6Block{firstKey: append([]byte(nil), keys[0]...), dataOff: uint64(off), dataLen: uint32(len(raw)) | stateDomainChangeBinaryAccessorV6StoredRaw, rawLen: uint32(len(raw)), checksum: crc32.ChecksumIEEE(raw), keyCount: uint32(len(keys))})
	}
	keyStat, err := keyData.Stat()
	if err != nil {
		return SegmentRef{}, postingStats, err
	}
	postingStat, err := postingFile.Stat()
	if err != nil {
		return SegmentRef{}, postingStats, err
	}
	dirLen := uint64(0)
	for _, block := range blocks {
		dirLen += 2 + uint64(len(block.firstKey)) + stateDomainChangeBinaryAccessorV6BlockTail
	}
	dirLen += uint64(len(blocks)) * 8
	tmp, tmpName, err := createStateDomainChangeBinaryTempFileInDir(filepath.Dir(abs), filepath.Base(abs))
	if err != nil {
		return SegmentRef{}, postingStats, err
	}
	defer func() { _ = tmp.Close(); _ = os.Remove(tmpName) }()
	metadata := newSnapshotMetadataWriter(tmp)
	var header [stateDomainChangeBinaryAccessorV6HeaderSize]byte
	copy(header[:8], stateDomainChangeBinaryAccessorMagic[:])
	binary.BigEndian.PutUint32(header[8:12], stateDomainChangeBinaryVersionV6)
	binary.BigEndian.PutUint64(header[12:20], ref.FromTxNum)
	binary.BigEndian.PutUint64(header[20:28], ref.ToTxNum)
	binary.BigEndian.PutUint64(header[28:36], recordCount)
	binary.BigEndian.PutUint32(header[36:40], stateDomainChangeBinaryAccessorV6BlockKeys)
	binary.BigEndian.PutUint64(header[40:48], uint64(b.keyCount))
	binary.BigEndian.PutUint64(header[48:56], uint64(len(blocks)))
	binary.BigEndian.PutUint64(header[56:64], dirLen)
	binary.BigEndian.PutUint64(header[64:72], uint64(keyStat.Size()))
	copy(header[72:104], b.dictionaryDigest[:])
	binary.BigEndian.PutUint32(header[104:108], crc32.ChecksumIEEE(header[:104]))
	if _, err := metadata.Write(header[:]); err != nil {
		return SegmentRef{}, postingStats, err
	}
	keyDataBase := uint64(stateDomainChangeBinaryAccessorV6HeaderSize) + dirLen
	entryOffset := uint64(stateDomainChangeBinaryAccessorV6HeaderSize) + uint64(len(blocks))*8
	var offsetRaw [8]byte
	for _, block := range blocks {
		binary.BigEndian.PutUint64(offsetRaw[:], entryOffset)
		if _, err := metadata.Write(offsetRaw[:]); err != nil {
			return SegmentRef{}, postingStats, err
		}
		entryOffset += 2 + uint64(len(block.firstKey)) + stateDomainChangeBinaryAccessorV6BlockTail
	}
	var tail [stateDomainChangeBinaryAccessorV6BlockTail]byte
	for _, block := range blocks {
		if len(block.firstKey) > math.MaxUint16 {
			return SegmentRef{}, postingStats, errors.New("snapshots: V6 key too long")
		}
		var l [2]byte
		binary.BigEndian.PutUint16(l[:], uint16(len(block.firstKey)))
		if _, err := metadata.Write(l[:]); err != nil {
			return SegmentRef{}, postingStats, err
		}
		if _, err := metadata.Write(block.firstKey); err != nil {
			return SegmentRef{}, postingStats, err
		}
		binary.BigEndian.PutUint64(tail[0:8], keyDataBase+block.dataOff)
		binary.BigEndian.PutUint32(tail[8:12], block.dataLen)
		binary.BigEndian.PutUint32(tail[12:16], block.rawLen)
		binary.BigEndian.PutUint32(tail[16:20], block.checksum)
		binary.BigEndian.PutUint32(tail[20:24], block.keyCount)
		c := crc32.NewIEEE()
		c.Write(block.firstKey)
		c.Write(tail[:24])
		binary.BigEndian.PutUint32(tail[24:28], c.Sum32())
		if _, err := metadata.Write(tail[:]); err != nil {
			return SegmentRef{}, postingStats, err
		}
	}
	for _, src := range []*os.File{keyData, postingFile} {
		if _, err := src.Seek(0, io.SeekStart); err != nil {
			return SegmentRef{}, postingStats, err
		}
		if _, err := io.Copy(metadata, src); err != nil {
			return SegmentRef{}, postingStats, err
		}
	}
	if uint64(postingStat.Size()) != recordCount*stateDomainChangeBinaryAccessorV6PostingSize {
		return SegmentRef{}, postingStats, errors.New("snapshots: V6 posting file size mismatch")
	}
	result, err := finalizeStateDomainChangeHistoryFileWithMetadata(dir, ref, tmp, tmpName, metadata.Metadata(), false)
	return result, postingStats, err
}

var _ ethdb.KeyValueWriter = (*stateDomainChangeV6DictionaryWriter)(nil)
var _ ethdb.KeyValueWriter = (*stateDomainChangeV6PostingWriter)(nil)
