package snapshots

import (
	"bufio"
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"

	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/rawdb/etl"
)

const (
	ChainIndexSegmentVersion = 1
	chainIndexHeaderSize     = 8 + 8 + 8 + 8 + 8
	chainIndexBlockEntrySize = common.HashLength + 8
	chainIndexTxEntrySize    = common.HashLength + 8 + 4 + 4
	chainIndexETLBlockTag    = 0x00
	chainIndexETLTxTag       = 0x01
)

var chainIndexMagic = [8]byte{'g', 't', 'c', 'i', 'd', 'x', '1', '\n'}

type ChainIndexTxLookup = rawdb.ChainIndexTxLookup

type ChainIndexSegment struct {
	ref    SegmentRef
	file   *os.File
	header chainIndexHeader
}

type chainIndexHeader struct {
	fromBlock  uint64
	toBlock    uint64
	blockCount uint64
	txCount    uint64
}

type chainIndexBlockEntry struct {
	hash     common.Hash
	blockNum uint64
}

type chainIndexTxEntry struct {
	hash     common.Hash
	blockNum uint64
	txIndex  uint32
}

func ChainIndexSegmentPath(fromBlock, toBlock uint64) string {
	return fmt.Sprintf("chain/index-%d-%d.idx", fromBlock, toBlock)
}

func BuildChainIndexSegmentFromChainFreezerSegment(dir string, freezerRef SegmentRef, relPath string) (SegmentRef, error) {
	return BuildChainIndexSegmentFromChainFreezerSegmentWithOptions(dir, freezerRef, relPath, RestoreETLOptions{})
}

func BuildChainIndexSegmentFromChainFreezerSegmentWithOptions(dir string, freezerRef SegmentRef, relPath string, opts RestoreETLOptions) (SegmentRef, error) {
	if err := validateSegmentRef(freezerRef); err != nil {
		return SegmentRef{}, err
	}
	if freezerRef.Kind != SegmentChainFreezer {
		return SegmentRef{}, fmt.Errorf("snapshots: chain index requires %s segment, got %s", SegmentChainFreezer, freezerRef.Kind)
	}
	// The build pass validates each row. Verify its file identity first so the
	// derived index cannot be published from stale source metadata.
	if err := checkSegmentFileMetadata(dir, freezerRef, false); err != nil {
		return SegmentRef{}, fmt.Errorf("snapshots: verify chain-index source: %w", err)
	}
	if relPath == "" {
		relPath = ChainIndexSegmentPath(freezerRef.FromTxNum, freezerRef.ToTxNum)
	}
	ref := SegmentRef{
		Dataset:   SegmentDatasetChainFreezer,
		Kind:      SegmentChainIndex,
		FromTxNum: freezerRef.FromTxNum,
		ToTxNum:   freezerRef.ToTxNum,
		Path:      relPath,
	}
	if err := validateSegmentRef(ref); err != nil {
		return SegmentRef{}, err
	}
	expectedBlocks, err := chainFreezerRowCount(ref.FromTxNum, ref.ToTxNum)
	if err != nil {
		return SegmentRef{}, err
	}
	collector, err := etl.NewCollector(opts.collectorOptions())
	if err != nil {
		return SegmentRef{}, fmt.Errorf("snapshots: create chain-index build ETL collector: %w", err)
	}
	defer collector.Close()
	var blocksSeen uint64
	if err := iterateChainFreezerSegmentRows(dir, freezerRef, func(row chainFreezerRow) error {
		verified, err := validateChainFreezerRowPayload(row, "chain-index build")
		if err != nil {
			return err
		}
		block := verified.block
		blocksSeen++
		if err := collector.Put(chainIndexBlockETLKey(block.Hash(), row.blockNum), nil); err != nil {
			return err
		}
		for i, tx := range block.Transactions() {
			if uint64(i) > uint64(^uint32(0)) {
				return fmt.Errorf("snapshots: block %d transaction index %d exceeds uint32", row.blockNum, i)
			}
			if err := collector.Put(chainIndexTxETLKey(tx.Hash(), row.blockNum, uint32(i)), nil); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		return SegmentRef{}, err
	}
	if blocksSeen != expectedBlocks {
		return SegmentRef{}, fmt.Errorf("snapshots: chain index block entries %d, want %d", blocksSeen, expectedBlocks)
	}
	return writeChainIndexSegmentFromETL(dir, ref, collector, expectedBlocks)
}

func CheckChainIndexSegment(dir string, ref SegmentRef) error {
	if err := validateSegmentRef(ref); err != nil {
		return err
	}
	if ref.Kind != SegmentChainIndex {
		return fmt.Errorf("snapshots: chain-index checker got %s segment %q", ref.Kind, ref.Path)
	}
	if err := checkSegmentFileMetadata(dir, ref, false); err != nil {
		return err
	}
	file, err := os.Open(filepath.Join(dir, ref.Path))
	if err != nil {
		return err
	}
	defer file.Close()
	stat, err := file.Stat()
	if err != nil {
		return err
	}
	header, err := readChainIndexHeader(file)
	if err != nil {
		return err
	}
	if header.fromBlock != ref.FromTxNum || header.toBlock != ref.ToTxNum {
		return fmt.Errorf("snapshots: chain-index segment %q range [%d,%d], want [%d,%d]",
			ref.Path, header.fromBlock, header.toBlock, ref.FromTxNum, ref.ToTxNum)
	}
	expectedBlocks, err := chainFreezerRowCount(ref.FromTxNum, ref.ToTxNum)
	if err != nil {
		return err
	}
	if header.blockCount != expectedBlocks {
		return fmt.Errorf("snapshots: chain-index segment %q block entries %d, want %d", ref.Path, header.blockCount, expectedBlocks)
	}
	expectedSize, err := chainIndexExpectedSize(header)
	if err != nil {
		return err
	}
	if uint64(stat.Size()) != expectedSize {
		return fmt.Errorf("snapshots: chain-index segment %q size %d, want %d", ref.Path, stat.Size(), expectedSize)
	}
	if err := checkChainIndexBlockEntries(file, header, ref); err != nil {
		return err
	}
	if err := checkChainIndexTxEntries(file, header, ref); err != nil {
		return err
	}
	return nil
}

func OpenChainIndexSegment(dir string, ref SegmentRef) (*ChainIndexSegment, error) {
	if err := validateSegmentRef(ref); err != nil {
		return nil, err
	}
	if ref.Kind != SegmentChainIndex {
		return nil, fmt.Errorf("snapshots: expected %s segment, got %s", SegmentChainIndex, ref.Kind)
	}
	file, err := os.Open(filepath.Join(dir, ref.Path))
	if err != nil {
		return nil, err
	}
	header, err := readChainIndexHeader(file)
	if err != nil {
		_ = file.Close()
		return nil, err
	}
	if header.fromBlock != ref.FromTxNum || header.toBlock != ref.ToTxNum {
		_ = file.Close()
		return nil, fmt.Errorf("snapshots: chain-index segment %q range [%d,%d], want [%d,%d]",
			ref.Path, header.fromBlock, header.toBlock, ref.FromTxNum, ref.ToTxNum)
	}
	expectedBlocks, err := chainFreezerRowCount(ref.FromTxNum, ref.ToTxNum)
	if err != nil {
		_ = file.Close()
		return nil, err
	}
	if header.blockCount != expectedBlocks {
		_ = file.Close()
		return nil, fmt.Errorf("snapshots: chain-index segment %q block entries %d, want %d", ref.Path, header.blockCount, expectedBlocks)
	}
	expectedSize, err := chainIndexExpectedSize(header)
	if err != nil {
		_ = file.Close()
		return nil, err
	}
	stat, err := file.Stat()
	if err != nil {
		_ = file.Close()
		return nil, err
	}
	if uint64(stat.Size()) != expectedSize {
		_ = file.Close()
		return nil, fmt.Errorf("snapshots: chain-index segment %q size %d, want %d", ref.Path, stat.Size(), expectedSize)
	}
	return &ChainIndexSegment{ref: ref, file: file, header: header}, nil
}

func (s *ChainIndexSegment) Close() error {
	if s == nil || s.file == nil {
		return nil
	}
	return s.file.Close()
}

func (s *ChainIndexSegment) BlockNumberByHash(hash common.Hash) (uint64, bool, error) {
	if s == nil || s.file == nil {
		return 0, false, nil
	}
	lo, hi := uint64(0), s.header.blockCount
	for lo < hi {
		mid := lo + (hi-lo)/2
		entry, err := readChainIndexBlockEntryAt(s.file, chainIndexBlockEntryOffset(mid))
		if err != nil {
			return 0, false, err
		}
		cmp := bytes.Compare(entry.hash[:], hash[:])
		switch {
		case cmp == 0:
			return entry.blockNum, true, nil
		case cmp < 0:
			lo = mid + 1
		default:
			hi = mid
		}
	}
	return 0, false, nil
}

func (s *ChainIndexSegment) TransactionIndexByHash(hash common.Hash) (ChainIndexTxLookup, bool, error) {
	var zero ChainIndexTxLookup
	if s == nil || s.file == nil {
		return zero, false, nil
	}
	lo, hi := uint64(0), s.header.txCount
	for lo < hi {
		mid := lo + (hi-lo)/2
		entry, err := readChainIndexTxEntryAt(s.file, chainIndexTxEntryOffset(s.header, mid))
		if err != nil {
			return zero, false, err
		}
		cmp := bytes.Compare(entry.hash[:], hash[:])
		switch {
		case cmp == 0:
			return ChainIndexTxLookup{BlockNum: entry.blockNum, TxIndex: entry.txIndex}, true, nil
		case cmp < 0:
			lo = mid + 1
		default:
			hi = mid
		}
	}
	return zero, false, nil
}

func (m *Manager) BlockNumberByHash(hash common.Hash) (uint64, bool, error) {
	manifest, err := m.currentManifest()
	if err != nil || manifest == nil {
		return 0, false, err
	}
	for _, ref := range chainIndexRefs(manifest) {
		seg, err := OpenChainIndexSegment(m.dir, ref)
		if err != nil {
			return 0, false, err
		}
		num, ok, lookupErr := seg.BlockNumberByHash(hash)
		closeErr := seg.Close()
		if lookupErr != nil {
			return 0, false, lookupErr
		}
		if closeErr != nil {
			return 0, false, closeErr
		}
		if ok {
			if err := m.verifyChainIndexBlockLookup(manifest, ref, hash, num); err != nil {
				return 0, false, err
			}
			return num, true, nil
		}
	}
	return 0, false, nil
}

func (m *Manager) TransactionIndexByHash(hash common.Hash) (ChainIndexTxLookup, bool, error) {
	var zero ChainIndexTxLookup
	manifest, err := m.currentManifest()
	if err != nil || manifest == nil {
		return zero, false, err
	}
	for _, ref := range chainIndexRefs(manifest) {
		seg, err := OpenChainIndexSegment(m.dir, ref)
		if err != nil {
			return zero, false, err
		}
		lookup, ok, lookupErr := seg.TransactionIndexByHash(hash)
		closeErr := seg.Close()
		if lookupErr != nil {
			return zero, false, lookupErr
		}
		if closeErr != nil {
			return zero, false, closeErr
		}
		if ok {
			if err := m.verifyChainIndexTxLookup(manifest, ref, hash, lookup); err != nil {
				return zero, false, err
			}
			return lookup, true, nil
		}
	}
	return zero, false, nil
}

func (m *Manager) TransactionBlockNumberByHash(hash common.Hash) (uint64, bool, error) {
	lookup, ok, err := m.TransactionIndexByHash(hash)
	if err != nil || !ok {
		return 0, ok, err
	}
	return lookup.BlockNum, true, nil
}

func (m *Manager) verifyChainIndexBlockLookup(manifest *Manifest, indexRef SegmentRef, hash common.Hash, blockNum uint64) error {
	verified, err := m.readVerifiedChainIndexLookupBlock(manifest, indexRef, blockNum, "chain-index block lookup")
	if err != nil {
		return err
	}
	if got := verified.block.Hash(); got != hash {
		return fmt.Errorf("snapshots: chain-index segment %q block hash %x points to block %d with hash %x", indexRef.Path, hash, blockNum, got)
	}
	return nil
}

func (m *Manager) verifyChainIndexTxLookup(manifest *Manifest, indexRef SegmentRef, hash common.Hash, lookup ChainIndexTxLookup) error {
	verified, err := m.readVerifiedChainIndexLookupBlock(manifest, indexRef, lookup.BlockNum, "chain-index transaction lookup")
	if err != nil {
		return err
	}
	txs := verified.block.Transactions()
	if uint64(lookup.TxIndex) >= uint64(len(txs)) {
		return fmt.Errorf("snapshots: chain-index segment %q tx hash %x points to block %d index %d outside %d transactions", indexRef.Path, hash, lookup.BlockNum, lookup.TxIndex, len(txs))
	}
	tx := txs[int(lookup.TxIndex)]
	if tx == nil {
		return fmt.Errorf("snapshots: chain-index segment %q tx hash %x points to nil tx at block %d index %d", indexRef.Path, hash, lookup.BlockNum, lookup.TxIndex)
	}
	if got := tx.Hash(); got != hash {
		return fmt.Errorf("snapshots: chain-index segment %q tx hash %x points to block %d index %d with hash %x", indexRef.Path, hash, lookup.BlockNum, lookup.TxIndex, got)
	}
	return nil
}

func (m *Manager) readVerifiedChainIndexLookupBlock(manifest *Manifest, indexRef SegmentRef, blockNum uint64, context string) (validatedChainFreezerRow, error) {
	var zero validatedChainFreezerRow
	if blockNum < indexRef.FromTxNum || blockNum > indexRef.ToTxNum {
		return zero, fmt.Errorf("snapshots: chain-index segment %q returned block %d outside range [%d,%d]", indexRef.Path, blockNum, indexRef.FromTxNum, indexRef.ToTxNum)
	}
	freezerRef, ok := chainFreezerRefForIndex(manifest, indexRef)
	if !ok {
		return zero, fmt.Errorf("snapshots: chain-index segment %q has no matching chain-freezer segment for block range [%d,%d]", indexRef.Path, indexRef.FromTxNum, indexRef.ToTxNum)
	}
	row, found, err := readChainFreezerSegmentRow(m.dir, freezerRef, blockNum)
	if err != nil {
		return zero, err
	}
	if !found {
		return zero, fmt.Errorf("snapshots: chain-index segment %q returned block %d missing from chain-freezer segment %q", indexRef.Path, blockNum, freezerRef.Path)
	}
	return validateChainFreezerRowPayload(row, context)
}

func VerifyChainIndexSegmentAgainstChainFreezer(dir string, indexRef, freezerRef SegmentRef) error {
	if indexRef.Kind != SegmentChainIndex || freezerRef.Kind != SegmentChainFreezer {
		return fmt.Errorf("snapshots: chain index verification requires %s against %s, got %s against %s",
			SegmentChainIndex, SegmentChainFreezer, indexRef.Kind, freezerRef.Kind)
	}
	if indexRef.FromTxNum != freezerRef.FromTxNum || indexRef.ToTxNum != freezerRef.ToTxNum {
		return fmt.Errorf("snapshots: chain-index range [%d,%d] does not match chain-freezer range [%d,%d]",
			indexRef.FromTxNum, indexRef.ToTxNum, freezerRef.FromTxNum, freezerRef.ToTxNum)
	}
	if err := CheckChainIndexSegment(dir, indexRef); err != nil {
		return err
	}
	if err := CheckChainFreezerSegment(dir, freezerRef); err != nil {
		return err
	}
	seg, err := OpenChainIndexSegment(dir, indexRef)
	if err != nil {
		return err
	}
	defer seg.Close()
	var blocksSeen, txsSeen uint64
	if err := iterateChainFreezerSegmentRows(dir, freezerRef, func(row chainFreezerRow) error {
		verified, err := validateChainFreezerRowPayload(row, "chain-index verification")
		if err != nil {
			return err
		}
		block := verified.block
		blockHash := block.Hash()
		blockNum, ok, err := seg.BlockNumberByHash(blockHash)
		if err != nil {
			return err
		}
		if !ok || blockNum != row.blockNum {
			return fmt.Errorf("snapshots: chain-index segment %q missing block hash %x at block %d", indexRef.Path, blockHash, row.blockNum)
		}
		blocksSeen++
		for i, tx := range block.Transactions() {
			txHash := tx.Hash()
			lookup, ok, err := seg.TransactionIndexByHash(txHash)
			if err != nil {
				return err
			}
			if !ok || lookup.BlockNum != row.blockNum || lookup.TxIndex != uint32(i) {
				return fmt.Errorf("snapshots: chain-index segment %q missing tx hash %x at block %d index %d", indexRef.Path, txHash, row.blockNum, i)
			}
			txsSeen++
		}
		return nil
	}); err != nil {
		return err
	}
	if blocksSeen != seg.header.blockCount {
		return fmt.Errorf("snapshots: chain-index segment %q block entries %d, freezer has %d", indexRef.Path, seg.header.blockCount, blocksSeen)
	}
	if txsSeen != seg.header.txCount {
		return fmt.Errorf("snapshots: chain-index segment %q tx entries %d, freezer has %d", indexRef.Path, seg.header.txCount, txsSeen)
	}
	return nil
}

func validateChainIndexCompanions(manifest *Manifest) error {
	if manifest == nil {
		return nil
	}
	freezerRanges := make(map[chainBlockSegmentRange]struct{})
	for _, ref := range manifest.Segments {
		if ref.Kind != SegmentChainFreezer {
			continue
		}
		freezerRanges[chainBlockSegmentRange{from: ref.FromTxNum, to: ref.ToTxNum}] = struct{}{}
	}
	for _, ref := range manifest.Segments {
		if ref.Kind != SegmentChainIndex && ref.Kind != SegmentChainFreezerAccessor {
			continue
		}
		if _, ok := freezerRanges[chainBlockSegmentRange{from: ref.FromTxNum, to: ref.ToTxNum}]; !ok {
			return fmt.Errorf("snapshots: %s segment %q has no matching chain-freezer segment for block range [%d,%d]",
				ref.Kind, ref.Path, ref.FromTxNum, ref.ToTxNum)
		}
	}
	return nil
}

func writeChainIndexSegment(dir string, ref SegmentRef, blocks []chainIndexBlockEntry, txs []chainIndexTxEntry) (SegmentRef, error) {
	if err := validateSegmentRef(ref); err != nil {
		return SegmentRef{}, err
	}
	if ref.Kind != SegmentChainIndex {
		return SegmentRef{}, fmt.Errorf("snapshots: chain-index writer got %s segment %q", ref.Kind, ref.Path)
	}
	abs := filepath.Join(dir, ref.Path)
	if err := os.MkdirAll(filepath.Dir(abs), 0o755); err != nil {
		return SegmentRef{}, err
	}
	tmp, err := os.CreateTemp(filepath.Dir(abs), "."+filepath.Base(abs)+".*.tmp")
	if err != nil {
		return SegmentRef{}, err
	}
	tmpName := tmp.Name()
	defer os.Remove(tmpName)

	hash := sha256.New()
	counter := &countingWriter{w: io.MultiWriter(tmp, hash)}
	buf := bufio.NewWriter(counter)
	header := chainIndexHeader{
		fromBlock:  ref.FromTxNum,
		toBlock:    ref.ToTxNum,
		blockCount: uint64(len(blocks)),
		txCount:    uint64(len(txs)),
	}
	if err := writeChainIndexHeader(buf, header); err != nil {
		_ = tmp.Close()
		return SegmentRef{}, err
	}
	for _, entry := range blocks {
		if err := writeChainIndexBlockEntry(buf, entry); err != nil {
			_ = tmp.Close()
			return SegmentRef{}, err
		}
	}
	for _, entry := range txs {
		if err := writeChainIndexTxEntry(buf, entry); err != nil {
			_ = tmp.Close()
			return SegmentRef{}, err
		}
	}
	if err := buf.Flush(); err != nil {
		_ = tmp.Close()
		return SegmentRef{}, err
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return SegmentRef{}, err
	}
	if err := tmp.Close(); err != nil {
		return SegmentRef{}, err
	}

	ref.Size = counter.n
	ref.Checksum = "sha256:" + hex.EncodeToString(hash.Sum(nil))
	ref.Path = contentAddressedSnapshotPath(ref.Path, ref.Checksum)
	finalAbs := filepath.Join(dir, ref.Path)
	if err := os.MkdirAll(filepath.Dir(finalAbs), 0o755); err != nil {
		return SegmentRef{}, err
	}
	if err := os.Rename(tmpName, finalAbs); err != nil {
		return SegmentRef{}, err
	}
	return ref, nil
}

func writeChainIndexSegmentFromETL(dir string, ref SegmentRef, collector *etl.Collector, expectedBlocks uint64) (SegmentRef, error) {
	if err := validateSegmentRef(ref); err != nil {
		return SegmentRef{}, err
	}
	if ref.Kind != SegmentChainIndex {
		return SegmentRef{}, fmt.Errorf("snapshots: chain-index writer got %s segment %q", ref.Kind, ref.Path)
	}
	if collector == nil {
		return SegmentRef{}, errors.New("snapshots: nil chain-index ETL collector")
	}
	abs := filepath.Join(dir, ref.Path)
	if err := os.MkdirAll(filepath.Dir(abs), 0o755); err != nil {
		return SegmentRef{}, err
	}
	tmp, err := os.CreateTemp(filepath.Dir(abs), "."+filepath.Base(abs)+".*.tmp")
	if err != nil {
		return SegmentRef{}, err
	}
	tmpName := tmp.Name()
	defer os.Remove(tmpName)

	writer := newChainIndexETLSegmentWriter(ref, tmp)
	if err := writer.writePlaceholderHeader(); err != nil {
		_ = tmp.Close()
		return SegmentRef{}, err
	}
	if _, err := collector.Load(writer); err != nil {
		_ = tmp.Close()
		return SegmentRef{}, err
	}
	if writer.blockCount != expectedBlocks {
		_ = tmp.Close()
		return SegmentRef{}, fmt.Errorf("snapshots: chain index block entries %d, want %d", writer.blockCount, expectedBlocks)
	}
	if err := writer.flush(); err != nil {
		_ = tmp.Close()
		return SegmentRef{}, err
	}
	if _, err := tmp.Seek(0, io.SeekStart); err != nil {
		_ = tmp.Close()
		return SegmentRef{}, err
	}
	header := chainIndexHeader{
		fromBlock:  ref.FromTxNum,
		toBlock:    ref.ToTxNum,
		blockCount: writer.blockCount,
		txCount:    writer.txCount,
	}
	if err := writeChainIndexHeader(tmp, header); err != nil {
		_ = tmp.Close()
		return SegmentRef{}, err
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return SegmentRef{}, err
	}
	if err := tmp.Close(); err != nil {
		return SegmentRef{}, err
	}

	size, checksum, err := stateDomainChangeBinaryFileMetadata(tmpName)
	if err != nil {
		return SegmentRef{}, err
	}
	ref.Size = size
	ref.Checksum = checksum
	ref.Path = contentAddressedSnapshotPath(ref.Path, ref.Checksum)
	finalAbs := filepath.Join(dir, ref.Path)
	if err := os.MkdirAll(filepath.Dir(finalAbs), 0o755); err != nil {
		return SegmentRef{}, err
	}
	if err := os.Rename(tmpName, finalAbs); err != nil {
		return SegmentRef{}, err
	}
	return ref, nil
}

type chainIndexETLSegmentWriter struct {
	ref           SegmentRef
	buf           *bufio.Writer
	blockCount    uint64
	txCount       uint64
	sawTx         bool
	haveBlockHash bool
	haveTxHash    bool
	prevBlockHash common.Hash
	prevTxHash    common.Hash
}

func newChainIndexETLSegmentWriter(ref SegmentRef, w io.Writer) *chainIndexETLSegmentWriter {
	return &chainIndexETLSegmentWriter{
		ref: ref,
		buf: bufio.NewWriter(w),
	}
}

func (w *chainIndexETLSegmentWriter) writePlaceholderHeader() error {
	return writeChainIndexHeader(w.buf, chainIndexHeader{
		fromBlock: w.ref.FromTxNum,
		toBlock:   w.ref.ToTxNum,
	})
}

func (w *chainIndexETLSegmentWriter) Put(key, _ []byte) error {
	tag, hash, blockNum, txIndex, err := decodeChainIndexETLKey(key)
	if err != nil {
		return err
	}
	if blockNum < w.ref.FromTxNum || blockNum > w.ref.ToTxNum {
		return fmt.Errorf("snapshots: chain-index ETL key points to block %d outside [%d,%d]",
			blockNum, w.ref.FromTxNum, w.ref.ToTxNum)
	}
	switch tag {
	case chainIndexETLBlockTag:
		if w.sawTx {
			return errors.New("snapshots: chain-index ETL block entry after transaction entries")
		}
		if w.haveBlockHash {
			cmp := bytes.Compare(w.prevBlockHash[:], hash[:])
			if cmp == 0 {
				return fmt.Errorf("snapshots: duplicate chain-index block hash %x", hash)
			}
			if cmp > 0 {
				return errors.New("snapshots: chain-index ETL block entries are out of hash order")
			}
		}
		if err := writeChainIndexBlockEntry(w.buf, chainIndexBlockEntry{hash: hash, blockNum: blockNum}); err != nil {
			return err
		}
		w.prevBlockHash = hash
		w.haveBlockHash = true
		w.blockCount++
	case chainIndexETLTxTag:
		w.sawTx = true
		if w.haveTxHash {
			cmp := bytes.Compare(w.prevTxHash[:], hash[:])
			if cmp == 0 {
				return fmt.Errorf("snapshots: duplicate chain-index tx hash %x", hash)
			}
			if cmp > 0 {
				return errors.New("snapshots: chain-index ETL tx entries are out of hash order")
			}
		}
		if err := writeChainIndexTxEntry(w.buf, chainIndexTxEntry{hash: hash, blockNum: blockNum, txIndex: txIndex}); err != nil {
			return err
		}
		w.prevTxHash = hash
		w.haveTxHash = true
		w.txCount++
	default:
		return fmt.Errorf("snapshots: unknown chain-index ETL tag %d", tag)
	}
	return nil
}

func (w *chainIndexETLSegmentWriter) Delete(key []byte) error {
	return fmt.Errorf("snapshots: chain-index ETL delete unsupported for key %x", key)
}

func (w *chainIndexETLSegmentWriter) flush() error {
	return w.buf.Flush()
}

func chainIndexBlockETLKey(hash common.Hash, blockNum uint64) []byte {
	key := make([]byte, 1+common.HashLength+8)
	key[0] = chainIndexETLBlockTag
	copy(key[1:1+common.HashLength], hash[:])
	binary.BigEndian.PutUint64(key[1+common.HashLength:], blockNum)
	return key
}

func chainIndexTxETLKey(hash common.Hash, blockNum uint64, txIndex uint32) []byte {
	key := make([]byte, 1+common.HashLength+8+4)
	key[0] = chainIndexETLTxTag
	copy(key[1:1+common.HashLength], hash[:])
	binary.BigEndian.PutUint64(key[1+common.HashLength:1+common.HashLength+8], blockNum)
	binary.BigEndian.PutUint32(key[1+common.HashLength+8:], txIndex)
	return key
}

func decodeChainIndexETLKey(key []byte) (byte, common.Hash, uint64, uint32, error) {
	var hash common.Hash
	if len(key) != 1+common.HashLength+8 && len(key) != 1+common.HashLength+8+4 {
		return 0, hash, 0, 0, fmt.Errorf("snapshots: malformed chain-index ETL key length %d", len(key))
	}
	tag := key[0]
	copy(hash[:], key[1:1+common.HashLength])
	blockNum := binary.BigEndian.Uint64(key[1+common.HashLength : 1+common.HashLength+8])
	var txIndex uint32
	switch tag {
	case chainIndexETLBlockTag:
		if len(key) != 1+common.HashLength+8 {
			return 0, hash, 0, 0, fmt.Errorf("snapshots: malformed chain-index block ETL key length %d", len(key))
		}
	case chainIndexETLTxTag:
		if len(key) != 1+common.HashLength+8+4 {
			return 0, hash, 0, 0, fmt.Errorf("snapshots: malformed chain-index tx ETL key length %d", len(key))
		}
		txIndex = binary.BigEndian.Uint32(key[1+common.HashLength+8:])
	default:
		return 0, hash, 0, 0, fmt.Errorf("snapshots: unknown chain-index ETL tag %d", tag)
	}
	return tag, hash, blockNum, txIndex, nil
}

func sortChainIndexEntries(blocks []chainIndexBlockEntry, txs []chainIndexTxEntry) {
	sort.Slice(blocks, func(i, j int) bool {
		cmp := bytes.Compare(blocks[i].hash[:], blocks[j].hash[:])
		if cmp != 0 {
			return cmp < 0
		}
		return blocks[i].blockNum < blocks[j].blockNum
	})
	sort.Slice(txs, func(i, j int) bool {
		cmp := bytes.Compare(txs[i].hash[:], txs[j].hash[:])
		if cmp != 0 {
			return cmp < 0
		}
		if txs[i].blockNum != txs[j].blockNum {
			return txs[i].blockNum < txs[j].blockNum
		}
		return txs[i].txIndex < txs[j].txIndex
	})
}

func validateUniqueChainIndexBlockEntries(entries []chainIndexBlockEntry) error {
	for i := 1; i < len(entries); i++ {
		if bytes.Equal(entries[i-1].hash[:], entries[i].hash[:]) {
			return fmt.Errorf("snapshots: duplicate chain-index block hash %x", entries[i].hash)
		}
	}
	return nil
}

func validateUniqueChainIndexTxEntries(entries []chainIndexTxEntry) error {
	for i := 1; i < len(entries); i++ {
		if bytes.Equal(entries[i-1].hash[:], entries[i].hash[:]) {
			return fmt.Errorf("snapshots: duplicate chain-index tx hash %x", entries[i].hash)
		}
	}
	return nil
}

func checkChainIndexBlockEntries(file io.ReaderAt, header chainIndexHeader, ref SegmentRef) error {
	var prev common.Hash
	for i := uint64(0); i < header.blockCount; i++ {
		entry, err := readChainIndexBlockEntryAt(file, chainIndexBlockEntryOffset(i))
		if err != nil {
			return fmt.Errorf("snapshots: read chain-index block entry %d in %q: %w", i, ref.Path, err)
		}
		if entry.blockNum < ref.FromTxNum || entry.blockNum > ref.ToTxNum {
			return fmt.Errorf("snapshots: chain-index block entry %d in %q points to block %d outside [%d,%d]",
				i, ref.Path, entry.blockNum, ref.FromTxNum, ref.ToTxNum)
		}
		if i != 0 {
			cmp := bytes.Compare(prev[:], entry.hash[:])
			if cmp == 0 {
				return fmt.Errorf("snapshots: chain-index block entry %d in %q duplicates hash %x", i, ref.Path, entry.hash)
			}
			if cmp > 0 {
				return fmt.Errorf("snapshots: chain-index block entry %d in %q is out of hash order", i, ref.Path)
			}
		}
		prev = entry.hash
	}
	return nil
}

func checkChainIndexTxEntries(file io.ReaderAt, header chainIndexHeader, ref SegmentRef) error {
	var prev common.Hash
	for i := uint64(0); i < header.txCount; i++ {
		entry, err := readChainIndexTxEntryAt(file, chainIndexTxEntryOffset(header, i))
		if err != nil {
			return fmt.Errorf("snapshots: read chain-index tx entry %d in %q: %w", i, ref.Path, err)
		}
		if entry.blockNum < ref.FromTxNum || entry.blockNum > ref.ToTxNum {
			return fmt.Errorf("snapshots: chain-index tx entry %d in %q points to block %d outside [%d,%d]",
				i, ref.Path, entry.blockNum, ref.FromTxNum, ref.ToTxNum)
		}
		if i != 0 {
			cmp := bytes.Compare(prev[:], entry.hash[:])
			if cmp == 0 {
				return fmt.Errorf("snapshots: chain-index tx entry %d in %q duplicates hash %x", i, ref.Path, entry.hash)
			}
			if cmp > 0 {
				return fmt.Errorf("snapshots: chain-index tx entry %d in %q is out of hash order", i, ref.Path)
			}
		}
		prev = entry.hash
	}
	return nil
}

func chainIndexRefs(manifest *Manifest) []SegmentRef {
	if manifest == nil {
		return nil
	}
	refs := make([]SegmentRef, 0)
	for _, ref := range manifest.Segments {
		if ref.Kind != SegmentChainIndex || ref.normalizedDataset() != SegmentDatasetChainFreezer {
			continue
		}
		refs = append(refs, ref)
	}
	sort.Slice(refs, func(i, j int) bool {
		if refs[i].ToTxNum != refs[j].ToTxNum {
			return refs[i].ToTxNum > refs[j].ToTxNum
		}
		if refs[i].FromTxNum != refs[j].FromTxNum {
			return refs[i].FromTxNum > refs[j].FromTxNum
		}
		return refs[i].Path < refs[j].Path
	})
	return refs
}

func chainIndexRefForFreezer(manifest *Manifest, freezerRef SegmentRef) (SegmentRef, bool) {
	if manifest == nil || freezerRef.Kind != SegmentChainFreezer {
		return SegmentRef{}, false
	}
	for _, ref := range manifest.Segments {
		if ref.Kind != SegmentChainIndex ||
			ref.normalizedDataset() != SegmentDatasetChainFreezer ||
			ref.Domain != freezerRef.Domain ||
			ref.FromTxNum != freezerRef.FromTxNum ||
			ref.ToTxNum != freezerRef.ToTxNum {
			continue
		}
		return ref, true
	}
	return SegmentRef{}, false
}

func chainFreezerRefForIndex(manifest *Manifest, indexRef SegmentRef) (SegmentRef, bool) {
	if manifest == nil || indexRef.Kind != SegmentChainIndex {
		return SegmentRef{}, false
	}
	for _, ref := range manifest.Segments {
		if ref.Kind != SegmentChainFreezer ||
			ref.normalizedDataset() != SegmentDatasetChainFreezer ||
			ref.Domain != indexRef.Domain ||
			ref.FromTxNum != indexRef.FromTxNum ||
			ref.ToTxNum != indexRef.ToTxNum {
			continue
		}
		return ref, true
	}
	return SegmentRef{}, false
}

func writeChainIndexHeader(w io.Writer, header chainIndexHeader) error {
	var raw [chainIndexHeaderSize]byte
	copy(raw[0:8], chainIndexMagic[:])
	binary.BigEndian.PutUint64(raw[8:16], header.fromBlock)
	binary.BigEndian.PutUint64(raw[16:24], header.toBlock)
	binary.BigEndian.PutUint64(raw[24:32], header.blockCount)
	binary.BigEndian.PutUint64(raw[32:40], header.txCount)
	_, err := w.Write(raw[:])
	return err
}

func readChainIndexHeader(r io.Reader) (chainIndexHeader, error) {
	var raw [chainIndexHeaderSize]byte
	if _, err := io.ReadFull(r, raw[:]); err != nil {
		if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
			return chainIndexHeader{}, io.ErrUnexpectedEOF
		}
		return chainIndexHeader{}, err
	}
	if headerMagic := [8]byte(raw[0:8]); headerMagic != chainIndexMagic {
		return chainIndexHeader{}, errors.New("snapshots: invalid chain-index segment magic")
	}
	return chainIndexHeader{
		fromBlock:  binary.BigEndian.Uint64(raw[8:16]),
		toBlock:    binary.BigEndian.Uint64(raw[16:24]),
		blockCount: binary.BigEndian.Uint64(raw[24:32]),
		txCount:    binary.BigEndian.Uint64(raw[32:40]),
	}, nil
}

func writeChainIndexBlockEntry(w io.Writer, entry chainIndexBlockEntry) error {
	var raw [chainIndexBlockEntrySize]byte
	copy(raw[0:common.HashLength], entry.hash[:])
	binary.BigEndian.PutUint64(raw[common.HashLength:common.HashLength+8], entry.blockNum)
	_, err := w.Write(raw[:])
	return err
}

func writeChainIndexTxEntry(w io.Writer, entry chainIndexTxEntry) error {
	var raw [chainIndexTxEntrySize]byte
	copy(raw[0:common.HashLength], entry.hash[:])
	binary.BigEndian.PutUint64(raw[common.HashLength:common.HashLength+8], entry.blockNum)
	binary.BigEndian.PutUint32(raw[common.HashLength+8:common.HashLength+12], entry.txIndex)
	_, err := w.Write(raw[:])
	return err
}

func readChainIndexBlockEntryAt(r io.ReaderAt, offset int64) (chainIndexBlockEntry, error) {
	var raw [chainIndexBlockEntrySize]byte
	if _, err := r.ReadAt(raw[:], offset); err != nil {
		if errors.Is(err, io.EOF) {
			return chainIndexBlockEntry{}, io.ErrUnexpectedEOF
		}
		return chainIndexBlockEntry{}, err
	}
	var entry chainIndexBlockEntry
	copy(entry.hash[:], raw[0:common.HashLength])
	entry.blockNum = binary.BigEndian.Uint64(raw[common.HashLength : common.HashLength+8])
	return entry, nil
}

func readChainIndexTxEntryAt(r io.ReaderAt, offset int64) (chainIndexTxEntry, error) {
	var raw [chainIndexTxEntrySize]byte
	if _, err := r.ReadAt(raw[:], offset); err != nil {
		if errors.Is(err, io.EOF) {
			return chainIndexTxEntry{}, io.ErrUnexpectedEOF
		}
		return chainIndexTxEntry{}, err
	}
	var entry chainIndexTxEntry
	copy(entry.hash[:], raw[0:common.HashLength])
	entry.blockNum = binary.BigEndian.Uint64(raw[common.HashLength : common.HashLength+8])
	entry.txIndex = binary.BigEndian.Uint32(raw[common.HashLength+8 : common.HashLength+12])
	return entry, nil
}

func chainIndexBlockEntryOffset(index uint64) int64 {
	return int64(chainIndexHeaderSize + index*chainIndexBlockEntrySize)
}

func chainIndexTxEntryOffset(header chainIndexHeader, index uint64) int64 {
	return int64(chainIndexHeaderSize + header.blockCount*chainIndexBlockEntrySize + index*chainIndexTxEntrySize)
}

func chainIndexExpectedSize(header chainIndexHeader) (uint64, error) {
	blockBytes, overflow := checkedMul(header.blockCount, chainIndexBlockEntrySize)
	if overflow {
		return 0, fmt.Errorf("snapshots: chain-index block table size overflows")
	}
	txBytes, overflow := checkedMul(header.txCount, chainIndexTxEntrySize)
	if overflow {
		return 0, fmt.Errorf("snapshots: chain-index tx table size overflows")
	}
	total, overflow := checkedAdd(chainIndexHeaderSize, blockBytes)
	if overflow {
		return 0, fmt.Errorf("snapshots: chain-index file size overflows")
	}
	total, overflow = checkedAdd(total, txBytes)
	if overflow {
		return 0, fmt.Errorf("snapshots: chain-index file size overflows")
	}
	return total, nil
}

func checkedMul(a uint64, b uint64) (uint64, bool) {
	if a == 0 || b == 0 {
		return 0, false
	}
	if a > ^uint64(0)/b {
		return 0, true
	}
	return a * b, false
}

func checkedAdd(a uint64, b uint64) (uint64, bool) {
	out := a + b
	return out, out < a
}

type chainBlockSegmentRange struct {
	from uint64
	to   uint64
}
