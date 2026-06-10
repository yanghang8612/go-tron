package snapshots

import (
	"bufio"
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"

	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/rawdb/etl"
	"github.com/tronprotocol/go-tron/core/types"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	"google.golang.org/protobuf/proto"
)

const (
	ChainFreezerSegmentVersion = 1
	chainFreezerHeaderSize     = 8 + 8 + 8 + 8
)

var chainFreezerMagic = [8]byte{'g', 't', 'f', 'r', 'z', 'r', '1', '\n'}

type ChainFreezerAncientStore interface {
	rawdb.AncientReader
	rawdb.AncientWriter
}

type RestoreVerifiedChainFreezerResult struct {
	Verification         ManifestVerificationReport
	HasRange             bool
	FromBlock            uint64
	ToBlock              uint64
	BlocksRestored       uint64
	BlockIndexesRestored uint64
	TxIndexesRestored    uint64
	TxInfosRestored      uint64
	ColdIndexSegments    uint64
}

type chainFreezerRow struct {
	blockNum     uint64
	blockRaw     []byte
	txInfosRaw   []byte
	stateRootRaw []byte
}

type RestoreChainFreezerOptions struct {
	IndexWriter       ethdb.KeyValueWriter
	ProgressWriter    ethdb.KeyValueWriter
	PreferColdIndexes bool
	ETL               RestoreETLOptions
}

type RestoreChainFreezerSegmentResult struct {
	BlocksRestored       uint64
	BlockIndexesRestored uint64
	TxIndexesRestored    uint64
	TxInfosRestored      uint64
	AlreadyInstalled     bool
}

func ChainFreezerSegmentPath(fromBlock, toBlock uint64) string {
	return fmt.Sprintf("chain/freezer-%d-%d.seg", fromBlock, toBlock)
}

func BuildChainFreezerSegmentFromAncient(reader rawdb.AncientReader, dir, relPath string, fromBlock, toBlock uint64) (SegmentRef, error) {
	if reader == nil {
		return SegmentRef{}, errors.New("snapshots: nil ancient reader")
	}
	if relPath == "" {
		relPath = ChainFreezerSegmentPath(fromBlock, toBlock)
	}
	ref := SegmentRef{
		Dataset:   SegmentDatasetChainFreezer,
		Kind:      SegmentChainFreezer,
		FromTxNum: fromBlock,
		ToTxNum:   toBlock,
		Path:      relPath,
	}
	count, err := chainFreezerRowCount(fromBlock, toBlock)
	if err != nil {
		return SegmentRef{}, err
	}
	if err := validateSegmentRef(ref); err != nil {
		return SegmentRef{}, err
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
	if err := writeChainFreezerHeader(buf, fromBlock, toBlock, count); err != nil {
		_ = tmp.Close()
		return SegmentRef{}, err
	}
	for n := fromBlock; n <= toBlock; n++ {
		blockRaw, err := reader.Ancient(rawdb.AncientBlocksTable, n)
		if err != nil {
			_ = tmp.Close()
			return SegmentRef{}, fmt.Errorf("snapshots: read ancient %s block %d: %w", rawdb.AncientBlocksTable, n, err)
		}
		txInfosRaw, err := reader.Ancient(rawdb.AncientTxInfosTable, n)
		if err != nil {
			_ = tmp.Close()
			return SegmentRef{}, fmt.Errorf("snapshots: read ancient %s block %d: %w", rawdb.AncientTxInfosTable, n, err)
		}
		stateRootRaw, err := reader.Ancient(rawdb.AncientStateRootsTable, n)
		if err != nil {
			_ = tmp.Close()
			return SegmentRef{}, fmt.Errorf("snapshots: read ancient %s block %d: %w", rawdb.AncientStateRootsTable, n, err)
		}
		if err := writeChainFreezerBytes(buf, blockRaw); err != nil {
			_ = tmp.Close()
			return SegmentRef{}, fmt.Errorf("snapshots: write freezer block %d body: %w", n, err)
		}
		if err := writeChainFreezerBytes(buf, txInfosRaw); err != nil {
			_ = tmp.Close()
			return SegmentRef{}, fmt.Errorf("snapshots: write freezer block %d tx infos: %w", n, err)
		}
		if err := writeChainFreezerBytes(buf, stateRootRaw); err != nil {
			_ = tmp.Close()
			return SegmentRef{}, fmt.Errorf("snapshots: write freezer block %d state root: %w", n, err)
		}
		if n == toBlock {
			break
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

func CheckChainFreezerSegment(dir string, ref SegmentRef) error {
	if err := validateSegmentRef(ref); err != nil {
		return err
	}
	if err := checkSegmentFileMetadata(dir, ref, false); err != nil {
		return err
	}
	expectedRows, err := chainFreezerRowCount(ref.FromTxNum, ref.ToTxNum)
	if err != nil {
		return err
	}
	var rows uint64
	if err := iterateChainFreezerSegmentRows(dir, ref, func(row chainFreezerRow) error {
		if row.blockNum != ref.FromTxNum+rows {
			return fmt.Errorf("snapshots: chain-freezer segment %q block %d out of order at row %d", ref.Path, row.blockNum, rows)
		}
		rows++
		return nil
	}); err != nil {
		return err
	}
	if rows != expectedRows {
		return fmt.Errorf("snapshots: chain-freezer segment %q rows %d, want %d", ref.Path, rows, expectedRows)
	}
	return nil
}

var _ rawdb.AncientReader = (*Manager)(nil)

// Ancient reads chain-freezer snapshot rows through the AncientReader shape so
// ChainDB can fall back to verified snapshot files after local freezer rows have
// been hidden by a virtual tail.
func (m *Manager) Ancient(kind string, number uint64) ([]byte, error) {
	if !isChainFreezerAncientKind(kind) {
		return nil, rawdb.ErrNotInAncient
	}
	manifest, err := m.currentManifest()
	if err != nil {
		return nil, err
	}
	for _, ref := range chainFreezerRefs(manifest) {
		if number < ref.FromTxNum || number > ref.ToTxNum {
			continue
		}
		if accessorRef, ok := chainFreezerAccessorRefForFreezer(manifest, ref); ok {
			row, ok, err := readChainFreezerSegmentRowWithAccessor(m.dir, ref, accessorRef, number)
			if err != nil {
				return nil, err
			}
			if ok {
				return chainFreezerAncientPayload(row, kind), nil
			}
		}
		row, ok, err := readChainFreezerSegmentRow(m.dir, ref, number)
		if err != nil {
			return nil, err
		}
		if !ok {
			continue
		}
		return chainFreezerAncientPayload(row, kind), nil
	}
	return nil, rawdb.ErrNotInAncient
}

// AncientRange returns sequential chain-freezer snapshot rows. It mirrors the
// freezer contract loosely: the first missing row is an error, later missing
// rows stop the returned range.
func (m *Manager) AncientRange(kind string, start, count, maxBytes uint64) ([][]byte, error) {
	if count == 0 {
		return nil, nil
	}
	var (
		out        [][]byte
		totalBytes uint64
	)
	for i := uint64(0); i < count; i++ {
		number := start + i
		if number < start {
			break
		}
		data, err := m.Ancient(kind, number)
		if err != nil {
			if len(out) > 0 && errors.Is(err, rawdb.ErrNotInAncient) {
				break
			}
			return nil, err
		}
		if maxBytes > 0 && len(out) > 0 && totalBytes+uint64(len(data)) > maxBytes {
			break
		}
		out = append(out, data)
		totalBytes += uint64(len(data))
	}
	if len(out) == 0 {
		return nil, rawdb.ErrNotInAncient
	}
	return out, nil
}

// AncientCount returns the highest chain-freezer snapshot block plus one for
// the requested ancient table. It is a read-side boundary, not an append target.
func (m *Manager) AncientCount(kind string) (uint64, error) {
	if !isChainFreezerAncientKind(kind) {
		return 0, nil
	}
	manifest, err := m.currentManifest()
	if err != nil || manifest == nil {
		return 0, err
	}
	var count uint64
	for _, ref := range chainFreezerRefs(manifest) {
		tail := tailAfterInclusiveBlock(ref.ToTxNum)
		if tail > count {
			count = tail
		}
	}
	return count, nil
}

func (m *Manager) HasAncient(kind string, number uint64) (bool, error) {
	if !isChainFreezerAncientKind(kind) {
		return false, nil
	}
	manifest, err := m.currentManifest()
	if err != nil || manifest == nil {
		return false, err
	}
	for _, ref := range chainFreezerRefs(manifest) {
		if number >= ref.FromTxNum && number <= ref.ToTxNum {
			return true, nil
		}
	}
	return false, nil
}

func RestoreChainFreezerFromVerifiedManifest(store ChainFreezerAncientStore, dir string, expected ChainIdentity) (*RestoreVerifiedChainFreezerResult, error) {
	return RestoreChainFreezerFromVerifiedManifestWithOptions(store, dir, expected, RestoreChainFreezerOptions{})
}

func RestoreChainFreezerFromVerifiedManifestWithOptions(store ChainFreezerAncientStore, dir string, expected ChainIdentity, opts RestoreChainFreezerOptions) (*RestoreVerifiedChainFreezerResult, error) {
	if store == nil {
		return nil, errors.New("snapshots: nil ancient store")
	}
	report, err := VerifyRemoteManifestFiles(dir, expected)
	if err != nil {
		return nil, err
	}
	manifest, err := LoadProductionManifest(dir)
	if err != nil {
		return nil, err
	}
	return restoreChainFreezerSegmentsFromManifest(store, dir, manifest, *report, opts)
}

func RestoreChainFreezerFromVerifiedCatalog(store ChainFreezerAncientStore, dir string, expected ChainIdentity, trustedKeys []ed25519.PublicKey) (*RestoreVerifiedChainFreezerResult, error) {
	return RestoreChainFreezerFromVerifiedCatalogWithOptions(store, dir, expected, trustedKeys, RestoreChainFreezerOptions{})
}

func RestoreChainFreezerFromVerifiedCatalogWithOptions(store ChainFreezerAncientStore, dir string, expected ChainIdentity, trustedKeys []ed25519.PublicKey, opts RestoreChainFreezerOptions) (*RestoreVerifiedChainFreezerResult, error) {
	if store == nil {
		return nil, errors.New("snapshots: nil ancient store")
	}
	_, report, err := VerifySignedSnapshotCatalog(dir, expected, trustedKeys)
	if err != nil {
		return nil, err
	}
	manifest, err := LoadProductionManifest(dir)
	if err != nil {
		return nil, err
	}
	return restoreChainFreezerSegmentsFromManifest(store, dir, manifest, *report, opts)
}

func restoreChainFreezerSegmentsFromManifest(store ChainFreezerAncientStore, dir string, manifest *Manifest, report ManifestVerificationReport, opts RestoreChainFreezerOptions) (*RestoreVerifiedChainFreezerResult, error) {
	if store == nil {
		return nil, errors.New("snapshots: nil ancient store")
	}
	if manifest == nil {
		return nil, errors.New("snapshots: nil manifest")
	}
	result := &RestoreVerifiedChainFreezerResult{Verification: report}
	for _, ref := range manifest.Segments {
		if ref.Kind != SegmentChainFreezer {
			continue
		}
		segmentOpts := opts
		if opts.PreferColdIndexes {
			if indexRef, ok := chainIndexRefForFreezer(manifest, ref); ok {
				if err := VerifyChainIndexSegmentAgainstChainFreezer(dir, indexRef, ref); err != nil {
					return nil, err
				}
				segmentOpts.IndexWriter = nil
				result.ColdIndexSegments++
			}
		}
		restored, err := RestoreChainFreezerSegmentToAncientWithOptions(store, dir, ref, segmentOpts)
		if err != nil {
			return nil, err
		}
		if !result.HasRange {
			result.HasRange = true
			result.FromBlock = ref.FromTxNum
		}
		result.ToBlock = ref.ToTxNum
		result.BlocksRestored += restored.BlocksRestored
		result.BlockIndexesRestored += restored.BlockIndexesRestored
		result.TxIndexesRestored += restored.TxIndexesRestored
		result.TxInfosRestored += restored.TxInfosRestored
	}
	return result, nil
}

func RestoreChainFreezerSegmentToAncient(store ChainFreezerAncientStore, dir string, ref SegmentRef) (uint64, error) {
	result, err := RestoreChainFreezerSegmentToAncientWithOptions(store, dir, ref, RestoreChainFreezerOptions{})
	if err != nil {
		return result.BlocksRestored, err
	}
	return result.BlocksRestored, nil
}

func RestoreChainFreezerSegmentToAncientWithOptions(store ChainFreezerAncientStore, dir string, ref SegmentRef, opts RestoreChainFreezerOptions) (RestoreChainFreezerSegmentResult, error) {
	var result RestoreChainFreezerSegmentResult
	if store == nil {
		return result, errors.New("snapshots: nil ancient store")
	}
	if err := CheckChainFreezerSegment(dir, ref); err != nil {
		return result, err
	}
	expectedHead := ref.ToTxNum + 1
	if expectedHead == 0 {
		return result, fmt.Errorf("snapshots: chain-freezer segment %q toBlock overflows head", ref.Path)
	}
	heads := make([]uint64, 0, 3)
	for _, table := range []string{rawdb.AncientBlocksTable, rawdb.AncientTxInfosTable, rawdb.AncientStateRootsTable} {
		head, err := store.AncientCount(table)
		if err != nil {
			return result, err
		}
		heads = append(heads, head)
	}

	switch {
	case heads[0] == ref.FromTxNum && heads[1] == ref.FromTxNum && heads[2] == ref.FromTxNum:
		expectedRows, err := chainFreezerRowCount(ref.FromTxNum, ref.ToTxNum)
		if err != nil {
			return result, err
		}
		var appended uint64
		if _, err := store.ModifyAncients(func(op rawdb.AncientWriteOp) error {
			appended = 0
			if err := iterateChainFreezerSegmentRows(dir, ref, func(row chainFreezerRow) error {
				wantBlock := ref.FromTxNum + appended
				if row.blockNum != wantBlock {
					return fmt.Errorf("snapshots: chain-freezer segment %q block %d, want %d", ref.Path, row.blockNum, wantBlock)
				}
				if err := op.AppendRaw(rawdb.AncientBlocksTable, row.blockNum, row.blockRaw); err != nil {
					return err
				}
				if err := op.AppendRaw(rawdb.AncientTxInfosTable, row.blockNum, row.txInfosRaw); err != nil {
					return err
				}
				if err := op.AppendRaw(rawdb.AncientStateRootsTable, row.blockNum, row.stateRootRaw); err != nil {
					return err
				}
				appended++
				return nil
			}); err != nil {
				return err
			}
			if appended != expectedRows {
				return fmt.Errorf("snapshots: chain-freezer segment %q rows %d, want %d", ref.Path, appended, expectedRows)
			}
			return nil
		}); err != nil {
			result.BlocksRestored = appended
			return result, err
		}
		if err := store.Sync(); err != nil {
			result.BlocksRestored = appended
			return result, err
		}
		result.BlocksRestored = appended
	case heads[0] == expectedHead && heads[1] == expectedHead && heads[2] == expectedHead:
		if err := verifyChainFreezerSegmentAlreadyInstalled(store, dir, ref); err != nil {
			return result, err
		}
		result.AlreadyInstalled = true
	default:
		return result, fmt.Errorf("snapshots: chain-freezer install requires ancient heads all %d or all %d, got %s=%d %s=%d %s=%d",
			ref.FromTxNum, expectedHead,
			rawdb.AncientBlocksTable, heads[0],
			rawdb.AncientTxInfosTable, heads[1],
			rawdb.AncientStateRootsTable, heads[2])
	}
	if opts.ProgressWriter != nil {
		if err := writeChainFreezerStageProgress(opts.ProgressWriter, ref.ToTxNum); err != nil {
			return result, err
		}
	}
	if opts.IndexWriter != nil {
		indexes, err := RestoreChainFreezerIndexesWithOptions(opts.IndexWriter, dir, ref, opts.ETL)
		if err != nil {
			return result, err
		}
		result.BlockIndexesRestored = indexes.BlockIndexesRestored
		result.TxIndexesRestored = indexes.TxIndexesRestored
		result.TxInfosRestored = indexes.TxInfosRestored
	}
	return result, nil
}

func writeChainFreezerStageProgress(db ethdb.KeyValueWriter, blockNum uint64) error {
	if reader, ok := db.(ethdb.KeyValueReader); ok {
		current, ok, err := rawdb.ReadStageProgress(reader, rawdb.StageChainFreezer)
		if err != nil {
			return err
		}
		if ok && current >= blockNum {
			return nil
		}
	}
	return rawdb.WriteStageProgress(db, rawdb.StageChainFreezer, blockNum)
}

func RestoreChainFreezerIndexes(db ethdb.KeyValueWriter, dir string, ref SegmentRef) (RestoreChainFreezerSegmentResult, error) {
	return RestoreChainFreezerIndexesWithOptions(db, dir, ref, RestoreETLOptions{})
}

func RestoreChainFreezerIndexesWithOptions(db ethdb.KeyValueWriter, dir string, ref SegmentRef, opts RestoreETLOptions) (RestoreChainFreezerSegmentResult, error) {
	var result RestoreChainFreezerSegmentResult
	if db == nil {
		return result, errors.New("snapshots: nil chain-freezer index writer")
	}
	if err := CheckChainFreezerSegment(dir, ref); err != nil {
		return result, err
	}
	collector, err := etl.NewCollector(opts.collectorOptions())
	if err != nil {
		return result, fmt.Errorf("snapshots: create chain-freezer index restore ETL collector: %w", err)
	}
	defer collector.Close()
	restoreWriter := ethdb.KeyValueWriter(collector)

	err = iterateChainFreezerSegmentRows(dir, ref, func(row chainFreezerRow) error {
		counts, err := restoreChainFreezerIndexesForRow(restoreWriter, row)
		if err != nil {
			return err
		}
		result.BlockIndexesRestored += counts.BlockIndexesRestored
		result.TxIndexesRestored += counts.TxIndexesRestored
		result.TxInfosRestored += counts.TxInfosRestored
		return nil
	})
	if err != nil {
		return result, err
	}
	if _, err := collector.Load(db); err != nil {
		return result, fmt.Errorf("snapshots: load chain-freezer index restore ETL collector: %w", err)
	}
	return result, nil
}

func restoreChainFreezerIndexesForRow(db ethdb.KeyValueWriter, row chainFreezerRow) (RestoreChainFreezerSegmentResult, error) {
	var result RestoreChainFreezerSegmentResult
	block, err := types.UnmarshalBlock(row.blockRaw)
	if err != nil {
		return result, fmt.Errorf("snapshots: decode chain-freezer block %d: %w", row.blockNum, err)
	}
	if block.Number() != row.blockNum {
		return result, fmt.Errorf("snapshots: chain-freezer row %d contains block number %d", row.blockNum, block.Number())
	}
	blockHash := block.Hash()
	if err := rawdb.WriteBlockNumber(db, blockHash, row.blockNum); err != nil {
		return result, err
	}
	result.BlockIndexesRestored++
	for _, tx := range block.Transactions() {
		txHash := tx.Hash()
		if err := rawdb.WriteTransactionIndex(db, txHash[:], row.blockNum); err != nil {
			return result, err
		}
		result.TxIndexesRestored++
	}
	if len(row.txInfosRaw) == 0 {
		return result, nil
	}
	var ret corepb.TransactionRet
	if err := proto.Unmarshal(row.txInfosRaw, &ret); err != nil {
		return result, fmt.Errorf("snapshots: decode chain-freezer tx infos for block %d: %w", row.blockNum, err)
	}
	if ret.BlockNumber != 0 && uint64(ret.BlockNumber) != row.blockNum {
		return result, fmt.Errorf("snapshots: chain-freezer tx infos row %d contains block number %d", row.blockNum, ret.BlockNumber)
	}
	for _, info := range ret.Transactioninfo {
		if info == nil || len(info.Id) == 0 {
			continue
		}
		if err := rawdb.WriteTransactionInfo(db, info.Id, info); err != nil {
			return result, err
		}
		result.TxInfosRestored++
	}
	return result, nil
}

func verifyChainFreezerSegmentAlreadyInstalled(store rawdb.AncientReader, dir string, ref SegmentRef) error {
	return iterateChainFreezerSegmentRows(dir, ref, func(row chainFreezerRow) error {
		for _, item := range []struct {
			table string
			want  []byte
		}{
			{table: rawdb.AncientBlocksTable, want: row.blockRaw},
			{table: rawdb.AncientTxInfosTable, want: row.txInfosRaw},
			{table: rawdb.AncientStateRootsTable, want: row.stateRootRaw},
		} {
			got, err := store.Ancient(item.table, row.blockNum)
			if err != nil {
				return err
			}
			if string(got) != string(item.want) {
				return fmt.Errorf("snapshots: installed chain-freezer %s[%d] does not match segment %q", item.table, row.blockNum, ref.Path)
			}
		}
		return nil
	})
}

func iterateChainFreezerSegmentRows(dir string, ref SegmentRef, fn func(chainFreezerRow) error) error {
	if err := validateSegmentRef(ref); err != nil {
		return err
	}
	if fn == nil {
		return errors.New("snapshots: nil chain-freezer row iterator")
	}
	file, err := os.Open(filepath.Join(dir, ref.Path))
	if err != nil {
		return err
	}
	defer file.Close()
	reader := bufio.NewReader(file)
	fromBlock, toBlock, count, err := readChainFreezerHeader(reader)
	if err != nil {
		return err
	}
	if fromBlock != ref.FromTxNum || toBlock != ref.ToTxNum {
		return fmt.Errorf("snapshots: chain-freezer segment %q range [%d,%d], want [%d,%d]", ref.Path, fromBlock, toBlock, ref.FromTxNum, ref.ToTxNum)
	}
	expectedRows, err := chainFreezerRowCount(ref.FromTxNum, ref.ToTxNum)
	if err != nil {
		return err
	}
	if count != expectedRows {
		return fmt.Errorf("snapshots: chain-freezer segment %q rows %d, want %d", ref.Path, count, expectedRows)
	}
	for i := uint64(0); i < count; i++ {
		row := chainFreezerRow{blockNum: fromBlock + i}
		if row.blockRaw, err = readChainFreezerBytes(reader); err != nil {
			return fmt.Errorf("snapshots: read chain-freezer block %d body: %w", row.blockNum, err)
		}
		if row.txInfosRaw, err = readChainFreezerBytes(reader); err != nil {
			return fmt.Errorf("snapshots: read chain-freezer block %d tx infos: %w", row.blockNum, err)
		}
		if row.stateRootRaw, err = readChainFreezerBytes(reader); err != nil {
			return fmt.Errorf("snapshots: read chain-freezer block %d state root: %w", row.blockNum, err)
		}
		if err := fn(row); err != nil {
			return err
		}
	}
	if _, err := reader.Peek(1); err != io.EOF {
		if err == nil {
			return fmt.Errorf("snapshots: chain-freezer segment %q has trailing bytes", ref.Path)
		}
		return err
	}
	return nil
}

func readChainFreezerSegmentRow(dir string, ref SegmentRef, blockNum uint64) (chainFreezerRow, bool, error) {
	var (
		out   chainFreezerRow
		found bool
	)
	err := iterateChainFreezerSegmentRows(dir, ref, func(row chainFreezerRow) error {
		if row.blockNum != blockNum {
			return nil
		}
		out = chainFreezerRow{
			blockNum:     row.blockNum,
			blockRaw:     append([]byte(nil), row.blockRaw...),
			txInfosRaw:   append([]byte(nil), row.txInfosRaw...),
			stateRootRaw: append([]byte(nil), row.stateRootRaw...),
		}
		found = true
		return nil
	})
	return out, found, err
}

func chainFreezerAncientPayload(row chainFreezerRow, kind string) []byte {
	switch kind {
	case rawdb.AncientBlocksTable:
		return append([]byte(nil), row.blockRaw...)
	case rawdb.AncientTxInfosTable:
		return append([]byte(nil), row.txInfosRaw...)
	case rawdb.AncientStateRootsTable:
		return append([]byte(nil), row.stateRootRaw...)
	default:
		return nil
	}
}

func isChainFreezerAncientKind(kind string) bool {
	return kind == rawdb.AncientBlocksTable ||
		kind == rawdb.AncientTxInfosTable ||
		kind == rawdb.AncientStateRootsTable
}

func chainFreezerRefs(manifest *Manifest) []SegmentRef {
	if manifest == nil {
		return nil
	}
	refs := make([]SegmentRef, 0)
	for _, ref := range manifest.Segments {
		if ref.Kind != SegmentChainFreezer || ref.normalizedDataset() != SegmentDatasetChainFreezer {
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

func writeChainFreezerHeader(w io.Writer, fromBlock, toBlock, count uint64) error {
	var header [chainFreezerHeaderSize]byte
	copy(header[0:8], chainFreezerMagic[:])
	binary.BigEndian.PutUint64(header[8:16], fromBlock)
	binary.BigEndian.PutUint64(header[16:24], toBlock)
	binary.BigEndian.PutUint64(header[24:32], count)
	_, err := w.Write(header[:])
	return err
}

func readChainFreezerHeader(r io.Reader) (uint64, uint64, uint64, error) {
	var header [chainFreezerHeaderSize]byte
	if _, err := io.ReadFull(r, header[:]); err != nil {
		if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
			return 0, 0, 0, io.ErrUnexpectedEOF
		}
		return 0, 0, 0, err
	}
	if headerMagic := [8]byte(header[0:8]); headerMagic != chainFreezerMagic {
		return 0, 0, 0, errors.New("snapshots: invalid chain-freezer segment magic")
	}
	return binary.BigEndian.Uint64(header[8:16]),
		binary.BigEndian.Uint64(header[16:24]),
		binary.BigEndian.Uint64(header[24:32]),
		nil
}

func writeChainFreezerBytes(w io.Writer, data []byte) error {
	if uint64(len(data)) > uint64(^uint32(0)) {
		return fmt.Errorf("snapshots: chain-freezer blob length %d exceeds uint32", len(data))
	}
	var rawLen [4]byte
	binary.BigEndian.PutUint32(rawLen[:], uint32(len(data)))
	if _, err := w.Write(rawLen[:]); err != nil {
		return err
	}
	if len(data) == 0 {
		return nil
	}
	_, err := w.Write(data)
	return err
}

func readChainFreezerBytes(r io.Reader) ([]byte, error) {
	var rawLen [4]byte
	if _, err := io.ReadFull(r, rawLen[:]); err != nil {
		if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
			return nil, io.ErrUnexpectedEOF
		}
		return nil, err
	}
	length := binary.BigEndian.Uint32(rawLen[:])
	if length == 0 {
		return nil, nil
	}
	out := make([]byte, int(length))
	if _, err := io.ReadFull(r, out); err != nil {
		if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
			return nil, io.ErrUnexpectedEOF
		}
		return nil, err
	}
	return out, nil
}

func chainFreezerRowCount(fromBlock, toBlock uint64) (uint64, error) {
	if toBlock < fromBlock {
		return 0, fmt.Errorf("snapshots: chain-freezer range [%d,%d] is inverted", fromBlock, toBlock)
	}
	count := toBlock - fromBlock + 1
	if count == 0 {
		return 0, fmt.Errorf("snapshots: chain-freezer range [%d,%d] is too large", fromBlock, toBlock)
	}
	return count, nil
}

type countingWriter struct {
	w io.Writer
	n uint64
}

func (w *countingWriter) Write(p []byte) (int, error) {
	n, err := w.w.Write(p)
	w.n += uint64(n)
	return n, err
}
