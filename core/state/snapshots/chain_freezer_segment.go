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
	"github.com/tronprotocol/go-tron/common"
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

type validatedChainFreezerRow struct {
	block *types.Block
	txRet *corepb.TransactionRet
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
		row := chainFreezerRow{
			blockNum:     n,
			blockRaw:     blockRaw,
			txInfosRaw:   txInfosRaw,
			stateRootRaw: stateRootRaw,
		}
		if _, err := validateChainFreezerRowPayload(row, "chain-freezer segment build"); err != nil {
			_ = tmp.Close()
			return SegmentRef{}, err
		}
		if err := writeChainFreezerBytes(buf, row.blockRaw); err != nil {
			_ = tmp.Close()
			return SegmentRef{}, fmt.Errorf("snapshots: write freezer block %d body: %w", n, err)
		}
		if err := writeChainFreezerBytes(buf, row.txInfosRaw); err != nil {
			_ = tmp.Close()
			return SegmentRef{}, fmt.Errorf("snapshots: write freezer block %d tx infos: %w", n, err)
		}
		if err := writeChainFreezerBytes(buf, row.stateRootRaw); err != nil {
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
		if _, err := validateChainFreezerRowPayload(row, "chain-freezer segment check"); err != nil {
			return err
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

func validateChainFreezerRowPayload(row chainFreezerRow, context string) (validatedChainFreezerRow, error) {
	var out validatedChainFreezerRow
	if context == "" {
		context = "chain-freezer segment"
	}
	block, err := types.UnmarshalBlock(row.blockRaw)
	if err != nil {
		return out, fmt.Errorf("snapshots: decode %s block %d: %w", context, row.blockNum, err)
	}
	if block.Number() != row.blockNum {
		return out, fmt.Errorf("snapshots: %s row %d contains block number %d", context, row.blockNum, block.Number())
	}
	if len(row.stateRootRaw) != 0 && len(row.stateRootRaw) != common.HashLength {
		return out, fmt.Errorf("snapshots: %s row %d state root length %d, want 0 or %d", context, row.blockNum, len(row.stateRootRaw), common.HashLength)
	}
	out.block = block
	if len(row.txInfosRaw) == 0 {
		if len(block.Transactions()) != 0 {
			return out, fmt.Errorf("snapshots: %s row %d missing transaction info coverage for %d transactions", context, row.blockNum, len(block.Transactions()))
		}
		return out, nil
	}
	ret := new(corepb.TransactionRet)
	if err := proto.Unmarshal(row.txInfosRaw, ret); err != nil {
		return out, fmt.Errorf("snapshots: decode %s tx infos for block %d: %w", context, row.blockNum, err)
	}
	if ret.BlockNumber != 0 && (ret.BlockNumber < 0 || uint64(ret.BlockNumber) != row.blockNum) {
		return out, fmt.Errorf("snapshots: %s tx infos row %d contains block number %d", context, row.blockNum, ret.BlockNumber)
	}
	if err := rawdb.ValidateTransactionInfosForBlock(row.blockNum, block.Transactions(), ret.Transactioninfo, context); err != nil {
		return out, err
	}
	out.txRet = ret
	return out, nil
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
	if !isChainFreezerAncientKind(kind) {
		return nil, rawdb.ErrNotInAncient
	}
	manifest, err := m.currentManifest()
	if err != nil {
		return nil, err
	}
	if manifest == nil {
		return nil, rawdb.ErrNotInAncient
	}
	refs := chainFreezerRefs(manifest)
	sortSegmentRefsAscending(refs)
	var out [][]byte
	var totalBytes uint64
	next := start
	remaining := count
	for remaining > 0 {
		ref, ok := chainFreezerRefContaining(refs, next)
		if !ok {
			break
		}
		rows, read, bytesRead, hitLimit, err := m.ancientRangeFromChainFreezerSegment(manifest, ref, kind, next, remaining, maxBytes, totalBytes, len(out) > 0)
		if err != nil {
			return nil, err
		}
		out = append(out, rows...)
		totalBytes += bytesRead
		if hitLimit || read == 0 {
			break
		}
		remaining -= read
		prev := next
		next += read
		if next < prev {
			break
		}
	}
	if len(out) > 0 {
		return out, nil
	}
	return nil, rawdb.ErrNotInAncient
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
	var (
		count      uint64
		highestRef SegmentRef
		hasHighest bool
	)
	for _, ref := range chainFreezerRefs(manifest) {
		tail := tailAfterInclusiveBlock(ref.ToTxNum)
		if tail > count {
			count = tail
			highestRef = ref
			hasHighest = true
		}
	}
	if !hasHighest {
		return 0, nil
	}
	if _, err := m.Ancient(kind, highestRef.ToTxNum); err != nil {
		return 0, err
	}
	return count, nil
}

func (m *Manager) HasAncient(kind string, number uint64) (bool, error) {
	if !isChainFreezerAncientKind(kind) {
		return false, nil
	}
	_, err := m.Ancient(kind, number)
	if err == nil {
		return true, nil
	}
	if errors.Is(err, rawdb.ErrNotInAncient) {
		return false, nil
	}
	return false, err
}

func (m *Manager) ChainFreezerRangeCovered(fromBlock, toBlock uint64) (bool, error) {
	if m == nil {
		return false, nil
	}
	if toBlock < fromBlock {
		return false, fmt.Errorf("snapshots: chain-freezer coverage range [%d,%d] is inverted", fromBlock, toBlock)
	}
	manifest, err := m.currentManifest()
	if err != nil || manifest == nil {
		return false, err
	}
	refs := chainFreezerRefs(manifest)
	sort.Slice(refs, func(i, j int) bool {
		if refs[i].FromTxNum != refs[j].FromTxNum {
			return refs[i].FromTxNum < refs[j].FromTxNum
		}
		if refs[i].ToTxNum != refs[j].ToTxNum {
			return refs[i].ToTxNum < refs[j].ToTxNum
		}
		return refs[i].Path < refs[j].Path
	})
	next := fromBlock
	for _, ref := range refs {
		if ref.ToTxNum < next {
			continue
		}
		if ref.FromTxNum > next {
			return false, nil
		}
		if err := CheckChainFreezerSegment(m.dir, ref); err != nil {
			return false, err
		}
		if accessorRef, ok := chainFreezerAccessorRefForFreezer(manifest, ref); ok {
			if err := VerifyChainFreezerAccessorSegmentAgainstChainFreezer(m.dir, accessorRef, ref); err != nil {
				return false, err
			}
		}
		if ref.ToTxNum >= toBlock {
			return true, nil
		}
		if ref.ToTxNum == ^uint64(0) {
			return false, nil
		}
		next = ref.ToTxNum + 1
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
				if _, err := validateChainFreezerRowPayload(row, "chain-freezer segment restore"); err != nil {
					return err
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
		if err := writeChainFreezerStageProgress(opts.ProgressWriter, dir, ref); err != nil {
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

func writeChainFreezerStageProgress(db ethdb.KeyValueWriter, dir string, ref SegmentRef) error {
	blockNum := ref.ToTxNum
	blockHash, err := chainFreezerSegmentBlockHash(dir, ref, blockNum)
	if err != nil {
		return err
	}
	if reader, ok := db.(ethdb.KeyValueReader); ok {
		current, ok, err := rawdb.ReadStageProgressRow(reader, rawdb.StageChainFreezer)
		if err != nil {
			return err
		}
		if ok && current.BlockNum > blockNum {
			return nil
		}
		if ok && current.BlockNum == blockNum && current.HasBlockHash {
			if current.BlockHash != blockHash {
				return fmt.Errorf("snapshots: ChainFreezer stage %d hash %x does not match segment hash %x", blockNum, current.BlockHash, blockHash)
			}
			return nil
		}
	}
	return rawdb.WriteStageProgressWithHash(db, rawdb.StageChainFreezer, blockNum, blockHash)
}

func chainFreezerSegmentBlockHash(dir string, ref SegmentRef, blockNum uint64) (common.Hash, error) {
	row, ok, err := readChainFreezerSegmentRow(dir, ref, blockNum)
	if err != nil {
		return common.Hash{}, err
	}
	if !ok {
		return common.Hash{}, fmt.Errorf("snapshots: chain-freezer segment %q missing block %d", ref.Path, blockNum)
	}
	verified, err := validateChainFreezerRowPayload(row, "chain-freezer stage progress")
	if err != nil {
		return common.Hash{}, err
	}
	return verified.block.Hash(), nil
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
	verified, err := validateChainFreezerRowPayload(row, "chain-freezer index restore")
	if err != nil {
		return result, err
	}
	block := verified.block
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
	if verified.txRet == nil {
		return result, nil
	}
	for _, info := range verified.txRet.Transactioninfo {
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

func (m *Manager) ancientRangeFromChainFreezerSegment(manifest *Manifest, ref SegmentRef, kind string, start, count, maxBytes, totalBytes uint64, haveRows bool) ([][]byte, uint64, uint64, bool, error) {
	if count == 0 || start < ref.FromTxNum || start > ref.ToTxNum {
		return nil, 0, 0, false, nil
	}
	offset, ok, err := m.chainFreezerSegmentRowOffset(manifest, ref, start)
	if err != nil || !ok {
		return nil, 0, 0, false, err
	}
	file, err := os.Open(filepath.Join(m.dir, ref.Path))
	if err != nil {
		return nil, 0, 0, false, err
	}
	defer file.Close()
	limit := ref.ToTxNum - start + 1
	if count < limit {
		limit = count
	}
	rows := make([][]byte, 0, limit)
	var bytesRead uint64
	blockNum := start
	for i := uint64(0); i < limit; i++ {
		row, nextOffset, err := readChainFreezerSegmentRowAtWithNext(file, offset, blockNum)
		if err != nil {
			return nil, 0, 0, false, err
		}
		payload := chainFreezerAncientPayload(row, kind)
		payloadLen := uint64(len(payload))
		if maxBytes > 0 && (haveRows || len(rows) > 0) && totalBytes+bytesRead+payloadLen > maxBytes {
			return rows, i, bytesRead, true, nil
		}
		rows = append(rows, payload)
		bytesRead += payloadLen
		offset = nextOffset
		blockNum++
		if blockNum == 0 {
			return rows, i + 1, bytesRead, false, nil
		}
	}
	return rows, limit, bytesRead, false, nil
}

func (m *Manager) chainFreezerSegmentRowOffset(manifest *Manifest, ref SegmentRef, blockNum uint64) (uint64, bool, error) {
	if blockNum < ref.FromTxNum || blockNum > ref.ToTxNum {
		return 0, false, nil
	}
	if accessorRef, ok := chainFreezerAccessorRefForFreezer(manifest, ref); ok {
		accessor, err := OpenChainFreezerAccessorSegment(m.dir, accessorRef)
		if err != nil {
			return 0, false, err
		}
		offset, found, lookupErr := accessor.RowOffset(blockNum)
		closeErr := accessor.Close()
		if lookupErr != nil {
			return 0, false, lookupErr
		}
		if closeErr != nil {
			return 0, false, closeErr
		}
		return offset, found, nil
	}
	offsets, err := chainFreezerRowOffsets(m.dir, ref)
	if err != nil {
		return 0, false, err
	}
	ordinal := blockNum - ref.FromTxNum
	if ordinal >= uint64(len(offsets)) {
		return 0, false, nil
	}
	return offsets[ordinal], true, nil
}

func chainFreezerRefContaining(refs []SegmentRef, blockNum uint64) (SegmentRef, bool) {
	for _, ref := range refs {
		if ref.ToTxNum < blockNum {
			continue
		}
		if ref.FromTxNum > blockNum {
			return SegmentRef{}, false
		}
		return ref, true
	}
	return SegmentRef{}, false
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
