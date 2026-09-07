package snapshots

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sort"
	"strings"

	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
)

// VerifyLegacyStateDomainHistoryBoundariesContext checks the state-history
// portion of an explicitly pinned, unbound local manifest against the stopped
// datadir. It reads format metadata and two block ranges per trio, without ETL
// or writes. This is a chain-boundary proof, not a full segment scrub and not
// permission to label unrelated datasets with expected. The deletion caller
// must still verify complete covering trios and every remaining hot preimage.
func VerifyLegacyStateDomainHistoryBoundariesContext(ctx context.Context, db ethdb.KeyValueReader, dir string, manifest *Manifest, expected ChainIdentity, canonical func(uint64) (common.Hash, bool, error)) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if db == nil || canonical == nil || manifest == nil || manifest.Chain != nil {
		return errors.New("snapshots: legacy history proof requires a database, canonical reader and unbound manifest")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	normalizeChainIdentity(&expected)
	if err := validateChainIdentity(&expected); err != nil {
		return fmt.Errorf("snapshots: invalid legacy expected chain identity: %w", err)
	}
	genesis, ok, err := canonical(0)
	if err != nil {
		return fmt.Errorf("snapshots: read canonical genesis for legacy history: %w", err)
	}
	if !ok || genesis == (common.Hash{}) || genesis != common.HexToHash(expected.GenesisHash) {
		return errors.New("snapshots: legacy history canonical genesis mismatch or missing")
	}
	if err := manifest.Validate(); err != nil {
		return err
	}
	if err := manifest.ValidateProduction(); err != nil {
		return err
	}
	byPath := make(map[string]SegmentRef, len(manifest.Segments))
	var histories []SegmentRef
	for _, ref := range manifest.Segments {
		byPath[ref.Path] = ref
		if ref.NormalizedDataset() == SegmentDatasetStateDomainChange && ref.Kind == SegmentHistory {
			histories = append(histories, ref)
		}
	}
	if len(histories) == 0 {
		return errors.New("snapshots: legacy manifest has no state-history boundary proof")
	}
	sort.Slice(histories, func(i, j int) bool { return histories[i].FromTxNum < histories[j].FromTxNum })
	cfg, _ := DefaultDomainRegistry().Dataset(SegmentDatasetStateDomainChange)
	var previous *rawdb.StateTxRange
	for _, ref := range histories {
		if err := ctx.Err(); err != nil {
			return err
		}
		index, haveIndex := byPath[cfg.HistoryIndexPathFor(ref.Path)]
		accessor, haveAccessor := byPath[cfg.HistoryAccessorPathFor(ref.Path)]
		if !haveIndex || !haveAccessor {
			return fmt.Errorf("snapshots: legacy history %q lacks a complete trio", ref.Path)
		}
		for _, part := range []SegmentRef{ref, index, accessor} {
			if strings.TrimSpace(part.Checksum) == "" {
				return fmt.Errorf("snapshots: legacy history proof requires checksum for %q", part.Path)
			}
		}
		first, last, err := legacyStateDomainHistoryTrioBoundary(ctx, dir, ref, index, accessor)
		if err != nil {
			return err
		}
		if previous != nil && (previous.EndTxNum == math.MaxUint64 || previous.BlockNum == math.MaxUint64 ||
			first.BeginTxNum != previous.EndTxNum+1 || first.BlockNum != previous.BlockNum+1) {
			return errors.New("snapshots: legacy state-history tx/block boundaries are not contiguous")
		}
		for _, row := range []*rawdb.StateTxRange{first, last} {
			if err := ctx.Err(); err != nil {
				return err
			}
			hot, ok, err := rawdb.ReadStateTxRange(db, row.BlockNum)
			if err != nil {
				return err
			}
			if !ok || hot == nil || *hot != *row {
				return fmt.Errorf("snapshots: legacy cold/retained block range mismatch at block %d", row.BlockNum)
			}
			hash, ok, err := canonical(row.BlockNum)
			if err != nil {
				return err
			}
			if !ok || hash == (common.Hash{}) || hash != row.BlockHash {
				return fmt.Errorf("snapshots: legacy cold/canonical block hash mismatch at block %d", row.BlockNum)
			}
		}
		previous = last
	}
	return ctx.Err()
}

func legacyStateDomainHistoryTrioBoundary(ctx context.Context, dir string, historyRef, indexRef, accessorRef SegmentRef) (*rawdb.StateTxRange, *rawdb.StateTxRange, error) {
	segment, size, header, err := openHistorySegmentForRead(dir, historyRef)
	if err != nil {
		return nil, nil, err
	}
	defer segment.Close()
	reader := contextReaderAt{ctx: ctx, r: segment}
	count, _, err := stateDomainChangeBinaryTxRangeTableBoundsAt(reader, size, historyRef, header)
	if err != nil {
		return nil, nil, err
	}
	if count == 0 {
		return nil, nil, fmt.Errorf("snapshots: legacy history %q has no explicit block range table", historyRef.Path)
	}
	first, err := readStateDomainChangeBinaryTxRangeAt(reader, historyRef, header.version, 0)
	if err != nil {
		return nil, nil, err
	}
	last, err := readStateDomainChangeBinaryTxRangeAt(reader, historyRef, header.version, count-1)
	if err != nil {
		return nil, nil, err
	}
	if first.EndTxNum < first.BeginTxNum || last.EndTxNum < last.BeginTxNum || first.BeginTxNum != historyRef.FromTxNum ||
		last.EndTxNum != historyRef.ToTxNum || first.EndTxNum > last.EndTxNum || first.BlockNum > last.BlockNum ||
		last.BlockNum-first.BlockNum == math.MaxUint64 || count != last.BlockNum-first.BlockNum+1 {
		return nil, nil, fmt.Errorf("snapshots: legacy history %q endpoints do not cover its declared range", historyRef.Path)
	}
	index, indexHeader, err := openStateDomainChangeBinaryIndexReader(dir, indexRef)
	if err != nil {
		return nil, nil, err
	}
	defer index.Close()
	accessor, accessorHeader, _, err := openStateDomainChangeBinaryAccessorReader(dir, accessorRef)
	if err != nil {
		return nil, nil, err
	}
	defer accessor.Close()
	if indexHeader.count > header.count || accessorHeader.count != header.count {
		return nil, nil, fmt.Errorf("snapshots: legacy history %q companion record counts mismatch", historyRef.Path)
	}
	return first, last, ctx.Err()
}
