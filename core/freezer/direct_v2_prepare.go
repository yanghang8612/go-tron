package freezer

import (
	"context"
	"fmt"
	"runtime"

	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	coretypes "github.com/tronprotocol/go-tron/core/types"
)

func directV2PreparationWorkers() int {
	return max(1, min(4, runtime.GOMAXPROCS(0)/2))
}

// directV2Transform moves record validation out of the source reader. Bodies
// are read once for the receipt pass and shared by its validation and compact
// transform; previously validation and transform each read/decode the body.
// Calls for different record numbers may run concurrently. The migration
// starts this pipeline after dictionary sampling and joins workers before
// verification and the next table, so per-number hashes are complete before
// state-root reads begin.
func directV2Transform(ctx context.Context, start uint64, hashes []tcommon.Hash, externalize bool) func(string, uint64, []byte, []byte) ([]byte, error) {
	compact := rawdb.CompactAncientV2Record
	if externalize {
		compact = rawdb.CompactAncientV2RecordWithExternalLogs
	}
	return func(kind string, number uint64, data, body []byte) ([]byte, error) {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if number < start || number-start >= uint64(len(hashes)) {
			return nil, fmt.Errorf("freezer: direct V2 transform %s[%d] outside hash range", kind, number)
		}
		switch kind {
		case rawdbAncientBlocks:
			block, err := decodeDirectV2BlockBorrowed(number, data)
			if err != nil {
				return nil, err
			}
			hashes[number-start] = block.Hash()
		case rawdbAncientTxInfos:
			block, err := decodeDirectV2BlockBorrowed(number, body)
			if err != nil {
				return nil, err
			}
			if err := validateFreezerTransactionInfosRaw(number, block, data); err != nil {
				return nil, err
			}
		case rawdbAncientStateRoots:
			if err := validateFreezerStateRootRaw(number, data); err != nil {
				return nil, err
			}
		default:
			return nil, fmt.Errorf("freezer: unknown direct V2 transform table %s", kind)
		}
		return compact(kind, number, data, body)
	}
}

// A direct transform consumes the block synchronously while its source bytes
// remain immutable. Parallel preparation already owns both body and data; the
// serial dictionary/verification paths finish before another source read.
// Keep the complete decoder, including malformed and pre-PQ legacy fallback.
func decodeDirectV2BlockBorrowed(blockNum uint64, raw []byte) (*coretypes.Block, error) {
	block, err := coretypes.UnmarshalBlockBorrowed(raw)
	if err != nil {
		return nil, fmt.Errorf("freezer: decode block %d: %w", blockNum, err)
	}
	if block.Number() != blockNum {
		return nil, fmt.Errorf("freezer: block row %d contains block number %d", blockNum, block.Number())
	}
	return block, nil
}
