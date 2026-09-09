package freezer

import (
	"context"
	"fmt"
	"runtime"

	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
)

func directV2PreparationWorkers() int {
	return max(1, min(4, runtime.GOMAXPROCS(0)/2))
}

// directV2Transform moves record validation out of the source reader. Bodies
// are read once for the receipt pass and shared by its validation and compact
// transform; previously validation and transform each read/decode the body.
// Calls for different record numbers may run concurrently. The migration
// joins workers before dictionary sampling, verification and the next table,
// so the per-number hashes are complete before state-root reads begin.
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
			block, err := decodeFreezerBlockRaw(number, data)
			if err != nil {
				return nil, err
			}
			hashes[number-start] = block.Hash()
		case rawdbAncientTxInfos:
			block, err := decodeFreezerBlockRaw(number, body)
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
