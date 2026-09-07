package pruning

import (
	"context"
	"errors"
	"fmt"
	"math"
	"math/bits"

	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
)

var ErrOfflineHistoryInputLimit = errors.New("pruning: offline history input byte limit exceeded")

// OfflineHistoryInputStats counts decoded input, not compressed SST ranges.
// InputBytes is the byte size of the complete V1 change encoding (including
// fields omitted by modern hot/cold formats). Derived *UpperBytes values sum
// conservative file-length bounds across phases, including simultaneously live
// staging/output copies. They exclude Pebble/WAL writes, existing files,
// filesystem allocation/metadata, manifest publication and other processes.
// Callers must reserve those separately and keep checking available space.
type OfflineHistoryInputStats struct {
	Blocks                uint64 `json:"blocks"`
	Records               uint64 `json:"records"`
	InputBytes            uint64 `json:"inputBytes"`
	KeyBytes              uint64 `json:"keyBytes"`
	LogicalKeyBytes       uint64 `json:"logicalKeyBytes"`
	PreviousValueBytes    uint64 `json:"previousValueBytes"`
	NextValueBytes        uint64 `json:"nextValueBytes"`
	EncodedRecordBytes    uint64 `json:"encodedRecordBytes"`
	MaxRecordBytes        uint64 `json:"maxRecordBytes"`
	KeyETLBytes           uint64 `json:"keyETLBytes"`
	PostingETLBytes       uint64 `json:"postingETLBytes"`
	FallbackETLBytes      uint64 `json:"fallbackETLBytes"`
	DictionaryUpperBytes  uint64 `json:"dictionaryUpperBytes"`
	SegmentUpperBytes     uint64 `json:"segmentUpperBytes"`
	AccessorUpperBytes    uint64 `json:"accessorUpperBytes"`
	IndexUpperBytes       uint64 `json:"indexUpperBytes"`
	ColdScratchUpperBytes uint64 `json:"coldScratchUpperBytes"`
	Ordered               bool   `json:"ordered"`
	FirstBlock            uint64 `json:"firstBlock"`
	LastBlock             uint64 `json:"lastBlock"`
	FirstTxNum            uint64 `json:"firstTxNum"`
	FirstSeq              uint64 `json:"firstSeq"`
	LastTxNum             uint64 `json:"lastTxNum"`
	LastSeq               uint64 `json:"lastSeq"`
}

// scanOfflineHistoryBlock scans the whole physical block, rather than filtering
// to the claimed tx range: a corrupt out-of-range record must be rejected, not
// silently omitted from a plan. No borrowed key/value bytes escape the callback.
// A nonzero optional limit aborts as soon as this block exceeds that limit.
// Legacy/non-ordered packs are rejected by the production borrowed iterator;
// the maintenance planner must not silently switch to a different decoder.
func scanOfflineHistoryBlock(ctx context.Context, db ethdb.Iteratee, row rawdb.StateTxRange, maxInputBytes ...uint64) (OfflineHistoryInputStats, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if db == nil || row.EndTxNum < row.BeginTxNum || len(maxInputBytes) > 1 {
		return OfflineHistoryInputStats{}, errors.New("pruning: invalid offline history input scan")
	}
	if err := ctx.Err(); err != nil {
		return OfflineHistoryInputStats{}, err
	}
	limit := uint64(0)
	if len(maxInputBytes) == 1 {
		limit = maxInputBytes[0]
	}
	s := OfflineHistoryInputStats{Blocks: 1, FirstBlock: row.BlockNum, LastBlock: row.BlockNum, Ordered: true}
	err := rawdb.IterateStateDomainChangesByBlockTxRangeBorrowed(db, row.BlockNum, row.BlockNum, 0, math.MaxUint64, func(c *rawdb.StateDomainChange) (bool, error) {
		if err := ctx.Err(); err != nil {
			return false, err
		}
		if c == nil || c.BlockNum != row.BlockNum || c.BlockHash != row.BlockHash || c.TxNum < row.BeginTxNum || c.TxNum > row.EndTxNum {
			return false, fmt.Errorf("pruning: offline history change disagrees with block %d tx range", row.BlockNum)
		}
		if s.Records > 0 && (c.TxNum < s.LastTxNum || c.TxNum == s.LastTxNum && c.Seq <= s.LastSeq) {
			return false, errors.New("pruning: offline history changes are not ordered")
		}
		logicalKey := uint64(1 + common.AccountIDLength)
		if c.FlatDomain == rawdb.StateFlatDomainKVLatest {
			logicalKey += 8 + 2 + uint64(len(c.Key))
		}
		if logicalKey > math.MaxUint16 || uint64(len(c.Key)) > math.MaxUint32 || uint64(len(c.Prev)) > math.MaxUint32-17 || uint64(len(c.Next)) > math.MaxUint32 {
			return false, errors.New("pruning: offline history record exceeds production encoding limits")
		}
		// V1: block/hash/tx/seq/domain/owner/generation/subdomain, three
		// uint32 byte lengths and two presence flags. V6: a 4-byte frame
		// length plus key ID, tx, presence flag, previous-value length/data.
		input := uint64(8+common.HashLength+8+8+1+common.AddressLength+8+2+3*4+2) + uint64(len(c.Key)) + uint64(len(c.Prev)) + uint64(len(c.Next))
		if s.Records == math.MaxUint32 {
			return false, errors.New("pruning: offline history exceeds uint32 record ordinals")
		}
		for _, part := range []struct {
			dst *uint64
			n   uint64
		}{
			{&s.InputBytes, input}, {&s.KeyBytes, uint64(len(c.Key))},
			{&s.LogicalKeyBytes, logicalKey}, {&s.PreviousValueBytes, uint64(len(c.Prev))},
			{&s.NextValueBytes, uint64(len(c.Next))}, {&s.EncodedRecordBytes, 21 + uint64(len(c.Prev))},
		} {
			if err := offlineHistoryAddBytes(part.dst, part.n); err != nil {
				return false, err
			}
		}
		if limit > 0 && s.InputBytes > limit {
			return false, fmt.Errorf("%w: block %d needs at least %d bytes, limit %d", ErrOfflineHistoryInputLimit, row.BlockNum, s.InputBytes, limit)
		}
		if s.Records == 0 {
			s.FirstTxNum, s.FirstSeq = c.TxNum, c.Seq
		}
		s.LastTxNum, s.LastSeq = c.TxNum, c.Seq
		s.Records++
		s.MaxRecordBytes = max(s.MaxRecordBytes, input)
		return true, nil
	})
	if err == nil {
		err = ctx.Err()
	}
	if err == nil {
		err = s.deriveBounds()
	}
	if err != nil {
		return OfflineHistoryInputStats{}, err
	}
	return s, nil
}

// Add atomically combines consecutive scans; an error leaves the receiver
// unchanged. Bound formulas are recomputed for the combined batch, so fixed
// per-trio allowances are not charged once for each block.
func (s *OfflineHistoryInputStats) Add(next OfflineHistoryInputStats) error {
	if s == nil || !next.Ordered || s.Blocks > 0 && (!s.Ordered || next.Blocks > 0 && next.FirstBlock <= s.LastBlock) {
		return errors.New("pruning: unordered offline history input aggregation")
	}
	if s.Records > 0 && next.Records > 0 && (next.FirstTxNum < s.LastTxNum || next.FirstTxNum == s.LastTxNum && next.FirstSeq <= s.LastSeq) {
		return errors.New("pruning: offline history input order crosses batch boundary")
	}
	out := *s
	for _, part := range []struct {
		dst *uint64
		n   uint64
	}{
		{&out.Blocks, next.Blocks}, {&out.Records, next.Records}, {&out.InputBytes, next.InputBytes},
		{&out.KeyBytes, next.KeyBytes}, {&out.LogicalKeyBytes, next.LogicalKeyBytes},
		{&out.PreviousValueBytes, next.PreviousValueBytes}, {&out.NextValueBytes, next.NextValueBytes},
		{&out.EncodedRecordBytes, next.EncodedRecordBytes},
	} {
		if err := offlineHistoryAddBytes(part.dst, part.n); err != nil {
			return err
		}
	}
	if out.Records > math.MaxUint32 {
		return errors.New("pruning: offline history exceeds uint32 record ordinals")
	}
	if out.Blocks == next.Blocks {
		out.FirstBlock = next.FirstBlock
	}
	if next.Blocks > 0 {
		out.LastBlock = next.LastBlock
	}
	if s.Records == 0 && next.Records > 0 {
		out.FirstTxNum, out.FirstSeq = next.FirstTxNum, next.FirstSeq
	}
	if next.Records > 0 {
		out.LastTxNum, out.LastSeq = next.LastTxNum, next.LastSeq
	}
	out.Ordered, out.MaxRecordBytes = true, max(out.MaxRecordBytes, next.MaxRecordBytes)
	if err := out.deriveBounds(); err != nil {
		return err
	}
	*s = out
	return nil
}

func (s *OfflineHistoryInputStats) deriveBounds() error {
	// At most N distinct keys and N ETL runs: every spill row has a 17-byte
	// header and every run an 8-byte magic. Key and posting collectors use
	// (logicalKey, empty) and (8-byte key, 18-byte value), respectively.
	var err error
	calc := func(dst *uint64, terms ...uint64) {
		if err != nil {
			return
		}
		*dst = 0
		err = offlineHistoryAddBytes(dst, terms...)
	}
	mul := func(a, b uint64) uint64 {
		hi, lo := bits.Mul64(a, b)
		if hi != 0 {
			err = errors.New("pruning: offline history byte budget overflows")
		}
		return lo
	}
	n, keys := s.Records, s.LogicalKeyBytes
	calc(&s.KeyETLBytes, keys, mul(25, n))
	calc(&s.PostingETLBytes, mul(51, n))
	var variable uint64
	calc(&variable, s.KeyBytes, s.PreviousValueBytes, s.NextValueBytes)
	// Fallback sorting includes full V1 input and a lexicographic sort key:
	// 104 fixed bytes plus each variable byte escaped to at most two bytes.
	calc(&s.FallbackETLBytes, s.InputBytes, mul(129, n), mul(2, variable))
	calc(&s.DictionaryUpperBytes, keys, mul(20, n)) // two maximal uvarints/key
	var logical uint64
	calc(&logical, 76, mul(56, s.Blocks), s.EncodedRecordBytes)
	// The production 128-KiB zstd chunks have far less than 100% framing
	// overhead. This covers incompressible payload, block table and footer;
	// below we charge two complete copies for compression format 1 finalize.
	calc(&s.SegmentUpperBytes, mul(2, logical), 4096)
	// Accessor: key data <= keys+40*N; dictionary directory <= keys+38*N;
	// posting frames <= 68*N (three maximal uvarints, directory, CRC/prefix).
	calc(&s.AccessorUpperBytes, 120, mul(2, keys), mul(146, n))
	// Tx index has <=N entries: four maximal uvarints + a 32-byte frame
	// directory per entry is deliberately looser than 256 entries/frame.
	calc(&s.IndexUpperBytes, 80, mul(72, n))
	// Sum even mutually exclusive phases. Also count the dictionary, metadata,
	// a full extra posting scratch, all ETL runs, and two copies of every
	// final file. The 1 MiB constant covers fixed headers/tiny temp files.
	calc(&s.ColdScratchUpperBytes, 1<<20, s.KeyETLBytes, s.PostingETLBytes,
		s.FallbackETLBytes, s.DictionaryUpperBytes, mul(42, n),
		mul(2, s.SegmentUpperBytes), mul(2, s.AccessorUpperBytes), mul(2, s.IndexUpperBytes))
	return err
}

func offlineHistoryAddBytes(dst *uint64, values ...uint64) error {
	for _, value := range values {
		if value > math.MaxUint64-*dst {
			return errors.New("pruning: offline history byte budget overflows")
		}
		*dst += value
	}
	return nil
}
