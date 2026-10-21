package rawdb

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"sort"
	"sync"

	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/metrics"
	"github.com/ethereum/go-ethereum/rlp"
	"github.com/golang/snappy"
	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/pointread"
	"golang.org/x/crypto/blake2b"
)

const (
	historyOwnerSpansMaxBytes = 4 << 20
	historyOwnerSpansMaxItems = 1 << 16
	historyOwnerSpansMinRaw   = 16 << 10
)

var historyOwnerSpansMagic = [8]byte{'G', 'T', 'H', 'O', 'S', '0', '0', '2'}

var historyOwnerSpansHits = metrics.NewRegisteredCounter("state/history/query/owner_spans/validated_hits", nil)
var historyOwnerSpansSelectedRows = metrics.NewRegisteredCounter("state/history/query/owner_spans/selected_rows", nil)
var historyOwnerSpansSkippedRows = metrics.NewRegisteredCounter("state/history/query/owner_spans/skipped_rows", nil)

type historyOwnerSpan struct {
	owner, rows, size uint32
}

type historyOwnerSpans struct {
	block, firstSeq                       uint64
	rawBytes, rowsOffset, rowsBytes, rows uint64
	digest                                [32]byte
	owners                                []common.Address
	spans                                 []historyOwnerSpan
}

// Owner spans are optional derived data, never authoritative rows. Their source
// is the exact encoded canonical pack, not a block number or a mutable head.
// Its normal materializer still authenticates all shared chunks and pack SHA.
// Version 2 uses BLAKE2b-256 only for auxiliary binding/checksum; canonical
// history, commitments and consensus hashes keep their original algorithms.
func buildHistoryOwnerSpans(raw, pack []byte, block uint64, known []*StateDomainChange) ([]byte, error) {
	if len(raw) < historyOwnerSpansMinRaw || len(raw) > stateDomainChangeBlockMaxDecodedBytes {
		return nil, nil
	}
	if known != nil && len(known) < 32 {
		return nil, nil
	}
	first, rows, err := splitHistoryOwnerRows(raw)
	if err != nil {
		return nil, err
	}
	type run struct {
		owner      common.Address
		rows, size uint32
	}
	var runs []run
	owners := make(map[common.Address]uint32)
	rowBytes, index := rows, uint64(0)
	var scratch StateDomainChange
	var previousTx uint64
	for len(rows) > 0 {
		row, rest, err := rlp.SplitList(rows)
		if err != nil || index > ^uint64(0)-first {
			return nil, errors.New("rawdb: invalid owner span source row")
		}
		var owner common.Address
		var tx uint64
		if known != nil {
			if index >= uint64(len(known)) || known[index] == nil || known[index].Seq != first+index {
				return nil, errors.New("rawdb: owner span writer row mismatch")
			}
			owner, tx = known[index].Owner, known[index].TxNum
		} else {
			if err := decodeBorrowedStateDomainChangeRow(row, block, first+index, &scratch); err != nil {
				return nil, err
			}
			owner, tx = scratch.Owner, scratch.TxNum
		}
		if index != 0 && tx < previousTx {
			return nil, errors.New("rawdb: owner span source txNum order")
		}
		previousTx = tx
		size := uint32(len(rows) - len(rest))
		if len(runs) != 0 && runs[len(runs)-1].owner == owner {
			runs[len(runs)-1].rows++
			runs[len(runs)-1].size += size
		} else {
			if len(runs) >= historyOwnerSpansMaxItems {
				return nil, nil
			}
			runs = append(runs, run{owner: owner, rows: 1, size: size})
		}
		owners[owner] = 0
		if len(owners) > historyOwnerSpansMaxItems {
			return nil, nil
		}
		index++
		rows = rest
	}
	if known != nil && index != uint64(len(known)) {
		return nil, errors.New("rawdb: owner span writer row count mismatch")
	}
	if index < 32 || len(owners) < 2 || uint64(len(raw))/index > 4096 {
		return nil, nil // Few/huge rows do not justify an additional whole-source hash.
	}
	ordered := make([]common.Address, 0, len(owners))
	for owner := range owners {
		ordered = append(ordered, owner)
	}
	sort.Slice(ordered, func(i, j int) bool { return bytes.Compare(ordered[i][:], ordered[j][:]) < 0 })
	for i, owner := range ordered {
		owners[owner] = uint32(i)
	}
	payload := make([]byte, 0, min(historyOwnerSpansMaxBytes, len(ordered)*21+len(runs)*6+128))
	for _, n := range []uint64{block, uint64(len(raw)), first, uint64(len(raw) - len(rowBytes)), uint64(len(rowBytes)), index, uint64(len(ordered)), uint64(len(runs))} {
		payload = binary.AppendUvarint(payload, n)
	}
	digest := blake2b.Sum256(pack)
	payload = append(payload, digest[:]...)
	for _, owner := range ordered {
		payload = append(payload, owner[:]...)
	}
	for _, run := range runs {
		payload = binary.AppendUvarint(payload, uint64(owners[run.owner]))
		payload = binary.AppendUvarint(payload, uint64(run.rows))
		payload = binary.AppendUvarint(payload, uint64(run.size))
	}
	if len(payload) > historyOwnerSpansMaxBytes {
		return nil, nil
	}
	digest = blake2b.Sum256(payload)
	encoded := append([]byte(nil), historyOwnerSpansMagic[:]...)
	encoded = binary.AppendUvarint(encoded, uint64(len(payload)))
	encoded = append(encoded, digest[:]...)
	return append(encoded, snappy.Encode(nil, payload)...), nil
}

func splitHistoryOwnerRows(raw []byte) (uint64, []byte, error) {
	block, trailing, err := rlp.SplitList(raw)
	if err != nil || len(trailing) != 0 {
		return 0, nil, errors.New("rawdb: invalid owner span source block")
	}
	version, block, err := splitBorrowedStateDomainChangeUint(block, "version", ^uint64(0))
	if err != nil || version != uint64(persistedStateDomainChangeBlockVersion) {
		return 0, nil, errors.New("rawdb: unsupported owner span source version")
	}
	first, block, err := splitBorrowedStateDomainChangeUint(block, "first sequence", ^uint64(0))
	if err != nil || first == 0 {
		return 0, nil, errors.New("rawdb: invalid owner span first sequence")
	}
	rows, rest, err := rlp.SplitList(block)
	if err != nil || len(rest) != 0 || len(rows) == 0 {
		return 0, nil, errors.New("rawdb: invalid owner span source rows")
	}
	return first, rows, nil
}

type historyOwnerSpansWork struct {
	payload []byte
	owners  []common.Address
	spans   []historyOwnerSpan
	index   historyOwnerSpans
}

var historyOwnerSpansWorkPool = sync.Pool{New: func() any { return new(historyOwnerSpansWork) }}

func decodeHistoryOwnerSpans(data []byte, block uint64) (*historyOwnerSpans, error) {
	return decodeHistoryOwnerSpansInto(data, block, nil)
}

func decodeHistoryOwnerSpansInto(data []byte, block uint64, work *historyOwnerSpansWork) (*historyOwnerSpans, error) {
	bad := errors.New("rawdb: invalid optional owner spans")
	if len(data) > historyOwnerSpansMaxBytes+64 || len(data) < 42 || !bytes.Equal(data[:8], historyOwnerSpansMagic[:]) {
		return nil, bad
	}
	size, n := binary.Uvarint(data[8:])
	if n <= 0 || size == 0 || size > historyOwnerSpansMaxBytes || len(data) < 8+n+32 {
		return nil, bad
	}
	checksum, encoded := data[8+n:8+n+32], data[8+n+32:]
	decodedLen, err := snappy.DecodedLen(encoded)
	if err != nil || uint64(decodedLen) != size {
		return nil, bad
	}
	var scratch []byte
	if work != nil {
		scratch = work.payload[:0]
	}
	p, err := snappy.Decode(scratch, encoded)
	if err != nil {
		return nil, bad
	}
	if work != nil {
		work.payload = p
	}
	digest := blake2b.Sum256(p)
	if !bytes.Equal(checksum, digest[:]) {
		return nil, bad
	}
	var fields [8]uint64
	for i := range fields {
		v, n := binary.Uvarint(p)
		if n <= 0 {
			return nil, bad
		}
		fields[i], p = v, p[n:]
	}
	r := new(historyOwnerSpans)
	if work != nil {
		r = &work.index
	}
	*r = historyOwnerSpans{block: fields[0], rawBytes: fields[1], firstSeq: fields[2], rowsOffset: fields[3], rowsBytes: fields[4], rows: fields[5]}
	owners, spans := fields[6], fields[7]
	if r.block != block || r.rawBytes > stateDomainChangeBlockMaxDecodedBytes || r.rawBytes == 0 || r.firstSeq == 0 || r.rows == 0 || r.rows > r.rowsBytes || r.rows-1 > ^uint64(0)-r.firstSeq ||
		r.rowsOffset > r.rawBytes || r.rowsBytes == 0 || r.rowsBytes != r.rawBytes-r.rowsOffset || owners == 0 || owners > historyOwnerSpansMaxItems || spans == 0 || spans > historyOwnerSpansMaxItems || uint64(len(p)) < 32+owners*common.AddressLength+spans*3 {
		return nil, bad
	}
	copy(r.digest[:], p[:32])
	p = p[32:]
	if work != nil && uint64(cap(work.owners)) >= owners {
		r.owners = work.owners[:owners]
	} else {
		r.owners = make([]common.Address, owners)
	}
	if work != nil {
		work.owners = r.owners
	}
	for i := range r.owners {
		copy(r.owners[i][:], p[:common.AddressLength])
		p = p[common.AddressLength:]
		if i != 0 && bytes.Compare(r.owners[i-1][:], r.owners[i][:]) >= 0 {
			return nil, bad
		}
	}
	if work != nil && uint64(cap(work.spans)) >= spans {
		r.spans = work.spans[:spans]
	} else {
		r.spans = make([]historyOwnerSpan, spans)
	}
	if work != nil {
		work.spans = r.spans
	}
	var totalRows, totalBytes uint64
	for i := range r.spans {
		var entry [3]uint64
		for j := range entry {
			v, n := binary.Uvarint(p)
			if n <= 0 || v > uint64(^uint32(0)) {
				return nil, bad
			}
			entry[j], p = v, p[n:]
		}
		if entry[0] >= owners || entry[1] == 0 || entry[1] > r.rows-totalRows || entry[2] == 0 || entry[2] > r.rowsBytes-totalBytes {
			return nil, bad
		}
		totalRows += entry[1]
		totalBytes += entry[2]
		r.spans[i] = historyOwnerSpan{uint32(entry[0]), uint32(entry[1]), uint32(entry[2])}
	}
	if len(p) != 0 || totalRows != r.rows || totalBytes != r.rowsBytes {
		return nil, bad
	}
	return r, nil
}

// BuildStateHistoryOwnerSpans derives only one optional index from a pinned
// canonical pack. The caller serializes maintenance and commits the destination
// batch. Canonical bytes, repairs and progress are not changed.
func BuildStateHistoryOwnerSpans(ctx context.Context, source StateHistoryReadView, target ethdb.KeyValueWriter, block uint64) (bool, error) {
	if source == nil || !source.IsPinnedKeyValueView() {
		return false, ErrStateHistoryReadViewUnpinned
	}
	if target == nil {
		return false, errors.New("rawdb: missing owner span destination")
	}
	if err := ctx.Err(); err != nil {
		return false, err
	}
	pack, present, err := readPresentValue(source, stateChangeSetKey(block, 0), "owner span source")
	if err != nil || !present {
		return false, err
	}
	materialized, err := materializeStateHistorySharedPack(pack, block, []ethdb.KeyValueReader{source})
	if err != nil {
		return false, err
	}
	raw, pooled, err := borrowStateDomainChangeBlockPayload(materialized)
	if pooled != nil {
		defer releaseBorrowedStateDomainChangeBlockPayload(pooled)
	}
	if err != nil {
		return false, err
	}
	if len(pack) > len(raw)/2 {
		return false, nil // Hashing an incompressible source can cost more than the saved decode.
	}
	index, err := buildHistoryOwnerSpans(raw, pack, block, nil)
	if err != nil && !isStateHistorySharedPack(pack) {
		if _, legacyErr := decodePersistedStateDomainChange(pack, block, 0); legacyErr == nil {
			return false, nil // Old sequence-zero standalone rows are not block packs.
		}
	}
	if err != nil || len(index) == 0 || len(index) > 4096+len(pack)/2 {
		return false, err
	}
	if err := ctx.Err(); err != nil {
		return false, err
	}
	return true, target.Put(stateChangeOwnerSpansKey(block), index)
}

// An invalid/stale sidecar is an optimization miss. Canonical materialization
// errors remain hard failures. No callback occurs until the optional metadata
// and every selected span have passed their complete validation.
func iterateHistoryOwnerSpans(ctx context.Context, db StateKVHistoryReader, pack []byte, block uint64, selected func(common.Address) bool, scratch *StateDomainChange, fn func(*StateDomainChange) (bool, error)) (used bool, err error) {
	if err := ctx.Err(); err != nil {
		return true, err
	}
	marker, ok := db.(pointread.PinnedKeyValueView)
	if !ok || !marker.IsPinnedKeyValueView() {
		return false, nil
	}
	index, present, err := readPresentValue(db, stateChangeOwnerSpansKey(block), "owner spans")
	if err != nil {
		return true, err
	}
	if !present {
		return false, nil
	}
	// This pool holds scratch capacity only. No parsed entry is trusted across
	// calls: current bytes/checksum/source are re-read and verified every time.
	work := historyOwnerSpansWorkPool.Get().(*historyOwnerSpansWork)
	defer func() {
		work.index = historyOwnerSpans{}
		if cap(work.payload) <= 256<<10 && cap(work.owners)*common.AddressLength <= 256<<10 && cap(work.spans)*12 <= 256<<10 {
			historyOwnerSpansWorkPool.Put(work)
		}
	}()
	r, err := decodeHistoryOwnerSpansInto(index, block, work)
	if err != nil {
		return false, nil
	}
	wanted := make([]bool, len(r.owners))
	for i, owner := range r.owners {
		wanted[i] = selected(owner)
	}
	var selectedRows uint64
	for _, span := range r.spans {
		if wanted[span.owner] {
			selectedRows += uint64(span.rows)
		}
	}
	if selectedRows > r.rows/3 {
		return false, nil // Two sparse validation passes must beat a full decode.
	}
	if blake2b.Sum256(pack) != r.digest {
		return false, nil
	}
	materialized, err := materializeStateHistorySharedPack(pack, block, []ethdb.KeyValueReader{db})
	if err != nil {
		return true, err
	}
	raw, pooled, err := borrowStateDomainChangeBlockPayload(materialized)
	if pooled != nil {
		defer releaseBorrowedStateDomainChangeBlockPayload(pooled)
	}
	if err != nil {
		return true, err
	}
	if uint64(len(raw)) != r.rawBytes {
		return false, nil
	}
	first, rows, err := splitHistoryOwnerRows(raw)
	if err != nil || first != r.firstSeq || uint64(len(rows)) != r.rowsBytes || uint64(len(raw)-len(rows)) != r.rowsOffset {
		return false, nil
	}
	// Pass zero validates selected rows without publishing any result. Pass one
	// borrows those same immutable bytes to preserve ordinary early callbacks.
	for pass := 0; pass < 2; pass++ {
		var offset, ordinal, previousTx uint64
		haveTx := false
		for _, span := range r.spans {
			if err := ctx.Err(); err != nil {
				return true, err
			}
			if wanted[span.owner] {
				payload := rows[offset : offset+uint64(span.size)]
				for i := uint32(0); i < span.rows; i++ {
					if err := ctx.Err(); err != nil {
						return true, err
					}
					row, rest, decodeErr := rlp.SplitList(payload)
					if decodeErr != nil || decodeBorrowedStateDomainChangeRow(row, block, first+ordinal+uint64(i), scratch) != nil || scratch.Owner != r.owners[span.owner] || haveTx && scratch.TxNum < previousTx {
						if pass != 0 {
							return true, errors.New("rawdb: owner span callback changed validated source bytes")
						}
						return false, nil
					}
					previousTx, haveTx = scratch.TxNum, true
					if pass != 0 {
						cont, err := fn(scratch)
						if err != nil || !cont {
							return true, err
						}
					}
					payload = rest
				}
				if len(payload) != 0 {
					if pass != 0 {
						return true, errors.New("rawdb: owner span source changed after validation")
					}
					return false, nil
				}
			}
			offset += uint64(span.size)
			ordinal += uint64(span.rows)
		}
		if pass == 0 {
			historyOwnerSpansHits.Inc(1)
			historyOwnerSpansSelectedRows.Inc(int64(selectedRows))
			historyOwnerSpansSkippedRows.Inc(int64(r.rows - selectedRows))
		}
	}
	return true, nil
}

// CheckStateHistoryOwnerSpans rebuilds from all original rows. It is intended
// for fixed-copy acceptance and strict maintenance, not the point-query path.
func CheckStateHistoryOwnerSpans(ctx context.Context, db StateHistoryReadView, block uint64) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if db == nil || !db.IsPinnedKeyValueView() {
		return ErrStateHistoryReadViewUnpinned
	}
	stored, present, err := readPresentValue(db, stateChangeOwnerSpansKey(block), "check owner spans")
	if err != nil || !present {
		return err
	}
	pack, present, err := readPresentValue(db, stateChangeSetKey(block, 0), "check owner span source")
	if err != nil {
		return err
	}
	if !present {
		return fmt.Errorf("rawdb: owner span source unavailable at block %d", block)
	}
	materialized, err := materializeStateHistorySharedPack(pack, block, []ethdb.KeyValueReader{db})
	if err != nil {
		return err
	}
	raw, pooled, err := borrowStateDomainChangeBlockPayload(materialized)
	if pooled != nil {
		defer releaseBorrowedStateDomainChangeBlockPayload(pooled)
	}
	if err != nil {
		return err
	}
	want, err := buildHistoryOwnerSpans(raw, pack, block, nil)
	if err != nil {
		return err
	}
	if !bytes.Equal(stored, want) {
		return errors.New("rawdb: owner spans differ from authoritative rows")
	}
	return ctx.Err()
}

// Retire only selected blocks, including a stale index whose source is already
// absent. Auxiliary deletes are excluded from authoritative changeset stats.
func deleteHistoryOwnerSpans(db StateKVLatestStore, blocks []uint64) error {
	if len(blocks) == 0 {
		return nil
	}
	if blocks[len(blocks)-1]-blocks[0] > uint64(len(blocks))*4 {
		for _, block := range blocks {
			if err := deleteHistoryOwnerSpans(db, []uint64{block}); err != nil {
				return err
			}
		}
		return nil
	}
	var start [8]byte
	binary.BigEndian.PutUint64(start[:], blocks[0])
	it := db.NewIterator(stateChangeOwnerSpansPrefix, start[:])
	defer it.Release()
	selected := 0
	for it.Next() {
		key := it.Key()
		if len(key) != len(stateChangeOwnerSpansPrefix)+8 || !bytes.HasPrefix(key, stateChangeOwnerSpansPrefix) {
			continue
		}
		block := binary.BigEndian.Uint64(key[len(stateChangeOwnerSpansPrefix):])
		for selected < len(blocks) && blocks[selected] < block {
			selected++
		}
		if selected == len(blocks) {
			break
		}
		if blocks[selected] == block {
			if err := db.Delete(key); err != nil {
				return err
			}
		}
	}
	return it.Error()
}

// RetireStateHistoryOwnerSpans removes only an optional accelerator at or below
// a verified posting watermark. Coarse publication uses one schema-contained
// range in the caller's batch. Small/no-progress passes use at most 512 point
// deletes and 4MiB plus one complete value, avoiding per-block range fragments.
// Starting at the prefix also catches old buffered writes flushed after an
// earlier retirement. The caller serializes publication and owns batch commit.
func RetireStateHistoryOwnerSpans(source ethdb.Iteratee, target ethdb.KeyValueWriter, through uint64, coarse bool) error {
	if coarse {
		if ranges, ok := target.(interface{ DeleteRange([]byte, []byte) error }); ok {
			end := prefixUpperBound(stateChangeOwnerSpansPrefix)
			if through != ^uint64(0) {
				end = stateChangeOwnerSpansKey(through + 1)
			}
			return ranges.DeleteRange(bytes.Clone(stateChangeOwnerSpansPrefix), end)
		}
	}
	it := source.NewIterator(stateChangeOwnerSpansPrefix, nil)
	defer it.Release()
	scanned, work := 0, 0
	for scanned < 512 && work < 4<<20 && it.Next() {
		scanned++
		key := it.Key()
		if len(key) != len(stateChangeOwnerSpansPrefix)+8 || !bytes.HasPrefix(key, stateChangeOwnerSpansPrefix) {
			continue
		}
		if binary.BigEndian.Uint64(key[len(stateChangeOwnerSpansPrefix):]) > through {
			break
		}
		work += len(key) + len(it.Value())
		if err := target.Delete(key); err != nil {
			return err
		}
	}
	return it.Error()
}
