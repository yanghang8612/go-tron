package rawdb

import (
	"bytes"
	"cmp"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"sort"

	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/rlp"
	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/internal/historychunk"
)

var (
	ErrStateHistorySpanExpired = errors.New("rawdb: history span block is outside its callback lifetime")
	ErrStateHistorySpanBudget  = errors.New("rawdb: history span compatibility block exceeds bounded budget")
)

const stateHistorySpanRepairRows = 262144

// StateHistorySpan identifies bytes in one authenticated, block-local chunk.
// It is a logical byte interval, never a database key or external dependency.
type StateHistorySpan struct{ ChunkIndex, Offset, Length uint32 }
type StateHistorySpanChunk struct {
	Digest [32]byte
	Length uint32
}
type StateHistorySpanBlockInfo struct {
	BlockNum                   uint64
	BlockHash                  common.Hash
	BeginTxNum, EndTxNum, Rows uint64
	// DecodedBytes is materialized RLP length, or total owning Key+Prev bytes
	// after repair/standalone fallback. It is not retained heap.
	DecodedBytes         uint64
	SharedPack, Fallback bool
}

// StateHistorySpanRow carries archival metadata. Prev and Next are nil; presence
// and the exact Prev bytes are represented separately, including present-empty.
// Key and PrevSpans are borrowed until the row callback returns. They do not
// alias the authenticated source bytes and may not be retained without copying.
type StateHistorySpanRow struct {
	Change     StateDomainChange
	PrevLength uint64
	PrevSpans  []StateHistorySpan
}
type stateHistorySpanChunk struct {
	StateHistorySpanChunk
	start uint32
	raw   []byte
}

// StateHistorySpanBlock is valid only inside IterateStateHistorySpanBlocks' fn.
// Its private payload cannot escape through an alias. CopyChunk returns owned
// bytes in caller storage. Methods are serial and must not race the callback's
// return. The caller owns the pinned read view and must keep it alive throughout.
type StateHistorySpanBlock struct {
	ctx             context.Context
	info            StateHistorySpanBlockInfo
	raw             []byte
	pooled          *[]byte
	chunks          []stateHistorySpanChunk
	active          bool
	fromTx, toTx    uint64
	ownRows, extras []*StateDomainChange
	ownStarts       []uint32
	packed          bool
	repairBytes     uint64
	repairCount     int
}

func (b *StateHistorySpanBlock) Info() StateHistorySpanBlockInfo {
	if b == nil {
		return StateHistorySpanBlockInfo{}
	}
	return b.info
}
func (b *StateHistorySpanBlock) ChunkCount() int {
	if b == nil || !b.active {
		return 0
	}
	return len(b.chunks)
}
func (b *StateHistorySpanBlock) check() error {
	if b == nil || !b.active {
		return ErrStateHistorySpanExpired
	}
	return b.ctx.Err()
}
func (b *StateHistorySpanBlock) Chunk(index int) (StateHistorySpanChunk, error) {
	if err := b.check(); err != nil {
		return StateHistorySpanChunk{}, err
	}
	if index < 0 || index >= len(b.chunks) {
		return StateHistorySpanChunk{}, fmt.Errorf("rawdb: history span chunk index %d out of bounds", index)
	}
	return b.chunks[index].StateHistorySpanChunk, nil
}
func (b *StateHistorySpanBlock) CopyChunk(index int, dst []byte) (int, error) {
	c, err := b.Chunk(index)
	if err != nil {
		return 0, err
	}
	if len(dst) < int(c.Length) {
		return 0, io.ErrShortBuffer
	}
	return copy(dst, b.chunks[index].raw), nil
}
func (b *StateHistorySpanBlock) discard() {
	pooled := b.pooled
	*b = StateHistorySpanBlock{}
	releaseBorrowedStateDomainChangeBlockPayload(pooled)
}

// IterateRows replays validated metadata without source reads, decompression or
// pack/chunk authentication. The first pass validated every row, even rows
// outside the requested tx filter. Callback mutation cannot affect later reads.
// Owning compatibility/repair rows use the cold record order (TxNum, Seq,
// remaining fields), after applying the ordinary reader's physical overrides.
func (b *StateHistorySpanBlock) IterateRows(fn func(*StateHistorySpanRow) (bool, error)) (bool, error) {
	if err := b.check(); err != nil {
		return false, err
	}
	if fn == nil {
		return false, errors.New("rawdb: nil history span row callback")
	}
	var out StateHistorySpanRow
	var key []byte
	var spans []StateHistorySpan
	emit := func(row *StateDomainChange, offset uint64, ownIndex int) (bool, error) {
		if err := b.check(); err != nil {
			return false, err
		}
		if row.TxNum < b.fromTx || row.TxNum > b.toTx {
			return true, nil
		}
		out = StateHistorySpanRow{}
		if cap(key) < len(row.Key) {
			key = make([]byte, len(row.Key))
		} else {
			key = key[:len(row.Key)]
		}
		copy(key, row.Key)
		spans = spans[:0]
		remaining := uint64(len(row.Prev))
		if remaining > 0 {
			if ownIndex >= 0 {
				for i := b.ownStarts[ownIndex]; remaining > 0; i++ {
					c := b.chunks[i]
					n := uint64(c.Length)
					spans = append(spans, StateHistorySpan{i, 0, uint32(n)})
					remaining -= n
				}
			} else {
				i := sort.Search(len(b.chunks), func(i int) bool { return uint64(b.chunks[i].start)+uint64(b.chunks[i].Length) > offset })
				for remaining > 0 {
					if i >= len(b.chunks) {
						return false, errors.New("rawdb: history Prev span exceeds authenticated chunks")
					}
					c := b.chunks[i]
					begin := offset - uint64(c.start)
					n := min(remaining, uint64(c.Length)-begin)
					spans = append(spans, StateHistorySpan{uint32(i), uint32(begin), uint32(n)})
					remaining -= n
					offset += n
					i++
				}
			}
		}
		out = StateHistorySpanRow{Change: *row, PrevLength: uint64(len(row.Prev)), PrevSpans: spans}
		out.Change.Key = key
		out.Change.Prev = nil
		out.Change.Next = nil
		out.Change.NextExists = false
		cont, err := fn(&out)
		if err != nil {
			return cont, err
		}
		if err := b.ctx.Err(); err != nil {
			return false, err
		}
		return cont, nil
	}
	if b.ownRows != nil {
		for i, row := range b.ownRows {
			cont, err := emit(row, 0, i)
			if err != nil || !cont {
				return cont, err
			}
		}
		return true, nil
	}
	if len(b.raw) == 0 {
		return true, nil
	}
	return b.parseRows(func(row *StateDomainChange, off uint64) (bool, error) { return emit(row, off, -1) })
}

// IterateStateHistorySpanBlocks is an explicit single-authentication export for
// a self-contained cold container. It does not acquire, unwrap or close view.
// Every physical block must have a contiguous StateTxRange in this same pinned
// sequence. That row supplies BlockHash; canonical/ancient-chain agreement is
// still the caller's separate planning/publication proof, as for other readers.
//
// A normal v3 block owns at most the existing 128MiB pack plus its bounded ref
// metadata. One copied row Key (at most the bounded pack size), its span array,
// and chunk Get/codec buffers are additional. Compatibility decoding may
// also retain a pooled inner payload or an owning repair image. Repair input plus
// the initial decoded pack is capped at 128MiB, and 262144 standalone rows. These
// are explicit error bounds, not RSS bounds. Nothing is truncated or published
// on a failing block. No caller callback is issued before complete block checks.
func IterateStateHistorySpanBlocks(ctx context.Context, view StateHistoryReadView, fromBlock, toBlock, fromTx, toTx uint64, fn func(*StateHistorySpanBlock) (bool, error)) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if view == nil || !view.IsPinnedKeyValueView() {
		return ErrStateHistoryReadViewUnpinned
	}
	if fn == nil {
		return errors.New("rawdb: nil history span block callback")
	}
	if fromBlock > toBlock || fromTx > toTx {
		return errors.New("rawdb: inverted history span range")
	}
	var start [8]byte
	binary.BigEndian.PutUint64(start[:], fromBlock)
	ranges := view.NewIterator(stateTxRangePrefix, start[:])
	defer ranges.Release()
	changes := view.NewIterator(stateChangeSetPrefix, start[:])
	defer changes.Release()
	next := func() (bool, error) {
		if err := ctx.Err(); err != nil {
			return false, err
		}
		ok := changes.Next()
		if err := ctx.Err(); err != nil {
			return false, err
		}
		if !ok {
			return false, changes.Error()
		}
		return true, nil
	}
	haveChange, err := next()
	if err != nil {
		return err
	}
	expected := fromBlock
	var previousEnd uint64
	first := true
	for ranges.Next() {
		if err := ctx.Err(); err != nil {
			return err
		}
		key := ranges.Key()
		if !bytes.HasPrefix(key, stateTxRangePrefix) || len(key) != len(stateTxRangePrefix)+8 {
			return errors.New("rawdb: invalid history span tx-range key")
		}
		block := binary.BigEndian.Uint64(key[len(stateTxRangePrefix):])
		if block > toBlock {
			break
		}
		if block != expected {
			return fmt.Errorf("rawdb: missing history span tx range at block %d", expected)
		}
		hash, begin, end, err := decodeBorrowedStateTxRange(ranges.Value(), block)
		if err != nil {
			return err
		}
		if !first && (previousEnd == ^uint64(0) || begin != previousEnd+1) {
			return errors.New("rawdb: history span tx ranges are not contiguous")
		}
		b := &StateHistorySpanBlock{ctx: ctx, active: true, fromTx: fromTx, toTx: toTx, info: StateHistorySpanBlockInfo{BlockNum: block, BlockHash: hash, BeginTxNum: begin, EndTxNum: end}}
		err = func() error {
			defer b.discard()
			for haveChange {
				k := changes.Key()
				if !bytes.HasPrefix(k, stateChangeSetPrefix) || len(k) != len(stateChangeSetPrefix)+16 {
					return errors.New("rawdb: invalid history span changeset key")
				}
				height := binary.BigEndian.Uint64(k[len(stateChangeSetPrefix):])
				if height > block {
					break
				}
				if height < block {
					return errors.New("rawdb: history span changeset has no matching tx range")
				}
				seq := binary.BigEndian.Uint64(k[len(stateChangeSetPrefix)+8:])
				if err := b.add(view, changes.Value(), seq); err != nil {
					return err
				}
				haveChange, err = next()
				if err != nil {
					return err
				}
			}
			if err := b.finish(); err != nil {
				return err
			}
			cont, err := fn(b)
			if err != nil {
				return err
			}
			if err := ctx.Err(); err != nil {
				return err
			}
			if !cont {
				return errStateHistorySpanStop
			}
			return nil
		}()
		if errors.Is(err, errStateHistorySpanStop) {
			return nil
		}
		if err != nil {
			return err
		}
		previousEnd = end
		first = false
		if block == toBlock {
			return ranges.Error()
		}
		expected = block + 1
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := ranges.Error(); err != nil {
		return err
	}
	return fmt.Errorf("rawdb: missing history span tx range at block %d", expected)
}

var errStateHistorySpanStop = errors.New("rawdb: stop history span iteration")

func (b *StateHistorySpanBlock) add(view StateHistoryReadView, encoded []byte, seq uint64) error {
	if err := b.ctx.Err(); err != nil {
		return err
	}
	if seq == 0 {
		if len(encoded) > stateDomainChangeBlockMaxDecodedBytes {
			return ErrStateHistorySpanBudget
		}
		shared := isStateHistorySharedPack(encoded)
		r := stateHistorySpanReader{StateHistoryReadView: view, ctx: b.ctx}
		var reader ethdb.KeyValueReader = r
		if presence, ok := view.(interface {
			GetWithPresence([]byte) ([]byte, bool, error)
		}); ok {
			reader = stateHistorySpanPresenceReader{r, presence}
		}
		data, err := materializeStateHistorySharedPack(encoded, b.info.BlockNum, []ethdb.KeyValueReader{reader})
		if err != nil {
			return err
		}
		return b.initializeAuthenticatedPack(encoded, data, shared)
	}
	if len(encoded) > stateDomainChangeBlockMaxDecodedBytes || uint64(len(encoded)) > stateDomainChangeBlockMaxDecodedBytes-b.repairBytes || b.repairCount >= stateHistorySpanRepairRows {
		return ErrStateHistorySpanBudget
	}
	b.repairBytes += uint64(len(encoded))
	b.repairCount++
	if b.ownRows == nil && len(b.raw) > 0 {
		// Only compatibility/repair needs an owning image. The pack was already
		// authenticated once; do not call its materializer or reread its chunks.
		count := 0
		_, err := b.parseRows(func(*StateDomainChange, uint64) (bool, error) {
			count++
			if count > stateHistorySpanRepairRows {
				return false, ErrStateHistorySpanBudget
			}
			return true, nil
		})
		if err != nil {
			return err
		}
		b.ownRows = make([]*StateDomainChange, 0, count)
		_, err = b.parseRows(func(row *StateDomainChange, _ uint64) (bool, error) {
			b.ownRows = append(b.ownRows, cloneStateDomainChange(row))
			return true, nil
		})
		if err != nil {
			return err
		}
		b.releasePayload()
	}
	row, err := decodePersistedStateDomainChange(encoded, b.info.BlockNum, seq)
	if err != nil {
		return err
	}
	if err := b.validateRow(row); err != nil {
		return err
	}
	b.info.Fallback = true
	if b.packed && len(b.ownRows) > 0 {
		first := b.ownRows[0].Seq
		if seq >= first && seq-first < uint64(len(b.ownRows)) {
			b.ownRows[seq-first] = row
		} else {
			b.extras = append(b.extras, row)
		}
	} else {
		b.ownRows = append(b.ownRows, row)
	}
	return nil
}

// initializeAuthenticatedPack consumes bytes already authenticated by the
// unchanged materializer. Parallel readers call it only after the worker's
// complete chunk/pack checks; the serial add path retains the same call order.
func (b *StateHistorySpanBlock) initializeAuthenticatedPack(encoded, data []byte, shared bool) error {
	payload, pooled, err := borrowStateDomainChangeBlockPayload(data)
	if err != nil {
		return err
	}
	b.pooled = pooled
	if pooled == nil && !shared {
		payload = bytes.Clone(payload)
	}
	b.raw = payload
	b.info.SharedPack = shared
	b.info.Fallback = !shared || pooled != nil
	b.repairBytes = uint64(len(payload))
	b.info.DecodedBytes = uint64(len(payload))
	if b.repairBytes > stateDomainChangeBlockMaxDecodedBytes {
		return ErrStateHistorySpanBudget
	}
	_, parseErr := b.parseRows(func(row *StateDomainChange, _ uint64) (bool, error) {
		if err := b.validateRow(row); err != nil {
			return false, err
		}
		if row.TxNum >= b.fromTx && row.TxNum <= b.toTx {
			b.info.Rows++
		}
		return true, nil
	})
	if parseErr != nil {
		// A shared envelope is never reinterpreted as a standalone row. The
		// compatibility probe applies only to old sequence-zero physical rows.
		if shared {
			return parseErr
		}
		row, legacyErr := decodePersistedStateDomainChange(encoded, b.info.BlockNum, 0)
		if legacyErr != nil {
			return parseErr
		}
		b.releasePayload()
		b.info.Fallback = true
		b.ownRows = []*StateDomainChange{row}
		b.repairBytes = uint64(len(encoded))
		b.repairCount = 1
		return b.validateRow(row)
	}
	b.packed = true
	if shared && pooled == nil {
		refs, _, count, _, err := sharedStateHistoryPackHeader(encoded, b.info.BlockNum)
		if err != nil {
			return err
		}
		b.chunks = make([]stateHistorySpanChunk, 0, count)
		offset := 0
		for i := 0; i < count; i++ {
			size, n := binary.Uvarint(refs)
			var digest [32]byte
			copy(digest[:], refs[n:n+32])
			refs = refs[n+32:]
			b.chunks = append(b.chunks, stateHistorySpanChunk{StateHistorySpanChunk{digest, uint32(size)}, uint32(offset), b.raw[offset : offset+int(size) : offset+int(size)]})
			offset += int(size)
		}
	} else {
		b.addFixedChunks(b.raw)
	}
	return nil
}

func (b *StateHistorySpanBlock) releasePayload() {
	p := b.pooled
	b.raw = nil
	b.pooled = nil
	b.chunks = nil
	releaseBorrowedStateDomainChangeBlockPayload(p)
}
func (b *StateHistorySpanBlock) addFixedChunks(raw []byte) {
	for offset := 0; offset < len(raw); {
		end := min(len(raw), offset+historychunk.MaxSize)
		part := raw[offset:end:end]
		b.chunks = append(b.chunks, stateHistorySpanChunk{StateHistorySpanChunk{sha256.Sum256(part), uint32(len(part))}, uint32(offset), part})
		offset = end
	}
}
func (b *StateHistorySpanBlock) validateRow(row *StateDomainChange) error {
	if err := b.ctx.Err(); err != nil {
		return err
	}
	if row.BlockNum != b.info.BlockNum || row.BlockHash != (common.Hash{}) && row.BlockHash != b.info.BlockHash {
		return errors.New("rawdb: history span row block binding differs")
	}
	if row.TxNum < b.info.BeginTxNum || row.TxNum > b.info.EndTxNum {
		return fmt.Errorf("rawdb: history span row txNum %d outside block %d range", row.TxNum, b.info.BlockNum)
	}
	row.BlockHash = b.info.BlockHash
	return nil
}
func (b *StateHistorySpanBlock) finish() error {
	if err := b.ctx.Err(); err != nil {
		return err
	}
	if b.ownRows != nil {
		b.info.Rows = 0
		if len(b.extras) > 0 {
			b.ownRows = append(b.ownRows, b.extras...)
			b.extras = nil
		}
		if len(b.ownRows) > stateHistorySpanRepairRows {
			return ErrStateHistorySpanBudget
		}
		// Repairs can append an older transaction under a later physical Seq.
		// The old cold builder applies its complete record comparator via ETL
		// after the owning reader's overrides. Only this already-owning fallback
		// needs that ordering; the normal v3 pack stays on its validated order.
		sort.SliceStable(b.ownRows, func(i, j int) bool {
			return compareStateHistorySpanRows(b.ownRows[i], b.ownRows[j]) < 0
		})
		b.chunks = nil
		b.ownStarts = make([]uint32, len(b.ownRows))
		b.info.DecodedBytes = 0
		for i, row := range b.ownRows {
			if err := b.validateRow(row); err != nil {
				return err
			}
			b.ownStarts[i] = uint32(len(b.chunks))
			b.addFixedChunks(row.Prev)
			b.info.DecodedBytes += uint64(len(row.Key) + len(row.Prev))
			if row.TxNum >= b.fromTx && row.TxNum <= b.toTx {
				b.info.Rows++
			}
		}
		return nil
	}
	return nil
}

// compareStateHistorySpanRows mirrors the existing cold binary record order.
// Keep all tie fields, including transient Next, until ordering is complete;
// IterateRows strips Next only when exporting archival metadata. A frozen
// independent comparator in the tests guards against accidental divergence.
func compareStateHistorySpanRows(a, b *StateDomainChange) int {
	if n := cmp.Compare(a.TxNum, b.TxNum); n != 0 {
		return n
	}
	if n := cmp.Compare(a.Seq, b.Seq); n != 0 {
		return n
	}
	if n := cmp.Compare(a.BlockNum, b.BlockNum); n != 0 {
		return n
	}
	if n := bytes.Compare(a.BlockHash[:], b.BlockHash[:]); n != 0 {
		return n
	}
	if n := cmp.Compare(a.FlatDomain, b.FlatDomain); n != 0 {
		return n
	}
	if n := bytes.Compare(a.Owner[:], b.Owner[:]); n != 0 {
		return n
	}
	if n := cmp.Compare(a.Generation, b.Generation); n != 0 {
		return n
	}
	if n := cmp.Compare(a.Domain, b.Domain); n != 0 {
		return n
	}
	if n := bytes.Compare(a.Key, b.Key); n != 0 {
		return n
	}
	if a.PrevExists != b.PrevExists {
		if a.PrevExists {
			return 1
		}
		return -1
	}
	if n := bytes.Compare(a.Prev, b.Prev); n != 0 {
		return n
	}
	if a.NextExists != b.NextExists {
		if a.NextExists {
			return 1
		}
		return -1
	}
	return bytes.Compare(a.Next, b.Next)
}

// parseRows uses only RLP lengths to locate Prev. The consumed-prefix identity
// is checked by the same strict field decoder used by the ordinary borrowed
// reader; no unsafe pointers, searching for equal payloads or guessed offsets.
func (b *StateHistorySpanBlock) parseRows(fn func(*StateDomainChange, uint64) (bool, error)) (bool, error) {
	block, trailing, err := rlp.SplitList(b.raw)
	if err != nil {
		return false, fmt.Errorf("rawdb: decode history span block: %w", err)
	}
	if len(trailing) != 0 {
		return false, errors.New("rawdb: history span block has trailing bytes")
	}
	base := len(b.raw) - len(block)
	version, rest, err := splitBorrowedStateDomainChangeUint(block, "version", ^uint64(0))
	if err != nil {
		return false, err
	}
	if version != uint64(persistedStateDomainChangeBlockVersion) {
		return false, fmt.Errorf("rawdb: unsupported state domain change block version %d", version)
	}
	base += len(block) - len(rest)
	block = rest
	first, rest, err := splitBorrowedStateDomainChangeUint(block, "first sequence", ^uint64(0))
	if err != nil {
		return false, err
	}
	if first == 0 {
		return false, errors.New("rawdb: invalid history span first sequence")
	}
	base += len(block) - len(rest)
	block = rest
	rows, tail, err := rlp.SplitList(block)
	if err != nil {
		return false, err
	}
	if len(tail) != 0 || len(rows) == 0 {
		return false, errors.New("rawdb: invalid history span rows/header")
	}
	base += len(block) - len(rows)
	var index, previous uint64
	var scratch StateDomainChange
	for len(rows) > 0 {
		if err := b.ctx.Err(); err != nil {
			return false, err
		}
		row, next, err := rlp.SplitList(rows)
		if err != nil {
			return false, err
		}
		if index > ^uint64(0)-first {
			return false, errors.New("rawdb: history span sequence overflow")
		}
		if err := decodeBorrowedStateDomainChangeRow(row, b.info.BlockNum, first+index, &scratch); err != nil {
			return false, err
		}
		if index > 0 && scratch.TxNum < previous {
			return false, errors.New("rawdb: history span pack is not tx ordered")
		}
		previous = scratch.TxNum
		cursor := row
		for i := 0; i < 7; i++ {
			_, _, rest, err := rlp.Split(cursor)
			if err != nil {
				return false, err
			}
			cursor = rest
		}
		prev, rest, err := rlp.SplitString(cursor)
		if err != nil {
			return false, err
		}
		offset := base + len(rows) - len(next) - len(row) + len(row) - len(cursor) + len(cursor) - len(rest) - len(prev)
		if offset < 0 || offset > len(b.raw) || len(prev) > len(b.raw)-offset {
			return false, errors.New("rawdb: invalid history span RLP interval")
		}
		scratch.BlockHash = b.info.BlockHash
		cont, err := fn(&scratch, uint64(offset))
		if err != nil || !cont {
			return cont, err
		}
		base += len(rows) - len(next)
		rows = next
		index++
	}
	return true, nil
}

// Preserve optional coupled presence and owned-byte contracts while checking
// cancellation between the original operations and on cache hits.
type stateHistorySpanReader struct {
	StateHistoryReadView
	ctx context.Context
}

func (r stateHistorySpanReader) Has(k []byte) (bool, error) {
	if err := r.ctx.Err(); err != nil {
		return false, err
	}
	return r.StateHistoryReadView.Has(k)
}
func (r stateHistorySpanReader) Get(k []byte) ([]byte, error) {
	if err := r.ctx.Err(); err != nil {
		return nil, err
	}
	return r.StateHistoryReadView.Get(k)
}
func (r stateHistorySpanReader) GetReturnsOwnedBytes() bool {
	p, ok := r.StateHistoryReadView.(interface{ GetReturnsOwnedBytes() bool })
	return ok && p.GetReturnsOwnedBytes()
}
func (r stateHistorySpanReader) historyChunkCacheState() (*StateHistoryChunkCache, context.Context) {
	if p, ok := r.StateHistoryReadView.(historyChunkCacheProvider); ok {
		cache, _ := p.historyChunkCacheState()
		return cache, r.ctx
	}
	return nil, r.ctx
}

type stateHistorySpanPresenceReader struct {
	stateHistorySpanReader
	presence interface {
		GetWithPresence([]byte) ([]byte, bool, error)
	}
}

func (r stateHistorySpanPresenceReader) GetWithPresence(k []byte) ([]byte, bool, error) {
	if err := r.ctx.Err(); err != nil {
		return nil, false, err
	}
	return r.presence.GetWithPresence(k)
}
