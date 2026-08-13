package rawdb

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"sort"
	"sync"
	"sync/atomic"

	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/metrics"
	"github.com/ethereum/go-ethereum/rlp"
	"github.com/golang/snappy"
	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/state/kvdomains"
)

const stateChangeEncodingSampleInterval = uint64(128)

var (
	stateChangeEncodingSampleSequence           atomic.Uint64
	stateChangeEncodingSampleRowsCounter        = metrics.NewRegisteredCounter("state/history/changeset/sample/rows", nil)
	stateChangeEncodingSampleEncodedCounter     = metrics.NewRegisteredCounter("state/history/changeset/sample/encoded_bytes", nil)
	stateChangeEncodingSampleKeyCounter         = metrics.NewRegisteredCounter("state/history/changeset/sample/logical_key_bytes", nil)
	stateChangeEncodingSamplePrevCounter        = metrics.NewRegisteredCounter("state/history/changeset/sample/prev_bytes", nil)
	stateChangeEncodingSampleNextCounter        = metrics.NewRegisteredCounter("state/history/changeset/sample/next_bytes", nil)
	stateChangeEncodingSampleOmittedNextCounter = metrics.NewRegisteredCounter("state/history/changeset/sample/omitted_next_bytes", nil)
	stateChangeEncodingSampleFixedCounter       = metrics.NewRegisteredCounter("state/history/changeset/sample/fixed_bytes", nil)
	stateChangeEncodingSamplePrevRows           = metrics.NewRegisteredCounter("state/history/changeset/sample/prev_rows", nil)
	stateChangeEncodingSampleNextRows           = metrics.NewRegisteredCounter("state/history/changeset/sample/next_rows", nil)
	stateChangeEncodingSampleOmittedNextRows    = metrics.NewRegisteredCounter("state/history/changeset/sample/omitted_next_rows", nil)
	stateChangeBlockPackBlocksCounter           = metrics.NewRegisteredCounter("state/history/changeset/block_pack/blocks", nil)
	stateChangeBlockPackRowsCounter             = metrics.NewRegisteredCounter("state/history/changeset/block_pack/rows", nil)
	stateChangeBlockPackEncodedBytesCounter     = metrics.NewRegisteredCounter("state/history/changeset/block_pack/encoded_bytes", nil)
	stateChangeBlockPackLogicalBytesCounter     = metrics.NewRegisteredCounter("state/history/changeset/block_pack/logical_bytes", nil)
	stateChangeBlockPackWritesAvoidedCounter    = metrics.NewRegisteredCounter("state/history/changeset/block_pack/writes_avoided", nil)
	stateChangeBlockPackKeyBytesAvoidedCounter  = metrics.NewRegisteredCounter("state/history/changeset/block_pack/key_bytes_avoided", nil)
	stateChangeBlockPackUncompressedCounter     = metrics.NewRegisteredCounter("state/history/changeset/block_pack/uncompressed_bytes", nil)
	stateChangeBlockPackCompressionSavedCounter = metrics.NewRegisteredCounter("state/history/changeset/block_pack/compression_saved_bytes", nil)
	stateChangeBlockPackCompressedCounter       = metrics.NewRegisteredCounter("state/history/changeset/block_pack/compressed_blocks", nil)
	stateChangeBlockPackRawCounter              = metrics.NewRegisteredCounter("state/history/changeset/block_pack/raw_blocks", nil)
	stateChangeBlockRawBufferPool               = sync.Pool{New: func() any { return new(bytes.Buffer) }}
	stateChangeBlockDecodeBufferPool            = sync.Pool{New: func() any { return new([]byte) }}
)

// NextStateTxRange returns the compact global txNum range for the next block.
// The range covers every transaction ordinal plus one block-final ordinal for
// maintenance and derived stores flushed after transaction execution.
func NextStateTxRange(parentEndTxNum, txCount uint64) (uint64, uint64, error) {
	if parentEndTxNum == ^uint64(0) {
		return 0, 0, fmt.Errorf("rawdb: state tx range overflows after parent end %d", parentEndTxNum)
	}
	begin := parentEndTxNum + 1
	end, err := StateTxNumAt(begin, txCount)
	if err != nil {
		return 0, 0, err
	}
	return begin, end, nil
}

// StateTxNumAt returns the txNum for an ordinal inside a compact block txNum
// range returned by NextStateTxRange.
func StateTxNumAt(beginTxNum, ordinal uint64) (uint64, error) {
	if ordinal > ^uint64(0)-beginTxNum {
		return 0, fmt.Errorf("rawdb: state tx ordinal %d overflows block begin txNum %d", ordinal, beginTxNum)
	}
	return beginTxNum + ordinal, nil
}

type StateTxRange struct {
	BlockNum   uint64
	BlockHash  common.Hash
	BeginTxNum uint64
	EndTxNum   uint64
}

type StateFlatDomain uint8

const (
	StateFlatDomainUnknown StateFlatDomain = iota
	StateFlatDomainAccountLatest
	StateFlatDomainKVLatest
	StateFlatDomainKVGeneration
)

func (d StateFlatDomain) String() string {
	switch d {
	case StateFlatDomainAccountLatest:
		return "account-latest"
	case StateFlatDomainKVLatest:
		return "kv-latest"
	case StateFlatDomainKVGeneration:
		return "kv-generation"
	default:
		return fmt.Sprintf("unknown(%d)", uint8(d))
	}
}

// StateDomainChange records one mutation in a flat latest domain. Prev is the
// history payload used by unwind and as-of reads. Next is transient commit
// context (and remains populated when decoding legacy rows); new history rows
// do not persist it because the current image already lives in the latest
// domain. BlockNum and Seq are restored from the physical changeset key.
// BlockHash is block-level context: tx-range iteration restores it from the
// corresponding StateTxRange row, while direct block iteration leaves it zero.
// KV-latest values are semantic account-KV payloads, with the physical presence
// wrapper applied only when rows or commitment leaves are restored.
type StateDomainChange struct {
	BlockNum   uint64
	BlockHash  common.Hash
	TxNum      uint64
	Seq        uint64
	FlatDomain StateFlatDomain
	Owner      common.Address
	Generation uint64
	Domain     kvdomains.KVDomain
	Key        []byte
	PrevExists bool
	Prev       []byte
	NextExists bool
	Next       []byte
}

// persistedStateDomainChange is the hot-history representation. Like Erigon's
// DomainBufferedWriter/AddPrevValue path, history stores the value from before
// the mutation; the current value lives once in the flat latest domain. Keeping
// Next on StateDomainChange remains useful while commit capture compares the
// before/after images, but it is intentionally absent from persisted history.
// BlockNum and Seq are encoded in the physical key and BlockHash is stored once
// in StateTxRange, so none of that common block context is repeated per row.
type persistedStateDomainChange struct {
	TxNum      uint64
	FlatDomain StateFlatDomain
	Owner      common.Address
	Generation uint64
	Domain     kvdomains.KVDomain
	Key        []byte
	PrevExists bool
	Prev       []byte
}

const persistedStateDomainChangeBlockVersion = uint8(1)

const (
	stateDomainChangeBlockCompressionMinBytes = 1024
	stateDomainChangeBlockMaxDecodedBytes     = 128 << 20
	stateDomainChangeBlockPooledBufferMax     = 4 << 20
	stateDomainChangeBlockSnappyVersion       = byte(1)
)

// RLP lists always start at 0xc0 or above, so a leading zero is an
// unambiguous envelope marker beside every uncompressed/legacy representation.
var stateDomainChangeBlockEnvelopeMagic = [...]byte{0x00, 'g', 't', 'c', 's'}

// persistedStateDomainChangeBlock removes one physical Pebble key per state
// mutation while preserving the exact transaction/sequence order needed by
// unwind and temporal reads. Sequence zero in the block prefix owns this
// container; FirstSeq makes the format self-describing for repair/import tools.
type persistedStateDomainChangeBlock struct {
	Version  uint8
	FirstSeq uint64
	Rows     []persistedStateDomainChange
}

// legacyPersistedStateDomainChange is the previous-image-only layout emitted
// before block context was hoisted out of each row. It is read-only transition
// support for a node that restarts on the preceding test database.
type legacyPersistedStateDomainChange struct {
	BlockNum   uint64
	BlockHash  common.Hash
	TxNum      uint64
	Seq        uint64
	FlatDomain StateFlatDomain
	Owner      common.Address
	Generation uint64
	Domain     kvdomains.KVDomain
	Key        []byte
	PrevExists bool
	Prev       []byte
}

type StateKVHistoryReader interface {
	ethdb.KeyValueReader
	ethdb.Iteratee
}

type stateKVHistoryReader = StateKVHistoryReader

func WriteStateTxRange(db ethdb.KeyValueWriter, blockNum uint64, blockHash common.Hash, beginTxNum, endTxNum uint64) error {
	if endTxNum < beginTxNum {
		return fmt.Errorf("rawdb: invalid state tx range for block %d: [%d,%d]", blockNum, beginTxNum, endTxNum)
	}
	row := &StateTxRange{
		BlockNum:   blockNum,
		BlockHash:  blockHash,
		BeginTxNum: beginTxNum,
		EndTxNum:   endTxNum,
	}
	data, err := rlp.EncodeToBytes(row)
	if err != nil {
		return err
	}
	return db.Put(stateTxRangeKey(blockNum), data)
}

func ReadStateTxRange(db ethdb.KeyValueReader, blockNum uint64) (*StateTxRange, bool, error) {
	data, ok, err := readPresentValue(db, stateTxRangeKey(blockNum), fmt.Sprintf("state tx range for block %d", blockNum))
	if err != nil || !ok {
		return nil, ok, err
	}
	var row StateTxRange
	if err := rlp.DecodeBytes(data, &row); err != nil {
		return nil, false, err
	}
	return &row, true, nil
}

// StateTxNumAtBlockEnd returns the txNum that represents end-of-block state.
// Blocks written before txNum ranges existed fall back to the legacy blockNum
// value so old block-scoped change rows keep their original ordering.
func StateTxNumAtBlockEnd(db ethdb.KeyValueReader, blockNum uint64) (uint64, error) {
	_, endTxNum, err := stateBlockTxRange(db, blockNum)
	return endTxNum, err
}

func DeleteStateTxRange(db ethdb.KeyValueWriter, blockNum uint64) error {
	return db.Delete(stateTxRangeKey(blockNum))
}

func IterateStateTxRanges(db ethdb.Iteratee, fn func(*StateTxRange) (bool, error)) error {
	return iterateStateTxRanges(db, nil, 0, false, fn)
}

// IterateStateTxRangesByBlockRange seeks directly to fromBlock and walks the
// inclusive physical block range [fromBlock, toBlock]. Cold history builders
// already know their block boundaries, so using them avoids rescanning every
// tx-range row from genesis for each successive immutable segment.
func IterateStateTxRangesByBlockRange(db ethdb.Iteratee, fromBlock, toBlock uint64, fn func(*StateTxRange) (bool, error)) error {
	if toBlock < fromBlock {
		return fmt.Errorf("rawdb: inverted state tx block range [%d,%d]", fromBlock, toBlock)
	}
	var start [8]byte
	binary.BigEndian.PutUint64(start[:], fromBlock)
	return iterateStateTxRanges(db, start[:], toBlock, true, fn)
}

func iterateStateTxRanges(db ethdb.Iteratee, start []byte, toBlock uint64, bounded bool, fn func(*StateTxRange) (bool, error)) error {
	it := db.NewIterator(stateTxRangePrefix, start)
	defer it.Release()
	for it.Next() {
		key := it.Key()
		if !bytes.HasPrefix(key, stateTxRangePrefix) || len(key) != len(stateTxRangePrefix)+8 {
			continue
		}
		blockNum := binary.BigEndian.Uint64(key[len(stateTxRangePrefix):])
		if bounded && blockNum > toBlock {
			return nil
		}
		var row StateTxRange
		if err := rlp.DecodeBytes(it.Value(), &row); err != nil {
			return err
		}
		cont, err := fn(&row)
		if err != nil {
			return err
		}
		if !cont {
			return nil
		}
	}
	return it.Error()
}

func WriteStateDomainChange(db ethdb.KeyValueWriter, change *StateDomainChange) error {
	if err := WriteStateDomainChangeRow(db, change); err != nil {
		return err
	}
	return WriteStateDomainChangePostingIndex(db, change)
}

// WriteStateDomainChangeRow writes the block-scoped temporal mutation row
// without publishing its latest-key posting index. Staged publishers use this
// with WriteStateDomainChangePostingIndex so hot history row and accessor
// publication are explicit domain-stage steps.
func WriteStateDomainChangeRow(db ethdb.KeyValueWriter, change *StateDomainChange) error {
	if change == nil {
		return errors.New("rawdb: nil StateDomainChange")
	}
	if err := validateStateDomainChange(change); err != nil {
		return err
	}
	data, err := encodePersistedStateDomainChange(change)
	if err != nil {
		return err
	}
	observeStateChangeEncoding(change, data)
	return db.Put(stateChangeSetKey(change.BlockNum, change.Seq), data)
}

// WriteStateDomainChangeBlockRows publishes a complete block's authoritative
// history as one value. Canonical execution calls this only after every tx and
// block-final mutation has succeeded, so no reader can observe a partial pack.
// Positive-sequence single rows remain readable for restart/repair compatibility.
func WriteStateDomainChangeBlockRows(db ethdb.KeyValueWriter, changes []*StateDomainChange) error {
	if len(changes) == 0 {
		return nil
	}
	first := changes[0]
	if first == nil {
		return errors.New("rawdb: nil StateDomainChange at block pack index 0")
	}
	blockNum := first.BlockNum
	firstSeq := first.Seq
	if firstSeq == 0 {
		return fmt.Errorf("rawdb: state domain change block %d starts at reserved sequence zero", blockNum)
	}
	rows := make([]persistedStateDomainChange, len(changes))
	previousTxNum := first.TxNum
	for i, change := range changes {
		if change == nil {
			return fmt.Errorf("rawdb: nil StateDomainChange at block pack index %d", i)
		}
		if change.BlockNum != blockNum {
			return fmt.Errorf("rawdb: state domain change block pack crosses blocks %d and %d", blockNum, change.BlockNum)
		}
		wantSeq := firstSeq + uint64(i)
		if wantSeq < firstSeq || change.Seq != wantSeq {
			return fmt.Errorf("rawdb: state domain change block %d sequence %d at index %d, want %d", blockNum, change.Seq, i, wantSeq)
		}
		if i > 0 && change.TxNum < previousTxNum {
			return fmt.Errorf("rawdb: state domain change block %d txNum %d at sequence %d follows txNum %d", blockNum, change.TxNum, change.Seq, previousTxNum)
		}
		if err := validateStateDomainChange(change); err != nil {
			return err
		}
		previousTxNum = change.TxNum
		rows[i] = persistedStateDomainChange{
			TxNum:      change.TxNum,
			FlatDomain: change.FlatDomain,
			Owner:      change.Owner,
			Generation: change.Generation,
			Domain:     change.Domain,
			Key:        change.Key,
			PrevExists: change.PrevExists,
			Prev:       change.Prev,
		}
		observeStateChangeEncodingLazy(change)
	}
	rawBuffer := stateChangeBlockRawBufferPool.Get().(*bytes.Buffer)
	rawBuffer.Reset()
	if err := rlp.Encode(rawBuffer, &persistedStateDomainChangeBlock{
		Version:  persistedStateDomainChangeBlockVersion,
		FirstSeq: firstSeq,
		Rows:     rows,
	}); err != nil {
		if rawBuffer.Cap() <= stateDomainChangeBlockPooledBufferMax {
			stateChangeBlockRawBufferPool.Put(rawBuffer)
		}
		return err
	}
	uncompressedBytes := rawBuffer.Len()
	data, compressed := encodeStateDomainChangeBlockStorage(rawBuffer.Bytes())
	if compressed && rawBuffer.Cap() <= stateDomainChangeBlockPooledBufferMax {
		stateChangeBlockRawBufferPool.Put(rawBuffer)
	}
	physicalKey := stateChangeSetKey(blockNum, 0)
	if err := db.Put(physicalKey, data); err != nil {
		return err
	}
	stateChangeBlockPackBlocksCounter.Inc(1)
	stateChangeBlockPackRowsCounter.Inc(int64(len(changes)))
	stateChangeBlockPackEncodedBytesCounter.Inc(int64(len(data)))
	stateChangeBlockPackLogicalBytesCounter.Inc(int64(len(physicalKey) + len(data)))
	stateChangeBlockPackUncompressedCounter.Inc(int64(uncompressedBytes))
	if compressed {
		stateChangeBlockPackCompressedCounter.Inc(1)
		stateChangeBlockPackCompressionSavedCounter.Inc(int64(uncompressedBytes - len(data)))
	} else {
		stateChangeBlockPackRawCounter.Inc(1)
	}
	if avoided := len(changes) - 1; avoided > 0 {
		stateChangeBlockPackWritesAvoidedCounter.Inc(int64(avoided))
		stateChangeBlockPackKeyBytesAvoidedCounter.Inc(int64(avoided * len(physicalKey)))
	}
	return nil
}

func encodeStateDomainChangeBlockStorage(raw []byte) ([]byte, bool) {
	if len(raw) < stateDomainChangeBlockCompressionMinBytes {
		return raw, false
	}
	maxEncoded := snappy.MaxEncodedLen(len(raw))
	if maxEncoded < 0 {
		return raw, false
	}
	headerLen := len(stateDomainChangeBlockEnvelopeMagic) + 1
	buf := make([]byte, headerLen+maxEncoded)
	encoded := snappy.Encode(buf[headerLen:], raw)
	storedLen := headerLen + len(encoded)
	// Hot compression must buy at least 12.5%. Smaller wins do not justify
	// paying decompression on unwind, cold build, and history index rebuild.
	if storedLen*8 >= len(raw)*7 {
		return raw, false
	}
	copy(buf, stateDomainChangeBlockEnvelopeMagic[:])
	buf[len(stateDomainChangeBlockEnvelopeMagic)] = stateDomainChangeBlockSnappyVersion
	return buf[:storedLen], true
}

func decodeStateDomainChangeBlockStorage(data []byte) ([]byte, error) {
	payload, _, compressed, err := stateDomainChangeBlockCompressionPayload(data)
	if err != nil || !compressed {
		return payload, err
	}
	decoded, err := snappy.Decode(nil, payload)
	if err != nil {
		return nil, fmt.Errorf("rawdb: decode compressed state domain change block: %w", err)
	}
	return decoded, nil
}

func stateDomainChangeBlockCompressionPayload(data []byte) (payload []byte, decodedLen int, compressed bool, err error) {
	if len(data) < len(stateDomainChangeBlockEnvelopeMagic) || !bytes.Equal(data[:len(stateDomainChangeBlockEnvelopeMagic)], stateDomainChangeBlockEnvelopeMagic[:]) {
		return data, 0, false, nil
	}
	if len(data) == len(stateDomainChangeBlockEnvelopeMagic) {
		return nil, 0, false, fmt.Errorf("rawdb: truncated state domain change block compression envelope")
	}
	version := data[len(stateDomainChangeBlockEnvelopeMagic)]
	if version != stateDomainChangeBlockSnappyVersion {
		return nil, 0, false, fmt.Errorf("rawdb: unsupported state domain change block compression version %d", version)
	}
	payload = data[len(stateDomainChangeBlockEnvelopeMagic)+1:]
	decodedLen, err = snappy.DecodedLen(payload)
	if err != nil {
		return nil, 0, false, fmt.Errorf("rawdb: decode compressed state domain change block length: %w", err)
	}
	if decodedLen <= 0 || decodedLen > stateDomainChangeBlockMaxDecodedBytes {
		return nil, 0, false, fmt.Errorf("rawdb: compressed state domain change block decoded size %d exceeds limit", decodedLen)
	}
	return payload, decodedLen, true, nil
}

// observeStateChangeEncoding attributes the encoded previous-image-only row
// without decoding or allocating. The fixed bucket contains tx/domain/owner
// metadata, flags, and RLP framing. omitted_next_bytes measures
// the transient forward image deliberately excluded from the persisted row;
// the legacy next_bytes/next_rows counters remain registered at zero so the
// deployment can be compared directly with the preceding binary.
func observeStateChangeEncoding(change *StateDomainChange, encoded []byte) {
	if change == nil || stateChangeEncodingSampleSequence.Add(1)%stateChangeEncodingSampleInterval != 1 {
		return
	}
	observeStateChangeEncodingSample(change, encoded)
}

func observeStateChangeEncodingLazy(change *StateDomainChange) {
	if change == nil || stateChangeEncodingSampleSequence.Add(1)%stateChangeEncodingSampleInterval != 1 {
		return
	}
	encoded, err := encodePersistedStateDomainChange(change)
	if err != nil {
		return
	}
	observeStateChangeEncodingSample(change, encoded)
}

func observeStateChangeEncodingSample(change *StateDomainChange, encoded []byte) {
	keyBytes := len(change.Key)
	prevBytes := len(change.Prev)
	omittedNextBytes := len(change.Next)
	fixedBytes := len(encoded) - keyBytes - prevBytes
	if fixedBytes < 0 {
		fixedBytes = 0
	}
	stateChangeEncodingSampleRowsCounter.Inc(1)
	stateChangeEncodingSampleEncodedCounter.Inc(int64(len(encoded)))
	stateChangeEncodingSampleKeyCounter.Inc(int64(keyBytes))
	stateChangeEncodingSamplePrevCounter.Inc(int64(prevBytes))
	stateChangeEncodingSampleOmittedNextCounter.Inc(int64(omittedNextBytes))
	stateChangeEncodingSampleFixedCounter.Inc(int64(fixedBytes))
	if change.PrevExists {
		stateChangeEncodingSamplePrevRows.Inc(1)
	}
	if change.NextExists {
		stateChangeEncodingSampleOmittedNextRows.Inc(1)
	}
}

func encodePersistedStateDomainChange(change *StateDomainChange) ([]byte, error) {
	return rlp.EncodeToBytes(&persistedStateDomainChange{
		TxNum:      change.TxNum,
		FlatDomain: change.FlatDomain,
		Owner:      change.Owner,
		Generation: change.Generation,
		Domain:     change.Domain,
		Key:        change.Key,
		PrevExists: change.PrevExists,
		Prev:       change.Prev,
	})
}

func decodePersistedStateDomainChange(data []byte, blockNum, seq uint64) (*StateDomainChange, error) {
	var row persistedStateDomainChange
	if err := rlp.DecodeBytes(data, &row); err == nil {
		return &StateDomainChange{
			BlockNum:   blockNum,
			TxNum:      row.TxNum,
			Seq:        seq,
			FlatDomain: row.FlatDomain,
			Owner:      row.Owner,
			Generation: row.Generation,
			Domain:     row.Domain,
			Key:        append([]byte(nil), row.Key...),
			PrevExists: row.PrevExists,
			Prev:       append([]byte(nil), row.Prev...),
		}, nil
	}
	var prior legacyPersistedStateDomainChange
	if err := rlp.DecodeBytes(data, &prior); err == nil {
		return &StateDomainChange{
			BlockNum:   prior.BlockNum,
			BlockHash:  prior.BlockHash,
			TxNum:      prior.TxNum,
			Seq:        prior.Seq,
			FlatDomain: prior.FlatDomain,
			Owner:      prior.Owner,
			Generation: prior.Generation,
			Domain:     prior.Domain,
			Key:        append([]byte(nil), prior.Key...),
			PrevExists: prior.PrevExists,
			Prev:       append([]byte(nil), prior.Prev...),
		}, nil
	}
	// Read-only transition for the currently running test database. Fresh rows
	// never take this path; once the node is reset, the legacy decoder can be
	// deleted without another format migration.
	var legacy StateDomainChange
	if err := rlp.DecodeBytes(data, &legacy); err != nil {
		return nil, err
	}
	return cloneStateDomainChange(&legacy), nil
}

func decodePersistedStateDomainChangeBlock(data []byte, blockNum uint64) ([]*StateDomainChange, error) {
	payload, decodedLen, compressed, err := stateDomainChangeBlockCompressionPayload(data)
	if err != nil {
		return nil, err
	}
	decoded := payload
	var pooled *[]byte
	if compressed {
		pooled = stateChangeBlockDecodeBufferPool.Get().(*[]byte)
		if cap(*pooled) < decodedLen {
			*pooled = make([]byte, decodedLen)
		} else {
			*pooled = (*pooled)[:decodedLen]
		}
		decoded, err = snappy.Decode(*pooled, payload)
		if err != nil {
			if cap(*pooled) <= stateDomainChangeBlockPooledBufferMax {
				*pooled = (*pooled)[:0]
				stateChangeBlockDecodeBufferPool.Put(pooled)
			}
			return nil, fmt.Errorf("rawdb: decode compressed state domain change block: %w", err)
		}
	}
	var block persistedStateDomainChangeBlock
	if err := rlp.DecodeBytes(decoded, &block); err != nil {
		if pooled != nil && cap(*pooled) <= stateDomainChangeBlockPooledBufferMax {
			*pooled = (*pooled)[:0]
			stateChangeBlockDecodeBufferPool.Put(pooled)
		}
		return nil, err
	}
	if pooled != nil && cap(*pooled) <= stateDomainChangeBlockPooledBufferMax {
		*pooled = (*pooled)[:0]
		stateChangeBlockDecodeBufferPool.Put(pooled)
	}
	if block.Version != persistedStateDomainChangeBlockVersion {
		return nil, fmt.Errorf("rawdb: unsupported state domain change block version %d", block.Version)
	}
	if len(block.Rows) == 0 {
		return nil, fmt.Errorf("rawdb: empty state domain change block pack for block %d", blockNum)
	}
	if block.FirstSeq == 0 || uint64(len(block.Rows)-1) > ^uint64(0)-block.FirstSeq {
		return nil, fmt.Errorf("rawdb: invalid state domain change block sequence range for block %d", blockNum)
	}
	changes := make([]*StateDomainChange, len(block.Rows))
	for i := range block.Rows {
		row := &block.Rows[i]
		changes[i] = &StateDomainChange{
			BlockNum:   blockNum,
			TxNum:      row.TxNum,
			Seq:        block.FirstSeq + uint64(i),
			FlatDomain: row.FlatDomain,
			Owner:      row.Owner,
			Generation: row.Generation,
			Domain:     row.Domain,
			Key:        row.Key,
			PrevExists: row.PrevExists,
			Prev:       row.Prev,
		}
	}
	return changes, nil
}

// WriteStateDomainChangePostingIndex writes the compact exact-key candidate
// index for an already materialized StateDomainChange row. Live canonical
// blocks use one immutable frame; bulk sync combines frames in its stage.
func WriteStateDomainChangePostingIndex(db ethdb.KeyValueWriter, change *StateDomainChange) error {
	if change == nil {
		return errors.New("rawdb: nil StateDomainChange")
	}
	if err := validateStateDomainChange(change); err != nil {
		return err
	}
	latestKey, err := stateDomainChangeLatestKey(change)
	if err != nil {
		return err
	}
	return writeStateChangePostingIndex(db, latestKey, change.BlockNum)
}

func validateStateDomainChange(change *StateDomainChange) error {
	if change == nil {
		return errors.New("rawdb: nil StateDomainChange")
	}
	if _, err := stateDomainChangeLatestKey(change); err != nil {
		return err
	}
	if change.PrevExists {
		if _, err := stateDomainChangeCommitmentValue(change, change.Prev); err != nil {
			return err
		}
	}
	if change.NextExists {
		if _, err := stateDomainChangeCommitmentValue(change, change.Next); err != nil {
			return err
		}
	}
	return nil
}

func ReadStateDomainChange(db ethdb.KeyValueReader, blockNum, seq uint64) (*StateDomainChange, bool, error) {
	// A positive-sequence repair/transition row has the same overwrite
	// precedence it had before block packs existed.
	if seq != 0 {
		data, ok, err := readPresentValue(db, stateChangeSetKey(blockNum, seq), fmt.Sprintf("state domain change for block %d seq %d", blockNum, seq))
		if err != nil {
			return nil, false, err
		}
		if ok {
			row, err := decodePersistedStateDomainChange(data, blockNum, seq)
			if err != nil {
				return nil, false, err
			}
			return row, true, nil
		}
	}
	packed, packedOK, err := readPresentValue(db, stateChangeSetKey(blockNum, 0), fmt.Sprintf("packed state domain changes for block %d", blockNum))
	if err != nil {
		return nil, false, err
	}
	if packedOK {
		changes, err := decodePersistedStateDomainChangeBlock(packed, blockNum)
		if err == nil {
			firstSeq := changes[0].Seq
			if seq >= firstSeq && seq-firstSeq < uint64(len(changes)) {
				return changes[seq-firstSeq], true, nil
			}
			return nil, false, nil
		}
		// Sequence zero was not reserved by the legacy schema. A few repair
		// and fixture writers used it for an ordinary row, so only treat the
		// key as a block pack when the versioned container decodes strictly.
		if seq == 0 {
			row, legacyErr := decodePersistedStateDomainChange(packed, blockNum, 0)
			if legacyErr != nil {
				return nil, false, err
			}
			return row, true, nil
		}
	}
	return nil, false, nil
}

func IterateStateDomainChanges(db ethdb.Iteratee, blockNum uint64, fn func(*StateDomainChange) (bool, error)) error {
	prefix := stateChangeSetBlockPrefix(blockNum)
	it := db.NewIterator(prefix, nil)
	defer it.Release()
	var packed []*StateDomainChange
	var packedExtras []*StateDomainChange
	for it.Next() {
		key := it.Key()
		if !bytes.HasPrefix(key, prefix) || len(key) != len(stateChangeSetPrefix)+16 {
			continue
		}
		seq := binary.BigEndian.Uint64(key[len(stateChangeSetPrefix)+8:])
		if seq == 0 {
			changes, err := decodePersistedStateDomainChangeBlock(it.Value(), blockNum)
			if err == nil {
				packed = changes
				continue
			}
			row, legacyErr := decodePersistedStateDomainChange(it.Value(), blockNum, 0)
			if legacyErr != nil {
				return err
			}
			cont, err := fn(row)
			if err != nil {
				return err
			}
			if !cont {
				return nil
			}
			continue
		}
		row, err := decodePersistedStateDomainChange(it.Value(), blockNum, seq)
		if err != nil {
			return err
		}
		if len(packed) > 0 {
			firstSeq := packed[0].Seq
			if seq >= firstSeq && seq-firstSeq < uint64(len(packed)) {
				// Preserve the old physical-key overwrite rule when a repair or
				// transition writer adds positive rows beside a block pack.
				packed[seq-firstSeq] = row
			} else {
				packedExtras = append(packedExtras, row)
			}
			continue
		}
		cont, err := fn(row)
		if err != nil {
			return err
		}
		if !cont {
			return nil
		}
	}
	if err := it.Error(); err != nil {
		return err
	}
	if len(packedExtras) > 0 {
		packed = append(packed, packedExtras...)
		sort.Slice(packed, func(i, j int) bool { return packed[i].Seq < packed[j].Seq })
	}
	for _, row := range packed {
		cont, err := fn(row)
		if err != nil {
			return err
		}
		if !cont {
			return nil
		}
	}
	return nil
}

// IterateStateDomainChangesByBlockRange walks the authoritative logical
// changeset view for every block in the inclusive range [fromBlock, toBlock]
// with one ordered iterator. Historical reads use this for the unindexed stage
// tail: opening one iterator for the whole tail avoids rebuilding Pebble and
// blockbuffer overlay iterator state once per block while preserving the same
// block-pack/positive-sequence overwrite rules as IterateStateDomainChanges.
func IterateStateDomainChangesByBlockRange(db ethdb.Iteratee, fromBlock, toBlock uint64, fn func(*StateDomainChange) (bool, error)) error {
	if fromBlock > toBlock {
		return nil
	}
	var start [8]byte
	binary.BigEndian.PutUint64(start[:], fromBlock)
	it := db.NewIterator(stateChangeSetPrefix, start[:])
	defer it.Release()

	var (
		blockNum     uint64
		haveBlock    bool
		packed       []*StateDomainChange
		packedExtras []*StateDomainChange
	)
	flushBlock := func() (bool, error) {
		if len(packedExtras) > 0 {
			packed = append(packed, packedExtras...)
			sort.Slice(packed, func(i, j int) bool { return packed[i].Seq < packed[j].Seq })
		}
		for _, row := range packed {
			cont, err := fn(row)
			if err != nil || !cont {
				return cont, err
			}
		}
		packed = nil
		packedExtras = nil
		return true, nil
	}

	for it.Next() {
		key := it.Key()
		if !bytes.HasPrefix(key, stateChangeSetPrefix) || len(key) != len(stateChangeSetPrefix)+16 {
			continue
		}
		candidateBlock := binary.BigEndian.Uint64(key[len(stateChangeSetPrefix):])
		if candidateBlock < fromBlock {
			continue
		}
		if candidateBlock > toBlock {
			break
		}
		if !haveBlock || candidateBlock != blockNum {
			if haveBlock {
				cont, err := flushBlock()
				if err != nil || !cont {
					return err
				}
			}
			blockNum = candidateBlock
			haveBlock = true
		}

		seq := binary.BigEndian.Uint64(key[len(stateChangeSetPrefix)+8:])
		if seq == 0 {
			changes, err := decodePersistedStateDomainChangeBlock(it.Value(), blockNum)
			if err == nil {
				packed = changes
				continue
			}
			row, legacyErr := decodePersistedStateDomainChange(it.Value(), blockNum, 0)
			if legacyErr != nil {
				return err
			}
			cont, callbackErr := fn(row)
			if callbackErr != nil || !cont {
				return callbackErr
			}
			continue
		}

		row, err := decodePersistedStateDomainChange(it.Value(), blockNum, seq)
		if err != nil {
			return err
		}
		if len(packed) > 0 {
			firstSeq := packed[0].Seq
			if seq >= firstSeq && seq-firstSeq < uint64(len(packed)) {
				packed[seq-firstSeq] = row
			} else {
				packedExtras = append(packedExtras, row)
			}
			continue
		}
		cont, callbackErr := fn(row)
		if callbackErr != nil || !cont {
			return callbackErr
		}
	}
	if err := it.Error(); err != nil {
		return err
	}
	if haveBlock {
		_, err := flushBlock()
		return err
	}
	return nil
}

// iteratePhysicalStateDomainChanges includes shadowed rows from both the block
// pack and positive-sequence repair representation. Logical readers use the
// overwrite view above; deletion needs the union so no live posting survives.
func iteratePhysicalStateDomainChanges(db ethdb.Iteratee, blockNum uint64, fn func(*StateDomainChange) (bool, error)) error {
	prefix := stateChangeSetBlockPrefix(blockNum)
	it := db.NewIterator(prefix, nil)
	defer it.Release()
	for it.Next() {
		key := it.Key()
		if !bytes.HasPrefix(key, prefix) || len(key) != len(stateChangeSetPrefix)+16 {
			continue
		}
		seq := binary.BigEndian.Uint64(key[len(stateChangeSetPrefix)+8:])
		if seq == 0 {
			if changes, err := decodePersistedStateDomainChangeBlock(it.Value(), blockNum); err == nil {
				for _, change := range changes {
					cont, err := fn(change)
					if err != nil || !cont {
						return err
					}
				}
				continue
			}
		}
		change, err := decodePersistedStateDomainChange(it.Value(), blockNum, seq)
		if err != nil {
			return err
		}
		cont, err := fn(change)
		if err != nil || !cont {
			return err
		}
	}
	return it.Error()
}

// iteratePhysicalStateDomainChangesBorrowed is the pruning counterpart of the
// owning compatibility iterator above. Current sequence-zero block packs are
// decoded into one reusable row whose byte fields alias the iterator value.
// Retired standalone rows still take the owning transition decoder so restart
// compatibility and repair-row union semantics remain unchanged.
func iteratePhysicalStateDomainChangesBorrowed(db ethdb.Iteratee, blockNum uint64, fn func(*StateDomainChange) (bool, error)) error {
	prefix := stateChangeSetBlockPrefix(blockNum)
	it := db.NewIterator(prefix, nil)
	defer it.Release()
	var scratch StateDomainChange
	for it.Next() {
		key := it.Key()
		if !bytes.HasPrefix(key, prefix) || len(key) != len(stateChangeSetPrefix)+16 {
			continue
		}
		seq := binary.BigEndian.Uint64(key[len(stateChangeSetPrefix)+8:])
		if seq == 0 {
			var callbackErr error
			sawPackRow := false
			cont, packErr := iteratePersistedStateDomainChangeBlockBorrowedWithScratch(it.Value(), blockNum, &scratch, func(change *StateDomainChange) (bool, error) {
				sawPackRow = true
				var keepGoing bool
				keepGoing, callbackErr = fn(change)
				return keepGoing, callbackErr
			})
			if callbackErr != nil {
				return callbackErr
			}
			if packErr == nil {
				if !cont {
					return nil
				}
				continue
			}
			// A malformed pack must not be reinterpreted after callbacks have
			// observed any of its rows. Before the first row, sequence zero may
			// still be an ordinary row written by the transition schema.
			if sawPackRow {
				return packErr
			}
			change, legacyErr := decodePersistedStateDomainChange(it.Value(), blockNum, 0)
			if legacyErr != nil {
				return packErr
			}
			cont, err := fn(change)
			if err != nil || !cont {
				return err
			}
			continue
		}
		change, err := decodePersistedStateDomainChange(it.Value(), blockNum, seq)
		if err != nil {
			return err
		}
		cont, err := fn(change)
		if err != nil || !cont {
			return err
		}
	}
	return it.Error()
}

// IterateStateDomainChangesByTxRange walks StateDomainChange rows whose
// TxNum is inside [fromTxNum, toTxNum]. StateTxRange rows provide the block to
// txNum mapping, so callers can build txNum-native history files without
// scanning unrelated blocks.
func IterateStateDomainChangesByTxRange(db ethdb.Iteratee, fromTxNum, toTxNum uint64, fn func(*StateDomainChange) (bool, error)) error {
	if toTxNum < fromTxNum {
		return fmt.Errorf("rawdb: inverted state domain change tx range [%d,%d]", fromTxNum, toTxNum)
	}
	return IterateStateTxRanges(db, func(row *StateTxRange) (bool, error) {
		if row.EndTxNum < fromTxNum || row.BeginTxNum > toTxNum {
			return true, nil
		}
		return iterateStateDomainChangesForTxRange(db, row, fromTxNum, toTxNum, fn)
	})
}

// IterateStateDomainChangesByBlockTxRange is the bounded cold-build form of
// IterateStateDomainChangesByTxRange. It seeks to fromBlock instead of walking
// the monotonically growing StateTxRange prefix from genesis on every build.
func IterateStateDomainChangesByBlockTxRange(db ethdb.Iteratee, fromBlock, toBlock, fromTxNum, toTxNum uint64, fn func(*StateDomainChange) (bool, error)) error {
	if toTxNum < fromTxNum {
		return fmt.Errorf("rawdb: inverted state domain change tx range [%d,%d]", fromTxNum, toTxNum)
	}
	type txRangeBlockHash struct {
		blockNum  uint64
		blockHash common.Hash
	}
	var ranges []txRangeBlockHash
	if err := IterateStateTxRangesByBlockRange(db, fromBlock, toBlock, func(row *StateTxRange) (bool, error) {
		if row.EndTxNum < fromTxNum || row.BeginTxNum > toTxNum {
			return true, nil
		}
		ranges = append(ranges, txRangeBlockHash{blockNum: row.BlockNum, blockHash: row.BlockHash})
		return true, nil
	}); err != nil {
		return err
	}
	if len(ranges) == 0 {
		return nil
	}
	rangeIndex := 0
	return IterateStateDomainChangesByBlockRange(db, fromBlock, toBlock, func(change *StateDomainChange) (bool, error) {
		for rangeIndex < len(ranges) && ranges[rangeIndex].blockNum < change.BlockNum {
			rangeIndex++
		}
		if rangeIndex >= len(ranges) || ranges[rangeIndex].blockNum != change.BlockNum || change.TxNum < fromTxNum || change.TxNum > toTxNum {
			return true, nil
		}
		change.BlockHash = ranges[rangeIndex].blockHash
		return fn(change)
	})
}

func iterateStateDomainChangesForTxRange(db ethdb.Iteratee, row *StateTxRange, fromTxNum, toTxNum uint64, fn func(*StateDomainChange) (bool, error)) (bool, error) {
	if err := IterateStateDomainChanges(db, row.BlockNum, func(change *StateDomainChange) (bool, error) {
		if change.TxNum < fromTxNum || change.TxNum > toTxNum {
			return true, nil
		}
		change.BlockHash = row.BlockHash
		return fn(change)
	}); err != nil {
		return false, err
	}
	return true, nil
}

func DeleteStateDomainChanges(db stateKVLatestStore, blockNum uint64) error {
	// A block records every mutation, but its posting index contains only one
	// candidate per physical latest key. Collapse repeated writes before doing
	// point reads/deletes; smart-contract blocks frequently touch the same slot
	// many times across transactions.
	var postingDeduper stateChangePostingDeduper
	postingDeduper.Reset()
	latestKeyScratch := make([]byte, 0, 128)
	postingKeyScratch := make([]byte, 0, len(stateChangePostingPrefix)+sha256.Size+8)
	if err := iteratePhysicalStateDomainChangesBorrowed(db, blockNum, func(change *StateDomainChange) (bool, error) {
		var err error
		latestKeyScratch, err = appendStateDomainChangeLatestKey(latestKeyScratch[:0], change)
		if err != nil {
			return false, err
		}
		hash := stateChangePostingHash(latestKeyScratch)
		if postingDeduper.Seen(hash) {
			return true, nil
		}
		postingKeyScratch, err = deleteLiveStateChangePostingByHash(db, hash, blockNum, postingKeyScratch)
		if err != nil {
			return false, err
		}
		return true, nil
	}); err != nil {
		return err
	}
	// Domain pruning deletes a small per-block prefix repeatedly while a node
	// is syncing. Use point deletes here: Pebble range tombstones are excellent
	// for one-shot resets, but high-frequency per-block DeleteRange calls make
	// every later iterator pay keyspan-fragment costs.
	return deleteStateKVPrefixByPointScan(db, stateChangeSetBlockPrefix(blockNum))
}

// DeleteStateDomainChangeBlocks deletes an increasing set of physical history
// blocks with one changeset iterator. Pruning normally removes thousands of
// adjacent current-schema block packs; opening and seeking a new iterator for
// every block makes Pebble repeat the same table/index work. The scan still
// visits the physical union of packed and standalone repair rows so inverse
// posting cleanup has exactly the same semantics as DeleteStateDomainChanges.
func DeleteStateDomainChangeBlocks(db stateKVLatestStore, blockNums []uint64) error {
	if len(blockNums) == 0 {
		return nil
	}
	for i := 1; i < len(blockNums); i++ {
		if blockNums[i] <= blockNums[i-1] {
			return fmt.Errorf("rawdb: state domain change delete blocks are not strictly increasing at %d after %d", blockNums[i], blockNums[i-1])
		}
	}
	// The prune runner supplies a dense prefix. Keep the exported helper safe
	// for repair callers with widely separated blocks: scanning every physical
	// changeset between sparse endpoints would be worse than precise seeks.
	spanMinusOne := blockNums[len(blockNums)-1] - blockNums[0]
	if spanMinusOne > uint64(len(blockNums))*4 {
		for _, blockNum := range blockNums {
			if err := DeleteStateDomainChanges(db, blockNum); err != nil {
				return err
			}
		}
		return nil
	}
	var start [8]byte
	binary.BigEndian.PutUint64(start[:], blockNums[0])
	it := db.NewIterator(stateChangeSetPrefix, start[:])
	defer it.Release()
	var (
		blockIndex        int
		scratch           StateDomainChange
		latestKeyScratch  []byte
		postingKeyScratch []byte
		postingDeduper    stateChangePostingDeduper
		postingBlock      uint64
		havePostingBlock  bool
	)
	deletePosting := func(change *StateDomainChange) (bool, error) {
		if !havePostingBlock || postingBlock != change.BlockNum {
			postingBlock = change.BlockNum
			havePostingBlock = true
			postingDeduper.Reset()
		}
		latestKey, err := appendStateDomainChangeLatestKey(latestKeyScratch[:0], change)
		if err != nil {
			return false, err
		}
		latestKeyScratch = latestKey
		hash := stateChangePostingHash(latestKey)
		if postingDeduper.Seen(hash) {
			return true, nil
		}
		postingKeyScratch, err = deleteLiveStateChangePostingByHash(db, hash, change.BlockNum, postingKeyScratch)
		if err != nil {
			return false, err
		}
		return true, nil
	}
	for it.Next() {
		key := it.Key()
		if !bytes.HasPrefix(key, stateChangeSetPrefix) || len(key) != len(stateChangeSetPrefix)+16 {
			continue
		}
		blockNum := binary.BigEndian.Uint64(key[len(stateChangeSetPrefix):])
		for blockIndex < len(blockNums) && blockNums[blockIndex] < blockNum {
			blockIndex++
		}
		if blockIndex >= len(blockNums) {
			break
		}
		if blockNums[blockIndex] != blockNum {
			continue
		}
		seq := binary.BigEndian.Uint64(key[len(stateChangeSetPrefix)+8:])
		value := it.Value()
		if seq == 0 {
			var callbackErr error
			sawPackRow := false
			_, packErr := iteratePersistedStateDomainChangeBlockBorrowedWithScratch(value, blockNum, &scratch, func(change *StateDomainChange) (bool, error) {
				sawPackRow = true
				var keepGoing bool
				keepGoing, callbackErr = deletePosting(change)
				return keepGoing, callbackErr
			})
			if callbackErr != nil {
				return callbackErr
			}
			if packErr == nil {
				// KeyValueWriter.Delete must consume/copy the iterator key before
				// returning. This matches the point-scan delete path and avoids
				// retaining one copied key per history row.
				if err := db.Delete(key); err != nil {
					return err
				}
				continue
			}
			if sawPackRow {
				return packErr
			}
			row, legacyErr := decodePersistedStateDomainChange(value, blockNum, 0)
			if legacyErr != nil {
				return packErr
			}
			if _, err := deletePosting(row); err != nil {
				return err
			}
		} else {
			row, err := decodePersistedStateDomainChange(value, blockNum, seq)
			if err != nil {
				return err
			}
			if _, err := deletePosting(row); err != nil {
				return err
			}
		}
		if err := db.Delete(key); err != nil {
			return err
		}
	}
	if err := it.Error(); err != nil {
		return err
	}
	return nil
}

func IterateStateDomainChangeBlocks(db ethdb.Iteratee, owner common.Address, generation uint64, domain kvdomains.KVDomain, key []byte, fn func(blockNum uint64) (bool, error)) error {
	return iterateStateDomainChangePostingBlocks(db, StateKVLatestCommitmentKey(owner, generation, domain, key), 0, ^uint64(0), fn)
}

func IterateStateKVGenerationChangeBlocks(db ethdb.Iteratee, owner common.Address, fn func(blockNum uint64) (bool, error)) error {
	return iterateStateDomainChangePostingBlocks(db, StateKVGenerationCommitmentKey(owner), 0, ^uint64(0), fn)
}

func IterateStateAccountLatestChangeBlocks(db ethdb.Iteratee, owner common.Address, fn func(blockNum uint64) (bool, error)) error {
	return iterateStateDomainChangePostingBlocks(db, StateAccountLatestCommitmentKey(owner), 0, ^uint64(0), fn)
}

func IterateStateDomainChangeBlocksByKey(db ethdb.Iteratee, flatDomain StateFlatDomain, owner common.Address, generation uint64, domain kvdomains.KVDomain, key []byte, fn func(blockNum uint64) (bool, error)) error {
	latestKey, ok := stateDomainChangeLatestKeyByKey(flatDomain, owner, generation, domain, key)
	if !ok {
		return nil
	}
	return iterateStateDomainChangePostingBlocks(db, latestKey, 0, ^uint64(0), fn)
}

// IterateStateDomainChangeBlocksByKeyRange walks exact-key posting candidates
// inside the inclusive block range [fromBlock, toBlock].
func IterateStateDomainChangeBlocksByKeyRange(db ethdb.Iteratee, fromBlock, toBlock uint64, flatDomain StateFlatDomain, owner common.Address, generation uint64, domain kvdomains.KVDomain, key []byte, fn func(blockNum uint64) (bool, error)) error {
	if fromBlock > toBlock {
		return nil
	}
	latestKey, ok := stateDomainChangeLatestKeyByKey(flatDomain, owner, generation, domain, key)
	if !ok {
		return nil
	}
	return iterateStateDomainChangePostingBlocks(db, latestKey, fromBlock, toBlock, fn)
}

// ReadFirstStateDomainChangeByKeyBlockRange seeks the earliest matching hot
// mutation after targetBlock. The posting reader stops after the first exact
// candidate whose original latest key matches the changeset.
func ReadFirstStateDomainChangeByKeyBlockRange(db StateKVHistoryReader, targetBlock, headBlock, targetTxNum, headTxNum uint64, flatDomain StateFlatDomain, owner common.Address, generation uint64, domain kvdomains.KVDomain, key []byte) (*StateDomainChange, error) {
	if targetBlock >= headBlock || targetTxNum >= headTxNum {
		return nil, nil
	}
	fromBlock := targetBlock + 1
	indexedHead, staged, err := stateHistoryIndexedHead(db, headBlock)
	if err != nil {
		return nil, err
	}
	if !staged {
		indexedHead = headBlock
	}
	for fromBlock <= indexedHead {
		blockNum, ok, err := firstStateDomainChangeBlockByKeyRange(db, fromBlock, indexedHead, staged, flatDomain, owner, generation, domain, key)
		if err != nil || !ok {
			return nil, err
		}
		var first *StateDomainChange
		if err := IterateStateDomainChanges(db, blockNum, func(change *StateDomainChange) (bool, error) {
			if !stateDomainChangeInTxWindow(change, targetTxNum, headTxNum) ||
				!stateDomainChangeMatchesKey(change, flatDomain, owner, generation, domain, key) {
				return true, nil
			}
			first = cloneStateDomainChange(change)
			return false, nil
		}); err != nil {
			return nil, err
		}
		if first != nil {
			return first, nil
		}
		if blockNum == ^uint64(0) {
			break
		}
		fromBlock = blockNum + 1
	}
	if staged {
		directFrom := targetBlock + 1
		if indexedHead >= directFrom {
			if indexedHead == ^uint64(0) {
				return nil, nil
			}
			directFrom = indexedHead + 1
		}
		var first *StateDomainChange
		if err := IterateStateDomainChangesByBlockRange(db, directFrom, headBlock, func(change *StateDomainChange) (bool, error) {
			if !stateDomainChangeInTxWindow(change, targetTxNum, headTxNum) ||
				!stateDomainChangeMatchesKey(change, flatDomain, owner, generation, domain, key) {
				return true, nil
			}
			first = cloneStateDomainChange(change)
			return false, nil
		}); err != nil {
			return nil, err
		}
		if first != nil {
			return first, nil
		}
	}
	return nil, nil
}

// ReadFirstStateKVChangesByKeysBlockRange resolves the earliest mutation for
// many logical KV keys. The indexed prefix still uses one ordered seek per
// key, while the unindexed stage tail is scanned once for the whole batch
// instead of once per key. This is the common shape for historical dynamic
// properties, where roughly a hundred keys share one owner and domain.
func ReadFirstStateKVChangesByKeysBlockRange(db StateKVHistoryReader, targetBlock, headBlock, targetTxNum, headTxNum uint64, owner common.Address, generation uint64, domain kvdomains.KVDomain, keys [][]byte) (map[string]*StateDomainChange, error) {
	first := make(map[string]*StateDomainChange, len(keys))
	if targetBlock >= headBlock || targetTxNum >= headTxNum || len(keys) == 0 {
		return first, nil
	}
	indexedHead, staged, err := stateHistoryIndexedHead(db, headBlock)
	if err != nil {
		return nil, err
	}
	if !staged {
		indexedHead = headBlock
	}
	wanted := make(map[string][]byte, len(keys))
	for _, key := range keys {
		keyString := string(key)
		if _, exists := wanted[keyString]; !exists {
			wanted[keyString] = key
		}
	}
	if targetBlock < indexedHead {
		for keyString, key := range wanted {
			change, err := ReadFirstStateDomainChangeByKeyBlockRange(db, targetBlock, indexedHead, targetTxNum, headTxNum, StateFlatDomainKVLatest, owner, generation, domain, key)
			if err != nil {
				return nil, err
			}
			if change != nil {
				first[keyString] = change
				delete(wanted, keyString)
			}
		}
	}
	if !staged || len(wanted) == 0 {
		return first, nil
	}
	directFrom := targetBlock + 1
	if indexedHead >= directFrom {
		if indexedHead == ^uint64(0) {
			return first, nil
		}
		directFrom = indexedHead + 1
	}
	if err := IterateStateDomainChangesByBlockRange(db, directFrom, headBlock, func(change *StateDomainChange) (bool, error) {
		if !stateDomainChangeInTxWindow(change, targetTxNum, headTxNum) ||
			change.FlatDomain != StateFlatDomainKVLatest || change.Owner != owner ||
			change.Generation != generation || change.Domain != domain {
			return true, nil
		}
		keyString := string(change.Key)
		if _, ok := wanted[keyString]; !ok {
			return true, nil
		}
		first[keyString] = cloneStateDomainChange(change)
		delete(wanted, keyString)
		return len(wanted) > 0, nil
	}); err != nil {
		return nil, err
	}
	return first, nil
}

// stateHistoryIndexedHead returns the inclusive posting-index watermark capped
// to the query head. Missing stage metadata denotes a standalone/all-inline
// writer whose posting rows are immediately visible.
func stateHistoryIndexedHead(db ethdb.KeyValueReader, headBlock uint64) (uint64, bool, error) {
	row, ok, err := ReadStageProgressRow(db, StageStateHistoryIndex)
	if err != nil || !ok {
		return 0, ok, err
	}
	if row.BlockNum < headBlock {
		return row.BlockNum, true, nil
	}
	return headBlock, true, nil
}

func firstStateDomainChangeBlockByKeyRange(db ethdb.Iteratee, fromBlock, toBlock uint64, _ bool, flatDomain StateFlatDomain, owner common.Address, generation uint64, domain kvdomains.KVDomain, key []byte) (uint64, bool, error) {
	latestKey, ok := stateDomainChangeLatestKeyByKey(flatDomain, owner, generation, domain, key)
	if !ok || fromBlock > toBlock {
		return 0, false, nil
	}
	var first uint64
	found := false
	err := iterateStateDomainChangePostingBlocks(db, latestKey, fromBlock, toBlock, func(blockNum uint64) (bool, error) {
		first = blockNum
		found = true
		return false, nil
	})
	return first, found, err
}

func stateDomainChangeLatestKeyByKey(flatDomain StateFlatDomain, owner common.Address, generation uint64, domain kvdomains.KVDomain, key []byte) ([]byte, bool) {
	switch flatDomain {
	case StateFlatDomainAccountLatest:
		return StateAccountLatestCommitmentKey(owner), true
	case StateFlatDomainKVLatest:
		return StateKVLatestCommitmentKey(owner, generation, domain, key), true
	case StateFlatDomainKVGeneration:
		return StateKVGenerationCommitmentKey(owner), true
	default:
		return nil, false
	}
}

func iterateStateDomainChangePostingBlocks(db ethdb.Iteratee, latestKey []byte, fromBlock, toBlock uint64, fn func(uint64) (bool, error)) error {
	return iterateStateChangePostingCandidates(db, latestKey, fromBlock, toBlock, func(blockNum uint64) (bool, error) {
		// SHA-256 is only a candidate selector. The changeset's reconstructed
		// original latest key is the authoritative collision check.
		matches, err := stateDomainChangeBlockMatchesLatestKey(db, blockNum, latestKey)
		if err != nil || !matches {
			return err == nil, err
		}
		return fn(blockNum)
	})
}

func stateDomainChangeBlockMatchesLatestKey(db ethdb.Iteratee, blockNum uint64, latestKey []byte) (bool, error) {
	found := false
	err := IterateStateDomainChanges(db, blockNum, func(change *StateDomainChange) (bool, error) {
		candidate, err := stateDomainChangeLatestKey(change)
		if err != nil {
			return false, err
		}
		if bytes.Equal(candidate, latestKey) {
			found = true
			return false, nil
		}
		return true, nil
	})
	return found, err
}

// IterateStateDomainChangesByKey walks hot StateDomainChange rows matching one
// latest-domain logical key inside the tx window (targetTxNum, headTxNum].
func IterateStateDomainChangesByKey(db StateKVHistoryReader, targetTxNum, headTxNum uint64, flatDomain StateFlatDomain, owner common.Address, generation uint64, domain kvdomains.KVDomain, key []byte, fn func(*StateDomainChange) (bool, error)) error {
	return iterateStateDomainChangesByKey(db, 0, ^uint64(0), false, targetTxNum, headTxNum, flatDomain, owner, generation, domain, key, fn)
}

// IterateStateDomainChangesByKeyBlockRange is the block-bounded form used by
// live archive queries. targetBlock is the state being requested, so only
// posting candidates in (targetBlock, headBlock] can contribute rollback values.
func IterateStateDomainChangesByKeyBlockRange(db StateKVHistoryReader, targetBlock, headBlock, targetTxNum, headTxNum uint64, flatDomain StateFlatDomain, owner common.Address, generation uint64, domain kvdomains.KVDomain, key []byte, fn func(*StateDomainChange) (bool, error)) error {
	if targetBlock >= headBlock {
		return nil
	}
	return iterateStateDomainChangesByKey(db, targetBlock+1, headBlock, true, targetTxNum, headTxNum, flatDomain, owner, generation, domain, key, fn)
}

func iterateStateDomainChangesByKey(db StateKVHistoryReader, fromBlock, toBlock uint64, bounded bool, targetTxNum, headTxNum uint64, flatDomain StateFlatDomain, owner common.Address, generation uint64, domain kvdomains.KVDomain, key []byte, fn func(*StateDomainChange) (bool, error)) error {
	if targetTxNum >= headTxNum {
		return nil
	}
	stop := false
	visitBlock := func(blockNum uint64) (bool, error) {
		// The block-bounded archive path already restricts candidates to
		// (targetBlock, headBlock]. targetTxNum/headTxNum are the respective
		// end-of-block boundaries, so every change in those candidate blocks is
		// necessarily inside the tx window. Avoid one StateTxRange Has+Get per
		// posting-index hit — frequently updated dynamic properties otherwise
		// turn a 260k-block query into hundreds of thousands of Pebble point
		// reads before the actual history row is examined.
		if !bounded {
			ok, err := stateBlockIntersectsTxWindow(db, blockNum, targetTxNum, headTxNum)
			if err != nil {
				return false, err
			}
			if !ok {
				return true, nil
			}
		}
		if err := IterateStateDomainChanges(db, blockNum, func(change *StateDomainChange) (bool, error) {
			if !stateDomainChangeInTxWindow(change, targetTxNum, headTxNum) {
				return true, nil
			}
			if !stateDomainChangeMatchesKey(change, flatDomain, owner, generation, domain, key) {
				return true, nil
			}
			cont, err := fn(change)
			if err != nil {
				return false, err
			}
			if !cont {
				stop = true
				return false, nil
			}
			return true, nil
		}); err != nil {
			return false, err
		}
		return !stop, nil
	}
	if bounded {
		indexedHead, staged, err := stateHistoryIndexedHead(db, toBlock)
		if err != nil {
			return err
		}
		if !staged {
			return IterateStateDomainChangeBlocksByKeyRange(db, fromBlock, toBlock, flatDomain, owner, generation, domain, key, visitBlock)
		}
		if fromBlock <= indexedHead {
			if err := IterateStateDomainChangeBlocksByKeyRange(db, fromBlock, indexedHead, flatDomain, owner, generation, domain, key, visitBlock); err != nil || stop {
				return err
			}
		}
		directFrom := fromBlock
		if indexedHead >= directFrom {
			if indexedHead == ^uint64(0) {
				return nil
			}
			directFrom = indexedHead + 1
		}
		return IterateStateDomainChangesByBlockRange(db, directFrom, toBlock, func(change *StateDomainChange) (bool, error) {
			if !stateDomainChangeInTxWindow(change, targetTxNum, headTxNum) ||
				!stateDomainChangeMatchesKey(change, flatDomain, owner, generation, domain, key) {
				return true, nil
			}
			return fn(change)
		})
	}
	indexedHead, staged, err := stateHistoryIndexedHead(db, ^uint64(0))
	if err != nil {
		return err
	}
	if !staged {
		return IterateStateDomainChangeBlocksByKey(db, flatDomain, owner, generation, domain, key, visitBlock)
	}
	if err := IterateStateDomainChangeBlocksByKeyRange(db, 0, indexedHead, flatDomain, owner, generation, domain, key, visitBlock); err != nil || stop {
		return err
	}
	return IterateStateDomainChangesByTxRange(db, targetTxNum+1, headTxNum, func(change *StateDomainChange) (bool, error) {
		if change.BlockNum <= indexedHead || !stateDomainChangeMatchesKey(change, flatDomain, owner, generation, domain, key) {
			return true, nil
		}
		return fn(change)
	})
}

// IterateStateDomainChangesByPrefix walks hot KV-latest StateDomainChange rows
// matching one logical key prefix inside the tx window (targetTxNum, headTxNum].
func IterateStateDomainChangesByPrefix(db StateKVHistoryReader, targetTxNum, headTxNum uint64, owner common.Address, generation uint64, domain kvdomains.KVDomain, prefix []byte, fn func(*StateDomainChange) (bool, error)) error {
	return iterateStateDomainChangesByPrefix(db, 0, ^uint64(0), false, targetTxNum, headTxNum, owner, generation, domain, prefix, fn)
}

// IterateStateDomainChangesByPrefixBlockRange filters prefix-index candidates
// to (targetBlock, headBlock] before reading their StateTxRange rows. Unlike an
// exact key, a logical prefix spans variable key bytes before the block suffix,
// so Pebble cannot seek by block globally; early filtering still removes the
// expensive point-read amplification from older history.
func IterateStateDomainChangesByPrefixBlockRange(db StateKVHistoryReader, targetBlock, headBlock, targetTxNum, headTxNum uint64, owner common.Address, generation uint64, domain kvdomains.KVDomain, prefix []byte, fn func(*StateDomainChange) (bool, error)) error {
	if targetBlock >= headBlock {
		return nil
	}
	return iterateStateDomainChangesByPrefix(db, targetBlock+1, headBlock, true, targetTxNum, headTxNum, owner, generation, domain, prefix, fn)
}

func iterateStateDomainChangesByPrefix(db StateKVHistoryReader, fromBlock, toBlock uint64, bounded bool, targetTxNum, headTxNum uint64, owner common.Address, generation uint64, domain kvdomains.KVDomain, prefix []byte, fn func(*StateDomainChange) (bool, error)) error {
	if targetTxNum >= headTxNum {
		return nil
	}
	indexedHead := toBlock
	staged := false
	if bounded {
		var err error
		indexedHead, staged, err = stateHistoryIndexedHead(db, toBlock)
		if err != nil {
			return err
		}
		if !staged {
			indexedHead = toBlock
		}
	} else {
		var err error
		indexedHead, staged, err = stateHistoryIndexedHead(db, ^uint64(0))
		if err != nil {
			return err
		}
	}
	blocks := make(map[uint64]struct{})
	if err := IterateStateDomainChangeBlocksByPrefix(db, owner, generation, domain, prefix, func(blockNum uint64) (bool, error) {
		if staged && (blockNum < fromBlock || blockNum > indexedHead) {
			return true, nil
		}
		ok, err := stateBlockIntersectsTxWindow(db, blockNum, targetTxNum, headTxNum)
		if err != nil {
			return false, err
		}
		if ok {
			blocks[blockNum] = struct{}{}
		}
		return true, nil
	}); err != nil {
		return err
	}
	ordered := make([]uint64, 0, len(blocks))
	for blockNum := range blocks {
		ordered = append(ordered, blockNum)
	}
	sort.Slice(ordered, func(i, j int) bool { return ordered[i] < ordered[j] })
	for _, blockNum := range ordered {
		if err := IterateStateDomainChanges(db, blockNum, func(change *StateDomainChange) (bool, error) {
			if !stateDomainChangeInTxWindow(change, targetTxNum, headTxNum) {
				return true, nil
			}
			if !stateDomainChangeMatchesKVLatestPrefix(change, owner, generation, domain, prefix) {
				return true, nil
			}
			return fn(change)
		}); err != nil {
			return err
		}
	}
	if staged {
		if !bounded {
			return IterateStateDomainChangesByTxRange(db, targetTxNum+1, headTxNum, func(change *StateDomainChange) (bool, error) {
				if change.BlockNum <= indexedHead || !stateDomainChangeMatchesKVLatestPrefix(change, owner, generation, domain, prefix) {
					return true, nil
				}
				return fn(change)
			})
		}
		directFrom := fromBlock
		if indexedHead >= directFrom {
			if indexedHead == ^uint64(0) {
				return nil
			}
			directFrom = indexedHead + 1
		}
		return IterateStateDomainChangesByBlockRange(db, directFrom, toBlock, func(change *StateDomainChange) (bool, error) {
			if !stateDomainChangeInTxWindow(change, targetTxNum, headTxNum) ||
				!stateDomainChangeMatchesKVLatestPrefix(change, owner, generation, domain, prefix) {
				return true, nil
			}
			return fn(change)
		})
	}
	return nil
}

func stateDomainChangeMatchesKey(change *StateDomainChange, flatDomain StateFlatDomain, owner common.Address, generation uint64, domain kvdomains.KVDomain, key []byte) bool {
	if change == nil || change.FlatDomain != flatDomain || change.Owner != owner {
		return false
	}
	switch flatDomain {
	case StateFlatDomainAccountLatest:
		return true
	case StateFlatDomainKVLatest:
		return change.Generation == generation && change.Domain == domain && bytes.Equal(change.Key, key)
	case StateFlatDomainKVGeneration:
		return true
	default:
		return false
	}
}

func IterateStateDomainChangeBlocksByPrefix(db ethdb.Iteratee, owner common.Address, generation uint64, domain kvdomains.KVDomain, keyPrefix []byte, fn func(blockNum uint64) (bool, error)) error {
	latestPrefix := StateKVLatestCommitmentKey(owner, generation, domain, keyPrefix)
	seen := make(map[uint64]struct{})
	stopped := false
	visit := func(blockNum uint64) (bool, error) {
		if _, ok := seen[blockNum]; ok {
			return true, nil
		}
		seen[blockNum] = struct{}{}
		cont, err := fn(blockNum)
		if !cont {
			stopped = true
		}
		return cont, err
	}
	directoryPrefix := append(append([]byte(nil), stateChangeKeyDirectoryPrefix...), latestPrefix...)
	it := db.NewIterator(directoryPrefix, nil)
	defer it.Release()
	for it.Next() {
		if stopped {
			return nil
		}
		physical := it.Key()
		if !bytes.HasPrefix(physical, directoryPrefix) {
			continue
		}
		latestKey := physical[len(stateChangeKeyDirectoryPrefix):]
		if err := iterateStateDomainChangePostingBlocks(db, latestKey, 0, ^uint64(0), visit); err != nil {
			return err
		}
	}
	return it.Error()
}

func stateDomainChangeLatestKey(change *StateDomainChange) ([]byte, error) {
	return appendStateDomainChangeLatestKey(nil, change)
}

// appendStateDomainChangeLatestKey appends the physical latest-domain key to
// dst. History-index builders reuse caller-owned scratch and copy the final key
// directly into their ETL arenas instead of allocating multiple transient key
// objects per source change.
func appendStateDomainChangeLatestKey(dst []byte, change *StateDomainChange) ([]byte, error) {
	if change == nil {
		return nil, errors.New("rawdb: nil StateDomainChange")
	}
	switch change.FlatDomain {
	case StateFlatDomainAccountLatest:
		return AppendStateAccountLatestCommitmentKey(dst, change.Owner), nil
	case StateFlatDomainKVLatest:
		if !kvdomains.IsRegistered(change.Domain) {
			return nil, fmt.Errorf("rawdb: unregistered change KV domain %#04x", uint16(change.Domain))
		}
		return AppendStateKVLatestCommitmentKey(dst, change.Owner, change.Generation, change.Domain, change.Key), nil
	case StateFlatDomainKVGeneration:
		return AppendStateKVGenerationCommitmentKey(dst, change.Owner), nil
	default:
		return nil, fmt.Errorf("rawdb: unknown state flat domain %d", uint8(change.FlatDomain))
	}
}

func stateDomainChangeCommitmentValue(change *StateDomainChange, value []byte) ([]byte, error) {
	switch change.FlatDomain {
	case StateFlatDomainAccountLatest:
		return append([]byte(nil), value...), nil
	case StateFlatDomainKVLatest:
		return EncodeStateKVLatestValue(value), nil
	case StateFlatDomainKVGeneration:
		if len(value) != 8 {
			return nil, fmt.Errorf("rawdb: bad KV generation change value length %d", len(value))
		}
		return append([]byte(nil), value...), nil
	default:
		return nil, fmt.Errorf("rawdb: unknown state flat domain %d", uint8(change.FlatDomain))
	}
}

func writeStateDomainLatestRow(db stateKVLatestStore, change *StateDomainChange) error {
	switch change.FlatDomain {
	case StateFlatDomainAccountLatest:
		return WriteStateAccountLatest(db, change.Owner, change.Prev)
	case StateFlatDomainKVLatest:
		return WriteStateKVLatest(db, change.Owner, change.Generation, change.Domain, change.Key, change.Prev)
	case StateFlatDomainKVGeneration:
		if len(change.Prev) != 8 {
			return fmt.Errorf("rawdb: bad KV generation change value length %d", len(change.Prev))
		}
		return WriteStateKVGeneration(db, change.Owner, binary.BigEndian.Uint64(change.Prev))
	default:
		return fmt.Errorf("rawdb: unknown state flat domain %d", uint8(change.FlatDomain))
	}
}

func deleteStateDomainLatestRow(db stateKVLatestStore, change *StateDomainChange) error {
	switch change.FlatDomain {
	case StateFlatDomainAccountLatest:
		return DeleteStateAccountLatest(db, change.Owner)
	case StateFlatDomainKVLatest:
		return DeleteStateKVLatest(db, change.Owner, change.Generation, change.Domain, change.Key)
	case StateFlatDomainKVGeneration:
		return DeleteStateKVGeneration(db, change.Owner)
	default:
		return fmt.Errorf("rawdb: unknown state flat domain %d", uint8(change.FlatDomain))
	}
}

func stateDomainChangeMatchesKVLatest(change *StateDomainChange, owner common.Address, generation uint64, domain kvdomains.KVDomain, key []byte) bool {
	return change.FlatDomain == StateFlatDomainKVLatest &&
		change.Owner == owner &&
		change.Generation == generation &&
		change.Domain == domain &&
		bytes.Equal(change.Key, key)
}

func stateDomainChangeMatchesKVLatestPrefix(change *StateDomainChange, owner common.Address, generation uint64, domain kvdomains.KVDomain, prefix []byte) bool {
	return change.FlatDomain == StateFlatDomainKVLatest &&
		change.Owner == owner &&
		change.Generation == generation &&
		change.Domain == domain &&
		bytes.HasPrefix(change.Key, prefix)
}

func stateDomainChangeMatchesKVGeneration(change *StateDomainChange, owner common.Address) bool {
	return change.FlatDomain == StateFlatDomainKVGeneration && change.Owner == owner
}

func stateDomainChangeMatchesAccountLatest(change *StateDomainChange, owner common.Address) bool {
	return change.FlatDomain == StateFlatDomainAccountLatest && change.Owner == owner
}

func stateDomainChangeInTxWindow(change *StateDomainChange, targetTxNum, headTxNum uint64) bool {
	return change.TxNum > targetTxNum && change.TxNum <= headTxNum
}

func stateBlockTxRange(db ethdb.KeyValueReader, blockNum uint64) (uint64, uint64, error) {
	row, ok, err := ReadStateTxRange(db, blockNum)
	if err != nil {
		return 0, 0, err
	}
	if !ok {
		return blockNum, blockNum, nil
	}
	if row.EndTxNum < row.BeginTxNum {
		return 0, 0, fmt.Errorf("rawdb: invalid stored state tx range for block %d: [%d,%d]", blockNum, row.BeginTxNum, row.EndTxNum)
	}
	return row.BeginTxNum, row.EndTxNum, nil
}

func stateBlockIntersectsTxWindow(db ethdb.KeyValueReader, blockNum, targetTxNum, headTxNum uint64) (bool, error) {
	beginTxNum, endTxNum, err := stateBlockTxRange(db, blockNum)
	if err != nil {
		return false, err
	}
	return endTxNum > targetTxNum && beginTxNum <= headTxNum, nil
}

// ReadStateKVAsOf reconstructs one account-KV value at the end of targetBlock.
// The first subsequent mutation contains that value in Prev; if there is no
// subsequent mutation, the current latest row is still valid at targetBlock.
func ReadStateKVAsOf(db stateKVHistoryReader, owner common.Address, generation uint64, domain kvdomains.KVDomain, key []byte, targetBlock, headBlock uint64) ([]byte, bool, error) {
	targetTxNum, err := StateTxNumAtBlockEnd(db, targetBlock)
	if err != nil {
		return nil, false, err
	}
	headTxNum, err := StateTxNumAtBlockEnd(db, headBlock)
	if err != nil {
		return nil, false, err
	}
	return ReadStateKVAsOfTxNum(db, owner, generation, domain, key, targetTxNum, headTxNum)
}

func ReadStateAccountLatestAsOf(db stateKVHistoryReader, owner common.Address, targetBlock, headBlock uint64) ([]byte, bool, error) {
	targetTxNum, err := StateTxNumAtBlockEnd(db, targetBlock)
	if err != nil {
		return nil, false, err
	}
	headTxNum, err := StateTxNumAtBlockEnd(db, headBlock)
	if err != nil {
		return nil, false, err
	}
	return ReadStateAccountLatestAsOfTxNum(db, owner, targetTxNum, headTxNum)
}

func ReadStateAccountLatestAsOfTxNum(db stateKVHistoryReader, owner common.Address, targetTxNum, headTxNum uint64) ([]byte, bool, error) {
	if targetTxNum < headTxNum {
		change, err := firstStateDomainChangeByKey(db, targetTxNum, headTxNum, StateFlatDomainAccountLatest, owner, 0, 0, nil)
		if err != nil {
			return nil, false, err
		}
		if change != nil {
			if !change.PrevExists {
				return nil, false, nil
			}
			return append([]byte(nil), change.Prev...), true, nil
		}
	}
	value, exists, err := ReadStateAccountLatest(db, owner)
	if err != nil {
		return nil, false, err
	}
	return append([]byte(nil), value...), exists, nil
}

// ReadStateKVAsOfTxNum reconstructs one account-KV value at targetTxNum by
// seeking the first mutation in (targetTxNum, headTxNum].
func ReadStateKVAsOfTxNum(db stateKVHistoryReader, owner common.Address, generation uint64, domain kvdomains.KVDomain, key []byte, targetTxNum, headTxNum uint64) ([]byte, bool, error) {
	if targetTxNum < headTxNum {
		change, err := firstStateDomainChangeByKey(db, targetTxNum, headTxNum, StateFlatDomainKVLatest, owner, generation, domain, key)
		if err != nil {
			return nil, false, err
		}
		if change != nil {
			if !change.PrevExists {
				return nil, false, nil
			}
			return append([]byte(nil), change.Prev...), true, nil
		}
	}
	value, exists, err := ReadStateKVLatest(db, owner, generation, domain, key)
	if err != nil {
		return nil, false, err
	}
	return append([]byte(nil), value...), exists, nil
}

func ReadStateKVGenerationAsOf(db stateKVHistoryReader, owner common.Address, targetBlock, headBlock uint64) (uint64, bool, error) {
	targetTxNum, err := StateTxNumAtBlockEnd(db, targetBlock)
	if err != nil {
		return 0, false, err
	}
	headTxNum, err := StateTxNumAtBlockEnd(db, headBlock)
	if err != nil {
		return 0, false, err
	}
	return ReadStateKVGenerationAsOfTxNum(db, owner, targetTxNum, headTxNum)
}

func ReadStateKVGenerationAsOfTxNum(db stateKVHistoryReader, owner common.Address, targetTxNum, headTxNum uint64) (uint64, bool, error) {
	if targetTxNum < headTxNum {
		change, err := firstStateDomainChangeByKey(db, targetTxNum, headTxNum, StateFlatDomainKVGeneration, owner, 0, 0, nil)
		if err != nil {
			return 0, false, err
		}
		if change != nil {
			if !change.PrevExists {
				return 0, false, nil
			}
			generation, err := DecodeStateKVGenerationValue(change.Prev)
			if err != nil {
				return 0, false, err
			}
			return generation, true, nil
		}
	}
	return ReadStateKVGeneration(db, owner)
}

// firstStateDomainChangeByKey returns the first mutation after targetTxNum.
// Its Prev value is the value as of targetTxNum, so point-in-time reads do not
// need to materialize and replay the key's complete subsequent history.
func firstStateDomainChangeByKey(db stateKVHistoryReader, targetTxNum, headTxNum uint64, flatDomain StateFlatDomain, owner common.Address, generation uint64, domain kvdomains.KVDomain, key []byte) (*StateDomainChange, error) {
	var first *StateDomainChange
	err := IterateStateDomainChangesByKey(db, targetTxNum, headTxNum, flatDomain, owner, generation, domain, key, func(change *StateDomainChange) (bool, error) {
		first = cloneStateDomainChange(change)
		return false, nil
	})
	return first, err
}

func collectStateDomainChangesByKey(db stateKVHistoryReader, targetTxNum, headTxNum uint64, flatDomain StateFlatDomain, owner common.Address, generation uint64, domain kvdomains.KVDomain, key []byte) ([]*StateDomainChange, error) {
	var changes []*StateDomainChange
	if err := IterateStateDomainChangesByKey(db, targetTxNum, headTxNum, flatDomain, owner, generation, domain, key, func(change *StateDomainChange) (bool, error) {
		changes = append(changes, change)
		return true, nil
	}); err != nil {
		return nil, err
	}
	sortStateDomainChangesForReplay(changes)
	return changes, nil
}

func collectStateDomainChangesByPrefix(db stateKVHistoryReader, targetTxNum, headTxNum uint64, owner common.Address, generation uint64, domain kvdomains.KVDomain, prefix []byte) ([]*StateDomainChange, error) {
	var changes []*StateDomainChange
	if err := IterateStateDomainChangesByPrefix(db, targetTxNum, headTxNum, owner, generation, domain, prefix, func(change *StateDomainChange) (bool, error) {
		changes = append(changes, change)
		return true, nil
	}); err != nil {
		return nil, err
	}
	sortStateDomainChangesForReplay(changes)
	return changes, nil
}

func collectStateAccountKVChangesByTxNum(db stateKVHistoryReader, owner common.Address, generation uint64, domain kvdomains.KVDomain, key []byte, targetTxNum, headTxNum uint64) ([]*StateDomainChange, error) {
	changes, err := collectStateDomainChangesByKey(db, targetTxNum, headTxNum, StateFlatDomainKVLatest, owner, generation, domain, key)
	if err != nil {
		return nil, err
	}
	generationChanges, err := collectStateDomainChangesByKey(db, targetTxNum, headTxNum, StateFlatDomainKVGeneration, owner, 0, 0, nil)
	if err != nil {
		return nil, err
	}
	changes = append(changes, generationChanges...)
	sortStateDomainChangesForReplay(changes)
	return changes, nil
}

func collectStateAccountKVPrefixChangesByTxNum(db stateKVHistoryReader, owner common.Address, generation uint64, domain kvdomains.KVDomain, prefix []byte, targetTxNum, headTxNum uint64) ([]*StateDomainChange, error) {
	changes, err := collectStateDomainChangesByPrefix(db, targetTxNum, headTxNum, owner, generation, domain, prefix)
	if err != nil {
		return nil, err
	}
	generationChanges, err := collectStateDomainChangesByKey(db, targetTxNum, headTxNum, StateFlatDomainKVGeneration, owner, 0, 0, nil)
	if err != nil {
		return nil, err
	}
	changes = append(changes, generationChanges...)
	sortStateDomainChangesForReplay(changes)
	return changes, nil
}

func sortStateDomainChangesForReplay(changes []*StateDomainChange) {
	sort.Slice(changes, func(i, j int) bool {
		if changes[i].BlockNum != changes[j].BlockNum {
			return changes[i].BlockNum < changes[j].BlockNum
		}
		if changes[i].Seq != changes[j].Seq {
			return changes[i].Seq < changes[j].Seq
		}
		return changes[i].TxNum < changes[j].TxNum
	})
}

func ReadStateAccountKVAsOf(db stateKVHistoryReader, owner common.Address, domain kvdomains.KVDomain, key []byte, targetBlock, headBlock uint64) ([]byte, bool, error) {
	targetTxNum, err := StateTxNumAtBlockEnd(db, targetBlock)
	if err != nil {
		return nil, false, err
	}
	headTxNum, err := StateTxNumAtBlockEnd(db, headBlock)
	if err != nil {
		return nil, false, err
	}
	return ReadStateAccountKVAsOfTxNum(db, owner, domain, key, targetTxNum, headTxNum)
}

func ReadStateAccountKVAsOfTxNum(db stateKVHistoryReader, owner common.Address, domain kvdomains.KVDomain, key []byte, targetTxNum, headTxNum uint64) ([]byte, bool, error) {
	generation, _, err := ReadStateKVGeneration(db, owner)
	if err != nil {
		return nil, false, err
	}
	value, exists, err := ReadStateKVLatest(db, owner, generation, domain, key)
	if err != nil {
		return nil, false, err
	}
	if targetTxNum >= headTxNum {
		return value, exists, nil
	}
	upperTxNum := headTxNum
	for targetTxNum < upperTxNum {
		changes, err := collectStateAccountKVChangesByTxNum(db, owner, generation, domain, key, targetTxNum, upperTxNum)
		if err != nil {
			return nil, false, err
		}
		if len(changes) == 0 {
			break
		}
		generationChanged := false
		for i := len(changes) - 1; i >= 0; i-- {
			change := changes[i]
			switch {
			case stateDomainChangeMatchesKVLatest(change, owner, generation, domain, key):
				if change.PrevExists {
					value = append([]byte(nil), change.Prev...)
					exists = true
				} else {
					value = nil
					exists = false
				}
			case stateDomainChangeMatchesKVGeneration(change, owner):
				generation, err = prevStateKVGeneration(change)
				if err != nil {
					return nil, false, err
				}
				value, exists, err = ReadStateKVLatest(db, owner, generation, domain, key)
				if err != nil {
					return nil, false, err
				}
				if change.TxNum == 0 {
					upperTxNum = 0
				} else {
					upperTxNum = change.TxNum - 1
				}
				generationChanged = true
			}
			if generationChanged {
				break
			}
		}
		if !generationChanged {
			break
		}
	}
	return append([]byte(nil), value...), exists, nil
}

func prevStateKVGeneration(change *StateDomainChange) (uint64, error) {
	if change == nil || !change.PrevExists {
		return 0, nil
	}
	return DecodeStateKVGenerationValue(change.Prev)
}

func IterateStateKVAsOfPrefix(db stateKVHistoryReader, owner common.Address, generation uint64, domain kvdomains.KVDomain, prefix []byte, targetBlock, headBlock uint64, fn func(key, value []byte) (bool, error)) error {
	targetTxNum, err := StateTxNumAtBlockEnd(db, targetBlock)
	if err != nil {
		return err
	}
	headTxNum, err := StateTxNumAtBlockEnd(db, headBlock)
	if err != nil {
		return err
	}
	return IterateStateKVAsOfPrefixTxNum(db, owner, generation, domain, prefix, targetTxNum, headTxNum, fn)
}

func IterateStateKVAsOfPrefixTxNum(db stateKVHistoryReader, owner common.Address, generation uint64, domain kvdomains.KVDomain, prefix []byte, targetTxNum, headTxNum uint64, fn func(key, value []byte) (bool, error)) error {
	entries := make(map[string][]byte)
	if err := IterateStateKVLatest(db, owner, generation, domain, prefix, func(key, value []byte) (bool, error) {
		entries[string(key)] = append([]byte(nil), value...)
		return true, nil
	}); err != nil {
		return err
	}
	if targetTxNum < headTxNum {
		changes, err := collectStateDomainChangesByPrefix(db, targetTxNum, headTxNum, owner, generation, domain, prefix)
		if err != nil {
			return err
		}
		for i := len(changes) - 1; i >= 0; i-- {
			change := changes[i]
			if change.PrevExists {
				entries[string(change.Key)] = append([]byte(nil), change.Prev...)
			} else {
				delete(entries, string(change.Key))
			}
		}
	}
	return iterateStateKVEntries(entries, fn)
}

func iterateStateKVEntries(entries map[string][]byte, fn func(key, value []byte) (bool, error)) error {
	keys := make([]string, 0, len(entries))
	for key := range entries {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		cont, err := fn([]byte(key), append([]byte(nil), entries[key]...))
		if err != nil {
			return err
		}
		if !cont {
			return nil
		}
	}
	return nil
}

func IterateStateAccountKVAsOfPrefix(db stateKVHistoryReader, owner common.Address, domain kvdomains.KVDomain, prefix []byte, targetBlock, headBlock uint64, fn func(key, value []byte) (bool, error)) error {
	targetTxNum, err := StateTxNumAtBlockEnd(db, targetBlock)
	if err != nil {
		return err
	}
	headTxNum, err := StateTxNumAtBlockEnd(db, headBlock)
	if err != nil {
		return err
	}
	return IterateStateAccountKVAsOfPrefixTxNum(db, owner, domain, prefix, targetTxNum, headTxNum, fn)
}

func IterateStateAccountKVAsOfPrefixTxNum(db stateKVHistoryReader, owner common.Address, domain kvdomains.KVDomain, prefix []byte, targetTxNum, headTxNum uint64, fn func(key, value []byte) (bool, error)) error {
	generation, _, err := ReadStateKVGeneration(db, owner)
	if err != nil {
		return err
	}
	entries := make(map[string][]byte)
	if err := readStateKVLatestPrefixInto(db, owner, generation, domain, prefix, entries); err != nil {
		return err
	}
	upperTxNum := headTxNum
	for targetTxNum < upperTxNum {
		changes, err := collectStateAccountKVPrefixChangesByTxNum(db, owner, generation, domain, prefix, targetTxNum, upperTxNum)
		if err != nil {
			return err
		}
		if len(changes) == 0 {
			break
		}
		generationChanged := false
		for i := len(changes) - 1; i >= 0; i-- {
			change := changes[i]
			switch {
			case stateDomainChangeMatchesKVLatestPrefix(change, owner, generation, domain, prefix):
				if change.PrevExists {
					entries[string(change.Key)] = append([]byte(nil), change.Prev...)
				} else {
					delete(entries, string(change.Key))
				}
			case stateDomainChangeMatchesKVGeneration(change, owner):
				generation, err = prevStateKVGeneration(change)
				if err != nil {
					return err
				}
				entries = make(map[string][]byte)
				if err := readStateKVLatestPrefixInto(db, owner, generation, domain, prefix, entries); err != nil {
					return err
				}
				if change.TxNum == 0 {
					upperTxNum = 0
				} else {
					upperTxNum = change.TxNum - 1
				}
				generationChanged = true
			}
			if generationChanged {
				break
			}
		}
		if !generationChanged {
			break
		}
	}
	return iterateStateKVEntries(entries, fn)
}

func readStateKVLatestPrefixInto(db stateKVHistoryReader, owner common.Address, generation uint64, domain kvdomains.KVDomain, prefix []byte, entries map[string][]byte) error {
	return IterateStateKVLatest(db, owner, generation, domain, prefix, func(key, value []byte) (bool, error) {
		entries[string(key)] = append([]byte(nil), value...)
		return true, nil
	})
}

func cloneStateDomainChange(in *StateDomainChange) *StateDomainChange {
	if in == nil {
		return nil
	}
	out := *in
	out.Key = append([]byte(nil), in.Key...)
	out.Prev = append([]byte(nil), in.Prev...)
	out.Next = append([]byte(nil), in.Next...)
	return &out
}
