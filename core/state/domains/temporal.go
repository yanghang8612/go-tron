package domains

import (
	"context"
	"errors"

	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/state/kvdomains"
)

var (
	ErrTemporalTxClosed       = errors.New("domains: temporal transaction closed")
	ErrNilCommitmentProcessor = errors.New("domains: nil commitment processor")
)

// AsOfReader is the history side of a temporal domain. The timestamp is named
// txNum to match Erigon's TemporalTx API; gtron still maps it to block numbers
// in the current history implementation.
type AsOfReader interface {
	GetAsOf(owner common.Address, domain kvdomains.KVDomain, key []byte, txNum uint64) ([]byte, bool, error)
}

type HeadTxNumSetter interface {
	SetHeadTxNum(txNum uint64)
}

// CommitmentProcessor is the commitment side of a temporal domain transaction.
// It mirrors the small part of Erigon SharedDomains used by execution:
// locating the current commitment position and computing a root from staged
// domain updates.
type CommitmentProcessor interface {
	SeekCommitment(ctx context.Context) (txNum, blockNum uint64, err error)
	ComputeCommitment(ctx context.Context, blockNum, txNum uint64) (common.Hash, error)
}

// CommitmentMutationRecorder lets a commitment processor collect the logical
// domain keys touched by a flush before the backing latest store is mutated.
type CommitmentMutationRecorder interface {
	RecordCommitmentMutations(ctx context.Context, mutations []Mutation) error
}

// TemporalTx is the narrow local contract that future state execution should
// depend on instead of calling rawdb latest/history/commitment accessors
// directly. It is intentionally small: latest reads and writes, historical
// reads, txNum tracking, flushing, and commitment.
type TemporalTx interface {
	LatestReader
	AsOfReader
	Writer

	SetTxNum(txNum uint64)
	TxNum() uint64
	Flush(ctx context.Context) error
	Discard()
	SeekCommitment(ctx context.Context) (txNum, blockNum uint64, err error)
	ComputeCommitment(ctx context.Context, blockNum, txNum uint64) (common.Hash, error)
	Close() error
}

type SharedDomainTxConfig struct {
	Latest     LatestReader
	Writer     Writer
	History    AsOfReader
	Commitment CommitmentProcessor
	Hooks      Hooks
}

// SharedDomainTx is a thin adapter shaped after Erigon's SharedDomains. It
// keeps block/transaction-local mutations in an overlay, reads through to a
// latest-state parent, delegates historical reads to a single AsOfReader, and
// routes commitment work through a single processor.
//
// StateDB commit uses this transaction for generic-KV latest writes. Account
// envelope writes and parts of the history/commitment pipeline are still being
// collapsed onto the same dependency direction.
type SharedDomainTx struct {
	txNum      uint64
	overlay    *Overlay
	writer     Writer
	history    AsOfReader
	commitment CommitmentProcessor
	closed     bool
}

var _ TemporalTx = (*SharedDomainTx)(nil)

func NewSharedDomainTx(cfg SharedDomainTxConfig) *SharedDomainTx {
	return &SharedDomainTx{
		overlay:    NewOverlay(cfg.Latest, WithHooks(cfg.Hooks)),
		writer:     cfg.Writer,
		history:    cfg.History,
		commitment: cfg.Commitment,
	}
}

func (tx *SharedDomainTx) SetTxNum(txNum uint64) {
	if tx == nil {
		return
	}
	tx.txNum = txNum
	if history, ok := tx.history.(HeadTxNumSetter); ok {
		history.SetHeadTxNum(txNum)
	}
}

func (tx *SharedDomainTx) TxNum() uint64 {
	if tx == nil {
		return 0
	}
	return tx.txNum
}

func (tx *SharedDomainTx) GetLatest(owner common.Address, domain kvdomains.KVDomain, key []byte) ([]byte, bool, error) {
	if err := tx.checkOpen(); err != nil {
		return nil, false, err
	}
	return tx.overlay.GetLatest(owner, domain, key)
}

func (tx *SharedDomainTx) GetAsOf(owner common.Address, domain kvdomains.KVDomain, key []byte, txNum uint64) ([]byte, bool, error) {
	if err := tx.checkOpen(); err != nil {
		return nil, false, err
	}
	if tx.history == nil {
		return nil, false, nil
	}
	return tx.history.GetAsOf(owner, domain, key, txNum)
}

func (tx *SharedDomainTx) DomainPut(owner common.Address, domain kvdomains.KVDomain, key, value []byte) error {
	if err := tx.checkOpen(); err != nil {
		return err
	}
	return tx.overlay.DomainPut(owner, domain, key, value)
}

func (tx *SharedDomainTx) DomainDel(owner common.Address, domain kvdomains.KVDomain, key []byte) error {
	if err := tx.checkOpen(); err != nil {
		return err
	}
	return tx.overlay.DomainDel(owner, domain, key)
}

func (tx *SharedDomainTx) DomainDelPrefix(owner common.Address, domain kvdomains.KVDomain, prefix []byte) error {
	if err := tx.checkOpen(); err != nil {
		return err
	}
	return tx.overlay.DomainDelPrefix(owner, domain, prefix)
}

func (tx *SharedDomainTx) Flush(ctx context.Context) error {
	if err := tx.checkOpen(); err != nil {
		return err
	}
	if recorder, ok := tx.commitment.(CommitmentMutationRecorder); ok {
		if err := recorder.RecordCommitmentMutations(ctx, tx.Mutations()); err != nil {
			return err
		}
	}
	return tx.overlay.FlushTo(tx.writer)
}

func (tx *SharedDomainTx) Discard() {
	if tx == nil || tx.overlay == nil {
		return
	}
	tx.overlay.Discard()
}

func (tx *SharedDomainTx) SeekCommitment(ctx context.Context) (uint64, uint64, error) {
	if err := tx.checkOpen(); err != nil {
		return 0, 0, err
	}
	if tx.commitment == nil {
		return 0, 0, ErrNilCommitmentProcessor
	}
	return tx.commitment.SeekCommitment(ctx)
}

func (tx *SharedDomainTx) ComputeCommitment(ctx context.Context, blockNum, txNum uint64) (common.Hash, error) {
	if err := tx.checkOpen(); err != nil {
		return common.Hash{}, err
	}
	if tx.commitment == nil {
		return common.Hash{}, ErrNilCommitmentProcessor
	}
	return tx.commitment.ComputeCommitment(ctx, blockNum, txNum)
}

func (tx *SharedDomainTx) Close() error {
	if tx == nil || tx.closed {
		return nil
	}
	tx.Discard()
	tx.closed = true
	return nil
}

func (tx *SharedDomainTx) Mutations() []Mutation {
	if tx == nil || tx.overlay == nil {
		return nil
	}
	return tx.overlay.Mutations()
}

func (tx *SharedDomainTx) Metrics() Metrics {
	if tx == nil || tx.overlay == nil {
		return Metrics{}
	}
	return tx.overlay.Metrics()
}

func (tx *SharedDomainTx) checkOpen() error {
	if tx == nil || tx.closed {
		return ErrTemporalTxClosed
	}
	return nil
}
