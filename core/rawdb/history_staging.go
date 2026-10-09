package rawdb

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"sync"

	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/pointread"
)

// The staging database retains the original payload keys and encodings. Only
// ownership and durability metadata use the new schema-owned key family.
const HistoryStagingFormatVersion = uint8(1)

var (
	ErrHistoryStagingUninitialized = errors.New("rawdb: history staging identity not initialized")
	ErrHistoryStagingConflict      = errors.New("rawdb: history staging identity or epoch conflict")
	ErrHistoryStagingIncomplete    = errors.New("rawdb: history staging route or receipt incomplete")
	ErrHistoryStagingResetting     = errors.New("rawdb: history staging reset in progress")
	ErrHistoryStagingColdOwned     = errors.New("rawdb: history row is cold-owned")
)

type HistoryStagingIdentity struct {
	Version     uint8
	GenesisHash common.Hash
	NetworkID   uint64
	SourceID    [32]byte // hash of the canonical source DB path and chain identity
	TargetID    [32]byte // hash of the canonical staging DB path and chain identity
}

type HistoryStagingOwner uint8

const (
	HistoryStagingOwnerUnknown HistoryStagingOwner = iota
	HistoryStagingOwnerSource
	HistoryStagingOwnerTarget
	HistoryStagingOwnerCold
)

// BlockProof is supplied by the canonical writer/CLI under its chain/index
// guard. There must be exactly one entry for every height in a full bucket.
type HistoryStagingBlockProof struct {
	Number     uint64
	Hash       common.Hash
	BeginTxNum uint64
	EndTxNum   uint64
}

// ColdSpan is an authenticated old cold-covered subrange. ContentID is the
// logical trio identity, not a file path or manifest generation.
type HistoryStagingColdSpan struct {
	From, To      uint64
	ContentID     [32]byte
	SemanticHash  [32]byte
	TxRangeDigest [32]byte
	RowCount      uint64
}

// Proof is an immutable claim input. The caller must certify Finish, index,
// solid, retention, flush/settled and cold coverage under the canonical guard;
// rawdb independently checks structure and all source tx-range rows.
type HistoryStagingProof struct {
	Bucket          uint64
	Epoch           uint64
	EligibleThrough uint64
	Blocks          []HistoryStagingBlockProof
	ColdSpans       []HistoryStagingColdSpan
	FinishBlock     uint64
	FinishHash      common.Hash
	IndexBlock      uint64
	IndexHash       common.Hash
}

type HistoryStagingClaim struct {
	Version      uint8
	Bucket       uint64
	Epoch        uint64
	WriteVersion uint64
	ClaimID      [32]byte
	ProofDigest  [32]byte
	SourceDigest [32]byte
	SourceRows   uint64
	SourceBytes  uint64
	TargetReady  bool
	Cancelled    bool
	Proof        HistoryStagingProof
}

type HistoryStagingReceipt struct {
	Version      uint8
	Bucket       uint64
	Epoch        uint64
	ClaimID      [32]byte
	ProofDigest  [32]byte
	DataDigest   [32]byte
	PayloadRows  uint64
	PayloadBytes uint64
	TxRangeRows  uint64
	ChunkRows    uint64
}

type HistoryStagingRoute struct {
	Version          uint8
	Bucket           uint64
	Epoch            uint64
	WriteVersion     uint64
	Owner            HistoryStagingOwner
	ReceiptDigest    [32]byte
	SourceCleared    bool
	TargetCleared    bool
	ColdBindingEpoch uint64
}

type HistoryStagingColdBinding struct {
	Version       uint8
	Bucket        uint64
	Epoch         uint64
	BindingEpoch  uint64
	ManifestEpoch uint64
	Spans         []HistoryStagingColdSpan
}

type HistoryStagingResetIntent struct {
	Version      uint8
	OldEpoch     uint64
	NewEpoch     uint64
	TargetHeight uint64
	TargetHash   common.Hash
	BlockDigest  [32]byte
	ReadyThrough uint64
	Complete     bool
}

// RouteBarrier records the verified offline cutover inventory. The normal
// node refuses to start without matching identity, epoch and route coverage.
type HistoryStagingRouteBarrier struct {
	Version         uint8
	Epoch           uint64
	ThroughBucket   uint64 // bucket index containing the frozen head, not block height
	EligibleThrough uint64 // last eligible complete bucket index, not block height
	PlanDigest      [32]byte
	CandidateSHA    [32]byte
	RouteDigest     [32]byte
}

// HistoryStagingMaxDecodedBytes is the shared changeset codec's per-block
// decoded ceiling. The CLI must not offer a migration budget above this limit.
const HistoryStagingMaxDecodedBytes = stateDomainChangeBlockMaxDecodedBytes

// HistoryStagingMaxCopyPhysicalBytes leaves room for all bounded source,
// copy, and verification passes under the total per-bucket work budget.
func HistoryStagingMaxCopyPhysicalBytes(maxWorkBytes uint64) uint64 { return maxWorkBytes / 4 }

// HistoryStagingLimits are mandatory hard limits. Every numeric field and the
// caller-supplied free-space check must be nonzero or nonnil, respectively.
type HistoryStagingLimits struct {
	MaxRowBytes     uint64
	MaxBucketBytes  uint64
	MaxBatchBytes   uint64
	MaxWorkBytes    uint64
	MaxDecodedBytes uint64
	MinFreeBytes    uint64
	FreeBytes       func() (uint64, error)
	// Checkpoint optionally yields runtime maintenance between bounded rows,
	// decoded blocks and batches. It runs outside route/canonical writer locks.
	// Nil preserves the offline protocol. A failure never authorizes adoption.
	Checkpoint func(workBytes uint64) error
}

func (l HistoryStagingLimits) checkpoint(workBytes uint64) error {
	if l.Checkpoint != nil {
		return l.Checkpoint(workBytes)
	}
	return nil
}

// Manager is the shared storage protocol used by the node and offline CLI.
// Chain/Finish/cold manifest verification belongs to core callbacks, never
// to an unsafe rawdb approximation of those states.
type HistoryStagingManager struct {
	hot, stage ethdb.KeyValueStore
	identity   HistoryStagingIdentity
	routeMu    sync.RWMutex
	jobMu      sync.RWMutex // transfer reads; reset intent takes exclusive drain
	initMu     sync.Mutex
	leaseMu    sync.Mutex
	leases     map[uint64]uint64
	generation uint64
	// coldGCMu spans a cold-binding mutation through WAL sync. A failed sync
	// leaves GC denied until reopen/recovery establishes durable state.
	coldGCMu        sync.RWMutex
	coldGCUncertain bool
}

type historyStagingSyncStore interface {
	ethdb.KeyValueStore
	pointread.KeyValueSnapshotter
	SyncKeyValue() error
}

func NewHistoryStagingManager(hot, stage ethdb.KeyValueStore, identity HistoryStagingIdentity) (*HistoryStagingManager, error) {
	if hot == nil || stage == nil || hot == stage {
		return nil, errors.New("rawdb: history staging requires distinct source and target stores")
	}
	if _, ok := hot.(historyStagingSyncStore); !ok {
		return nil, errors.New("rawdb: history staging source requires snapshots and WAL sync")
	}
	if _, ok := stage.(historyStagingSyncStore); !ok {
		return nil, errors.New("rawdb: history staging target requires snapshots and WAL sync")
	}
	if identity.Version != HistoryStagingFormatVersion || identity.GenesisHash == (common.Hash{}) || identity.SourceID == identity.TargetID || identity.SourceID == ([32]byte{}) || identity.TargetID == ([32]byte{}) {
		return nil, errors.New("rawdb: invalid history staging identity")
	}
	return &HistoryStagingManager{hot: hot, stage: stage, identity: identity, leases: make(map[uint64]uint64)}, nil
}

func (p HistoryStagingProof) validate() error {
	first, last, err := StateHistoryChunkBucketBounds(p.Bucket)
	if err != nil || p.Bucket == 0 || p.Epoch == 0 || p.EligibleThrough < last || len(p.Blocks) != int(StateHistoryChunkBucketBlocks) || p.FinishBlock < last || p.IndexBlock < last || p.FinishHash == (common.Hash{}) || p.IndexHash == (common.Hash{}) {
		return errors.New("rawdb: invalid history staging eligibility proof")
	}
	var prevEnd uint64
	for i, block := range p.Blocks {
		if block.Number != first+uint64(i) || block.Hash == (common.Hash{}) || block.EndTxNum < block.BeginTxNum || (i > 0 && (prevEnd == ^uint64(0) || block.BeginTxNum != prevEnd+1)) {
			return fmt.Errorf("rawdb: invalid history staging tx range at block %d", block.Number)
		}
		prevEnd = block.EndTxNum
	}
	var previous uint64
	for i, span := range p.ColdSpans {
		if span.From < first || span.To > last || span.To < span.From || (i > 0 && span.From <= previous) || span.ContentID == ([32]byte{}) || span.SemanticHash == ([32]byte{}) || span.TxRangeDigest == ([32]byte{}) {
			return errors.New("rawdb: invalid history staging cold span")
		}
		previous = span.To
	}
	return nil
}

func (p HistoryStagingProof) digest() ([32]byte, error) {
	if err := p.validate(); err != nil {
		return [32]byte{}, err
	}
	encoded, err := encodeHistoryStaging(p)
	if err != nil {
		return [32]byte{}, err
	}
	return sha256.Sum256(encoded), nil
}

func (l HistoryStagingLimits) validate() error {
	if l.MaxRowBytes == 0 || l.MaxBucketBytes == 0 || l.MaxBatchBytes == 0 || l.MaxWorkBytes == 0 || l.MaxDecodedBytes == 0 || l.MaxDecodedBytes > HistoryStagingMaxDecodedBytes || l.MinFreeBytes == 0 || l.FreeBytes == nil || l.MaxRowBytes > l.MaxBatchBytes || l.MaxBatchBytes > l.MaxBucketBytes || l.MaxBucketBytes > l.MaxWorkBytes {
		return errors.New("rawdb: invalid history staging work limits")
	}
	return nil
}

// VerifyHistoryStagingProof is exported for the CLI plan validator.
func VerifyHistoryStagingProof(p HistoryStagingProof) error { return p.validate() }

// BeginClaim, CopyClaim, AdoptClaim and ClearSource intentionally expose the
// durable phase boundaries so callers can checkpoint or resume each phase.
// Their implementations live beside this shared contract.
type HistoryStagingRevalidate func(context.Context, HistoryStagingProof) error
