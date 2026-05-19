package net

import (
	"crypto/sha256"
	"encoding/binary"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ethereum/go-ethereum/ethdb"
	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/types"
	"github.com/tronprotocol/go-tron/crypto"
	"github.com/tronprotocol/go-tron/p2p"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	"google.golang.org/protobuf/proto"
)

const (
	pbftCommitCacheTTL     = 10 * time.Minute
	pbftCommitCacheMaxSize = 200
)

type commitCacheEntry struct {
	result *corepb.PBFTCommitResult
	raw    *corepb.PBFTMessage_Raw
	added  time.Time
}

// PbftDataSyncHandler processes PBFT_COMMIT_MSG (0x14) messages: cache pre-
// aggregated commit results and validate them when a matching block is inserted.
type PbftDataSyncHandler struct {
	mu    sync.Mutex
	cache map[int64]*commitCacheEntry // viewN or epoch → entry

	chain *core.BlockChain
	db    ethdb.KeyValueStore

	// pbftActive sticks at true once AllowPbft flips on. The PBFT activation
	// flag is monotonic — once a proposal sets allow_pbft=1 the chain cannot
	// turn it off — so a one-shot atomic is sufficient and lets allowPBFT()
	// skip the DB read on every post-activation block. Pre-activation we
	// fall through to a *single* point Get on the AllowPbft DP key rather
	// than the previous full state.LoadDynamicProperties scan (which fired
	// ~133 Pebble Gets per block hook and was empirically the dominant cost
	// of the BlockHook fan-out — Nile soak at h≈890k showed hooks=1s/8s of
	// wall time, almost all of it here).
	pbftActive atomic.Bool
}

// NewPbftDataSyncHandler creates a handler. It must be wired into BlockChain's
// AddBlockHook so ProcessOnBlock is called after each InsertBlock.
func NewPbftDataSyncHandler(chain *core.BlockChain, db ethdb.KeyValueStore) *PbftDataSyncHandler {
	return &PbftDataSyncHandler{
		cache: make(map[int64]*commitCacheEntry),
		chain: chain,
		db:    db,
	}
}

// Start satisfies node.Lifecycle (no background goroutines needed).
func (h *PbftDataSyncHandler) Start() error { return nil }

// Stop satisfies node.Lifecycle.
func (h *PbftDataSyncHandler) Stop() error { return nil }

// HandleCommitMsg handles an incoming PBFT_COMMIT_MSG (0x14).
func (h *PbftDataSyncHandler) HandleCommitMsg(peer *p2p.Peer, payload []byte) {
	if !h.allowPBFT() {
		return
	}

	var result corepb.PBFTCommitResult
	if err := proto.Unmarshal(payload, &result); err != nil {
		return
	}

	var raw corepb.PBFTMessage_Raw
	if err := proto.Unmarshal(result.GetData(), &raw); err != nil {
		return
	}

	viewN := raw.GetViewN()

	h.mu.Lock()
	defer h.mu.Unlock()

	if len(h.cache) >= pbftCommitCacheMaxSize {
		h.evictStaleNoLock()
		if len(h.cache) >= pbftCommitCacheMaxSize {
			return
		}
	}
	h.cache[viewN] = &commitCacheEntry{result: &result, raw: &raw, added: time.Now()}
}

// ProcessOnBlock is called after a block is successfully inserted. It looks up
// a cached PBFTCommitResult for the block and validates + persists it.
func (h *PbftDataSyncHandler) ProcessOnBlock(block *types.Block) {
	if !h.allowPBFT() {
		return
	}

	h.mu.Lock()
	entry := h.cache[int64(block.Number())]
	if entry == nil {
		// Try epoch key (SRL messages use epoch as viewN). Reading the two
		// DP keys directly avoids state.LoadDynamicProperties, which would
		// otherwise scan ~133 keys just to compute one epoch number.
		nextMaint := readDPInt64(h.db, "next_maintenance_time")
		interval := readDPInt64(h.db, "maintenance_time_interval")
		entry = h.cache[nextMaint-interval]
	}
	h.mu.Unlock()

	if entry == nil {
		return
	}

	witnesses := rawdb.ReadShuffledWitnesses(h.db)
	if len(witnesses) == 0 {
		witnesses = rawdb.ReadPreviousShuffledWitnesses(h.db)
	}

	sigs := entry.result.GetSignature()
	rawBytes := entry.result.GetData()
	if !h.validPbftSign(rawBytes, sigs, witnesses) {
		return
	}

	switch entry.raw.GetDataType() {
	case corepb.PBFTMessage_BLOCK:
		rawdb.WriteBlockSignData(h.db, int64(block.Number()), entry.result)
		rawdb.WriteLatestPbftBlockNum(h.db, int64(block.Number()))
	case corepb.PBFTMessage_SRL:
		rawdb.WriteSrSignData(h.db, entry.raw.GetEpoch(), entry.result)
	}
}

// validPbftSign checks that sigs contains at least pbftAgreeCount valid
// signatures over SHA-256(rawBytes) from known SR witnesses.
func (h *PbftDataSyncHandler) validPbftSign(rawBytes []byte, sigs [][]byte, witnesses []tcommon.Address) bool {
	hash := sha256.Sum256(rawBytes)
	seen := make(map[tcommon.Address]struct{})
	valid := 0
	for _, sig := range sigs {
		if len(sig) != 65 {
			continue
		}
		pub, err := crypto.SigToPub(hash[:], sig)
		if err != nil {
			continue
		}
		addr := crypto.PubkeyToAddress(pub)
		if _, dup := seen[addr]; dup {
			continue
		}
		for _, w := range witnesses {
			if w == addr {
				seen[addr] = struct{}{}
				valid++
				break
			}
		}
	}
	return valid >= pbftAgreeCount
}

func (h *PbftDataSyncHandler) allowPBFT() bool {
	if h.pbftActive.Load() {
		return true
	}
	// One point Get on the AllowPbft key — versus the previous
	// LoadDynamicProperties full scan. Once active, the atomic short-circuits
	// every future call.
	if readDPInt64(h.db, "allow_pbft") >= 1 {
		h.pbftActive.Store(true)
		return true
	}
	return false
}

// readDPInt64 fetches a single DynamicProperties key as a big-endian int64.
// Returns 0 when the key is absent or malformed. Equivalent to the per-key
// branch inside state.LoadDynamicProperties, but without the surrounding
// 133-key scan.
func readDPInt64(db ethdb.KeyValueReader, name string) int64 {
	data := rawdb.ReadDynamicProperty(db, name)
	if len(data) != 8 {
		return 0
	}
	return int64(binary.BigEndian.Uint64(data))
}

func (h *PbftDataSyncHandler) evictStaleNoLock() {
	now := time.Now()
	for k, e := range h.cache {
		if now.Sub(e.added) > pbftCommitCacheTTL {
			delete(h.cache, k)
		}
	}
}
