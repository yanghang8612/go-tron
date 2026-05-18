// Package net — pbft_producer.go: SR-side PBFT signing & emit (M6b slice 2).
//
// Slice 1 ported java-tron's PbftMessage builders (PREPREPARE / PREPARE /
// COMMIT) and registered a no-op block hook in --witness mode.
//
// Slice 2 promotes the producer to a first-class participant in the
// three-phase state machine implemented by net/pbft_handler.go:
//
//   * `OnBlockApplied` builds and broadcasts a BLOCK PREPREPARE for the
//     freshly inserted block (mirrors java-tron PbftManager.blockPrePrepare:
//     consensus/src/main/java/org/tron/consensus/pbft/PbftManager.java:42).
//
//   * `OnMaintenance` (new chain hook) builds and broadcasts an SRL
//     PREPREPARE carrying the post-rotation active-witness set (mirrors
//     PbftManager.srPrePrepare: PbftManager.java:57).
//
//   * `EmitPrepare` / `EmitCommit` are invoked by the receive-side state
//     machine in pbft_handler.go when an inbound PREPREPARE / quorum-of-
//     PREPARE event fires, mirroring PbftMessageHandle.onPrePrepare's
//     `for (Miner miner : getSrMinerList(epoch))` loop
//     (PbftMessageHandle.java:128) and onPrepare's analogous loop at line 173.
//
// Multi-SR key support: the producer can hold N keys (one per local SR).
// Every emit method iterates the keys and produces one signed payload per
// local SR whose address is in the current/previous shuffled witness set —
// matching java-tron's `getSrMinerList(epoch)` which filters Param.miners
// against the maintenance witness list.

package net

import (
	"crypto/ecdsa"
	"crypto/sha256"
	"fmt"
	"log"
	"sync"

	"github.com/ethereum/go-ethereum/ethdb"
	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core"
	"github.com/tronprotocol/go-tron/core/forks"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/state"
	"github.com/tronprotocol/go-tron/core/types"
	"github.com/tronprotocol/go-tron/crypto"
	"github.com/tronprotocol/go-tron/p2p"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	"google.golang.org/protobuf/proto"
)

// PbftProducer holds the local SR signing keys and constructs / broadcasts
// PBFT_MSG payloads. Wire compatibility with java-tron is locked by the
// round-trip tests in pbft_producer_test.go and pbft_handler_test.go.
type PbftProducer struct {
	chain   *core.BlockChain
	db      ethdb.KeyValueStore
	server  *p2p.Server
	sync    *SyncService
	handler *PbftHandler // optional: when set, self-emitted msgs re-enter the SM here

	mu      sync.RWMutex
	srKeys  []*ecdsa.PrivateKey
	srAddrs []tcommon.Address

	// broadcast is the per-payload fan-out callback. In production it sends
	// to every peer of p.server; tests override it to capture outbound
	// traffic. Set via SetBroadcastFunc.
	broadcast func(payload []byte)
}

// NewPbftProducer builds a producer bound to the given SR keys. Returns nil
// if keys is empty so callers can detect a misconfigured witness mode.
func NewPbftProducer(chain *core.BlockChain, db ethdb.KeyValueStore, server *p2p.Server, sync *SyncService, keys ...*ecdsa.PrivateKey) *PbftProducer {
	// Filter out nil keys defensively.
	filtered := make([]*ecdsa.PrivateKey, 0, len(keys))
	addrs := make([]tcommon.Address, 0, len(keys))
	for _, k := range keys {
		if k == nil {
			continue
		}
		filtered = append(filtered, k)
		addrs = append(addrs, crypto.PubkeyToAddress(&k.PublicKey))
	}
	if len(filtered) == 0 {
		return nil
	}
	p := &PbftProducer{
		chain:   chain,
		db:      db,
		server:  server,
		sync:    sync,
		srKeys:  filtered,
		srAddrs: addrs,
	}
	p.broadcast = p.defaultBroadcast
	return p
}

// SetBroadcastFunc overrides the outbound fan-out — used by tests to capture
// payloads. Call before any emit.
func (p *PbftProducer) SetBroadcastFunc(fn func([]byte)) {
	if p == nil {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if fn == nil {
		p.broadcast = p.defaultBroadcast
		return
	}
	p.broadcast = fn
}

// SetHandler wires the receive-side state machine so self-emitted PBFT
// messages re-enter onPrePrepare/onPrepare/onCommit for self-counting,
// matching java-tron's `forwardMessage(...); onPrepare(paMessage)` pattern
// (PbftMessageHandle.java:130).
func (p *PbftProducer) SetHandler(h *PbftHandler) {
	if p == nil {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.handler = h
}

// SrAddresses returns the addresses derived from the configured SR keys.
func (p *PbftProducer) SrAddresses() []tcommon.Address {
	if p == nil {
		return nil
	}
	p.mu.RLock()
	defer p.mu.RUnlock()
	out := make([]tcommon.Address, len(p.srAddrs))
	copy(out, p.srAddrs)
	return out
}

func (p *PbftProducer) defaultBroadcast(payload []byte) {
	if p.server == nil {
		return
	}
	for _, peer := range p.server.Peers() {
		peer.Send(p2p.MsgPbftMsg, payload)
	}
}

// signPbftRaw marshals raw, signs SHA-256(rawBytes) with key, and returns the
// wire-encoded PBFTMessage payload.
func signPbftRaw(raw *corepb.PBFTMessage_Raw, key *ecdsa.PrivateKey) ([]byte, error) {
	rawBytes, err := proto.Marshal(raw)
	if err != nil {
		return nil, fmt.Errorf("marshal raw: %w", err)
	}
	hash := sha256.Sum256(rawBytes)
	sig, err := crypto.Sign(hash[:], key)
	if err != nil {
		return nil, fmt.Errorf("sign: %w", err)
	}
	if len(sig) != 65 {
		return nil, fmt.Errorf("unexpected signature length %d (want 65)", len(sig))
	}
	msg := &corepb.PBFTMessage{RawData: raw, Signature: sig}
	return proto.Marshal(msg)
}

// BuildBlockPrePrepareMsg constructs a signed PREPREPARE for the given block,
// using the producer's first SR key. Mirrors java-tron
// PbftMessage.prePrepareBlockMsg (consensus/.../message/PbftMessage.java:31).
//
//	Raw {
//	  msg_type  = PREPREPARE
//	  data_type = BLOCK
//	  view_n    = block.Number()
//	  epoch     = epoch
//	  data      = block.ID().Hash[:]
//	}
func (p *PbftProducer) BuildBlockPrePrepareMsg(block *types.Block, epoch int64) ([]byte, error) {
	return p.BuildBlockPrePrepareMsgWith(block, epoch, p.firstKey())
}

// BuildBlockPrePrepareMsgWith is the multi-key variant.
func (p *PbftProducer) BuildBlockPrePrepareMsgWith(block *types.Block, epoch int64, key *ecdsa.PrivateKey) ([]byte, error) {
	if block == nil {
		return nil, fmt.Errorf("nil block")
	}
	if key == nil {
		return nil, fmt.Errorf("nil key")
	}
	id := block.ID()
	raw := &corepb.PBFTMessage_Raw{
		MsgType:  corepb.PBFTMessage_PREPREPARE,
		DataType: corepb.PBFTMessage_BLOCK,
		ViewN:    int64(block.Number()),
		Epoch:    epoch,
		Data:     id.Hash[:],
	}
	return signPbftRaw(raw, key)
}

// BuildSrlPrePrepareMsg constructs a signed SRL PREPREPARE carrying the
// given witness set. Mirrors java-tron PbftMessage.prePrepareSRLMsg
// (PbftMessage.java:42). For SRL viewN == epoch == nextMaintenanceTime.
func (p *PbftProducer) BuildSrlPrePrepareMsg(witnesses []tcommon.Address, epoch int64, key *ecdsa.PrivateKey) ([]byte, error) {
	if key == nil {
		return nil, fmt.Errorf("nil key")
	}
	srl := &corepb.SRL{}
	for _, w := range witnesses {
		// Java SRL.sr_address is bytes; pass full 21-byte TRON address.
		addr := w
		srl.SrAddress = append(srl.SrAddress, addr[:])
	}
	srlBytes, err := proto.Marshal(srl)
	if err != nil {
		return nil, fmt.Errorf("marshal SRL: %w", err)
	}
	raw := &corepb.PBFTMessage_Raw{
		MsgType:  corepb.PBFTMessage_PREPREPARE,
		DataType: corepb.PBFTMessage_SRL,
		ViewN:    epoch,
		Epoch:    epoch,
		Data:     srlBytes,
	}
	return signPbftRaw(raw, key)
}

// BuildPrepareMsg derives a signed PREPARE from a parsed parent Raw, using
// the producer's first SR key.
func (p *PbftProducer) BuildPrepareMsg(parent *corepb.PBFTMessage_Raw) ([]byte, error) {
	return p.deriveAndSign(parent, corepb.PBFTMessage_PREPARE, p.firstKey())
}

// BuildPrepareMsgWith is the multi-key variant.
func (p *PbftProducer) BuildPrepareMsgWith(parent *corepb.PBFTMessage_Raw, key *ecdsa.PrivateKey) ([]byte, error) {
	return p.deriveAndSign(parent, corepb.PBFTMessage_PREPARE, key)
}

// BuildCommitMsg derives a signed COMMIT from a parsed parent Raw.
func (p *PbftProducer) BuildCommitMsg(parent *corepb.PBFTMessage_Raw) ([]byte, error) {
	return p.deriveAndSign(parent, corepb.PBFTMessage_COMMIT, p.firstKey())
}

// BuildCommitMsgWith is the multi-key variant.
func (p *PbftProducer) BuildCommitMsgWith(parent *corepb.PBFTMessage_Raw, key *ecdsa.PrivateKey) ([]byte, error) {
	return p.deriveAndSign(parent, corepb.PBFTMessage_COMMIT, key)
}

func (p *PbftProducer) deriveAndSign(parent *corepb.PBFTMessage_Raw, mt corepb.PBFTMessage_MsgType, key *ecdsa.PrivateKey) ([]byte, error) {
	if parent == nil {
		return nil, fmt.Errorf("nil parent raw")
	}
	if key == nil {
		return nil, fmt.Errorf("nil key")
	}
	data := append([]byte(nil), parent.GetData()...)
	raw := &corepb.PBFTMessage_Raw{
		MsgType:  mt,
		DataType: parent.GetDataType(),
		ViewN:    parent.GetViewN(),
		Epoch:    parent.GetEpoch(),
		Data:     data,
	}
	return signPbftRaw(raw, key)
}

func (p *PbftProducer) firstKey() *ecdsa.PrivateKey {
	if p == nil {
		return nil
	}
	p.mu.RLock()
	defer p.mu.RUnlock()
	if len(p.srKeys) == 0 {
		return nil
	}
	return p.srKeys[0]
}

// allowPBFT mirrors net/pbft_handler.go: gate on the AllowPbft fork bit.
func (p *PbftProducer) allowPBFT() bool {
	if p == nil || p.chain == nil || p.db == nil {
		return false
	}
	headNum := p.chain.CurrentBlock().Number()
	dp := state.LoadDynamicProperties(p.db)
	return forks.IsActive(forks.AllowPbft, headNum, dp)
}

// localSRKeys returns the subset of (key, addr) pairs whose address is in
// the current OR previous shuffled witness list — equivalent to java-tron's
// `getSrMinerList(epoch)`. Java picks current vs previous based on
// `epoch > beforeMaintenanceTime`; we accept both since the receive-side
// already accepts both (PbftMessageHandle.java:96-99 and pbft_handler.go's
// isSRMember).
func (p *PbftProducer) localSRKeys() ([]*ecdsa.PrivateKey, []tcommon.Address) {
	if p == nil {
		return nil, nil
	}
	if p.db == nil {
		// In tests with a nil db, treat all configured keys as local SRs.
		p.mu.RLock()
		defer p.mu.RUnlock()
		ks := make([]*ecdsa.PrivateKey, len(p.srKeys))
		copy(ks, p.srKeys)
		as := make([]tcommon.Address, len(p.srAddrs))
		copy(as, p.srAddrs)
		return ks, as
	}
	current := rawdb.ReadShuffledWitnesses(p.db)
	previous := rawdb.ReadPreviousShuffledWitnesses(p.db)
	inSet := make(map[tcommon.Address]struct{}, len(current)+len(previous))
	for _, w := range current {
		inSet[w] = struct{}{}
	}
	for _, w := range previous {
		inSet[w] = struct{}{}
	}

	p.mu.RLock()
	defer p.mu.RUnlock()
	keys := make([]*ecdsa.PrivateKey, 0, len(p.srKeys))
	addrs := make([]tcommon.Address, 0, len(p.srAddrs))
	for i, addr := range p.srAddrs {
		if _, ok := inSet[addr]; ok {
			keys = append(keys, p.srKeys[i])
			addrs = append(addrs, addr)
		}
	}
	return keys, addrs
}

// IsLocalSR reports whether ANY of the producer's configured keys is a
// current-or-previous-epoch SR.
func (p *PbftProducer) IsLocalSR() bool {
	keys, _ := p.localSRKeys()
	return len(keys) > 0
}

// canSend gates outbound PBFT messages on (allow_pbft fork active) AND
// (not currently syncing) AND (at least one local SR). Mirrors java-tron
// `PbftMessageHandle.checkIsCanSendMsg(epoch)` (PbftMessageHandle.java:250).
func (p *PbftProducer) canSend() bool {
	if !p.allowPBFT() {
		return false
	}
	if p.sync != nil && (p.sync.IsSyncing() || p.sync.IsPaused()) {
		return false
	}
	return p.IsLocalSR()
}

// OnBlockApplied is the BlockChain.AddBlockHook callback. For each local
// SR this builds a BLOCK PREPREPARE, broadcasts it, and re-enters the
// receive-side state machine for self-counting.
//
// Java parallel: PbftManager.blockPrePrepare iterating
// `pbftMessageHandle.getSrMinerList(epoch)` (PbftManager.java:48).
func (p *PbftProducer) OnBlockApplied(block *types.Block) {
	if p == nil || block == nil {
		return
	}
	if !p.canSend() {
		return
	}
	keys, _ := p.localSRKeys()
	// Java uses the post-maintenance NextMaintenanceTime as the epoch for
	// block PREPREPARE (MaintenanceManager.java:81: `pbftManager.blockPrePrepare(blockCapsule, nextMaintenanceTime)`).
	epoch := p.epoch()
	for _, k := range keys {
		payload, err := p.BuildBlockPrePrepareMsgWith(block, epoch, k)
		if err != nil {
			log.Printf("pbft-producer: build block PREPREPARE: %v", err)
			continue
		}
		p.dispatch(payload)
	}
}

// OnMaintenance is the BlockChain.AddMaintenanceHook callback. For each
// local SR this builds an SRL PREPREPARE for the new witness set,
// broadcasts it, and re-enters the receive-side state machine.
//
// Java parallel: PbftManager.srPrePrepare invoked from
// MaintenanceManager.applyBlock (MaintenanceManager.java:72).
func (p *PbftProducer) OnMaintenance(block *types.Block, newWitnesses []tcommon.Address) {
	if p == nil || block == nil {
		return
	}
	if !p.canSend() {
		return
	}
	keys, _ := p.localSRKeys()
	epoch := p.epoch()
	for _, k := range keys {
		payload, err := p.BuildSrlPrePrepareMsg(newWitnesses, epoch, k)
		if err != nil {
			log.Printf("pbft-producer: build SRL PREPREPARE: %v", err)
			continue
		}
		p.dispatch(payload)
	}
}

// EmitPrepare is invoked by pbft_handler.go onPrePrepare *after* the SR
// membership and dedup checks pass. For each local SR key it builds a
// PREPARE derived from parent and dispatches it.
//
// Java parallel: PbftMessageHandle.onPrePrepare's loop at line 128.
func (p *PbftProducer) EmitPrepare(parent *corepb.PBFTMessage_Raw) {
	if p == nil || parent == nil {
		return
	}
	if !p.canSend() {
		return
	}
	keys, _ := p.localSRKeys()
	for _, k := range keys {
		payload, err := p.BuildPrepareMsgWith(parent, k)
		if err != nil {
			log.Printf("pbft-producer: build PREPARE: %v", err)
			continue
		}
		p.dispatch(payload)
	}
}

// EmitCommit is invoked by pbft_handler.go onPrepare when the PREPARE
// quorum is reached. For each local SR key it builds a COMMIT derived from
// parent and dispatches it.
//
// Java parallel: PbftMessageHandle.onPrepare's loop at line 173.
func (p *PbftProducer) EmitCommit(parent *corepb.PBFTMessage_Raw) {
	if p == nil || parent == nil {
		return
	}
	if !p.canSend() {
		return
	}
	keys, _ := p.localSRKeys()
	for _, k := range keys {
		payload, err := p.BuildCommitMsgWith(parent, k)
		if err != nil {
			log.Printf("pbft-producer: build COMMIT: %v", err)
			continue
		}
		p.dispatch(payload)
	}
}

// dispatch broadcasts a self-built payload to peers and re-enters the
// receive-side SM for self-counting. The SM dispatch happens in a goroutine
// to avoid acquiring smMu while the caller may already hold it.
func (p *PbftProducer) dispatch(payload []byte) {
	p.mu.RLock()
	bc := p.broadcast
	h := p.handler
	p.mu.RUnlock()
	if bc != nil {
		bc(payload)
	}
	if h != nil {
		// Use a goroutine so a self-emit issued from inside an SM transition
		// doesn't recurse and deadlock on smMu.
		go h.HandleSelfPbftMsg(payload)
	}
}

// epoch returns the epoch value used for outbound PREPREPARE messages —
// nextMaintenanceTime, matching java-tron MaintenanceManager.applyBlock.
func (p *PbftProducer) epoch() int64 {
	if p.db == nil {
		return 0
	}
	return state.LoadDynamicProperties(p.db).NextMaintenanceTime()
}
