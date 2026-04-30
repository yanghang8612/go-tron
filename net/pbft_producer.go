// Package net — pbft_producer.go: SR-side PBFT signing scaffolding (M6b slice 1).
//
// Slice 1 ports java-tron's PbftMessage builders (PREPREPARE / PREPARE / COMMIT)
// and registers a no-op block hook on witness nodes. The real three-phase
// state machine, peer broadcast, and COMMIT aggregation land in slice 2 — see
// docs/superpowers/specs/2026-04-30-m6b-sr-signing-design.md.
//
// Spec assumption (NOT byte-level verified against a live java-tron):
// the wire format below is read directly from
//   consensus/src/main/java/org/tron/consensus/pbft/message/PbftMessage.java
// and matches what net/pbft_handler.go's receive parser already accepts. A
// live cross-impl byte comparison is deferred to slice 2.

package net

import (
	"crypto/ecdsa"
	"crypto/sha256"
	"fmt"
	"log"

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

// PbftProducer holds the local SR's signing key and constructs / (in slice 2)
// broadcasts PBFT_MSG payloads.
type PbftProducer struct {
	chain  *core.BlockChain
	db     ethdb.KeyValueStore
	server *p2p.Server  // reserved for slice 2 (forwardMessage)
	sync   *SyncService // optional: skip sending while syncing
	srKey  *ecdsa.PrivateKey
	srAddr tcommon.Address
}

// NewPbftProducer builds a producer bound to the given SR key. The server and
// sync arguments may be nil in tests; in production they are wired by main.go.
func NewPbftProducer(chain *core.BlockChain, db ethdb.KeyValueStore, server *p2p.Server, sync *SyncService, key *ecdsa.PrivateKey) *PbftProducer {
	if key == nil {
		return nil
	}
	return &PbftProducer{
		chain:  chain,
		db:     db,
		server: server,
		sync:   sync,
		srKey:  key,
		srAddr: crypto.PubkeyToAddress(&key.PublicKey),
	}
}

// SrAddress returns the address derived from the SR signing key.
func (p *PbftProducer) SrAddress() tcommon.Address { return p.srAddr }

// signPbftRaw marshals raw, signs SHA-256(rawBytes) with the local SR key, and
// returns the wire-encoded PBFTMessage payload (the same bytes that go onto
// the p2p socket as the body of a MsgPbftMsg / 0x34 frame).
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

// BuildBlockPrePrepareMsg constructs a signed PREPREPARE for the given block.
//
// Mirrors java-tron PbftMessage.prePrepareBlockMsg:
//
//	Raw {
//	  msg_type  = PREPREPARE
//	  data_type = BLOCK
//	  view_n    = block.Number()
//	  epoch     = epoch
//	  data      = block.ID().Hash[:]   // 32-byte: first 8 = num BE
//	}
//
// epoch is supplied by the caller. In production this is the maintenance-period
// timestamp computed by the consensus layer; slice 1's tests pass it explicitly.
func (p *PbftProducer) BuildBlockPrePrepareMsg(block *types.Block, epoch int64) ([]byte, error) {
	if block == nil {
		return nil, fmt.Errorf("nil block")
	}
	id := block.ID()
	raw := &corepb.PBFTMessage_Raw{
		MsgType:  corepb.PBFTMessage_PREPREPARE,
		DataType: corepb.PBFTMessage_BLOCK,
		ViewN:    int64(block.Number()),
		Epoch:    epoch,
		Data:     id.Hash[:],
	}
	return signPbftRaw(raw, p.srKey)
}

// BuildPrepareMsg derives a signed PREPARE from a parsed PREPREPARE Raw.
//
// Mirrors java-tron PbftMessage.buildPrePareMessage / buildMessageCapsule:
// keep view_n / data_type / epoch / data, only flip msg_type and re-sign with
// the local SR key.
func (p *PbftProducer) BuildPrepareMsg(parent *corepb.PBFTMessage_Raw) ([]byte, error) {
	return p.deriveAndSign(parent, corepb.PBFTMessage_PREPARE)
}

// BuildCommitMsg derives a signed COMMIT from a parsed PREPARE (or PREPREPARE)
// Raw — same field-cloning rules as BuildPrepareMsg.
func (p *PbftProducer) BuildCommitMsg(parent *corepb.PBFTMessage_Raw) ([]byte, error) {
	return p.deriveAndSign(parent, corepb.PBFTMessage_COMMIT)
}

func (p *PbftProducer) deriveAndSign(parent *corepb.PBFTMessage_Raw, mt corepb.PBFTMessage_MsgType) ([]byte, error) {
	if parent == nil {
		return nil, fmt.Errorf("nil parent raw")
	}
	// Copy data slice to avoid sharing storage with the caller's Raw.
	data := append([]byte(nil), parent.GetData()...)
	raw := &corepb.PBFTMessage_Raw{
		MsgType:  mt,
		DataType: parent.GetDataType(),
		ViewN:    parent.GetViewN(),
		Epoch:    parent.GetEpoch(),
		Data:     data,
	}
	return signPbftRaw(raw, p.srKey)
}

// allowPBFT mirrors net/pbft_handler.go: gate on the AllowPbft fork bit. We
// re-implement the check (rather than expose pbft_handler's helper) to keep
// the receive-side surface untouched.
func (p *PbftProducer) allowPBFT() bool {
	if p.chain == nil {
		return false
	}
	headNum := p.chain.CurrentBlock().Number()
	dp := state.LoadDynamicProperties(p.db)
	return forks.IsActive(forks.AllowPbft, headNum, dp)
}

// isLocalSR reports whether our srAddr is in the current or previous shuffled
// witness set — equivalent to java-tron's getSrMinerList(epoch) returning a
// non-empty list for our key.
func (p *PbftProducer) isLocalSR() bool {
	for _, w := range rawdb.ReadShuffledWitnesses(p.db) {
		if w == p.srAddr {
			return true
		}
	}
	for _, w := range rawdb.ReadPreviousShuffledWitnesses(p.db) {
		if w == p.srAddr {
			return true
		}
	}
	return false
}

// OnBlockApplied is the BlockChain.AddBlockHook callback. Slice 1 is a no-op:
// it just runs the gates that slice 2 will reuse and logs at debug level. No
// message is built, no peer is contacted, no DB key is touched.
func (p *PbftProducer) OnBlockApplied(block *types.Block) {
	if p == nil || block == nil {
		return
	}
	if !p.allowPBFT() {
		return
	}
	if p.sync != nil && p.sync.IsSyncing() {
		return
	}
	if !p.isLocalSR() {
		return
	}
	// Slice 2 will: BuildBlockPrePrepareMsg, broadcast via p.server, then
	// re-enter PbftHandler.onPrePrepare for self-counting.
	log.Printf("pbft-producer: slice-1 no-op for block #%d (sr=%x)", block.Number(), p.srAddr[:6])
}
