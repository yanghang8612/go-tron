package net

import (
	"crypto/sha256"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/ethdb"
	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core"
	"github.com/tronprotocol/go-tron/core/forks"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/state"
	"github.com/tronprotocol/go-tron/crypto"
	"github.com/tronprotocol/go-tron/p2p"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	"google.golang.org/protobuf/proto"
)

const (
	pbftDedupTTL     = 10 * time.Minute
	pbftDedupMaxSize = 10000
	pbftBlockExpiry  = 20 // drop if headNum - viewN > this
	pbftGCInterval   = 1 * time.Second
	pbftSMTimeout    = 60 * time.Second
	pbftCacheTTL     = 2 * time.Minute
	pbftCacheMaxSize = 10000
	pbftAgreeCount   = 19 // 27 * 2/3 + 1
)

// pbftCachedMsg holds a PBFT message that arrived out of order.
type pbftCachedMsg struct {
	raw      *corepb.PBFTMessage_Raw
	rawBytes []byte
	sig      []byte
	srAddr   tcommon.Address
	added    time.Time
}

// PbftHandler processes PBFT_MSG (0x34) messages: verify, dedup, forward,
// and run the local three-phase PBFT state machine for a full node.
type PbftHandler struct {
	mu   sync.Mutex // protects dedup map
	smMu sync.Mutex // protects all state machine maps

	chain  *core.BlockChain
	db     ethdb.KeyValueStore
	server *p2p.Server
	sync   *SyncService

	// dedup: global per-message dedup (key = dedupKey including msgType)
	dedup map[string]time.Time

	// State machine (protected by smMu):
	preVotes      map[string]struct{}   // key = no (viewN_dataType)
	pareVoteMap   map[string]struct{}   // key = smKey (viewN_dataType_srAddrHex)
	commitVoteMap map[string]struct{}   // key = smKey
	agreePare     map[string]int        // key = dataKey
	agreeCommit   map[string]int        // key = dataKey
	dataSignCache map[string][][]byte   // key = dataKey → collected sigs
	pareMsgCache  []pbftCachedMsg       // PREPARE msgs before PREPREPARE (cap + TTL)
	commitMsgCache []pbftCachedMsg      // COMMIT msgs before PREPARE (cap + TTL)
	timeOuts      map[string]time.Time  // key = no → first PREPREPARE time
	doneMsg       map[string]struct{}   // key = no → slot has entered commit phase

	quit chan struct{}
	wg   sync.WaitGroup
}

// NewPbftHandler creates a PbftHandler. Call Start() to activate the GC loop.
func NewPbftHandler(chain *core.BlockChain, db ethdb.KeyValueStore, server *p2p.Server, sync *SyncService) *PbftHandler {
	return &PbftHandler{
		chain:         chain,
		db:            db,
		server:        server,
		sync:          sync,
		dedup:         make(map[string]time.Time),
		preVotes:      make(map[string]struct{}),
		pareVoteMap:   make(map[string]struct{}),
		commitVoteMap: make(map[string]struct{}),
		agreePare:     make(map[string]int),
		agreeCommit:   make(map[string]int),
		dataSignCache: make(map[string][][]byte),
		timeOuts:      make(map[string]time.Time),
		doneMsg:       make(map[string]struct{}),
		quit:          make(chan struct{}),
	}
}

// Start launches the background GC goroutine. Satisfies node.Lifecycle.
func (h *PbftHandler) Start() error {
	h.wg.Add(1)
	go h.gcLoop()
	return nil
}

// Stop signals the GC goroutine to exit and waits for it. Satisfies node.Lifecycle.
func (h *PbftHandler) Stop() error {
	close(h.quit)
	h.wg.Wait()
	return nil
}

func (h *PbftHandler) gcLoop() {
	defer h.wg.Done()
	ticker := time.NewTicker(pbftGCInterval)
	defer ticker.Stop()
	for {
		select {
		case <-h.quit:
			return
		case <-ticker.C:
			h.gcDedup()
			h.gcSMTimeout()
		}
	}
}

func (h *PbftHandler) gcDedup() {
	now := time.Now()
	h.mu.Lock()
	defer h.mu.Unlock()
	for k, expiry := range h.dedup {
		if now.After(expiry) {
			delete(h.dedup, k)
		}
	}
}

func (h *PbftHandler) gcSMTimeout() {
	now := time.Now()
	h.smMu.Lock()
	defer h.smMu.Unlock()
	for no, start := range h.timeOuts {
		if now.Sub(start) > pbftSMTimeout {
			h.removeNoLock(no)
		}
	}
	// Evict stale msg cache entries
	h.pareMsgCache = evictCacheTTL(h.pareMsgCache, now)
	h.commitMsgCache = evictCacheTTL(h.commitMsgCache, now)
}

func evictCacheTTL(cache []pbftCachedMsg, now time.Time) []pbftCachedMsg {
	out := cache[:0]
	for _, m := range cache {
		if now.Sub(m.added) < pbftCacheTTL {
			out = append(out, m)
		}
	}
	return out
}

func (h *PbftHandler) allowPBFT() bool {
	headNum := h.chain.CurrentBlock().Number()
	dp := state.LoadDynamicProperties(h.db)
	return forks.IsActive(forks.AllowPbft, headNum, dp)
}

// pbftSigToAddress recovers the SR address from a PBFT signature.
// hash = SHA-256(rawDataBytes); signature is 65-byte secp256k1 (r|s|v).
func pbftSigToAddress(rawDataBytes, sig []byte) (tcommon.Address, error) {
	hash := sha256.Sum256(rawDataBytes)
	pub, err := crypto.SigToPub(hash[:], sig)
	if err != nil {
		return tcommon.Address{}, err
	}
	return crypto.PubkeyToAddress(pub), nil
}

func (h *PbftHandler) isSRMember(addr tcommon.Address) bool {
	for _, w := range rawdb.ReadShuffledWitnesses(h.db) {
		if w == addr {
			return true
		}
	}
	for _, w := range rawdb.ReadPreviousShuffledWitnesses(h.db) {
		if w == addr {
			return true
		}
	}
	return false
}

// pbftDedupKey includes msgType: one (SR, slot, msgType) only processed once.
func pbftDedupKey(viewN int64, dt corepb.PBFTMessage_DataType, srAddr tcommon.Address, msgType corepb.PBFTMessage_MsgType) string {
	return fmt.Sprintf("%d_%v_%x_%v", viewN, dt, srAddr, msgType)
}

// pbftDataKey identifies a specific data value for quorum counting.
func pbftDataKey(viewN int64, dt corepb.PBFTMessage_DataType, data []byte) string {
	return fmt.Sprintf("%d_%v_%x", viewN, dt, data)
}

// smKey identifies an (SR, slot) pair across phase maps (no msgType).
func smKey(viewN int64, dt corepb.PBFTMessage_DataType, srAddr tcommon.Address) string {
	return fmt.Sprintf("%d_%v_%x", viewN, dt, srAddr)
}

// pbftNo identifies a consensus slot (viewN + dataType).
func pbftNo(viewN int64, dt corepb.PBFTMessage_DataType) string {
	return fmt.Sprintf("%d_%v", viewN, dt)
}

// HandlePbftMsg is the entry point for PBFT_MSG (0x34) messages.
func (h *PbftHandler) HandlePbftMsg(peer *p2p.Peer, payload []byte) {
	if !h.allowPBFT() {
		return
	}
	if h.sync != nil && h.sync.IsSyncing() {
		return
	}

	var msg corepb.PBFTMessage
	if err := proto.Unmarshal(payload, &msg); err != nil {
		return
	}
	raw := msg.GetRawData()
	if raw == nil {
		return
	}

	rawBytes, err := proto.Marshal(raw)
	if err != nil {
		return
	}

	sig := msg.GetSignature()
	if len(sig) != 65 {
		return
	}

	srAddr, err := pbftSigToAddress(rawBytes, sig)
	if err != nil {
		return
	}

	viewN := raw.GetViewN()
	dt := raw.GetDataType()
	msgType := raw.GetMsgType()

	// Expiry check.
	if dt == corepb.PBFTMessage_BLOCK {
		headNum := h.chain.CurrentBlock().Number()
		if headNum > uint64(viewN) && headNum-uint64(viewN) > pbftBlockExpiry {
			return
		}
	} else if dt == corepb.PBFTMessage_SRL {
		dp := state.LoadDynamicProperties(h.db)
		epoch := raw.GetEpoch()
		interval := dp.MaintenanceTimeInterval()
		nextMaint := dp.NextMaintenanceTime()
		if interval > 0 && nextMaint-epoch > 2*interval {
			return
		}
	}

	// SR membership check.
	if !h.isSRMember(srAddr) {
		return
	}

	// Global dedup check (includes msgType).
	dk := pbftDedupKey(viewN, dt, srAddr, msgType)
	now := time.Now()
	h.mu.Lock()
	if exp, seen := h.dedup[dk]; seen && now.Before(exp) {
		h.mu.Unlock()
		return
	}
	if len(h.dedup) < pbftDedupMaxSize {
		h.dedup[dk] = now.Add(pbftDedupTTL)
	}
	h.mu.Unlock()

	// Forward original bytes to all other peers.
	if h.server != nil {
		for _, p := range h.server.Peers() {
			if p != peer {
				p.Send(p2p.MsgPbftMsg, payload)
			}
		}
	}

	// State machine dispatch.
	switch msgType {
	case corepb.PBFTMessage_PREPREPARE:
		h.onPrePrepare(raw, rawBytes, sig, srAddr)
	case corepb.PBFTMessage_PREPARE:
		h.onPrepare(raw, rawBytes, sig, srAddr)
	case corepb.PBFTMessage_COMMIT:
		h.onCommit(raw, rawBytes, sig, srAddr)
	// VIEW_CHANGE and REQUEST are no-ops in java-tron; skip.
	}
}

func (h *PbftHandler) onPrePrepare(raw *corepb.PBFTMessage_Raw, rawBytes, sig []byte, srAddr tcommon.Address) {
	viewN := raw.GetViewN()
	dt := raw.GetDataType()
	no := pbftNo(viewN, dt)

	h.smMu.Lock()
	defer h.smMu.Unlock()

	// isSwitch not in proto; full node never receives it as true.
	if _, ok := h.preVotes[no]; ok {
		return // already seen PREPREPARE for this slot
	}
	h.preVotes[no] = struct{}{}
	h.timeOuts[no] = time.Now()
	h.checkPrepareMsgCacheNoLock(no)
}

func (h *PbftHandler) onPrepare(raw *corepb.PBFTMessage_Raw, rawBytes, sig []byte, srAddr tcommon.Address) {
	viewN := raw.GetViewN()
	dt := raw.GetDataType()
	no := pbftNo(viewN, dt)
	sk := smKey(viewN, dt, srAddr)
	dk := pbftDataKey(viewN, dt, raw.GetData())

	h.smMu.Lock()
	defer h.smMu.Unlock()

	if _, ok := h.preVotes[no]; !ok {
		// PREPREPARE hasn't arrived yet; cache this PREPARE.
		if len(h.pareMsgCache) < pbftCacheMaxSize {
			h.pareMsgCache = append(h.pareMsgCache, pbftCachedMsg{raw: raw, rawBytes: rawBytes, sig: sig, srAddr: srAddr, added: time.Now()})
		}
		return
	}
	if _, ok := h.pareVoteMap[sk]; ok {
		return // already counted this SR's PREPARE
	}
	h.pareVoteMap[sk] = struct{}{}
	h.checkCommitMsgCacheNoLock(no)

	// Full node only counts votes; does not emit COMMIT.
	if _, done := h.doneMsg[no]; !done {
		h.agreePare[dk]++
		if h.agreePare[dk] >= pbftAgreeCount {
			delete(h.agreePare, dk)
		}
	}
}

func (h *PbftHandler) onCommit(raw *corepb.PBFTMessage_Raw, rawBytes, sig []byte, srAddr tcommon.Address) {
	viewN := raw.GetViewN()
	dt := raw.GetDataType()
	no := pbftNo(viewN, dt)
	sk := smKey(viewN, dt, srAddr)
	dk := pbftDataKey(viewN, dt, raw.GetData())

	h.smMu.Lock()
	defer h.smMu.Unlock()

	if _, ok := h.pareVoteMap[sk]; !ok {
		// PREPARE from this SR hasn't arrived yet; cache this COMMIT.
		if len(h.commitMsgCache) < pbftCacheMaxSize {
			h.commitMsgCache = append(h.commitMsgCache, pbftCachedMsg{raw: raw, rawBytes: rawBytes, sig: sig, srAddr: srAddr, added: time.Now()})
		}
		return
	}
	if _, ok := h.commitVoteMap[sk]; ok {
		return // already counted this SR's COMMIT
	}
	h.commitVoteMap[sk] = struct{}{}

	h.agreeCommit[dk]++
	h.dataSignCache[dk] = append(h.dataSignCache[dk], sig)

	if h.agreeCommit[dk] >= pbftAgreeCount {
		sigs := make([][]byte, len(h.dataSignCache[dk]))
		copy(sigs, h.dataSignCache[dk])
		h.removeNoLock(no)
		h.writeQuorumResult(raw, rawBytes, sigs)
	}
}

func (h *PbftHandler) checkPrepareMsgCacheNoLock(no string) {
	remaining := h.pareMsgCache[:0]
	for _, m := range h.pareMsgCache {
		if strings.HasPrefix(smKey(m.raw.GetViewN(), m.raw.GetDataType(), m.srAddr), no) {
			go h.onPrepare(m.raw, m.rawBytes, m.sig, m.srAddr) // unlock before re-entry
		} else {
			remaining = append(remaining, m)
		}
	}
	h.pareMsgCache = remaining
}

func (h *PbftHandler) checkCommitMsgCacheNoLock(no string) {
	remaining := h.commitMsgCache[:0]
	for _, m := range h.commitMsgCache {
		if strings.HasPrefix(smKey(m.raw.GetViewN(), m.raw.GetDataType(), m.srAddr), no) {
			go h.onCommit(m.raw, m.rawBytes, m.sig, m.srAddr)
		} else {
			remaining = append(remaining, m)
		}
	}
	h.commitMsgCache = remaining
}

// removeNoLock cleans all state for the given slot (caller holds smMu).
func (h *PbftHandler) removeNoLock(no string) {
	pre := no + "_"
	delete(h.preVotes, no)
	delete(h.timeOuts, no)
	delete(h.doneMsg, no)

	for k := range h.pareVoteMap {
		if strings.HasPrefix(k, pre) {
			delete(h.pareVoteMap, k)
		}
	}
	for k := range h.commitVoteMap {
		if strings.HasPrefix(k, pre) {
			delete(h.commitVoteMap, k)
		}
	}
	for k := range h.agreePare {
		if strings.HasPrefix(k, pre) {
			delete(h.agreePare, k)
		}
	}
	for k := range h.agreeCommit {
		if strings.HasPrefix(k, pre) {
			delete(h.agreeCommit, k)
		}
	}
	for k := range h.dataSignCache {
		if strings.HasPrefix(k, pre) {
			delete(h.dataSignCache, k)
		}
	}
}

func (h *PbftHandler) writeQuorumResult(raw *corepb.PBFTMessage_Raw, rawBytes []byte, sigs [][]byte) {
	result := &corepb.PBFTCommitResult{
		Data:      rawBytes,
		Signature: sigs,
	}
	viewN := raw.GetViewN()
	switch raw.GetDataType() {
	case corepb.PBFTMessage_BLOCK:
		rawdb.WriteBlockSignData(h.db, viewN, result)
		rawdb.WriteLatestPbftBlockNum(h.db, viewN)
	case corepb.PBFTMessage_SRL:
		rawdb.WriteSrSignData(h.db, raw.GetEpoch(), result)
	}
}
