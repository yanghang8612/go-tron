package net

import (
	"crypto/ecdsa"
	"sync"
	"testing"
	"time"

	ethrawdb "github.com/ethereum/go-ethereum/core/rawdb"
	ethcrypto "github.com/ethereum/go-ethereum/crypto"
	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/state"
	"github.com/tronprotocol/go-tron/core/types"
	"github.com/tronprotocol/go-tron/crypto"
	"github.com/tronprotocol/go-tron/params"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	"google.golang.org/protobuf/proto"
)

// makePbftRig wires a PbftHandler + PbftProducer for slice-2 testing. The
// producer's outbound broadcast is captured into a slice (no real network).
type pbftRig struct {
	h        *PbftHandler
	p        *PbftProducer
	bc       *core.BlockChain
	srKeys   []*ecdsa.PrivateKey
	srAddrs  []tcommon.Address
	localKey *ecdsa.PrivateKey

	mu        sync.Mutex
	captured  [][]byte
}

func (r *pbftRig) outboundCount() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.captured)
}

func (r *pbftRig) lastOutbound() []byte {
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.captured) == 0 {
		return nil
	}
	return r.captured[len(r.captured)-1]
}

// outboundFiltered returns captured payloads with the given MsgType.
func (r *pbftRig) outboundFiltered(mt corepb.PBFTMessage_MsgType) [][]byte {
	r.mu.Lock()
	defer r.mu.Unlock()
	var out [][]byte
	for _, payload := range r.captured {
		var msg corepb.PBFTMessage
		if err := proto.Unmarshal(payload, &msg); err != nil {
			continue
		}
		if msg.GetRawData().GetMsgType() == mt {
			out = append(out, payload)
		}
	}
	return out
}

// newPbftRig builds the handler/producer with `numSRs` registered SR
// addresses; the local SR keys are the first `localKeyCount` of those.
// The producer holds those local keys.
func newPbftRig(t *testing.T, numSRs, localKeyCount int) *pbftRig {
	t.Helper()
	if localKeyCount > numSRs {
		t.Fatalf("localKeyCount %d > numSRs %d", localKeyCount, numSRs)
	}
	diskdb := ethrawdb.NewMemoryDatabase()
	sdb := state.NewDatabase(diskdb)
	genesis := &params.Genesis{Config: params.MainnetChainConfig, Timestamp: 0}
	core.SetupGenesisBlock(diskdb, genesis)
	bc, err := core.NewBlockChain(diskdb, sdb, params.MainnetChainConfig)
	if err != nil {
		t.Fatal(err)
	}
	dp := state.LoadDynamicProperties(diskdb)
	dp.Set("allow_pbft", 1)
	dp.Flush(diskdb)

	keys := make([]*ecdsa.PrivateKey, numSRs)
	addrs := make([]tcommon.Address, numSRs)
	for i := range keys {
		k, err := ethcrypto.GenerateKey()
		if err != nil {
			t.Fatal(err)
		}
		keys[i] = k
		addrs[i] = crypto.PubkeyToAddress(&k.PublicKey)
	}
	rawdb.WriteShuffledWitnesses(bc.DB(), addrs)

	h := NewPbftHandler(bc, bc.DB(), nil, nil)
	h.Start() //nolint:errcheck
	t.Cleanup(func() { h.Stop() }) //nolint:errcheck

	localKeys := keys[:localKeyCount]
	p := NewPbftProducer(bc, bc.DB(), nil, nil, localKeys...)
	if p == nil && localKeyCount > 0 {
		t.Fatal("NewPbftProducer returned nil")
	}

	rig := &pbftRig{
		h:       h,
		p:       p,
		bc:      bc,
		srKeys:  keys,
		srAddrs: addrs,
	}
	if localKeyCount > 0 {
		rig.localKey = localKeys[0]
		p.SetBroadcastFunc(func(payload []byte) {
			rig.mu.Lock()
			rig.captured = append(rig.captured, append([]byte(nil), payload...))
			rig.mu.Unlock()
		})
		p.SetHandler(h)
		h.SetProducer(p)
	}
	return rig
}

// TestPbft_PreparePhaseQuorum verifies that 18 PREPARE sigs do NOT trigger
// COMMIT emit, but the 19th does. Mirrors java-tron PbftMessageHandle.onPrepare
// quorum check at line 170.
func TestPbft_PreparePhaseQuorum(t *testing.T) {
	rig := newPbftRig(t, 27, 1)
	viewN := int64(11)
	data := []byte("blockid_prepare_quorum")

	// Send the local SR's PREPREPARE first so preVotes is set. This also
	// triggers the producer to emit a PREPARE for the local SR.
	pp := makePbftPayloadWithData(t, rig.srKeys[0], viewN, corepb.PBFTMessage_PREPREPARE, corepb.PBFTMessage_BLOCK, data)
	rig.h.HandlePbftMsg(nil, pp)

	// Producer should have emitted exactly one PREPARE (one local key).
	time.Sleep(20 * time.Millisecond)
	if got := len(rig.outboundFiltered(corepb.PBFTMessage_PREPARE)); got != 1 {
		t.Fatalf("after PREPREPARE: outbound PREPARE count = %d, want 1", got)
	}
	if got := len(rig.outboundFiltered(corepb.PBFTMessage_COMMIT)); got != 0 {
		t.Fatalf("after PREPREPARE: outbound COMMIT count = %d, want 0", got)
	}

	// Self-emitted PREPARE re-enters via HandleSelfPbftMsg → onPrepare,
	// counting as 1 toward quorum. Feed 17 more PREPAREs from other SRs (2..18)
	// to reach 18 total.
	for i := 1; i < 18; i++ {
		p := makePbftPayloadWithData(t, rig.srKeys[i], viewN, corepb.PBFTMessage_PREPARE, corepb.PBFTMessage_BLOCK, data)
		rig.h.HandlePbftMsg(nil, p)
	}
	time.Sleep(50 * time.Millisecond)

	// 18 PREPAREs → still no COMMIT.
	if got := len(rig.outboundFiltered(corepb.PBFTMessage_COMMIT)); got != 0 {
		t.Fatalf("at 18 PREPAREs: outbound COMMIT = %d, want 0", got)
	}

	// 19th PREPARE → quorum hit → COMMIT emit.
	p19 := makePbftPayloadWithData(t, rig.srKeys[18], viewN, corepb.PBFTMessage_PREPARE, corepb.PBFTMessage_BLOCK, data)
	rig.h.HandlePbftMsg(nil, p19)
	time.Sleep(80 * time.Millisecond)

	if got := len(rig.outboundFiltered(corepb.PBFTMessage_COMMIT)); got != 1 {
		t.Fatalf("at 19 PREPAREs: outbound COMMIT = %d, want 1", got)
	}
}

// TestPbft_CommitPhaseQuorum feeds 19 COMMITs and asserts PbftSignData +
// LatestPbftBlockNum are written.
func TestPbft_CommitPhaseQuorum(t *testing.T) {
	rig := newPbftRig(t, 27, 1)
	viewN := int64(22)
	data := []byte("blockid_commit_quorum")

	// Send PREPREPARE
	rig.h.HandlePbftMsg(nil, makePbftPayloadWithData(t, rig.srKeys[0], viewN, corepb.PBFTMessage_PREPREPARE, corepb.PBFTMessage_BLOCK, data))

	// Send 19 PREPAREs from SRs 0..18 (the producer also emits 1 for SR[0]
	// which is dropped by the SM map dedup).
	for i := 0; i < 19; i++ {
		rig.h.HandlePbftMsg(nil, makePbftPayloadWithData(t, rig.srKeys[i], viewN, corepb.PBFTMessage_PREPARE, corepb.PBFTMessage_BLOCK, data))
	}
	time.Sleep(50 * time.Millisecond)

	// At this point the SM has hit quorum on PREPARE and the producer should
	// have emitted COMMIT for SR[0]. Feed 17 more inbound COMMITs from
	// SR[1..17] for a total of 18 distinct COMMITs (self SR[0] + 17 peers).
	// (Note: an inbound COMMIT from SR[0] would be deduped against the
	// self-emitted one, so we skip i==0 here.)
	for i := 1; i < 18; i++ {
		rig.h.HandlePbftMsg(nil, makePbftPayloadWithData(t, rig.srKeys[i], viewN, corepb.PBFTMessage_COMMIT, corepb.PBFTMessage_BLOCK, data))
	}
	time.Sleep(40 * time.Millisecond)

	// At 18 distinct COMMITs, no quorum write yet.
	if r := rawdb.ReadBlockSignData(rig.bc.DB(), viewN); r != nil {
		t.Fatal("at 18 COMMITs, expected no PbftSignData written yet")
	}
	if got := rawdb.ReadLatestPbftBlockNum(rig.bc.DB()); got == viewN {
		t.Fatal("at 18 COMMITs, LatestPbftBlockNum should NOT yet equal viewN")
	}

	// 19th COMMIT → quorum write.
	rig.h.HandlePbftMsg(nil, makePbftPayloadWithData(t, rig.srKeys[18], viewN, corepb.PBFTMessage_COMMIT, corepb.PBFTMessage_BLOCK, data))
	time.Sleep(80 * time.Millisecond)

	if r := rawdb.ReadBlockSignData(rig.bc.DB(), viewN); r == nil {
		t.Fatal("expected ReadBlockSignData to return a result after 19 COMMITs")
	}
	if got := rawdb.ReadLatestPbftBlockNum(rig.bc.DB()); got != viewN {
		t.Errorf("LatestPbftBlockNum = %d, want %d", got, viewN)
	}
}

// TestPbft_DropDuplicateSigner — same SR signs PREPARE twice; second is
// silently dropped (no double-count).
func TestPbft_DropDuplicateSigner(t *testing.T) {
	rig := newPbftRig(t, 27, 0) // no local SR — we only test counting
	viewN := int64(33)
	data := []byte("blockid_dup")

	rig.h.HandlePbftMsg(nil, makePbftPayloadWithData(t, rig.srKeys[0], viewN, corepb.PBFTMessage_PREPREPARE, corepb.PBFTMessage_BLOCK, data))

	// First PREPARE from SR[1].
	p1 := makePbftPayloadWithData(t, rig.srKeys[1], viewN, corepb.PBFTMessage_PREPARE, corepb.PBFTMessage_BLOCK, data)
	rig.h.HandlePbftMsg(nil, p1)

	// Second PREPARE from same SR (dedup must silence it).
	rig.h.HandlePbftMsg(nil, p1)

	// Inspect map cardinality.
	rig.h.smMu.Lock()
	pareCount := len(rig.h.pareVoteMap)
	rig.h.smMu.Unlock()
	if pareCount != 1 {
		t.Errorf("pareVoteMap size after duplicate = %d, want 1", pareCount)
	}
}

// TestPbft_DropNonSrSigner — non-SR signs; dedup map filter drops it before
// the SM transitions.
func TestPbft_DropNonSrSigner(t *testing.T) {
	rig := newPbftRig(t, 27, 0)
	viewN := int64(44)
	data := []byte("blockid_nonsr")

	// Generate a key NOT in the SR list.
	intruder, err := ethcrypto.GenerateKey()
	if err != nil {
		t.Fatal(err)
	}
	pp := makePbftPayloadWithData(t, intruder, viewN, corepb.PBFTMessage_PREPREPARE, corepb.PBFTMessage_BLOCK, data)
	rig.h.HandlePbftMsg(nil, pp)

	// Non-SR PREPREPARE was rejected — preVotes must be empty.
	no := pbftNo(viewN, corepb.PBFTMessage_BLOCK)
	rig.h.smMu.Lock()
	_, in := rig.h.preVotes[no]
	rig.h.smMu.Unlock()
	if in {
		t.Error("non-SR PREPREPARE was accepted (should be dropped)")
	}
}

// TestPbft_SrlTriggerAtMaintenance — calling OnMaintenance broadcasts an SRL
// PREPREPARE carrying the new witness set.
func TestPbft_SrlTriggerAtMaintenance(t *testing.T) {
	rig := newPbftRig(t, 27, 1)

	// Build a synthetic block (the real chain isn't advanced; OnMaintenance
	// only uses the block number for logging context, the witness set
	// is what goes into the SRL data field).
	block := types.NewBlockFromPB(&corepb.Block{
		BlockHeader: &corepb.BlockHeader{
			RawData: &corepb.BlockHeaderRaw{Number: 100, Timestamp: 99999},
		},
	})
	// New witnesses post-rotation — pretend SR[20..26] are dropped.
	newSet := rig.srAddrs[:20]
	rig.p.OnMaintenance(block, newSet)
	time.Sleep(20 * time.Millisecond)

	srl := rig.outboundFiltered(corepb.PBFTMessage_PREPREPARE)
	if len(srl) != 1 {
		t.Fatalf("expected 1 SRL PREPREPARE, got %d", len(srl))
	}
	var msg corepb.PBFTMessage
	if err := proto.Unmarshal(srl[0], &msg); err != nil {
		t.Fatalf("unmarshal SRL payload: %v", err)
	}
	if dt := msg.GetRawData().GetDataType(); dt != corepb.PBFTMessage_SRL {
		t.Errorf("DataType = %v, want SRL", dt)
	}
	// Decode the inner SRL list and verify cardinality.
	var inner corepb.SRL
	if err := proto.Unmarshal(msg.GetRawData().GetData(), &inner); err != nil {
		t.Fatalf("unmarshal inner SRL: %v", err)
	}
	if got := len(inner.GetSrAddress()); got != 20 {
		t.Errorf("SRL.SrAddress length = %d, want 20", got)
	}
}

// TestPbft_MultiSrKeys — three local keys; OnBlockApplied must produce three
// PREPREPARE payloads (one per local SR).
func TestPbft_MultiSrKeys(t *testing.T) {
	rig := newPbftRig(t, 27, 3)
	block := types.NewBlockFromPB(&corepb.Block{
		BlockHeader: &corepb.BlockHeader{
			RawData: &corepb.BlockHeaderRaw{Number: 50, Timestamp: 12345},
		},
	})
	rig.p.OnBlockApplied(block)
	time.Sleep(40 * time.Millisecond)

	pp := rig.outboundFiltered(corepb.PBFTMessage_PREPREPARE)
	if len(pp) != 3 {
		t.Fatalf("expected 3 PREPREPARE outbound, got %d", len(pp))
	}

	// Each payload must be signed by a distinct local SR address.
	seen := make(map[tcommon.Address]int)
	for _, payload := range pp {
		var msg corepb.PBFTMessage
		if err := proto.Unmarshal(payload, &msg); err != nil {
			t.Fatalf("unmarshal: %v", err)
		}
		rawBytes, _ := proto.Marshal(msg.GetRawData())
		addr, err := pbftSigToAddress(rawBytes, msg.GetSignature())
		if err != nil {
			t.Fatalf("recover signer: %v", err)
		}
		seen[addr]++
	}
	if len(seen) != 3 {
		t.Errorf("distinct signer count = %d, want 3 (got %v)", len(seen), seen)
	}
}

// TestPbft_LocalSRKeysFiltering verifies localSRKeys returns only keys whose
// addresses are in the shuffled witness list. Keys not in the SR list are
// excluded so a misconfigured node doesn't broadcast junk.
func TestPbft_LocalSRKeysFiltering(t *testing.T) {
	rig := newPbftRig(t, 5, 5) // all 5 local keys are SRs

	keys, addrs := rig.p.localSRKeys()
	if len(keys) != 5 {
		t.Errorf("with all 5 keys in SR list: got %d, want 5", len(keys))
	}
	if len(addrs) != 5 {
		t.Errorf("addrs length = %d, want 5", len(addrs))
	}

	// Shrink shuffled witness set — only first 2 of our keys remain SRs.
	rawdb.WriteShuffledWitnesses(rig.bc.DB(), rig.srAddrs[:2])
	rawdb.WritePreviousShuffledWitnesses(rig.bc.DB(), nil)

	keys2, _ := rig.p.localSRKeys()
	if len(keys2) != 2 {
		t.Errorf("after shrink: got %d local SRs, want 2", len(keys2))
	}
}
