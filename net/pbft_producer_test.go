package net

import (
	"crypto/ecdsa"
	"crypto/sha256"
	"testing"

	ethcrypto "github.com/ethereum/go-ethereum/crypto"
	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/types"
	"github.com/tronprotocol/go-tron/crypto"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	"google.golang.org/protobuf/proto"
)

// makeProducerForTest constructs a PbftProducer with a fresh key. chain/db are
// nil because slice-1 builders don't touch them; the no-op hook test stubs
// them out separately.
func makeProducerForTest(t *testing.T) (*PbftProducer, *ecdsa.PrivateKey, tcommon.Address) {
	t.Helper()
	key, err := ethcrypto.GenerateKey()
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}
	p := &PbftProducer{
		srKey:  key,
		srAddr: crypto.PubkeyToAddress(&key.PublicKey),
	}
	return p, key, p.srAddr
}

func makeBlockWithNumber(num uint64) *types.Block {
	return types.NewBlockFromPB(&corepb.Block{
		BlockHeader: &corepb.BlockHeader{
			RawData: &corepb.BlockHeaderRaw{Number: int64(num), Timestamp: 12345},
		},
	})
}

// decodePbftPayload mirrors what net/pbft_handler.go does on receive: parse
// the wire bytes back into a PBFTMessage, re-marshal the inner Raw to recover
// the bytes that were signed, and recover the SR address from the signature.
func decodePbftPayload(t *testing.T, payload []byte) (*corepb.PBFTMessage_Raw, []byte, []byte, tcommon.Address) {
	t.Helper()
	var msg corepb.PBFTMessage
	if err := proto.Unmarshal(payload, &msg); err != nil {
		t.Fatalf("unmarshal payload: %v", err)
	}
	raw := msg.GetRawData()
	if raw == nil {
		t.Fatal("decoded payload has nil RawData")
	}
	rawBytes, err := proto.Marshal(raw)
	if err != nil {
		t.Fatalf("re-marshal raw: %v", err)
	}
	sig := msg.GetSignature()
	if len(sig) != 65 {
		t.Fatalf("signature length = %d, want 65", len(sig))
	}
	addr, err := pbftSigToAddress(rawBytes, sig)
	if err != nil {
		t.Fatalf("pbftSigToAddress: %v", err)
	}
	return raw, rawBytes, sig, addr
}

func TestBuildBlockPrePrepareMsg_RoundTrip(t *testing.T) {
	p, _, srAddr := makeProducerForTest(t)
	block := makeBlockWithNumber(42)

	payload, err := p.BuildBlockPrePrepareMsg(block, 100)
	if err != nil {
		t.Fatalf("BuildBlockPrePrepareMsg: %v", err)
	}

	raw, _, _, recovered := decodePbftPayload(t, payload)

	if raw.GetMsgType() != corepb.PBFTMessage_PREPREPARE {
		t.Errorf("MsgType = %v, want PREPREPARE", raw.GetMsgType())
	}
	if raw.GetDataType() != corepb.PBFTMessage_BLOCK {
		t.Errorf("DataType = %v, want BLOCK", raw.GetDataType())
	}
	if raw.GetViewN() != 42 {
		t.Errorf("ViewN = %d, want 42", raw.GetViewN())
	}
	if raw.GetEpoch() != 100 {
		t.Errorf("Epoch = %d, want 100", raw.GetEpoch())
	}
	id := block.ID()
	if string(raw.GetData()) != string(id.Hash[:]) {
		t.Errorf("Data = %x, want block.ID().Hash %x", raw.GetData(), id.Hash[:])
	}
	if recovered != srAddr {
		t.Errorf("recovered SR addr = %x, want %x", recovered, srAddr)
	}
}

func TestBuildPrepareMsg_DerivesFromParent(t *testing.T) {
	p, _, srAddr := makeProducerForTest(t)
	block := makeBlockWithNumber(7)

	parentPayload, err := p.BuildBlockPrePrepareMsg(block, 200)
	if err != nil {
		t.Fatalf("BuildBlockPrePrepareMsg: %v", err)
	}
	parentRaw, _, _, _ := decodePbftPayload(t, parentPayload)

	preparePayload, err := p.BuildPrepareMsg(parentRaw)
	if err != nil {
		t.Fatalf("BuildPrepareMsg: %v", err)
	}

	raw, _, _, recovered := decodePbftPayload(t, preparePayload)

	if raw.GetMsgType() != corepb.PBFTMessage_PREPARE {
		t.Errorf("MsgType = %v, want PREPARE", raw.GetMsgType())
	}
	if raw.GetDataType() != parentRaw.GetDataType() {
		t.Errorf("DataType = %v, want %v", raw.GetDataType(), parentRaw.GetDataType())
	}
	if raw.GetViewN() != parentRaw.GetViewN() {
		t.Errorf("ViewN = %d, want %d", raw.GetViewN(), parentRaw.GetViewN())
	}
	if raw.GetEpoch() != parentRaw.GetEpoch() {
		t.Errorf("Epoch = %d, want %d", raw.GetEpoch(), parentRaw.GetEpoch())
	}
	if string(raw.GetData()) != string(parentRaw.GetData()) {
		t.Errorf("Data = %x, want %x", raw.GetData(), parentRaw.GetData())
	}
	if recovered != srAddr {
		t.Errorf("recovered SR addr = %x, want %x", recovered, srAddr)
	}
}

func TestBuildCommitMsg_DerivesFromParent(t *testing.T) {
	p, _, srAddr := makeProducerForTest(t)
	block := makeBlockWithNumber(13)

	parentPayload, err := p.BuildBlockPrePrepareMsg(block, 300)
	if err != nil {
		t.Fatalf("BuildBlockPrePrepareMsg: %v", err)
	}
	parentRaw, _, _, _ := decodePbftPayload(t, parentPayload)

	commitPayload, err := p.BuildCommitMsg(parentRaw)
	if err != nil {
		t.Fatalf("BuildCommitMsg: %v", err)
	}

	raw, _, _, recovered := decodePbftPayload(t, commitPayload)

	if raw.GetMsgType() != corepb.PBFTMessage_COMMIT {
		t.Errorf("MsgType = %v, want COMMIT", raw.GetMsgType())
	}
	if raw.GetViewN() != parentRaw.GetViewN() ||
		raw.GetEpoch() != parentRaw.GetEpoch() ||
		raw.GetDataType() != parentRaw.GetDataType() ||
		string(raw.GetData()) != string(parentRaw.GetData()) {
		t.Errorf("inherited fields mismatch: %+v vs parent %+v", raw, parentRaw)
	}
	if recovered != srAddr {
		t.Errorf("recovered SR addr = %x, want %x", recovered, srAddr)
	}
}

func TestBuildPrepareMsg_DifferentSignatureFromPrePrepare(t *testing.T) {
	p, _, _ := makeProducerForTest(t)
	block := makeBlockWithNumber(99)

	parentPayload, err := p.BuildBlockPrePrepareMsg(block, 0)
	if err != nil {
		t.Fatalf("BuildBlockPrePrepareMsg: %v", err)
	}
	parentRaw, _, parentSig, _ := decodePbftPayload(t, parentPayload)

	preparePayload, err := p.BuildPrepareMsg(parentRaw)
	if err != nil {
		t.Fatalf("BuildPrepareMsg: %v", err)
	}
	_, _, prepareSig, _ := decodePbftPayload(t, preparePayload)

	if string(prepareSig) == string(parentSig) {
		t.Error("PREPARE signature equals PREPREPARE signature; flipping msg_type must change the signed digest")
	}
}

func TestBuildBlockPrePrepareMsg_NilBlock(t *testing.T) {
	p, _, _ := makeProducerForTest(t)
	if _, err := p.BuildBlockPrePrepareMsg(nil, 0); err == nil {
		t.Error("expected error for nil block")
	}
}

func TestBuildPrepareMsg_NilParent(t *testing.T) {
	p, _, _ := makeProducerForTest(t)
	if _, err := p.BuildPrepareMsg(nil); err == nil {
		t.Error("expected error for nil parent raw")
	}
}

// TestNewPbftProducer_NilKey verifies the constructor refuses a nil key.
func TestNewPbftProducer_NilKey(t *testing.T) {
	if got := NewPbftProducer(nil, nil, nil, nil, nil); got != nil {
		t.Errorf("NewPbftProducer(nil key) = %v, want nil", got)
	}
}

// TestOnBlockApplied_NoOp_DoesNotPanic confirms that the slice-1 hook is safe
// to register: it must not panic on a synthetic block, and it must do nothing
// (no DB writes, no peer sends — verified by the absence of any wired server
// or db on the producer).
func TestOnBlockApplied_NoOp_DoesNotPanic(t *testing.T) {
	p, _, _ := makeProducerForTest(t)
	// chain/db are nil → allowPBFT() short-circuits to false; hook is a no-op.
	block := makeBlockWithNumber(1)

	// Must not panic.
	p.OnBlockApplied(block)

	// Nil block is also tolerated.
	p.OnBlockApplied(nil)

	// Nil receiver is tolerated.
	var pp *PbftProducer
	pp.OnBlockApplied(block)
}

// TestSignPbftRaw_ProducesRecoverableSig is a direct unit test of the internal
// signing helper, independent of the public builders.
func TestSignPbftRaw_ProducesRecoverableSig(t *testing.T) {
	key, err := ethcrypto.GenerateKey()
	if err != nil {
		t.Fatal(err)
	}
	raw := &corepb.PBFTMessage_Raw{
		MsgType:  corepb.PBFTMessage_PREPREPARE,
		DataType: corepb.PBFTMessage_BLOCK,
		ViewN:    1,
		Epoch:    2,
		Data:     []byte{0xAB, 0xCD},
	}
	payload, err := signPbftRaw(raw, key)
	if err != nil {
		t.Fatalf("signPbftRaw: %v", err)
	}

	var msg corepb.PBFTMessage
	if err := proto.Unmarshal(payload, &msg); err != nil {
		t.Fatal(err)
	}

	rawBytes, _ := proto.Marshal(msg.GetRawData())
	hash := sha256.Sum256(rawBytes)

	pub, err := crypto.SigToPub(hash[:], msg.GetSignature())
	if err != nil {
		t.Fatalf("SigToPub: %v", err)
	}
	if got := crypto.PubkeyToAddress(pub); got != crypto.PubkeyToAddress(&key.PublicKey) {
		t.Errorf("recovered addr = %x, want %x", got, crypto.PubkeyToAddress(&key.PublicKey))
	}
}
