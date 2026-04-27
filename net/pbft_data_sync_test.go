package net

import (
	"crypto/ecdsa"
	"crypto/sha256"
	"testing"

	ethcrypto "github.com/ethereum/go-ethereum/crypto"
	ethrawdb "github.com/ethereum/go-ethereum/core/rawdb"
	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/state"
	"github.com/tronprotocol/go-tron/crypto"
	"github.com/tronprotocol/go-tron/params"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	"github.com/tronprotocol/go-tron/core/types"
	"google.golang.org/protobuf/proto"
)

func makeDataSyncHandler(t *testing.T) (*PbftDataSyncHandler, *core.BlockChain) {
	t.Helper()
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

	h := NewPbftDataSyncHandler(bc, bc.DB())
	return h, bc
}

// buildCommitResult creates a PBFTCommitResult for blockNum signed by the given keys.
func buildCommitResult(t *testing.T, keys []*ecdsa.PrivateKey, viewN int64, dt corepb.PBFTMessage_DataType) ([]byte, *corepb.PBFTCommitResult) {
	t.Helper()
	raw := &corepb.PBFTMessage_Raw{
		MsgType:  corepb.PBFTMessage_COMMIT,
		DataType: dt,
		ViewN:    viewN,
		Epoch:    0,
		Data:     []byte("blockid"),
	}
	rawBytes, err := proto.Marshal(raw)
	if err != nil {
		t.Fatal(err)
	}
	hashArr := sha256.Sum256(rawBytes)
	sigs := make([][]byte, len(keys))
	for i, k := range keys {
		sig, err := crypto.Sign(hashArr[:], k)
		if err != nil {
			t.Fatal(err)
		}
		sigs[i] = sig
	}
	result := &corepb.PBFTCommitResult{Data: rawBytes, Signature: sigs}
	payload, err := proto.Marshal(result)
	if err != nil {
		t.Fatal(err)
	}
	return payload, result
}

func registerSRs(t *testing.T, db interface{ DB() interface{ Put([]byte, []byte) error } }, keys []*ecdsa.PrivateKey) []tcommon.Address {
	addrs := make([]tcommon.Address, len(keys))
	for i, k := range keys {
		addrs[i] = crypto.PubkeyToAddress(&k.PublicKey)
	}
	return addrs
}

func genesisBlock(bc *core.BlockChain) *types.Block {
	return bc.GetBlockByNumber(0)
}

func TestPbftDataSyncValid(t *testing.T) {
	h, bc := makeDataSyncHandler(t)

	keys := make([]*ecdsa.PrivateKey, 19)
	addrs := make([]tcommon.Address, 19)
	for i := range keys {
		k, _ := ethcrypto.GenerateKey()
		keys[i] = k
		addrs[i] = crypto.PubkeyToAddress(&k.PublicKey)
	}
	rawdb.WriteShuffledWitnesses(bc.DB(), addrs)

	viewN := int64(0) // genesis block number
	payload, _ := buildCommitResult(t, keys, viewN, corepb.PBFTMessage_BLOCK)

	h.HandleCommitMsg(nil, payload)

	// Trigger on genesis block insert
	h.ProcessOnBlock(genesisBlock(bc))

	result := rawdb.ReadBlockSignData(bc.DB(), viewN)
	if result == nil {
		t.Fatal("expected WriteBlockSignData after valid 19-sig commit result, got nil")
	}
}

func TestPbftDataSyncInvalidSig(t *testing.T) {
	h, bc := makeDataSyncHandler(t)

	keys := make([]*ecdsa.PrivateKey, 19)
	addrs := make([]tcommon.Address, 19)
	for i := range keys {
		k, _ := ethcrypto.GenerateKey()
		keys[i] = k
		addrs[i] = crypto.PubkeyToAddress(&k.PublicKey)
	}
	rawdb.WriteShuffledWitnesses(bc.DB(), addrs)

	viewN := int64(0)
	payload, result := buildCommitResult(t, keys, viewN, corepb.PBFTMessage_BLOCK)
	_ = result

	// Tamper the last signature — replace byte 0 with 0xFF
	var cr corepb.PBFTCommitResult
	proto.Unmarshal(payload, &cr)
	if len(cr.Signature) > 0 {
		cr.Signature[len(cr.Signature)-1][0] ^= 0xFF
	}
	tampered, _ := proto.Marshal(&cr)

	h.HandleCommitMsg(nil, tampered)
	h.ProcessOnBlock(genesisBlock(bc))

	r := rawdb.ReadBlockSignData(bc.DB(), viewN)
	if r != nil {
		t.Error("expected no rawdb write for tampered signature")
	}
}

func TestPbftDataSyncInsufficientSigs(t *testing.T) {
	h, bc := makeDataSyncHandler(t)

	keys := make([]*ecdsa.PrivateKey, 18) // only 18
	addrs := make([]tcommon.Address, 18)
	for i := range keys {
		k, _ := ethcrypto.GenerateKey()
		keys[i] = k
		addrs[i] = crypto.PubkeyToAddress(&k.PublicKey)
	}
	rawdb.WriteShuffledWitnesses(bc.DB(), addrs)

	viewN := int64(0)
	payload, _ := buildCommitResult(t, keys, viewN, corepb.PBFTMessage_BLOCK)

	h.HandleCommitMsg(nil, payload)
	h.ProcessOnBlock(genesisBlock(bc))

	r := rawdb.ReadBlockSignData(bc.DB(), viewN)
	if r != nil {
		t.Error("expected no rawdb write for insufficient sigs (18 < 19)")
	}
}
