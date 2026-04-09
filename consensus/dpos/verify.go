package dpos

import (
	"crypto/sha256"
	"errors"

	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/consensus"
	"github.com/tronprotocol/go-tron/core/types"
	"github.com/tronprotocol/go-tron/crypto"
	"github.com/tronprotocol/go-tron/params"
	"google.golang.org/protobuf/proto"
)

var (
	ErrInvalidBlockNumber = errors.New("invalid block number")
	ErrInvalidParentHash  = errors.New("parent hash mismatch")
	ErrInvalidTimestamp   = errors.New("invalid timestamp")
	ErrInvalidWitness     = errors.New("not the scheduled witness")
	ErrInvalidSignature   = errors.New("invalid block signature")
)

func VerifyHeader(chain consensus.ChainReader, block *types.Block) error {
	parent := chain.CurrentBlock()
	if parent == nil {
		return errors.New("parent block not found")
	}
	if block.Number() != parent.Number()+1 {
		return ErrInvalidBlockNumber
	}
	if block.ParentHash() != parent.Hash() {
		return ErrInvalidParentHash
	}
	if block.Timestamp() <= parent.Timestamp() {
		return ErrInvalidTimestamp
	}
	genesisTime := chain.GenesisTimestamp()
	if (block.Timestamp()-genesisTime)%int64(params.BlockProducedInterval) != 0 {
		return ErrInvalidTimestamp
	}

	witness, err := recoverWitness(block)
	if err != nil {
		return ErrInvalidSignature
	}

	slot := AbsoluteSlot(block.Timestamp(), genesisTime)
	witnesses := chain.ActiveWitnesses()
	idx := WitnessIndex(slot, len(witnesses))
	if idx >= len(witnesses) {
		return ErrInvalidWitness
	}
	if witnesses[idx] != witness {
		return ErrInvalidWitness
	}
	return nil
}

func recoverWitness(block *types.Block) (common.Address, error) {
	sig := block.WitnessSignature()
	if len(sig) != 65 {
		return common.Address{}, ErrInvalidSignature
	}
	headerRaw := block.Proto().BlockHeader.RawData
	data, err := proto.Marshal(headerRaw)
	if err != nil {
		return common.Address{}, err
	}
	hash := sha256.Sum256(data)

	pub, err := crypto.SigToPub(hash[:], sig)
	if err != nil {
		return common.Address{}, err
	}
	return crypto.PubkeyToAddress(pub), nil
}
