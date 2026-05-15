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
	dp := chain.DynProps()
	// mod-3000 alignment + slot==0 rejection were unconditional in early
	// gtron but java-tron gates both on proposal #88 (`DposService.java:120,
	// 134`). Pre-#88, java accepts misaligned timestamps and slot-0 blocks;
	// gtron must do the same for replay parity. In practice real producers
	// only mint aligned slots, so the loosening is theoretical.
	if dp.ConsensusLogicOptimization() {
		if block.Timestamp()%int64(params.BlockProducedInterval) != 0 {
			return ErrInvalidTimestamp
		}
		isMaintenance := dp.StateFlag() == 1
		if SlotForTime(block.Timestamp(), parent.Timestamp(), genesisTime, isMaintenance, int64(params.MaintenanceSkipSlots)) == 0 {
			return ErrInvalidTimestamp
		}
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
