package core

import (
	"bytes"
	"errors"

	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/types"
)

// ErrTaposBadLength is returned when ref_block_bytes / ref_block_hash on a
// tx don't have their canonical lengths (2 and 8 bytes respectively).
var ErrTaposBadLength = errors.New("tx ref_block_bytes/ref_block_hash length mismatch")

// ErrTaposUnknownRef is returned when the slot derived from
// ref_block_bytes is empty in the recent-block ring — either a malformed
// tx or one referencing a block older than the 65535-block window.
// java-tron throws TaposException("No reference block found") here.
var ErrTaposUnknownRef = errors.New("tx references unknown recent block")

// ErrTaposHashMismatch fires when the recorded hash tail for the slot
// doesn't equal the tx's ref_block_hash. Mirrors java-tron's TaposException
// with "different block hash" — a fork-replay or stale-tx signal.
var ErrTaposHashMismatch = errors.New("tx ref_block_hash diverges from recent block")

// ValidateTAPOS checks the tx's TAPOS reference against the recent-block
// ring backed by rawdb. The validator runs at every entry that may admit a
// tx — pool admission (BlockChain.ValidateTransaction), peer-gossip
// (TronHandler.handleTx / handleTrxs), and per-tx during replay
// (ApplyTransaction with validateEnvelope=true). Mirrors java-tron
// Manager.validateTapos.
//
// The 2-byte ref_block_bytes are interpreted as the LOW 16 bits of an
// older block's number; the 8-byte ref_block_hash must equal that block's
// hash bytes 8..16. Because the ring is overwritten in place, any tx whose
// referenced block is older than ~65535 blocks back fails ErrTaposUnknownRef
// even if the chain still has the original block — matching java-tron's
// implicit expiry behavior.
func ValidateTAPOS(tx *types.Transaction, db ethdb.KeyValueReader) error {
	pb := tx.Proto()
	if pb == nil || pb.RawData == nil {
		return ErrTaposBadLength
	}
	refBytes := pb.RawData.RefBlockBytes
	refHash := pb.RawData.RefBlockHash
	if len(refBytes) != 2 || len(refHash) != 8 {
		return ErrTaposBadLength
	}
	recent := rawdb.ReadTaposRef(db, refBytes)
	if recent == nil {
		return ErrTaposUnknownRef
	}
	if !bytes.Equal(recent, refHash) {
		return ErrTaposHashMismatch
	}
	return nil
}
