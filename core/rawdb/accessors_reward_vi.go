package rawdb

import (
	"math/big"

	"github.com/ethereum/go-ethereum/ethdb"
)

// rewardViIsDoneValue is the single-byte value written to the IS_DONE sentinel.
var rewardViIsDoneValue = []byte{0x01}

// WriteRewardViIsDone marks the reward-vi migration as complete. Mirrors
// java-tron RewardViCalService.startRewardCal's final IS_DONE write.
func WriteRewardViIsDone(db ethdb.KeyValueWriter) {
	_ = db.Put(rewardViIsDoneKey(), rewardViIsDoneValue)
}

// IsRewardViDone reports whether the one-time reward-vi migration has been
// completed for this node.
func IsRewardViDone(db ethdb.KeyValueReader) bool {
	ok, _ := db.Has(rewardViIsDoneKey())
	return ok
}

// WriteRewardVi stores the cumulative VI for a witness at a given cycle
// boundary. VI is a BigInteger (two's-complement big-endian, minimum bytes);
// zero values are not stored (mirrors java-tron's "Zero vi will not be
// record" comment in RewardViCalService.accumulateWitnessVi).
func WriteRewardVi(db ethdb.KeyValueWriter, cycle int64, addr []byte, vi *big.Int) {
	if vi == nil || vi.Sign() == 0 {
		return
	}
	_ = db.Put(rewardViKey(cycle, addr), vi.Bytes())
}

// ReadRewardVi returns the cumulative VI stored for (cycle, addr). Returns
// zero if absent, matching java-tron's BigInteger.ZERO default.
func ReadRewardVi(db ethdb.KeyValueReader, cycle int64, addr []byte) *big.Int {
	data, err := db.Get(rewardViKey(cycle, addr))
	if err != nil || len(data) == 0 {
		return new(big.Int)
	}
	// Matches java-tron BigInteger(byte[]) — big-endian two's-complement.
	return new(big.Int).SetBytes(data)
}

// DeleteRewardVi removes the VI entry for (cycle, addr).
func DeleteRewardVi(db ethdb.KeyValueWriter, cycle int64, addr []byte) error {
	return db.Delete(rewardViKey(cycle, addr))
}
