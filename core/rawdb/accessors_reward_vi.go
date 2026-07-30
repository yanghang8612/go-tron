package rawdb

import (
	"fmt"
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
	ok, err := IsRewardViDoneStrict(db)
	return err == nil && ok
}

// IsRewardViDoneStrict reports whether the one-time reward-vi migration has
// completed and surfaces storage errors.
func IsRewardViDoneStrict(db ethdb.KeyValueReader) (bool, error) {
	if db == nil {
		return false, fmt.Errorf("rawdb: nil database while reading reward vi migration sentinel")
	}
	ok, err := db.Has(rewardViIsDoneKey())
	if err != nil {
		return false, fmt.Errorf("rawdb: read reward vi migration sentinel presence: %w", err)
	}
	return ok, nil
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
	vi, ok, err := ReadRewardViStrict(db, cycle, addr)
	if err != nil || !ok {
		return new(big.Int)
	}
	return vi
}

// ReadRewardViStrict returns the cumulative VI stored for (cycle, addr) and
// surfaces storage errors. Missing rows return (0, false, nil).
func ReadRewardViStrict(db ethdb.KeyValueReader, cycle int64, addr []byte) (*big.Int, bool, error) {
	data, ok, err := readPresentValue(db, rewardViKey(cycle, addr), "reward vi")
	if err != nil || !ok || len(data) == 0 {
		return new(big.Int), ok, err
	}
	// Matches java-tron BigInteger(byte[]) — big-endian two's-complement.
	return new(big.Int).SetBytes(data), true, nil
}

// DeleteRewardVi removes the VI entry for (cycle, addr).
func DeleteRewardVi(db ethdb.KeyValueWriter, cycle int64, addr []byte) error {
	return db.Delete(rewardViKey(cycle, addr))
}
