package rawdb

import (
	"encoding/binary"
	"math/big"

	"github.com/ethereum/go-ethereum/ethdb"
)

// DefaultBrokerage mirrors java-tron's DelegationStore.DEFAULT_BROKERAGE (20%).
const DefaultBrokerage = 20

// RewardRemark mirrors java-tron's DelegationStore.REMARK (-1).
// Used as the sentinel for "no per-cycle snapshot recorded yet".
const RewardRemark int64 = -1

// ---- per-cycle voter reward pool ---------------------------------------

// ReadCycleReward returns the accumulated voter reward pool for a witness in
// a given cycle. Returns 0 if absent.
func ReadCycleReward(db ethdb.KeyValueReader, cycle int64, addr []byte) int64 {
	data, _ := db.Get(delegRewardKey(cycle, addr, "reward"))
	if len(data) != 8 {
		return 0
	}
	return int64(binary.BigEndian.Uint64(data))
}

// WriteCycleReward overwrites the voter pool for a witness in a cycle.
func WriteCycleReward(db ethdb.KeyValueWriter, cycle int64, addr []byte, v int64) {
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], uint64(v))
	_ = db.Put(delegRewardKey(cycle, addr, "reward"), buf[:])
}

// AddCycleReward increments the voter pool by delta. Creates the key if
// absent. Mirrors DelegationStore.addReward.
func AddCycleReward(db ethdb.KeyValueStore, cycle int64, addr []byte, delta int64) {
	WriteCycleReward(db, cycle, addr, ReadCycleReward(db, cycle, addr)+delta)
}

// ---- per-cycle witness vote snapshot -----------------------------------

// ReadCycleVote returns the total vote count snapshot for a witness in a
// cycle. Returns RewardRemark (-1) if never written, matching java-tron's
// DelegationStore.getWitnessVote sentinel.
func ReadCycleVote(db ethdb.KeyValueReader, cycle int64, addr []byte) int64 {
	data, _ := db.Get(delegRewardKey(cycle, addr, "vote"))
	if len(data) != 8 {
		return RewardRemark
	}
	return int64(binary.BigEndian.Uint64(data))
}

// WriteCycleVote stores the vote snapshot for a witness in a cycle.
func WriteCycleVote(db ethdb.KeyValueWriter, cycle int64, addr []byte, v int64) {
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], uint64(v))
	_ = db.Put(delegRewardKey(cycle, addr, "vote"), buf[:])
}

// ---- per-cycle witness VI ----------------------------------------------

// ReadWitnessVI returns the accumulated VI for a witness at a given cycle
// boundary. Zero if never written. Uses big.Int to mirror java-tron's
// BigInteger (VI values overflow int64 at high vote volumes × 10^18).
func ReadWitnessVI(db ethdb.KeyValueReader, cycle int64, addr []byte) *big.Int {
	data, _ := db.Get(delegRewardKey(cycle, addr, "vi"))
	if len(data) == 0 {
		return new(big.Int)
	}
	// Java-tron uses BigInteger.toByteArray (two's-complement, big-endian,
	// with sign bit). Mirror that format exactly.
	return new(big.Int).SetBytes(data)
}

// WriteWitnessVI stores the accumulated VI for a witness at a cycle.
func WriteWitnessVI(db ethdb.KeyValueWriter, cycle int64, addr []byte, vi *big.Int) {
	_ = db.Put(delegRewardKey(cycle, addr, "vi"), vi.Bytes())
}

// ---- per-cycle brokerage snapshot --------------------------------------

// ReadCycleBrokerage returns the brokerage rate (0-100) for a witness at a
// cycle. Default 20 if absent. When cycle == -1 this is the "current"
// brokerage rate set by the UpdateBrokerage actuator.
func ReadCycleBrokerage(db ethdb.KeyValueReader, cycle int64, addr []byte) int {
	data, _ := db.Get(delegRewardKey(cycle, addr, "brokerage"))
	if len(data) != 4 {
		return DefaultBrokerage
	}
	return int(int32(binary.BigEndian.Uint32(data)))
}

// WriteCycleBrokerage stores the brokerage rate for a witness at a cycle.
func WriteCycleBrokerage(db ethdb.KeyValueWriter, cycle int64, addr []byte, rate int) {
	var buf [4]byte
	binary.BigEndian.PutUint32(buf[:], uint32(int32(rate)))
	_ = db.Put(delegRewardKey(cycle, addr, "brokerage"), buf[:])
}

// ---- voter per-cycle account-vote snapshot -----------------------------

// ReadCycleAccountVote returns the voter account snapshot for a given cycle.
// Nil if absent.
func ReadCycleAccountVote(db ethdb.KeyValueReader, cycle int64, addr []byte) []byte {
	data, _ := db.Get(delegRewardKey(cycle, addr, "account-vote"))
	if len(data) == 0 {
		return nil
	}
	return data
}

// WriteCycleAccountVote stores the voter account protobuf snapshot for a
// given cycle.
func WriteCycleAccountVote(db ethdb.KeyValueWriter, cycle int64, addr []byte, proto []byte) {
	_ = db.Put(delegRewardKey(cycle, addr, "account-vote"), proto)
}

// ---- voter beginCycle / endCycle cursors -------------------------------

// ReadBeginCycle returns the voter's beginCycle cursor. Zero if unset.
func ReadBeginCycle(db ethdb.KeyValueReader, addr []byte) int64 {
	data, _ := db.Get(delegBeginCycleKey(addr))
	if len(data) != 8 {
		return 0
	}
	return int64(binary.BigEndian.Uint64(data))
}

// WriteBeginCycle stores the voter's beginCycle cursor.
func WriteBeginCycle(db ethdb.KeyValueWriter, addr []byte, cycle int64) {
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], uint64(cycle))
	_ = db.Put(delegBeginCycleKey(addr), buf[:])
}

// ReadEndCycle returns the voter's endCycle cursor. Returns RewardRemark (-1)
// if never written, matching java-tron's DelegationStore.getEndCycle sentinel.
func ReadEndCycle(db ethdb.KeyValueReader, addr []byte) int64 {
	data, _ := db.Get(delegEndCycleKey(addr))
	if len(data) != 8 {
		return RewardRemark
	}
	return int64(binary.BigEndian.Uint64(data))
}

// WriteEndCycle stores the voter's endCycle cursor.
func WriteEndCycle(db ethdb.KeyValueWriter, addr []byte, cycle int64) {
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], uint64(cycle))
	_ = db.Put(delegEndCycleKey(addr), buf[:])
}
