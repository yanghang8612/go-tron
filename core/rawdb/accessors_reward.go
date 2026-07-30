package rawdb

import (
	"encoding/binary"
	"fmt"
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
	value, ok, err := ReadCycleRewardStrict(db, cycle, addr)
	if err != nil || !ok {
		return 0
	}
	return value
}

// ReadCycleRewardStrict returns the accumulated voter reward pool for a
// witness in a given cycle and surfaces storage/corruption errors. Missing
// rows return (0, false, nil), preserving the java-tron default for callers
// that choose to ignore the presence flag.
func ReadCycleRewardStrict(db ethdb.KeyValueReader, cycle int64, addr []byte) (int64, bool, error) {
	return readRewardInt64(db, delegRewardKey(cycle, addr, "reward"), "cycle reward", 0)
}

// WriteCycleReward overwrites the voter pool for a witness in a cycle.
func WriteCycleReward(db ethdb.KeyValueWriter, cycle int64, addr []byte, v int64) {
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], uint64(v))
	_ = db.Put(delegRewardKey(cycle, addr, "reward"), buf[:])
}

// AddCycleReward increments the voter pool by delta. Creates the key if
// absent. Mirrors DelegationStore.addReward. The db parameter is the
// read+write composite so both `ethdb.KeyValueStore` and
// `core/blockbuffer.Buffer` satisfy it (slice 3 of the fork-rewind fix
// routes per-block AddCycleReward writes through the buffer).
func AddCycleReward(db interface {
	ethdb.KeyValueReader
	ethdb.KeyValueWriter
}, cycle int64, addr []byte, delta int64) {
	WriteCycleReward(db, cycle, addr, ReadCycleReward(db, cycle, addr)+delta)
}

// ---- per-cycle witness vote snapshot -----------------------------------

// ReadCycleVote returns the total vote count snapshot for a witness in a
// cycle. Returns RewardRemark (-1) if never written, matching java-tron's
// DelegationStore.getWitnessVote sentinel.
func ReadCycleVote(db ethdb.KeyValueReader, cycle int64, addr []byte) int64 {
	value, ok, err := ReadCycleVoteStrict(db, cycle, addr)
	if err != nil || !ok {
		return RewardRemark
	}
	return value
}

// ReadCycleVoteStrict returns the witness vote snapshot for a cycle and
// surfaces storage/corruption errors. Missing rows return
// (RewardRemark, false, nil).
func ReadCycleVoteStrict(db ethdb.KeyValueReader, cycle int64, addr []byte) (int64, bool, error) {
	return readRewardInt64(db, delegRewardKey(cycle, addr, "vote"), "cycle vote", RewardRemark)
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
	vi, ok, err := ReadWitnessVIStrict(db, cycle, addr)
	if err != nil || !ok {
		return new(big.Int)
	}
	return vi
}

// ReadWitnessVIStrict returns the accumulated VI for a witness at a cycle
// boundary and surfaces storage errors. Empty present values are valid zeroes
// because WriteWitnessVI stores vi.Bytes().
func ReadWitnessVIStrict(db ethdb.KeyValueReader, cycle int64, addr []byte) (*big.Int, bool, error) {
	data, ok, err := readPresentValue(db, delegRewardKey(cycle, addr, "vi"), "witness vi")
	if err != nil || !ok || len(data) == 0 {
		return new(big.Int), ok, err
	}
	// Java-tron uses BigInteger.toByteArray (two's-complement, big-endian,
	// with sign bit). Mirror that format exactly.
	return new(big.Int).SetBytes(data), true, nil
}

// EncodeJavaNonNegativeBigInteger mirrors BigInteger.toByteArray for the
// non-negative values used by java-tron's VI stores.
func EncodeJavaNonNegativeBigInteger(value *big.Int) []byte {
	if value == nil || value.Sign() == 0 {
		return []byte{0}
	}
	if value.Sign() < 0 {
		panic("cannot encode negative value as non-negative Java BigInteger")
	}
	magnitude := value.Bytes()
	if magnitude[0]&0x80 == 0 {
		return magnitude
	}
	encoded := make([]byte, len(magnitude)+1)
	copy(encoded[1:], magnitude)
	return encoded
}

// WriteWitnessVI stores the accumulated VI for a witness at a cycle.
func WriteWitnessVI(db ethdb.KeyValueWriter, cycle int64, addr []byte, vi *big.Int) {
	_ = db.Put(delegRewardKey(cycle, addr, "vi"), EncodeJavaNonNegativeBigInteger(vi))
}

// ---- per-cycle brokerage snapshot --------------------------------------

// ReadCycleBrokerage returns the brokerage rate (0-100) for a witness at a
// cycle. Default 20 if absent. When cycle == -1 this is the "current"
// brokerage rate set by the UpdateBrokerage actuator.
func ReadCycleBrokerage(db ethdb.KeyValueReader, cycle int64, addr []byte) int {
	value, ok, err := ReadCycleBrokerageStrict(db, cycle, addr)
	if err != nil || !ok {
		return DefaultBrokerage
	}
	return value
}

// ReadCycleBrokerageStrict returns the brokerage rate for a witness at a cycle
// and surfaces storage/corruption errors. Missing rows return
// (DefaultBrokerage, false, nil).
func ReadCycleBrokerageStrict(db ethdb.KeyValueReader, cycle int64, addr []byte) (int, bool, error) {
	data, ok, err := readPresentValue(db, delegRewardKey(cycle, addr, "brokerage"), "cycle brokerage")
	if err != nil || !ok {
		return DefaultBrokerage, ok, err
	}
	if len(data) != 4 {
		return DefaultBrokerage, false, fmt.Errorf("rawdb: decode cycle brokerage: length %d, want 4", len(data))
	}
	return int(int32(binary.BigEndian.Uint32(data))), true, nil
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
	data, ok, err := ReadCycleAccountVoteStrict(db, cycle, addr)
	if err != nil || !ok || len(data) == 0 {
		return nil
	}
	return data
}

// ReadCycleAccountVoteStrict returns the voter account snapshot for a given
// cycle and surfaces storage errors. Missing rows return (nil, false, nil).
func ReadCycleAccountVoteStrict(db ethdb.KeyValueReader, cycle int64, addr []byte) ([]byte, bool, error) {
	return readPresentValue(db, delegRewardKey(cycle, addr, "account-vote"), "cycle account vote")
}

// WriteCycleAccountVote stores the voter account protobuf snapshot for a
// given cycle.
func WriteCycleAccountVote(db ethdb.KeyValueWriter, cycle int64, addr []byte, proto []byte) {
	_ = db.Put(delegRewardKey(cycle, addr, "account-vote"), proto)
}

func CycleRewardStateKey(cycle int64, addr []byte) []byte {
	return delegRewardKey(cycle, addr, "reward")
}

func AppendCycleRewardStateKey(dst []byte, cycle int64, addr []byte) []byte {
	return appendDelegRewardKey(dst, cycle, addr, "reward")
}

func CycleVoteStateKey(cycle int64, addr []byte) []byte {
	return delegRewardKey(cycle, addr, "vote")
}

func AppendCycleVoteStateKey(dst []byte, cycle int64, addr []byte) []byte {
	return appendDelegRewardKey(dst, cycle, addr, "vote")
}

func WitnessVIStateKey(cycle int64, addr []byte) []byte {
	return delegRewardKey(cycle, addr, "vi")
}

func AppendWitnessVIStateKey(dst []byte, cycle int64, addr []byte) []byte {
	return appendDelegRewardKey(dst, cycle, addr, "vi")
}

func CycleBrokerageStateKey(cycle int64, addr []byte) []byte {
	return delegRewardKey(cycle, addr, "brokerage")
}

func AppendCycleBrokerageStateKey(dst []byte, cycle int64, addr []byte) []byte {
	return appendDelegRewardKey(dst, cycle, addr, "brokerage")
}

func CycleAccountVoteStateKey(cycle int64, addr []byte) []byte {
	return delegRewardKey(cycle, addr, "account-vote")
}

func AppendCycleAccountVoteStateKey(dst []byte, cycle int64, addr []byte) []byte {
	return appendDelegRewardKey(dst, cycle, addr, "account-vote")
}

// ---- voter beginCycle / endCycle cursors -------------------------------

// ReadBeginCycle returns the voter's beginCycle cursor. Zero if unset.
func ReadBeginCycle(db ethdb.KeyValueReader, addr []byte) int64 {
	value, ok, err := ReadBeginCycleStrict(db, addr)
	if err != nil || !ok {
		return 0
	}
	return value
}

// ReadBeginCycleStrict returns the voter's beginCycle cursor and surfaces
// storage/corruption errors. Missing rows return (0, false, nil).
func ReadBeginCycleStrict(db ethdb.KeyValueReader, addr []byte) (int64, bool, error) {
	return readRewardInt64(db, delegBeginCycleKey(addr), "begin cycle", 0)
}

// WriteBeginCycle stores the voter's beginCycle cursor.
func WriteBeginCycle(db ethdb.KeyValueWriter, addr []byte, cycle int64) {
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], uint64(cycle))
	_ = db.Put(delegBeginCycleKey(addr), buf[:])
}

func BeginCycleStateKey(addr []byte) []byte {
	return delegBeginCycleKey(addr)
}

// ReadEndCycle returns the voter's endCycle cursor. Returns RewardRemark (-1)
// if never written, matching java-tron's DelegationStore.getEndCycle sentinel.
func ReadEndCycle(db ethdb.KeyValueReader, addr []byte) int64 {
	value, ok, err := ReadEndCycleStrict(db, addr)
	if err != nil || !ok {
		return RewardRemark
	}
	return value
}

// ReadEndCycleStrict returns the voter's endCycle cursor and surfaces
// storage/corruption errors. Missing rows return (RewardRemark, false, nil).
func ReadEndCycleStrict(db ethdb.KeyValueReader, addr []byte) (int64, bool, error) {
	return readRewardInt64(db, delegEndCycleKey(addr), "end cycle", RewardRemark)
}

// WriteEndCycle stores the voter's endCycle cursor.
func WriteEndCycle(db ethdb.KeyValueWriter, addr []byte, cycle int64) {
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], uint64(cycle))
	_ = db.Put(delegEndCycleKey(addr), buf[:])
}

func EndCycleStateKey(addr []byte) []byte {
	return delegEndCycleKey(addr)
}

func readRewardInt64(db ethdb.KeyValueReader, key []byte, context string, missing int64) (int64, bool, error) {
	data, ok, err := readPresentValue(db, key, context)
	if err != nil || !ok {
		return missing, ok, err
	}
	if len(data) != 8 {
		return missing, false, fmt.Errorf("rawdb: decode %s: length %d, want 8", context, len(data))
	}
	return int64(binary.BigEndian.Uint64(data)), true, nil
}
