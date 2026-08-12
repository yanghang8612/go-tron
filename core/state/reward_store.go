package state

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math/big"
	"sort"

	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/state/kvdomains"
	"github.com/tronprotocol/go-tron/core/state/statecodec"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	"google.golang.org/protobuf/proto"
)

// CycleRewardSink can intercept block-final writes to the current-cycle reward
// pool. BlockChain uses it to batch the hot dl-<cycle>-<witness>-reward keys
// outside the rooted SystemReward domain until the next maintenance boundary.
type CycleRewardSink interface {
	AddCycleReward(cycle int64, addr tcommon.Address, delta int64) (bool, error)
	AddCycleRewards(cycle int64, deltas map[tcommon.Address]int64) (bool, error)
	PendingCycleReward(cycle int64, addr tcommon.Address) (int64, bool)
}

func (s *StateDB) SetCycleRewardSink(sink CycleRewardSink) {
	s.cycleRewardSink = sink
}

func (s *StateDB) readSystemRewardWithError(key []byte) ([]byte, bool, error) {
	return s.GetAccountKV(tcommon.SystemAccountAddress, kvdomains.SystemReward, key)
}

func (s *StateDB) readSystemReward(key []byte) ([]byte, bool) {
	raw, ok, err := s.readSystemRewardWithError(key)
	if err != nil {
		s.recordStateError(fmt.Sprintf("read system reward key=%x", key), err)
		return nil, false
	}
	return raw, ok
}

// readSystemRewardForDecoding borrows immutable bytes for immediate scalar or
// big.Int decoding. Callers must not retain or mutate the returned slice.
func (s *StateDB) readSystemRewardForDecoding(key []byte) ([]byte, bool) {
	raw, ok, err := s.getAccountKVForDecoding(tcommon.SystemAccountAddress, kvdomains.SystemReward, key)
	if err != nil || !ok {
		return nil, false
	}
	return raw, true
}

func (s *StateDB) writeSystemReward(key, value []byte) error {
	return s.SetAccountKV(tcommon.SystemAccountAddress, kvdomains.SystemReward, key, value)
}

func (s *StateDB) ReadCycleReward(cycle int64, addr []byte) int64 {
	key := rawdb.AppendCycleRewardStateKey(s.delegRewardKeyScratch[:0], cycle, addr)
	raw, ok := s.readSystemRewardForDecoding(key)
	base := int64(0)
	if ok {
		if len(raw) != 8 {
			s.recordStateError(fmt.Sprintf("decode cycle reward cycle=%d addr=%x", cycle, addr), fmt.Errorf("length %d, want 8", len(raw)))
		} else {
			base = int64(binary.BigEndian.Uint64(raw))
		}
	}
	if s.cycleRewardSink != nil {
		if pending, ok := s.cycleRewardSink.PendingCycleReward(cycle, tcommon.BytesToAddress(addr)); ok {
			base += pending
		}
	}
	return base
}

func (s *StateDB) ReadCycleRewardStrict(cycle int64, addr []byte) (int64, bool, error) {
	raw, ok, err := s.readSystemRewardWithError(rawdb.CycleRewardStateKey(cycle, addr))
	base, present, err := decodeSystemRewardInt64(raw, ok, err, 0, "cycle reward")
	if err != nil {
		return 0, present, err
	}
	if s.cycleRewardSink != nil {
		if pending, ok := s.cycleRewardSink.PendingCycleReward(cycle, tcommon.BytesToAddress(addr)); ok {
			base += pending
		}
	}
	return base, present, nil
}

func (s *StateDB) ReadCycleRewards(cycle int64, addrs []tcommon.Address) map[tcommon.Address]int64 {
	out, err := s.ReadCycleRewardsStrict(cycle, addrs)
	if err != nil {
		s.recordStateError(fmt.Sprintf("read cycle rewards cycle=%d", cycle), err)
		return make(map[tcommon.Address]int64, len(addrs))
	}
	return out
}

func (s *StateDB) ReadCycleRewardsStrict(cycle int64, addrs []tcommon.Address) (map[tcommon.Address]int64, error) {
	keys := make([][]byte, 0, len(addrs))
	for _, addr := range addrs {
		keys = append(keys, rawdb.CycleRewardStateKey(cycle, addr.Bytes()))
	}
	values, err := s.GetAccountKVBatch(tcommon.SystemAccountAddress, kvdomains.SystemReward, keys)
	out := make(map[tcommon.Address]int64, len(addrs))
	if err != nil {
		return out, err
	}
	for i, addr := range addrs {
		raw, exists := values[string(keys[i])]
		if exists {
			if len(raw) != 8 {
				return out, fmt.Errorf("state: decode cycle reward: length %d, want 8", len(raw))
			}
			out[addr] = int64(binary.BigEndian.Uint64(raw))
		}
		if s.cycleRewardSink != nil {
			if pending, ok := s.cycleRewardSink.PendingCycleReward(cycle, addr); ok {
				out[addr] += pending
			}
		}
	}
	return out, nil
}

func (s *StateDB) WriteCycleReward(cycle int64, addr []byte, value int64) error {
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], uint64(value))
	key := rawdb.AppendCycleRewardStateKey(s.delegRewardKeyScratch[:0], cycle, addr)
	return s.writeSystemReward(key, buf[:])
}

func (s *StateDB) WriteCycleRewardFinal(cycle int64, addr []byte, value int64) error {
	key := rawdb.CycleRewardStateKey(cycle, addr)
	raw, exists, err := s.readSystemRewardWithError(key)
	if err != nil {
		return err
	}
	return s.writeCycleRewardFinalWithPrev(key, raw, exists, value)
}

func (s *StateDB) writeCycleRewardFinalWithPrev(key, prev []byte, prevExists bool, value int64) error {
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], uint64(value))
	return s.setAccountKVFinalWithPrev(tcommon.SystemAccountAddress, kvdomains.SystemReward, key, prev, buf[:], prevExists)
}

func decodeCycleReward(raw []byte, exists bool) (int64, error) {
	if !exists {
		return 0, nil
	}
	if len(raw) != 8 {
		return 0, fmt.Errorf("state: decode cycle reward: length %d, want 8", len(raw))
	}
	return int64(binary.BigEndian.Uint64(raw)), nil
}

func (s *StateDB) AddCycleReward(cycle int64, addr []byte, delta int64) error {
	current, _, err := s.ReadCycleRewardStrict(cycle, addr)
	if err != nil {
		return err
	}
	return s.WriteCycleReward(cycle, addr, current+delta)
}

func (s *StateDB) AddCycleRewardFinal(cycle int64, addr []byte, delta int64) error {
	if s.cycleRewardSink != nil {
		handled, err := s.cycleRewardSink.AddCycleReward(cycle, tcommon.BytesToAddress(addr), delta)
		if err != nil || handled {
			return err
		}
	}
	key := rawdb.CycleRewardStateKey(cycle, addr)
	raw, exists, err := s.readSystemRewardWithError(key)
	if err != nil {
		return err
	}
	current, err := decodeCycleReward(raw, exists)
	if err != nil {
		return err
	}
	return s.writeCycleRewardFinalWithPrev(key, raw, exists, current+delta)
}

func (s *StateDB) AddCycleRewards(cycle int64, deltas map[tcommon.Address]int64) error {
	return s.addCycleRewards(cycle, deltas, false)
}

func (s *StateDB) AddCycleRewardsFinal(cycle int64, deltas map[tcommon.Address]int64) error {
	return s.addCycleRewards(cycle, deltas, true)
}

func (s *StateDB) addCycleRewards(cycle int64, deltas map[tcommon.Address]int64, final bool) error {
	if len(deltas) == 0 {
		return nil
	}
	if final && s.cycleRewardSink != nil {
		handled, err := s.cycleRewardSink.AddCycleRewards(cycle, deltas)
		if err != nil || handled {
			return err
		}
	}
	addrs := make([]tcommon.Address, 0, len(deltas))
	for addr, delta := range deltas {
		if delta != 0 {
			addrs = append(addrs, addr)
		}
	}
	if len(addrs) == 0 {
		return nil
	}
	sort.Slice(addrs, func(i, j int) bool {
		return bytes.Compare(addrs[i].Bytes(), addrs[j].Bytes()) < 0
	})
	keys := make([][]byte, 0, len(addrs))
	for _, addr := range addrs {
		keys = append(keys, rawdb.CycleRewardStateKey(cycle, addr.Bytes()))
	}
	current, err := s.GetAccountKVBatch(tcommon.SystemAccountAddress, kvdomains.SystemReward, keys)
	if err != nil {
		return err
	}
	for i, addr := range addrs {
		var err error
		key := keys[i]
		raw, exists := current[string(key)]
		base, err := decodeCycleReward(raw, exists)
		if err != nil {
			return err
		}
		next := base + deltas[addr]
		if final {
			err = s.writeCycleRewardFinalWithPrev(key, raw, exists, next)
		} else {
			err = s.WriteCycleReward(cycle, addr.Bytes(), next)
		}
		if err != nil {
			return err
		}
	}
	return nil
}

func (s *StateDB) ReadCycleVote(cycle int64, addr []byte) int64 {
	value, ok, err := s.ReadCycleVoteStrict(cycle, addr)
	if err != nil {
		s.recordStateError(fmt.Sprintf("read cycle vote cycle=%d addr=%x", cycle, addr), err)
		return rawdb.RewardRemark
	}
	if !ok {
		return rawdb.RewardRemark
	}
	return value
}

func (s *StateDB) ReadCycleVoteStrict(cycle int64, addr []byte) (int64, bool, error) {
	raw, ok, err := s.readSystemRewardWithError(rawdb.CycleVoteStateKey(cycle, addr))
	return decodeSystemRewardInt64(raw, ok, err, rawdb.RewardRemark, "cycle vote")
}

func (s *StateDB) WriteCycleVote(cycle int64, addr []byte, value int64) error {
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], uint64(value))
	return s.writeSystemReward(rawdb.CycleVoteStateKey(cycle, addr), buf[:])
}

func (s *StateDB) ReadWitnessVI(cycle int64, addr []byte) *big.Int {
	value, ok, err := s.ReadWitnessVIStrict(cycle, addr)
	if err != nil {
		s.recordStateError(fmt.Sprintf("read witness VI cycle=%d addr=%x", cycle, addr), err)
		return new(big.Int)
	}
	if !ok {
		return new(big.Int)
	}
	return value
}

func (s *StateDB) ReadWitnessVIStrict(cycle int64, addr []byte) (*big.Int, bool, error) {
	raw, ok, err := s.readSystemRewardWithError(rawdb.WitnessVIStateKey(cycle, addr))
	if err != nil || !ok || len(raw) == 0 {
		return new(big.Int), ok, err
	}
	return new(big.Int).SetBytes(raw), true, nil
}

func (s *StateDB) WriteWitnessVI(cycle int64, addr []byte, vi *big.Int) error {
	if vi == nil {
		vi = new(big.Int)
	}
	return s.writeSystemReward(rawdb.WitnessVIStateKey(cycle, addr), vi.Bytes())
}

func (s *StateDB) ReadCycleBrokerage(cycle int64, addr []byte) int {
	value, ok, err := s.ReadCycleBrokerageStrict(cycle, addr)
	if err != nil {
		s.recordStateError(fmt.Sprintf("read cycle brokerage cycle=%d addr=%x", cycle, addr), err)
		return rawdb.DefaultBrokerage
	}
	if !ok {
		return rawdb.DefaultBrokerage
	}
	return value
}

func (s *StateDB) ReadCycleBrokerageStrict(cycle int64, addr []byte) (int, bool, error) {
	raw, ok, err := s.readSystemRewardWithError(rawdb.CycleBrokerageStateKey(cycle, addr))
	if err != nil || !ok {
		return rawdb.DefaultBrokerage, ok, err
	}
	if len(raw) != 4 {
		return rawdb.DefaultBrokerage, false, fmt.Errorf("state: decode cycle brokerage: length %d, want 4", len(raw))
	}
	return int(int32(binary.BigEndian.Uint32(raw))), true, nil
}

// CycleBrokerageAt reconstructs a witness brokerage snapshot for cycle at the
// end of blockNum. Missing rows default to java-tron's DEFAULT_BROKERAGE, while
// malformed retained/cold history rows surface as archive data errors.
func (r *PersistentHistoryReader) CycleBrokerageAt(cycle int64, addr []byte, blockNum uint64) (int64, error) {
	raw, ok, err := r.AccountKVAt(tcommon.SystemAccountAddress, kvdomains.SystemReward, rawdb.CycleBrokerageStateKey(cycle, addr), blockNum)
	if err != nil {
		return 0, err
	}
	if !ok {
		return int64(rawdb.DefaultBrokerage), nil
	}
	if len(raw) != 4 {
		return 0, fmt.Errorf("decode cycle brokerage at block %d: length %d, want 4", blockNum, len(raw))
	}
	return int64(int32(binary.BigEndian.Uint32(raw))), nil
}

func (s *StateDB) WriteCycleBrokerage(cycle int64, addr []byte, rate int) error {
	var buf [4]byte
	binary.BigEndian.PutUint32(buf[:], uint32(int32(rate)))
	return s.writeSystemReward(rawdb.CycleBrokerageStateKey(cycle, addr), buf[:])
}

func (s *StateDB) ReadCycleAccountVote(cycle int64, addr []byte) []byte {
	raw, ok, err := s.ReadCycleAccountVoteStrict(cycle, addr)
	if err != nil {
		s.recordStateError(fmt.Sprintf("read cycle account vote cycle=%d addr=%x", cycle, addr), err)
		return nil
	}
	if !ok || len(raw) == 0 {
		return nil
	}
	return raw
}

func (s *StateDB) ReadCycleAccountVoteStrict(cycle int64, addr []byte) ([]byte, bool, error) {
	raw, ok, err := s.readSystemRewardWithError(rawdb.CycleAccountVoteStateKey(cycle, addr))
	if err != nil || !ok {
		return nil, ok, err
	}
	account := new(corepb.Account)
	if err := statecodec.Unmarshal(raw, account); err != nil {
		return nil, true, fmt.Errorf("state: decode cycle account vote: %w", err)
	}
	data, err := proto.Marshal(account)
	if err != nil {
		return nil, true, fmt.Errorf("state: encode cycle account vote API value: %w", err)
	}
	return data, true, nil
}

func (s *StateDB) WriteCycleAccountVote(cycle int64, addr, data []byte) error {
	account := new(corepb.Account)
	if err := proto.Unmarshal(data, account); err != nil {
		return fmt.Errorf("state: decode cycle account vote API value: %w", err)
	}
	native, err := statecodec.Marshal(account)
	if err != nil {
		return err
	}
	return s.writeSystemReward(rawdb.CycleAccountVoteStateKey(cycle, addr), native)
}

func (s *StateDB) ReadBeginCycle(addr []byte) int64 {
	value, ok, err := s.ReadBeginCycleStrict(addr)
	if err != nil {
		s.recordStateError(fmt.Sprintf("read begin cycle addr=%x", addr), err)
		return 0
	}
	if !ok {
		return 0
	}
	return value
}

func (s *StateDB) ReadBeginCycleStrict(addr []byte) (int64, bool, error) {
	raw, ok, err := s.readSystemRewardWithError(rawdb.BeginCycleStateKey(addr))
	return decodeSystemRewardInt64(raw, ok, err, 0, "begin cycle")
}

func (s *StateDB) WriteBeginCycle(addr []byte, cycle int64) error {
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], uint64(cycle))
	return s.writeSystemReward(rawdb.BeginCycleStateKey(addr), buf[:])
}

func (s *StateDB) ReadEndCycle(addr []byte) int64 {
	value, ok, err := s.ReadEndCycleStrict(addr)
	if err != nil {
		s.recordStateError(fmt.Sprintf("read end cycle addr=%x", addr), err)
		return rawdb.RewardRemark
	}
	if !ok {
		return rawdb.RewardRemark
	}
	return value
}

func (s *StateDB) ReadEndCycleStrict(addr []byte) (int64, bool, error) {
	raw, ok, err := s.readSystemRewardWithError(rawdb.EndCycleStateKey(addr))
	return decodeSystemRewardInt64(raw, ok, err, rawdb.RewardRemark, "end cycle")
}

func (s *StateDB) WriteEndCycle(addr []byte, cycle int64) error {
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], uint64(cycle))
	return s.writeSystemReward(rawdb.EndCycleStateKey(addr), buf[:])
}

func decodeSystemRewardInt64(raw []byte, ok bool, err error, missing int64, context string) (int64, bool, error) {
	if err != nil || !ok {
		return missing, ok, err
	}
	if len(raw) != 8 {
		return missing, false, fmt.Errorf("state: decode %s: length %d, want 8", context, len(raw))
	}
	return int64(binary.BigEndian.Uint64(raw)), true, nil
}
