package dpos

import (
	"testing"

	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/state"
	"github.com/tronprotocol/go-tron/core/types"
	"github.com/tronprotocol/go-tron/params"
	corepb "github.com/tronprotocol/go-tron/proto/core"
)

func statAddr(b byte) common.Address {
	var addr common.Address
	addr[0] = 0x41
	addr[20] = b
	return addr
}

// makeBlock builds a minimal block for statistics tests. Number must be ≥ 1.
func makeBlock(number uint64, timestamp int64, producer common.Address) *types.Block {
	return types.NewBlockFromPB(&corepb.Block{
		BlockHeader: &corepb.BlockHeader{
			RawData: &corepb.BlockHeaderRaw{
				Number:         int64(number),
				Timestamp:      timestamp,
				WitnessAddress: producer.Bytes(),
			},
		},
	})
}

type statWitnessStore struct {
	witnesses map[common.Address]*types.Witness
}

func newStatWitnessStore() *statWitnessStore {
	return &statWitnessStore{witnesses: make(map[common.Address]*types.Witness)}
}

func (s *statWitnessStore) GetWitness(addr common.Address) *types.Witness {
	if w := s.witnesses[addr]; w != nil {
		return w.Copy()
	}
	return nil
}

func (s *statWitnessStore) SetWitnessCapsule(w *types.Witness) error {
	if w != nil {
		s.witnesses[w.Address()] = w.Copy()
	}
	return nil
}

func TestApplyBlockStatistics_ProducerCounters(t *testing.T) {
	wstore := newStatWitnessStore()
	dp := state.NewDynamicProperties()
	witnesses := []common.Address{statAddr(1), statAddr(2), statAddr(3)}
	producer := witnesses[0]

	const genesisTs = int64(1_000_000)
	// Block 1: parent is genesis. blockTime exactly one slot after genesis.
	blockTime := genesisTs + params.BlockProducedInterval
	block := makeBlock(1, blockTime, producer)

	ApplyBlockStatistics(wstore, dp, block, genesisTs, witnesses, genesisTs, false)

	w := wstore.GetWitness(producer)
	if w == nil {
		t.Fatal("producer witness was not persisted")
	}
	if w.TotalProduced() != 1 {
		t.Errorf("TotalProduced: want 1, got %d", w.TotalProduced())
	}
	if w.LatestBlockNum() != 1 {
		t.Errorf("LatestBlockNum: want 1, got %d", w.LatestBlockNum())
	}
	wantSlot := AbsoluteSlot(blockTime, genesisTs)
	if w.LatestSlotNum() != wantSlot {
		t.Errorf("LatestSlotNum: want %d, got %d", wantSlot, w.LatestSlotNum())
	}
	// The current slot bit must be filled.
	if got := dp.BlockFilledSlotsIndex(); got != 1 {
		t.Errorf("filled-slots index after one block: want 1, got %d", got)
	}
	if dp.BlockFilledSlots()[0] != 1 {
		t.Error("filled-slots[0] must be 1 for produced block")
	}
}

func TestApplyBlockStatistics_NoMissed(t *testing.T) {
	wstore := newStatWitnessStore()
	dp := state.NewDynamicProperties()
	witnesses := []common.Address{statAddr(1), statAddr(2), statAddr(3)}
	producer := witnesses[0]

	const genesisTs = int64(1_000_000)
	// Insert block 1 first to set up a non-genesis chain head.
	prevTime := genesisTs + params.BlockProducedInterval
	ApplyBlockStatistics(wstore, dp,
		makeBlock(1, prevTime, producer), genesisTs, witnesses, genesisTs, false)

	// Block 2: exactly one slot after the previous head — slot=1, no misses.
	block2Time := prevTime + params.BlockProducedInterval
	ApplyBlockStatistics(wstore, dp,
		makeBlock(2, block2Time, producer), prevTime, witnesses, genesisTs, false)

	for i, addr := range witnesses {
		w := wstore.GetWitness(addr)
		var totalMissed int64
		if w != nil {
			totalMissed = w.TotalMissed()
		}
		if totalMissed != 0 {
			t.Errorf("witness %d TotalMissed: want 0, got %d", i, totalMissed)
		}
	}
}

func TestApplyBlockStatistics_OneMissed(t *testing.T) {
	wstore := newStatWitnessStore()
	dp := state.NewDynamicProperties()
	witnesses := []common.Address{statAddr(1), statAddr(2), statAddr(3)}

	const genesisTs = int64(1_000_000)
	// Block 1 sets the head.
	prevTime := genesisTs + params.BlockProducedInterval
	ApplyBlockStatistics(wstore, dp,
		makeBlock(1, prevTime, witnesses[0]), genesisTs, witnesses, genesisTs, false)

	// Block 2 is two slots later — one slot is missed.
	block2Time := prevTime + 2*params.BlockProducedInterval
	expectedMissed := GetScheduledWitness(1, prevTime, genesisTs, witnesses, false, params.MaintenanceSkipSlots)
	ApplyBlockStatistics(wstore, dp,
		makeBlock(2, block2Time, witnesses[0]), prevTime, witnesses, genesisTs, false)

	wMissed := wstore.GetWitness(expectedMissed)
	if wMissed == nil {
		t.Fatalf("expected-missed witness %x has no record", expectedMissed)
	}
	// Block 1 produced by witnesses[0] charges 0 misses; block 2 charges 1
	// miss against the witness scheduled-at-1 above.
	if wMissed.TotalMissed() != 1 {
		t.Errorf("missed witness TotalMissed: want 1, got %d", wMissed.TotalMissed())
	}

	// Filled-slots ring after block 1 (1 filled) + block 2 (1 missed + 1 filled):
	// idx 0=1, idx 1=0, idx 2=1, current index = 3.
	slots := dp.BlockFilledSlots()
	if slots[0] != 1 || slots[1] != 0 || slots[2] != 1 {
		t.Errorf("ring head pattern: want [1 0 1 ...], got [%d %d %d ...]",
			slots[0], slots[1], slots[2])
	}
	if got := dp.BlockFilledSlotsIndex(); got != 3 {
		t.Errorf("ring index: want 3, got %d", got)
	}
}

func TestApplyBlockStatistics_MaintenanceSkipDoesNotShiftMissedWitness(t *testing.T) {
	wstore := newStatWitnessStore()
	dp := state.NewDynamicProperties()
	witnesses := []common.Address{statAddr(1), statAddr(2), statAddr(3)}

	const genesisTs = int64(0)
	const prevTime = int64(6000)
	const blockTime = int64(18000)

	ApplyBlockStatistics(wstore, dp,
		makeBlock(2, blockTime, witnesses[1]), prevTime, witnesses, genesisTs, true)

	wMissed := wstore.GetWitness(witnesses[0])
	if wMissed == nil {
		t.Fatalf("expected witness[0] to be charged for the skipped post-maintenance slot")
	}
	if wMissed.TotalMissed() != 1 {
		t.Fatalf("witness[0] TotalMissed: got %d, want 1", wMissed.TotalMissed())
	}
	for _, addr := range witnesses[1:] {
		if w := wstore.GetWitness(addr); w != nil && w.TotalMissed() != 0 {
			t.Fatalf("witness %s TotalMissed: got %d, want 0", addr.Hex(), w.TotalMissed())
		}
	}
}

func TestApplyBlockStatistics_Block1Skip(t *testing.T) {
	wstore := newStatWitnessStore()
	dp := state.NewDynamicProperties()
	witnesses := []common.Address{statAddr(1), statAddr(2), statAddr(3)}
	producer := witnesses[0]

	const genesisTs = int64(1_000_000)
	// Block 1 with a blockTime FAR from genesis. Java-tron short-circuits
	// missed-slot computation for block 1, so no other witness gets credit
	// for missed slots even though many slots passed.
	farFuture := genesisTs + 50*params.BlockProducedInterval
	block := makeBlock(1, farFuture, producer)

	ApplyBlockStatistics(wstore, dp, block, genesisTs, witnesses, genesisTs, false)

	for i, addr := range witnesses {
		if addr == producer {
			continue
		}
		w := wstore.GetWitness(addr)
		if w == nil {
			continue
		}
		if w.TotalMissed() != 0 {
			t.Errorf("witness %d (non-producer) TotalMissed on block 1: want 0, got %d",
				i, w.TotalMissed())
		}
	}
	// Only the produced bit is set.
	if dp.BlockFilledSlotsIndex() != 1 {
		t.Errorf("ring index after block 1: want 1, got %d", dp.BlockFilledSlotsIndex())
	}
}

func TestApplyBlockStatistics_LoadOrInit(t *testing.T) {
	wstore := newStatWitnessStore()
	dp := state.NewDynamicProperties()
	witnesses := []common.Address{statAddr(1), statAddr(2)}
	// Producer not previously in witness store.
	producer := witnesses[0]
	if wstore.GetWitness(producer) != nil {
		t.Fatal("test setup invariant: producer must not be pre-existing")
	}

	const genesisTs = int64(1_000_000)
	blockTime := genesisTs + params.BlockProducedInterval
	ApplyBlockStatistics(wstore, dp,
		makeBlock(1, blockTime, producer), genesisTs, witnesses, genesisTs, false)

	w := wstore.GetWitness(producer)
	if w == nil {
		t.Fatal("loadOrInit failed to persist producer")
	}
	if w.TotalProduced() != 1 {
		t.Errorf("fresh producer TotalProduced: want 1, got %d", w.TotalProduced())
	}
}
