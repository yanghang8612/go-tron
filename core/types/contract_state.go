package types

import (
	"math"

	"github.com/tronprotocol/go-tron/internal/math/strictmath"
	contractpb "github.com/tronprotocol/go-tron/proto/core/contract"
	"google.golang.org/protobuf/proto"
)

// DynamicEnergyFactorDecimal mirrors java-tron Constant.DYNAMIC_ENERGY_FACTOR_DECIMAL:
// the precision base for fixed-point factor storage. A stored factor of 0
// represents 1.0× (the decimal value is added on read to yield the
// multiplier). Stored 5_000 → 1.5× multiplier, stored 10_000 → 2.0×.
const DynamicEnergyFactorDecimal int64 = 10_000

// dynamicEnergyDecreaseDivision mirrors Constant.DYNAMIC_ENERGY_DECREASE_DIVISION:
// the rate-down divisor (4) keeps decreases at 1/4 the speed of increases.
const dynamicEnergyDecreaseDivision int64 = 4

// ContractState wraps the per-contract dynamic-energy state (cumulative
// base energy usage for the current cycle, the adaptive energy factor,
// and the last cycle it was updated in).
//
// Mirrors java-tron ContractStateCapsule. The factor is stored as an
// offset from DynamicEnergyFactorDecimal — readers add the decimal on
// fetch to recover the actual multiplier.
type ContractState struct {
	pb *contractpb.ContractState
}

// NewContractState creates a fresh capsule anchored at the given cycle.
// Mirrors java-tron's ContractStateCapsule constructor that takes only a
// cycle number (new contracts skip any back-update math).
func NewContractState(cycle int64) *ContractState {
	return &ContractState{pb: &contractpb.ContractState{UpdateCycle: cycle}}
}

// NewContractStateFromBytes decodes a protobuf-serialized ContractState.
func NewContractStateFromBytes(data []byte) (*ContractState, error) {
	pb := &contractpb.ContractState{}
	if err := proto.Unmarshal(data, pb); err != nil {
		return nil, err
	}
	return &ContractState{pb: pb}, nil
}

// Bytes serializes the capsule for DB storage.
func (c *ContractState) Bytes() ([]byte, error) {
	return proto.Marshal(c.pb)
}

func (c *ContractState) Proto() *contractpb.ContractState { return c.pb }

func (c *ContractState) EnergyUsage() int64    { return c.pb.EnergyUsage }
func (c *ContractState) EnergyFactor() int64   { return c.pb.EnergyFactor }
func (c *ContractState) UpdateCycle() int64    { return c.pb.UpdateCycle }
func (c *ContractState) SetEnergyFactor(v int64) { c.pb.EnergyFactor = v }
func (c *ContractState) SetUpdateCycle(v int64)  { c.pb.UpdateCycle = v }
func (c *ContractState) AddEnergyUsage(v int64)  { c.pb.EnergyUsage += v }

// Reset wipes usage and factor and anchors the state at latestCycle.
// Mirrors java-tron ContractStateCapsule.reset.
func (c *ContractState) Reset(latestCycle int64) {
	c.pb.EnergyUsage = 0
	c.pb.EnergyFactor = 0
	c.pb.UpdateCycle = latestCycle
}

// CatchUpToCycle brings the factor forward from its last update cycle to
// newCycle. Mirrors java-tron ContractStateCapsule.catchUpToCycle.
//
// Steps:
//  1. If we're still in the same cycle, nothing to do.
//  2. If the stored cycle is zero (uninitialized) or ahead of newCycle,
//     reset the state to newCycle.
//  3. If energy usage in the last cycle exceeded threshold, apply one
//     increase step: factor = min(maxFactor, (factor+precision) × (1 +
//     increaseFactor/precision) − precision). Bump lastCycle by 1 so the
//     decay math below counts from the post-increase cycle.
//  4. For any remaining cycles with no high-usage event, apply a compound
//     decay: factor = max(0, (factor+precision) × (1 −
//     increaseFactor/decreaseDivision/precision)^cycleCount − precision).
//
// Returns true when the state was mutated (caller must persist).
//
// `useStrictMath` routes the decay pow through `strictmath.Pow` (java-tron
// `StrictMath.pow` parity) after proposal #87 activates. See java-tron
// `ContractStateCapsule.catchUpToCycle`.
func (c *ContractState) CatchUpToCycle(newCycle, threshold, increaseFactor, maxFactor int64, useStrictMath bool) bool {
	lastCycle := c.pb.UpdateCycle

	if lastCycle == newCycle {
		return false
	}
	if lastCycle > newCycle || lastCycle == 0 {
		c.Reset(newCycle)
		return true
	}

	const precision = DynamicEnergyFactorDecimal

	// Increase phase: if the contract went over threshold last cycle,
	// bump the factor once — mirrors java-tron's single-step increase
	// guarded on `getEnergyUsage() > threshold`.
	if c.pb.EnergyUsage > threshold {
		lastCycle++
		increasePercent := 1.0 + float64(increaseFactor)/float64(precision)
		raw := int64(float64(c.pb.EnergyFactor+precision)*increasePercent) - precision
		if raw > maxFactor {
			raw = maxFactor
		}
		c.pb.UpdateCycle = lastCycle
		c.pb.EnergyFactor = raw
		c.pb.EnergyUsage = 0 // zeroed by the newBuilder() in java-tron
	}

	cycleCount := newCycle - lastCycle
	if cycleCount <= 0 {
		return true
	}

	// Decrease phase: compound decay over cycleCount quiet cycles.
	// decreasePercent = (1 − increaseFactor / decreaseDivision / precision)^cycleCount
	base := 1.0 - float64(increaseFactor)/float64(dynamicEnergyDecreaseDivision)/float64(precision)
	var decreasePercent float64
	if useStrictMath {
		decreasePercent = strictmath.Pow(base, float64(cycleCount))
	} else {
		decreasePercent = math.Pow(base, float64(cycleCount))
	}
	raw := int64(float64(c.pb.EnergyFactor+precision)*decreasePercent) - precision
	if raw < 0 {
		raw = 0
	}
	c.pb.UpdateCycle = newCycle
	c.pb.EnergyFactor = raw
	c.pb.EnergyUsage = 0
	return true
}
