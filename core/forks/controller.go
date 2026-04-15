package forks

import (
	"math"
	"sync"

	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/tronprotocol/go-tron/core/rawdb"
)

// ForkController tallies per-block SR votes for software fork versions and
// answers Pass(version) against java-tron's activation rules (hardForkTime
// + rate quorum for versions > VERSION_4_0; strict all-slots-upgrade for
// older versions). State is stored in rawdb under the `fv-` prefix.
//
// Callers: producer path feeds it via Update(blockVersion, slot, n); reader
// paths query via Pass. IsActive composes Pass with a DP soft-flag check —
// see IsActive below.
//
// Not thread-safe for concurrent Update — the block-processing pipeline is
// serial. Reads (Pass) are protected by a mutex because audit-loop callers
// (actuators, rpc) may race with producer updates.
type ForkController struct {
	db ethdb.KeyValueStore
	mu sync.RWMutex
}

// NewForkController binds a controller to a KV store.
func NewForkController(db ethdb.KeyValueStore) *ForkController {
	return &ForkController{db: db}
}

// Update records the vote carried by a block: the producing SR's slot is
// marked VoteUpgrade in every known version bitmap with Value <= blockVersion,
// and VoteDowngrade in all higher versions. Mirrors java-tron
// ForkController.update(BlockCapsule).
//
// witnessCount must equal len(activeWitnesses) at this block height; if a
// stored bitmap is nil or has a different length, it's reset to a fresh
// zero slice of witnessCount.
func (fc *ForkController) Update(blockVersion int32, slot, witnessCount int) {
	if slot < 0 || slot >= witnessCount {
		return
	}
	fc.mu.Lock()
	defer fc.mu.Unlock()

	for _, vp := range KnownVersions {
		stats := rawdb.ReadForkStats(fc.db, vp.Value)
		if len(stats) != witnessCount {
			stats = make([]byte, witnessCount)
		} else {
			// Copy so we don't mutate the db-returned slice in place (Pebble reuses).
			fresh := make([]byte, witnessCount)
			copy(fresh, stats)
			stats = fresh
		}
		if vp.Value <= blockVersion {
			stats[slot] = VoteUpgrade
		} else {
			stats[slot] = VoteDowngrade
		}
		rawdb.WriteForkStats(fc.db, vp.Value, stats)
	}
}

// Pass reports whether `version` is activated given `latestBlockTime` (ms).
// For versions > VERSION_4_0, both (a) latestBlockTime >= HardForkTime
// ceil-aligned to the maintenance interval and (b) >= ceil(rate% * length)
// slots voting VoteUpgrade must hold. For older versions every slot in the
// bitmap must read VoteUpgrade.
//
// maintenanceIntervalMs is the chain's maintenance period (genesis default
// 21_600_000). It's passed in rather than read off DP to keep this package
// import-free of core/state and safe to call from anywhere.
func (fc *ForkController) Pass(version int32, latestBlockTime, maintenanceIntervalMs int64) bool {
	vp, ok := lookupVersion(version)
	if !ok {
		return false
	}
	fc.mu.RLock()
	stats := rawdb.ReadForkStats(fc.db, version)
	fc.mu.RUnlock()
	if len(stats) == 0 {
		return false
	}

	if version <= Version4_0 {
		// Legacy: every slot must have voted upgrade. Matches java-tron's
		// ForkController.check(). ENERGY_LIMIT (version 5) uses a block-number
		// gate in java-tron; go-tron doesn't implement that legacy branch.
		for _, b := range stats {
			if b != VoteUpgrade {
				return false
			}
		}
		return true
	}

	if maintenanceIntervalMs <= 0 {
		return false
	}
	// Ceil-align HardForkTime to the next maintenance boundary.
	alignedHardForkTime := ((vp.HardForkTime-1)/maintenanceIntervalMs + 1) * maintenanceIntervalMs
	if latestBlockTime < alignedHardForkTime {
		return false
	}

	upvotes := 0
	for _, b := range stats {
		if b == VoteUpgrade {
			upvotes++
		}
	}
	required := int(math.Ceil(float64(vp.HardForkRate) * float64(len(stats)) / 100.0))
	return upvotes >= required
}

// Stats returns the raw bitmap for a version, primarily for tests and
// diagnostic endpoints. Nil means no update has touched this version yet.
func (fc *ForkController) Stats(version int32) []byte {
	fc.mu.RLock()
	defer fc.mu.RUnlock()
	return rawdb.ReadForkStats(fc.db, version)
}

// Reset clears the bitmap for every known version that has not yet
// activated. Called at maintenance-period boundaries to rebuild vote
// tallies against the new active witness set.
//
// Unlike java-tron's downgrade(), this does NOT try to preserve slot
// votes across witness-set churn — the next round of Updates will
// refill from fresh. witnessCount is the size of the new active set.
func (fc *ForkController) Reset(latestBlockTime, maintenanceIntervalMs int64, witnessCount int) {
	fc.mu.Lock()
	defer fc.mu.Unlock()
	for _, vp := range KnownVersions {
		if fc.passLocked(vp.Value, latestBlockTime, maintenanceIntervalMs) {
			continue
		}
		rawdb.WriteForkStats(fc.db, vp.Value, make([]byte, witnessCount))
	}
}

// passLocked is the mutex-free core of Pass for use from already-locked
// contexts (Reset). Duplication over Pass is intentional — it avoids the
// correctness hazard of sync.RWMutex upgrade.
func (fc *ForkController) passLocked(version int32, latestBlockTime, maintenanceIntervalMs int64) bool {
	vp, ok := lookupVersion(version)
	if !ok {
		return false
	}
	stats := rawdb.ReadForkStats(fc.db, version)
	if len(stats) == 0 {
		return false
	}
	if version <= Version4_0 {
		for _, b := range stats {
			if b != VoteUpgrade {
				return false
			}
		}
		return true
	}
	if maintenanceIntervalMs <= 0 {
		return false
	}
	alignedHardForkTime := ((vp.HardForkTime-1)/maintenanceIntervalMs + 1) * maintenanceIntervalMs
	if latestBlockTime < alignedHardForkTime {
		return false
	}
	upvotes := 0
	for _, b := range stats {
		if b == VoteUpgrade {
			upvotes++
		}
	}
	required := int(math.Ceil(float64(vp.HardForkRate) * float64(len(stats)) / 100.0))
	return upvotes >= required
}
