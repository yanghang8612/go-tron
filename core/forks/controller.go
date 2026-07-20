package forks

import (
	"math"
	"sync"

	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/state"
)

// ForkController tallies per-block SR votes for software fork versions and
// answers Pass(version) against java-tron's activation rules (hardForkTime
// + rate quorum for versions > VERSION_4_0; strict all-slots-upgrade for
// older versions). Production block execution binds the controller to StateDB's
// rooted SystemForkVote domain; the rawdb-backed constructor remains for tests
// and compatibility readers that do not have a StateDB handle.
//
// The block pipeline feeds it via UpdateJava; Update remains a low-level
// bitmap helper used by focused tests. Reader paths query via Pass. IsActive
// composes Pass with a DP soft-flag check —
// see IsActive below.
//
// Not thread-safe for concurrent Update — the block-processing pipeline is
// serial. Reads (Pass) are protected by a mutex because audit-loop callers
// (actuators, rpc) may race with producer updates.
type ForkController struct {
	store ForkStatsStore
	mu    sync.RWMutex
}

type keyValueReadWriter interface {
	ethdb.KeyValueReader
	ethdb.KeyValueWriter
}

type ForkStatsReader interface {
	ReadForkStats(version int32) []byte
}

type ForkStatsBatchReader interface {
	ReadForkStatsBatch(versions []int32) map[int32][]byte
}

type ForkStatsStore interface {
	ForkStatsReader
	WriteForkStats(version int32, stats []byte)
}

type rawDBForkStatsStore struct {
	db keyValueReadWriter
}

func (s rawDBForkStatsStore) ReadForkStats(version int32) []byte {
	return rawdb.ReadForkStats(s.db, version)
}

func (s rawDBForkStatsStore) WriteForkStats(version int32, stats []byte) {
	rawdb.WriteForkStats(s.db, version, stats)
}

// NewForkController binds a controller to a KV store.
func NewForkController(db keyValueReadWriter) *ForkController {
	return NewForkControllerFromStore(rawDBForkStatsStore{db: db})
}

// NewForkControllerFromStore binds a controller to a typed fork-stats store.
func NewForkControllerFromStore(store ForkStatsStore) *ForkController {
	return &ForkController{store: store}
}

// NewForkControllerFromState binds a controller to StateDB's rooted
// SystemForkVote domain.
func NewForkControllerFromState(statedb *state.StateDB) *ForkController {
	return NewForkControllerFromStore(statedb)
}

var knownVersionValues = func() []int32 {
	values := make([]int32, len(KnownVersions))
	for i, vp := range KnownVersions {
		values[i] = vp.Value
	}
	return values
}()

// Update records the vote carried by a block: the producing SR's slot is
// marked VoteUpgrade in every known version bitmap with Value <= blockVersion,
// and VoteDowngrade in all higher versions. Mirrors java-tron
// ForkController.update(BlockCapsule).
//
// witnessCount must equal len(activeWitnesses) at this block height; if a
// stored bitmap is nil or has a different length, it's reset to a fresh
// zero slice of witnessCount.
func (fc *ForkController) Update(blockVersion int32, slot, witnessCount int) {
	if blockVersion < KnownVersions[0].Value {
		return
	}
	if slot < 0 || slot >= witnessCount {
		return
	}
	fc.mu.Lock()
	defer fc.mu.Unlock()

	var batchStats map[int32][]byte
	if br, ok := fc.store.(ForkStatsBatchReader); ok {
		batchStats = br.ReadForkStatsBatch(knownVersionValues)
	}
	for _, vp := range KnownVersions {
		var stats []byte
		if batchStats != nil {
			stats = batchStats[vp.Value]
		} else {
			stats = fc.store.ReadForkStats(vp.Value)
		}
		changed := false
		if len(stats) != witnessCount {
			stats = make([]byte, witnessCount)
			changed = true
		} else {
			// Copy so we don't mutate the db-returned slice in place (Pebble reuses).
			fresh := make([]byte, witnessCount)
			copy(fresh, stats)
			stats = fresh
		}
		want := VoteDowngrade
		if vp.Value <= blockVersion {
			want = VoteUpgrade
		}
		if stats[slot] != want {
			stats[slot] = want
			changed = true
		}
		if changed {
			fc.store.WriteForkStats(vp.Value, stats)
		}
	}
}

// UpdateJava applies java-tron's ForkController.update state machine and
// returns the VERSION_NUMBER value the caller must persist. In particular,
// java updates only the block's current version bitmap; lower versions are
// filled only after the current version already passed on a previous block,
// and higher versions are downgraded only when their bitmap already exists.
//
// latestBlockTime is the current block timestamp (java calls update after
// updateDynamicProperties). latestVersion is DynamicProperties.VERSION_NUMBER.
func (fc *ForkController) UpdateJava(blockVersion int32, slot, witnessCount int, latestVersion int32, latestBlockTime, maintenanceIntervalMs int64) (int32, bool) {
	if blockVersion < KnownVersions[0].Value || latestVersion >= blockVersion {
		return latestVersion, false
	}
	if slot < 0 || slot >= witnessCount {
		return latestVersion, false
	}
	fc.mu.Lock()
	defer fc.mu.Unlock()

	// downgrade(version, slot): do not materialize a missing future bitmap.
	for _, vp := range KnownVersions {
		if vp.Value <= blockVersion || fc.passLocked(vp.Value, latestBlockTime, maintenanceIntervalMs) {
			continue
		}
		stats := fc.store.ReadForkStats(vp.Value)
		if stats == nil || slot >= len(stats) || stats[slot] == VoteDowngrade {
			continue
		}
		stats = append([]byte(nil), stats...)
		stats[slot] = VoteDowngrade
		fc.store.WriteForkStats(vp.Value, stats)
	}

	stats := fc.store.ReadForkStats(blockVersion)
	if len(stats) != witnessCount {
		stats = make([]byte, witnessCount)
	} else {
		stats = append([]byte(nil), stats...)
	}
	// Java evaluates pass(version) before recording this block's vote. The
	// block after quorum is therefore the one that advances VERSION_NUMBER.
	if fc.passLocked(blockVersion, latestBlockTime, maintenanceIntervalMs) {
		for _, vp := range KnownVersions {
			if vp.Value >= blockVersion || fc.passLocked(vp.Value, latestBlockTime, maintenanceIntervalMs) {
				continue
			}
			lower := fc.store.ReadForkStats(vp.Value)
			if len(lower) == 0 {
				lower = make([]byte, len(stats))
			} else {
				lower = append([]byte(nil), lower...)
			}
			changed := false
			for i := range lower {
				if lower[i] != VoteUpgrade {
					lower[i] = VoteUpgrade
					changed = true
				}
			}
			if changed {
				fc.store.WriteForkStats(vp.Value, lower)
			}
		}
		return blockVersion, true
	}

	if stats[slot] != VoteUpgrade {
		stats[slot] = VoteUpgrade
		fc.store.WriteForkStats(blockVersion, stats)
	}
	return latestVersion, false
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
	stats := fc.store.ReadForkStats(version)
	fc.mu.RUnlock()
	return passFromStats(stats, vp, latestBlockTime, maintenanceIntervalMs)
}

// IsActive answers the "feature is on" question for a governance AllowFlag.
// Two checks: the DP soft flag (allow_X) must be nonzero AND — if the
// flag has a required version — that version must have passed the
// hardForkTime + rate quorum gate.
//
// This is the call the actuator / VM path should make going forward.
// During Task 5 migration, existing forks.IsActive(flag, blockNum, dp)
// sites are moved to fc.IsActive(flag, dp, latestBlockTime).
func (fc *ForkController) IsActive(flag AllowFlag, dp *state.DynamicProperties, latestBlockTime int64) bool {
	if dp == nil {
		return false
	}
	key, ok := dynKey[flag]
	if !ok {
		return false
	}
	if v, _ := dp.Get(key); v == 0 {
		return false
	}
	req, hasVersionGate := RequiredVersion(flag)
	if !hasVersionGate {
		return true
	}
	return fc.Pass(req, latestBlockTime, dp.MaintenanceTimeInterval())
}

// Stats returns the raw bitmap for a version, primarily for tests and
// diagnostic endpoints. Nil means no update has touched this version yet.
func (fc *ForkController) Stats(version int32) []byte {
	fc.mu.RLock()
	defer fc.mu.RUnlock()
	return fc.store.ReadForkStats(version)
}

// Reset clears each existing bitmap that has not yet activated. Called at
// maintenance-period boundaries to rebuild vote tallies against the new active
// witness set. Missing versions stay missing, matching java-tron.
//
// Unlike java-tron's downgrade(), this does NOT try to preserve slot
// votes across witness-set churn — the next round of Updates will
// refill from fresh. witnessCount is the size of the new active set.
func (fc *ForkController) Reset(latestBlockTime, maintenanceIntervalMs int64, witnessCount int) {
	fc.mu.Lock()
	defer fc.mu.Unlock()
	for _, vp := range KnownVersions {
		if fc.store.ReadForkStats(vp.Value) == nil {
			continue
		}
		if fc.passLocked(vp.Value, latestBlockTime, maintenanceIntervalMs) {
			continue
		}
		fc.store.WriteForkStats(vp.Value, make([]byte, witnessCount))
	}
}

// passLocked is the mutex-free core of Pass for use from already-locked
// contexts (Reset).
func (fc *ForkController) passLocked(version int32, latestBlockTime, maintenanceIntervalMs int64) bool {
	vp, ok := lookupVersion(version)
	if !ok {
		return false
	}
	stats := fc.store.ReadForkStats(version)
	return passFromStats(stats, vp, latestBlockTime, maintenanceIntervalMs)
}

// passFromStats is the pure activation check shared by ForkController.Pass,
// passLocked, and the stateless PassVersion helper.
func passFromStats(stats []byte, vp VersionParam, latestBlockTime, maintenanceIntervalMs int64) bool {
	if len(stats) == 0 {
		return false
	}
	if vp.Value <= Version4_0 {
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
