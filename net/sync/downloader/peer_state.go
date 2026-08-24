package downloader

import (
	"math"
	"time"

	"github.com/tronprotocol/go-tron/core/types"
)

// FetchWindow is the block-number range a peer can serve from its latest
// CHAIN_INVENTORY response.
type FetchWindow struct {
	Min uint64
	Max uint64
}

// BoundInventoryRemain normalizes the peer-reported remaining count to the
// already-clamped target projection. Negative values cannot keep a drained peer
// artificially active, and oversized values cannot overflow progress sums.
func BoundInventoryRemain(remainNum int64, inventoryTip, observed uint64) int64 {
	if remainNum <= 0 || observed <= inventoryTip {
		return 0
	}
	remaining := observed - inventoryTip
	if remaining > math.MaxInt64 {
		return math.MaxInt64
	}
	return int64(remaining)
}

// InventoryTargetUpdate is the downloader target/window state derived from one
// CHAIN_INVENTORY response.
type InventoryTargetUpdate struct {
	Window      FetchWindow
	Target      uint64
	StageTarget uint64
	Observed    uint64
	Advanced    bool
}

// DrainedPeerAction is the next action for a peer whose local fetch queue has
// no eligible block IDs to request.
type DrainedPeerAction uint8

const (
	DrainedPeerIdle DrainedPeerAction = iota
	DrainedPeerWaitLocalHead
	DrainedPeerRequestInventory
)

// PeerFetchAction is the next scheduler action for an otherwise eligible peer.
type PeerFetchAction uint8

const (
	PeerFetchIdle PeerFetchAction = iota
	PeerFetchWaitLocalHead
	PeerFetchRequestInventory
	PeerFetchDelay
	PeerFetchSend
)

// ReadyPeerFetchInput contains the side-effect-free state needed after retry
// assignment and peer-local fetch-list filtering have produced a candidate
// batch.
type ReadyPeerFetchInput struct {
	BatchLen     int
	FetchWait    time.Duration
	Done         bool
	InventoryTip uint64
	CurrentHead  uint64
}

// PeerFetchPlan tells the service how to advance an eligible peer without
// owning timers, pending maps, or network sends.
type PeerFetchPlan struct {
	Action PeerFetchAction
	Wait   time.Duration
}

// NewFetchWindow derives the serviceable range from an inventory tip. A zero
// tip means the peer has not yet advertised a usable inventory window.
func NewFetchWindow(inventoryTip uint64, inventoryLimit int) FetchWindow {
	if inventoryTip == 0 {
		return FetchWindow{}
	}
	window := FetchWindow{Max: inventoryTip}
	if inventoryLimit > 0 {
		span := uint64(inventoryLimit) * 2
		if inventoryTip > span {
			window.Min = inventoryTip - span
		}
	}
	return window
}

// ObserveInventoryTarget derives the peer fetch window and global target head
// from one CHAIN_INVENTORY tail. remainNum follows java-tron's payload:
// positive values extend the advertised target beyond the last returned ID.
func ObserveInventoryTarget(currentTarget, inventoryTip uint64, remainNum int64, inventoryLimit int, maxTarget ...uint64) InventoryTargetUpdate {
	if inventoryTip == 0 {
		return InventoryTargetUpdate{Target: currentTarget}
	}
	effectiveTip := inventoryTip
	if len(maxTarget) > 0 && maxTarget[0] > 0 && effectiveTip > maxTarget[0] {
		effectiveTip = maxTarget[0]
	}
	observed := effectiveTip
	if remainNum > 0 {
		remaining := uint64(remainNum)
		if remaining > math.MaxUint64-observed {
			observed = math.MaxUint64
		} else {
			observed += remaining
		}
	}
	target := currentTarget
	if len(maxTarget) > 0 && maxTarget[0] > 0 {
		limit := maxTarget[0]
		if observed > limit {
			observed = limit
		}
		if target > limit {
			target = limit
		}
	}
	advanced := false
	if observed > target {
		target = observed
		advanced = true
	}
	return InventoryTargetUpdate{
		Window: NewFetchWindow(effectiveTip, inventoryLimit),
		Target: target,
		// Persist only the highest explicit block ID, not the peer-controlled
		// remainNum estimate. The service applies this watermark monotonically.
		// A restart therefore cannot inherit a fabricated multi-year backlog.
		StageTarget: effectiveTip,
		Observed:    observed,
		Advanced:    advanced,
	}
}

// ShouldMarkInventoryDone mirrors java-tron's "one id and no remain" completion
// signal once no new block IDs were queued from the inventory response.
func ShouldMarkInventoryDone(inventoryIDs, queued int, remainNum int64) bool {
	return inventoryIDs == 0 || (queued == 0 && inventoryIDs == 1 && remainNum == 0)
}

// PlanDrainedPeerAction decides whether an idle peer should wait for local
// import to reach its last advertised inventory tip or request a fresh
// CHAIN_INVENTORY window.
func PlanDrainedPeerAction(done bool, inventoryTip, currentHead uint64) DrainedPeerAction {
	if done {
		return DrainedPeerIdle
	}
	if inventoryTip > currentHead {
		return DrainedPeerWaitLocalHead
	}
	return DrainedPeerRequestInventory
}

// PlanReadyPeerFetch decides what to do with an eligible peer after the
// downloader queue has been drained for a candidate batch.
func PlanReadyPeerFetch(in ReadyPeerFetchInput) PeerFetchPlan {
	if in.BatchLen == 0 {
		switch PlanDrainedPeerAction(in.Done, in.InventoryTip, in.CurrentHead) {
		case DrainedPeerWaitLocalHead:
			return PeerFetchPlan{Action: PeerFetchWaitLocalHead}
		case DrainedPeerRequestInventory:
			return PeerFetchPlan{Action: PeerFetchRequestInventory}
		default:
			return PeerFetchPlan{Action: PeerFetchIdle}
		}
	}
	if in.FetchWait > 0 {
		return PeerFetchPlan{Action: PeerFetchDelay, Wait: in.FetchWait}
	}
	return PeerFetchPlan{Action: PeerFetchSend}
}

// Contains reports whether bid falls inside the peer's advertised fetch range.
func (w FetchWindow) Contains(bid types.BlockID) bool {
	if w.Max == 0 {
		return false
	}
	return bid.Num >= w.Min && bid.Num <= w.Max
}
