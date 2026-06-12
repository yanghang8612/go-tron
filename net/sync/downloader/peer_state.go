package downloader

import "github.com/tronprotocol/go-tron/core/types"

// FetchWindow is the block-number range a peer can serve from its latest
// CHAIN_INVENTORY response.
type FetchWindow struct {
	Min uint64
	Max uint64
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
func ObserveInventoryTarget(currentTarget, inventoryTip uint64, remainNum int64, inventoryLimit int) InventoryTargetUpdate {
	if inventoryTip == 0 {
		return InventoryTargetUpdate{Target: currentTarget}
	}
	observed := inventoryTip
	if remainNum > 0 {
		observed += uint64(remainNum)
	}
	target := currentTarget
	advanced := false
	if observed > target {
		target = observed
		advanced = true
	}
	return InventoryTargetUpdate{
		Window:      NewFetchWindow(inventoryTip, inventoryLimit),
		Target:      target,
		StageTarget: target,
		Observed:    observed,
		Advanced:    advanced,
	}
}

// Contains reports whether bid falls inside the peer's advertised fetch range.
func (w FetchWindow) Contains(bid types.BlockID) bool {
	if w.Max == 0 {
		return false
	}
	return bid.Num >= w.Min && bid.Num <= w.Max
}
