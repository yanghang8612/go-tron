package downloader

import "github.com/tronprotocol/go-tron/core/types"

// FetchWindow is the block-number range a peer can serve from its latest
// CHAIN_INVENTORY response.
type FetchWindow struct {
	Min uint64
	Max uint64
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

// Contains reports whether bid falls inside the peer's advertised fetch range.
func (w FetchWindow) Contains(bid types.BlockID) bool {
	if w.Max == 0 {
		return false
	}
	return bid.Num >= w.Min && bid.Num <= w.Max
}
