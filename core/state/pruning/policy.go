package pruning

import (
	"errors"
	"fmt"
)

type Mode string

const (
	ModeArchive Mode = "archive"
	ModeFull    Mode = "full"
	ModeBlocks  Mode = "blocks"
	ModeMinimal Mode = "minimal"
	ModeSnap    Mode = "snap"
)

type Policy struct {
	Mode Mode

	// HistoryWindow is the number of recent blocks whose domain history must be
	// retained in the hot database. In archive mode a zero value keeps the
	// legacy no-prune behaviour; a positive value permits pruning only after an
	// immutable, verified cold history copy covers the same rows. Archive's
	// logical history retention is therefore still unbounded.
	HistoryWindow uint64

	// ReorgWindow is the minimum recent range that must retain enough latest
	// and change-set data to survive local fork switches without replaying from
	// genesis.
	ReorgWindow uint64
}

func ArchivePolicy() Policy {
	return Policy{Mode: ModeArchive}
}

// ArchiveColdPolicy retains complete logical history while bounding duplicate
// hot history to historyWindow blocks. Rows outside the window may be removed
// only after the Worker verifies matching immutable snapshot coverage.
func ArchiveColdPolicy(historyWindow, reorgWindow uint64) Policy {
	return Policy{Mode: ModeArchive, HistoryWindow: historyWindow, ReorgWindow: reorgWindow}
}

func FullPolicy(historyWindow, reorgWindow uint64) Policy {
	return Policy{Mode: ModeFull, HistoryWindow: historyWindow, ReorgWindow: reorgWindow}
}

func BlocksPolicy(historyWindow, reorgWindow uint64) Policy {
	return Policy{Mode: ModeBlocks, HistoryWindow: historyWindow, ReorgWindow: reorgWindow}
}

func MinimalPolicy(historyWindow, reorgWindow uint64) Policy {
	return Policy{Mode: ModeMinimal, HistoryWindow: historyWindow, ReorgWindow: reorgWindow}
}

func SnapPolicy(historyWindow, reorgWindow uint64) Policy {
	return Policy{Mode: ModeSnap, HistoryWindow: historyWindow, ReorgWindow: reorgWindow}
}

func (p Policy) Validate() error {
	switch p.Mode {
	case ModeArchive:
		// Preserve ArchivePolicy() as the explicit legacy/no-prune policy used by
		// offline checks and callers that have no cold lifecycle configured.
		if p.HistoryWindow == 0 {
			return nil
		}
		if p.ReorgWindow == 0 {
			return errors.New("pruning: archive cold history reorg window must be positive")
		}
		if p.HistoryWindow < p.ReorgWindow {
			return fmt.Errorf("pruning: archive hot history window %d is smaller than reorg window %d", p.HistoryWindow, p.ReorgWindow)
		}
		return nil
	case ModeFull, ModeBlocks, ModeMinimal, ModeSnap:
		if p.HistoryWindow == 0 {
			return errors.New("pruning: history window must be positive outside archive mode")
		}
		if p.ReorgWindow == 0 {
			return errors.New("pruning: reorg window must be positive outside archive mode")
		}
		if p.HistoryWindow < p.ReorgWindow {
			return fmt.Errorf("pruning: history window %d is smaller than reorg window %d", p.HistoryWindow, p.ReorgWindow)
		}
		return nil
	default:
		return fmt.Errorf("pruning: unknown mode %q", p.Mode)
	}
}

func (p Policy) RetainHistory(blockNum, headNum uint64) bool {
	if p.Mode == ModeArchive {
		return true
	}
	if blockNum > headNum {
		return true
	}
	return headNum-blockNum < p.HistoryWindow
}

// RetainHotHistory reports whether a history row must remain in the mutable
// database. It differs from RetainHistory only for archive cold policies:
// archive retains the logical row forever, but the verified immutable copy may
// become its sole physical representation outside the hot window.
func (p Policy) RetainHotHistory(blockNum, headNum uint64) bool {
	if p.Mode == ModeArchive && p.HistoryWindow == 0 {
		return true
	}
	if blockNum > headNum {
		return true
	}
	return headNum-blockNum < p.HistoryWindow
}

func (p Policy) RetainReorgData(blockNum, headNum uint64) bool {
	if p.Mode == ModeArchive {
		return true
	}
	if blockNum > headNum {
		return true
	}
	return headNum-blockNum < p.ReorgWindow
}

func (p Policy) RetainSnapshot(txNum, visibleFrom, visibleTo uint64) bool {
	if p.Mode == ModeArchive {
		return true
	}
	if txNum < visibleFrom || txNum > visibleTo {
		return false
	}
	switch p.Mode {
	case ModeFull:
		return false
	case ModeBlocks:
		return false
	case ModeMinimal:
		return false
	case ModeSnap:
		return true
	default:
		return false
	}
}
