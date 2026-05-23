package pruning

import (
	"errors"
	"fmt"
)

type Mode string

const (
	ModeArchive Mode = "archive"
	ModeFull    Mode = "full"
	ModeSnap    Mode = "snap"
)

type Policy struct {
	Mode Mode

	// HistoryWindow is the number of recent blocks whose domain history must be
	// retained. Archive mode ignores it and keeps all history.
	HistoryWindow uint64

	// ReorgWindow is the minimum recent range that must retain enough latest
	// and change-set data to survive local fork switches without replaying from
	// genesis.
	ReorgWindow uint64
}

func ArchivePolicy() Policy {
	return Policy{Mode: ModeArchive}
}

func FullPolicy(historyWindow, reorgWindow uint64) Policy {
	return Policy{Mode: ModeFull, HistoryWindow: historyWindow, ReorgWindow: reorgWindow}
}

func SnapPolicy(historyWindow, reorgWindow uint64) Policy {
	return Policy{Mode: ModeSnap, HistoryWindow: historyWindow, ReorgWindow: reorgWindow}
}

func (p Policy) Validate() error {
	switch p.Mode {
	case ModeArchive:
		return nil
	case ModeFull, ModeSnap:
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
	case ModeSnap:
		return true
	default:
		return false
	}
}
