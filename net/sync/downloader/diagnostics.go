package downloader

import (
	"fmt"
	"sort"
	"strings"
)

// PeerDiagnostics is the per-peer state shown in sync import detail logs.
type PeerDiagnostics struct {
	ID             string
	Inflight       int
	FetchListLen   int
	PendingLen     int
	RemainNum      int64
	ChainRequested bool
	Done           bool
}

// Diagnostics is the lock-free sync-session snapshot consumed by import
// summary logging.
type Diagnostics struct {
	BlockBufferLen           int
	RequestedLen             int
	RetryListLen             int
	PeerState                string
	ImportStageScheduled     int
	ImportStageCompleted     int
	ImportStageComplete      bool
	ImportStageNext          string
	ImportStageBlockedStatus string
}

// NewDiagnostics builds a deterministic diagnostics snapshot. Peer state is
// sorted by peer ID so log output and tests do not depend on map iteration.
func NewDiagnostics(blockBufferLen, requestedLen, retryListLen int, peers []PeerDiagnostics) Diagnostics {
	diag := Diagnostics{
		BlockBufferLen: blockBufferLen,
		RequestedLen:   requestedLen,
		RetryListLen:   retryListLen,
	}
	if len(peers) == 0 {
		return diag
	}
	peers = append([]PeerDiagnostics(nil), peers...)
	sort.Slice(peers, func(i, j int) bool {
		return peers[i].ID < peers[j].ID
	})
	parts := make([]string, 0, len(peers))
	for _, peer := range peers {
		if peer.ID == "" {
			continue
		}
		parts = append(parts, fmt.Sprintf("%s{inflight=%d fetchList=%d pending=%d remain=%d chainRequested=%t done=%t}",
			peer.ID, peer.Inflight, peer.FetchListLen, peer.PendingLen, peer.RemainNum, peer.ChainRequested, peer.Done))
	}
	diag.PeerState = strings.Join(parts, ";")
	return diag
}

// WithImportStagePlan adds downloader-owned import stage planner diagnostics to
// the existing sync-session snapshot.
func (d Diagnostics) WithImportStagePlan(plan ImportStagePlan) Diagnostics {
	stage := plan.Diagnostics()
	d.ImportStageScheduled = stage.Scheduled
	d.ImportStageCompleted = stage.Completed
	d.ImportStageComplete = stage.Complete
	if stage.HasBlocked {
		if stage.NextPhase != "" {
			d.ImportStageNext = string(stage.NextPhase)
		} else if stage.NextStage != "" {
			d.ImportStageNext = string(stage.NextStage)
		}
		d.ImportStageBlockedStatus = stage.BlockedStatus.String()
	}
	return d
}

// HasImportStagePlan reports whether Diagnostics carries import stage planner
// state for the current imported batch.
func (d Diagnostics) HasImportStagePlan() bool {
	return d.ImportStageScheduled > 0
}
