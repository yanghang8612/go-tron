package downloader

import "testing"

func TestNewDiagnosticsSortsPeerState(t *testing.T) {
	diag := NewDiagnostics(3, 4, 5, []PeerDiagnostics{
		{
			ID:             "peer-b",
			Inflight:       2,
			FetchListLen:   7,
			PendingLen:     1,
			RemainNum:      9,
			ChainRequested: true,
			Done:           false,
		},
		{
			ID:           "peer-a",
			Inflight:     0,
			FetchListLen: 1,
			PendingLen:   0,
			RemainNum:    0,
			Done:         true,
		},
	})

	if diag.BlockBufferLen != 3 || diag.RequestedLen != 4 || diag.RetryListLen != 5 {
		t.Fatalf("counts = %d/%d/%d, want 3/4/5", diag.BlockBufferLen, diag.RequestedLen, diag.RetryListLen)
	}
	want := "peer-a{inflight=0 fetchList=1 pending=0 remain=0 chainRequested=false done=true};" +
		"peer-b{inflight=2 fetchList=7 pending=1 remain=9 chainRequested=true done=false}"
	if diag.PeerState != want {
		t.Fatalf("PeerState = %q, want %q", diag.PeerState, want)
	}
}

func TestNewDiagnosticsOmitsEmptyPeerIDs(t *testing.T) {
	diag := NewDiagnostics(0, 0, 0, []PeerDiagnostics{
		{ID: "", Inflight: 99},
		{ID: "peer", Done: true},
	})
	want := "peer{inflight=0 fetchList=0 pending=0 remain=0 chainRequested=false done=true}"
	if diag.PeerState != want {
		t.Fatalf("PeerState = %q, want %q", diag.PeerState, want)
	}
}

func TestNewDiagnosticsWithoutPeersHasNoPeerState(t *testing.T) {
	diag := NewDiagnostics(1, 2, 3, nil)
	if diag.PeerState != "" {
		t.Fatalf("PeerState = %q, want empty", diag.PeerState)
	}
}
