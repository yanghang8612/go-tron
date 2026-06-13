package downloader

import "testing"

func TestClassifyRetryCandidate(t *testing.T) {
	tests := []struct {
		name  string
		facts RetryCandidateFacts
		want  RetryDecision
	}{
		{
			name:  "known drops stale retry",
			facts: RetryCandidateFacts{KnownOrRequested: true},
			want:  RetryDrop,
		},
		{
			name:  "outside window stays retryable",
			facts: RetryCandidateFacts{InWindow: false},
			want:  RetryKeep,
		},
		{
			name:  "same peer already requested stays retryable",
			facts: RetryCandidateFacts{InWindow: true, PeerRequested: true},
			want:  RetryKeep,
		},
		{
			name:  "path conflict drops fork retry",
			facts: RetryCandidateFacts{InWindow: true, ReservedPath: false},
			want:  RetryDrop,
		},
		{
			name:  "eligible retry assigns to peer",
			facts: RetryCandidateFacts{InWindow: true, ReservedPath: true},
			want:  RetryAssign,
		},
	}

	for _, tt := range tests {
		if got := ClassifyRetryCandidate(tt.facts); got != tt.want {
			t.Fatalf("%s: decision = %v, want %v", tt.name, got, tt.want)
		}
	}
}

func TestAcceptFetchCandidate(t *testing.T) {
	tests := []struct {
		name         string
		facts        FetchCandidateFacts
		wantDecision FetchCandidateDecision
		wantAccept   bool
	}{
		{name: "known rejected", facts: FetchCandidateFacts{KnownOrRequested: true, ReservedPath: true}, wantDecision: FetchCandidateKnownOrRequested},
		{name: "path conflict rejected", facts: FetchCandidateFacts{}, wantDecision: FetchCandidatePathConflict},
		{name: "same peer duplicate rejected", facts: FetchCandidateFacts{ReservedPath: true, PeerRequested: true}, wantDecision: FetchCandidatePeerDuplicate},
		{name: "eligible accepted", facts: FetchCandidateFacts{ReservedPath: true}, wantDecision: FetchCandidateAccepted, wantAccept: true},
	}

	for _, tt := range tests {
		if got := ClassifyFetchCandidate(tt.facts); got != tt.wantDecision {
			t.Fatalf("%s: decision = %v, want %v", tt.name, got, tt.wantDecision)
		}
		if got := AcceptFetchCandidate(tt.facts); got != tt.wantAccept {
			t.Fatalf("%s: accept = %v, want %v", tt.name, got, tt.wantAccept)
		}
	}
}

func TestAcceptInventoryCandidate(t *testing.T) {
	tests := []struct {
		name  string
		facts InventoryCandidateFacts
		want  bool
	}{
		{name: "known rejected", facts: InventoryCandidateFacts{KnownOrRequested: true, ReservedPath: true}, want: false},
		{name: "same peer duplicate rejected", facts: InventoryCandidateFacts{PeerRequested: true, ReservedPath: true}, want: false},
		{name: "path conflict rejected", facts: InventoryCandidateFacts{}, want: false},
		{name: "eligible accepted", facts: InventoryCandidateFacts{ReservedPath: true}, want: true},
	}

	for _, tt := range tests {
		if got := AcceptInventoryCandidate(tt.facts); got != tt.want {
			t.Fatalf("%s: accept = %v, want %v", tt.name, got, tt.want)
		}
	}
}
