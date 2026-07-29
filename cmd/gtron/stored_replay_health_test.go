package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/tronprotocol/go-tron/core/types"
	corepb "github.com/tronprotocol/go-tron/proto/core"
)

type fixedStoredReplayHead struct {
	block *types.Block
}

func (h fixedStoredReplayHead) CurrentBlock() *types.Block { return h.block }

type recordingCommitmentCacheSizer struct {
	sizes []int
}

func (s *recordingCommitmentCacheSizer) SetCommitmentBranchCacheSize(sizeBytes int) {
	s.sizes = append(s.sizes, sizeBytes)
}

func TestStoredReplayCommitmentCacheIsTemporary(t *testing.T) {
	cache := new(recordingCommitmentCacheSizer)
	restore, enlarged := temporarilyEnlargeStoredReplayCommitmentCache(cache, 128)
	if !enlarged {
		t.Fatal("replay cache was not enlarged")
	}
	restore()

	want := []int{
		round141StoredReplayCommitmentCacheMiB * 1024 * 1024,
		128 * 1024 * 1024,
	}
	if len(cache.sizes) != len(want) {
		t.Fatalf("cache resize calls = %v, want %v", cache.sizes, want)
	}
	for i := range want {
		if cache.sizes[i] != want[i] {
			t.Fatalf("cache resize calls = %v, want %v", cache.sizes, want)
		}
	}
}

func TestStoredReplayCommitmentCacheKeepsLargerOperatorBudget(t *testing.T) {
	cache := new(recordingCommitmentCacheSizer)
	restore, enlarged := temporarilyEnlargeStoredReplayCommitmentCache(cache, round141StoredReplayCommitmentCacheMiB)
	if enlarged {
		t.Fatal("operator cache at replay budget was unexpectedly changed")
	}
	restore()
	if len(cache.sizes) != 0 {
		t.Fatalf("cache resize calls = %v, want none", cache.sizes)
	}
}

func TestStoredReplayHealthHandlerReportsCurrentHead(t *testing.T) {
	block := types.NewBlockFromPB(&corepb.Block{BlockHeader: &corepb.BlockHeader{
		RawData: &corepb.BlockHeaderRaw{Number: 42, Timestamp: 1234},
	}})
	recorder := httptest.NewRecorder()
	storedReplayHealthHandler(fixedStoredReplayHead{block: block}).ServeHTTP(
		recorder,
		httptest.NewRequest(http.MethodGet, "/wallet/getnowblock", nil),
	)
	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200 (body=%q)", recorder.Code, recorder.Body.String())
	}
	var response struct {
		BlockID     string `json:"blockID"`
		BlockHeader struct {
			RawData struct {
				Number uint64 `json:"number"`
			} `json:"raw_data"`
		} `json:"block_header"`
	}
	if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil {
		t.Fatalf("decode response: %v (body=%q)", err, recorder.Body.String())
	}
	if response.BlockID != block.Hash().Hex() || response.BlockHeader.RawData.Number != 42 {
		t.Fatalf("response = %+v, want block %s at 42", response, block.Hash().Hex())
	}
}

func TestStoredReplayHealthHandlerRejectsMissingHead(t *testing.T) {
	recorder := httptest.NewRecorder()
	storedReplayHealthHandler(fixedStoredReplayHead{}).ServeHTTP(
		recorder,
		httptest.NewRequest(http.MethodGet, "/wallet/getnowblock", nil),
	)
	if recorder.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503", recorder.Code)
	}
}
