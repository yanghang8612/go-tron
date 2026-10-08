package main

import (
	"errors"
	"strings"
	"testing"

	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/types"
	corepb "github.com/tronprotocol/go-tron/proto/core"
)

// Only the canonical body is cold; stage metadata remains in the hot store.
type constructorIndexAncient struct {
	rawdb.NoopAncient
	body []byte
	err  error
}

func (a constructorIndexAncient) Ancient(kind string, number uint64) ([]byte, error) {
	if kind == rawdb.AncientBlocksTable && number == 2 {
		if a.err != nil {
			return nil, a.err
		}
		if a.body != nil {
			return a.body, nil
		}
	}
	return nil, rawdb.ErrNotInAncient
}

func TestHistoryStagingConstructorColdCanonicalIndex(t *testing.T) {
	block := types.NewBlockFromPB(&corepb.Block{BlockHeader: &corepb.BlockHeader{
		RawData: &corepb.BlockHeaderRaw{Number: 2, Timestamp: 6000},
	}})
	body, err := block.Marshal()
	if err != nil {
		t.Fatal(err)
	}
	wrongBlock := types.NewBlockFromPB(&corepb.Block{BlockHeader: &corepb.BlockHeader{
		RawData: &corepb.BlockHeaderRaw{Number: 3, Timestamp: 9000},
	}})
	wrongBody, err := wrongBlock.Marshal()
	if err != nil {
		t.Fatal(err)
	}
	readErr := errors.New("cold canonical body I/O failed")
	for _, tc := range []struct {
		name        string
		body        []byte
		readErr     error
		stageHash   common.Hash
		head        uint64
		missingRow  bool
		unhashed    bool
		wantMessage string
	}{
		{name: "lagging", body: body, stageHash: block.Hash(), head: 10},
		{name: "ahead", body: body, stageHash: block.Hash(), head: 1, wantMessage: "at or below head"},
		{name: "missing stage", body: body, head: 10, missingRow: true, wantMessage: "at or below head"},
		{name: "unhashed stage", body: body, head: 10, unhashed: true, wantMessage: "not hash-bound"},
		{name: "wrong hash", body: body, stageHash: common.Hash{0xff}, head: 10, wantMessage: "does not match canonical hash"},
		{name: "missing body", stageHash: block.Hash(), head: 10, wantMessage: "canonical block is unavailable"},
		{name: "corrupt body", body: []byte("not-a-valid-proto"), stageHash: block.Hash(), head: 10, wantMessage: "block 2 decode"},
		{name: "wrong body number", body: wrongBody, stageHash: block.Hash(), head: 10, wantMessage: "row 2 contains block number 3"},
		{name: "body I/O", readErr: readErr, stageHash: block.Hash(), head: 10, wantMessage: readErr.Error()},
	} {
		t.Run(tc.name, func(t *testing.T) {
			hot := rawdb.NewMemoryDatabase()
			defer hot.Close()
			if !tc.missingRow {
				var err error
				if tc.unhashed {
					err = rawdb.WriteStageProgress(hot, rawdb.StageStateHistoryIndex, 2)
				} else {
					err = rawdb.WriteStageProgressWithHash(hot, rawdb.StageStateHistoryIndex, 2, tc.stageHash)
				}
				if err != nil {
					t.Fatal(err)
				}
			}
			canonical := rawdb.NewChainDB(hot, constructorIndexAncient{body: tc.body, err: tc.readErr})
			before, beforeOK, err := rawdb.ReadStageProgressRow(hot, rawdb.StageStateHistoryIndex)
			if err != nil {
				t.Fatal(err)
			}
			err = verifyHistoryStagingConstructorIndex(hot, canonical, offlineChainBoundary{HeadBlock: tc.head})
			if tc.wantMessage == "" {
				if err != nil {
					t.Fatalf("cold-only lagging canonical index rejected: %v", err)
				}
			} else if err == nil || !strings.Contains(err.Error(), tc.wantMessage) {
				t.Fatalf("got %v, want error containing %q", err, tc.wantMessage)
			}
			if tc.readErr != nil && !errors.Is(err, tc.readErr) {
				t.Fatalf("cold I/O error identity lost: %v", err)
			}
			after, afterOK, err := rawdb.ReadStageProgressRow(hot, rawdb.StageStateHistoryIndex)
			if err != nil || before != after || beforeOK != afterOK {
				t.Fatalf("constructor changed stage metadata: before=%+v/%v after=%+v/%v err=%v", before, beforeOK, after, afterOK, err)
			}
		})
	}
}
