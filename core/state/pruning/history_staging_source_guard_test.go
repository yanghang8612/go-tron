package pruning

import (
	"context"
	"path/filepath"
	"strings"
	"testing"

	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
)

func TestSnapshotChainSourceRejectsHotFallbackForStagedHistory(t *testing.T) {
	root := t.TempDir()
	hot, err := rawdb.NewPebbleDB(filepath.Join(root, "hot"), 16, 16)
	if err != nil {
		t.Fatal(err)
	}
	defer hot.Close()
	stage, err := rawdb.NewHistoryStagingPebbleDB(filepath.Join(root, "stage"), 16, 16, false)
	if err != nil {
		t.Fatal(err)
	}
	defer stage.Close()
	chain := &fakePruneChain{db: hot, solidified: 1}
	view, release, err := (snapshotChainSource{chain: chain}).AcquireStateHistorySourceView(context.Background())
	if err != nil || view == nil || release == nil {
		t.Fatalf("legacy hot fallback: view=%v release=%v err=%v", view, release != nil, err)
	}
	if err := release(); err != nil {
		t.Fatal(err)
	}
	identity := rawdb.HistoryStagingIdentity{Version: rawdb.HistoryStagingFormatVersion, GenesisHash: common.Hash{1}, NetworkID: 1, SourceID: [32]byte{2}, TargetID: [32]byte{3}}
	manager, err := rawdb.NewHistoryStagingManager(hot, stage, identity)
	if err != nil {
		t.Fatal(err)
	}
	if err := manager.Initialize(context.Background()); err != nil {
		t.Fatal(err)
	}
	view, release, err = (snapshotChainSource{chain: chain}).AcquireStateHistorySourceView(context.Background())
	if view != nil || release != nil || err == nil || !strings.Contains(err.Error(), "lacks a routed view") {
		t.Fatalf("staged hot fallback: view=%v release=%v err=%v", view, release != nil, err)
	}
}
