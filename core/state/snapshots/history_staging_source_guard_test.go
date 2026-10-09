package snapshots

import (
	"context"
	"path/filepath"
	"strings"
	"testing"

	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
)

func TestColdBuilderRejectsHotFallbackForStagedHistory(t *testing.T) {
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
	identity := rawdb.HistoryStagingIdentity{Version: rawdb.HistoryStagingFormatVersion, GenesisHash: common.Hash{1}, NetworkID: 1, SourceID: [32]byte{2}, TargetID: [32]byte{3}}
	manager, err := rawdb.NewHistoryStagingManager(hot, stage, identity)
	if err != nil {
		t.Fatal(err)
	}
	if err := manager.Initialize(context.Background()); err != nil {
		t.Fatal(err)
	}
	runner := NewRunner(&coldBuilderChain{db: hot, solidified: 2}, Config{Dir: filepath.Join(root, "cold"), Enabled: true, HistoryWindow: 1})
	_, err = runner.OnePass()
	if err == nil || !strings.Contains(err.Error(), "lacks a routed view") {
		t.Fatalf("staged cold builder accepted hot-only source: %v", err)
	}
	if _, err := LoadManifest(filepath.Join(root, "cold")); err == nil {
		t.Fatal("hot-only cold manifest was published")
	}
}
