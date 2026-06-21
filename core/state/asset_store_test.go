package state

import (
	"strings"
	"testing"

	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/state/kvdomains"
)

func TestAssetIssueAtSurfacesCorruptMetadata(t *testing.T) {
	f := newHistoryFixture(t)
	const tokenID int64 = 1_000_001

	f.applyBlock(tcommon.Hash{0x01}, func(s *StateDB) {
		if err := s.SystemKVPut(kvdomains.SystemAsset, assetIDKey(assetV2Tag, tokenID), []byte{0x80}); err != nil {
			t.Fatalf("write corrupt asset metadata: %v", err)
		}
	})
	f.applyBlock(tcommon.Hash{0x02}, func(*StateDB) {})

	got, err := f.reader().AssetIssueAt(tokenID, 1)
	if err == nil {
		t.Fatal("AssetIssueAt corrupt metadata error = nil")
	}
	if got != nil {
		t.Fatalf("AssetIssueAt corrupt metadata asset = %+v, want nil", got)
	}
	if !strings.Contains(err.Error(), "decode asset metadata at block 1") {
		t.Fatalf("AssetIssueAt corrupt metadata error = %v, want decode asset metadata context", err)
	}
}
