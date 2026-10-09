package core

import (
	"context"
	"testing"
)

func TestHistoryStagingStartupFailureClearsPreviousReadyBit(t *testing.T) {
	bc := new(BlockChain)
	bc.historyStagingReady.Store(true)
	if err := bc.VerifyHistoryStagingRuntimeReady(nil); err == nil {
		t.Fatal("nil context accepted")
	}
	if bc.historyStagingReady.Load() {
		t.Fatal("failed startup verification retained an earlier ready bit")
	}
	bc.historyStagingReady.Store(true)
	if err := bc.VerifyHistoryStagingRuntimeReady(context.Background()); err == nil {
		t.Fatal("missing staging manager accepted")
	}
	if bc.historyStagingReady.Load() {
		t.Fatal("missing manager retained an earlier ready bit")
	}
}
