package rawdb

import (
	"context"
	"testing"
)

func TestHistoryStagingOfflineColdResumeRetiresInterruptedChunks(t *testing.T) {
	m, closeStores, _, _, _ := stagingProtocolStores(t)
	defer closeStores()
	ctx := context.Background()
	proof := stagingProtocolProof(t, m, 1)
	if err := m.InitializeOfflineSourceRoutes(ctx, 1, 1, 1); err != nil {
		t.Fatal(err)
	}
	chunk := seedSharedGCBucket(t, m.hot, 1)
	binding := HistoryStagingColdBinding{Version: HistoryStagingFormatVersion, Bucket: 1, Epoch: 1, BindingEpoch: 1, ManifestEpoch: 1,
		Spans: []HistoryStagingColdSpan{{From: 1024, To: 2047, ContentID: [32]byte{1}, TxRangeDigest: [32]byte{2}, SemanticHash: [32]byte{3}}}}
	if err := m.CertifyColdRange(ctx, binding, func() error { return nil }); err != nil {
		t.Fatal(err)
	}
	route, present, err := m.ReadRoute(1)
	if err != nil || !present {
		t.Fatal(err)
	}
	// This is the durable state immediately after the complete range deletion
	// commits its COLD route, before retireCertifiedSourceChunks is entered.
	route.Owner = HistoryStagingOwnerCold
	route.WriteVersion++
	if err := writeHistoryStagingValue(m.hot, historyStagingBucketKey(historyStagingRoutePrefix, 1), route); err != nil {
		t.Fatal(err)
	}
	if err := m.syncHot(); err != nil {
		t.Fatal(err)
	}
	if present, err := m.hot.Has(chunk); err != nil || !present {
		t.Fatal("interrupted chunk fixture absent")
	}
	first, last, _ := StateHistoryChunkBucketBounds(proof.Bucket)
	if err := m.ClearCertifiedSourceRange(ctx, 1, first, last, stagingProtocolLimits()); err != nil {
		t.Fatal(err)
	}
	if present, err := m.hot.Has(chunk); err != nil || present {
		t.Fatal("cold resume retained source chunk")
	}
	if err := m.ClearCertifiedSourceRange(ctx, 1, first, last, stagingProtocolLimits()); err != nil {
		t.Fatalf("idempotent retry: %v", err)
	}
}
