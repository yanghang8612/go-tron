package state

import "testing"

var assetIssueTimeBenchmarkSink int64

func BenchmarkStateDBReadAssetIssueTimeDirty(b *testing.B) {
	sdb := newTestStateDB(b)
	if err := sdb.WriteAssetIssueTime(1_000_001, 1_234_567); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		assetIssueTimeBenchmarkSink = sdb.ReadAssetIssueTime(1_000_001)
	}
}
