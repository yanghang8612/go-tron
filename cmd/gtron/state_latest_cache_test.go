package main

import "testing"

func TestStateLatestCacheSize(t *testing.T) {
	for _, tc := range []struct{ mib, want int }{{0, 0}, {128, 128 * 1024 * 1024}, {512, 512 * 1024 * 1024}} {
		got, err := stateLatestCacheSizeBytes(tc.mib)
		if err != nil || got != tc.want {
			t.Fatalf("size(%d)=%d %v", tc.mib, got, err)
		}
	}
	if stateLatestCacheFlag.Value != 128 {
		t.Fatalf("default=%d, want prior flat cache share128MiB", stateLatestCacheFlag.Value)
	}
	if _, err := stateLatestCacheSizeBytes(-1); err == nil {
		t.Fatal("negative size accepted")
	}
	if _, err := stateLatestCacheSizeBytes(int(^uint(0) >> 1)); err == nil {
		t.Fatal("overflow accepted")
	}
}
