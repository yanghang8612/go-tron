package main

import "fmt"

func commitmentCacheSizeBytes(baseMiB, tailMiB int) (baseBytes, tailBytes int, err error) {
	if baseMiB < 0 || tailMiB < 0 {
		return 0, 0, fmt.Errorf("--state.commitment.cache and --state.commitment.cache.tail must be >= 0")
	}
	if tailMiB > 0 && baseMiB == 0 {
		return 0, 0, fmt.Errorf("--state.commitment.cache.tail requires --state.commitment.cache > 0")
	}
	const maxCacheMiB = int(^uint(0)>>1) / (1024 * 1024)
	if baseMiB > maxCacheMiB || tailMiB > maxCacheMiB-baseMiB {
		return 0, 0, fmt.Errorf("combined commitment cache size exceeds addressable bytes")
	}
	return baseMiB * 1024 * 1024, tailMiB * 1024 * 1024, nil
}
