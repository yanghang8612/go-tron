package main

import "fmt"

func stateLatestCacheSizeBytes(mib int) (int, error) {
	if mib < 0 || mib > int(^uint(0)>>1)/(1024*1024) {
		return 0, fmt.Errorf("--state.latest.cache must be a nonnegative MiB size within addressable bytes")
	}
	return mib * 1024 * 1024, nil
}
