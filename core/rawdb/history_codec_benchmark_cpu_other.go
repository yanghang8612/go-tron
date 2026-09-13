//go:build !linux && !darwin

package rawdb

import "runtime"

func historyBenchmarkProcessCPU() (int64, bool) { return 0, false }

func historyBenchmarkCDCWorkers() int { return min(4, runtime.GOMAXPROCS(0)) }
