//go:build linux || darwin

package rawdb

import (
	"runtime"

	"golang.org/x/sys/unix"
)

func historyBenchmarkProcessCPU() (int64, bool) {
	var usage unix.Rusage
	if err := unix.Getrusage(unix.RUSAGE_SELF, &usage); err != nil {
		return 0, false
	}
	return usage.Utime.Nano() + usage.Stime.Nano(), true
}

func historyBenchmarkCDCWorkers() int { return min(4, runtime.GOMAXPROCS(0)) }
