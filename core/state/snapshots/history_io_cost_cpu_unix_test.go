//go:build darwin || linux

package snapshots

import (
	"golang.org/x/sys/unix"
	"time"
)

func historyIOProcessCPU() (user, system time.Duration, available bool) {
	var usage unix.Rusage
	if unix.Getrusage(unix.RUSAGE_SELF, &usage) != nil {
		return 0, 0, false
	}
	return time.Duration(usage.Utime.Sec)*time.Second + time.Duration(usage.Utime.Usec)*time.Microsecond,
		time.Duration(usage.Stime.Sec)*time.Second + time.Duration(usage.Stime.Usec)*time.Microsecond, true
}
