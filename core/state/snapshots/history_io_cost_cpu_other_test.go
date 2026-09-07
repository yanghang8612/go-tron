//go:build !darwin && !linux

package snapshots

import "time"

func historyIOProcessCPU() (user, system time.Duration, available bool) { return 0, 0, false }
