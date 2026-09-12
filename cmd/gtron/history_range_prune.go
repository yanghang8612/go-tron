package main

import "fmt"

func historyRangePruneEnabled(value string) (bool, error) {
	switch value {
	case "", "0":
		return false, nil
	case "1":
		return true, nil
	default:
		return false, fmt.Errorf("GTRON_HISTORY_RANGE_PRUNE must be 0 or 1")
	}
}
