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

func historyRangeQueueEnabled(value string, rangeEnabled bool) (bool, error) {
	switch value {
	case "", "0":
		return false, nil
	case "1":
		if !rangeEnabled {
			return false, fmt.Errorf("GTRON_HISTORY_RANGE_QUEUE requires GTRON_HISTORY_RANGE_PRUNE=1")
		}
		return true, nil
	default:
		return false, fmt.Errorf("GTRON_HISTORY_RANGE_QUEUE must be 0 or 1")
	}
}
