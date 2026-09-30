package main

import (
	"math"
	"path/filepath"
	"strconv"
	"strings"
)

// The host values are an upper bound on unsafe pages in any descendant
// cgroup. They are deliberately used when an old kernel omits cgroup-local
// dirty, writeback, or shmem accounting.
func historyParallelHostUnsafeFile(raw []byte) (uint64, bool) {
	if len(raw) == 0 || len(raw) > historyParallelSmallLimit {
		return 0, false
	}
	values, ok := historyParallelNamedCounters(raw, []string{"Dirty:", "Writeback:", "Shmem:"}, true)
	if !ok {
		return 0, false
	}
	return historyParallelSumSaturating(values), true
}

func historyParallelNamedCounters(raw []byte, names []string, kilobytes bool) ([]uint64, bool) {
	values := make([]uint64, len(names))
	seen := make([]bool, len(names))
	for _, line := range strings.Split(string(raw), "\n") {
		fields := strings.Fields(line)
		if len(fields) == 0 {
			continue
		}
		for i, name := range names {
			if fields[0] != name {
				continue
			}
			if seen[i] || len(fields) != 2 && !kilobytes || len(fields) != 3 && kilobytes || kilobytes && fields[2] != "kB" {
				return nil, false
			}
			value, err := strconv.ParseUint(fields[1], 10, 64)
			if err != nil || kilobytes && value > math.MaxUint64/1024 {
				return nil, false
			}
			if kilobytes {
				value *= 1024
			}
			values[i], seen[i] = value, true
		}
	}
	for _, found := range seen {
		if !found {
			return nil, false
		}
	}
	return values, true
}

func historyParallelSumSaturating(values []uint64) uint64 {
	var total uint64
	for _, value := range values {
		if value > math.MaxUint64-total {
			return math.MaxUint64
		}
		total += value
	}
	return total
}

func historyParallelCleanFileCredit(read historyParallelReadFile, dir string, v2 bool, hostUnsafe, usage, limit uint64) uint64 {
	raw, err := read(filepath.Join(dir, "memory.stat"), historyParallelSmallLimit)
	if err != nil {
		return 0
	}
	inactiveName, unsafeNames := "total_inactive_file", []string{"total_dirty", "total_writeback", "total_shmem"}
	if v2 {
		inactiveName, unsafeNames = "inactive_file", []string{"file_dirty", "file_writeback", "shmem"}
	}
	inactive, ok := historyParallelNamedCounters(raw, []string{inactiveName}, false)
	if !ok {
		return 0
	}
	unsafe := hostUnsafe
	if local, complete := historyParallelNamedCounters(raw, unsafeNames, false); complete {
		unsafe = historyParallelSumSaturating(local)
	}
	// Stats and charge are separate kernel reads. A stale or corrupt inactive
	// value cannot create more credit than this hierarchy can hold.
	inactive[0] = min(inactive[0], usage, limit)
	if inactive[0] <= unsafe {
		return 0
	}
	return (inactive[0] - unsafe) / 2
}

// Missing or malformed OOM evidence removes the cache credit, while leaving
// the original limit-minus-usage capacity estimate available. v1 may not
// expose a kill counter; under_oom is then the strongest available signal.
func historyParallelOOM(read historyParallelReadFile, dir string, v2 bool) (bool, bool, uint64) {
	name := "memory.oom_control"
	if v2 {
		name = "memory.events"
	}
	raw, err := read(filepath.Join(dir, name), historyParallelCgroupLimit)
	if err != nil {
		return false, false, 0
	}
	if v2 {
		values, ok := historyParallelNamedCounters(raw, []string{"oom", "oom_kill"}, false)
		if !ok || values[0] > math.MaxUint64-values[1] {
			return false, false, 0
		}
		return true, false, values[0] + values[1]
	}
	values, ok := historyParallelNamedCounters(raw, []string{"under_oom"}, false)
	if !ok || values[0] > 1 {
		return false, false, 0
	}
	return true, values[0] == 1, 0
}

func historyParallelMemoryCreditReady(before, after *historyParallelObservation) bool {
	if before == nil || after == nil || !before.memoryOOMKnown || !after.memoryOOMKnown ||
		before.memoryUnderOOM || after.memoryUnderOOM ||
		len(before.memoryOOMEvents) == 0 || len(before.memoryOOMEvents) != len(after.memoryOOMEvents) {
		return false
	}
	for i, count := range after.memoryOOMEvents {
		if count != before.memoryOOMEvents[i] {
			return false
		}
	}
	return true
}
