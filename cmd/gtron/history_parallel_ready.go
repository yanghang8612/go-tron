package main

import (
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/metrics"
)

const (
	historyParallelSampleInterval = 5 * time.Second
	historyParallelMaxSpan        = 30 * time.Second
	historyParallelMinimumMemory  = 2 << 30
	historyParallelProcLimit      = 1 << 20
	historyParallelSmallLimit     = 64 << 10
	historyParallelCgroupLimit    = 4096
	historyParallelMaxCPUs        = 4096
)

type historyParallelCPU struct {
	id    int
	ticks [8]uint64 // user,nice,system,idle,iowait,irq,softirq,steal; guest is already in user/nice.
}

type historyParallelObservation struct {
	at              time.Time
	cpus            []historyParallelCPU
	memoryAvailable uint64
	cpuLimitMilli   uint64
	scope           string
}

// This is an additional admission hint, never a replacement for the shared I/O
// gate. The node resource sampler refreshes its bounded proc/cgroup observations
// even when storage admission short-circuits the parallel-readiness callback.
type runtimeHistoryParallelProbe struct {
	mu          sync.Mutex
	now         func() time.Time
	read        func() (historyParallelObservation, error)
	gomax       func() int
	numCPU      func() int
	lastAttempt time.Time
	previous    *historyParallelObservation
	known       bool
	idlePPM     uint64
	metrics     map[string]*metrics.Gauge
}

func newRuntimeHistoryParallelProbe() *runtimeHistoryParallelProbe {
	p := &runtimeHistoryParallelProbe{now: time.Now, read: readRuntimeHistoryParallel,
		gomax: func() int { return runtime.GOMAXPROCS(0) }, numCPU: runtime.NumCPU,
		metrics: make(map[string]*metrics.Gauge)}
	for _, name := range []string{"known", "idle_ppm", "idle_cores_milli", "memory_available_bytes", "ready"} {
		p.metrics[name] = metrics.GetOrRegisterGauge("state/snapshot/cold/history/parallel/runtime/"+name, nil)
	}
	return p
}

func (p *runtimeHistoryParallelProbe) reset() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.lastAttempt, p.previous, p.known, p.idlePPM = time.Time{}, nil, false, 0
	for _, gauge := range p.metrics {
		gauge.Update(0)
	}
}

func (p *runtimeHistoryParallelProbe) ready() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	now := p.now()
	age := now.Sub(p.lastAttempt)
	if p.lastAttempt.IsZero() || age < 0 || age >= historyParallelSampleInterval {
		p.lastAttempt, p.known, p.idlePPM = now, false, 0
		previous := p.previous
		p.previous = nil
		current, err := p.read()
		finished := p.now()
		// Start the cache interval at completion, matching current.at. Otherwise
		// a faster next read can produce a <5s pair despite waiting 5s since the
		// previous attempt began, repeatedly discarding fresh idle evidence.
		p.lastAttempt = finished
		// An unexpectedly slow or backwards observation is not fresh capacity.
		if err == nil && finished.Sub(now) >= 0 && finished.Sub(now) <= time.Second {
			current.at = finished
			p.previous = &current
			if previous != nil && current.scope == previous.scope &&
				current.at.Sub(previous.at) >= historyParallelSampleInterval && current.at.Sub(previous.at) <= historyParallelMaxSpan {
				p.idlePPM, p.known = historyParallelIdle(previous.cpus, current.cpus)
			}
		}
	}
	var idleCores, memory uint64
	unlimitedCPU := false
	gomax := p.gomax()
	if p.previous != nil {
		memory = p.previous.memoryAvailable
		// Host idle time cannot establish unused quota inside a finite CPU
		// cgroup. Its own usage/throttle deltas would be needed to enable this
		// case safely; retain serial operation until that evidence is available.
		unlimitedCPU = p.previous.cpuLimitMilli == math.MaxUint64
		cores := min(gomax, p.numCPU(), len(p.previous.cpus))
		if cores > 0 {
			capacity := min(uint64(cores)*1000, p.previous.cpuLimitMilli)
			allowed := uint64(len(p.previous.cpus)) * 1000
			// Charge all observed busy allowed CPUs against Go's smaller
			// effective capacity. Multiplying host idle by GOMAXPROCS would
			// invent spare slots when Go already occupies its entire limit.
			busy := allowed - historyLoadRatio(allowed, p.idlePPM, 1_000_000)
			if capacity > busy {
				idleCores = capacity - busy
			}
		}
	}
	ready := p.known && unlimitedCPU && gomax >= 8 && p.idlePPM >= 250_000 && idleCores >= 2000 && memory >= historyParallelMinimumMemory
	for name, value := range map[string]uint64{"known": historyParallelBool(p.known), "idle_ppm": p.idlePPM,
		"idle_cores_milli": idleCores, "memory_available_bytes": memory, "ready": historyParallelBool(ready)} {
		if gauge := p.metrics[name]; gauge != nil {
			gauge.Update(int64(min(value, uint64(math.MaxInt64))))
		}
	}
	return ready
}

func historyParallelBool(value bool) uint64 {
	if value {
		return 1
	}
	return 0
}

func historyParallelIdle(before, after []historyParallelCPU) (uint64, bool) {
	if len(before) == 0 || len(before) != len(after) {
		return 0, false
	}
	var total, idle uint64
	for i, current := range after {
		if current.id != before[i].id {
			return 0, false
		}
		for field, value := range current.ticks {
			if value < before[i].ticks[field] {
				return 0, false
			}
			delta := value - before[i].ticks[field]
			if delta > math.MaxUint64-total {
				return 0, false
			}
			total += delta
			if field == 3 {
				idle += delta
			} // Neither steal nor I/O wait establishes spare CPU.
		}
	}
	if total == 0 {
		return 0, false
	}
	return historyLoadRatio(idle, 1_000_000, total), true
}

type historyParallelReadFile func(string, int) ([]byte, error)

func readHistoryParallelFile(path string, limit int) ([]byte, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	raw, err := io.ReadAll(io.LimitReader(f, int64(limit)+1))
	if err != nil {
		return nil, err
	}
	if len(raw) > limit {
		return nil, fmt.Errorf("history parallel probe: %s exceeds limit", path)
	}
	return raw, nil
}

// Parsers live in the common file so hermetic tests cover Linux observations
// even when the development machine uses another operating system.
func collectHistoryParallel(read historyParallelReadFile) (historyParallelObservation, error) {
	var out historyParallelObservation
	status, err := read("/proc/self/status", historyParallelSmallLimit)
	if err != nil {
		return out, err
	}
	allowed, err := historyParallelAllowedCPUs(status)
	if err != nil {
		return out, err
	}
	stat, err := read("/proc/stat", historyParallelProcLimit)
	if err != nil {
		return out, err
	}
	out.cpus, err = historyParallelCPUCounters(stat, allowed)
	if err != nil {
		return out, err
	}
	mem, err := read("/proc/meminfo", historyParallelSmallLimit)
	if err != nil {
		return out, err
	}
	out.memoryAvailable, err = historyParallelMemAvailable(mem)
	if err != nil {
		return out, err
	}
	groups, err := read("/proc/self/cgroup", historyParallelSmallLimit)
	if err != nil {
		return out, err
	}
	mounts, err := read("/proc/self/mountinfo", historyParallelProcLimit)
	if err != nil {
		return out, err
	}
	out.memoryAvailable, out.cpuLimitMilli, out.scope, err = historyParallelCgroupHeadroom(read, groups, mounts, out.memoryAvailable)
	return out, err
}

func historyParallelAllowedCPUs(raw []byte) ([]int, error) {
	if len(raw) > historyParallelSmallLimit {
		return nil, errors.New("status exceeds limit")
	}
	var ids []int
	for _, line := range strings.Split(string(raw), "\n") {
		if !strings.HasPrefix(line, "Cpus_allowed_list:") {
			continue
		}
		if ids != nil {
			return nil, errors.New("duplicate CPU affinity")
		}
		for _, span := range strings.Split(strings.TrimSpace(strings.TrimPrefix(line, "Cpus_allowed_list:")), ",") {
			bounds := strings.Split(span, "-")
			if len(bounds) > 2 {
				return nil, errors.New("invalid CPU affinity")
			}
			first, err := strconv.ParseUint(bounds[0], 10, 12)
			if err != nil {
				return nil, err
			}
			last := first
			if len(bounds) == 2 {
				last, err = strconv.ParseUint(bounds[1], 10, 12)
			}
			if err != nil || last < first || len(ids) > 0 && int(first) <= ids[len(ids)-1] {
				return nil, errors.New("invalid CPU affinity range")
			}
			for id := first; id <= last; id++ {
				ids = append(ids, int(id))
			}
		}
	}
	if len(ids) == 0 || len(ids) > historyParallelMaxCPUs {
		return nil, errors.New("CPU affinity unavailable")
	}
	return ids, nil
}

func historyParallelCPUCounters(raw []byte, allowed []int) ([]historyParallelCPU, error) {
	if len(raw) > historyParallelProcLimit {
		return nil, errors.New("stat exceeds limit")
	}
	wanted := make(map[int]int, len(allowed))
	for i, id := range allowed {
		wanted[id] = i
	}
	out, seen := make([]historyParallelCPU, len(allowed)), make([]bool, len(allowed))
	for _, line := range strings.Split(string(raw), "\n") {
		fields := strings.Fields(line)
		if len(fields) == 0 || !strings.HasPrefix(fields[0], "cpu") || fields[0] == "cpu" {
			continue
		}
		id, err := strconv.Atoi(strings.TrimPrefix(fields[0], "cpu"))
		if err != nil {
			return nil, err
		}
		index, ok := wanted[id]
		if !ok {
			continue
		}
		if seen[index] || len(fields) < 9 {
			return nil, errors.New("duplicate or truncated CPU counters")
		}
		seen[index], out[index].id = true, id
		for j := range out[index].ticks {
			out[index].ticks[j], err = strconv.ParseUint(fields[j+1], 10, 64)
			if err != nil {
				return nil, err
			}
		}
	}
	for _, ok := range seen {
		if !ok {
			return nil, errors.New("allowed CPU is missing")
		}
	}
	return out, nil
}

func historyParallelMemAvailable(raw []byte) (uint64, error) {
	if len(raw) > historyParallelSmallLimit {
		return 0, errors.New("meminfo exceeds limit")
	}
	var total, available uint64
	var totalSeen, availableSeen bool
	for _, line := range strings.Split(string(raw), "\n") {
		fields := strings.Fields(line)
		if len(fields) == 0 || fields[0] != "MemAvailable:" && fields[0] != "MemTotal:" {
			continue
		}
		if len(fields) != 3 || fields[2] != "kB" {
			return 0, errors.New("invalid memory units")
		}
		value, err := strconv.ParseUint(fields[1], 10, 64)
		if err != nil || value > math.MaxUint64/1024 {
			return 0, errors.New("invalid memory value")
		}
		if fields[0] == "MemTotal:" {
			if totalSeen {
				return 0, errors.New("duplicate total memory")
			}
			total, totalSeen = value*1024, true
		} else {
			if availableSeen {
				return 0, errors.New("duplicate available memory")
			}
			available, availableSeen = value*1024, true
		}
	}
	if !totalSeen || !availableSeen || total == 0 || available > total {
		return 0, errors.New("available memory unknown")
	}
	return available, nil
}

type historyParallelCgroup struct {
	path, mount string
	v2          bool
}

func historyParallelCgroupHeadroom(read historyParallelReadFile, groups, mounts []byte, available uint64) (uint64, uint64, string, error) {
	memory, err := historyParallelResolveCgroup(groups, mounts, "memory")
	if err != nil {
		return 0, 0, "", err
	}
	cpu, err := historyParallelResolveCgroup(groups, mounts, "cpu")
	if err != nil {
		return 0, 0, "", err
	}
	for _, group := range []historyParallelCgroup{memory, cpu} {
		depth := 0
		for dir := group.path; ; dir = filepath.Dir(dir) {
			depth++
			if depth > 32 {
				return 0, 0, "", errors.New("cgroup hierarchy exceeds limit")
			}
			if dir == group.mount {
				break
			}
			if dir == filepath.Dir(dir) {
				return 0, 0, "", errors.New("cgroup escaped mount")
			}
		}
	}
	for dir := memory.path; ; dir = filepath.Dir(dir) {
		limitName, usageName, softName := "memory.limit_in_bytes", "memory.usage_in_bytes", "memory.soft_limit_in_bytes"
		if memory.v2 {
			limitName, usageName, softName = "memory.max", "memory.current", "memory.high"
		}
		limitRaw, err := read(filepath.Join(dir, limitName), historyParallelCgroupLimit)
		if !(memory.v2 && dir == memory.mount && os.IsNotExist(err)) {
			if err != nil {
				return 0, 0, "", err
			}
			limit, err := historyParallelLimit(limitRaw, memory.v2)
			if err != nil {
				return 0, 0, "", err
			}
			softRaw, err := read(filepath.Join(dir, softName), historyParallelCgroupLimit)
			if err != nil {
				return 0, 0, "", err
			}
			soft, err := historyParallelLimit(softRaw, memory.v2)
			if err != nil {
				return 0, 0, "", err
			}
			usageRaw, err := read(filepath.Join(dir, usageName), historyParallelCgroupLimit)
			if err != nil {
				return 0, 0, "", err
			}
			usage, err := historyParallelLimit(usageRaw, false)
			if err != nil {
				return 0, 0, "", err
			}
			limit = min(limit, soft)
			if usage >= limit {
				available = 0
			} else {
				available = min(available, limit-usage)
			}
		}
		if dir == memory.mount {
			break
		}
	}
	capacity := uint64(math.MaxUint64)
	for dir := cpu.path; ; dir = filepath.Dir(dir) {
		quotaName := "cpu.cfs_quota_us"
		if cpu.v2 {
			quotaName = "cpu.max"
		}
		raw, err := read(filepath.Join(dir, quotaName), historyParallelCgroupLimit)
		if !(cpu.v2 && dir == cpu.mount && os.IsNotExist(err)) {
			if err != nil {
				return 0, 0, "", err
			}
			parts := strings.Fields(string(raw))
			if !cpu.v2 {
				period, err := read(filepath.Join(dir, "cpu.cfs_period_us"), historyParallelCgroupLimit)
				if err != nil {
					return 0, 0, "", err
				}
				parts = append(parts, strings.Fields(string(period))...)
			}
			if len(parts) != 2 {
				return 0, 0, "", errors.New("invalid CPU quota")
			}
			period, err := strconv.ParseUint(parts[1], 10, 64)
			if err != nil || period == 0 {
				return 0, 0, "", errors.New("invalid CPU quota period")
			}
			if !(cpu.v2 && parts[0] == "max" || !cpu.v2 && parts[0] == "-1") {
				quota, err := strconv.ParseUint(parts[0], 10, 64)
				if err != nil || quota == 0 {
					return 0, 0, "", errors.New("invalid CPU quota value")
				}
				// A saturated finite ratio must never become the unlimited
				// sentinel used by the readiness decision.
				capacity = min(capacity, uint64(math.MaxUint64-1), historyLoadRatio(quota, 1000, period))
			}
		}
		if dir == cpu.mount {
			break
		}
	}
	return available, capacity, fmt.Sprintf("%v|%v", memory, cpu), nil
}

func historyParallelLimit(raw []byte, allowMax bool) (uint64, error) {
	if len(raw) > historyParallelCgroupLimit {
		return 0, errors.New("cgroup value exceeds limit")
	}
	value := strings.TrimSpace(string(raw))
	if allowMax && value == "max" {
		return math.MaxUint64, nil
	}
	return strconv.ParseUint(value, 10, 64)
}

func historyParallelResolveCgroup(groups, mounts []byte, controller string) (historyParallelCgroup, error) {
	var out historyParallelCgroup
	if len(groups) > historyParallelSmallLimit || len(mounts) > historyParallelProcLimit {
		return out, errors.New("cgroup metadata exceeds limit")
	}
	member, unified := "", ""
	for _, line := range strings.Split(string(groups), "\n") {
		if line == "" {
			continue
		}
		parts := strings.SplitN(line, ":", 3)
		if len(parts) != 3 {
			return out, errors.New("invalid cgroup membership")
		}
		if parts[0] == "0" && parts[1] == "" {
			unified = parts[2]
		}
		if historyParallelHasToken(parts[1], controller) {
			if member != "" {
				return out, errors.New("duplicate cgroup controller")
			}
			member = parts[2]
		}
	}
	if member == "" {
		member, out.v2 = unified, true
	}
	if !filepath.IsAbs(member) || filepath.Clean(member) != member {
		return out, errors.New("cgroup membership unknown")
	}
	for _, line := range strings.Split(string(mounts), "\n") {
		parts := strings.SplitN(line, " - ", 2)
		if len(parts) != 2 {
			continue
		}
		left, right := strings.Fields(parts[0]), strings.Fields(parts[1])
		if len(left) < 6 || len(right) < 3 {
			continue
		}
		if !(out.v2 && right[0] == "cgroup2" || !out.v2 && right[0] == "cgroup" && historyParallelHasToken(right[2], controller)) {
			continue
		}
		// Bind/subtree mounts can hide a tighter ancestor; don't guess headroom.
		if left[3] != "/" || strings.Contains(left[4], "\\") || !filepath.IsAbs(left[4]) || filepath.Clean(left[4]) != left[4] {
			continue
		}
		if out.mount != "" {
			return historyParallelCgroup{}, errors.New("ambiguous cgroup mount")
		}
		out.mount = left[4]
		out.path = filepath.Join(out.mount, member)
	}
	if out.mount == "" {
		return out, errors.New("complete cgroup hierarchy unavailable")
	}
	return out, nil
}

func historyParallelHasToken(list, token string) bool {
	for _, value := range strings.Split(list, ",") {
		if value == token {
			return true
		}
	}
	return false
}
