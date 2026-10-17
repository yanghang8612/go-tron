package pebbledb

import (
	"bytes"
	"math"

	"github.com/cockroachdb/pebble"
	"github.com/ethereum/go-ethereum/metrics"
)

const (
	compactionBoundInside = iota
	compactionBoundOutside
	compactionBoundCross
	compactionBoundUnknown
	compactionBoundClasses
)

// These are manifest table bounds, not observations of individual keys or IO.
var compactionBoundClassNames = [compactionBoundClasses]string{
	"pure_commitment", "pure_outside_commitment", "crosses_commitment_boundary", "unknown",
}

var compactionBoundCohortNames = [3]string{
	"all_successful_rewrite", "has_cross_boundary_input", "has_pure_inside_and_outside_inputs",
}

var compactionBoundEventNames = [9]string{
	"rewrite", "move", "copy", "delete_only", "flush", "ingested_flushable", "unknown", "failed", "not_done",
}

type compactionBoundTableMetrics struct {
	tables, tableSizeBytes *metrics.Counter
}

type compactionBoundCohortMetrics struct {
	jobs   *metrics.Counter
	tables [2][compactionBoundClasses]compactionBoundTableMetrics
}

// compactionBoundsObserver owns immutable bounds copied from schema-owned,
// validated Open options. It never retains CompactionInfo or reenters the DB.
// Counters can be shared by same-namespace instances; bounds are per instance.
type compactionBoundsObserver struct {
	start, end []byte
	cohorts    [3]compactionBoundCohortMetrics
	events     [9]*metrics.Counter
	overflows  *metrics.Counter
}

func newCompactionBoundsObserver(namespace string, ranges []DiskSpaceRange) *compactionBoundsObserver {
	o := new(compactionBoundsObserver)
	for _, r := range ranges {
		if r.Name == "commitment" && len(r.Start) > 0 && len(r.End) > 0 && bytes.Compare(r.Start, r.End) < 0 {
			o.start, o.end = bytes.Clone(r.Start), bytes.Clone(r.End)
			break
		}
	}
	prefix := namespace + "compact/table_bounds/"
	configured := metrics.GetOrRegisterCounter(prefix+"configured_instances", nil)
	unconfigured := metrics.GetOrRegisterCounter(prefix+"unconfigured_instances", nil)
	if o.start != nil {
		configured.Inc(1)
	} else {
		unconfigured.Inc(1)
	}
	for c, name := range compactionBoundCohortNames {
		p := prefix + "cohort/" + name + "/"
		o.cohorts[c].jobs = metrics.GetOrRegisterCounter(p+"jobs", nil)
		for direction, directionName := range [2]string{"input", "output"} {
			for class, name := range compactionBoundClassNames {
				p := p + directionName + "/" + name + "/"
				o.cohorts[c].tables[direction][class] = compactionBoundTableMetrics{
					metrics.GetOrRegisterCounter(p+"tables", nil),
					metrics.GetOrRegisterCounter(p+"table_size_bytes", nil),
				}
			}
		}
	}
	for i, name := range compactionBoundEventNames {
		o.events[i] = metrics.GetOrRegisterCounter(prefix+"events/"+name, nil)
	}
	o.overflows = metrics.GetOrRegisterCounter(prefix+"size_overflow_events", nil)
	return o
}

func (o *compactionBoundsObserver) classify(small, large []byte) int {
	if o.start == nil || bytes.Compare(small, large) > 0 {
		return compactionBoundUnknown
	}
	if bytes.Compare(large, o.start) < 0 || bytes.Compare(small, o.end) >= 0 {
		return compactionBoundOutside
	}
	if bytes.Compare(small, o.start) >= 0 && bytes.Compare(large, o.end) < 0 {
		return compactionBoundInside
	}
	return compactionBoundCross
}

func compactionBoundEvent(info *pebble.CompactionInfo) int {
	if !info.Done {
		return 8
	}
	if info.Err != nil {
		return 7
	}
	switch info.Reason {
	case "default", "elision-only", "read", "rewrite":
		return 0
	case "move":
		return 1
	case "copy":
		return 2
	case "delete-only":
		return 3
	case "flush":
		return 4
	case "ingested-flushable":
		return 5
	default:
		return 6
	}
}

type compactionBoundTotals struct {
	tables, size [2][compactionBoundClasses]int64
	overflow     bool
}

func (o *compactionBoundsObserver) addTable(total *compactionBoundTotals, direction int, table *pebble.TableInfo) {
	class := o.classify(table.Smallest.UserKey, table.Largest.UserKey)
	total.tables[direction][class]++
	current := total.size[direction][class]
	if table.Size > uint64(math.MaxInt64-current) {
		total.size[direction][class] = math.MaxInt64
		total.overflow = true
	} else {
		total.size[direction][class] = current + int64(table.Size)
	}
}

func (o *compactionBoundsObserver) observe(info pebble.CompactionInfo) {
	if o == nil {
		return
	}
	event := compactionBoundEvent(&info)
	o.events[event].Inc(1)
	if event != 0 {
		return
	}
	var total compactionBoundTotals
	for i := range info.Input {
		for j := range info.Input[i].Tables {
			o.addTable(&total, 0, &info.Input[i].Tables[j])
		}
	}
	for i := range info.Output.Tables {
		o.addTable(&total, 1, &info.Output.Tables[i])
	}
	if total.overflow {
		o.overflows.Inc(1)
	}
	selected := [3]bool{
		true,
		total.tables[0][compactionBoundCross] > 0,
		total.tables[0][compactionBoundInside] > 0 && total.tables[0][compactionBoundOutside] > 0,
	}
	for c, selected := range selected {
		if !selected {
			continue
		}
		cohort := &o.cohorts[c]
		cohort.jobs.Inc(1)
		for direction := range total.tables {
			for class, count := range total.tables[direction] {
				cohort.tables[direction][class].tables.Inc(count)
				cohort.tables[direction][class].tableSizeBytes.Inc(total.size[direction][class])
			}
		}
	}
}
