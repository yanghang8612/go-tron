package pebbledb

import (
	"errors"
	"fmt"
	"sync"
	"testing"

	"github.com/cockroachdb/pebble"
)

// The opener leaf is synthetic here so error/panic cleanup can be checked
// without inducing an engine panic that could strand its own unrelated locks.
// The separate WAL regression invokes the actual production constructor.
func TestMaintenanceOpenWindowKnownJobsAndInvalidBoundaries(t *testing.T) {
	o := newMaintenanceWorkObserver(t.Name() + "/")
	_, err := o.observeOpen(func() (*pebble.DB, error) {
		o.tableCreated(pebble.TableCreateInfo{JobID: 1, Reason: "flushing"})
		for _, flush := range []bool{false, true} {
			reason := "compacting"
			if flush {
				reason = "flushing"
			}
			o.begin(2, flush, 123)
			o.tableCreated(pebble.TableCreateInfo{JobID: 2, Reason: reason})
			o.end(2, flush, nil)
		}
		o.tableCreated(pebble.TableCreateInfo{JobID: 90, Reason: "compacting"}) // invalid in Open
		o.end(91, true, nil)                                                    // unknown End remains invalid
		o.begin(3, true, 7)
		o.begin(3, true, 7)
		o.end(3, true, nil) // duplicate remains invalid
		return &pebble.DB{}, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	o.tableCreated(pebble.TableCreateInfo{JobID: 99, Reason: "flushing"}) // after Open invalid
	o.publish(nil, false)
	if workGauge(o, "maintenance/work/startup_unscoped_table_created") != 1 || workGauge(o, "maintenance/work/invalid_events") != 4 {
		t.Fatal("window swallowed unrelated invalid event")
	}
	for _, p := range []string{"flush/work/", "compact/work/"} {
		if workGauge(o, p+"output_files_created") != 1 || workGauge(o, p+"job_wall_nanos") <= 0 {
			t.Fatal("known Open job bypassed normal accounting")
		}
	}
	if workGauge(o, "flush/work/started_jobs") != 2 || workGauge(o, "compact/work/started_jobs") != 1 {
		t.Fatal("unscoped event fabricated job")
	}
	o.publish(nil, true)
	// Even an artificially stale opening bit cannot exempt a closed observer.
	o.mu.Lock()
	o.opening = true
	o.mu.Unlock()
	o.tableCreated(pebble.TableCreateInfo{JobID: 100, Reason: "flushing"})
	o.publish(nil, false)
	if workGauge(o, "maintenance/work/invalid_events") != 5 || workGauge(o, "maintenance/work/active_instances") != 0 {
		t.Fatal("closed observer exempted/revived")
	}
}

func TestMaintenanceOpenErrorPanicAndNamespaceIsolation(t *testing.T) {
	for _, panics := range []bool{false, true} {
		t.Run(fmt.Sprint(panics), func(t *testing.T) {
			ns := t.Name() + "/"
			other := newMaintenanceWorkObserver(ns)
			other.publish(nil, false)
			o := newMaintenanceWorkObserver(ns)
			sentinel := errors.New("open leaf error")
			func() {
				defer func() {
					if r := recover(); panics && r != sentinel {
						t.Fatalf("panic changed: %v", r)
					} else if !panics && r != nil {
						t.Fatal(r)
					}
				}()
				_, e := o.observeOpen(func() (*pebble.DB, error) {
					o.tableCreated(pebble.TableCreateInfo{JobID: 1, Reason: "flushing"})
					o.publish(nil, false)
					if panics {
						panic(sentinel)
					}
					return nil, sentinel
				})
				if !panics && e != sentinel {
					t.Fatal("error changed")
				}
			}()
			o.mu.Lock()
			opening, closed := o.opening, o.closed
			o.mu.Unlock()
			if opening || !closed {
				t.Fatal("failed Open retained live window")
			}
			o.publish(nil, false) // failure retirement must not resurrect on refresh
			if workGauge(o, "maintenance/work/active_instances") != 1 || workGauge(o, "maintenance/work/startup_unscoped_table_created") != 1 {
				t.Fatal("failed instance leaked/cleared other instance or cumulative event")
			}
			o.tableCreated(pebble.TableCreateInfo{JobID: 2, Reason: "flushing"})
			o.publish(nil, false)
			if workGauge(o, "maintenance/work/invalid_events") != 1 {
				t.Fatal("error/panic left window accepting events")
			}
			other.publish(nil, true)
			if workGauge(o, "maintenance/work/active_instances") != 0 {
				t.Fatal("other close did not remove own contribution")
			}
		})
	}
}

func TestMaintenanceOpenRealFailureNoActiveInstance(t *testing.T) {
	ns := t.Name() + "/"
	// Real readonly Open of a missing DB returns an engine error before the meter
	// starts. No partial Database.Close is needed or safe on this error path.
	d, e := New(t.TempDir()+"/missing", 16, 16, ns, true, DefaultOptions())
	if e == nil || d != nil {
		if d != nil {
			d.Close()
		}
		t.Fatal("expected actual readonly missing-DB error")
	}
	o := newMaintenanceWorkObserver(ns) // only reads shared gauges; not published
	if workGauge(o, "maintenance/work/active_instances") != 0 {
		t.Fatal("actual failed constructor leaked active instance")
	}
}

func TestMaintenanceOpenConcurrentTransitionAndScrape(t *testing.T) {
	o := newMaintenanceWorkObserver(t.Name() + "/")
	entered, release := make(chan struct{}), make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		o.observeOpen(func() (*pebble.DB, error) { close(entered); <-release; return &pebble.DB{}, nil })
	}()
	<-entered
	const n = 128
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		for i := 0; i < n; i++ {
			o.tableCreated(pebble.TableCreateInfo{JobID: i, Reason: "flushing"})
		}
	}()
	go func() {
		defer wg.Done()
		for i := 0; i < n; i++ {
			o.publish(nil, false)
		}
	}()
	close(release)
	wg.Wait()
	<-done
	o.tableCreated(pebble.TableCreateInfo{JobID: 999, Reason: "flushing"})
	o.publish(nil, false)
	if workGauge(o, "maintenance/work/startup_unscoped_table_created")+workGauge(o, "maintenance/work/invalid_events") != n+1 {
		t.Fatal("transition lost/doubled callback")
	}
	if workGauge(o, "maintenance/work/invalid_events") < 1 {
		t.Fatal("post-return event exempted")
	}
	if workGauge(o, "flush/work/started_jobs") != 0 || workGauge(o, "flush/work/job_wall_nanos") != 0 {
		t.Fatal("concurrent startup invented paired work")
	}
	o.publish(nil, true)
}
