package etl

import (
	"errors"
	"fmt"
	"testing"
)

func TestCollectorGrowthPreservesLatestOperationAndRetry(t *testing.T) {
	for _, limit := range []int{1 << 20, 4096} {
		t.Run(fmt.Sprint(limit), func(t *testing.T) {
			c := newTestCollector(t, Options{BufferLimit: limit})
			defer c.Close()
			// Ensure growth is exercised even when another test populated the pool.
			buffer := []entry(nil)
			c.rowBuffer = &buffer
			for i := 0; i < 4096; i++ {
				key := fmt.Sprintf("%04d", i)
				mustPut(t, c, key, "old")
			}
			for i := 4095; i >= 0; i-- {
				key := fmt.Sprintf("%04d", i)
				if i%2 == 0 {
					mustDelete(t, c, key)
				} else {
					mustPut(t, c, key, "new")
				}
			}
			writer := newRecordingWriter()
			if _, err := c.LoadInterruptible(writer, func() bool { return true }); !errors.Is(err, ErrLoadInterrupted) {
				t.Fatalf("interrupt: %v", err)
			}
			if len(writer.ops) != 0 {
				t.Fatalf("interrupted writes: %d", len(writer.ops))
			}
			stats, err := c.Load(writer)
			if err != nil {
				t.Fatal(err)
			}
			if stats.Applied != 4096 || stats.AppliedDeletes != 2048 || stats.AppliedPuts != 2048 {
				t.Fatalf("stats: %+v", stats)
			}
			for i := 0; i < 4096; i++ {
				key := fmt.Sprintf("%04d", i)
				value, ok := writer.data[key]
				if i%2 == 0 {
					if ok {
						t.Fatalf("deleted key %s present", key)
					}
				} else if !ok || string(value) != "new" {
					t.Fatalf("key %s: %q present %v", key, value, ok)
				}
			}
		})
	}
}

func TestCollectorGrowthPoolBoundary(t *testing.T) {
	// Crossing the retention bound must still work; releaseRows must discard it.
	rows := make([]entry, collectorRowsPoolMaxCapacity)
	c := Collector{opts: Options{BufferLimit: 1 << 30}, rows: rows, rowBuffer: &rows}
	if err := c.append(entry{op: opPut}); err != nil {
		t.Fatal(err)
	}
	if len(c.rows) != collectorRowsPoolMaxCapacity+1 {
		t.Fatal(len(c.rows))
	}
	c.releaseRows()
	if c.rows != nil || c.rowBuffer != nil {
		t.Fatal("collector retained released metadata")
	}
}
