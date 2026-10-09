package etl

import (
	"bytes"
	"errors"
	"fmt"
	"reflect"
	"testing"
)

func TestCollectorWorkCheckpointPreservesSpilledStream(t *testing.T) {
	var want []string
	for _, cooperative := range []bool{false, true} {
		var charged uint64
		calls := 0
		opts := Options{BufferLimit: 4096}
		if cooperative {
			opts.Checkpoint = func(n uint64) error { calls++; charged += n; return nil }
		}
		c := newTestCollector(t, opts)
		defer c.Close()
		for i := 999; i >= 0; i-- {
			if err := c.Put([]byte(fmt.Sprintf("%04d", i%71)), []byte(fmt.Sprint(i))); err != nil {
				t.Fatal(err)
			}
		}
		var got []string
		if _, err := c.Iterate(func(k, v []byte) error { got = append(got, string(k)+"="+string(v)); return nil }); err != nil {
			t.Fatal(err)
		}
		if c.Stats().SpilledRuns < 2 {
			t.Fatal("fixture did not spill")
		}
		if !cooperative {
			want = got
		} else if !reflect.DeepEqual(want, got) || charged < c.Stats().InputBytes || calls < c.Stats().SpilledRuns*3 {
			t.Fatalf("stream/checkpoint mismatch calls%d charge%d stats%+v", calls, charged, c.Stats())
		}
	}
}

func TestCollectorWorkCheckpointReturnsExactBoundaryError(t *testing.T) {
	stop := errors.New("checkpoint stopped")
	for _, boundary := range []int{1, 2, 3} {
		t.Run(fmt.Sprint(boundary), func(t *testing.T) {
			calls := 0
			c := newTestCollector(t, Options{BufferLimit: 1024, Checkpoint: func(uint64) error {
				calls++
				if calls == boundary {
					return stop
				}
				return nil
			}})
			defer c.Close()
			err := c.Put([]byte("key"), bytes.Repeat([]byte("v"), 2048))
			if !errors.Is(err, stop) || len(c.runFiles) != 0 {
				t.Fatalf("spill accepted stop at%d: %v files%d", boundary, err, len(c.runFiles))
			}
		})
	}
	c := newTestCollector(t, Options{BufferLimit: 128})
	defer c.Close()
	for i := 0; i < 40; i++ {
		if err := c.Put([]byte(fmt.Sprint(i)), bytes.Repeat([]byte("v"), 80)); err != nil {
			t.Fatal(err)
		}
	}
	calls, visits := 0, 0
	// Run open is a boundary before the first header read; each reader's
	// prefetch is also charged before priming or visitor callbacks occur.
	c.opts.Checkpoint = func(n uint64) error {
		calls++
		if n > 0 {
			return stop
		}
		return nil
	}
	_, err := c.Iterate(func([]byte, []byte) error { visits++; return nil })
	if !errors.Is(err, stop) || visits != 0 || calls < 2 {
		t.Fatalf("merge opening failed to yield %v calls%d visits%d", err, calls, visits)
	}
}
