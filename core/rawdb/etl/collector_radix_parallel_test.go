package etl

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"runtime"
	"slices"
	"testing"
)

func parallelRadixFixture(shape string, rows int) []entry {
	rng := rand.New(rand.NewSource(909))
	entries := make([]entry, rows)
	keys := make([]byte, rows*48)
	for i := range entries {
		key := keys[i*48 : (i+1)*48 : (i+1)*48]
		switch shape {
		case "random":
			_, _ = rng.Read(key)
		case "shared-prefix":
			copy(key, bytes.Repeat([]byte{0x5a}, 32))
			_, _ = rng.Read(key[32:])
		case "duplicates":
			group := (i * 104729) % 1024
			key[0] = byte(group % 16)
			binary.BigEndian.PutUint64(key[40:], uint64(group))
		case "ordered":
			binary.BigEndian.PutUint64(key[:8], uint64(i))
		case "equal":
			copy(key, "one-terminal-key")
		default:
			panic("unknown radix fixture")
		}
		entries[i] = entry{key: key, seq: uint64(rows - i), op: opPut}
	}
	return entries
}

func identityEntryOrder(n int) []uint32 {
	order := make([]uint32, n)
	for i := range order {
		order[i] = uint32(i)
	}
	return order
}

func TestParallelRadixMatchesKeySequenceOracle(t *testing.T) {
	for _, shape := range []string{"random", "duplicates", "shared-prefix", "ordered", "equal"} {
		t.Run(shape, func(t *testing.T) {
			entries := parallelRadixFixture(shape, 2*radixEntryOrderParallelMin)
			// End-of-key must stay before byte 0 and byte 255, including when
			// it shares a partition with arbitrary binary snapshot keys.
			if shape != "equal" {
				entries[0].key = nil
				entries[1].key = []byte{0}
				entries[2].key = []byte{0, 0xff}
			}
			want := identityEntryOrder(len(entries))
			sortEntryOrderComparison(want, entries)
			for _, workers := range []int{1, 2, 4} {
				order := identityEntryOrder(len(entries))
				groups, err := radixSortEntryOrderWorkers(order, make([]uint32, len(order)), entries, entryOrderRange{hi: len(order)}, workers, nil)
				if err != nil || !slices.Equal(order, want) {
					t.Fatalf("workers=%d groups=%d differs from key/seq oracle: %v", workers, groups, err)
				}
				if shape != "equal" && workers > 1 && groups < 2 {
					t.Fatalf("workers=%d did not use independent partitions", workers)
				}
				if shape == "equal" && groups != 0 {
					t.Fatal("one terminal bucket must stay serial")
				}
			}
		})
	}
}

func TestParallelRadixGroupsBoundedAndDisjoint(t *testing.T) {
	ranges := make([]entryOrderRange, 256)
	for i := range ranges {
		ranges[i] = entryOrderRange{lo: i * 2048, hi: (i + 1) * 2048, depth: 7}
	}
	for _, workers := range []int{1, 2, 4, 64} {
		groups := groupRadixEntryRanges(ranges, workers)
		if workers == 1 {
			if groups != nil {
				t.Fatal("serial admission spawned groups")
			}
			continue
		}
		if len(groups) < 2 || len(groups) > min(workers, radixEntryOrderMaxWorkers) {
			t.Fatalf("worker limit exceeded: %d", len(groups))
		}
		next := 0
		for _, group := range groups {
			rows := 0
			for _, r := range group {
				if r != ranges[next] {
					t.Fatal("lost, overlapping or reordered task bucket")
				}
				next++
				rows += r.hi - r.lo
			}
			if rows < radixEntryOrderGroupMin {
				t.Fatal("small group admitted to parallel work")
			}
		}
		if next != len(ranges) {
			t.Fatal("tasks did not cover every bucket")
		}
	}
	if got := groupRadixEntryRanges([]entryOrderRange{{hi: 1 << 22}}, 4); got != nil {
		t.Fatal("oversized single bucket must not be split across writers")
	}
	if got := groupRadixEntryRanges([]entryOrderRange{{hi: 1 << 22}, {lo: 1 << 22, hi: 1<<22 + 2}}, 4); got != nil {
		t.Fatal("oversized bucket with only tiny independent work must stay serial")
	}
	for _, procs := range []int{1, 2, 4, 8, 64} {
		previous := runtime.GOMAXPROCS(procs)
		workers := radixEntryOrderWorkers(1 << 20)
		small := radixEntryOrderWorkers(radixEntryOrderParallelMin - 1)
		runtime.GOMAXPROCS(previous)
		if workers != min(4, max(1, procs/2)) || small != 1 {
			t.Fatalf("procs=%d workers=%d small=%d", procs, workers, small)
		}
	}
}

func TestParallelRadixCancellationJoinsAndLeavesInputReusable(t *testing.T) {
	entries := parallelRadixFixture("random", 1<<18)
	order, scratch := identityEntryOrder(len(entries)), make([]uint32, len(entries))
	groups := make([][]entryOrderRange, 4)
	for i := range groups {
		groups[i] = []entryOrderRange{{lo: i * (len(entries) / 4), hi: (i + 1) * (len(entries) / 4)}}
	}
	// Deliberately not atomic: cancellation callbacks are caller-owned and
	// must never be invoked concurrently by the worker goroutines.
	checks := 0
	err := runParallelRadixEntryRanges(order, scratch, entries, groups, func() bool {
		checks++
		return checks == 12
	})
	if !errors.Is(err, ErrLoadInterrupted) {
		t.Fatalf("lost cancellation: %v", err)
	}
	// Race testing detects a worker which outlives the return and still owns
	// either array. Original entry slices and sequence remain untouched.
	clear(scratch)
	for i := range order {
		order[i] = uint32(i)
		if entries[i].seq != uint64(len(entries)-i) {
			t.Fatal("sort mutated source metadata")
		}
	}
	_, err = radixSortEntryOrderWorkers(order, scratch, entries, entryOrderRange{hi: len(entries)}, 4, nil)
	if err != nil || !slices.IsSortedFunc(order, func(a, b uint32) int { return bytes.Compare(entries[a].key, entries[b].key) }) {
		t.Fatalf("reusing joined scratch failed: %v", err)
	}
}

func TestParallelRadixLongPrefixAndCancellation(t *testing.T) {
	// Large logical keys share owned backing storage; the sorter must not copy
	// their contents or recurse once per prefix byte. This represents 512 MiB
	// of logical key input with only 64 KiB of physical key storage.
	const prefixBytes, distinct = 4096, 16
	keys := make([][]byte, distinct)
	for i := range keys {
		keys[i] = bytes.Repeat([]byte{0x5a}, prefixBytes+1)
		keys[i][prefixBytes] = byte(i)
	}
	entries := make([]entry, radixEntryOrderParallelMin)
	for i := range entries {
		entries[i] = entry{key: keys[i%distinct], seq: uint64(len(entries) - i)}
	}
	order, scratch := identityEntryOrder(len(entries)), make([]uint32, len(entries))
	groups, err := radixSortEntryOrderWorkers(order, scratch, entries, entryOrderRange{hi: len(entries)}, 4, nil)
	if err != nil || groups != 4 {
		t.Fatalf("long prefix did not reach four disjoint buckets: groups=%d err=%v", groups, err)
	}
	for position, row := range order {
		keyID := position / (len(entries) / distinct)
		expected := len(entries) - distinct + keyID - (position%(len(entries)/distinct))*distinct
		if int(row) != expected {
			t.Fatalf("position=%d row=%d expected=%d", position, row, expected)
		}
	}
	// Even a single extreme common prefix is cooperatively cancellable. The
	// sentinel must propagate from the byte scan without becoming a key depth.
	long := bytes.Repeat([]byte{0xa5}, 8<<20)
	checks := 0
	depth := sharedEntryKeyPrefixDepthInterruptible([]uint32{0, 1}, []entry{{key: long}, {key: long}}, 1, func() bool {
		checks++
		return checks == 3
	})
	if depth != -1 || checks != 3 {
		t.Fatalf("extreme prefix ignored cancellation: depth=%d checks=%d", depth, checks)
	}
}

func TestParallelRadixCallbackPanicJoinsWorkers(t *testing.T) {
	entries := parallelRadixFixture("random", radixEntryOrderParallelMin)
	order, scratch := identityEntryOrder(len(entries)), make([]uint32, len(entries))
	groups := [][]entryOrderRange{{{hi: len(entries) / 2}}, {{lo: len(entries) / 2, hi: len(entries)}}}
	func() {
		defer func() {
			if got := recover(); got != "caller stopped" {
				t.Fatalf("caller panic not preserved: %v", got)
			}
		}()
		_ = runParallelRadixEntryRanges(order, scratch, entries, groups, func() bool { panic("caller stopped") })
	}()
	// A worker that still holds either array races with immediate reuse.
	clear(order)
	clear(scratch)
}

func TestParallelRadixCollectorInterruptedRetryPreservesLatestDelete(t *testing.T) {
	c := newTestCollector(t, Options{BufferLimit: 64 << 20})
	defer c.Close()
	want := make(map[string]string)
	for i := 0; i < radixEntryOrderParallelMin; i++ {
		key := []byte(fmt.Sprintf("%02x/shared-key/%08x", i%16, i%8192))
		if i%7 == 0 {
			if err := c.Delete(key); err != nil {
				t.Fatal(err)
			}
			delete(want, string(key))
		} else {
			value := []byte(fmt.Sprint(i))
			if err := c.Put(key, value); err != nil {
				t.Fatal(err)
			}
			want[string(key)] = string(value)
		}
	}
	writer := newRecordingWriter()
	checks := 0
	_, err := c.LoadInterruptible(writer, func() bool { checks++; return checks >= 80 })
	if !errors.Is(err, ErrLoadInterrupted) || c.loaded || len(c.rows) != radixEntryOrderParallelMin {
		t.Fatalf("interrupted load lost retry state: %v rows=%d loaded=%t", err, len(c.rows), c.loaded)
	}
	if _, err := c.Load(writer); err != nil {
		t.Fatal(err)
	}
	if len(writer.data) != len(want) {
		t.Fatalf("latest/delete cardinality=%d want=%d", len(writer.data), len(want))
	}
	for key, value := range want {
		if string(writer.data[key]) != value {
			t.Fatalf("latest operation differs at %q", key)
		}
	}
}

func TestParallelRadixCancelledSpillDoesNotPublishRun(t *testing.T) {
	c := newTestCollector(t, Options{BufferLimit: 64 << 20})
	defer c.Close()
	for _, e := range parallelRadixFixture("duplicates", radixEntryOrderParallelMin) {
		if err := c.Put(e.key, nil); err != nil {
			t.Fatal(err)
		}
	}
	checks := 0
	err := c.spillBufferInterruptible(func() bool { checks++; return checks > 50 })
	if !errors.Is(err, ErrLoadInterrupted) || len(c.runFiles) != 0 || c.stats.SpilledRuns != 0 || len(c.rows) != radixEntryOrderParallelMin {
		t.Fatalf("cancelled spill published or discarded input: %v", err)
	}
	files, err := os.ReadDir(c.dir)
	if err != nil || len(files) != 0 {
		t.Fatalf("cancelled spill retained temporary output: %v %v", files, err)
	}
	if err := c.spillBuffer(); err != nil || len(c.runFiles) != 1 {
		t.Fatalf("spill retry failed: %v", err)
	}
}

func TestParallelRadixSpillWriteCancellationRemovesPartialFile(t *testing.T) {
	c := newTestCollector(t, Options{BufferLimit: 64 << 20})
	defer c.Close()
	for _, e := range parallelRadixFixture("random", 8192) {
		if err := c.Put(e.key, nil); err != nil {
			t.Fatal(err)
		}
	}
	writeChecks := 0
	err := c.spillBufferInterruptible(func() bool {
		files, err := os.ReadDir(c.dir)
		if err != nil {
			t.Fatal(err)
		}
		if len(files) > 0 {
			writeChecks++
		}
		return writeChecks == 2 // A full 4096 rows reached the temporary file.
	})
	files, readErr := os.ReadDir(c.dir)
	if !errors.Is(err, ErrLoadInterrupted) || readErr != nil || len(files) != 0 || len(c.runFiles) != 0 || len(c.rows) != 8192 {
		t.Fatalf("partial spill escaped cancellation: err=%v read=%v files=%v runs=%v rows=%d", err, readErr, files, c.runFiles, len(c.rows))
	}
	if err := c.spillBuffer(); err != nil {
		t.Fatal(err)
	}
}

func TestParallelRadixSpilledRunsPreserveLatestDelete(t *testing.T) {
	c := newTestCollector(t, Options{BufferLimit: 64 << 20})
	defer c.Close()
	want := make(map[string]string)
	for run := 0; run < 2; run++ {
		for i := 0; i < radixEntryOrderParallelMin; i++ {
			key := []byte(fmt.Sprintf("%02x/shared-key/%08x", i%16, i%8192))
			if (i+run)%7 == 0 {
				if err := c.Delete(key); err != nil {
					t.Fatal(err)
				}
				delete(want, string(key))
			} else {
				value := []byte(fmt.Sprintf("%d/%d", run, i))
				if err := c.Put(key, value); err != nil {
					t.Fatal(err)
				}
				want[string(key)] = string(value)
			}
		}
		if run == 0 {
			if err := c.spillBuffer(); err != nil {
				t.Fatal(err)
			}
		}
	}
	// The first large run is already on disk; Load must sort/write the second
	// large run and merge both by key/seq before collapsing the latest op.
	writer := newRecordingWriter()
	stats, err := c.Load(writer)
	if err != nil || stats.SpilledRuns != 2 || stats.Applied != 8192 || len(writer.data) != len(want) {
		t.Fatalf("cross-run result differs: err=%v stats=%+v keys=%d want=%d", err, stats, len(writer.data), len(want))
	}
	for key, value := range want {
		if string(writer.data[key]) != value {
			t.Fatalf("cross-run latest operation differs at %q", key)
		}
	}
}
