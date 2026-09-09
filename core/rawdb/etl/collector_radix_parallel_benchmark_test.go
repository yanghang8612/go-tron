package etl

import (
	"encoding/binary"
	"fmt"
	"math/bits"
	"slices"
	"testing"
)

// Same entries, pooled radix algorithm, key/seq oracle and reused order/scratch
// for every worker count. No run writing, disk merge or workload change is
// included; this measures the CPU sort phase, not end-to-end cold throughput.
func BenchmarkParallelRadixEntryOrder(b *testing.B) {
	for _, shape := range []string{"random", "duplicates", "shared-prefix", "ordered"} {
		entries := parallelRadixFixture(shape, 1<<18)
		want := identityEntryOrder(len(entries))
		sortEntryOrderComparison(want, entries)
		for _, workers := range []int{0, 1, 2, 4} {
			label := fmt.Sprintf("workers=%d", workers)
			if workers == 0 {
				label = "legacy-serial"
			}
			b.Run(shape+"/"+label, func(b *testing.B) {
				order, scratch := identityEntryOrder(len(entries)), make([]uint32, len(entries))
				sortOrder := func() (int, error) {
					if workers == 0 {
						legacyRadixSortEntryOrder(order, scratch, entries)
						return 0, nil
					}
					return radixSortEntryOrderWorkers(order, scratch, entries, entryOrderRange{hi: len(entries)}, workers, nil)
				}
				groups, err := sortOrder()
				if err != nil || !slices.Equal(order, want) {
					b.Fatal("complete sort differs from key/sequence oracle")
				}
				b.ReportAllocs()
				b.SetBytes(int64(len(entries) * 48))
				b.ResetTimer()
				for range b.N {
					for i := range order {
						order[i] = uint32(i)
					}
					if _, err := sortOrder(); err != nil {
						b.Fatal(err)
					}
				}
				b.StopTimer()
				b.ReportMetric(float64(groups), "parallel_groups")
				if !slices.Equal(order, want) {
					b.Fatal("timed output differs from oracle")
				}
			})
		}
	}
}

// Frozen pre-parallel implementation, retained only for honest before/after
// benchmarks. It shares production entry/order/stack pools but has none of the
// new interruption or scheduling checks. Benchmark outputs use an independent
// key/seq comparison oracle.
func legacyRadixSortEntryOrder(order, scratch []uint32, entries []entry) {
	stackBuffer := collectorRadixRangePool.Get().(*[]entryOrderRange)
	stack := append((*stackBuffer)[:0], entryOrderRange{hi: len(order)})
	defer func() {
		*stackBuffer = stack[:0]
		if cap(*stackBuffer) <= collectorRadixRangePoolMaxCapacity {
			collectorRadixRangePool.Put(stackBuffer)
		}
	}()
	for len(stack) > 0 {
		last := len(stack) - 1
		current := stack[last]
		stack = stack[:last]
		for {
			length := current.hi - current.lo
			if length < 2 {
				break
			}
			if length < radixEntryOrderMin {
				sortEntryOrderComparison(order[current.lo:current.hi], entries)
				break
			}
			var counts [257]int
			nonEmpty := 0
			onlyBucket := 0
			for _, row := range order[current.lo:current.hi] {
				key := entries[row].key
				bucket := 0
				if current.depth < len(key) {
					bucket = int(key[current.depth]) + 1
				}
				if counts[bucket] == 0 {
					nonEmpty++
					onlyBucket = bucket
				}
				counts[bucket]++
			}
			if nonEmpty == 1 {
				if onlyBucket == 0 {
					slices.SortFunc(order[current.lo:current.hi], func(left, right uint32) int {
						a, b := entries[left].seq, entries[right].seq
						if a < b {
							return -1
						}
						if a > b {
							return 1
						}
						return 0
					})
					break
				}
				current.depth = legacySharedEntryKeyPrefixDepth(order[current.lo:current.hi], entries, current.depth+1)
				continue
			}

			var starts [257]int
			next := current.lo
			for bucket, count := range counts {
				starts[bucket] = next
				next += count
			}
			positions := starts
			for _, row := range order[current.lo:current.hi] {
				key := entries[row].key
				bucket := 0
				if current.depth < len(key) {
					bucket = int(key[current.depth]) + 1
				}
				scratch[positions[bucket]] = row
				positions[bucket]++
			}
			copy(order[current.lo:current.hi], scratch[current.lo:current.hi])

			if counts[0] > 1 {
				lo, hi := starts[0], starts[0]+counts[0]
				slices.SortFunc(order[lo:hi], func(left, right uint32) int {
					a, b := entries[left].seq, entries[right].seq
					if a < b {
						return -1
					}
					if a > b {
						return 1
					}
					return 0
				})
			}
			for bucket := 256; bucket >= 1; bucket-- {
				if counts[bucket] > 1 {
					stack = append(stack, entryOrderRange{
						lo:    starts[bucket],
						hi:    starts[bucket] + counts[bucket],
						depth: current.depth + 1,
					})
				}
			}
			break
		}
	}
}

// legacySharedEntryKeyPrefixDepth skips a range's common continuation after the
// radix pass has proved that every key contains the same byte at start-1. A
// last/middle-row probe keeps the common one-byte case O(1); only a candidate
// longer prefix triggers the full range scan. Eight-byte comparisons avoid
// revisiting long encoded accessor prefixes one byte and one full pass at a
// time.
func legacySharedEntryKeyPrefixDepth(order []uint32, entries []entry, start int) int {
	if len(order) < 2 {
		return start
	}
	reference := entries[order[0]].key
	depth := legacyCommonKeyPrefixDepth(reference, entries[order[len(order)-1]].key, start, len(reference))
	if depth == start {
		return start
	}
	depth = legacyCommonKeyPrefixDepth(reference, entries[order[len(order)/2]].key, start, depth)
	if depth == start {
		return start
	}
	for _, row := range order[1:] {
		depth = legacyCommonKeyPrefixDepth(reference, entries[row].key, start, depth)
		if depth == start {
			return start
		}
	}
	return depth
}

func legacyCommonKeyPrefixDepth(left, right []byte, start, limit int) int {
	limit = min(limit, len(right))
	index := start
	for index+8 <= limit {
		difference := binary.LittleEndian.Uint64(left[index:index+8]) ^ binary.LittleEndian.Uint64(right[index:index+8])
		if difference != 0 {
			return index + bits.TrailingZeros64(difference)/8
		}
		index += 8
	}
	for index < limit && left[index] == right[index] {
		index++
	}
	return index
}
