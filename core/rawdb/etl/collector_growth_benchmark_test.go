package etl

import (
	"fmt"
	"testing"
)

// A cold collector models a pool miss after GC. Zero byte views isolate the
// sortable metadata growth attributed to Collector.append by sync profiles.
func BenchmarkCollectorColdRows(b *testing.B) {
	for _, count := range []int{1, 16, 1024, 65536, 262144} {
		b.Run(fmt.Sprint(count), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				buffer := []entry(nil)
				c := Collector{opts: Options{BufferLimit: 1 << 30}, rowBuffer: &buffer}
				for row := 0; row < count; row++ {
					if err := c.append(entry{op: opPut}); err != nil {
						b.Fatal(err)
					}
				}
				if len(c.rows) != count {
					b.Fatal(len(c.rows))
				}
			}
		})
	}
}
