package cache

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"github.com/cockroachdb/pebble/internal/base"
	"testing"
)

// Common-only trace: this source compiles unchanged against original B. It hashes
// every returned byte and pre-existing public cache metric after each operation.
// New observer IDs/accounting are deliberately absent from the comparison.
func TestCausalityPolicyTrace(t *testing.T) {
	c := newShards(1<<20, 64)
	defer c.Unref()
	digest := sha256.New()
	state := uint64(0x123456789abcdef)
	record := func(value []byte) {
		m := c.Metrics()
		if err := json.NewEncoder(digest).Encode(struct {
			Returned                  []byte
			Size, Count, Hits, Misses int64
			Clock                     ClockMetrics
		}{value, m.Size, m.Count, m.Hits, m.Misses, m.Clock}); err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i < 6000; i++ {
		state ^= state << 13
		state ^= state >> 7
		state ^= state << 17
		file := base.FileNum(1 + state%8).DiskFileNum()
		offset := (state >> 8) % 2048
		switch state % 12 {
		case 0, 1, 2, 3, 4, 5:
			size := 64 + int(state%1024)
			if i%127 == 0 {
				size = 32768
			}
			value := newValue(size)
			for j := range value.buf {
				value.buf[j] = byte(state + uint64(j))
			}
			handle := c.Set(1, file, offset, value)
			record(handle.Get())
			handle.Release()
		case 6, 7, 8:
			handle := c.Get(1, file, offset)
			record(handle.Get())
			handle.Release()
		case 9:
			c.Delete(1, file, offset)
			record(nil)
		case 10:
			c.EvictFile(1, file)
			record(nil)
		case 11:
			release := c.Reserve(int(state % 32768))
			record(nil)
			release()
			record(nil)
		}
	}
	t.Log("POLICY_TRACE_SHA256", hex.EncodeToString(digest.Sum(nil)))
}
