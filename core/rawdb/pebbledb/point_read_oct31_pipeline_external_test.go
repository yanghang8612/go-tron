package pebbledb_test

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"

	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/blockbuffer"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/rawdb/pebbledb"
	"github.com/tronprotocol/go-tron/core/state/kvdomains"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	"google.golang.org/protobuf/proto"
)

const oct31PipelineRows = 8192

var oct31PermissionKey = []byte("owner")

type oct31CaptureKV struct{ key, value []byte }

func (w *oct31CaptureKV) Put(k, v []byte) error {
	w.key = bytes.Clone(k)
	w.value = bytes.Clone(v)
	return nil
}
func (w *oct31CaptureKV) Delete([]byte) error {
	return fmt.Errorf("unexpected delete while deriving schema key")
}
func oct31Owner(i int) common.Address {
	var x [20]byte
	binary.BigEndian.PutUint64(x[12:], uint64(i+1))
	return common.BytesToAddress(x[:])
}
func oct31Permission(i int) []byte {
	p := &corepb.Permission{Id: 2, PermissionName: strings.Repeat("p", 32), Threshold: int64(i + 1), Operations: bytes.Repeat([]byte{0xff}, 32)}
	for j := 0; j < 5; j++ {
		p.Keys = append(p.Keys, &corepb.Key{Address: oct31Owner(i + j).Bytes(), Weight: int64(j + 1)})
	}
	b, e := proto.MarshalOptions{Deterministic: true}.Marshal(p)
	if e != nil {
		panic(e)
	}
	return b
}
func oct31Physical(i int, value []byte) ([]byte, []byte) {
	w := &oct31CaptureKV{}
	if e := rawdb.WriteStateKVLatest(w, oct31Owner(i), 1, kvdomains.AccountPermissionAux, oct31PermissionKey, value); e != nil {
		panic(e)
	}
	return w.key, w.value
}

var oct31CatalogOnce sync.Once
var oct31CatalogKeys, oct31CatalogEncoded, oct31CatalogValues [][]byte

func oct31Rows() ([][]byte, [][]byte) {
	oct31CatalogOnce.Do(func() {
		oct31CatalogKeys = make([][]byte, 2*oct31PipelineRows)
		oct31CatalogEncoded = make([][]byte, 2*oct31PipelineRows)
		oct31CatalogValues = make([][]byte, 2*oct31PipelineRows)
		for i := range oct31CatalogKeys {
			value := oct31Permission(i)
			oct31CatalogValues[i] = value
			oct31CatalogKeys[i], oct31CatalogEncoded[i] = oct31Physical(i, value)
		}
	})
	return oct31CatalogKeys[:oct31PipelineRows], oct31CatalogEncoded[:oct31PipelineRows]
}

// Image generation is separate from timing. Every benchmark process copies the
// exact frozen physical image, and every arm shares its asserted source shape.
func TestOct31WritePipelineImages(t *testing.T) {
	out := os.Getenv("OCT31_PIPELINE_IMAGE_OUT")
	if out == "" {
		t.Skip("explicit image output required")
	}
	keys, vals := oct31Rows()
	for _, s := range []struct {
		name           string
		l0             int
		middle, memory bool
	}{{"middle", 2, true, false}, {"hot", 2, false, false}, {"pressure", 16, false, false}, {"memory", 0, false, true}} {
		dir := filepath.Join(out, s.name)
		if _, e := os.Stat(dir); !os.IsNotExist(e) {
			t.Fatalf("refusing existing image %s", dir)
		}
		pebbledb.Oct31SeedPipelineImage(t, dir, keys, vals, s.l0, s.middle, s.memory)
	}
}

type oct31Request struct {
	owner     int
	key, want []byte
	present   bool
}
type oct31RunShape struct {
	name, image  string
	cacheKiB     int
	warm         bool
	reads, width int
	fast         bool
}

func oct31Shape(name string) oct31RunShape {
	s := oct31RunShape{name: name, image: "middle", cacheKiB: 64, reads: 256, width: 1}
	switch name {
	case "limited":
	case "fitwarm":
		s.cacheKiB, s.warm = 4096, true
	case "fast":
		s.fast = true
	case "short":
		s.reads = 16
	case "memory":
		s.image = "memory"
	case "hot":
		s.image, s.cacheKiB, s.warm = "hot", 4096, true
	case "pressure17":
		s.image = "pressure"
	case "simultaneous4":
		s.width = 4
	default:
		panic("unknown pipeline shape " + name)
	}
	return s
}

func oct31PipelineLife(t testing.TB, d *pebbledb.Oct31PipelineStore, s oct31RunShape, life int, commit bool) string {
	t.Helper()
	b := blockbuffer.New(d)
	b.SetBaseReadCacheSize(8 << 20)
	b.BeginBlock(common.Hash{byte(life), byte(life >> 8)}, uint64(life+1))
	widths := s.width
	plans := make([]*rawdb.StatePrefetchPlan, widths)
	rows := make([][]oct31Request, widths)
	for worker := 0; worker < widths; worker++ {
		count := s.reads
		if s.name == "short" {
			count = []int{0, 1, 4, 16}[life%4]
		}
		plan := rawdb.NewStatePrefetchPlan(count)
		plans[worker] = plan
		rows[worker] = make([]oct31Request, count)
		for j := 0; j < count; j++ {
			i := ((life*256+worker*1024+j)*4051 + 97) & (oct31PipelineRows - 1)
			if j%20 < 11 {
				i += oct31PipelineRows
			}
			want := oct31CatalogValues[i]
			key := oct31CatalogKeys[i]
			r := oct31Request{owner: i, key: key, want: want, present: i < oct31PipelineRows}
			if !r.present {
				r.want = nil
			}
			id := plan.AddKV(oct31Owner(i), 1, kvdomains.AccountPermissionAux, oct31PermissionKey)
			if id != j {
				t.Fatal("trace key dedup changed")
			}
			// Exact overlay value and tombstone precede durable lookup. Fast control
			// resolves all odd rows as cache positives/negatives and all even as overlay.
			if s.name != "short" && (j == 0 || s.fast && j%2 == 0) {
				if e := rawdb.WriteStateKVLatest(b, oct31Owner(i), 1, kvdomains.AccountPermissionAux, oct31PermissionKey, want); e != nil {
					t.Fatal(e)
				}
				r.want, r.present = want, true
			}
			if s.name != "short" && j == 1 {
				if e := b.Delete(key); e != nil {
					t.Fatal(e)
				}
				r.want, r.present = nil, false
			}
			if s.name != "short" && (j == 2 || j == 3 || s.fast && j%2 == 1 && j != 1) {
				v, ok, e := rawdb.PrefetchStateKVLatest(b, oct31Owner(i), 1, kvdomains.AccountPermissionAux, oct31PermissionKey)
				if e != nil || ok != r.present || !bytes.Equal(v, r.want) {
					t.Fatalf("cache seed%d: %v", j, e)
				}
			}
			rows[worker][j] = r
		}
	}
	// Four existing warmup workers, with a barrier after the first real View,
	// guarantee simultaneous live cursors/readStates without parallel VM work.
	if widths > 1 {
		var arrivals atomic.Int32
		ready := make(chan struct{})
		d.Counts.Barrier = func() {
			if arrivals.Add(1) == int32(widths) {
				close(ready)
			}
			<-ready
		}
	}
	beforeSnaps, beforeCursors, beforeReads := d.Counts.Snapshots.Load(), d.Counts.Cursors.Load(), d.Counts.ReadAt.Load()
	var wg sync.WaitGroup
	errs := make(chan error, widths)
	for w := 0; w < widths; w++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			seen := make([]bool, len(rows[worker]))
			e := plans[worker].Execute(b, func(id int, value []byte, present bool, err error) error {
				r := rows[worker][id]
				if seen[id] {
					return fmt.Errorf("duplicate callback")
				}
				seen[id] = true
				if err != nil || present != r.present || !bytes.Equal(value, r.want) {
					return fmt.Errorf("callback%d present=%t want%t err%v", id, present, r.present, err)
				}
				if present {
					var p corepb.Permission
					if e := proto.Unmarshal(value, &p); e != nil {
						return e
					}
					if p.Threshold != int64(r.owner+1) {
						return fmt.Errorf("decoded permission mismatch")
					}
				}
				return nil
			})
			if e == nil {
				for _, yes := range seen {
					if !yes {
						e = fmt.Errorf("missing callback")
						break
					}
				}
			}
			errs <- e
		}(w)
	}
	wg.Wait()
	d.Counts.Barrier = nil
	close(errs)
	for e := range errs {
		if e != nil {
			t.Fatal(e)
		}
	}
	expectedReaders := uint64(widths)
	if s.fast || len(rows[0]) == 0 {
		expectedReaders = 0
	}
	if d.Counts.Snapshots.Load()-beforeSnaps != expectedReaders || d.Counts.Cursors.Load()-beforeCursors != expectedReaders {
		t.Fatalf("first-round actualreader count mismatch: snapshots%d cursors%d want%d", d.Counts.Snapshots.Load()-beforeSnaps, d.Counts.Cursors.Load()-beforeCursors, expectedReaders)
	}
	if widths == 4 && d.Counts.Peak.Load() != 4 {
		t.Fatalf("actual simultaneous cursors%d want4", d.Counts.Peak.Load())
	}
	if s.fast && (d.Counts.Snapshots.Load() != beforeSnaps || d.Counts.ReadAt.Load() != beforeReads) {
		t.Fatal("all overlay/cache hits constructed a snapshot or read SST")
	}
	// The identical second plan must consume positive and negative cache entries
	// without constructing any snapshot/cursor or issuing an SST ReadAt.
	snaps, cursors, reads := d.Counts.Snapshots.Load(), d.Counts.Cursors.Load(), d.Counts.ReadAt.Load()
	for w, plan := range plans {
		if e := plan.Execute(b, func(id int, v []byte, ok bool, e error) error {
			r := rows[w][id]
			if e != nil || ok != r.present || !bytes.Equal(v, r.want) {
				return fmt.Errorf("second-round mismatch: %v", e)
			}
			return nil
		}); e != nil {
			t.Fatal(e)
		}
	}
	if d.Counts.Snapshots.Load() != snaps || d.Counts.Cursors.Load() != cursors || d.Counts.ReadAt.Load() != reads {
		t.Fatal("second-round cache hit opened reader")
	}
	h := sha256.New()
	for _, rs := range rows {
		for _, r := range rs {
			v, ok, e := rawdb.ReadStateKVLatestNoCopy(b, oct31Owner(r.owner), 1, kvdomains.AccountPermissionAux, oct31PermissionKey)
			if e != nil || ok != r.present || !bytes.Equal(v, r.want) {
				t.Fatalf("canonical consumer mismatch: %v", e)
			}
			h.Write(r.key)
			if ok {
				h.Write([]byte{1})
				h.Write(v)
			} else {
				h.Write([]byte{0})
			}
		}
	}
	if !commit {
		b.Discard()
		if d.Counts.Active.Load() != 0 || d.Counts.Retained.Load() != 0 {
			t.Fatal("read lifecycle leaked cursor ownership")
		}
		return hex.EncodeToString(h.Sum(nil))
	}
	b.CommitBlock()
	if e := b.FlushUpTo(uint64(life+1), d); e != nil {
		t.Fatal(e)
	}
	// Verify the committed physical bytes before restoring the fixture cut.
	for _, rs := range rows {
		for j, r := range rs {
			if s.name == "short" || j > 1 && !s.fast {
				continue
			}
			v, ok, e := rawdb.ReadStateKVLatest(d, oct31Owner(r.owner), 1, kvdomains.AccountPermissionAux, oct31PermissionKey)
			if e != nil || ok != r.present || !bytes.Equal(v, r.want) {
				t.Fatalf("post-commit durable mismatch: %v", e)
			}
		}
	}
	// Each lifecycle restores the small mutated overlay rows to the shared frozen
	// logical source; fixed N therefore also fixes write/flush/memtable horizons.
	for _, rs := range rows {
		for j, r := range rs {
			if s.name == "short" || j > 1 && !s.fast {
				continue
			}
			if r.owner < oct31PipelineRows {
				if e := rawdb.WriteStateKVLatest(d, oct31Owner(r.owner), 1, kvdomains.AccountPermissionAux, oct31PermissionKey, oct31CatalogValues[r.owner]); e != nil {
					t.Fatal(e)
				}
			} else if e := d.Delete(r.key); e != nil {
				t.Fatal(e)
			}
		}
	}
	b.Discard()
	if d.Counts.Active.Load() != 0 || d.Counts.Retained.Load() != 0 {
		t.Fatal("cursor resources remained after batch close")
	}
	return hex.EncodeToString(h.Sum(nil))
}

func BenchmarkOct31PrefetchPipeline(b *testing.B) {
	image := os.Getenv("OCT31_PIPELINE_IMAGE")
	if image == "" {
		b.Fatal("frozen physical image required")
	}
	s := oct31Shape(os.Getenv("OCT31_PIPELINE_SHAPE"))
	keys, values := oct31Rows()
	var begin, end runtime.MemStats
	runtime.ReadMemStats(&begin)
	d := pebbledb.Oct31OpenPipelineImage(b, filepath.Join(image, s.image), s.cacheKiB, s.warm, keys, values, s.image == "memory")
	if e := d.DrainSetupWAL(); e != nil {
		b.Fatal(e)
	}
	c := d.Counts
	initialWrites := d.WriteAccounting()
	initialLayout := d.Layout()
	beforeCalls, beforeBytes := c.ReadAt.Load(), c.Bytes.Load()
	var cpuBegin, cpuEnd syscall.Rusage
	if e := syscall.Getrusage(syscall.RUSAGE_SELF, &cpuBegin); e != nil {
		b.Fatal(e)
	}
	digest := sha256.New()
	b.ReportAllocs()
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		digest.Write([]byte(oct31PipelineLife(b, d, s, n, false)))
	}
	finalWrites := d.WriteAccounting()
	if initialWrites != finalWrites {
		b.Fatalf("read lifecycle mutated WAL/flush counters: %v -> %v", initialWrites, finalWrites)
	}
	finalLayout := d.Layout()
	{
		for i, v := range initialLayout {
			if finalLayout[i] != v {
				b.Fatalf("fixed fanout changed: initial%v final%v", initialLayout, finalLayout)
			}
		}
	}
	if e := d.Close(); e != nil {
		b.Fatal(e)
	}
	runtime.ReadMemStats(&end)
	if e := syscall.Getrusage(syscall.RUSAGE_SELF, &cpuEnd); e != nil {
		b.Fatal(e)
	}
	b.StopTimer()
	if s.name == "memory" && c.ReadAt.Load() != beforeCalls {
		b.Fatalf("memtable-only horizon issued SST ReadAt%d", c.ReadAt.Load()-beforeCalls)
	}
	if c.Active.Load() != 0 || c.Retained.Load() != 0 || c.Cursors.Load() != c.Closed.Load() || c.Snapshots.Load() != c.SnapshotClosed.Load() {
		b.Fatal("lifecycle close mismatch")
	}
	cpuNS := func(u syscall.Rusage) int64 {
		return u.Utime.Sec*1e9 + int64(u.Utime.Usec)*1000 + u.Stime.Sec*1e9 + int64(u.Stime.Usec)*1000
	}
	b.ReportMetric(float64(cpuNS(cpuEnd)-cpuNS(cpuBegin))/float64(b.N), "pipeline-CPU-ns/op")
	b.ReportMetric(float64(c.ReadAt.Load()-beforeCalls)/float64(b.N), "ReadAt/op")
	b.ReportMetric(float64(c.Bytes.Load()-beforeBytes)/float64(b.N), "VFS-B/op")
	b.ReportMetric(float64(c.Opens.Load())/float64(b.N), "table-opens/op")
	b.ReportMetric(float64(c.Fallback.Load())/float64(b.N), "fallback/op")
	b.ReportMetric(float64(c.Peak.Load()), "live-cursors-peak")
	b.ReportMetric(float64(c.RetainedPeak.Load()), "live-cursor-pair-highwater")
	b.ReportMetric(float64(end.TotalAlloc-begin.TotalAlloc)/float64(b.N), "lifecycle-B/op")
	fmt.Printf("PIPELINE_TRACE %s N=%d SHA256=%x SNAPSHOTS=%d CURSORS=%d CLOSED=%d LAYOUT_START=%v LAYOUT_BEFORE_CLOSE=%v WAL_START=%v WAL_BEFORE_CLOSE=%v\n", s.name, b.N, digest.Sum(nil), c.Snapshots.Load(), c.Cursors.Load(), c.Closed.Load(), initialLayout, finalLayout, initialWrites, finalWrites)
}

func TestOct31PrefetchPipelineLifecycle(t *testing.T) {
	images := t.TempDir()
	t.Setenv("OCT31_PIPELINE_IMAGE_OUT", images)
	TestOct31WritePipelineImages(t)
	keys, values := oct31Rows()
	names := []string{"limited", "fitwarm", "fast", "short", "memory", "hot", "pressure17", "simultaneous4", "commit_correctness"}
	for _, name := range names {
		t.Run(name, func(t *testing.T) {
			commit := name == "commit_correctness"
			shapeName := name
			if commit {
				shapeName = "limited"
			}
			s := oct31Shape(shapeName)
			d := pebbledb.Oct31OpenPipelineImage(t, filepath.Join(images, s.image), s.cacheKiB, s.warm, keys, values, s.image == "memory")
			if e := d.DrainSetupWAL(); e != nil {
				t.Fatal(e)
			}
			writes, layout, reads := d.WriteAccounting(), d.Layout(), d.Counts.ReadAt.Load()
			for n := 0; n < 4; n++ {
				oct31PipelineLife(t, d, s, n, commit)
			}
			if !commit {
				if d.WriteAccounting() != writes {
					t.Fatalf("read-only unit changed WAL/flush counters %v -> %v", writes, d.WriteAccounting())
				}
				for i, v := range layout {
					if d.Layout()[i] != v {
						t.Fatal("read-only unit changed layout")
					}
				}
				if s.name == "memory" && d.Counts.ReadAt.Load() != reads {
					t.Fatal("memory source read anSST")
				}
			}
			if e := d.Close(); e != nil {
				t.Fatal(e)
			}
			if d.Counts.Cursors.Load() != d.Counts.Closed.Load() || d.Counts.Snapshots.Load() != d.Counts.SnapshotClosed.Load() {
				t.Fatal("leaked readers")
			}
		})
	}
}
