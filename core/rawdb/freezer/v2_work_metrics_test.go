package freezer

import (
	"bytes"
	"context"
	"errors"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/metrics"
	"github.com/klauspost/compress/zstd"
)

func assertV2WorkIdle(t *testing.T, s map[string]int64) {
	t.Helper()
	for _, name := range []string{"waiting/frames", "waiting/raw_bytes", "waiting/raw_capacity_bytes", "ready_to_write/frames", "ready_to_write/raw_bytes", "ready_to_write/compressed_bytes", "ready_to_write/compressed_capacity_bytes", "pending/frames", "pending/raw_capacity_bytes", "pending/destination_capacity_bytes", "idle_pool/input_capacity_bytes", "idle_pool/output_capacity_bytes", "encode/active_raw_bytes", "encode/active_raw_capacity_bytes", "encode/active_destination_initial_capacity_bytes", "preparation/owned_rows", "preparation/owned_input_capacity_bytes"} {
		if s[name] != 0 {
			t.Errorf("%s remains %d", name, s[name])
		}
	}
	for _, name := range v2StageNames {
		if s[name+"/active"] != 0 {
			t.Errorf("stage %s still active", name)
		}
	}
}
func TestV2WorkOpenSpansAndQueueLeaseConservation(t *testing.T) {
	o := newV2WorkObserver(metrics.NewRegistry())
	a := o.begin(v2StageEncode)
	b := o.begin(v2StageEncode)
	first := o.snapshot()
	time.Sleep(time.Millisecond)
	second := o.snapshot()
	if second["encode/worker_wall_nanos"] <= first["encode/worker_wall_nanos"] || second["encode/busy_wall_nanos"] <= first["encode/busy_wall_nanos"] || second["encode/active"] != 2 {
		t.Fatal("open workers invisible")
	}
	if second["encode/worker_wall_nanos"] < second["encode/busy_wall_nanos"] {
		t.Fatal("worker sum below union")
	}
	a.end()
	b.end()
	one := &v2FrameEncodeJob{raw: v2RawFrame{data: make([]byte, 3, 20)}, dst: make([]byte, 0, 40)}
	two := &v2FrameEncodeJob{raw: v2RawFrame{data: make([]byte, 5, 30)}, dst: make([]byte, 0, 10)}
	o.admit(one)
	o.admit(two)
	o.startJob(one)
	o.ready(one, v2EncodedFrame{compressed: make([]byte, 7, 60)})
	s := o.snapshot()
	if s["waiting/frames"] != 1 || s["ready_to_write/frames"] != 1 || s["pending/raw_capacity_bytes"] != 50 || s["pending/destination_capacity_bytes"] != 70 {
		t.Fatalf("bad lease stages: %v", s)
	}
	o.writing(one)
	o.release(one, false)
	if o.snapshot()["waiting/frames"] != 1 {
		t.Fatal("one pipeline cleared another")
	}
	o.release(two, true)
	assertV2WorkIdle(t, o.snapshot())
	if o.snapshot()["abandoned/frames"] != 1 {
		t.Fatal("abandoned job not recorded")
	}
}
func TestV2WorkWholeSegmentAllEncodingPaths(t *testing.T) {
	for _, workers := range []int{1, 8} {
		for _, codec := range []uint32{v2CodecDefault, v2CodecBodiesRawDict, v2CodecBodiesTrainedDict} {
			before := v2Work.snapshot()
			path := filepath.Join(t.TempDir(), "observed.seg")
			rows := v2PipelineRecords(32, 1024)
			read := func(n uint64) ([]byte, error) { return rows[n], nil }
			if err := writeV2SegmentProfileWithWorkers(path, 0, 32, 8, codec, zstd.SpeedDefault, read, read, workers); err != nil {
				t.Fatal(err)
			}
			reader, err := openV2Segment(path, "bodies")
			if err != nil {
				t.Fatal(err)
			}
			var raw, compressed int64
			for _, f := range reader.frames {
				raw += int64(f.uncompressedLen)
				compressed += int64(f.compressedLen)
			}
			reader.Close()
			after := v2Work.snapshot()
			for field, want := range map[string]int64{"encode/completed_frames": 4, "encode/started_frames": 4, "encode/started_raw_bytes": raw, "encode/completed_raw_bytes": raw, "encode/completed_compressed_bytes": compressed, "frame_write/completed_frames": 4, "frame_write/returned_compressed_bytes": compressed, "segment/completed": 1} {
				if after[field]-before[field] != want {
					t.Fatalf("workers%d codec%d %s delta=%d want%d", workers, codec, field, after[field]-before[field], want)
				}
			}
			assertV2WorkIdle(t, after)
		}
	}
}
func TestV2WorkConcurrentPipelineFailureDoesNotClearOther(t *testing.T) {
	newEncoder := func() (*zstd.Encoder, error) { return zstd.NewWriter(nil, zstd.WithEncoderConcurrency(1)) }
	primary, _ := newEncoder()
	defer primary.Close()
	entered, release := make(chan struct{}), make(chan struct{})
	done := make(chan error, 1)
	wantErr := errors.New("write canceled")
	var once sync.Once
	readCount := 0
	go func() {
		done <- writeV2FramePipeline(2, 10, primary, newEncoder, func(buf []byte) (v2RawFrame, error) {
			readCount++
			return v2RawFrame{records: 1, data: append(buf, bytes.Repeat([]byte{byte(readCount)}, 4096)...)}, nil
		}, func(v2EncodedFrame) error { once.Do(func() { close(entered) }); <-release; return wantErr })
	}()
	<-entered
	deadline := time.Now().Add(time.Second)
	for v2Work.snapshot()["ready_to_write/frames"] == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	active := v2Work.snapshot()
	if active["pending/frames"] == 0 || active["ready_to_write/frames"] == 0 {
		t.Fatal("completed-awaiting-write work invisible")
	}
	other, _ := newEncoder()
	err := writeV2FramePipeline(2, 3, other, newEncoder, func([]byte) (v2RawFrame, error) { return v2RawFrame{}, errors.New("source canceled") }, func(v2EncodedFrame) error { return nil })
	other.Close()
	if err == nil || v2Work.snapshot()["pending/frames"] == 0 {
		t.Fatal("failed pipeline erased another owner")
	}
	close(release)
	if !errors.Is(<-done, wantErr) {
		t.Fatal("write failure changed")
	}
	assertV2WorkIdle(t, v2Work.snapshot())
}
func TestV2WorkEncodePanicAndPreparationCancelClearOwnedOnly(t *testing.T) {
	before := v2Work.snapshot()
	func() {
		defer func() {
			if recover() == nil {
				t.Error("original nil encoder panic suppressed")
			}
		}()
		encodeV2FrameInto(nil, v2RawFrame{data: []byte("x")}, nil)
	}()
	after := v2Work.snapshot()
	if after["encode/aborted_frames"]-before["encode/aborted_frames"] != 1 {
		t.Fatal("aborted encode missing")
	}
	assertV2WorkIdle(t, after)
	ctx, cancel := context.WithCancel(context.Background())
	entered, finish := make(chan struct{}), make(chan struct{})
	var prepareOnce sync.Once
	r := newV2PreparedReader(ctx, 0, 4, 2, func(uint64) ([]byte, []byte, error) { return []byte("data"), nil, nil }, func(uint64, []byte, []byte) ([]byte, error) {
		prepareOnce.Do(func() { close(entered) })
		<-finish
		return []byte("prepared"), nil
	})
	readDone := make(chan struct{})
	go func() { defer close(readDone); _, _ = r.Read(0) }()
	<-entered
	cancel()
	close(finish)
	<-readDone
	r.Close()
	assertV2WorkIdle(t, v2Work.snapshot())
}
func TestV2WorkRefreshIncludesEpochOpenStageAndConcurrentScrape(t *testing.T) {
	span := v2Work.begin(v2StageTrain)
	before := v2Work.snapshot()
	RefreshV2WorkMetrics()
	time.Sleep(time.Millisecond)
	RefreshV2WorkMetrics()
	if v2Work.gauges["dictionary_train/worker_wall_nanos"].Snapshot().Value() <= before["dictionary_train/worker_wall_nanos"] || v2Work.gauges["epoch_unix_nanos"].Snapshot().Value() == 0 || v2Work.gauges["refresh_unix_nanos"].Snapshot().Value() == 0 {
		t.Fatal("stale/zero open snapshot")
	}
	span.end()
	var wg sync.WaitGroup
	for range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range 100 {
				s := v2Work.begin(v2StageEncode)
				s.end()
				RefreshV2WorkMetrics()
			}
		}()
	}
	wg.Wait()
	assertV2WorkIdle(t, v2Work.snapshot())
}
