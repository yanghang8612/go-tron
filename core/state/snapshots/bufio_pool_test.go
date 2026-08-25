package snapshots

import (
	"bytes"
	"context"
	"errors"
	"io"
	"testing"
)

type stateDomainChangeHistoryFastPathReader struct {
	*bytes.Reader
	writeToCalled bool
}

func (r *stateDomainChangeHistoryFastPathReader) WriteTo(io.Writer) (int64, error) {
	r.writeToCalled = true
	return 0, io.ErrUnexpectedEOF
}

type stateDomainChangeHistoryFastPathWriter struct {
	bytes.Buffer
	readFromCalled bool
}

func (w *stateDomainChangeHistoryFastPathWriter) ReadFrom(io.Reader) (int64, error) {
	w.readFromCalled = true
	return 0, io.ErrUnexpectedEOF
}

func TestStateDomainChangeHistoryWriterPoolDiscardsAbortedBytes(t *testing.T) {
	var aborted bytes.Buffer
	writer := acquireStateDomainChangeHistoryWriter(&aborted)
	retained := writer
	if _, err := writer.WriteString("aborted"); err != nil {
		t.Fatal(err)
	}
	if retained.Buffered() == 0 {
		t.Fatal("test did not leave buffered bytes to discard")
	}

	releaseStateDomainChangeHistoryWriter(&writer)
	if writer != nil {
		t.Fatal("released writer still reachable through caller")
	}
	if retained.Buffered() != 0 {
		t.Fatalf("released writer retained %d aborted bytes", retained.Buffered())
	}
	if aborted.Len() != 0 {
		t.Fatalf("release flushed aborted bytes: %q", aborted.Bytes())
	}

	var committed bytes.Buffer
	writer = acquireStateDomainChangeHistoryWriter(&committed)
	defer releaseStateDomainChangeHistoryWriter(&writer)
	if _, err := writer.WriteString("committed"); err != nil {
		t.Fatal(err)
	}
	if err := writer.Flush(); err != nil {
		t.Fatal(err)
	}
	if got := committed.String(); got != "committed" {
		t.Fatalf("reused writer output = %q, want committed", got)
	}
}

func TestStateDomainChangeAccessorGroupOffsetsPoolResetsLength(t *testing.T) {
	offsets := acquireStateDomainChangeAccessorGroupOffsets()
	*offsets = append(*offsets, 1, 2, 3)
	retained := offsets
	releaseStateDomainChangeAccessorGroupOffsets(&offsets)
	if offsets != nil {
		t.Fatal("released offsets still reachable through caller")
	}
	if len(*retained) != 0 {
		t.Fatalf("released offsets retained length %d", len(*retained))
	}

	offsets = acquireStateDomainChangeAccessorGroupOffsets()
	defer releaseStateDomainChangeAccessorGroupOffsets(&offsets)
	if len(*offsets) != 0 {
		t.Fatalf("acquired offsets length = %d, want zero", len(*offsets))
	}
}

func TestEventLogV4ValidationReaderPoolDropsPreviousSource(t *testing.T) {
	reader := acquireEventLogV4ValidationReader(bytes.NewReader([]byte("previous-source")))
	retained := reader
	var prefix [8]byte
	if _, err := io.ReadFull(reader, prefix[:]); err != nil {
		t.Fatal(err)
	}
	releaseEventLogV4ValidationReader(&reader)
	if reader != nil {
		t.Fatal("released event-log validation reader still reachable through caller")
	}
	if retained.Buffered() != 0 {
		t.Fatalf("released event-log validation reader retained %d bytes", retained.Buffered())
	}

	reader = acquireEventLogV4ValidationReader(bytes.NewReader([]byte("new")))
	defer releaseEventLogV4ValidationReader(&reader)
	got, err := io.ReadAll(reader)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "new" {
		t.Fatalf("reused event-log validation reader returned %q, want new", got)
	}
}

func TestCopyStateDomainChangeHistoryDataUsesPooledBuffer(t *testing.T) {
	reader := &stateDomainChangeHistoryFastPathReader{Reader: bytes.NewReader([]byte("history payload"))}
	writer := new(stateDomainChangeHistoryFastPathWriter)
	n, err := copyStateDomainChangeHistoryData(writer, reader)
	if err != nil {
		t.Fatal(err)
	}
	if n != int64(len("history payload")) || writer.String() != "history payload" {
		t.Fatalf("copied (%d, %q), want (%d, history payload)", n, writer.String(), len("history payload"))
	}
	if reader.writeToCalled {
		t.Fatal("copy used reader WriterTo instead of the pooled buffer")
	}
	if writer.readFromCalled {
		t.Fatal("copy used writer ReadFrom instead of the pooled buffer")
	}
}

func TestStateDomainChangeAccessorValidationReaderKeepsTwoWindows(t *testing.T) {
	data := make([]byte, 3*stateDomainChangeAccessorValidationWindowSize)
	for i := range data {
		data[i] = byte(i)
	}
	source := &countingStateDomainReaderAt{reader: bytes.NewReader(data)}
	reader := acquireStateDomainChangeAccessorValidationReader(source, uint64(len(data)))
	defer releaseStateDomainChangeAccessorValidationReader(&reader)

	var got [16]byte
	for i := 0; i < 1_000; i++ {
		for _, off := range []int64{
			int64(i * 32),
			int64(2*stateDomainChangeAccessorValidationWindowSize + i*32),
		} {
			if _, err := reader.ReadAt(got[:], off); err != nil {
				t.Fatalf("ReadAt(%d): %v", off, err)
			}
			if want := data[off : off+int64(len(got))]; !bytes.Equal(got[:], want) {
				t.Fatalf("ReadAt(%d) = %x, want %x", off, got, want)
			}
		}
	}
	if source.reads != 2 {
		t.Fatalf("alternating validation reads used %d source reads, want 2 windows", source.reads)
	}
}

func TestStateDomainChangeAccessorValidationReaderCachedReadHonorsCancellation(t *testing.T) {
	data := make([]byte, stateDomainChangeAccessorValidationWindowSize)
	source := &countingStateDomainReaderAt{reader: bytes.NewReader(data)}
	buffered := acquireStateDomainChangeAccessorValidationReader(source, uint64(len(data)))
	defer releaseStateDomainChangeAccessorValidationReader(&buffered)

	ctx, cancel := context.WithCancel(context.Background())
	reader := contextReaderAt{ctx: ctx, r: buffered}
	var got [16]byte
	if _, err := reader.ReadAt(got[:], 0); err != nil {
		t.Fatalf("initial ReadAt: %v", err)
	}
	if source.reads != 1 {
		t.Fatalf("initial ReadAt used %d source reads, want 1", source.reads)
	}

	cancel()
	if _, err := reader.ReadAt(got[:], 0); !errors.Is(err, context.Canceled) {
		t.Fatalf("cached ReadAt error = %v, want context.Canceled", err)
	}
	if source.reads != 1 {
		t.Fatalf("canceled cached ReadAt used %d source reads, want 1", source.reads)
	}
}
