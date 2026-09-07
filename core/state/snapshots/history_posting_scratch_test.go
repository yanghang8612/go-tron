package snapshots

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"testing"
)

func TestHistoryPostingScratchBoundariesAndReuse(t *testing.T) {
	dir := t.TempDir()
	s := &historyPostingScratch{dir: dir, base: "frames"}
	defer s.Close()
	for _, n := range []int{1, historyPostingMemoryLimit - 1, historyPostingMemoryLimit, historyPostingMemoryLimit + 1, 3*historyPostingMemoryLimit + 19, 1, historyPostingMemoryLimit + 7} {
		want := bytes.Repeat([]byte{byte(n)}, n)
		for start := 0; start < n; start += 997 {
			if err := s.Write(want[start:min(n, start+997)]); err != nil {
				t.Fatal(err)
			}
		}
		if s.spilled != (n > historyPostingMemoryLimit) || cap(s.data) > historyPostingMemoryLimit {
			t.Fatalf("n=%d spilled=%v cap=%d", n, s.spilled, cap(s.data))
		}
		var got bytes.Buffer
		count, err := s.WriteTo(&got)
		if err != nil || count != int64(n) || !bytes.Equal(got.Bytes(), want) {
			t.Fatalf("n=%d count=%d err=%v", n, count, err)
		}
		if err := s.Reset(); err != nil {
			t.Fatal(err)
		}
		if s.file != nil {
			info, err := s.file.Stat()
			if err != nil || info.Size() != 0 {
				t.Fatalf("reset file: %v %v", info, err)
			}
		}
	}
	s.Close()
	files, err := os.ReadDir(dir)
	if err != nil || len(files) != 0 {
		t.Fatalf("scratch cleanup: %v %v", files, err)
	}
}

func TestHistoryPostingWriterMatchesV7Encoder(t *testing.T) {
	s := &historyPostingScratch{dir: t.TempDir(), base: "frames"}
	defer s.Close()
	counts := []int{1, 127, 128, 129, 2048, 30000, 1, 40000, 129}
	var postings, meta, wantPostings, wantMeta bytes.Buffer
	w := &stateDomainChangeV7PostingWriter{postings: bufio.NewWriterSize(&postings, 4096), meta: bufio.NewWriterSize(&meta, 4096), scratch: s, expected: uint32(len(counts)), fromTx: 100}
	for keyID, count := range counts {
		rows := make([]stateDomainChangeBinaryAccessorV6Posting, count)
		for i := range rows {
			rows[i] = stateDomainChangeBinaryAccessorV6Posting{txNum: 100 + uint64(i/2), offset: 1000 + uint64(i)*100, recordIndex: uint32(i)}
			var key [8]byte
			binary.BigEndian.PutUint32(key[:4], uint32(keyID))
			binary.BigEndian.PutUint32(key[4:], uint32(i))
			value := make([]byte, stateDomainChangeBinaryAccessorV6PostingSize)
			binary.BigEndian.PutUint64(value[:8], rows[i].txNum)
			if err := putStateDomainChangeBinaryAccessorV5Offset(value[8:14], rows[i].offset); err != nil {
				t.Fatal(err)
			}
			binary.BigEndian.PutUint32(value[14:18], rows[i].recordIndex)
			if err := w.Put(key[:], value); err != nil {
				t.Fatal(err)
			}
		}
		if keyID < 5 && s.file != nil {
			t.Fatal("small keys created scratch file")
		}
		encoded, err := encodeStateDomainChangeBinaryAccessorV7PostingList(100, rows)
		if err != nil {
			t.Fatal(err)
		}
		var m [12]byte
		binary.BigEndian.PutUint64(m[:8], uint64(wantPostings.Len()))
		binary.BigEndian.PutUint32(m[8:], uint32(count))
		wantMeta.Write(m[:])
		wantPostings.Write(encoded)
	}
	if err := w.Finish(); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(postings.Bytes(), wantPostings.Bytes()) || !bytes.Equal(meta.Bytes(), wantMeta.Bytes()) {
		t.Fatal("stream differs from independent V7 encoder")
	}
	if s.file == nil {
		t.Fatal("large key did not exercise spill")
	}
}

type historyPostingFailWriter struct{ err error }

func (w historyPostingFailWriter) Write([]byte) (int, error) { return 0, w.err }

func TestHistoryPostingScratchFailures(t *testing.T) {
	sentinel := errors.New("destination failure")
	for _, size := range []int{12, historyPostingMemoryLimit + 1} {
		t.Run(fmt.Sprint(size), func(t *testing.T) {
			dir := t.TempDir()
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			s := &historyPostingScratch{ctx: ctx, dir: dir, base: "frames"}
			defer s.Close()
			if err := s.Write(make([]byte, size)); err != nil {
				t.Fatal(err)
			}
			if _, err := s.WriteTo(historyPostingFailWriter{sentinel}); !errors.Is(err, sentinel) {
				t.Fatalf("destination error: %v", err)
			}
			cancel()
			if err := s.Write([]byte{1}); !errors.Is(err, context.Canceled) {
				t.Fatalf("canceled write: %v", err)
			}
			if _, err := s.WriteTo(io.Discard); !errors.Is(err, context.Canceled) {
				t.Fatalf("canceled read: %v", err)
			}
		})
	}
	t.Run("spill-open", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "file")
		if err := os.WriteFile(path, nil, 0600); err != nil {
			t.Fatal(err)
		}
		s := &historyPostingScratch{dir: path, base: "frames"}
		defer s.Close()
		if err := s.Write([]byte{1}); err != nil {
			t.Fatal(err)
		}
		if err := s.Write(make([]byte, historyPostingMemoryLimit)); err == nil {
			t.Fatal("missing spill failure")
		}
	})
	t.Run("spill-io", func(t *testing.T) {
		s := &historyPostingScratch{dir: t.TempDir(), base: "frames"}
		defer s.Close()
		if err := s.Write(make([]byte, historyPostingMemoryLimit+1)); err != nil {
			t.Fatal(err)
		}
		if err := s.file.Close(); err != nil {
			t.Fatal(err)
		}
		if _, err := s.WriteTo(io.Discard); err == nil {
			t.Fatal("missing file I/O failure")
		}
	})
}
