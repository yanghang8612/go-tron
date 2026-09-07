package snapshots

import (
	"bufio"
	"context"
	"errors"
	"io"
	"math"
	"os"

	"github.com/ethereum/go-ethereum/metrics"
)

const historyPostingMemoryLimit = 64 << 10

// These count successfully assembled key streams, including streams whose
// enclosing build may later be canceled. They do not count published files.
var (
	historyPostingMemoryKeys = metrics.NewRegisteredCounter(defaultColdSnapshotMetrics+"posting_assembly/memory_keys", nil)
	historyPostingSpillKeys  = metrics.NewRegisteredCounter(defaultColdSnapshotMetrics+"posting_assembly/spill_keys", nil)
	historyPostingSpillBytes = metrics.NewRegisteredCounter(defaultColdSnapshotMetrics+"posting_assembly/spill_bytes", nil)
)

// historyPostingScratch owns one key's encoded frame payload. Small keys never
// reach the filesystem; a large key spills to one reusable temporary file.
// Frame directories remain with the posting writer, as in the V7 format.
type historyPostingScratch struct {
	ctx      context.Context
	dir      string
	base     string
	data     []byte
	size     uint64
	spilled  bool
	file     *os.File
	buffered *bufio.Writer
}

func (s *historyPostingScratch) Write(data []byte) error {
	if err := contextError(s.ctx); err != nil {
		return err
	}
	if uint64(len(data)) > math.MaxUint32-s.size {
		return errors.New("snapshots: V7 posting scratch exceeds uint32")
	}
	if !s.spilled && s.size+uint64(len(data)) <= historyPostingMemoryLimit {
		if s.data == nil {
			s.data = make([]byte, 0, historyPostingMemoryLimit)
		}
		s.data = append(s.data, data...)
		s.size += uint64(len(data))
		return nil
	}
	if !s.spilled {
		if s.file == nil {
			file, _, err := createStateDomainChangeBinaryTempFileInDir(s.dir, s.base)
			if err != nil {
				return err
			}
			s.file = file
			s.buffered = bufio.NewWriterSize(file, historyPostingMemoryLimit)
		}
		if _, err := s.buffered.Write(s.data); err != nil {
			return err
		}
		s.spilled = true
		s.data = s.data[:0]
	}
	if _, err := s.buffered.Write(data); err != nil {
		return err
	}
	s.size += uint64(len(data))
	return nil
}

func (s *historyPostingScratch) WriteTo(dst io.Writer) (int64, error) {
	if err := contextError(s.ctx); err != nil {
		return 0, err
	}
	if !s.spilled {
		n, err := dst.Write(s.data)
		if err == nil && n != len(s.data) {
			err = io.ErrShortWrite
		}
		return int64(n), err
	}
	if err := s.buffered.Flush(); err != nil {
		return 0, err
	}
	// A bounded section avoids changing the append position or exposing stale
	// tail bytes from a previously larger key.
	n, err := copyStateDomainChangeHistoryData(dst, contextReader{ctx: s.ctx, r: io.NewSectionReader(s.file, 0, int64(s.size))})
	if err == nil && uint64(n) != s.size {
		err = io.ErrUnexpectedEOF
	}
	return n, err
}

func (s *historyPostingScratch) Reset() error {
	if s.spilled {
		if err := s.file.Truncate(0); err != nil {
			return err
		}
		if _, err := s.file.Seek(0, io.SeekStart); err != nil {
			return err
		}
		s.buffered.Reset(s.file)
	}
	s.data = s.data[:0]
	s.size, s.spilled = 0, false
	return nil
}

func (s *historyPostingScratch) Close() {
	if s.file != nil {
		// Do not flush abandoned partial keys on cancellation or failure.
		_ = s.file.Close()
		_ = os.Remove(s.file.Name())
		s.file = nil
	}
}
