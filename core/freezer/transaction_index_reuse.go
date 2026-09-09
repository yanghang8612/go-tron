package freezer

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
)

const (
	transactionHashReplayMemory = 8 << 20
	transactionHashReplayChunk  = 256 << 10
)

var transactionHashReplayCRC = crc32.MakeTable(crc32.Castagnoli)

// transactionHashReplay retains the already validated, sorted full hashes until
// their immutable index is installed. It is scratch, never a recovery source.
// The backing allocation (not just len) is capped; overflowing streams use
// checksummed sequential chunks in the collector's own temporary directory.
type transactionHashReplay struct {
	buffer       []byte
	limit        int
	file         *os.File
	dir          string
	cleanup      func() error
	rows         uint64
	start        uint64
	end          uint64
	sealed       bool
	peakMemory   int
	spilledBytes uint64
}

func (s *transactionHashReplay) append(hash []byte) error {
	if s.sealed || len(hash) != 32 {
		return errors.New("transaction hash replay: invalid append")
	}
	if s.limit == 0 {
		s.limit = transactionHashReplayMemory
	}
	if s.limit < 32 || s.limit%32 != 0 || s.limit > transactionHashReplayMemory {
		return errors.New("transaction hash replay: invalid memory budget")
	}
	if s.buffer == nil {
		// Allocate the exact byte budget once: append growth must not silently
		// retain a capacity larger than the configured replay allowance.
		s.buffer = make([]byte, 0, s.limit)
		s.peakMemory = cap(s.buffer)
	}
	if len(s.buffer)+32 > cap(s.buffer) {
		if s.file == nil {
			file, err := os.CreateTemp(s.dir, "tx-hashes-*.tmp")
			if err != nil {
				return err
			}
			s.file = file
		}
		if err := s.flush(); err != nil {
			return err
		}
	}
	s.buffer = append(s.buffer, hash...)
	s.rows++
	if s.file != nil && len(s.buffer) >= transactionHashReplayChunk {
		return s.flush()
	}
	return nil
}

func (s *transactionHashReplay) flush() error {
	for offset := 0; offset < len(s.buffer); {
		end := min(len(s.buffer), offset+transactionHashReplayChunk)
		data := s.buffer[offset:end]
		var header [8]byte
		binary.BigEndian.PutUint32(header[:4], uint32(len(data)))
		binary.BigEndian.PutUint32(header[4:], crc32.Checksum(data, transactionHashReplayCRC))
		if _, err := s.file.Write(header[:]); err != nil {
			return err
		}
		if _, err := s.file.Write(data); err != nil {
			return err
		}
		s.spilledBytes += uint64(len(header) + len(data))
		offset = end
	}
	s.buffer = s.buffer[:0]
	return nil
}

func (s *transactionHashReplay) seal(rows uint64) error {
	if rows != s.rows {
		return fmt.Errorf("transaction hash replay: got %d rows, want %d", s.rows, rows)
	}
	if s.file != nil {
		if err := s.flush(); err != nil {
			return err
		}
		if _, err := s.file.Seek(0, io.SeekStart); err != nil {
			return err
		}
	}
	s.sealed = true
	return nil
}

func (s *transactionHashReplay) iterate(ctx context.Context, yield func([]byte) error) error {
	if !s.sealed {
		return errors.New("transaction hash replay: unsealed stream")
	}
	var seen uint64
	visit := func(data []byte) error {
		if uint64(len(data)/32) > s.rows-seen {
			return errors.New("transaction hash replay: excessive rows")
		}
		for len(data) > 0 {
			if err := ctx.Err(); err != nil {
				return err
			}
			if err := yield(data[:32]); err != nil {
				return err
			}
			data = data[32:]
			seen++
		}
		return nil
	}
	if s.file == nil {
		if err := visit(s.buffer); err != nil {
			return err
		}
	} else {
		if _, err := s.file.Seek(0, io.SeekStart); err != nil {
			return err
		}
		for {
			var header [8]byte
			if _, err := io.ReadFull(s.file, header[:]); err != nil {
				if errors.Is(err, io.EOF) {
					break
				}
				return err
			}
			size := int(binary.BigEndian.Uint32(header[:4]))
			if size == 0 || size%32 != 0 || size > transactionHashReplayChunk || size > cap(s.buffer) {
				return errors.New("transaction hash replay: invalid chunk size")
			}
			data := s.buffer[:size]
			if _, err := io.ReadFull(s.file, data); err != nil {
				return err
			}
			if crc32.Checksum(data, transactionHashReplayCRC) != binary.BigEndian.Uint32(header[4:]) {
				return errors.New("transaction hash replay: chunk checksum mismatch")
			}
			if err := visit(data); err != nil {
				return err
			}
		}
	}
	if seen != s.rows {
		return fmt.Errorf("transaction hash replay: read %d rows, want %d", seen, s.rows)
	}
	return ctx.Err()
}

func (s *transactionHashReplay) close() {
	if s.file != nil {
		name := s.file.Name()
		_ = s.file.Close()
		_ = os.Remove(name)
		s.file = nil
	}
	s.buffer = nil
	if s.cleanup != nil {
		_ = s.cleanup()
		s.cleanup = nil
	}
}

// buildAndPruneTransactionIndexContext owns one crash-safe quantum. A restored
// run has no replay, so recovery deliberately reconstructs hashes from bodies.
func (r *Runner) buildAndPruneTransactionIndexContext(ctx context.Context, target uint64) (bool, error) {
	replay := &transactionHashReplay{}
	defer replay.close()
	defer func() {
		r.transactionIndexBudgetMetric("replay_peak_memory_bytes", int64(replay.peakMemory))
		r.transactionIndexBudgetMetric("replay_spilled_bytes", uint64GaugeValue(replay.spilledBytes))
	}()
	changed, err := r.ensureTransactionIndexCoverageReplay(ctx, target, replay)
	if err != nil || !changed {
		return changed, err
	}
	_, err = r.pruneTransactionIndexDebtReplay(ctx, target, replay)
	return changed, err
}

// Keep the replay next to its collector so ordinary stale-collector cleanup
// also removes scratch left by a killed process.
func (s *transactionHashReplay) useCollector(dir string, cleanup func() error, start, end uint64) {
	s.dir = filepath.Clean(dir)
	s.cleanup, s.start, s.end = cleanup, start, end
}
