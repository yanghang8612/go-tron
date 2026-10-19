package main

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/cockroachdb/pebble/bloom"
	"github.com/cockroachdb/pebble/objstorage/objstorageprovider"
	"github.com/cockroachdb/pebble/sstable"
	"github.com/cockroachdb/pebble/vfs"
)

// Diagnostic SSTs use identical table/block/filter settings and rotate every
// 500,000 input records. They are not a production schema or a whole-DB model.
type tableSeries struct {
	Rows              uint64 `json:"rows"`
	KeyBytes          uint64 `json:"key_bytes"`
	ValueBytes        uint64 `json:"value_bytes"`
	SSTBytes          uint64 `json:"sst_bytes"`
	DataBytes         uint64 `json:"data_block_bytes"`
	IndexBytes        uint64 `json:"index_bytes"`
	FilterBytes       uint64 `json:"filter_bytes"`
	Files             uint64 `json:"files"`
	ChecksumsVerified bool   `json:"checksums_verified"`
	dir, name         string
	compression       sstable.Compression
	writer            *sstable.Writer
	currentPath       string
	last              []byte
	finished          bool
}

func newExperiments(out string) (map[string]*tableSeries, error) {
	dir := filepath.Join(out, "sst")
	if err := os.Mkdir(dir, 0700); err != nil {
		return nil, err
	}
	m := map[string]*tableSeries{}
	for _, compression := range []string{"snappy", "none"} {
		for _, model := range []string{"int64-post-v2", "int64-trim", "account-baseline", "account-address-elided", "native-baseline", "native-zstd"} {
			name := model + "-" + compression
			c := sstable.SnappyCompression
			if compression == "none" {
				c = sstable.NoCompression
			}
			m[name] = &tableSeries{dir: dir, name: name, compression: c}
		}
	}
	return m, nil
}
func (s *tableSeries) set(k, v []byte) error {
	if s.finished {
		return errors.New("SST series finished")
	}
	if s.Rows > 0 && bytes.Compare(s.last, k) >= 0 {
		return fmt.Errorf("experiment %s unsorted/duplicate key", s.name)
	}
	s.last = append(s.last[:0], k...)
	if s.writer == nil {
		s.currentPath = filepath.Join(s.dir, fmt.Sprintf("%s-%04d.sst", s.name, s.Files))
		f, err := vfs.Default.Create(s.currentPath)
		if err != nil {
			return err
		}
		s.writer = sstable.NewWriter(objstorageprovider.NewFileWritable(f), sstable.WriterOptions{BlockSize: 4096, BlockRestartInterval: 16, IndexBlockSize: 4096, Compression: s.compression, FilterPolicy: bloom.FilterPolicy(10), FilterType: sstable.TableFilter, TableFormat: sstable.TableFormatPebblev4})
	}
	if err := s.writer.Set(k, v); err != nil {
		return err
	}
	s.Rows++
	s.KeyBytes += uint64(len(k))
	s.ValueBytes += uint64(len(v))
	if s.Rows%500000 == 0 {
		return s.closeFile()
	}
	return nil
}
func (s *tableSeries) closeFile() error {
	if s.writer == nil {
		return nil
	}
	w := s.writer
	s.writer = nil
	if err := w.Close(); err != nil {
		return err
	}
	meta, err := w.Metadata()
	if err != nil {
		return err
	}
	s.SSTBytes += meta.Size
	s.DataBytes += meta.Properties.DataSize
	s.IndexBytes += meta.Properties.IndexSize
	s.FilterBytes += meta.Properties.FilterSize
	s.Files++
	f, err := vfs.Default.Open(s.currentPath)
	if err != nil {
		return err
	}
	rd, err := sstable.NewSimpleReadable(f)
	if err != nil {
		f.Close()
		return err
	}
	reader, err := sstable.NewReader(rd, sstable.ReaderOptions{})
	if err != nil {
		return err
	}
	return errors.Join(reader.ValidateBlockChecksums(), reader.Close())
}
func (s *tableSeries) finish() error {
	if s.finished {
		return nil
	}
	s.finished = true
	err := s.closeFile()
	s.ChecksumsVerified = err == nil
	return err
}
