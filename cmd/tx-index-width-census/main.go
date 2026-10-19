// tx-index-width-census verifies immutable transaction-index runs and measures
// a lossless per-run relative-location width. It does not write index files.
package main

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"hash/crc32"
	"math/bits"
	"os"
	"path/filepath"
	"time"
)

var castagnoli = crc32.MakeTable(crc32.Castagnoli)

const marker = uint64(1) << 63

type manifest struct {
	Version uint32        `json:"version"`
	Runs    []manifestRun `json:"runs"`
}
type manifestRun struct {
	File  string `json:"file"`
	Start uint64 `json:"start_block"`
	End   uint64 `json:"end_block"`
	Rows  uint64 `json:"rows"`
}
type runStats struct {
	File           string
	SHA256         string
	Start          uint64
	End            uint64
	Rows           uint64
	Bytes          uint64
	Packed         uint64
	Plain          uint64
	MaxRelative    uint64
	RelativeWidth  int
	CandidateBytes uint64
	Buckets        uint64
}
type report struct {
	Complete       bool
	Started        time.Time
	Finished       time.Time
	ManifestSHA256 string
	Files          uint64
	Rows           uint64
	Packed         uint64
	Plain          uint64
	Bytes          uint64
	CandidateBytes uint64
	Widths         map[int]uint64
	Runs           []runStats
	Limitation     string
}

func be64(b []byte, n int) uint64 { return binary.BigEndian.Uint64(b[n : n+8]) }
func be32(b []byte, n int) uint32 { return binary.BigEndian.Uint32(b[n : n+4]) }
func inspect(data []byte, ref manifestRun) (s runStats, err error) {
	fail := func(msg string) (runStats, error) { return s, errors.New(msg) }
	if len(data) < 128 || string(data[:8]) != "gtxi0001" || (be32(data, 8) != 1 && be32(data, 8) != 2) || be32(data, 12) != 128 || be32(data, 20) != 64 {
		return fail("bad header")
	}
	if crc32.Checksum(data[:84], castagnoli) != be32(data, 84) {
		return fail("header CRC")
	}
	p := be32(data, 16)
	if p < 8 || p > 24 {
		return fail("prefix width")
	}
	start, end, rows := be64(data, 24), be64(data, 32), be64(data, 40)
	entries := be64(data, 56)
	off := be64(data, 64)
	if start != ref.Start || end != ref.End || rows != ref.Rows || end <= start || entries != (uint64(1)<<p)+1 || be64(data, 48) != 128 || off != 128+entries*8 || off > uint64(len(data)) || be64(data, 72) != uint64(len(data)) {
		return fail("manifest/header dimensions")
	}
	directory := data[128:off]
	if crc32.Checksum(directory, castagnoli) != be32(data, 80) {
		return fail("directory CRC")
	}
	sum := sha256.Sum256(data)
	s = runStats{File: ref.File, SHA256: hex.EncodeToString(sum[:]), Start: start, End: end, Rows: rows, Bytes: uint64(len(data))}
	var seen, storedLocationBytes uint64
	for i := uint64(0); i+1 < entries; i++ {
		a, z := be64(directory, int(i*8)), be64(directory, int((i+1)*8))
		if a < off || z < a || z > uint64(len(data)) {
			return fail("bucket bounds")
		}
		if a == z {
			continue
		}
		b := data[a:z]
		if len(b) < 16 {
			return fail("bucket span")
		}
		count := be32(b, 0)
		width := uint64(8)
		n := uint64(count)
		if be32(data, 8) == 2 {
			width = uint64(count >> 28)
			n = uint64(count & ((1 << 28) - 1))
		}
		if width < 1 || width > 8 || n > 1<<20 || uint64(len(b)) != 16+n*(8+width) || crc32.Checksum(b[:12], castagnoli) != be32(b, 12) {
			return fail("bucket header")
		}
		storedLocationBytes += n * width
		fps := b[16 : 16+8*n]
		locs := b[16+8*n:]
		if crc32.Checksum(fps, castagnoli) != be32(b, 4) || crc32.Checksum(locs, castagnoli) != be32(b, 8) {
			return fail("bucket data CRC")
		}
		s.Buckets++
		for j := uint64(0); j < n; j++ {
			var loc uint64
			for _, v := range locs[j*width : (j+1)*width] {
				loc = loc<<8 | uint64(v)
			}
			if width < 8 {
				if loc>>16 >= end-start || start+(loc>>16) >= 1<<47 {
					return fail("bad relative location")
				}
				loc = marker | ((start + (loc >> 16)) << 16) | (loc & 65535)
			}
			block := loc
			if loc&marker != 0 {
				block = (loc &^ marker) >> 16
				s.Packed++
			} else {
				s.Plain++
			}
			if block < start || block >= end {
				return fail("location outside run")
			}
			if loc&marker != 0 {
				relative := (block-start)<<16 | (loc & 65535)
				if ((relative>>16)+start)<<16|(relative&65535)|marker != loc {
					return fail("relative reconstruction differs")
				}
				s.MaxRelative = max(s.MaxRelative, relative)
			}
			seen++
		}
	}
	if seen != rows {
		return fail("row conservation")
	}
	s.RelativeWidth = 8
	s.CandidateBytes = s.Bytes
	if s.Packed == rows {
		s.RelativeWidth = max(1, (bits.Len64(s.MaxRelative)+7)/8)
		s.CandidateBytes = min(s.Bytes, s.Bytes-storedLocationBytes+rows*uint64(s.RelativeWidth))
	}
	return s, nil
}
func run() error {
	dir := flag.String("dir", "", "pinned tx-index directory")
	out := flag.String("out", "", "exclusive report file")
	flag.Parse()
	if *dir == "" || *out == "" {
		return errors.New("dir and out required")
	}
	b, e := os.ReadFile(filepath.Join(*dir, "manifest.json"))
	if e != nil {
		return e
	}
	var m manifest
	if e = json.Unmarshal(b, &m); e != nil {
		return e
	}
	if m.Version != 1 {
		return errors.New("manifest version")
	}
	sum := sha256.Sum256(b)
	r := report{Started: time.Now().UTC(), ManifestSHA256: hex.EncodeToString(sum[:]), Widths: map[int]uint64{}, Limitation: "calculated physical layout with existing headers/directories/fingerprints and minimum fixed relative-location width per all-packed run; no production writer or read-latency benchmark"}
	for _, ref := range m.Runs {
		if ref.File != filepath.Base(ref.File) {
			return errors.New("non-basename run path")
		}
		path := filepath.Join(*dir, "runs", ref.File)
		st, e := os.Stat(path)
		if e != nil {
			return e
		}
		if st.Size() > 256<<20 {
			return errors.New("run exceeds bounded whole-file buffer")
		}
		data, e := os.ReadFile(path)
		if e != nil {
			return e
		}
		s, e := inspect(data, ref)
		if e != nil {
			return fmt.Errorf("%s: %w", ref.File, e)
		}
		r.Runs = append(r.Runs, s)
		r.Files++
		r.Rows += s.Rows
		r.Packed += s.Packed
		r.Plain += s.Plain
		r.Bytes += s.Bytes
		r.CandidateBytes += s.CandidateBytes
		r.Widths[s.RelativeWidth] += s.Rows
		if r.Files%128 == 0 {
			fmt.Fprintln(os.Stderr, "runs", r.Files, "rows", r.Rows)
		}
	}
	r.Complete = true
	r.Finished = time.Now().UTC()
	f, e := os.OpenFile(*out, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if e != nil {
		return e
	}
	enc := json.NewEncoder(f)
	enc.SetIndent("", "  ")
	return errors.Join(enc.Encode(r), f.Sync(), f.Close())
}
func main() {
	if e := run(); e != nil {
		fmt.Fprintln(os.Stderr, e)
		os.Exit(1)
	}
}
