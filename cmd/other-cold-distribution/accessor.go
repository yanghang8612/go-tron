package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/klauspost/compress/zstd"
	"github.com/tronprotocol/go-tron/core/state/snapshots"
)

type pageModel struct {
	PageBytes         int
	Files             uint64
	Raw               uint64
	Stored            uint64
	Pages             uint64
	FrameDirectory    uint64
	LargestStoredPage uint64
	RawFallbackPages  uint64
}
type accessorComparison struct {
	Complete   bool
	Started    time.Time
	Finished   time.Time
	Models     map[string][]pageModel
	Files      []string
	Limitation string
}

// compareAccessors models independent checksummed pages with a raw-page
// fallback and a 24-byte directory entry per page. It measures complete sample
// files, not a projected whole-database saving or a production random-read cost.
func compareAccessors(dir, out string) error {
	b, e := os.ReadFile(filepath.Join(dir, "manifest.json"))
	if e != nil {
		return e
	}
	var m snapshots.Manifest
	if e = json.Unmarshal(b, &m); e != nil {
		return e
	}
	enc, e := zstd.NewWriter(nil, zstd.WithEncoderConcurrency(1), zstd.WithEncoderLevel(zstd.SpeedDefault), zstd.WithEncoderCRC(true))
	if e != nil {
		return e
	}
	defer enc.Close()
	dec, e := zstd.NewReader(nil, zstd.WithDecoderConcurrency(1))
	if e != nil {
		return e
	}
	defer dec.Close()
	r := accessorComparison{Started: time.Now().UTC(), Models: map[string][]pageModel{}, Limitation: "same full files compressed as independent pages; 48-byte wrapper header and 24-byte page directory counted; read latency/cache and integration not measured"}
	for _, ref := range m.Segments {
		if ref.Kind != snapshots.SegmentAccessor && ref.Kind != snapshots.SegmentInverted {
			continue
		}
		path := filepath.Join(dir, ref.Path)
		f, e := os.Open(path)
		if e != nil {
			return e
		}
		h := sha256.New()
		_, e = io.Copy(h, f)
		e = errors.Join(e, f.Close())
		if e != nil {
			return e
		}
		if ref.Checksum != "sha256:"+hex.EncodeToString(h.Sum(nil)) {
			return errors.New("accessor/index SHA mismatch")
		}
		name := string(ref.Kind)
		if r.Models[name] == nil {
			for _, size := range []int{4096, 16384, 65536} {
				r.Models[name] = append(r.Models[name], pageModel{PageBytes: size})
			}
		}
		for i := range r.Models[name] {
			p := &r.Models[name][i]
			p.Files++
			p.Stored += 48
			f, e := os.Open(path)
			if e != nil {
				return e
			}
			buf := make([]byte, p.PageBytes)
			for {
				n, e := io.ReadFull(f, buf)
				if e != nil && e != io.ErrUnexpectedEOF && e != io.EOF {
					f.Close()
					return e
				}
				if n > 0 {
					v := enc.EncodeAll(buf[:n], nil)
					decoded, e := dec.DecodeAll(v, nil)
					if e != nil || !bytes.Equal(decoded, buf[:n]) {
						f.Close()
						return errors.Join(e, errors.New("page round trip differs"))
					}
					size := len(v)
					if size >= n {
						size = n
						p.RawFallbackPages++
					}
					p.Raw += uint64(n)
					p.Stored += uint64(size + 24)
					p.FrameDirectory += 24
					p.Pages++
					p.LargestStoredPage = max(p.LargestStoredPage, uint64(size))
				}
				if e != nil {
					break
				}
			}
			if e = f.Close(); e != nil {
				return e
			}
		}
		r.Files = append(r.Files, ref.Path)
	}
	r.Complete = true
	r.Finished = time.Now().UTC()
	f, e := os.OpenFile(out, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if e != nil {
		return e
	}
	w := json.NewEncoder(f)
	w.SetIndent("", "  ")
	return errors.Join(w.Encode(r), f.Sync(), f.Close())
}
