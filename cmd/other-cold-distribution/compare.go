package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/tronprotocol/go-tron/core/state/snapshots"
)

type codecComparison struct {
	Complete bool
	Started  time.Time
	Finished time.Time
	Samples  []*snapshots.HistoryReferenceMigrationResult
	Before   uint64
	After    uint64
}

// compareCDC makes exclusive per-trio copies. The migrator may only replace
// links and manifests in those private directories; the pinned input and the
// running node's manifest are never passed to its mutation API.
func compareCDC(dir, out string) error {
	input, e := filepath.EvalSymlinks(dir)
	if e != nil {
		return e
	}
	input, e = filepath.Abs(input)
	if e != nil {
		return e
	}
	parent, e := filepath.EvalSymlinks(filepath.Dir(out))
	if e != nil {
		return e
	}
	parent, e = filepath.Abs(parent)
	if e != nil {
		return e
	}
	dest := filepath.Join(parent, filepath.Base(out))
	rel, e := filepath.Rel(input, dest)
	if e != nil || rel != ".." && !strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		return errors.New("comparison output must be outside input")
	}
	b, e := os.ReadFile(filepath.Join(input, "manifest.json"))
	if e != nil {
		return e
	}
	var m snapshots.Manifest
	if e = json.Unmarshal(b, &m); e != nil {
		return e
	}
	work := dest + ".work"
	if e = os.Mkdir(work, 0700); e != nil {
		return e
	}
	r := codecComparison{Started: time.Now().UTC()}
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Minute)
	defer cancel()
	for _, h := range m.Segments {
		if h.Kind != snapshots.SegmentHistory || h.Dataset != snapshots.SegmentDatasetStateDomainChange {
			continue
		}
		refs := []snapshots.SegmentRef{}
		for _, x := range m.Segments {
			if x.Dataset == h.Dataset && x.FromTxNum == h.FromTxNum && x.ToTxNum == h.ToTxNum {
				refs = append(refs, x)
			}
		}
		if len(refs) != 3 {
			return errors.New("missing trio")
		}
		d := filepath.Join(work, fmt.Sprintf("%d", h.FromTxNum))
		if e = os.Mkdir(d, 0700); e != nil {
			return e
		}
		for _, x := range refs {
			dst := filepath.Join(d, x.Path)
			if e = os.MkdirAll(filepath.Dir(dst), 0700); e != nil {
				return e
			}
			if e = os.Link(filepath.Join(input, x.Path), dst); e != nil {
				return e
			}
		}
		manifest := snapshots.NewManifest(h.FromTxNum, h.ToTxNum, refs)
		if e = snapshots.PublishManifest(d, manifest); e != nil {
			return e
		}
		res, e := snapshots.MigrateHistoryCDCContext(ctx, d, snapshots.HistoryCDCMigrationOptions{AllowGrowth: true})
		if e != nil {
			return e
		}
		r.Samples = append(r.Samples, res)
		r.Before += res.ActiveBytesBefore
		r.After += res.ActiveBytesAfter
		fmt.Fprintln(os.Stderr, "CDC", h.FromTxNum, res.ActiveBytesBefore, res.ActiveBytesAfter)
	}
	r.Complete = true
	r.Finished = time.Now().UTC()
	f, e := os.OpenFile(dest, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if e != nil {
		return e
	}
	enc := json.NewEncoder(f)
	enc.SetIndent("", "  ")
	return errors.Join(enc.Encode(r), f.Sync(), f.Close())
}
