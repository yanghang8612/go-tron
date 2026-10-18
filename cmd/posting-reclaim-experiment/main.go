// posting-reclaim-experiment creates two new disposable copies of a stopped
// census image. It can never open the input image or a production DB writable.
package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/cockroachdb/pebble/vfs"
	"github.com/tronprotocol/go-tron/core/rawdb"
)

type captureFile struct {
	Name   string `json:"name"`
	Size   int64  `json:"size"`
	Dev    uint64 `json:"dev"`
	Ino    uint64 `json:"ino"`
	SHA    string `json:"sha256"`
	Method string `json:"copy_method"`
}

type capturedImage struct {
	Complete    bool                     `json:"complete"`
	Source      string                   `json:"source"`
	Destination string                   `json:"destination"`
	ColdSHA     string                   `json:"cold_manifest_sha256"`
	Captured    map[string][]captureFile `json:"captured"`
}

type diskBytes struct {
	Files          uint64 `json:"files"`
	FileBytes      uint64 `json:"file_bytes"`
	AllocatedBytes uint64 `json:"allocated_bytes"`
	SSTBytes       uint64 `json:"sst_file_bytes"`
	SSTAllocated   uint64 `json:"sst_allocated_bytes"`
}

type branchResult struct {
	Path             string                        `json:"path"`
	Before           diskBytes                     `json:"before"`
	After            diskBytes                     `json:"after"`
	Census           rawdb.PostingCensus           `json:"census"`
	Complement       rawdb.PostingComplementCensus `json:"non_posting"`
	DeletedRows      uint64                        `json:"deleted_rows"`
	DeletedBytes     uint64                        `json:"deleted_logical_bytes"`
	Chunks           uint64                        `json:"chunks"`
	ScanDuration     time.Duration                 `json:"scan_ns"`
	WriteDuration    time.Duration                 `json:"write_ns"`
	MaxChunkDuration time.Duration                 `json:"max_chunk_ns"`
	Verified         bool                          `json:"verified"`
}

type experiment struct {
	Complete        bool                          `json:"complete"`
	Phase           string                        `json:"phase"`
	Started         time.Time                     `json:"started_utc"`
	Image           string                        `json:"input_image"`
	Through         uint64                        `json:"through_block"`
	Complement      rawdb.PostingComplementCensus `json:"non_posting_before"`
	Control         branchResult                  `json:"control_compaction_only"`
	Pruned          branchResult                  `json:"prune_then_compact"`
	SSTDelta        int64                         `json:"measured_posting_sst_delta_bytes"`
	AllocationDelta int64                         `json:"measured_posting_sst_allocation_delta_bytes"`
	Error           string                        `json:"error,omitempty"`
	Note            string                        `json:"note"`
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func hashFile(path string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer f.Close()
	h := sha256.New()
	if _, err = io.Copy(h, f); err != nil {
		return "", err
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

func loadJSON(path string, value any) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	d := json.NewDecoder(io.LimitReader(f, 128<<20))
	return d.Decode(value)
}

func checkImage(image string, capture capturedImage) (uint64, error) {
	if !capture.Complete || capture.Destination != image || capture.Source != "/data/gtron/main/datadir/gtron" || len(capture.Captured["history"]) == 0 {
		return 0, errors.New("not a complete captured production image")
	}
	sha, err := hashFile(filepath.Join(image, "cold-manifest.json"))
	if err != nil {
		return 0, err
	}
	if sha != capture.ColdSHA {
		return 0, errors.New("cold manifest image digest mismatch")
	}
	var manifest struct {
		Progress struct {
			H uint64 `json:"hotPruneBlockNum"`
		} `json:"progress"`
	}
	if err := loadJSON(filepath.Join(image, "cold-manifest.json"), &manifest); err != nil {
		return 0, err
	}
	if manifest.Progress.H == 0 {
		return 0, errors.New("missing frozen hot-prune boundary")
	}
	for _, row := range capture.Captured["history"] {
		if filepath.Base(row.Name) != row.Name {
			return 0, errors.New("unsafe captured filename")
		}
		if row.Name == "LOCK" {
			continue
		}
		path := filepath.Join(image, "history", row.Name)
		st, err := os.Lstat(path)
		if err != nil {
			return 0, err
		}
		if !st.Mode().IsRegular() || st.Size() != row.Size {
			return 0, errors.New("captured file changed: " + path)
		}
		if row.Method == "immutable-sst-hardlink" {
			sys := st.Sys().(*syscall.Stat_t)
			if uint64(sys.Dev) != row.Dev || uint64(sys.Ino) != row.Ino {
				return 0, errors.New("captured SST identity changed")
			}
		} else if row.Method == "independent-copy" && len(row.SHA) == 64 {
			sha, err := hashFile(path)
			if err != nil {
				return 0, err
			}
			if sha != row.SHA {
				return 0, errors.New("captured mutable file changed")
			}
		} else {
			return 0, errors.New("unsupported capture copy method")
		}
	}
	return manifest.Progress.H, nil
}

func copyImage(source, destination string) error {
	if err := os.Mkdir(destination, 0700); err != nil {
		return err
	}
	entries, err := os.ReadDir(source)
	if err != nil {
		return err
	}
	for _, entry := range entries {
		name := entry.Name()
		if name == "LOCK" {
			continue
		}
		src, dst := filepath.Join(source, name), filepath.Join(destination, name)
		st, err := os.Lstat(src)
		if err != nil {
			return err
		}
		if !st.Mode().IsRegular() {
			return errors.New("unexpected image entry")
		}
		if strings.HasSuffix(name, ".sst") {
			if _, err := strconv.ParseUint(strings.TrimSuffix(name, ".sst"), 10, 64); err != nil {
				return err
			}
			if err := os.Link(src, dst); err != nil {
				return err
			}
		} else {
			if err := copyFile(src, dst); err != nil {
				return err
			}
		}
	}
	return nil
}

func copyFile(src, dst string) error {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()
	out, err := os.OpenFile(dst, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return err
	}
	_, copyErr := io.Copy(out, in)
	return errors.Join(copyErr, out.Sync(), out.Close())
}

func measure(path string) (result diskBytes, err error) {
	entries, err := os.ReadDir(path)
	if err != nil {
		return result, err
	}
	for _, entry := range entries {
		st, err := os.Lstat(filepath.Join(path, entry.Name()))
		if err != nil {
			return result, err
		}
		if !st.Mode().IsRegular() {
			return result, errors.New("unexpected experiment entry")
		}
		allocated := uint64(st.Sys().(*syscall.Stat_t).Blocks) * 512
		result.Files++
		result.FileBytes += uint64(st.Size())
		result.AllocatedBytes += allocated
		if strings.HasSuffix(entry.Name(), ".sst") {
			result.SSTBytes += uint64(st.Size())
			result.SSTAllocated += allocated
		}
	}
	return result, nil
}

func enoughSpace(path string) error {
	d, err := vfs.Default.GetDiskUsage(path)
	if err != nil {
		return err
	}
	if d.AvailBytes < 500<<30 {
		return errors.New("experiment requires 500 GiB free reserve")
	}
	return nil
}

func run() (retErr error) {
	arg := flag.String("image", "", "complete immutable capture directory; never opened writable")
	flag.Parse()
	if *arg == "" || flag.NArg() != 0 {
		return errors.New("image is required")
	}
	image, err := filepath.EvalSymlinks(*arg)
	if err != nil {
		return err
	}
	image, err = filepath.Abs(image)
	if err != nil {
		return err
	}
	if filepath.Dir(image) != "/data/gtron" || !strings.HasPrefix(filepath.Base(image), "posting-census-") {
		return errors.New("image must be a private /data/gtron/posting-census-* capture")
	}
	var capture capturedImage
	if err := loadJSON(filepath.Join(image, "capture.json"), &capture); err != nil {
		return err
	}
	through, err := checkImage(image, capture)
	if err != nil {
		return err
	}
	var before struct {
		Source string `json:"source"`
		rawdb.PostingCensus
	}
	if err := loadJSON(filepath.Join(image, "census-before.json"), &before); err != nil {
		return err
	}
	if !before.Complete || before.Source != filepath.Join(image, "history") {
		return errors.New("complete source census required")
	}
	var cutoff *rawdb.PostingCensusCutoff
	for i := range before.Cutoffs {
		if before.Cutoffs[i].ThroughBlock == through {
			cutoff = &before.Cutoffs[i]
		}
	}
	if cutoff == nil {
		return errors.New("census lacks frozen boundary")
	}
	if err := enoughSpace(image); err != nil {
		return err
	}
	// A read-only DB lock pins the source while both copies are created. No RW
	// open in this program accepts the input image path.
	source, err := rawdb.NewPebbleDBReadOnly(filepath.Join(image, "history"), 64, 128)
	if err != nil {
		return err
	}
	defer source.Close()
	root, err := os.MkdirTemp(image, "private-reclaim-")
	if err != nil {
		return err
	}
	report := experiment{Started: time.Now().UTC(), Image: image, Through: through, Note: "Disposable-clone experiment only; no production deletion. Logical eligibility still requires online canonical/cold proof. Physical deltas compare identical full compaction with/without posting deletion; they are measured clone results, not byte-exact promises for a live LSM. Clone allocation includes shared immutable input SSTs, not host space freed."}
	save := func(phase string) error {
		report.Phase = phase
		fmt.Fprintln(os.Stderr, time.Now().UTC().Format(time.RFC3339), phase, root)
		b, err := json.MarshalIndent(report, "", "  ")
		if err != nil {
			return err
		}
		p := filepath.Join(root, "report.json.tmp")
		if err := os.WriteFile(p, b, 0600); err != nil {
			return err
		}
		return os.Rename(p, filepath.Join(root, "report.json"))
	}
	defer func() {
		if retErr != nil {
			report.Error = retErr.Error()
			_ = save("incomplete")
		}
	}()
	if err := save("copying"); err != nil {
		return err
	}
	report.Control.Path, report.Pruned.Path = filepath.Join(root, "control"), filepath.Join(root, "pruned")
	for _, path := range []string{report.Control.Path, report.Pruned.Path} {
		if err := copyImage(filepath.Join(image, "history"), path); err != nil {
			return err
		}
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := save("non-posting-source-census"); err != nil {
		return err
	}
	report.Complement, err = rawdb.InspectStateChangePostingComplement(ctx, source)
	if err != nil {
		return err
	}
	for _, branch := range []*branchResult{&report.Control, &report.Pruned} {
		if err := enoughSpace(image); err != nil {
			return err
		}
		branch.Before, err = measure(branch.Path)
		if err != nil {
			return err
		}
		phase := "control"
		deleting := branch == &report.Pruned
		if deleting {
			phase = "pruned"
		}
		if err := save(phase + "-open"); err != nil {
			return err
		}
		err = func() error {
			tune := rawdb.DefaultPebbleOptions()
			tune.EnableL6Filter = false
			tune.DisableAutomaticCompactions = true
			tune.MaxConcurrentCompactions = 1
			tune.MemTableSizeBytes = 128 << 20
			db, err := rawdb.NewPebbleDBWithOptions(branch.Path, 64, 128, tune)
			if err != nil {
				return err
			}
			defer db.Close()
			if deleting {
				if err := save("pruned-delete"); err != nil {
					return err
				}
				var cursor []byte
				last := time.Now()
				for {
					if err := ctx.Err(); err != nil {
						return err
					}
					started := time.Now()
					// Same deletion predicate, larger disposable-copy budgets.
					r, err := rawdb.PruneStaleStateChangePostingChunkContext(ctx, db, through, cursor, rawdb.StateChangePostingPruneLimits{MaxScannedRows: 65536, MaxScannedBytes: 8 << 20, MaxDeleteBytes: 8 << 20, MaxDuration: 250 * time.Millisecond})
					if err != nil {
						return err
					}
					branch.MaxChunkDuration = max(branch.MaxChunkDuration, time.Since(started))
					branch.Chunks++
					branch.ScanDuration += r.ScanDuration
					branch.WriteDuration += r.WriteDuration
					branch.DeletedRows += r.RowsDeleted
					branch.DeletedBytes += r.BytesDeleted
					cursor = r.NextCursor
					if time.Since(last) > 30*time.Second {
						if err := enoughSpace(image); err != nil {
							return err
						}
						if err := save("pruned-delete"); err != nil {
							return err
						}
						last = time.Now()
					}
					if r.Complete {
						break
					}
				}
				if branch.DeletedRows != cutoff.Eligible.Frames || branch.DeletedBytes != cutoff.Eligible.LogicalBytes {
					return errors.New("actual deletion disagrees with full census")
				}
			}
			if err := ctx.Err(); err != nil {
				return err
			}
			if err := save(phase + "-compact"); err != nil {
				return err
			}
			if err := db.Compact(nil, nil); err != nil {
				return err
			}
			if err := save(phase + "-verify"); err != nil {
				return err
			}
			branch.Census, err = rawdb.InspectStateChangePostings(ctx, db, []uint64{through}, nil)
			if err != nil {
				return err
			}
			branch.Complement, err = rawdb.InspectStateChangePostingComplement(ctx, db)
			if err != nil {
				return err
			}
			if branch.Complement != report.Complement || !branch.Census.Complete || branch.Census.DirectorySHA != before.DirectorySHA || branch.Census.DirectoryRows != before.DirectoryRows {
				return errors.New("non-posting data or directory changed")
			}
			got := branch.Census.Cutoffs[0]
			if !reflect.DeepEqual(got.Crossing, cutoff.Crossing) || !reflect.DeepEqual(got.Newer, cutoff.Newer) {
				return errors.New("retained posting classes changed")
			}
			if deleting {
				if got.Eligible.Frames != 0 || branch.Census.All.LogicalBytes+branch.DeletedBytes != before.All.LogicalBytes {
					return errors.New("pruned posting conservation failure")
				}
			} else if !reflect.DeepEqual(branch.Census.All, before.All) || !reflect.DeepEqual(got.Eligible, cutoff.Eligible) {
				return errors.New("control logical data changed")
			}
			branch.Verified = true
			return db.Close()
		}()
		if err != nil {
			return err
		}
		branch.After, err = measure(branch.Path)
		if err != nil {
			return err
		}
		if err := save(phase + "-verified"); err != nil {
			return err
		}
	}
	report.SSTDelta = int64(report.Control.After.SSTBytes) - int64(report.Pruned.After.SSTBytes)
	report.AllocationDelta = int64(report.Control.After.SSTAllocated) - int64(report.Pruned.After.SSTAllocated)
	report.Complete = true
	return save("complete")
}
