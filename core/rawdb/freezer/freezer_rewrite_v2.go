package freezer

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

// V2TxInfoRewriteOptions controls an offline, resumable rewrite of already
// published V2 tx_infos segments. Transform receives the tx_infos row and its
// matching bodies row. A manifest replacement is the commit point.
type V2TxInfoRewriteOptions struct {
	Context          context.Context
	MaxSegments      uint64
	ProgressInterval time.Duration
	Transform        func(number uint64, txInfo, body []byte) (data []byte, removed uint64, err error)
	// Observe runs exactly once per source row before transformation. It is
	// intended for safe prerequisite work such as upgrading hot reverse indexes.
	Observe func(number uint64, txInfo, body []byte) error
	// BeforePublish runs after the replacement segment verifies and before its
	// manifest is committed. Returning an error leaves the old segment active.
	BeforePublish func() error
	Progress      func(V2TxInfoRewriteProgress)
}

type V2TxInfoRewriteProgress struct {
	Stage        string
	Segment      uint64
	Start        uint64
	End          uint64
	Rows         uint64
	RemovedBytes uint64
	Elapsed      time.Duration
}

type V2TxInfoRewriteResult struct {
	Segments            uint64
	RewrittenSegments   uint64
	Rows                uint64
	RemovedBytes        uint64
	PhysicalBytesBefore uint64
	PhysicalBytesAfter  uint64
	Elapsed             time.Duration
}

// RewriteV2TransactionInfos atomically replaces existing V2 tx_infos files
// while retaining their bodies files. It is offline-only: callers must stop
// the node so no process holds the manifest or segment files open.
func (f *Freezer) RewriteV2TransactionInfos(options V2TxInfoRewriteOptions) (V2TxInfoRewriteResult, error) {
	var result V2TxInfoRewriteResult
	f.v2Migrate.Lock()
	defer f.v2Migrate.Unlock()
	if f.readonly {
		return result, errReadOnly
	}
	if options.Transform == nil {
		return result, fmt.Errorf("ancient V2 tx_infos rewrite: transform is required")
	}
	if options.Context == nil {
		options.Context = context.Background()
	}
	started := time.Now()
	result.PhysicalBytesBefore = f.v2TableSize("tx_infos")
	base := filepath.Join(f.datadir, "v2")
	manifests, err := readV2Manifests(base)
	if err != nil {
		return result, err
	}
	for _, manifest := range manifests {
		if err := options.Context.Err(); err != nil {
			return result, err
		}
		oldName, ok := manifest.Tables["tx_infos"]
		if !ok {
			return result, fmt.Errorf("ancient V2 manifest at %d has no tx_infos table", manifest.Start)
		}
		if manifest.TxInfoIDsCompacted {
			removeSupersededTxInfoSegment(base, manifest, oldName)
			continue
		}
		if options.MaxSegments > 0 && result.Segments >= options.MaxSegments {
			break
		}
		segmentStarted := time.Now()

		f.v2Mu.RLock()
		source := f.v2
		if source == nil || source.coverage < manifest.Start+manifest.Count {
			f.v2Mu.RUnlock()
			return result, fmt.Errorf("ancient V2 store does not cover manifest [%d,%d)", manifest.Start, manifest.Start+manifest.Count)
		}
		var (
			segmentRemoved uint64
			segmentChanged bool
			lastProgress   = time.Now()
		)
		readSource := func(number uint64) ([]byte, []byte, error) {
			txInfo, err := source.read("tx_infos", number)
			if err != nil {
				return nil, nil, err
			}
			body, err := source.read("bodies", number)
			if err != nil {
				return nil, nil, err
			}
			return txInfo, body, nil
		}
		for number := manifest.Start; number < manifest.Start+manifest.Count; number++ {
			if err := options.Context.Err(); err != nil {
				f.v2Mu.RUnlock()
				return result, err
			}
			txInfo, body, err := readSource(number)
			if err != nil {
				f.v2Mu.RUnlock()
				return result, fmt.Errorf("audit ancient V2 tx_infos segment %d row %d: %w", manifest.Start, number, err)
			}
			if options.Observe != nil {
				if err := options.Observe(number, txInfo, body); err != nil {
					f.v2Mu.RUnlock()
					return result, err
				}
			}
			data, removed, err := options.Transform(number, txInfo, body)
			if err != nil {
				f.v2Mu.RUnlock()
				return result, fmt.Errorf("audit ancient V2 tx_infos segment %d row %d: %w", manifest.Start, number, err)
			}
			segmentRemoved += removed
			if !bytes.Equal(data, txInfo) {
				segmentChanged = true
			}
			if options.Progress != nil && options.ProgressInterval > 0 && time.Since(lastProgress) >= options.ProgressInterval {
				options.Progress(V2TxInfoRewriteProgress{
					Stage: "auditing", Segment: result.Segments + 1,
					Start: manifest.Start, End: manifest.Start + manifest.Count,
					Rows: number - manifest.Start + 1, RemovedBytes: segmentRemoved,
					Elapsed: time.Since(segmentStarted),
				})
				lastProgress = time.Now()
			}
		}
		f.v2Mu.RUnlock()

		if !segmentChanged {
			if options.BeforePublish != nil {
				if err := options.BeforePublish(); err != nil {
					return result, fmt.Errorf("prepare audited V2 manifest %d: %w", manifest.Start, err)
				}
			}
			manifest.TxInfoIDsCompacted = true
			if err := publishV2Manifest(base, manifest); err != nil {
				return result, fmt.Errorf("publish audited V2 manifest %d: %w", manifest.Start, err)
			}
			result.Segments++
			result.Rows += manifest.Count
			if options.Progress != nil {
				options.Progress(V2TxInfoRewriteProgress{
					Stage: "marked", Segment: result.Segments,
					Start: manifest.Start, End: manifest.Start + manifest.Count,
					Rows: manifest.Count, Elapsed: time.Since(segmentStarted),
				})
			}
			continue
		}

		newName := strings.TrimSuffix(v2SegmentName(manifest.Start, manifest.Count), ".gtv2") + "-txidless.gtv2"
		newPath := filepath.Join(base, "tx_infos", newName)
		lastProgress = time.Now()
		var writeRows uint64
		readTransformed := func(number uint64) ([]byte, error) {
			txInfo, body, err := readSource(number)
			if err != nil {
				return nil, err
			}
			data, _, err := options.Transform(number, txInfo, body)
			writeRows++
			if options.Progress != nil && options.ProgressInterval > 0 && time.Since(lastProgress) >= options.ProgressInterval {
				options.Progress(V2TxInfoRewriteProgress{
					Stage: "rewriting", Segment: result.Segments + 1,
					Start: manifest.Start, End: manifest.Start + manifest.Count,
					Rows: writeRows, RemovedBytes: segmentRemoved,
					Elapsed: time.Since(segmentStarted),
				})
				lastProgress = time.Now()
			}
			return data, err
		}

		f.v2Mu.RLock()
		source = f.v2
		if source == nil || source.coverage < manifest.Start+manifest.Count {
			f.v2Mu.RUnlock()
			return result, fmt.Errorf("ancient V2 store does not cover manifest [%d,%d)", manifest.Start, manifest.Start+manifest.Count)
		}
		err = writeV2Segment(newPath, manifest.Start, manifest.Count, manifest.FrameBlocks, func(number uint64) ([]byte, error) {
			return readTransformed(number)
		})
		if err == nil {
			err = verifyV2Segment(options.Context, newPath, "tx_infos", manifest.Start, manifest.Count, func(number uint64) ([]byte, error) {
				txInfo, body, readErr := readSource(number)
				if readErr != nil {
					return nil, readErr
				}
				data, _, transformErr := options.Transform(number, txInfo, body)
				return data, transformErr
			})
		}
		f.v2Mu.RUnlock()
		if err != nil {
			_ = os.Remove(newPath)
			return result, fmt.Errorf("rewrite ancient V2 tx_infos segment %d: %w", manifest.Start, err)
		}
		if options.BeforePublish != nil {
			if err := options.BeforePublish(); err != nil {
				_ = os.Remove(newPath)
				return result, fmt.Errorf("prepare rewritten ancient V2 manifest %d: %w", manifest.Start, err)
			}
		}

		manifest.Tables["tx_infos"] = newName
		manifest.TxInfoIDsCompacted = true
		if err := publishV2Manifest(base, manifest); err != nil {
			return result, fmt.Errorf("publish rewritten ancient V2 manifest %d: %w", manifest.Start, err)
		}
		newStore, err := openV2Store(f.datadir)
		if err != nil {
			return result, fmt.Errorf("reload rewritten ancient V2 segment %d: %w", manifest.Start, err)
		}
		f.replaceV2Store(newStore)
		if oldName != newName {
			_ = os.Remove(filepath.Join(base, "tx_infos", oldName))
		}
		result.Segments++
		result.RewrittenSegments++
		result.Rows += manifest.Count
		result.RemovedBytes += segmentRemoved
		if options.Progress != nil {
			options.Progress(V2TxInfoRewriteProgress{
				Stage: "rewritten", Segment: result.Segments, Start: manifest.Start, End: manifest.Start + manifest.Count,
				Rows: manifest.Count, RemovedBytes: segmentRemoved, Elapsed: time.Since(segmentStarted),
			})
		}
	}
	result.PhysicalBytesAfter = f.v2TableSize("tx_infos")
	result.Elapsed = time.Since(started)
	return result, nil
}

func (f *Freezer) v2TableSize(kind string) uint64 {
	f.v2Mu.RLock()
	defer f.v2Mu.RUnlock()
	if f.v2 == nil {
		return 0
	}
	return f.v2.size(kind)
}

func readV2Manifests(base string) ([]v2Manifest, error) {
	dir := filepath.Join(base, "manifests")
	entries, err := os.ReadDir(dir)
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	manifests := make([]v2Manifest, 0, len(entries))
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".json") {
			continue
		}
		data, err := os.ReadFile(filepath.Join(dir, entry.Name()))
		if err != nil {
			return nil, err
		}
		var manifest v2Manifest
		if err := json.Unmarshal(data, &manifest); err != nil {
			return nil, fmt.Errorf("decode ancient V2 manifest %s: %w", entry.Name(), err)
		}
		manifests = append(manifests, manifest)
	}
	sort.Slice(manifests, func(i, j int) bool { return manifests[i].Start < manifests[j].Start })
	return manifests, nil
}

func removeSupersededTxInfoSegment(base string, manifest v2Manifest, current string) {
	legacy := v2SegmentName(manifest.Start, manifest.Count)
	if legacy != current {
		_ = os.Remove(filepath.Join(base, "tx_infos", legacy))
	}
}
