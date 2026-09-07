package snapshots

import (
	"bytes"
	"context"
	"encoding/binary"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/tronprotocol/go-tron/core/rawdb"
)

// CDC stores shared anchors rather than one physical frame per logical block.
// Treating its intentionally absent fixed-block table as an empty sample used
// to project zero history bytes for non-default candidate block sizes.
func TestInspectHistorySpaceRejectsCDCProjectionWithoutChangingFiles(t *testing.T) {
	for _, mixed := range []bool{false, true} {
		name := "cdc-only"
		if mixed {
			name = "mixed-v1-cdc"
		}
		t.Run(name, func(t *testing.T) {
			dir := t.TempDir()
			write := func(from, to uint64, format string) []SegmentRef {
				t.Setenv("GTRON_HISTORY_COMPRESSION_FORMAT", format)
				var changes []*rawdb.StateDomainChange
				for tx := from; tx <= to; tx++ {
					c := binaryStateDomainChange(tx, tx, 1, "key")
					c.PrevExists = true
					c.Prev = bytes.Repeat([]byte{byte(tx)}, 256<<10)
					changes = append(changes, c)
				}
				return writeCompressedV6HistorySpaceTrio(t, dir, from, to, changes)
			}
			var refs []SegmentRef
			if mixed {
				refs = append(refs, write(1, 2, "1")...)
			}
			cdcRefs := write(3, 4, "3")
			refs = append(refs, cdcRefs...)
			var history SegmentRef
			for _, ref := range cdcRefs {
				if ref.Kind == SegmentHistory {
					history = ref
				}
			}
			data, err := os.ReadFile(filepath.Join(dir, history.Path))
			if err != nil || len(data) < compressedBlockHeaderSize || binary.BigEndian.Uint32(data[8:12]) != compressedBlockCDCVersion {
				t.Fatalf("fixture must be CDC v3: %v", err)
			}
			manifest := NewManifest(1, 4, refs)
			if err := PublishManifest(dir, manifest); err != nil {
				t.Fatal(err)
			}
			before := make(map[string][]byte)
			for _, ref := range append(append([]SegmentRef{}, refs...), SegmentRef{Path: ManifestFile}) {
				data, err := os.ReadFile(filepath.Join(dir, ref.Path))
				if err != nil {
					t.Fatal(err)
				}
				before[ref.Path] = data
			}
			out, err := InspectHistorySpace(dir, HistorySpaceInspectOptions{SampleSegments: 1})
			if out != nil || err == nil || !strings.Contains(err.Error(), "unsupported for CDC v3") {
				t.Fatalf("must return an explicit unsupported error and no projections: out=%+v err=%v", out, err)
			}
			// Guard the lower-level sampler too: future callers must not count
			// a zero sample against a positive selected-physical denominator.
			current, raw, candidates, _, err := sampleHistorySpaceCompression(context.Background(), dir, history, 1<<20)
			if err == nil || !strings.Contains(err.Error(), "unsupported for CDC v3") || current != 0 || raw != 0 || candidates != nil {
				t.Fatalf("sampler must reject CDC: %d %d %v %v", current, raw, candidates, err)
			}
			for path, old := range before {
				got, err := os.ReadFile(filepath.Join(dir, path))
				if err != nil || !bytes.Equal(got, old) {
					t.Fatalf("read-only rejection changed %q: %v", path, err)
				}
			}
		})
	}
}
