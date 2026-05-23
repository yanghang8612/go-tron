package snapshots

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/tronprotocol/go-tron/core/state/kvdomains"
)

const (
	ManifestVersion = 1
	ManifestFile    = "manifest.json"
)

type SegmentKind string

const (
	SegmentLatest   SegmentKind = "latest"
	SegmentAccessor SegmentKind = "accessor"
	SegmentHistory  SegmentKind = "history"
	SegmentInverted SegmentKind = "inverted"
)

type Manifest struct {
	Version        uint32       `json:"version"`
	PublishedUnix  int64        `json:"publishedUnix"`
	VisibleTxStart uint64       `json:"visibleTxStart"`
	VisibleTxEnd   uint64       `json:"visibleTxEnd"`
	Segments       []SegmentRef `json:"segments"`
}

type SegmentRef struct {
	Domain    kvdomains.KVDomain `json:"domain"`
	Kind      SegmentKind        `json:"kind"`
	FromTxNum uint64             `json:"fromTxNum"`
	ToTxNum   uint64             `json:"toTxNum"`
	Path      string             `json:"path"`
	Size      uint64             `json:"size"`
	Checksum  string             `json:"checksum,omitempty"`
}

func NewManifest(visibleTxStart, visibleTxEnd uint64, segments []SegmentRef) *Manifest {
	out := &Manifest{
		Version:        ManifestVersion,
		PublishedUnix:  time.Now().Unix(),
		VisibleTxStart: visibleTxStart,
		VisibleTxEnd:   visibleTxEnd,
		Segments:       append([]SegmentRef(nil), segments...),
	}
	sortSegments(out.Segments)
	return out
}

func LoadManifest(dir string) (*Manifest, error) {
	data, err := os.ReadFile(filepath.Join(dir, ManifestFile))
	if err != nil {
		return nil, err
	}
	var manifest Manifest
	if err := json.Unmarshal(data, &manifest); err != nil {
		return nil, err
	}
	if err := manifest.Validate(); err != nil {
		return nil, err
	}
	sortSegments(manifest.Segments)
	return &manifest, nil
}

func PublishManifest(dir string, manifest *Manifest) error {
	if manifest == nil {
		return errors.New("snapshots: nil manifest")
	}
	if manifest.Version == 0 {
		manifest.Version = ManifestVersion
	}
	if manifest.PublishedUnix == 0 {
		manifest.PublishedUnix = time.Now().Unix()
	}
	sortSegments(manifest.Segments)
	if err := manifest.Validate(); err != nil {
		return err
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	data, err := json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		return err
	}
	tmp, err := os.CreateTemp(dir, ".manifest-*.tmp")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	defer os.Remove(tmpName)
	if _, err := tmp.Write(data); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	return os.Rename(tmpName, filepath.Join(dir, ManifestFile))
}

func (m *Manifest) Validate() error {
	if m == nil {
		return errors.New("snapshots: nil manifest")
	}
	if m.Version != ManifestVersion {
		return fmt.Errorf("snapshots: unsupported manifest version %d", m.Version)
	}
	if m.VisibleTxEnd < m.VisibleTxStart {
		return fmt.Errorf("snapshots: visible range [%d,%d] is inverted", m.VisibleTxStart, m.VisibleTxEnd)
	}
	seenPath := make(map[string]struct{}, len(m.Segments))
	byFamily := make(map[segmentFamily][]SegmentRef)
	for _, seg := range m.Segments {
		if err := validateSegment(seg, m.VisibleTxStart, m.VisibleTxEnd); err != nil {
			return err
		}
		if _, dup := seenPath[seg.Path]; dup {
			return fmt.Errorf("snapshots: duplicate segment path %q", seg.Path)
		}
		seenPath[seg.Path] = struct{}{}
		fam := segmentFamily{domain: seg.Domain, kind: seg.Kind}
		byFamily[fam] = append(byFamily[fam], seg)
	}
	for family, segments := range byFamily {
		sort.Slice(segments, func(i, j int) bool {
			if segments[i].FromTxNum == segments[j].FromTxNum {
				return segments[i].ToTxNum < segments[j].ToTxNum
			}
			return segments[i].FromTxNum < segments[j].FromTxNum
		})
		for i := 1; i < len(segments); i++ {
			if segments[i].FromTxNum <= segments[i-1].ToTxNum {
				return fmt.Errorf("snapshots: overlapping %s segments for domain %#04x: [%d,%d] and [%d,%d]",
					family.kind, uint16(family.domain),
					segments[i-1].FromTxNum, segments[i-1].ToTxNum,
					segments[i].FromTxNum, segments[i].ToTxNum)
			}
		}
	}
	return nil
}

type segmentFamily struct {
	domain kvdomains.KVDomain
	kind   SegmentKind
}

func validateSegment(seg SegmentRef, visibleStart, visibleEnd uint64) error {
	if !kvdomains.IsRegistered(seg.Domain) {
		return fmt.Errorf("snapshots: unregistered domain %#04x", uint16(seg.Domain))
	}
	switch seg.Kind {
	case SegmentLatest, SegmentAccessor, SegmentHistory, SegmentInverted:
	default:
		return fmt.Errorf("snapshots: unknown segment kind %q", seg.Kind)
	}
	if seg.ToTxNum < seg.FromTxNum {
		return fmt.Errorf("snapshots: segment %q range [%d,%d] is inverted", seg.Path, seg.FromTxNum, seg.ToTxNum)
	}
	if seg.FromTxNum < visibleStart || seg.ToTxNum > visibleEnd {
		return fmt.Errorf("snapshots: segment %q range [%d,%d] outside visible range [%d,%d]",
			seg.Path, seg.FromTxNum, seg.ToTxNum, visibleStart, visibleEnd)
	}
	if seg.Path == "" || filepath.IsAbs(seg.Path) || filepath.Clean(seg.Path) != seg.Path || seg.Path == "." || hasParentDir(seg.Path) {
		return fmt.Errorf("snapshots: invalid relative segment path %q", seg.Path)
	}
	return nil
}

func hasParentDir(path string) bool {
	for _, part := range strings.Split(filepath.ToSlash(path), "/") {
		if part == ".." {
			return true
		}
	}
	return false
}

func sortSegments(segments []SegmentRef) {
	sort.Slice(segments, func(i, j int) bool {
		if segments[i].Domain != segments[j].Domain {
			return segments[i].Domain < segments[j].Domain
		}
		if segments[i].Kind != segments[j].Kind {
			return segments[i].Kind < segments[j].Kind
		}
		if segments[i].FromTxNum != segments[j].FromTxNum {
			return segments[i].FromTxNum < segments[j].FromTxNum
		}
		if segments[i].ToTxNum != segments[j].ToTxNum {
			return segments[i].ToTxNum < segments[j].ToTxNum
		}
		return segments[i].Path < segments[j].Path
	})
}
