package snapshots

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/state/kvdomains"
)

// SegmentDatasetCommitmentBranch labels the staged commitment engine's
// branch-row snapshot family. It streams the dedicated
// state-commitment-branch-v1- keyspace (hex-trie prefix -> encoded BranchData),
// which the legacy CommitmentNode family (tree/node/ logical keys, 32-byte hash
// values) cannot represent. It is registered in DefaultDomainRegistry as a
// single-file (JSON) latest dataset (HasLatest=true, HasLatestAccessor=false,
// HasLatestBTree=false): one .json file per build, no binary companion files.
const SegmentDatasetCommitmentBranch SegmentDataset = "commitment-branch"

// CommitmentBranchSegmentVersion is the on-disk version of a branch segment.
const CommitmentBranchSegmentVersion = 1

// commitmentBranchEntry is one persisted branch row. Encoded is the opaque
// BranchData.Encode() value; the snapshot layer never decodes it.
type commitmentBranchEntry struct {
	Prefix  []byte `json:"prefix"`
	Encoded []byte `json:"encoded"`
}

// commitmentBranchSegment is the JSON document persisted for a branch family
// segment. It mirrors the shape of LatestSegment but is deliberately
// self-contained: it does not route through LatestSegment.Validate or the
// dataset registry.
type commitmentBranchSegment struct {
	Version   uint32                  `json:"version"`
	Dataset   SegmentDataset          `json:"dataset"`
	FromTxNum uint64                  `json:"fromTxNum"`
	ToTxNum   uint64                  `json:"toTxNum"`
	Entries   []commitmentBranchEntry `json:"entries"`
}

// CommitmentBranchSegment is an opened, validated branch segment ready for
// iteration.
type CommitmentBranchSegment struct {
	ref SegmentRef
	seg *commitmentBranchSegment
}

// BuildCommitmentBranchSegmentFromDB streams every state-commitment-branch-v1-
// row from db into a branch segment file at dir/relPath and returns its
// SegmentRef. Rows are written sorted by prefix for a deterministic file.
func BuildCommitmentBranchSegmentFromDB(db ethdb.Iteratee, dir, relPath string, fromTxNum, toTxNum uint64) (SegmentRef, error) {
	if db == nil {
		return SegmentRef{}, errors.New("snapshots: nil database")
	}
	if err := validateBranchSegmentPath(relPath); err != nil {
		return SegmentRef{}, err
	}
	if toTxNum < fromTxNum {
		return SegmentRef{}, fmt.Errorf("snapshots: branch segment range [%d,%d] is inverted", fromTxNum, toTxNum)
	}
	seg := &commitmentBranchSegment{
		Version:   CommitmentBranchSegmentVersion,
		Dataset:   SegmentDatasetCommitmentBranch,
		FromTxNum: fromTxNum,
		ToTxNum:   toTxNum,
	}
	if err := rawdb.IterateCommitmentBranches(db, func(prefix, encoded []byte) (bool, error) {
		seg.Entries = append(seg.Entries, commitmentBranchEntry{
			Prefix:  append([]byte(nil), prefix...),
			Encoded: append([]byte(nil), encoded...),
		})
		return true, nil
	}); err != nil {
		return SegmentRef{}, err
	}
	return writeCommitmentBranchSegment(dir, relPath, seg, fromTxNum, toTxNum)
}

func writeCommitmentBranchSegment(dir, relPath string, seg *commitmentBranchSegment, fromTxNum, toTxNum uint64) (SegmentRef, error) {
	data, err := json.Marshal(seg)
	if err != nil {
		return SegmentRef{}, err
	}
	sum := sha256.Sum256(data)
	ref := SegmentRef{
		Dataset:   SegmentDatasetCommitmentBranch,
		Kind:      SegmentLatest,
		FromTxNum: fromTxNum,
		ToTxNum:   toTxNum,
		Path:      filepath.ToSlash(relPath),
		Size:      uint64(len(data)),
		Checksum:  "sha256:" + hex.EncodeToString(sum[:]),
	}
	abs := filepath.Join(dir, relPath)
	if err := os.MkdirAll(filepath.Dir(abs), 0o755); err != nil {
		return SegmentRef{}, err
	}
	tmp, err := os.CreateTemp(filepath.Dir(abs), "."+filepath.Base(abs)+".*.tmp")
	if err != nil {
		return SegmentRef{}, err
	}
	tmpName := tmp.Name()
	defer os.Remove(tmpName)
	if _, err := tmp.Write(data); err != nil {
		_ = tmp.Close()
		return SegmentRef{}, err
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return SegmentRef{}, err
	}
	if err := tmp.Close(); err != nil {
		return SegmentRef{}, err
	}
	if err := os.Rename(tmpName, abs); err != nil {
		return SegmentRef{}, err
	}
	return ref, nil
}

// OpenCommitmentBranchSegment loads and validates the branch segment at
// dir/ref.Path.
func OpenCommitmentBranchSegment(dir string, ref SegmentRef) (*CommitmentBranchSegment, error) {
	if ref.Dataset != SegmentDatasetCommitmentBranch {
		return nil, fmt.Errorf("snapshots: segment %q dataset %q, want %q", ref.Path, ref.Dataset, SegmentDatasetCommitmentBranch)
	}
	if err := validateBranchSegmentPath(ref.Path); err != nil {
		return nil, err
	}
	data, err := os.ReadFile(filepath.Join(dir, ref.Path))
	if err != nil {
		return nil, err
	}
	if ref.Size != 0 && uint64(len(data)) != ref.Size {
		return nil, fmt.Errorf("snapshots: branch segment %q size %d, want %d", ref.Path, len(data), ref.Size)
	}
	if ref.Checksum != "" {
		sum := sha256.Sum256(data)
		want := "sha256:" + hex.EncodeToString(sum[:])
		if !strings.EqualFold(ref.Checksum, want) {
			return nil, fmt.Errorf("snapshots: branch segment %q checksum %s, want %s", ref.Path, want, ref.Checksum)
		}
	}
	var seg commitmentBranchSegment
	if err := json.Unmarshal(data, &seg); err != nil {
		return nil, err
	}
	if seg.Version != CommitmentBranchSegmentVersion {
		return nil, fmt.Errorf("snapshots: unsupported branch segment version %d", seg.Version)
	}
	if seg.Dataset != SegmentDatasetCommitmentBranch {
		return nil, fmt.Errorf("snapshots: branch segment %q dataset %q", ref.Path, seg.Dataset)
	}
	if seg.FromTxNum != ref.FromTxNum || seg.ToTxNum != ref.ToTxNum {
		return nil, fmt.Errorf("snapshots: branch segment %q metadata does not match manifest", ref.Path)
	}
	return &CommitmentBranchSegment{ref: ref, seg: &seg}, nil
}

// Iterate calls fn with each (prefix, encoded) branch row in the segment.
func (s *CommitmentBranchSegment) Iterate(fn func(prefix, encoded []byte) (bool, error)) error {
	if s == nil || s.seg == nil {
		return nil
	}
	for _, entry := range s.seg.Entries {
		cont, err := fn(append([]byte(nil), entry.Prefix...), append([]byte(nil), entry.Encoded...))
		if err != nil {
			return err
		}
		if !cont {
			return nil
		}
	}
	return nil
}

// CommitmentBranchSource adapts the cold-snapshot layer to the staged engine's
// restore seam. It embeds *Manager for the snapshot root (GetCommitmentRoot) and
// the legacy node iterator (so it also satisfies the engine-agnostic
// CommitmentSnapshotSource), and serves the staged branch rows directly from a
// branch segment file. It thus satisfies both domains.CommitmentSnapshotSource
// and domains.CommitmentBranchSnapshotSource WITHOUT this package importing
// domains (which would be an import cycle via the domains test package).
type CommitmentBranchSource struct {
	*Manager
	dir       string
	branchRef SegmentRef
}

// NewCommitmentBranchSource builds a CommitmentBranchSource. mgr supplies the
// snapshot root; branchRef locates the branch segment file under dir.
func NewCommitmentBranchSource(mgr *Manager, dir string, branchRef SegmentRef) *CommitmentBranchSource {
	return &CommitmentBranchSource{Manager: mgr, dir: dir, branchRef: branchRef}
}

// IterateCommitmentBranches streams the snapshotted branch rows when txNum falls
// within the branch segment's visible range, else yields nothing. The txNum gate
// mirrors the latest-segment selection rule so a restore request for a tx range
// the snapshot does not cover declines cleanly (the staged store then falls
// through to Rebuild).
func (s *CommitmentBranchSource) IterateCommitmentBranches(txNum uint64, fn func(prefix, encoded []byte) (bool, error)) error {
	if s == nil || s.branchRef.Path == "" {
		return nil
	}
	if txNum < s.branchRef.FromTxNum || txNum > s.branchRef.ToTxNum {
		return nil
	}
	seg, err := OpenCommitmentBranchSegment(s.dir, s.branchRef)
	if err != nil {
		return err
	}
	return seg.Iterate(fn)
}

// hasAnyCommitmentBranchRow reports whether the state-commitment-branch-v1-
// keyspace is non-empty without materializing it.
func hasAnyCommitmentBranchRow(db ethdb.Iteratee) (bool, error) {
	found := false
	if err := rawdb.IterateCommitmentBranches(db, func(_, _ []byte) (bool, error) {
		found = true
		return false, nil // stop after first row
	}); err != nil {
		return false, err
	}
	return found, nil
}

// buildCommitmentBranchLatest is the registry LatestSnapshotBuilder adapter for
// the CommitmentBranch family. It returns no ref (publishes nothing) when the
// branch keyspace is empty, mirroring Runner.onePass's "no rows, return early".
func buildCommitmentBranchLatest(db AggregatorDB, dir string, _ kvdomains.KVDomain, fromTxNum, toTxNum uint64, relPath string) ([]SegmentRef, error) {
	has, err := hasAnyCommitmentBranchRow(db)
	if err != nil {
		return nil, err
	}
	if !has {
		return nil, nil
	}
	ref, err := BuildCommitmentBranchSegmentFromDB(db, dir, relPath, fromTxNum, toTxNum)
	if err != nil {
		return nil, err
	}
	return []SegmentRef{ref}, nil
}

// checkCommitmentBranchSegment validates a published branch segment by opening
// it (checksum + metadata) — the registry CheckLatest hook for the family.
func checkCommitmentBranchSegment(dir string, ref SegmentRef) error {
	_, err := OpenCommitmentBranchSegment(dir, ref)
	return err
}

func validateBranchSegmentPath(path string) error {
	if path == "" || filepath.IsAbs(path) || filepath.Clean(path) != path || path == "." || hasParentDir(path) {
		return fmt.Errorf("snapshots: invalid relative branch segment path %q", path)
	}
	return nil
}
