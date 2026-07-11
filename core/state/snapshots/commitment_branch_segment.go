package snapshots

import (
	"bufio"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"hash"
	"io"
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

// CommitmentBranchSegment is an opened, validated branch segment ready for
// streaming iteration. It retains only the segment location; branch rows stay
// on disk until Iterate consumes them.
type CommitmentBranchSegment struct {
	ref  SegmentRef
	path string
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
	return writeCommitmentBranchSegmentFromDB(db, dir, relPath, fromTxNum, toTxNum)
}

func writeCommitmentBranchSegmentFromDB(db ethdb.Iteratee, dir, relPath string, fromTxNum, toTxNum uint64) (SegmentRef, error) {
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

	hash := sha256.New()
	counter := &countingWriter{w: io.MultiWriter(tmp, hash)}
	writer := bufio.NewWriterSize(counter, 1<<20)
	if _, err := fmt.Fprintf(writer, `{"version":%d,"dataset":%q,"fromTxNum":%d,"toTxNum":%d,"entries":[`, CommitmentBranchSegmentVersion, SegmentDatasetCommitmentBranch, fromTxNum, toTxNum); err != nil {
		_ = tmp.Close()
		return SegmentRef{}, err
	}
	first := true
	if err := rawdb.IterateCommitmentBranches(db, func(prefix, encoded []byte) (bool, error) {
		if !first {
			if err := writer.WriteByte(','); err != nil {
				return false, err
			}
		}
		first = false
		entry, err := json.Marshal(commitmentBranchEntry{Prefix: prefix, Encoded: encoded})
		if err != nil {
			return false, err
		}
		if _, err := writer.Write(entry); err != nil {
			return false, err
		}
		return true, nil
	}); err != nil {
		_ = tmp.Close()
		return SegmentRef{}, err
	}
	if _, err := writer.WriteString(`]}`); err != nil {
		_ = tmp.Close()
		return SegmentRef{}, err
	}
	if err := writer.Flush(); err != nil {
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

	ref := SegmentRef{
		Dataset:   SegmentDatasetCommitmentBranch,
		Kind:      SegmentLatest,
		FromTxNum: fromTxNum,
		ToTxNum:   toTxNum,
		Path:      filepath.ToSlash(relPath),
		Size:      counter.n,
		Checksum:  "sha256:" + hex.EncodeToString(hash.Sum(nil)),
	}
	ref.Path = contentAddressedSnapshotPath(ref.Path, ref.Checksum)
	finalAbs := filepath.Join(dir, ref.Path)
	if err := os.MkdirAll(filepath.Dir(finalAbs), 0o755); err != nil {
		return SegmentRef{}, err
	}
	if err := os.Rename(tmpName, finalAbs); err != nil {
		return SegmentRef{}, err
	}
	return ref, nil
}

// OpenCommitmentBranchSegment validates the branch segment at dir/ref.Path.
// The returned handle keeps no entries in memory; Iterate opens and streams the
// verified segment file when its rows are needed.
func OpenCommitmentBranchSegment(dir string, ref SegmentRef) (*CommitmentBranchSegment, error) {
	if ref.Dataset != SegmentDatasetCommitmentBranch {
		return nil, fmt.Errorf("snapshots: segment %q dataset %q, want %q", ref.Path, ref.Dataset, SegmentDatasetCommitmentBranch)
	}
	if err := validateBranchSegmentPath(ref.Path); err != nil {
		return nil, err
	}
	path := filepath.Join(dir, ref.Path)
	if err := streamCommitmentBranchSegment(path, ref, true, nil); err != nil {
		return nil, err
	}
	return &CommitmentBranchSegment{ref: ref, path: path}, nil
}

// Iterate calls fn with each (prefix, encoded) branch row in the segment.
func (s *CommitmentBranchSegment) Iterate(fn func(prefix, encoded []byte) (bool, error)) error {
	if s == nil || s.path == "" {
		return nil
	}
	return streamCommitmentBranchSegment(s.path, s.ref, false, fn)
}

// streamCommitmentBranchSegment parses the branch document one field and one
// entry at a time. Open calls it with verifyChecksum before handing the segment
// to a caller, so Restore cannot persist rows from a corrupt snapshot. Iterate
// then performs a second, bounded-memory pass over its immutable file.
func streamCommitmentBranchSegment(path string, ref SegmentRef, verifyChecksum bool, fn func(prefix, encoded []byte) (bool, error)) error {
	file, err := os.Open(path)
	if err != nil {
		return err
	}
	defer file.Close()
	stat, err := file.Stat()
	if err != nil {
		return err
	}
	if ref.Size != 0 && uint64(stat.Size()) != ref.Size {
		return fmt.Errorf("snapshots: branch segment %q size %d, want %d", ref.Path, stat.Size(), ref.Size)
	}

	reader := io.Reader(file)
	var checksum hash.Hash
	if verifyChecksum && ref.Checksum != "" {
		checksum = sha256.New()
		reader = io.TeeReader(reader, checksum)
	}
	decoder := json.NewDecoder(reader)
	start, err := decoder.Token()
	if err != nil {
		return fmt.Errorf("snapshots: decode branch segment %q: %w", ref.Path, err)
	}
	if delim, ok := start.(json.Delim); !ok || delim != '{' {
		return fmt.Errorf("snapshots: branch segment %q must contain an object", ref.Path)
	}

	var version uint32
	var dataset SegmentDataset
	var fromTxNum, toTxNum uint64
	for decoder.More() {
		field, err := decoder.Token()
		if err != nil {
			return fmt.Errorf("snapshots: decode branch segment %q field: %w", ref.Path, err)
		}
		name, ok := field.(string)
		if !ok {
			return fmt.Errorf("snapshots: branch segment %q contains a non-string field", ref.Path)
		}
		switch name {
		case "version":
			err = decoder.Decode(&version)
		case "dataset":
			err = decoder.Decode(&dataset)
		case "fromTxNum":
			err = decoder.Decode(&fromTxNum)
		case "toTxNum":
			err = decoder.Decode(&toTxNum)
		case "entries":
			err = streamCommitmentBranchEntries(decoder, fn)
		default:
			var ignored json.RawMessage
			err = decoder.Decode(&ignored)
		}
		if err != nil {
			return fmt.Errorf("snapshots: decode branch segment %q field %q: %w", ref.Path, name, err)
		}
	}
	end, err := decoder.Token()
	if err != nil {
		return fmt.Errorf("snapshots: decode branch segment %q end: %w", ref.Path, err)
	}
	if delim, ok := end.(json.Delim); !ok || delim != '}' {
		return fmt.Errorf("snapshots: branch segment %q has an invalid object terminator", ref.Path)
	}
	var trailing json.RawMessage
	if err := decoder.Decode(&trailing); err != io.EOF {
		if err == nil {
			return fmt.Errorf("snapshots: branch segment %q has trailing JSON data", ref.Path)
		}
		return fmt.Errorf("snapshots: decode branch segment %q trailing data: %w", ref.Path, err)
	}
	if _, err := io.Copy(io.Discard, reader); err != nil {
		return err
	}
	if checksum != nil {
		want := "sha256:" + hex.EncodeToString(checksum.Sum(nil))
		if !strings.EqualFold(ref.Checksum, want) {
			return fmt.Errorf("snapshots: branch segment %q checksum %s, want %s", ref.Path, want, ref.Checksum)
		}
	}
	if version != CommitmentBranchSegmentVersion {
		return fmt.Errorf("snapshots: unsupported branch segment version %d", version)
	}
	if dataset != SegmentDatasetCommitmentBranch {
		return fmt.Errorf("snapshots: branch segment %q dataset %q", ref.Path, dataset)
	}
	if fromTxNum != ref.FromTxNum || toTxNum != ref.ToTxNum {
		return fmt.Errorf("snapshots: branch segment %q metadata does not match manifest", ref.Path)
	}
	return nil
}

func streamCommitmentBranchEntries(decoder *json.Decoder, fn func(prefix, encoded []byte) (bool, error)) error {
	start, err := decoder.Token()
	if err != nil {
		return err
	}
	if start == nil {
		return nil
	}
	if delim, ok := start.(json.Delim); !ok || delim != '[' {
		return errors.New("entries must be an array")
	}
	callFn := fn != nil
	for decoder.More() {
		var entry commitmentBranchEntry
		if err := decoder.Decode(&entry); err != nil {
			return err
		}
		if !callFn {
			continue
		}
		cont, err := fn(entry.Prefix, entry.Encoded)
		if err != nil {
			return err
		}
		if !cont {
			// The caller asked to stop receiving rows, but the surrounding
			// document still has to be consumed so the parser remains aligned.
			callFn = false
		}
	}
	end, err := decoder.Token()
	if err != nil {
		return err
	}
	if delim, ok := end.(json.Delim); !ok || delim != ']' {
		return errors.New("entries array has an invalid terminator")
	}
	return nil
}

func (s *CommitmentBranchSegment) Restore(db ethdb.KeyValueWriter) error {
	if db == nil {
		return errors.New("snapshots: nil database")
	}
	return s.Iterate(func(prefix, encoded []byte) (bool, error) {
		return true, rawdb.WriteCommitmentBranch(db, prefix, encoded)
	})
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

// checkCommitmentBranchSegment validates a published branch segment without
// materializing its branch rows — the registry CheckLatest hook for the family.
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
