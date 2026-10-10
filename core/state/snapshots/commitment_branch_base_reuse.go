package snapshots

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/tronprotocol/go-tron/core/rawdb"
)

// loadPublishedCommitmentBranchBase authenticates only the exact root/branch
// families. Reuse never republishes a manifest or reports files as newly built.
// An absent boundary permits construction; a partial/conflicting or damaged
// published boundary is an error, never permission to overwrite its files.
func authenticatePublishedCommitmentBranchBase(ctx context.Context, dir string, rotation rawdb.CommitmentBranchRotation) (*VerifiedCommitmentBranchBase, bool, error) {
	if err := contextError(ctx); err != nil {
		return nil, false, err
	}
	manifest, err := LoadProductionManifest(dir)
	if os.IsNotExist(err) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	mains := make(map[SegmentDataset]SegmentRef, 2)
	for _, ref := range manifest.Segments {
		dataset := ref.NormalizedDataset()
		if (dataset != SegmentDatasetCommitmentRoot && dataset != SegmentDatasetCommitmentBranch) || ref.Kind != SegmentLatest {
			continue
		}
		if ref.ToTxNum > rotation.SnapshotTxNum {
			return nil, false, errors.New("snapshots: published commitment family is newer than pending rotation")
		}
		if ref.ToTxNum != rotation.SnapshotTxNum {
			continue
		}
		if _, exists := mains[dataset]; exists {
			return nil, false, fmt.Errorf("snapshots: duplicate published commitment family %s", dataset)
		}
		mains[dataset] = ref
	}
	if len(mains) == 0 {
		return nil, false, nil
	}
	if len(mains) != 2 {
		return nil, false, errors.New("snapshots: partial published commitment base")
	}
	root, branch := mains[SegmentDatasetCommitmentRoot], mains[SegmentDatasetCommitmentBranch]
	if root.FromTxNum != branch.FromTxNum || !isLatestBinarySegmentPath(root.Path) || !isLatestBinarySegmentPath(branch.Path) {
		return nil, false, errors.New("snapshots: published commitment base family range or format mismatch")
	}
	facts := make(map[SegmentRef]historyStagingFileState, 6)
	registry := DefaultDomainRegistry()
	for _, ref := range []SegmentRef{root, branch} {
		family := []SegmentRef{ref}
		btree, ok := latestBinaryBTreeRef(manifest, ref)
		if !ok {
			return nil, false, fmt.Errorf("snapshots: commitment base %s missing btree", ref.Path)
		}
		family = append(family, btree)
		accessor, ok := latestBinaryAccessorRef(manifest, ref)
		if !ok && ref.NormalizedDataset() == SegmentDatasetCommitmentBranch {
			return nil, false, errors.New("snapshots: commitment branch base missing accessor")
		}
		if ok {
			family = append(family, accessor)
		}
		for _, member := range family {
			if member.Size == 0 || strings.TrimSpace(member.Checksum) == "" {
				return nil, false, fmt.Errorf("snapshots: commitment base file %s lacks size/checksum", member.Path)
			}
			state, err := historyStagingFileFingerprint(dir, member)
			if err != nil || !state.hasChangeTime {
				return nil, false, fmt.Errorf("snapshots: commitment base identity %s unavailable: %v", member.Path, err)
			}
			facts[member] = state
		}
		cfg, ok := registry.Dataset(ref.NormalizedDataset())
		if !ok {
			return nil, false, errors.New("snapshots: unregistered commitment base family")
		}
		if err := verifyManifestLatestBinarySidecarSet(ctx, dir, manifest, cfg, ref); err != nil {
			return nil, false, err
		}
	}
	mgr, err := OpenPinnedManager(dir, manifest)
	if err != nil {
		return nil, false, err
	}
	got, ok, err := mgr.GetCommitmentRoot(rotation.SnapshotTxNum)
	if err != nil {
		return nil, false, err
	}
	if !ok || got != rotation.Root {
		return nil, false, errors.New("snapshots: published commitment base root differs from rotation")
	}
	proof := &VerifiedCommitmentBranchBase{manager: mgr, rotation: rotation, facts: facts}
	if err := proof.PrepareDurable(ctx); err != nil {
		return nil, false, err
	}
	if err := contextError(ctx); err != nil {
		return nil, false, err
	}
	return proof, true, nil
}

// VerifiedCommitmentBranchBase binds a complete family audit to immutable file
// identities and an exact rotation. Its private fields prevent callers from
// replacing full authentication with an unverified Manager.
type VerifiedCommitmentBranchBase struct {
	manager       *Manager
	rotation      rawdb.CommitmentBranchRotation
	facts         map[SegmentRef]historyStagingFileState
	durable       bool
	manifestState historyStagingFileState
}

func (p *VerifiedCommitmentBranchBase) Manager() *Manager {
	if p == nil {
		return nil
	}
	return p.manager
}

// Recheck is metadata-only and safe under the final chain publication barrier.
// Callers retain the maintenance lease from authentication through acceptance.
func (p *VerifiedCommitmentBranchBase) Recheck(rotation rawdb.CommitmentBranchRotation) error {
	if p == nil || !p.durable {
		return errors.New("snapshots: commitment base durability barrier incomplete")
	}
	after, err := historyStagingFileFingerprint(p.manager.dir, SegmentRef{Path: ManifestFile, Size: uint64(p.manifestState.info.Size())})
	if err != nil || !after.unchanged(p.manifestState) {
		return errors.New("snapshots: commitment base publication changed")
	}
	return p.recheckFiles(rotation)
}

func (p *VerifiedCommitmentBranchBase) recheckFiles(rotation rawdb.CommitmentBranchRotation) error {
	if p == nil || p.manager == nil || p.rotation != rotation || len(p.facts) < 5 {
		return errors.New("snapshots: missing or mismatched authenticated commitment base")
	}
	for ref, before := range p.facts {
		after, err := historyStagingFileFingerprint(p.manager.dir, ref)
		if err != nil || !after.unchanged(before) {
			return fmt.Errorf("snapshots: authenticated commitment base file changed: %s", ref.Path)
		}
	}
	return nil
}

// VerifyCommitmentBranchBase authenticates the exact published root/branch
// families outside chain locks. An absent family is an error for acceptance.
func VerifyCommitmentBranchBase(ctx context.Context, dir string, rotation rawdb.CommitmentBranchRotation) (*VerifiedCommitmentBranchBase, error) {
	proof, ok, err := authenticatePublishedCommitmentBranchBase(ctx, dir, rotation)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, errors.New("snapshots: commitment base is not published")
	}
	return proof, nil
}

func loadPublishedCommitmentBranchBase(ctx context.Context, dir string, rotation rawdb.CommitmentBranchRotation) (*AggregatorBuildResult, bool, error) {
	proof, ok, err := authenticatePublishedCommitmentBranchBase(ctx, dir, rotation)
	if err != nil || !ok {
		return nil, ok, err
	}
	return &AggregatorBuildResult{Manifest: proof.manager.manifest, commitmentBaseProof: proof}, true, nil
}

// PrepareDurable synchronizes the exact active family and manifest before hot
// deletion can become authoritative. It also repairs a visible publication whose
// prior directory sync failed. All slow I/O stays outside the chain barrier.
func (p *VerifiedCommitmentBranchBase) PrepareDurable(ctx context.Context) error {
	syncFile := func(path string) error {
		if err := contextError(ctx); err != nil {
			return err
		}
		f, err := os.Open(path)
		if err != nil {
			return err
		}
		return errors.Join(f.Sync(), f.Close())
	}
	return p.prepareDurable(ctx, syncFile, strictCommitmentSnapshotDirSync)
}

func (p *VerifiedCommitmentBranchBase) prepareDurable(ctx context.Context, syncFile, syncDir func(string) error) error {
	if p == nil || p.manager == nil {
		return errors.New("snapshots: missing commitment base proof")
	}
	p.durable = false
	if err := contextError(ctx); err != nil {
		return err
	}
	if err := p.recheckFiles(p.rotation); err != nil {
		return err
	}
	root, err := filepath.Abs(p.manager.dir)
	if err != nil {
		return err
	}
	info, err := os.Lstat(filepath.Join(root, ManifestFile))
	if err != nil {
		return err
	}
	manifestRef := SegmentRef{Path: ManifestFile, Size: uint64(info.Size())}
	before, err := historyStagingFileFingerprint(root, manifestRef)
	if err != nil || !before.hasChangeTime {
		return errors.New("snapshots: manifest strong identity unavailable")
	}
	current, err := LoadProductionManifest(p.manager.dir)
	if err != nil {
		return err
	}
	active := make(map[string]SegmentRef, len(current.Segments))
	for _, ref := range current.Segments {
		active[ref.Path] = ref
	}
	for ref := range p.facts {
		if active[ref.Path] != ref {
			return errors.New("snapshots: commitment base family no longer active")
		}
	}
	dirs := make(map[string]bool)
	for ref := range p.facts {
		if err := contextError(ctx); err != nil {
			return err
		}
		path := filepath.Join(root, ref.Path)
		if err := syncFile(path); err != nil {
			return err
		}
		for parent := filepath.Dir(path); parent != root; parent = filepath.Dir(parent) {
			if parent == filepath.Dir(parent) {
				return errors.New("snapshots: commitment path escapes root")
			}
			if !dirs[parent] {
				if err := syncDir(parent); err != nil {
					return err
				}
				dirs[parent] = true
			}
		}
	}
	if err := syncFile(filepath.Join(root, ManifestFile)); err != nil {
		return err
	}
	if err := syncDir(root); err != nil {
		return err
	}
	after, err := historyStagingFileFingerprint(root, manifestRef)
	if err != nil || !after.unchanged(before) {
		return errors.New("snapshots: manifest changed during durability barrier")
	}
	if err := p.recheckFiles(p.rotation); err != nil {
		return err
	}
	if err := contextError(ctx); err != nil {
		return err
	}
	p.manifestState, p.durable = after, true
	return nil
}

func (p *VerifiedCommitmentBranchBase) Rotation() rawdb.CommitmentBranchRotation {
	if p == nil {
		return rawdb.CommitmentBranchRotation{}
	}
	return p.rotation
}

// This barrier authorizes deletion of the only hot fallback. Unlike the generic
// FUSE-compatible directory helper, unsupported fsync must remain an error.
func strictCommitmentSnapshotDirSync(path string) error {
	file, err := os.Open(path)
	if err != nil {
		return err
	}
	return errors.Join(file.Sync(), file.Close())
}
