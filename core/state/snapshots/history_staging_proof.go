package snapshots

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"

	"github.com/tronprotocol/go-tron/core/rawdb"
)

// HistoryStagingColdProver amortizes full immutable state-history-trio
// authentication across adjacent buckets in one pinned manifest. It does not
// grant a legacy manifest chain identity: callers must separately establish
// canonical block hashes and tx ranges for each bucket before admitting its
// semantic proof. Construct a fresh prover after a manifest change; a
// successful cached audit is never reused across epochs.
type HistoryStagingColdProver struct {
	dir      string
	manifest *Manifest
	refs     []SegmentRef
	trios    map[string]historyStagingTrio
	verified map[[32]byte][3]historyStagingFileState
	open     *historyStagingOpenTrio
	reuse    bool
	stats    HistoryStagingProverStats
	// testAuth intercepts only the expensive full-file audit in focused
	// concurrency tests; production provers always use the real verifier.
	testAuth func(context.Context, [3]SegmentRef) error
	testWait func()
}

// HistoryStagingProverStats counts real opens and full trio audits in this
// worker. Counts describe work, not cache correctness or user-visible hits.
type HistoryStagingProverStats struct {
	HistoryOpens          uint64
	IndexOpens            uint64
	ReaderReuses          uint64
	FullTrioAuthenticates uint64
}

// EnableReaderReuse opts into retaining at most one history/index pair across
// adjacent Build calls. The caller must Close the prover when its job ends.
func (p *HistoryStagingColdProver) EnableReaderReuse() {
	if p != nil {
		p.reuse = true
	}
}

func (p *HistoryStagingColdProver) Stats() HistoryStagingProverStats {
	if p == nil {
		return HistoryStagingProverStats{}
	}
	return p.stats
}

// A prover is worker-local. Keep only one history/index pair open so adjacent
// buckets share reference metadata and decoded chunks without multiplying the
// cache by the number of cold trios in a large manifest.
type historyStagingOpenTrio struct {
	id          [32]byte
	refs        [3]SegmentRef
	states      [3]historyStagingFileState
	history     historySegmentReader
	historySize uint64
	header      stateDomainChangeBinaryHeader
	index       historySegmentReader
	indexHeader stateDomainChangeBinaryHeader
}

func (p *HistoryStagingColdProver) Close() error {
	if p == nil || p.open == nil {
		return nil
	}
	open := p.open
	p.open = nil
	first := open.index.Close()
	second := open.history.Close()
	if first != nil {
		return first
	}
	return second
}

// HistoryStagingDir exposes the canonical cold directory of a pinned read
// manager. The caller must retain its history read lease while proving rows.
func (m *Manager) HistoryStagingDir() string {
	if m == nil {
		return ""
	}
	return m.dir
}

type historyStagingTrio struct {
	refs [3]SegmentRef
	id   [32]byte
}

type historyStagingFileState struct {
	info os.FileInfo
}

const historyStagingPinnedProofCacheEntries = 4096

// ErrHistoryStagingColdSemanticMismatch means a newly published cold trio is
// readable but its decoded history differs from an already certified receipt.
// Missing files, I/O failures, and cancellation remain ordinary retry errors.
var ErrHistoryStagingColdSemanticMismatch = errors.New("snapshots: cold history semantic mismatch")

type historyStagingPinnedProofCacheEntry struct {
	files map[[32]byte][3]historyStagingFileState
	used  uint64
}

var historyStagingPinnedProofCache = struct {
	sync.Mutex
	entries map[[32]byte]historyStagingPinnedProofCacheEntry
	trios   map[[32]byte][3]historyStagingFileState
	indexes map[historyStagingManifestIndexKey]map[[32]byte][3]SegmentRef
	flights map[[32]byte]chan struct{}
	clock   uint64
}{entries: make(map[[32]byte]historyStagingPinnedProofCacheEntry),
	trios:   make(map[[32]byte][3]historyStagingFileState),
	indexes: make(map[historyStagingManifestIndexKey]map[[32]byte][3]SegmentRef),
	flights: make(map[[32]byte]chan struct{})}

type historyStagingManifestIndexKey struct {
	dir        string
	generation uint64
	published  int64
	segments   int
}

func historyStagingManifestTrioIndex(dir string, manifest *Manifest) (map[[32]byte][3]SegmentRef, error) {
	if manifest == nil {
		return nil, errors.New("snapshots: missing staging manifest")
	}
	key := historyStagingManifestIndexKey{dir, manifest.Generation, manifest.PublishedUnix, len(manifest.Segments)}
	historyStagingPinnedProofCache.Lock()
	index := historyStagingPinnedProofCache.indexes[key]
	historyStagingPinnedProofCache.Unlock()
	if index != nil {
		return index, nil
	}
	prover, err := NewHistoryStagingColdProver(dir, manifest)
	if err != nil {
		return nil, err
	}
	index = make(map[[32]byte][3]SegmentRef, len(prover.refs))
	for _, ref := range prover.refs {
		trio, id, err := prover.trio(ref)
		if err != nil {
			return nil, err
		}
		index[id] = trio
	}
	historyStagingPinnedProofCache.Lock()
	if len(historyStagingPinnedProofCache.indexes) >= 8 {
		for old := range historyStagingPinnedProofCache.indexes {
			delete(historyStagingPinnedProofCache.indexes, old)
			break
		}
	}
	historyStagingPinnedProofCache.indexes[key] = index
	historyStagingPinnedProofCache.Unlock()
	return index, nil
}

// VerifyHistoryStagingPinnedBinding authenticates a route's current cold
// binding. A pinned manifest plus immutable trio fingerprint permits cache
// hits without repeating 1024 hot tx-range Gets or large checksum scans.
// The caller supplies canonical blocks only on a cache miss. Cache entries
// cannot outlive a new route/manifest epoch or a replaced immutable file.
func (m *Manager) VerifyHistoryStagingPinnedBinding(ctx context.Context, binding rawdb.HistoryStagingColdBinding, loadBlocks func() ([]rawdb.HistoryStagingBlockProof, error)) error {
	if m == nil || !m.pinned || ctx == nil || loadBlocks == nil || len(binding.Spans) == 0 {
		return errors.New("snapshots: pinned cold binding proof requires a pinned manager and block loader")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	manifest, err := m.currentManifest()
	if err != nil || manifest == nil || binding.ManifestEpoch == 0 || manifest.Generation < binding.ManifestEpoch {
		return errors.New("snapshots: cold binding manifest epoch differs from pinned view")
	}
	prover := &HistoryStagingColdProver{dir: m.dir, manifest: manifest}
	files, err := prover.bindingFileStates(binding)
	if err != nil {
		return err
	}
	encoded, err := json.Marshal(binding)
	if err != nil {
		return err
	}
	key := sha256.Sum256(append(append([]byte("gtron-history-staging-pinned-binding-v1\x00"), m.dir...), encoded...))
	historyStagingPinnedProofCache.Lock()
	entry, hit := historyStagingPinnedProofCache.entries[key]
	if hit && historyStagingSamePinnedFiles(entry.files, files) {
		historyStagingPinnedProofCache.clock++
		entry.used = historyStagingPinnedProofCache.clock
		historyStagingPinnedProofCache.entries[key] = entry
		historyStagingPinnedProofCache.Unlock()
		return nil
	}
	historyStagingPinnedProofCache.Unlock()
	blocks, err := loadBlocks()
	if err != nil {
		return err
	}
	if err := VerifyHistoryStagingColdBinding(ctx, m.dir, manifest, binding, blocks); err != nil {
		return err
	}
	after, err := prover.bindingFileStates(binding)
	if err != nil || !historyStagingSamePinnedFiles(files, after) {
		return errors.New("snapshots: cold trio changed during pinned binding proof")
	}
	historyStagingPinnedProofCache.Lock()
	if len(historyStagingPinnedProofCache.entries) >= historyStagingPinnedProofCacheEntries {
		var oldest [32]byte
		oldestUsed := ^uint64(0)
		for candidate, value := range historyStagingPinnedProofCache.entries {
			if value.used < oldestUsed {
				oldest, oldestUsed = candidate, value.used
			}
		}
		delete(historyStagingPinnedProofCache.entries, oldest)
	}
	historyStagingPinnedProofCache.clock++
	historyStagingPinnedProofCache.entries[key] = historyStagingPinnedProofCacheEntry{files: after, used: historyStagingPinnedProofCache.clock}
	historyStagingPinnedProofCache.Unlock()
	return nil
}

// VerifyHistoryStagingPinnedBindingReceipt is the bounded startup audit for a
// previously durable cold certification. It validates route-bound metadata
// and authenticates each immutable trio's real checksums once across buckets;
// the persisted semantic receipt was already proven during offline adoption
// or rebind. RPC reads can still perform the full per-range semantic proof on
// their first cache miss using VerifyHistoryStagingPinnedBinding.
func (m *Manager) VerifyHistoryStagingPinnedBindingReceipt(ctx context.Context, binding rawdb.HistoryStagingColdBinding) error {
	if m == nil || !m.pinned || ctx == nil || binding.Version != rawdb.HistoryStagingFormatVersion ||
		binding.Epoch == 0 || binding.BindingEpoch == 0 || len(binding.Spans) == 0 {
		return errors.New("snapshots: invalid pinned cold binding receipt")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	first, last, err := rawdb.StateHistoryChunkBucketBounds(binding.Bucket)
	if err != nil {
		return err
	}
	previous := uint64(0)
	for i, span := range binding.Spans {
		if span.From < first || span.To > last || span.To < span.From ||
			(i > 0 && span.From <= previous) || span.ContentID == ([32]byte{}) ||
			span.SemanticHash == ([32]byte{}) || span.TxRangeDigest == ([32]byte{}) {
			return errors.New("snapshots: cold binding receipt has invalid coverage")
		}
		previous = span.To
	}
	manifest, err := m.currentManifest()
	if err != nil || manifest == nil || binding.ManifestEpoch == 0 || manifest.Generation < binding.ManifestEpoch {
		return errors.New("snapshots: pinned cold manifest differs from receipt")
	}
	prover := &HistoryStagingColdProver{dir: m.dir, manifest: manifest, verified: make(map[[32]byte][3]historyStagingFileState)}
	if _, err := prover.bindingFileStates(binding); err != nil {
		return err
	}
	index, err := historyStagingManifestTrioIndex(m.dir, manifest)
	if err != nil {
		return err
	}
	seen := make(map[[32]byte]struct{})
	for _, span := range binding.Spans {
		id := span.ContentID
		if _, present := seen[id]; present {
			continue
		}
		seen[id] = struct{}{}
		trio := index[id]
		if err := prover.authenticate(ctx, trio, id); err != nil {
			return err
		}
	}
	return nil
}

func historyStagingSamePinnedFiles(before, after map[[32]byte][3]historyStagingFileState) bool {
	if len(before) != len(after) {
		return false
	}
	for id, first := range before {
		second, ok := after[id]
		if !ok {
			return false
		}
		for i := range first {
			if !first[i].same(second[i]) {
				return false
			}
		}
	}
	return true
}

func (p *HistoryStagingColdProver) bindingFileStates(binding rawdb.HistoryStagingColdBinding) (map[[32]byte][3]historyStagingFileState, error) {
	if p == nil || len(binding.Spans) == 0 {
		return nil, errors.New("snapshots: missing cold binding spans")
	}
	refsByID, err := historyStagingManifestTrioIndex(p.dir, p.manifest)
	if err != nil {
		return nil, err
	}
	files := make(map[[32]byte][3]historyStagingFileState)
	for _, span := range binding.Spans {
		if span.ContentID == ([32]byte{}) || span.SemanticHash == ([32]byte{}) || span.TxRangeDigest == ([32]byte{}) || span.To < span.From {
			return nil, errors.New("snapshots: malformed cold binding span")
		}
		if _, seen := files[span.ContentID]; seen {
			continue
		}
		trio, present := refsByID[span.ContentID]
		if !present {
			return nil, errors.New("snapshots: cold binding trio is absent from pinned manifest")
		}
		var states [3]historyStagingFileState
		for i, ref := range trio {
			state, err := historyStagingFileFingerprint(p.dir, ref)
			if err != nil {
				return nil, err
			}
			states[i] = state
		}
		files[span.ContentID] = states
	}
	return files, nil
}

func (s historyStagingFileState) same(other historyStagingFileState) bool {
	return s.info != nil && other.info != nil && os.SameFile(s.info, other.info) &&
		s.info.Size() == other.info.Size() &&
		s.info.ModTime() == other.info.ModTime() &&
		s.info.Mode() == other.info.Mode()
}

const historyStagingProofMaxRecords = 4_000_000

func NewHistoryStagingColdProver(dir string, manifest *Manifest) (*HistoryStagingColdProver, error) {
	if dir == "" || manifest == nil {
		return nil, errors.New("snapshots: history staging requires a production manifest")
	}
	if err := manifest.ValidateProduction(); err != nil {
		return nil, err
	}
	refs := make([]SegmentRef, 0)
	for _, ref := range manifest.Segments {
		if ref.NormalizedDataset() == SegmentDatasetStateDomainChange && ref.Kind == SegmentHistory {
			refs = append(refs, ref)
		}
	}
	sort.Slice(refs, func(i, j int) bool {
		if refs[i].FromTxNum != refs[j].FromTxNum {
			return refs[i].FromTxNum < refs[j].FromTxNum
		}
		return refs[i].ToTxNum < refs[j].ToTxNum
	})
	for i := 1; i < len(refs); i++ {
		if refs[i-1].ToTxNum >= refs[i].FromTxNum {
			return nil, fmt.Errorf("snapshots: overlapping active history trios %s and %s",
				refs[i-1].Path, refs[i].Path)
		}
	}
	return &HistoryStagingColdProver{dir: dir, manifest: manifest, refs: refs,
		trios:    make(map[string]historyStagingTrio),
		verified: make(map[[32]byte][3]historyStagingFileState)}, nil
}

// BuildHistoryStagingColdSpans authenticates exactly the missing hot blocks.
// Reusing a prover for consecutive buckets avoids rescanning the same large
// immutable trio's checksums/semantic companion coverage on every bucket.
func BuildHistoryStagingColdSpans(ctx context.Context, dir string, manifest *Manifest, blocks []rawdb.HistoryStagingBlockProof, needed []bool) ([]rawdb.HistoryStagingColdSpan, error) {
	prover, err := NewHistoryStagingColdProver(dir, manifest)
	if err != nil {
		return nil, err
	}
	defer prover.Close()
	return prover.Build(ctx, blocks, needed)
}

func historyStagingChecksumBytes(checksum string) ([32]byte, error) {
	var result [32]byte
	if !strings.HasPrefix(checksum, "sha256:") || len(checksum) != 71 {
		return result, fmt.Errorf("snapshots: history staging requires SHA256 checksum %q", checksum)
	}
	decoded, err := hex.DecodeString(checksum[7:])
	if err != nil {
		return result, err
	}
	copy(result[:], decoded)
	return result, nil
}

func historyStagingTrioID(refs [3]SegmentRef) ([32]byte, error) {
	h := sha256.New()
	h.Write([]byte("gtron-history-staging-trio-v1\x00"))
	var number [8]byte
	for _, ref := range refs {
		checksum, err := historyStagingChecksumBytes(ref.Checksum)
		if err != nil {
			return [32]byte{}, err
		}
		binary.BigEndian.PutUint64(number[:], ref.FromTxNum)
		h.Write(number[:])
		binary.BigEndian.PutUint64(number[:], ref.ToTxNum)
		h.Write(number[:])
		h.Write([]byte(ref.Kind))
		h.Write([]byte{0})
		h.Write(checksum[:])
	}
	var id [32]byte
	copy(id[:], h.Sum(nil))
	return id, nil
}

func (p *HistoryStagingColdProver) trio(ref SegmentRef) ([3]SegmentRef, [32]byte, error) {
	var refs [3]SegmentRef
	if cached, ok := p.trios[ref.Path]; ok {
		return cached.refs, cached.id, nil
	}
	if ref.NormalizedDataset() != SegmentDatasetStateDomainChange || ref.Kind != SegmentHistory {
		return refs, [32]byte{}, errors.New("snapshots: non-history segment in staging proof")
	}
	cfg, ok := DefaultDomainRegistry().ConfigForRef(ref)
	if !ok || !cfg.IsHistoryBinarySegmentPath(ref.Path) {
		return refs, [32]byte{}, errors.New("snapshots: history staging requires registered binary cold trio")
	}
	idx, ok := cfg.HistoryIndexRef(p.manifest, ref)
	if !ok {
		return refs, [32]byte{}, fmt.Errorf("snapshots: history staging missing index for %s", ref.Path)
	}
	accessor, ok := cfg.HistoryAccessorRef(p.manifest, ref)
	if !ok {
		return refs, [32]byte{}, fmt.Errorf("snapshots: history staging missing accessor for %s", ref.Path)
	}
	refs = [3]SegmentRef{ref, idx, accessor}
	id, err := historyStagingTrioID(refs)
	if err == nil {
		p.trios[ref.Path] = historyStagingTrio{refs: refs, id: id}
	}
	return refs, id, err
}

func historyStagingFileFingerprint(dir string, ref SegmentRef) (historyStagingFileState, error) {
	path := filepath.Join(dir, ref.Path)
	info, err := os.Lstat(path)
	if err != nil {
		return historyStagingFileState{}, err
	}
	if !info.Mode().IsRegular() || info.Size() < 0 || uint64(info.Size()) != ref.Size {
		return historyStagingFileState{}, fmt.Errorf("snapshots: cold trio file identity changed: %s", path)
	}
	return historyStagingFileState{info: info}, nil
}

func historyStagingCheckFileStates(dir string, refs [3]SegmentRef, states [3]historyStagingFileState) error {
	for i, ref := range refs {
		after, err := historyStagingFileFingerprint(dir, ref)
		if err != nil || !after.same(states[i]) {
			return fmt.Errorf("snapshots: cold trio changed during proof: %s", ref.Path)
		}
	}
	return nil
}

func (p *HistoryStagingColdProver) authenticate(ctx context.Context, refs [3]SegmentRef, id [32]byte) error {
	var states [3]historyStagingFileState
	for i, ref := range refs {
		state, err := historyStagingFileFingerprint(p.dir, ref)
		if err != nil {
			return err
		}
		states[i] = state
	}
	if cached, ok := p.verified[id]; ok {
		match := true
		for i := range states {
			match = match && cached[i].same(states[i])
		}
		if match {
			return nil
		}
	}
	globalKey := sha256.Sum256(append(append([]byte("gtron-history-staging-trio-auth-v1\x00"), p.dir...), id[:]...))
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		historyStagingPinnedProofCache.Lock()
		shared, hit := historyStagingPinnedProofCache.trios[globalKey]
		if hit {
			match := true
			for i := range states {
				match = match && shared[i].same(states[i])
			}
			if match {
				historyStagingPinnedProofCache.Unlock()
				p.verified[id] = states
				return nil
			}
		}
		if flight := historyStagingPinnedProofCache.flights[globalKey]; flight != nil {
			historyStagingPinnedProofCache.Unlock()
			if p.testWait != nil {
				p.testWait()
			}
			select {
			case <-flight:
				// A cancelled or failed owner never installs a success; retry with
				// this waiter's own context and file fingerprint.
				if err := historyStagingCheckFileStates(p.dir, refs, states); err != nil {
					return err
				}
				continue
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		if len(historyStagingPinnedProofCache.flights) >= 8 {
			var flight chan struct{}
			for _, pending := range historyStagingPinnedProofCache.flights {
				flight = pending
				break
			}
			historyStagingPinnedProofCache.Unlock()
			select {
			case <-flight:
				if err := historyStagingCheckFileStates(p.dir, refs, states); err != nil {
					return err
				}
				continue
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		flight := make(chan struct{})
		historyStagingPinnedProofCache.flights[globalKey] = flight
		historyStagingPinnedProofCache.Unlock()

		p.stats.FullTrioAuthenticates++
		var err error
		if p.testAuth != nil {
			err = p.testAuth(ctx, refs)
		} else {
			err = VerifyHistorySegmentWithCompanionsContext(ctx, p.dir, p.manifest, refs[0])
		}
		if err == nil {
			for i, ref := range refs {
				after, fingerprintErr := historyStagingFileFingerprint(p.dir, ref)
				if fingerprintErr != nil || !after.same(states[i]) {
					err = fmt.Errorf("snapshots: cold trio changed during authentication: %s", ref.Path)
					break
				}
			}
		}
		historyStagingPinnedProofCache.Lock()
		if err == nil {
			if len(historyStagingPinnedProofCache.trios) >= historyStagingPinnedProofCacheEntries {
				for key := range historyStagingPinnedProofCache.trios {
					delete(historyStagingPinnedProofCache.trios, key)
					break
				}
			}
			historyStagingPinnedProofCache.trios[globalKey] = states
			p.verified[id] = states
		}
		delete(historyStagingPinnedProofCache.flights, globalKey)
		close(flight)
		historyStagingPinnedProofCache.Unlock()
		return err
	}
}

func historyStagingRangeDigest(blocks []rawdb.HistoryStagingBlockProof) [32]byte {
	h := sha256.New()
	h.Write([]byte("gtron-history-staging-txrange-v1\x00"))
	var number [8]byte
	for _, block := range blocks {
		for _, value := range []uint64{block.Number, block.BeginTxNum, block.EndTxNum} {
			binary.BigEndian.PutUint64(number[:], value)
			h.Write(number[:])
		}
		h.Write(block.Hash[:])
	}
	var digest [32]byte
	copy(digest[:], h.Sum(nil))
	return digest
}

func historyStagingRecordDigest(change *rawdb.StateDomainChange, ordinal uint64) [32]byte {
	digest, _ := historyStagingRecordDigestStream(change, ordinal, uint64(len(change.Prev)), func(dst io.Writer) error {
		_, err := dst.Write(change.Prev)
		return err
	})
	return digest
}

func historyStagingRecordDigestStream(change *rawdb.StateDomainChange, ordinal, prevLength uint64, writePrev func(io.Writer) error) ([32]byte, error) {
	h := sha256.New()
	h.Write([]byte("gtron-history-staging-change-v2\x00"))
	var number [8]byte
	// Seq is stored in hot packs but may be reconstructed with a different
	// numeric value by cold v5/merge. Its order within one TxNum is observable
	// by rollback readers, so bind the stable ordinal rather than raw Seq.
	for _, value := range []uint64{change.BlockNum, change.TxNum, ordinal, uint64(change.FlatDomain), change.Generation, uint64(change.Domain)} {
		binary.BigEndian.PutUint64(number[:], value)
		h.Write(number[:])
	}
	h.Write(change.BlockHash[:])
	h.Write(change.Owner[:])
	binary.BigEndian.PutUint64(number[:], uint64(len(change.Key)))
	h.Write(number[:])
	h.Write(change.Key)
	binary.BigEndian.PutUint64(number[:], prevLength)
	h.Write(number[:])
	if err := writePrev(h); err != nil {
		return [32]byte{}, err
	}
	if change.PrevExists {
		h.Write([]byte{1})
	} else {
		h.Write([]byte{0})
	}
	var digest [32]byte
	copy(digest[:], h.Sum(nil))
	return digest, nil
}

func (p *HistoryStagingColdProver) Build(ctx context.Context, blocks []rawdb.HistoryStagingBlockProof, needed []bool) ([]rawdb.HistoryStagingColdSpan, error) {
	if p == nil || ctx == nil || len(blocks) != int(rawdb.StateHistoryChunkBucketBlocks) || len(needed) != len(blocks) {
		return nil, errors.New("snapshots: staging proof needs one full bucket and matching cold mask")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	first, last, err := rawdb.StateHistoryChunkBucketBounds(blocks[0].Number / rawdb.StateHistoryChunkBucketBlocks)
	if err != nil || first < rawdb.StateHistoryChunkBucketBlocks ||
		first != blocks[0].Number || last != blocks[len(blocks)-1].Number {
		return nil, errors.New("snapshots: cold proof must cover one complete non-genesis bucket")
	}
	for i, block := range blocks {
		if block.Hash == ([32]byte{}) || block.EndTxNum < block.BeginTxNum ||
			(i > 0 && (block.Number != blocks[i-1].Number+1 || block.BeginTxNum != blocks[i-1].EndTxNum+1)) {
			return nil, fmt.Errorf("snapshots: invalid staging canonical range at offset %d", i)
		}
	}
	refs := p.refs
	owners := make([]int, len(blocks))
	for i := range owners {
		owners[i] = -1
		if !needed[i] {
			continue
		}
		block := blocks[i]
		if block.BeginTxNum < p.manifest.VisibleTxStart || block.EndTxNum > p.manifest.VisibleTxEnd {
			return nil, fmt.Errorf("snapshots: block %d outside visible cold range", block.Number)
		}
		// The manifest can contain tens of thousands of segments. One binary
		// search per missing block avoids scanning the full catalog per bucket.
		pastCandidate := sort.Search(len(refs), func(j int) bool {
			return refs[j].FromTxNum > block.BeginTxNum
		})
		if pastCandidate > 0 {
			ref := refs[pastCandidate-1]
			if block.BeginTxNum >= ref.FromTxNum && block.EndTxNum <= ref.ToTxNum {
				owners[i] = pastCandidate - 1
			}
		}
		if owners[i] == -1 {
			return nil, fmt.Errorf("snapshots: no complete cold trio for block %d", blocks[i].Number)
		}
	}
	var spans []rawdb.HistoryStagingColdSpan
	for start := 0; start < len(blocks); {
		if owners[start] == -1 {
			start++
			continue
		}
		end := start + 1
		for end < len(blocks) && owners[end] == owners[start] && needed[end] {
			end++
		}
		ref := refs[owners[start]]
		trio, id, err := p.trio(ref)
		if err != nil {
			return nil, err
		}
		if err := p.authenticate(ctx, trio, id); err != nil {
			return nil, err
		}
		span, err := p.buildSpan(ctx, ref, id, blocks[start:end])
		if err != nil {
			return nil, err
		}
		spans = append(spans, span)
		start = end
	}
	return spans, nil
}

func (p *HistoryStagingColdProver) openSpanTrio(ctx context.Context, ref SegmentRef, id [32]byte) (*historyStagingOpenTrio, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	refs, actualID, err := p.trio(ref)
	if err != nil || actualID != id {
		return nil, errors.New("snapshots: cold proof trio identity changed")
	}
	if p.open != nil {
		if p.open.id == id {
			if err := historyStagingCheckFileStates(p.dir, refs, p.open.states); err != nil {
				_ = p.Close()
				return nil, err
			}
			p.stats.ReaderReuses++
			return p.open, nil
		}
		if err := p.Close(); err != nil {
			return nil, err
		}
	}
	var states [3]historyStagingFileState
	for i, item := range refs {
		states[i], err = historyStagingFileFingerprint(p.dir, item)
		if err != nil {
			return nil, err
		}
	}
	verified, ok := p.verified[id]
	if !ok {
		return nil, errors.New("snapshots: cold proof trio was not authenticated")
	}
	for i := range states {
		if !states[i].same(verified[i]) {
			return nil, errors.New("snapshots: cold proof trio changed after authentication")
		}
	}
	history, size, header, err := openHistorySegmentForReadWithCacheLimit(p.dir, ref, 4)
	if err != nil {
		return nil, err
	}
	p.stats.HistoryOpens++
	index, indexHeader, err := openStateDomainChangeBinaryIndexReader(p.dir, refs[1])
	if err != nil {
		_ = history.Close()
		return nil, err
	}
	p.stats.IndexOpens++
	if indexHeader.fromTxNum != ref.FromTxNum || indexHeader.toTxNum != ref.ToTxNum {
		_ = index.Close()
		_ = history.Close()
		return nil, errors.New("snapshots: cold proof history/index range mismatch")
	}
	if err := historyStagingCheckFileStates(p.dir, refs, states); err != nil {
		_ = index.Close()
		_ = history.Close()
		return nil, err
	}
	p.open = &historyStagingOpenTrio{id: id, refs: refs, states: states,
		history: history, historySize: size, header: header,
		index: index, indexHeader: indexHeader}
	return p.open, nil
}

func (p *HistoryStagingColdProver) collectSpanRecords(ctx context.Context, ref SegmentRef, id [32]byte, blocks []rawdb.HistoryStagingBlockProof) ([][32]byte, error) {
	open, err := p.openSpanTrio(ctx, ref, id)
	if err != nil {
		return nil, err
	}
	if !p.reuse {
		defer p.Close()
	}
	history := contextReaderAt{ctx: ctx, r: open.history}
	index := contextReaderAt{ctx: ctx, r: open.index}
	for _, block := range blocks {
		cold, table, found, err := findStateDomainChangeBinaryTxRangeForBlock(history,
			open.historySize, ref, open.header, block.Number)
		want := rawdb.StateTxRange{BlockNum: block.Number, BlockHash: block.Hash,
			BeginTxNum: block.BeginTxNum, EndTxNum: block.EndTxNum}
		if err != nil {
			return nil, err
		}
		if !table || !found || cold == nil || *cold != want ||
			block.BeginTxNum < ref.FromTxNum || block.EndTxNum > ref.ToTxNum {
			return nil, fmt.Errorf("snapshots: offline cold block range mismatch at block %d", block.Number)
		}
	}
	var records [][32]byte
	var previousTx, ordinal uint64
	var havePrevious bool
	first, last := blocks[0], blocks[len(blocks)-1]
	start, found, err := stateDomainChangeBinaryIndexLowerBound(index, open.indexHeader.count, first.BeginTxNum)
	if err != nil {
		return nil, err
	}
	if found {
		for i := start; i < open.indexHeader.count; i++ {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			entry, err := readStateDomainChangeBinaryIndexEntryAt(index, i)
			if err != nil {
				return nil, err
			}
			if entry.txNum > last.EndTxNum {
				break
			}
			offset := entry.offset
			for j := uint64(0); j < entry.count; j++ {
				change, next, err := readStateDomainChangeBinaryRecordAtBoundedIndex(history,
					offset, open.historySize, entry.recordIndex+j)
				if err != nil {
					return nil, err
				}
				if change == nil {
					return nil, errors.New("snapshots: nil cold proof record")
				}
				if change.TxNum != entry.txNum {
					return nil, errors.New("snapshots: cold proof index/record tx mismatch")
				}
				if change.TxNum >= first.BeginTxNum {
					if err := ctx.Err(); err != nil {
						return nil, err
					}
					if change == nil || change.BlockNum < first.Number || change.BlockNum > last.Number {
						return nil, errors.New("snapshots: cold record outside certified block span")
					}
					block := blocks[change.BlockNum-first.Number]
					if change.BlockHash != block.Hash || change.TxNum < block.BeginTxNum || change.TxNum > block.EndTxNum ||
						len(records) >= historyStagingProofMaxRecords {
						return nil, errors.New("snapshots: cold record mismatch or staging proof row limit exceeded")
					}
					if havePrevious && change.TxNum < previousTx {
						return nil, errors.New("snapshots: cold record transaction order regressed")
					}
					if !havePrevious || change.TxNum != previousTx {
						ordinal = 0
					} else if ordinal == ^uint64(0) {
						return nil, errors.New("snapshots: cold record ordinal overflow")
					} else {
						ordinal++
					}
					previousTx, havePrevious = change.TxNum, true
					records = append(records, historyStagingRecordDigest(change, ordinal))
				}
				offset = next
			}
		}
	}
	if err := historyStagingCheckFileStates(p.dir, open.refs, open.states); err != nil {
		_ = p.Close()
		return nil, err
	}
	return records, nil
}

func historyStagingSemanticDigest(blocks []rawdb.HistoryStagingBlockProof, records [][32]byte) ([32]byte, [32]byte) {
	sort.Slice(records, func(i, j int) bool { return bytes.Compare(records[i][:], records[j][:]) < 0 })
	h := sha256.New()
	h.Write([]byte("gtron-history-staging-semantic-v1\x00"))
	rangeDigest := historyStagingRangeDigest(blocks)
	h.Write(rangeDigest[:])
	var count [8]byte
	binary.BigEndian.PutUint64(count[:], uint64(len(records)))
	h.Write(count[:])
	for _, record := range records {
		h.Write(record[:])
	}
	var semantic [32]byte
	copy(semantic[:], h.Sum(nil))
	return rangeDigest, semantic
}

func (p *HistoryStagingColdProver) buildSpan(ctx context.Context, ref SegmentRef, id [32]byte, blocks []rawdb.HistoryStagingBlockProof) (rawdb.HistoryStagingColdSpan, error) {
	records, err := p.collectSpanRecords(ctx, ref, id, blocks)
	if err != nil {
		return rawdb.HistoryStagingColdSpan{}, err
	}
	rangeDigest, semantic := historyStagingSemanticDigest(blocks, records)
	first, last := blocks[0], blocks[len(blocks)-1]
	span := rawdb.HistoryStagingColdSpan{From: first.Number, To: last.Number,
		ContentID: id, TxRangeDigest: rangeDigest, RowCount: uint64(len(records))}
	span.SemanticHash = semantic
	return span, nil
}

// VerifyHistoryStagingTargetColdEquivalence proves that every logical record
// in one pinned source bucket (TARGET or reset-replayed SOURCE) is identical to
// authenticated cold history. The cold proof supplies exact canonical ranges,
// including blocks with zero changes. This is required before retiring a
// target's physical payload after cold publication; a matching height alone
// cannot authorize deletion.
func VerifyHistoryStagingTargetColdEquivalence(ctx context.Context, source rawdb.StateHistoryReadView, dir string, manifest *Manifest, blocks []rawdb.HistoryStagingBlockProof) ([]rawdb.HistoryStagingColdSpan, error) {
	needed := make([]bool, len(blocks))
	for i := range needed {
		needed[i] = true
	}
	return verifyHistoryStagingSourceColdMask(ctx, source, dir, manifest, blocks, needed)
}

// VerifyHistoryStagingMixedTargetColdEquivalence extends a certified partial
// cold binding to a complete bucket. Old cold subranges are compared against
// their durable semantic receipts, while only the remaining target-owned
// blocks are compared to the pinned target's actual logical rows. Treating an
// already-pruned target prefix as a zero-change block would be unsafe.
func VerifyHistoryStagingMixedTargetColdEquivalence(ctx context.Context, target rawdb.StateHistoryReadView, dir string, manifest *Manifest, blocks []rawdb.HistoryStagingBlockProof, old rawdb.HistoryStagingColdBinding) ([]rawdb.HistoryStagingColdSpan, error) {
	if len(blocks) != int(rawdb.StateHistoryChunkBucketBlocks) || len(old.Spans) == 0 || old.Bucket != blocks[0].Number/rawdb.StateHistoryChunkBucketBlocks {
		return nil, errors.New("snapshots: invalid mixed target cold binding")
	}
	if _, err := RebindHistoryStagingColdBinding(ctx, dir, manifest, old, blocks); err != nil {
		return nil, err
	}
	remaining := make([]bool, len(blocks))
	for i := range remaining {
		remaining[i] = true
	}
	for _, span := range old.Spans {
		for i := int(span.From - blocks[0].Number); i <= int(span.To-blocks[0].Number); i++ {
			remaining[i] = false
		}
	}
	if _, err := verifyHistoryStagingSourceColdMask(ctx, target, dir, manifest, blocks, remaining); err != nil {
		return nil, err
	}
	full := make([]bool, len(blocks))
	for i := range full {
		full[i] = true
	}
	return BuildHistoryStagingColdSpans(ctx, dir, manifest, blocks, full)
}

func verifyHistoryStagingSourceColdMask(ctx context.Context, source rawdb.StateHistoryReadView, dir string, manifest *Manifest, blocks []rawdb.HistoryStagingBlockProof, needed []bool) ([]rawdb.HistoryStagingColdSpan, error) {
	if source == nil || !source.IsPinnedKeyValueView() || ctx == nil {
		return nil, errors.New("snapshots: target/cold equivalence requires a pinned source")
	}
	spans, err := BuildHistoryStagingColdSpans(ctx, dir, manifest, blocks, needed)
	if err != nil {
		return nil, err
	}
	if len(spans) == 0 {
		return spans, nil
	}
	records := make([][][32]byte, len(spans))
	previousTx := make([]uint64, len(spans))
	ordinals := make([]uint64, len(spans))
	havePrevious := make([]bool, len(spans))
	var rowCount int
	spanIndex := 0
	err = rawdb.IterateStateHistorySpanBlocks(ctx, source, blocks[0].Number, blocks[len(blocks)-1].Number,
		blocks[0].BeginTxNum, blocks[len(blocks)-1].EndTxNum, func(block *rawdb.StateHistorySpanBlock) (bool, error) {
			info := block.Info()
			if info.BlockNum < blocks[0].Number || info.BlockNum > blocks[len(blocks)-1].Number {
				return false, errors.New("snapshots: target span escaped certified bucket")
			}
			want := blocks[info.BlockNum-blocks[0].Number]
			if info.BlockHash != want.Hash || info.BeginTxNum != want.BeginTxNum || info.EndTxNum != want.EndTxNum {
				return false, fmt.Errorf("%w at block %d txrange", ErrHistoryStagingColdSemanticMismatch, info.BlockNum)
			}
			for spanIndex < len(spans) && info.BlockNum > spans[spanIndex].To {
				spanIndex++
			}
			if spanIndex >= len(spans) || info.BlockNum < spans[spanIndex].From {
				return true, nil // Already certified by an old cold binding.
			}
			_, err := block.IterateRows(func(row *rawdb.StateHistorySpanRow) (bool, error) {
				if row == nil || row.Change.BlockNum != info.BlockNum || row.Change.BlockHash != info.BlockHash ||
					row.Change.TxNum < info.BeginTxNum || row.Change.TxNum > info.EndTxNum {
					return false, fmt.Errorf("%w at block %d row", ErrHistoryStagingColdSemanticMismatch, info.BlockNum)
				}
				if rowCount >= historyStagingProofMaxRecords {
					return false, errors.New("snapshots: target/cold equivalence row budget exceeded")
				}
				if havePrevious[spanIndex] && row.Change.TxNum < previousTx[spanIndex] {
					return false, fmt.Errorf("%w at block %d transaction order", ErrHistoryStagingColdSemanticMismatch, info.BlockNum)
				}
				if !havePrevious[spanIndex] || row.Change.TxNum != previousTx[spanIndex] {
					ordinals[spanIndex] = 0
				} else if ordinals[spanIndex] == ^uint64(0) {
					return false, errors.New("snapshots: target/cold ordinal overflow")
				} else {
					ordinals[spanIndex]++
				}
				previousTx[spanIndex], havePrevious[spanIndex] = row.Change.TxNum, true
				digest, err := historyStagingRecordDigestStream(&row.Change, ordinals[spanIndex], row.PrevLength, func(dst io.Writer) error {
					return block.WritePrevTo(row, dst)
				})
				if err != nil {
					return false, err
				}
				records[spanIndex] = append(records[spanIndex], digest)
				rowCount++
				return true, nil
			})
			return true, err
		})
	if err != nil {
		return nil, err
	}
	for i, span := range spans {
		start, end := int(span.From-blocks[0].Number), int(span.To-blocks[0].Number)+1
		rangeDigest, semantic := historyStagingSemanticDigest(blocks[start:end], records[i])
		if rangeDigest != span.TxRangeDigest || semantic != span.SemanticHash || uint64(len(records[i])) != span.RowCount {
			return nil, fmt.Errorf("%w in bucket span [%d,%d]", ErrHistoryStagingColdSemanticMismatch, span.From, span.To)
		}
	}
	return spans, nil
}

// VerifyHistoryStagingColdBinding reauthenticates the current manifest's
// physical trio and semantic content without trusting its generation number.
func VerifyHistoryStagingColdBinding(ctx context.Context, dir string, manifest *Manifest, binding rawdb.HistoryStagingColdBinding, blocks []rawdb.HistoryStagingBlockProof) error {
	prover, err := NewHistoryStagingColdProver(dir, manifest)
	if err != nil {
		return err
	}
	defer prover.Close()
	return prover.VerifyBinding(ctx, binding, blocks)
}

// VerifyBinding keeps the complete per-span semantic check while allowing an
// offline worker to reuse its authenticated reader between adjacent buckets.
func (p *HistoryStagingColdProver) VerifyBinding(ctx context.Context, binding rawdb.HistoryStagingColdBinding, blocks []rawdb.HistoryStagingBlockProof) error {
	if len(blocks) != int(rawdb.StateHistoryChunkBucketBlocks) || len(binding.Spans) == 0 ||
		binding.Bucket != blocks[0].Number/rawdb.StateHistoryChunkBucketBlocks {
		return errors.New("snapshots: invalid cold binding proof coverage")
	}
	var previous uint64
	for i, span := range binding.Spans {
		if span.From < blocks[0].Number || span.To > blocks[len(blocks)-1].Number || span.To < span.From ||
			(i > 0 && span.From <= previous) {
			return fmt.Errorf("snapshots: cold binding span %d is outside its bucket or overlaps", i)
		}
		previous = span.To
		needed := make([]bool, len(blocks))
		for j := int(span.From - blocks[0].Number); j <= int(span.To-blocks[0].Number); j++ {
			needed[j] = true
		}
		got, err := p.Build(ctx, blocks, needed)
		if err != nil {
			return err
		}
		if len(got) != 1 || got[0] != span {
			return fmt.Errorf("snapshots: cold binding span %d semantic identity differs", i)
		}
	}
	return nil
}

// RebindHistoryStagingColdBinding verifies every old logical subrange against
// the current manifest and returns a candidate binding with current trio IDs.
// A replacement merge may turn two trios into one (or vice versa); identity
// and span count may change, but the old per-subrange semantic commitment may
// not. The caller still owns durable publication and old-file lease handling.
func RebindHistoryStagingColdBinding(ctx context.Context, dir string, manifest *Manifest, binding rawdb.HistoryStagingColdBinding, blocks []rawdb.HistoryStagingBlockProof) (rawdb.HistoryStagingColdBinding, error) {
	var zero rawdb.HistoryStagingColdBinding
	prover, err := NewHistoryStagingColdProver(dir, manifest)
	if err != nil {
		return zero, err
	}
	defer prover.Close()
	if len(blocks) != int(rawdb.StateHistoryChunkBucketBlocks) ||
		binding.BindingEpoch == ^uint64(0) || len(binding.Spans) == 0 {
		return zero, errors.New("snapshots: invalid old cold binding for rebind")
	}
	result := binding
	result.BindingEpoch++
	result.ManifestEpoch = manifest.Generation
	result.Spans = nil
	var previous uint64
	for i, old := range binding.Spans {
		if old.From > old.To || old.From < blocks[0].Number ||
			old.To > blocks[len(blocks)-1].Number ||
			(i > 0 && old.From <= previous) ||
			old.ContentID == ([32]byte{}) || old.SemanticHash == ([32]byte{}) ||
			old.TxRangeDigest == ([32]byte{}) {
			return zero, fmt.Errorf("snapshots: invalid old cold binding span %d", i)
		}
		previous = old.To
		start := int(old.From - blocks[0].Number)
		end := int(old.To-blocks[0].Number) + 1
		mask := make([]bool, len(blocks))
		for j := start; j < end; j++ {
			mask[j] = true
		}
		current, err := prover.Build(ctx, blocks, mask)
		if err != nil {
			return zero, err
		}
		var records [][32]byte
		for _, span := range current {
			a := int(span.From - blocks[0].Number)
			b := int(span.To-blocks[0].Number) + 1
			past := sort.Search(len(prover.refs), func(j int) bool {
				return prover.refs[j].FromTxNum > blocks[a].BeginTxNum
			})
			if past == 0 {
				return zero, errors.New("snapshots: rebound cold trio disappeared")
			}
			ref := prover.refs[past-1]
			_, id, err := prover.trio(ref)
			if err != nil || id != span.ContentID {
				return zero, errors.New("snapshots: rebound cold trio identity differs")
			}
			part, err := prover.collectSpanRecords(ctx, ref, id, blocks[a:b])
			if err != nil {
				return zero, err
			}
			if len(part) > historyStagingProofMaxRecords-len(records) {
				return zero, errors.New("snapshots: rebind semantic record budget exceeded")
			}
			records = append(records, part...)
		}
		txDigest, semantic := historyStagingSemanticDigest(blocks[start:end], records)
		if txDigest != old.TxRangeDigest || semantic != old.SemanticHash ||
			uint64(len(records)) != old.RowCount {
			return zero, fmt.Errorf("%w in [%d,%d]", ErrHistoryStagingColdSemanticMismatch, old.From, old.To)
		}
		result.Spans = append(result.Spans, current...)
	}
	return result, nil
}
