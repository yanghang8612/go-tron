package domains

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"hash"
	"sort"
	"sync"

	gethkeccak "github.com/ethereum/go-ethereum/crypto/keccak"
	"github.com/tronprotocol/go-tron/common"
)

// keccakPool reuses Legacy Keccak-256 hashers across fold passes. A
// single Nile-sync segment allocates ~16 GB of hashers via this constructor
// (1 per nodeHash/keyPath/leafValueHash call); the pool turns those into
// Reset-and-reuse and cuts that source of GC pressure to near zero. sync.Pool
// is safe across the parallel root's subtrie workers, and each borrow/return
// cycle remains strictly local to one hash invocation. go-ethereum's hasher is
// byte-identical to x/crypto's legacy implementation and provides an amd64
// Keccak-f assembly path, which is the production deployment architecture.
var keccakPool = sync.Pool{
	New: func() any {
		return &pooledKeccak{keccakState: gethkeccak.NewLegacyKeccak256().(keccakState)}
	},
}

// keccakState exposes the sponge's destructive Read fast-path. hash.Hash.Sum
// must preserve the absorb state, so the implementation clones its ~200-byte
// Keccak state on every call. These hashers are exclusively borrowed for one
// digest and Reset before reuse, letting Read write the digest directly into
// the caller's fixed buffer without that clone/allocation.
type keccakState interface {
	hash.Hash
	Read([]byte) (int, error)
}

// pooledKeccak keeps the tiny Write inputs in the same heap object as the
// pooled sponge. Local [1]byte/[8]byte arrays passed through hash.Hash.Write's
// interface escape on the fold hot path; reusing these fields removes those
// per-domain-byte, per-nibble and per-length objects.
type pooledKeccak struct {
	keccakState
	byteBuf [1]byte
	lenBuf  [8]byte
	// digestBuf is the target of the interface-dispatched Read call. Passing a
	// function-local [32]byte through that interface makes it escape; reading
	// into pooled storage and copying into the return value keeps hash results
	// allocation-free.
	digestBuf [common.HashLength]byte
	// nodeBuf holds the largest possible branch-hash preimage:
	// domain byte + 16 * (nibble byte + 32-byte child hash). Keeping it on the
	// pooled object avoids a per-node escape while letting nodeHash absorb the
	// whole preimage in one Write instead of up to 33 tiny Writes.
	nodeBuf [1 + 16*(1+common.HashLength)]byte
}

func borrowKeccak() *pooledKeccak {
	h := keccakPool.Get().(*pooledKeccak)
	h.Reset()
	return h
}

func returnKeccak(h *pooledKeccak) {
	keccakPool.Put(h)
}

func writeKeccakByte(h *pooledKeccak, b byte) {
	h.byteBuf[0] = b
	_, _ = h.Write(h.byteBuf[:])
}

func readKeccakHash(h *pooledKeccak) (out common.Hash) {
	_, _ = h.Read(h.digestBuf[:])
	copy(out[:], h.digestBuf[:])
	return out
}

// encodeBufPool reuses byte buffers for BranchData.Encode output during a fold.
// Each branch persisted via PutBranch grabs a buffer here, fills it via EncodeTo,
// hands it to the KV writer, then returns it. PutBranch holds the buffer for the
// entire writer call — pebble batches copy the value into their internal arena
// during Put, so reuse after that call is safe. The pool typically settles at
// the few largest branch sizes seen during a fold (root + per-segment hot
// branches), avoiding the ~29 GB/300s Encode-output allocation seen on Nile sync.
var encodeBufPool = sync.Pool{
	New: func() any { b := make([]byte, 0, 256); return &b },
}

func borrowEncodeBuf() *[]byte {
	bp := encodeBufPool.Get().(*[]byte)
	*bp = (*bp)[:0]
	return bp
}

func returnEncodeBuf(bp *[]byte) {
	encodeBufPool.Put(bp)
}

// branchPool reuses BranchData values during a fold descent. applyOnHash's
// `var child BranchData; &child` was the single largest allocation source on
// Nile sync (~246 GB / 300s, ~24% of all heap allocation): taking the address
// of a stack-local BranchData forces escape to the heap, and the fold makes
// one such call per hash-child descent on every block. The pool turns those
// per-descent allocations into a small reusable set.
//
// Safety: borrowed pointers are always local to one applyOnHash /
// insertIntoEmpty / applyOnLeaf call frame. linkChild consumes the data
// (PutBranch copies the value, DelBranch only uses the prefix) and never
// retains the pointer past return. Recursive descent borrows separate objects
// per level, and sync.Pool is safe across the parallel root's workers.
var branchPool = sync.Pool{
	New: func() any { return new(BranchData) },
}

func borrowBranch() *BranchData {
	b := branchPool.Get().(*BranchData)
	*b = BranchData{}
	return b
}

func returnBranch(b *BranchData) {
	if b == nil {
		return
	}
	branchPool.Put(b)
}

// opsBufPool reuses op slices for apply's bucket-sort scratch space. apply
// formerly used `var buckets [16][]op` + append per op, which heap-allocated up
// to 16 backing arrays per recursive call (the fold is recursive to depth 64).
// The replacement counting-sort writes into a single pooled scratch buffer per
// apply invocation, cutting per-descent slice churn.
var opsBufPool = sync.Pool{
	New: func() any { b := make([]op, 0, 64); return &b },
}

func borrowOpsBuf(size int) *[]op {
	bp := opsBufPool.Get().(*[]op)
	if cap(*bp) < size {
		*bp = make([]op, size)
	} else {
		*bp = (*bp)[:size]
	}
	return bp
}

func returnOpsBuf(bp *[]op) {
	*bp = (*bp)[:0]
	opsBufPool.Put(bp)
}

// childKind distinguishes the two child types stored in a BranchData node.
const (
	kindHash = uint8(0) // 32-byte intermediate hash
	kindLeaf = uint8(1) // plain key bytes + 32-byte value hash
)

// branchChild holds one present child entry of a hex-trie branch node.
type branchChild struct {
	present   bool
	kind      uint8
	valueHash common.Hash // child hash or leaf value hash, selected by kind
	leafKey   []byte      // valid when kind == kindLeaf
}

// BranchData represents a hex (16-way) trie branch node.  A branch has up to
// 16 children indexed by nibble 0–15.  Each present child is either an
// intermediate hash child or a leaf (key bytes + value hash).
//
// Children are stored in a fixed 16-slot array so insertion order never
// affects encoding — Encode always iterates nibbles low→high.
type BranchData struct {
	children [16]branchChild
}

// SetHashChild marks nibble as a hash child with the given 32-byte hash.
// Overwrites any previous child at that nibble.
func (b *BranchData) SetHashChild(nibble uint8, h common.Hash) {
	b.children[nibble] = branchChild{
		present:   true,
		kind:      kindHash,
		valueHash: h,
	}
}

// SetLeafChild marks nibble as a leaf child with the given key and value hash.
// Overwrites any previous child at that nibble.
func (b *BranchData) SetLeafChild(nibble uint8, key []byte, valHash common.Hash) {
	b.children[nibble] = branchChild{
		present:   true,
		kind:      kindLeaf,
		leafKey:   append([]byte(nil), key...),
		valueHash: valHash,
	}
}

// setLeafChildStable is the fold-internal counterpart of SetLeafChild. Its key
// comes from the synchronous Fold input or an existing BranchData and remains
// referenced until the branch store has encoded/copied it. Every store in this
// package obeys that contract, so a second defensive copy only adds allocation
// without strengthening lifetime.
func (b *BranchData) setLeafChildStable(nibble uint8, key []byte, valHash common.Hash) {
	b.children[nibble] = branchChild{
		present:   true,
		kind:      kindLeaf,
		leafKey:   key,
		valueHash: valHash,
	}
}

// Encode serialises the BranchData to a deterministic byte slice.
//
// Wire format:
//
//	[childMask uint16 big-endian]  — bitmask of present nibbles (bit i set ↔ child i present)
//	for each set bit i in childMask (low→high):
//	  [kind  1 byte]          0 = hash, 1 = leaf
//	  if kind == hash:
//	    [32-byte hash]
//	  if kind == leaf:
//	    [keyLen binary.Uvarint][key bytes][32-byte valHash]
func (b *BranchData) Encode() []byte {
	return b.EncodeTo(nil)
}

// EncodeTo appends BranchData's wire encoding to dst and returns the resulting
// slice. Allocates only if dst lacks the capacity. The bulk-sync writer path
// uses this with a sync.Pool-backed buffer to avoid 29 GB/300s of fresh
// per-PutBranch allocations observed on Nile sync.
func (b *BranchData) EncodeTo(dst []byte) []byte {
	// Compute childMask.
	var mask uint16
	for i := uint8(0); i < 16; i++ {
		if b.children[i].present {
			mask |= 1 << i
		}
	}

	// Pre-compute required capacity for a single grow.
	size := 2 // childMask
	for i := uint8(0); i < 16; i++ {
		c := &b.children[i]
		if !c.present {
			continue
		}
		size++ // kind byte
		if c.kind == kindHash {
			size += common.HashLength
		} else {
			// Uvarint for keyLen + key bytes + valHash
			size += binary.MaxVarintLen64 + len(c.leafKey) + common.HashLength
		}
	}
	if cap(dst)-len(dst) < size {
		grown := make([]byte, len(dst), len(dst)+size)
		copy(grown, dst)
		dst = grown
	}

	// Write childMask.
	dst = append(dst, byte(mask>>8), byte(mask))

	// Write children low→high nibble.
	for i := uint8(0); i < 16; i++ {
		c := &b.children[i]
		if !c.present {
			continue
		}
		dst = append(dst, c.kind)
		if c.kind == kindHash {
			dst = append(dst, c.valueHash[:]...)
		} else {
			var uvBuf [binary.MaxVarintLen64]byte
			n := binary.PutUvarint(uvBuf[:], uint64(len(c.leafKey)))
			dst = append(dst, uvBuf[:n]...)
			dst = append(dst, c.leafKey...)
			dst = append(dst, c.valueHash[:]...)
		}
	}
	return dst
}

// Equal reports whether b and other represent the same branch node.
// Two BranchData values are equal iff their encodings are byte-identical.
func (b BranchData) Equal(other BranchData) bool {
	enc1 := b.Encode()
	enc2 := other.Encode()
	if len(enc1) != len(enc2) {
		return false
	}
	for i := range enc1 {
		if enc1[i] != enc2[i] {
			return false
		}
	}
	return true
}

// DecodeBranchData parses a byte slice produced by BranchData.Encode.
// It returns an error on truncation, trailing bytes, invalid kind bytes, or
// a keyLen that exceeds the remaining input.
func DecodeBranchData(data []byte) (BranchData, error) {
	var b BranchData
	if err := DecodeBranchDataInto(data, &b); err != nil {
		return BranchData{}, err
	}
	return b, nil
}

// DecodeBranchDataInto is DecodeBranchData written directly into *dst (zeroed
// first). Used by GetBranchInto on the bulk-sync hot path to avoid the
// return-by-value copy of the ~1 KiB BranchData struct.
func DecodeBranchDataInto(data []byte, dst *BranchData) error {
	return decodeBranchDataInto(data, dst, true)
}

// decodeBranchDataIntoNoCopy is the fold reader's allocation-free variant.
// Leaf keys alias data; callers must keep data alive and immutable until dst is
// no longer used. rawdbBranchStore satisfies this with owned Get results or
// immutable-by-replacement blockbuffer/cache values.
func decodeBranchDataIntoNoCopy(data []byte, dst *BranchData) error {
	return decodeBranchDataInto(data, dst, false)
}

func decodeBranchDataInto(data []byte, dst *BranchData, copyLeafKeys bool) error {
	*dst = BranchData{}
	if len(data) < 2 {
		return errors.New("commitment_tree: input too short for childMask")
	}
	mask := uint16(data[0])<<8 | uint16(data[1])
	rest := data[2:]

	for i := uint8(0); i < 16; i++ {
		if mask&(1<<i) == 0 {
			continue
		}
		// Read kind byte.
		if len(rest) < 1 {
			return errors.New("commitment_tree: truncated at kind byte")
		}
		kind := rest[0]
		rest = rest[1:]

		switch kind {
		case kindHash:
			if len(rest) < common.HashLength {
				return errors.New("commitment_tree: truncated at hash child")
			}
			var h common.Hash
			copy(h[:], rest[:common.HashLength])
			rest = rest[common.HashLength:]
			dst.children[i] = branchChild{present: true, kind: kindHash, valueHash: h}

		case kindLeaf:
			// Decode keyLen via Uvarint; bound by remaining slice length.
			keyLen, n := binary.Uvarint(rest)
			if n <= 0 {
				return errors.New("commitment_tree: invalid uvarint for keyLen")
			}
			rest = rest[n:]
			if keyLen > uint64(len(rest)) {
				return errors.New("commitment_tree: keyLen exceeds remaining input")
			}
			key := rest[:keyLen]
			if copyLeafKeys {
				key = append([]byte(nil), key...)
			}
			rest = rest[keyLen:]
			if len(rest) < common.HashLength {
				return errors.New("commitment_tree: truncated at leaf valHash")
			}
			var vh common.Hash
			copy(vh[:], rest[:common.HashLength])
			rest = rest[common.HashLength:]
			dst.children[i] = branchChild{
				present:   true,
				kind:      kindLeaf,
				leafKey:   key,
				valueHash: vh,
			}

		default:
			return errors.New("commitment_tree: unknown child kind byte")
		}
	}

	if len(rest) != 0 {
		return errors.New("commitment_tree: trailing bytes after decode")
	}
	return nil
}

// ----------------------------------------------------------------------------
// BranchData read accessors
//
// These let the fold engine inspect children without exposing the unexported
// branchChild type. The wire format is unchanged.
// ----------------------------------------------------------------------------

// childPresent reports whether nibble has a present child.
func (b *BranchData) childPresent(nibble uint8) bool {
	return b.children[nibble].present
}

// childKindAt returns the kind (kindHash / kindLeaf) of the child at nibble.
// The caller must ensure the child is present.
func (b *BranchData) childKindAt(nibble uint8) uint8 {
	return b.children[nibble].kind
}

// hashChildAt returns the stored 32-byte hash of a hash child at nibble.
func (b *BranchData) hashChildAt(nibble uint8) common.Hash {
	return b.children[nibble].valueHash
}

// leafChildAt returns the key and value hash of a leaf child at nibble.
func (b *BranchData) leafChildAt(nibble uint8) (key []byte, valHash common.Hash) {
	c := &b.children[nibble]
	return c.leafKey, c.valueHash
}

// clearChild removes any child at nibble.
func (b *BranchData) clearChild(nibble uint8) {
	b.children[nibble] = branchChild{}
}

// childCount returns the number of present children.
func (b *BranchData) childCount() int {
	n := 0
	for i := uint8(0); i < 16; i++ {
		if b.children[i].present {
			n++
		}
	}
	return n
}

// onlyChildNibble returns the single present child's nibble. Callers use it only
// when childCount() == 1.
func (b *BranchData) onlyChildNibble() uint8 {
	for i := uint8(0); i < 16; i++ {
		if b.children[i].present {
			return i
		}
	}
	return 0
}

// nodeHash returns the hash of this branch node:
//
//	keccak256(0x01 || for each present child nibble low→high: nibble_byte || childHash)
//
// where childHash is the hash child's stored hash, or the leaf child's value
// hash.
func (b *BranchData) nodeHash() common.Hash {
	h := borrowKeccak()
	defer returnKeccak(h)
	h.nodeBuf[0] = 0x01
	off := 1
	for i := uint8(0); i < 16; i++ {
		c := &b.children[i]
		if !c.present {
			continue
		}
		h.nodeBuf[off] = i
		off++
		if c.kind == kindHash {
			copy(h.nodeBuf[off:], c.valueHash[:])
		} else {
			copy(h.nodeBuf[off:], c.valueHash[:])
		}
		off += common.HashLength
	}
	_, _ = h.Write(h.nodeBuf[:off])
	return readKeccakHash(h)
}

// ----------------------------------------------------------------------------
// Fold engine
// ----------------------------------------------------------------------------

// branchStore reads/writes persisted branch nodes during a fold, keyed by the
// trie prefix (nibble path from root, one byte per nibble, value 0..15).
type branchStore interface {
	GetBranch(prefix []byte) (BranchData, bool, error)
	// GetBranchInto reads a branch into *dst (zeroed first). The hot fold path
	// uses this with a pool-borrowed *BranchData so the ~1 KiB struct stays
	// out of the heap.
	GetBranchInto(prefix []byte, dst *BranchData) (bool, error)
	PutBranch(prefix []byte, b BranchData) error
	DelBranch(prefix []byte) error
}

// Update is one touched logical commitment key. Key is the gtron commitment key
// bytes (treated as opaque); Value is its current value (ignored if Delete).
type Update struct {
	Key    []byte
	Value  []byte
	Delete bool
}

// commitmentTrie is a hex-patricia (leaf-short-circuited) commitment trie backed
// by a branchStore. Branch nodes are keyed by their nibble prefix from the root.
type commitmentTrie struct {
	store branchStore

	// parallelMinOps, when > 0, folds the root's 16 first-nibble subtries
	// concurrently for any Fold with at least this many resolved ops. 0 (the
	// default for a bare newCommitmentTrie) keeps the fold fully sequential, so
	// existing callers and tests are unaffected; the staged store opts in. Both
	// paths produce byte-identical roots and branch rows (see applyRootParallel).
	parallelMinOps int
	// parallelLimit caps concurrent subtrie folds. <= 0 means GOMAXPROCS, itself
	// capped at the 16-way branching factor.
	parallelLimit int
}

func newCommitmentTrie(store branchStore) *commitmentTrie {
	return &commitmentTrie{store: store}
}

// pathLen is the number of nibbles in a hashed key path (keccak256 → 32 bytes).
const pathLen = common.HashLength * 2

// op is a resolved update: its full 64-nibble path plus the leaf value hash.
type op struct {
	path    [pathLen]byte
	key     []byte
	valHash common.Hash
	delete  bool
}

// Fold applies updates in any input order, emits the changed prefix-keyed branch
// nodes through the store, and returns the new root hash.
//
// Calling Fold with no updates re-derives and returns the current root without
// modifying the store.
func (t *commitmentTrie) Fold(updates []Update) (common.Hash, error) {
	ops, err := buildOps(updates)
	if err != nil {
		return common.Hash{}, err
	}

	// Load the root branch (empty prefix), if any.
	root, hasRoot, err := t.store.GetBranch(nil)
	if err != nil {
		return common.Hash{}, err
	}

	if len(ops) > 0 {
		var rootPtr *BranchData
		if hasRoot {
			rootPtr = &root
		}
		if t.parallelMinOps > 0 && len(ops) >= t.parallelMinOps {
			rootPtr, err = t.applyRootParallel(rootPtr, ops)
		} else {
			// Every recursive prefix is at most pathLen bytes. Reusing this
			// fold-local path stack avoids one allocation at every trie level;
			// stores consume/copy prefixes synchronously and never retain it.
			var path [pathLen]byte
			rootPtr, err = t.apply(path[:0], 0, rootPtr, ops)
		}
		if err != nil {
			return common.Hash{}, err
		}
		if rootPtr == nil {
			if hasRoot {
				if err := t.store.DelBranch(nil); err != nil {
					return common.Hash{}, err
				}
			}
			hasRoot = false
		} else {
			if err := t.store.PutBranch(nil, *rootPtr); err != nil {
				return common.Hash{}, err
			}
			root = *rootPtr
			hasRoot = true
		}
	}

	if !hasRoot {
		return common.Hash{}, nil
	}
	return rootHash(&root), nil
}

// rootHash returns the trie root hash for the root branch. The whole-trie
// singleton case (exactly one leaf child, no hash children at the root) collapses
// to that key's leaf value hash, per the spec.
func rootHash(root *BranchData) common.Hash {
	if root.childCount() == 1 {
		n := root.onlyChildNibble()
		if root.childKindAt(n) == kindLeaf {
			_, vh := root.leafChildAt(n)
			return vh
		}
	}
	return root.nodeHash()
}

// buildOps coalesces updates per key (last-writer-wins), resolves each to its
// 64-nibble path and leaf value hash, and returns them sorted by path. Sorting
// makes the in-tree walk order deterministic but does not affect the final
// structure (which is path-keyed).
func buildOps(updates []Update) ([]op, error) {
	if len(updates) == 0 {
		return nil, nil
	}
	// The production orchestrator hands staged Update the output of
	// rawdb.CoalesceStateCommitmentUpdates: keys are unique and strictly sorted.
	// Recognize that contract in one allocation-free scan and skip rebuilding a
	// second last-writer-wins map here. Direct Fold callers may still provide
	// arbitrary order or duplicates; those retain the general fallback below.
	strictlySorted := true
	for i := range updates {
		if len(updates[i].Key) == 0 {
			return nil, errors.New("commitment_tree: empty update key")
		}
		if strictlySorted && i > 0 && bytes.Compare(updates[i-1].Key, updates[i].Key) >= 0 {
			strictlySorted = false
		}
	}
	if strictlySorted {
		ops := make([]op, 0, len(updates))
		for _, u := range updates {
			ops = append(ops, resolveOp(u))
		}
		sortOps(ops)
		return ops, nil
	}

	byKey := make(map[string]Update, len(updates))
	for _, u := range updates {
		byKey[string(u.Key)] = u
	}
	ops := make([]op, 0, len(byKey))
	for _, u := range byKey {
		ops = append(ops, resolveOp(u))
	}
	sortOps(ops)
	return ops, nil
}

func resolveOp(u Update) op {
	// Fold is synchronous and every branch-store implementation consumes or
	// copies leaf keys before Fold returns. Borrowing the input key for that
	// interval avoids one allocation per update; persisted branch encodings do
	// not alias the caller's Update buffers.
	o := op{key: u.Key, delete: u.Delete}
	o.path = keyPath(u.Key)
	if !u.Delete {
		o.valHash = leafValueHash(u.Key, u.Value)
	}
	return o
}

// apply processes all ops that pass through the branch at prefix/depth and
// returns the resulting branch (nil if the branch should not exist).
//
// branch is the existing node at this prefix (nil if absent). All ops in the
// slice share the prefix path nibbles [0:depth).
func (t *commitmentTrie) apply(prefix []byte, depth int, branch *BranchData, ops []op) (*BranchData, error) {
	if branch == nil {
		branch = &BranchData{}
	}

	// Bucket ops by their nibble at this depth via counting sort into one
	// pooled scratch buffer. Replaces the prior `var buckets [16][]op` +
	// per-op append, which allocated up to 16 backing arrays per call frame
	// (recursive depth up to 64 → high churn on dense fold passes).
	var counts [16]int
	for _, o := range ops {
		counts[o.path[depth]]++
	}
	var starts [16]int
	for i := 1; i < 16; i++ {
		starts[i] = starts[i-1] + counts[i-1]
	}
	scratch := borrowOpsBuf(len(ops))
	defer returnOpsBuf(scratch)
	heads := starts
	for _, o := range ops {
		nb := o.path[depth]
		(*scratch)[heads[nb]] = o
		heads[nb]++
	}

	for nb := uint8(0); nb < 16; nb++ {
		n := counts[nb]
		if n == 0 {
			continue
		}
		group := (*scratch)[starts[nb] : starts[nb]+n]
		if err := t.applyNibble(prefix, depth, branch, nb, group); err != nil {
			return nil, err
		}
	}

	// An emptied branch must not persist. Single-LEAF collapse for non-root
	// branches is enforced by the parent in linkChild; the root keeps its
	// single-LEAF form (the root-hash rule special-cases it), so here we only
	// need to drop fully-empty branches.
	if branch.childCount() == 0 {
		return nil, nil
	}
	return branch, nil
}

// applyNibble applies the op group that descends into nibble nb of the branch at
// prefix/depth, mutating branch in place.
func (t *commitmentTrie) applyNibble(prefix []byte, depth int, branch *BranchData, nb uint8, group []op) error {
	childPrefix := appendNibble(prefix, nb)

	if !branch.childPresent(nb) {
		// Empty slot. Insert the surviving puts; if exactly one survives, it
		// becomes a leaf child, otherwise build a child subtree.
		return t.insertIntoEmpty(branch, nb, childPrefix, depth+1, group)
	}

	switch branch.childKindAt(nb) {
	case kindLeaf:
		return t.applyOnLeaf(branch, nb, childPrefix, depth+1, group)
	case kindHash:
		return t.applyOnHash(branch, nb, childPrefix, depth+1, group)
	default:
		return fmt.Errorf("commitment_tree: unknown child kind %d", branch.childKindAt(nb))
	}
}

// insertIntoEmpty fills an absent slot nb with the surviving puts in group.
func (t *commitmentTrie) insertIntoEmpty(branch *BranchData, nb uint8, childPrefix []byte, childDepth int, group []op) error {
	puts := livePutsInPlace(group)
	switch len(puts) {
	case 0:
		// Deletes into an empty slot are no-ops.
		return nil
	case 1:
		branch.setLeafChildStable(nb, puts[0].key, puts[0].valHash)
		return nil
	default:
		// Build a fresh child subtree rooted at childPrefix, borrowing the
		// branch from the pool so the descent doesn't escape to the heap.
		child := borrowBranch()
		defer returnBranch(child)
		updated, err := t.apply(childPrefix, childDepth, child, puts)
		if err != nil {
			return err
		}
		return t.linkChild(branch, nb, childPrefix, updated)
	}
}

// applyOnLeaf resolves group against an existing leaf child at nb.
func (t *commitmentTrie) applyOnLeaf(branch *BranchData, nb uint8, childPrefix []byte, childDepth int, group []op) error {
	existKey, existVH := branch.leafChildAt(nb)

	// Collect surviving entries under this slot via a small-set linear scan.
	// The original implementation used map[string]op{}, which heap-allocates
	// the map header + buckets per call (~3.8% of fold alloc count). In
	// practice the survivor count is ~1-2 (existing leaf + a few ops), so
	// linear scan over a stack-backed slice is both alloc-free and faster.
	// Slice capacity 16 covers the realistic worst case (group contains ops
	// for at most ~all 16 sibling-nibble slots).
	var stack [16]op
	survivors := stack[:0]
	// Delay hashing the existing key. The overwhelmingly common path updates or
	// deletes that same leaf, in which case the incoming op already carries its
	// path (or no path is needed). Only a split with the old leaf still present
	// needs its path for the recursive sort/descent.
	survivors = append(survivors, op{key: existKey, valHash: existVH})
	existingNeedsPath := true

	for _, o := range group {
		// Linear find by raw-key byte equality.
		idx := -1
		for i := range survivors {
			if bytes.Equal(survivors[i].key, o.key) {
				idx = i
				break
			}
		}
		if o.delete {
			if idx >= 0 {
				if idx == 0 {
					existingNeedsPath = false
				}
				// Swap-remove (order irrelevant — sorted below if we recurse).
				last := len(survivors) - 1
				survivors[idx] = survivors[last]
				survivors = survivors[:last]
			}
			continue
		}
		if idx >= 0 {
			if idx == 0 {
				existingNeedsPath = false
			}
			survivors[idx] = o
		} else {
			survivors = append(survivors, o)
		}
	}

	switch len(survivors) {
	case 0:
		branch.clearChild(nb)
		return nil
	case 1:
		// Exactly one survivor → leaf child.
		only := survivors[0]
		branch.setLeafChildStable(nb, only.key, only.valHash)
		return nil
	default:
		if existingNeedsPath {
			survivors[0].path = keyPath(existKey)
		}
		// Multiple survivors → build a child subtree in a separate frame.
		// Keeping the recursive apply/sortOps calls out of this function frame is
		// what lets the survivors `stack` array above stay on the stack: Go's
		// escape analysis is per-function, so passing `survivors` to an escaping
		// callee here would force the whole 16-op array to the heap on EVERY
		// applyOnLeaf call — including the common 0/1-survivor cases that never
		// recurse (the dominant fold allocation, ~15% of insertion heap). The
		// multi-survivor branch borrows a pooled op buffer instead.
		return t.applyLeafSplit(branch, nb, childPrefix, childDepth, survivors)
	}
}

// applyLeafSplit handles the multi-survivor case of applyOnLeaf: the slot's
// existing leaf plus incoming ops resolve to ≥2 distinct keys, so a child
// subtree must be built. Split into its own frame so applyOnLeaf's survivor
// scratch stays stack-allocated (see the call site). The survivors slice aliases
// the caller's stack array, so it is copied into a pooled buffer before the
// recursive descent (which sorts in place and may retain ordering across the
// fold); the pooled buffer is returned at frame exit.
func (t *commitmentTrie) applyLeafSplit(branch *BranchData, nb uint8, childPrefix []byte, childDepth int, survivors []op) error {
	bufP := borrowOpsBuf(len(survivors))
	defer returnOpsBuf(bufP)
	buf := *bufP
	copy(buf, survivors)

	// sortOps gives a deterministic traversal so apply's bucket sort is stable.
	sortOps(buf)
	child := borrowBranch()
	defer returnBranch(child)
	updated, err := t.apply(childPrefix, childDepth, child, buf)
	if err != nil {
		return err
	}
	return t.linkChild(branch, nb, childPrefix, updated)
}

// applyOnHash resolves group against an existing hash child (a child subtree) at
// nb. The child branch is borrowed from branchPool so the per-descent ~1 KiB
// BranchData allocation (formerly the #1 alloc source at ~24% of fold heap
// pressure) becomes pool reuse. linkChild consumes the data and never retains
// the pointer past return, so the deferred release is unconditional.
func (t *commitmentTrie) applyOnHash(branch *BranchData, nb uint8, childPrefix []byte, childDepth int, group []op) error {
	child := borrowBranch()
	defer returnBranch(child)
	ok, err := t.store.GetBranchInto(childPrefix, child)
	if err != nil {
		return err
	}
	if !ok {
		return fmt.Errorf("commitment_tree: missing hash child at prefix %x", childPrefix)
	}
	updated, err := t.apply(childPrefix, childDepth, child, group)
	if err != nil {
		return err
	}
	// updated is either child (mutated) or nil (subtree collapsed). linkChild
	// handles both. defer returnBranch(child) above releases child after
	// linkChild returns regardless of which case fired.
	return t.linkChild(branch, nb, childPrefix, updated)
}

// linkChild persists (or deletes) the child subtree at childPrefix and wires the
// parent's slot nb to match. It enforces the invariant that a non-root branch is
// never a single-LEAF node: such a child collapses up into the parent's slot as
// a leaf, and the child branch row is removed.
func (t *commitmentTrie) linkChild(branch *BranchData, nb uint8, childPrefix []byte, child *BranchData) error {
	if child == nil {
		// Child subtree vanished.
		if err := t.store.DelBranch(childPrefix); err != nil {
			return err
		}
		branch.clearChild(nb)
		return nil
	}
	if child.childCount() == 1 {
		cn := child.onlyChildNibble()
		if child.childKindAt(cn) == kindLeaf {
			// Collapse the single-leaf child into the parent slot.
			if err := t.store.DelBranch(childPrefix); err != nil {
				return err
			}
			k, vh := child.leafChildAt(cn)
			branch.setLeafChildStable(nb, k, vh)
			return nil
		}
		// Single HASH child is a valid (extension-like) node; keep it.
	}
	if err := t.store.PutBranch(childPrefix, *child); err != nil {
		return err
	}
	branch.SetHashChild(nb, child.nodeHash())
	return nil
}

// livePutsInPlace compacts the surviving puts to the front of group after
// dropping deletes. Groups always alias fold-owned scratch (never caller input),
// so reusing that storage avoids a heap slice at every descent into an empty
// slot. Within a Fold the group is already coalesced per key.
func livePutsInPlace(group []op) []op {
	out := group[:0]
	for _, o := range group {
		if !o.delete {
			out = append(out, o)
		}
	}
	clear(group[len(out):])
	return out
}

func sortOps(ops []op) {
	sort.Slice(ops, func(i, j int) bool {
		for n := 0; n < pathLen; n++ {
			if ops[i].path[n] != ops[j].path[n] {
				return ops[i].path[n] < ops[j].path[n]
			}
		}
		return string(ops[i].key) < string(ops[j].key)
	})
}

// appendNibble extends the fold-local path stack with nb. Fold and each
// parallel root worker provide pathLen capacity, so the recursive descent
// reuses one backing array. The fallback keeps direct internal callers safe.
// branchStore methods must consume or copy prefixes synchronously; every
// implementation in this package does so.
func appendNibble(prefix []byte, nb uint8) []byte {
	return append(prefix, nb)
}

// keyPath expands keccak256(lenPrefixed(key)) into pathLen nibbles, high nibble
// first.
func keyPath(key []byte) [pathLen]byte {
	h := borrowKeccak()
	defer returnKeccak(h)
	writeLen8Prefixed(h, key)
	sum := readKeccakHash(h)
	var out [pathLen]byte
	for i := 0; i < common.HashLength; i++ {
		out[2*i] = sum[i] >> 4
		out[2*i+1] = sum[i] & 0x0f
	}
	return out
}

// leafValueHash is the value hash of a key: keccak256(0x00 || lenPrefixed(key) ||
// lenPrefixed(value)).
func leafValueHash(key, value []byte) common.Hash {
	h := borrowKeccak()
	defer returnKeccak(h)
	writeKeccakByte(h, 0x00)
	writeLen8Prefixed(h, key)
	writeLen8Prefixed(h, value)
	return readKeccakHash(h)
}

// writeLen8Prefixed writes an 8-byte big-endian length followed by the bytes,
// matching the convention used elsewhere for commitment hashing.
func writeLen8Prefixed(h *pooledKeccak, data []byte) {
	binary.BigEndian.PutUint64(h.lenBuf[:], uint64(len(data)))
	_, _ = h.Write(h.lenBuf[:])
	_, _ = h.Write(data)
}
