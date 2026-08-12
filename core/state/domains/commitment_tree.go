package domains

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"hash"
	"math/bits"
	"slices"
	"sort"
	"sync"
	"sync/atomic"
	"unsafe"

	fastkeccak "github.com/erigontech/fastkeccak"
	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
)

// keccakPool reuses Legacy Keccak-256 hashers across fold passes. A
// single Nile-sync segment allocates ~16 GB of hashers via this constructor
// (1 per nodeHash/keyPath/leafValueHash call); the pool turns those into
// Reset-and-reuse and cuts that source of GC pressure to near zero. sync.Pool
// is safe across the parallel root's subtrie workers, and each borrow/return
// cycle remains strictly local to one hash invocation. Erigon's fastkeccak is
// byte-identical to x/crypto's legacy implementation and provides specialized
// amd64 and arm64 Keccak-f assembly paths.
var keccakPool = sync.Pool{
	New: func() any {
		return &pooledKeccak{keccakState: fastkeccak.NewFastKeccak()}
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
	// Preserve the pool's clean-on-borrow contract. Hot fold paths reuse the
	// same hasher for many digests, so this one Reset per Fold/worker is
	// negligible while keeping future callers safe from a returned sponge's
	// absorb/squeeze state.
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
var branchPool sync.Pool

// BranchData is a fixed, pointer-bearing structure whose leaf strings are
// cleared before pooling. Allocate cold pool misses in stable slabs: recursive
// fold descent still borrows ordinary *BranchData pointers, while the GC tracks
// one object per slab instead of one object per trie level/worker. sync.Pool may
// discard the spare interior pointers at any GC, bounding idle retention.
const branchPoolBatchSize = 8

func borrowBranch() *BranchData {
	if pooled := branchPool.Get(); pooled != nil {
		return pooled.(*BranchData)
	}
	batch := new([branchPoolBatchSize]BranchData)
	for i := 1; i < len(batch); i++ {
		branchPool.Put(&batch[i])
	}
	return &batch[0]
}

// borrowEmptyBranch returns a zeroed branch for callers constructing a new
// subtree. Callers that immediately decode or assign a complete BranchData use
// borrowBranch directly and avoid clearing the ~800-byte object twice.
func borrowEmptyBranch() *BranchData {
	b := borrowBranch()
	*b = BranchData{}
	return b
}

func returnBranch(b *BranchData) {
	if b == nil {
		return
	}
	clearBranchForPool(b)
	branchPool.Put(b)
}

func clearBranchForPool(b *BranchData) {
	// Pooled branches otherwise retain leaf-key backing storage until the pool
	// is cleared by a later GC. A fold can decode those strings from a large
	// shared arena, so a handful of idle BranchData objects may pin the complete
	// arena and make the mark phase resolve every retained leaf pointer. Clear
	// only the pointer-bearing live leaf slots; hashes/path leaves are fixed-width
	// values.
	// Path-only leaves carry no string. Excluding them makes the current rooted
	// state format's common return path just two mask loads/stores.
	legacyLeaves := b.leafMask() &^ uint16(atomic.LoadUint32(&b.leafPathMask))
	for remaining := legacyLeaves; remaining != 0; remaining &= remaining - 1 {
		nibble := bits.TrailingZeros16(remaining)
		b.children[nibble].leafKey = ""
	}
	atomic.StoreUint32(&b.childMask, 0)
	atomic.StoreUint32(&b.leafPathMask, 0)
}

// opsBufPool reuses op slices for Fold's resolved updates and apply's
// bucket-sort scratch space. apply
// formerly used `var buckets [16][]op` + append per op, which heap-allocated up
// to 16 backing arrays per recursive call (the fold is recursive to depth 64).
// The replacement counting-sort writes into a single pooled scratch buffer per
// apply invocation, cutting per-descent slice churn. Capping pooled capacity
// prevents an exceptional block from pinning an oversized backing array.
const maxPooledOps = 4096

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
	ops := *bp
	// Only op.key contains a reference into the Fold input. Every borrower
	// overwrites all fields in every used element, so clear just the slice header
	// rather than the path/value hashes too.
	for i := range ops {
		ops[i].key = nil
	}
	if cap(ops) > maxPooledOps {
		*bp = nil
		return
	}
	*bp = ops[:0]
	opsBufPool.Put(bp)
}

// childKind distinguishes the two child types stored in a BranchData node.
const (
	kindHash     = uint8(0) // 32-byte intermediate hash
	kindLeaf     = uint8(1) // legacy/plain key bytes + 32-byte value hash
	kindLeafPath = uint8(2) // 32-byte hashed key path + 32-byte value hash
)

// branchChild holds one present child entry of a hex-trie branch node.
type branchChild struct {
	valueHash common.Hash // child hash or leaf value hash, selected by childMask
	leafKey   string      // immutable raw key for legacy/plain leaves
	leafPath  common.Hash // owned path identity when leafPathMask marks this slot
}

// stableLeafKeyString aliases immutable fold/decoder storage without copying.
// Every caller retains the backing bytes for the BranchData lifetime.
func stableLeafKeyString(key []byte) string {
	if len(key) == 0 {
		return ""
	}
	return unsafe.String(unsafe.SliceData(key), len(key))
}

// leafKeyBytes returns a read-only byte view of an immutable leaf-key string.
// Callers must not mutate the returned slice.
func leafKeyBytes(key string) []byte {
	if len(key) == 0 {
		return nil
	}
	return unsafe.Slice(unsafe.StringData(key), len(key))
}

// BranchData represents a hex (16-way) trie branch node.  A branch has up to
// 16 children indexed by nibble 0–15.  Each present child is either an
// intermediate hash child or a leaf (raw key or shortened key path + value
// hash).
//
// Children are stored in a fixed 16-slot array so insertion order never
// affects encoding — Encode always iterates nibbles low→high.
type BranchData struct {
	// childMask packs the 16 presence bits in its low half and the 16 hash-kind
	// bits in its high half. It is updated atomically because applyRootParallel
	// mutates the root's disjoint child slots concurrently. Keeping the atomic
	// word as a plain uint32 preserves BranchData's safe value-copy semantics
	// after those workers join (unlike embedding atomic.Uint32, whose value must
	// not copy). The kind bits let encoding and decode cleanup skip hash children.
	childMask uint32
	// leafPathMask marks leaf slots whose leafPath contains the already-computed
	// 32-byte trie path instead of leafKey holding the complete physical
	// latest-domain key.
	// Commitment lookup is path-addressed already, so retaining the full key in
	// every leaf branch repeats 40+ bytes solely to hash it again on the next
	// collision/update. Erigon likewise shortens plain keys in commitment
	// branch data. A separate atomic word keeps the parallel root's disjoint
	// child mutations race-free without changing childMask's presence/kind bits.
	leafPathMask uint32
	children     [16]branchChild
}

func (b *BranchData) presentMask() uint16 {
	return uint16(atomic.LoadUint32(&b.childMask))
}

func (b *BranchData) leafMask() uint16 {
	bits := atomic.LoadUint32(&b.childMask)
	return uint16(bits) &^ uint16(bits>>16)
}

func (b *BranchData) leafPathAt(nibble uint8) bool {
	return uint16(atomic.LoadUint32(&b.leafPathMask))&(1<<nibble) != 0
}

func (b *BranchData) markLeafChild(nibble uint8, pathOnly bool) {
	childBit := uint32(1) << nibble
	hashBit := childBit << 16
	for {
		old := atomic.LoadUint32(&b.childMask)
		updated := (old | childBit) &^ hashBit
		if atomic.CompareAndSwapUint32(&b.childMask, old, updated) {
			break
		}
	}
	if pathOnly {
		atomic.OrUint32(&b.leafPathMask, childBit)
	} else {
		atomic.AndUint32(&b.leafPathMask, ^childBit)
	}
}

// SetHashChild marks nibble as a hash child with the given 32-byte hash.
// Overwrites any previous child at that nibble.
func (b *BranchData) SetHashChild(nibble uint8, h common.Hash) {
	childBit := uint32(1) << nibble
	atomic.OrUint32(&b.childMask, childBit|(childBit<<16))
	atomic.AndUint32(&b.leafPathMask, ^childBit)
	b.children[nibble] = branchChild{
		valueHash: h,
	}
}

// SetLeafChild marks nibble as a leaf child with the given key and value hash.
// Overwrites any previous child at that nibble.
func (b *BranchData) SetLeafChild(nibble uint8, key []byte, valHash common.Hash) {
	b.markLeafChild(nibble, false)
	b.children[nibble] = branchChild{
		leafKey:   string(key),
		valueHash: valHash,
	}
}

// setLeafChildPath stores the commitment key's 32-byte Keccak path as
// the leaf identity. The path is copied into the fixed-width child field, so
// op sorting/compaction and parallel branch buffering cannot alias it. The leaf
// value hash already commits to the complete raw key, while the trie can only
// distinguish keys by this path, so no authority is lost by dropping the
// repeated physical key from branch storage.
func (b *BranchData) setLeafChildPath(nibble uint8, path []byte, valHash common.Hash) {
	if len(path) != common.HashLength {
		panic("commitment_tree: leaf path must be 32 bytes")
	}
	b.markLeafChild(nibble, true)
	b.children[nibble] = branchChild{
		leafPath:  common.Hash(path),
		valueHash: valHash,
	}
}

// Encode serialises the BranchData to a deterministic byte slice.
//
// Wire format:
//
//	[childMask uint16 big-endian]  — bitmask of present nibbles (bit i set ↔ child i present)
//	for each set bit i in childMask (low→high):
//	  [kind  1 byte]          0 = hash, 1 = legacy/plain leaf, 2 = path leaf
//	  if kind == hash:
//	    [32-byte hash]
//	  if kind == legacy/plain leaf:
//	    [keyLen binary.Uvarint][key bytes][32-byte valHash]
//	  if kind == path leaf:
//	    [32-byte key path][32-byte valHash]
func (b *BranchData) Encode() []byte {
	return b.EncodeTo(nil)
}

func (b *BranchData) encodingLayout() (uint32, int) {
	childBits := atomic.LoadUint32(&b.childMask)
	pathMask := uint16(atomic.LoadUint32(&b.leafPathMask))
	mask := uint16(childBits)
	// Every present child contributes its kind byte and 32-byte hash. Only leaf
	// children need variable-length accounting on top of that fixed cost.
	size := 2 + (1+common.HashLength)*bits.OnesCount16(mask)
	for remaining := mask &^ uint16(childBits>>16); remaining != 0; remaining &= remaining - 1 {
		i := uint8(bits.TrailingZeros16(remaining))
		c := &b.children[i]
		// The fixed cost above already includes the value hash.
		if pathMask&(1<<i) != 0 {
			size += common.HashLength
		} else {
			size += uvarintEncodedLen(uint64(len(c.leafKey))) + len(c.leafKey)
		}
	}
	return childBits, size
}

// EncodeTo appends BranchData's wire encoding to dst and returns the resulting
// slice. Allocates only if dst lacks the capacity. The bulk-sync writer path
// uses this with a sync.Pool-backed buffer to avoid 29 GB/300s of fresh
// per-PutBranch allocations observed on Nile sync.
func (b *BranchData) EncodeTo(dst []byte) []byte {
	// Compute the mask and exact wire length together. Leaf key lengths are
	// ordinary small uvarints; reserving binary.MaxVarintLen64 (10 bytes) for
	// each one over-allocates the immutable encoding retained by blockbuffer.
	childBits, size := b.encodingLayout()
	return b.encodeToLayout(dst, childBits, size)
}

// encodeToLayout is EncodeTo with a caller-supplied layout. Bulk sibling
// persistence needs every encoded size up front to allocate one exact arena;
// retaining the computed child bits/size lets its encoding pass avoid
// rescanning all 16 child slots or reloading the atomic mask a second time.
func (b *BranchData) encodeToLayout(dst []byte, childBits uint32, size int) []byte {
	if cap(dst)-len(dst) < size {
		grown := make([]byte, len(dst), len(dst)+size)
		copy(grown, dst)
		dst = grown
	}

	mask := uint16(childBits)
	// Write childMask.
	dst = append(dst, byte(mask>>8), byte(mask))

	// Write present children low→high nibble. encodingLayout already produced
	// the presence and kind masks, so skip absent slots without rescanning all
	// 16 entries or reloading childMask.
	hashMask := uint16(childBits >> 16)
	pathMask := uint16(atomic.LoadUint32(&b.leafPathMask))
	for remaining := mask; remaining != 0; remaining &= remaining - 1 {
		i := uint8(bits.TrailingZeros16(remaining))
		c := &b.children[i]
		if hashMask&(1<<i) != 0 {
			dst = append(dst, kindHash)
			dst = append(dst, c.valueHash[:]...)
		} else if pathMask&(1<<i) != 0 {
			dst = append(dst, kindLeafPath)
			dst = append(dst, c.leafPath[:]...)
			dst = append(dst, c.valueHash[:]...)
		} else {
			dst = append(dst, kindLeaf)
			var uvBuf [binary.MaxVarintLen64]byte
			n := binary.PutUvarint(uvBuf[:], uint64(len(c.leafKey)))
			dst = append(dst, uvBuf[:n]...)
			dst = append(dst, c.leafKey...)
			dst = append(dst, c.valueHash[:]...)
		}
	}
	return dst
}

func uvarintEncodedLen(v uint64) int {
	n := 1
	for v >= 0x80 {
		v >>= 7
		n++
	}
	return n
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
// return-by-value copy of the fixed, kilobyte-scale BranchData struct.
func DecodeBranchDataInto(data []byte, dst *BranchData) error {
	return decodeBranchDataIntoArena(data, dst, nil)
}

// decodeBranchDataIntoArena is the fold-scoped form of DecodeBranchDataInto.
// arena remains immutable until every BranchData decoded during that fold has
// been encoded or discarded, so value copies can safely share its leaf-key
// strings. A nil arena preserves the public decoder's independent ownership.
func decodeBranchDataIntoArena(data []byte, dst *BranchData, arena *[]byte) error {
	if err := decodeBranchDataInto(data, dst); err != nil {
		// The no-copy parser may have installed views into data before detecting
		// a later malformed child. Public callers must never observe those
		// transient partial results.
		*dst = BranchData{}
		return err
	}
	// A cold Pebble view is valid only until its callback returns. Copy every
	// leaf key into one exact-size immutable arena. BranchData copies may share
	// those slices safely because the arena is never mutated. This replaces up
	// to 16 tiny allocations per decoded branch without retaining the
	// hash-dominated encoded Pebble value.
	leafMask := dst.leafMask()
	totalLeafKeyBytes := 0
	for remaining := leafMask; remaining != 0; remaining &= remaining - 1 {
		i := bits.TrailingZeros16(remaining)
		totalLeafKeyBytes += len(dst.children[i].leafKey)
	}
	if totalLeafKeyBytes == 0 {
		return nil
	}
	var leafKeys []byte
	if arena == nil {
		leafKeys = make([]byte, totalLeafKeyBytes)
	} else {
		start := len(*arena)
		*arena = slices.Grow(*arena, totalLeafKeyBytes)
		*arena = (*arena)[:start+totalLeafKeyBytes]
		leafKeys = (*arena)[start : start+totalLeafKeyBytes : start+totalLeafKeyBytes]
	}
	offset := 0
	for remaining := leafMask; remaining != 0; remaining &= remaining - 1 {
		i := bits.TrailingZeros16(remaining)
		child := &dst.children[i]
		if len(child.leafKey) == 0 {
			continue
		}
		end := offset + copy(leafKeys[offset:], child.leafKey)
		child.leafKey = stableLeafKeyString(leafKeys[offset:end:end])
		offset = end
	}
	return nil
}

// decodeBranchDataIntoNoCopy is the fold reader's allocation-free variant.
// Leaf keys alias data; callers must keep data alive and immutable until dst is
// no longer used. rawdbBranchStore uses it only for owned Get results and
// immutable overlay values; callback-scoped cache/Pebble values take the
// copying DecodeBranchDataInto path.
func decodeBranchDataIntoNoCopy(data []byte, dst *BranchData) error {
	if err := decodeBranchDataInto(data, dst); err != nil {
		// The pooled fold reader may have installed partial leaf-key views before
		// discovering malformed input. Errors are cold; fully clear here so the
		// next pool borrower cannot retain those views behind an empty mask.
		*dst = BranchData{}
		return err
	}
	return nil
}

func decodeBranchDataInto(data []byte, dst *BranchData) error {
	// Decoding overwrites every field of each newly-present child. Clear only
	// pointer-bearing fields from the previous presence mask instead of
	// zeroing the whole kilobyte-scale BranchData (mostly hashes) on every sparse pooled
	// read. At ten or more children the fixed-size memclr wins over the bit walk
	// (BenchmarkDecodeBranchDataIntoNoCopyReuse), so retain it for dense nodes.
	oldBits := atomic.LoadUint32(&dst.childMask)
	oldMask := uint16(oldBits)
	if bits.OnesCount16(oldMask) >= 10 {
		*dst = BranchData{}
	} else {
		for remaining := oldMask &^ uint16(oldBits>>16); remaining != 0; remaining &= remaining - 1 {
			i := bits.TrailingZeros16(remaining)
			dst.children[i].leafKey = ""
			dst.children[i].leafPath = common.Hash{}
		}
	}
	if len(data) < 2 {
		return errors.New("commitment_tree: input too short for childMask")
	}
	mask := uint16(data[0])<<8 | uint16(data[1])
	var hashMask uint16
	var pathMask uint16
	rest := data[2:]
	for remaining := mask; remaining != 0; remaining &= remaining - 1 {
		i := uint8(bits.TrailingZeros16(remaining))
		// Read kind byte.
		if len(rest) < 1 {
			return errors.New("commitment_tree: truncated at kind byte")
		}
		kind := rest[0]
		rest = rest[1:]

		switch kind {
		case kindHash:
			hashMask |= 1 << i
			if len(rest) < common.HashLength {
				return errors.New("commitment_tree: truncated at hash child")
			}
			child := &dst.children[i]
			child.leafKey = ""
			child.leafPath = common.Hash{}
			// The checked fixed-width conversion compiles to inline vector moves;
			// a slice copy otherwise calls runtime.memmove on linux/amd64.
			child.valueHash = common.Hash(rest[:common.HashLength])
			rest = rest[common.HashLength:]

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
			rest = rest[keyLen:]
			if len(rest) < common.HashLength {
				return errors.New("commitment_tree: truncated at leaf valHash")
			}
			child := &dst.children[i]
			child.leafKey = stableLeafKeyString(key)
			child.leafPath = common.Hash{}
			child.valueHash = common.Hash(rest[:common.HashLength])
			rest = rest[common.HashLength:]

		case kindLeafPath:
			if len(rest) < 2*common.HashLength {
				return errors.New("commitment_tree: truncated at path leaf")
			}
			child := &dst.children[i]
			child.leafKey = ""
			child.leafPath = common.Hash(rest[:common.HashLength])
			child.valueHash = common.Hash(rest[common.HashLength : 2*common.HashLength])
			pathMask |= 1 << i
			rest = rest[2*common.HashLength:]

		default:
			return errors.New("commitment_tree: unknown child kind byte")
		}
	}

	if len(rest) != 0 {
		return errors.New("commitment_tree: trailing bytes after decode")
	}
	atomic.StoreUint32(&dst.childMask, uint32(mask)|(uint32(hashMask)<<16))
	atomic.StoreUint32(&dst.leafPathMask, uint32(pathMask))
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
	return b.presentMask()&(1<<nibble) != 0
}

// childKindAt returns the kind (kindHash / kindLeaf) of the child at nibble.
// The caller must ensure the child is present.
func (b *BranchData) childKindAt(nibble uint8) uint8 {
	if uint16(atomic.LoadUint32(&b.childMask)>>16)&(1<<nibble) != 0 {
		return kindHash
	}
	return kindLeaf
}

// hashChildAt returns the stored 32-byte hash of a hash child at nibble.
func (b *BranchData) hashChildAt(nibble uint8) common.Hash {
	return b.children[nibble].valueHash
}

// leafChildAt returns the key and value hash of a leaf child at nibble.
func (b *BranchData) leafChildAt(nibble uint8) (key []byte, valHash common.Hash) {
	c := &b.children[nibble]
	return leafKeyBytes(c.leafKey), c.valueHash
}

func (b *BranchData) leafChildIdentityAt(nibble uint8) (identity []byte, pathOnly bool, valHash common.Hash) {
	c := &b.children[nibble]
	if b.leafPathAt(nibble) {
		return c.leafPath[:], true, c.valueHash
	}
	return leafKeyBytes(c.leafKey), false, c.valueHash
}

// clearChild removes any child at nibble.
func (b *BranchData) clearChild(nibble uint8) {
	childBit := uint32(1) << nibble
	atomic.AndUint32(&b.childMask, ^(childBit | (childBit << 16)))
	atomic.AndUint32(&b.leafPathMask, ^childBit)
	b.children[nibble] = branchChild{}
}

// childCount returns the number of present children.
func (b *BranchData) childCount() int {
	return bits.OnesCount16(b.presentMask())
}

// onlyChildNibble returns the single present child's nibble. Callers use it only
// when childCount() == 1.
func (b *BranchData) onlyChildNibble() uint8 {
	return uint8(bits.TrailingZeros16(b.presentMask()))
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
	return b.nodeHashWith(h)
}

// nodeHashWith is nodeHash using a caller-owned reusable sponge. A production
// fold keeps one per sequential worker, avoiding a sync.Pool round trip for
// every branch on the recursive path.
func (b *BranchData) nodeHashWith(h *pooledKeccak) common.Hash {
	return b.nodeHashWithStats(h, nil)
}

func (b *BranchData) nodeHashWithStats(h *pooledKeccak, stats *commitmentFoldStats) common.Hash {
	h.Reset()
	h.nodeBuf[0] = 0x01
	off := 1
	for remaining := b.presentMask(); remaining != 0; remaining &= remaining - 1 {
		i := uint8(bits.TrailingZeros16(remaining))
		c := &b.children[i]
		h.nodeBuf[off] = i
		off++
		copy(h.nodeBuf[off:], c.valueHash[:])
		off += common.HashLength
	}
	stats.observeNodeHashPreimage(uint64(off))
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
	// uses this with a pool-borrowed *BranchData so the ~800-byte struct stays
	// out of the heap.
	GetBranchInto(prefix []byte, dst *BranchData) (bool, error)
	PutBranch(prefix []byte, b BranchData) error
	DelBranch(prefix []byte) error
}

// Update is one touched logical commitment key. Keep it as an alias of the
// rawdb orchestrator's transport type so staged updates can flow into Fold
// without allocating and copying an identical intermediate slice.
type Update = rawdb.StateCommitmentUpdate

// commitmentTrie is a hex-patricia (leaf-short-circuited) commitment trie backed
// by a branchStore. Branch nodes are keyed by their nibble prefix from the root.
type commitmentTrie struct {
	store branchStore
	// hasher belongs exclusively to this sequential trie worker. The parallel
	// root creates a private sub-trie (and hasher) per long-lived worker.
	hasher *pooledKeccak
	// foldStats belongs to the current Fold. Sequential work updates it
	// directly; parallel root workers use sibling-local stats and merge after
	// joining, so node hashing never performs a process-wide atomic increment.
	foldStats *commitmentFoldStats

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

func (t *commitmentTrie) keyPath(key []byte) common.Hash {
	if t.hasher != nil {
		return keyPathWithHasher(t.hasher, key)
	}
	return keyPath(key)
}

func (t *commitmentTrie) nodeHash(branch *BranchData) common.Hash {
	if t.hasher != nil {
		return branch.nodeHashWithStats(t.hasher, t.foldStats)
	}
	h := borrowKeccak()
	defer returnKeccak(h)
	return branch.nodeHashWithStats(h, t.foldStats)
}

// pathLen is the number of nibbles in a hashed key path (keccak256 → 32 bytes).
const pathLen = common.HashLength * 2

// op is a resolved update: its 32-byte path digest plus the leaf value hash.
// Trie traversal extracts high/low nibbles on demand. Keeping the compact
// digest avoids expanding and clearing another 32 bytes in every top-level and
// recursive scratch op while preserving the identical 64-nibble path.
type op struct {
	path    common.Hash
	key     []byte
	valHash common.Hash
	delete  bool
}

// Fold applies updates in any input order, emits the changed prefix-keyed branch
// nodes through the store, and returns the new root hash.
//
// Calling Fold with no updates re-derives and returns the current root without
// modifying the store.
func (t *commitmentTrie) Fold(updates []Update) (result common.Hash, err error) {
	stats := beginCommitmentFoldStats(len(updates))
	previousStats := t.foldStats
	t.foldStats = stats
	defer func() {
		t.foldStats = previousStats
		finishCommitmentFoldStats(stats, err != nil)
	}()

	h := borrowKeccak()
	previousHasher := t.hasher
	t.hasher = h
	defer func() {
		t.hasher = previousHasher
		returnKeccak(h)
	}()

	opsP, err := buildOpsWithHasher(updates, h)
	if err != nil {
		return common.Hash{}, err
	}
	if opsP != nil {
		defer returnOpsBuf(opsP)
	}
	var ops []op
	if opsP != nil {
		ops = *opsP
	}
	stats.resolvedOps = uint64(len(ops))

	// Load the root branch (empty prefix), if any.
	root, hasRoot, err := t.store.GetBranch(nil)
	if err != nil {
		return common.Hash{}, err
	}

	if len(ops) > 0 {
		var rootPtr *BranchData
		var changed bool
		if hasRoot {
			rootPtr = &root
		}
		if t.parallelMinOps > 0 && len(ops) >= t.parallelMinOps {
			rootPtr, changed, err = t.applyRootParallel(rootPtr, ops)
		} else {
			// Every recursive prefix is at most pathLen bytes. Reusing this
			// fold-local path stack avoids one allocation at every trie level;
			// stores consume/copy prefixes synchronously and never retain it.
			var path [pathLen]byte
			rootPtr, changed, err = t.apply(path[:0], 0, rootPtr, ops)
		}
		if err != nil {
			return common.Hash{}, err
		}
		stats.changed = changed
		if changed {
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
	}

	if !hasRoot {
		return common.Hash{}, nil
	}
	return t.rootHash(&root), nil
}

func (t *commitmentTrie) rootHash(root *BranchData) common.Hash {
	if root.childCount() == 1 {
		n := root.onlyChildNibble()
		if root.childKindAt(n) == kindLeaf {
			_, vh := root.leafChildAt(n)
			return vh
		}
	}
	return t.nodeHash(root)
}

// rootHash returns the trie root hash for the root branch. The whole-trie
// singleton case (exactly one leaf child, no hash children at the root) collapses
// to that key's leaf value hash, per the spec.
func rootHash(root *BranchData) common.Hash {
	h := borrowKeccak()
	defer returnKeccak(h)
	return rootHashWithHasher(root, h)
}

func rootHashWithHasher(root *BranchData, h *pooledKeccak) common.Hash {
	if root.childCount() == 1 {
		n := root.onlyChildNibble()
		if root.childKindAt(n) == kindLeaf {
			_, vh := root.leafChildAt(n)
			return vh
		}
	}
	return root.nodeHashWith(h)
}

// buildOps coalesces updates per key (last-writer-wins), resolves each to its
// 64-nibble path and leaf value hash, and returns them sorted by path. Sorting
// makes the in-tree walk order deterministic but does not affect the final
// structure (which is path-keyed).
func buildOps(updates []Update) (*[]op, error) {
	h := borrowKeccak()
	defer returnKeccak(h)
	return buildOpsWithHasher(updates, h)
}

func buildOpsWithHasher(updates []Update, h *pooledKeccak) (*[]op, error) {
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
		opsP := borrowOpsBuf(len(updates))
		ops := *opsP
		for i, u := range updates {
			ops[i] = resolveOpWithHasher(u, h)
		}
		sortOps(ops)
		return opsP, nil
	}

	byKey := make(map[string]Update, len(updates))
	for _, u := range updates {
		byKey[string(u.Key)] = u
	}
	opsP := borrowOpsBuf(len(byKey))
	ops := *opsP
	i := 0
	for _, u := range byKey {
		ops[i] = resolveOpWithHasher(u, h)
		i++
	}
	sortOps(ops)
	return opsP, nil
}

func resolveOp(u Update) op {
	h := borrowKeccak()
	defer returnKeccak(h)
	return resolveOpWithHasher(u, h)
}

func resolveOpWithHasher(u Update, h *pooledKeccak) op {
	// Fold is synchronous and every branch-store implementation consumes or
	// copies leaf keys before Fold returns. Borrowing the input key for that
	// interval avoids one allocation per update; persisted branch encodings do
	// not alias the caller's Update buffers.
	o := op{key: u.Key, delete: u.Delete}
	o.path = keyPathWithHasher(h, u.Key)
	if !u.Delete {
		o.valHash = leafValueHashWithHasher(h, u.Key, u.Value)
	}
	return o
}

// apply processes all ops that pass through the branch at prefix/depth and
// returns the resulting branch (nil if the branch should not exist) plus
// whether its persisted representation changed. The changed bit lets an
// identical leaf update unwind without rewriting every ancestor branch.
//
// branch is the existing node at this prefix (nil if absent). All ops in the
// slice share the prefix path nibbles [0:depth).
func (t *commitmentTrie) apply(prefix []byte, depth int, branch *BranchData, ops []op) (*BranchData, bool, error) {
	if branch == nil {
		branch = &BranchData{}
	}

	// buildOps sorts by the complete path, and the leaf-split path re-sorts the
	// only locally constructed op group before recursing. Therefore every input
	// to apply is already partitioned into contiguous next-nibble runs. Scan those
	// runs in place instead of counting-sorting and copying the ~96-byte op values
	// into a pooled scratch buffer at every trie depth. Filtering deletes may
	// compact a run in place, but it cannot move the following run's boundary.
	changed := false
	for start := 0; start < len(ops); {
		nb := pathNibble(ops[start].path, depth)
		end := start + 1
		for end < len(ops) && pathNibble(ops[end].path, depth) == nb {
			end++
		}
		group := ops[start:end]
		nibbleChanged, err := t.applyNibble(prefix, depth, branch, nb, group)
		if err != nil {
			return nil, false, err
		}
		changed = changed || nibbleChanged
		start = end
	}

	// An emptied branch must not persist. Single-LEAF collapse for non-root
	// branches is enforced by the parent in linkChild; the root keeps its
	// single-LEAF form (the root-hash rule special-cases it), so here we only
	// need to drop fully-empty branches.
	if branch.childCount() == 0 {
		return nil, changed, nil
	}
	return branch, changed, nil
}

// applyNibble applies the op group that descends into nibble nb of the branch at
// prefix/depth, mutating branch in place and reporting whether it changed.
func (t *commitmentTrie) applyNibble(prefix []byte, depth int, branch *BranchData, nb uint8, group []op) (bool, error) {
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
		return false, fmt.Errorf("commitment_tree: unknown child kind %d", branch.childKindAt(nb))
	}
}

// insertIntoEmpty fills an absent slot nb with the surviving puts in group.
func (t *commitmentTrie) insertIntoEmpty(branch *BranchData, nb uint8, childPrefix []byte, childDepth int, group []op) (bool, error) {
	puts := livePutsInPlace(group)
	switch len(puts) {
	case 0:
		// Deletes into an empty slot are no-ops.
		return false, nil
	case 1:
		branch.setLeafChildPath(nb, puts[0].path[:], puts[0].valHash)
		return true, nil
	default:
		// Build a fresh child subtree rooted at childPrefix, borrowing the
		// branch from the pool so the descent doesn't escape to the heap.
		child := borrowEmptyBranch()
		defer returnBranch(child)
		updated, changed, err := t.apply(childPrefix, childDepth, child, puts)
		if err != nil {
			return false, err
		}
		if !changed {
			return false, nil
		}
		if err := t.linkChild(branch, nb, childPrefix, updated); err != nil {
			return false, err
		}
		return true, nil
	}
}

// applyOnLeaf resolves group against an existing leaf child at nb.
func (t *commitmentTrie) applyOnLeaf(branch *BranchData, nb uint8, childPrefix []byte, childDepth int, group []op) (bool, error) {
	existIdentity, existPathOnly, existVH := branch.leafChildIdentityAt(nb)

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
	existing := op{valHash: existVH}
	if existPathOnly {
		copy(existing.path[:], existIdentity)
	} else {
		existing.key = existIdentity
	}
	survivors = append(survivors, existing)
	existingNeedsPath := !existPathOnly

	for _, o := range group {
		// Linear find by raw key for legacy leaves and by the authoritative
		// hashed trie path for shortened leaves.
		idx := -1
		for i := range survivors {
			if sameLeafIdentity(survivors[i], o) {
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
		return true, nil
	case 1:
		// Exactly one survivor → leaf child.
		only := survivors[0]
		if sameLeafIdentity(only, existing) && only.valHash == existVH {
			return false, nil
		}
		if only.path == (common.Hash{}) && len(only.key) != 0 {
			only.path = t.keyPath(only.key)
		}
		branch.setLeafChildPath(nb, only.path[:], only.valHash)
		return true, nil
	default:
		if existingNeedsPath {
			for i := range survivors {
				if len(survivors[i].key) != 0 && bytes.Equal(survivors[i].key, existIdentity) {
					survivors[i].path = t.keyPath(existIdentity)
					break
				}
			}
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
func (t *commitmentTrie) applyLeafSplit(branch *BranchData, nb uint8, childPrefix []byte, childDepth int, survivors []op) (bool, error) {
	bufP := borrowOpsBuf(len(survivors))
	defer returnOpsBuf(bufP)
	buf := *bufP
	copy(buf, survivors)

	// sortOps gives a deterministic traversal so apply's bucket sort is stable.
	sortOps(buf)
	child := borrowEmptyBranch()
	defer returnBranch(child)
	updated, changed, err := t.apply(childPrefix, childDepth, child, buf)
	if err != nil {
		return false, err
	}
	if !changed {
		return false, nil
	}
	if err := t.linkChild(branch, nb, childPrefix, updated); err != nil {
		return false, err
	}
	return true, nil
}

// applyOnHash resolves group against an existing hash child (a child subtree) at
// nb. The child branch is borrowed from branchPool so the per-descent ~800-byte
// BranchData allocation (formerly the #1 alloc source at ~24% of fold heap
// pressure) becomes pool reuse. linkChild consumes the data and never retains
// the pointer past return, so the deferred release is unconditional.
func (t *commitmentTrie) applyOnHash(branch *BranchData, nb uint8, childPrefix []byte, childDepth int, group []op) (bool, error) {
	child := borrowBranch()
	defer returnBranch(child)
	ok, err := t.store.GetBranchInto(childPrefix, child)
	if err != nil {
		return false, err
	}
	if !ok {
		// A missing read may leave the overwrite-only pooled destination
		// untouched. Clear it before returning it to the pool so malformed state
		// cannot extend the lifetime of slices retained by its previous owner.
		*child = BranchData{}
		return false, fmt.Errorf("commitment_tree: missing hash child at prefix %x", childPrefix)
	}
	updated, changed, err := t.apply(childPrefix, childDepth, child, group)
	if err != nil {
		return false, err
	}
	if !changed {
		return false, nil
	}
	// updated is either child (mutated) or nil (subtree collapsed). linkChild
	// handles both. defer returnBranch(child) above releases child after
	// linkChild returns regardless of which case fired.
	if err := t.linkChild(branch, nb, childPrefix, updated); err != nil {
		return false, err
	}
	return true, nil
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
			identity, pathOnly, vh := child.leafChildIdentityAt(cn)
			if pathOnly {
				branch.setLeafChildPath(nb, identity, vh)
			} else {
				path := t.keyPath(identity)
				branch.setLeafChildPath(nb, path[:], vh)
			}
			return nil
		}
		// Single HASH child is a valid (extension-like) node; keep it.
	}
	if err := t.store.PutBranch(childPrefix, *child); err != nil {
		return err
	}
	branch.SetHashChild(nb, t.nodeHash(child))
	return nil
}

// sameLeafIdentity compares resolved fold ops without requiring a full raw key
// in persisted branch leaves. Incoming updates always have both key and path;
// decoded shortened leaves carry only path. Legacy/plain leaves retain raw-key
// equality until the containing branch is next rewritten.
func sameLeafIdentity(a, b op) bool {
	if len(a.key) != 0 && len(b.key) != 0 {
		return bytes.Equal(a.key, b.key)
	}
	return a.path == b.path
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

type opSorter struct {
	ops []op
}

func (s *opSorter) Len() int { return len(s.ops) }

func (s *opSorter) Less(i, j int) bool {
	if cmp := bytes.Compare(s.ops[i].path[:], s.ops[j].path[:]); cmp != 0 {
		return cmp < 0
	}
	return bytes.Compare(s.ops[i].key, s.ops[j].key) < 0
}

func (s *opSorter) Swap(i, j int) { s.ops[i], s.ops[j] = s.ops[j], s.ops[i] }

var opSorterPool = sync.Pool{
	New: func() any { return new(opSorter) },
}

func sortOps(ops []op) {
	sorter := opSorterPool.Get().(*opSorter)
	sorter.ops = ops
	sort.Sort(sorter)
	sorter.ops = nil
	opSorterPool.Put(sorter)
}

func pathNibble(path common.Hash, depth int) uint8 {
	b := path[depth>>1]
	if depth&1 == 0 {
		return b >> 4
	}
	return b & 0x0f
}

// appendNibble extends the fold-local path stack with nb. Fold and each
// parallel root worker provide pathLen capacity, so the recursive descent
// reuses one backing array. The fallback keeps direct internal callers safe.
// branchStore methods must consume or copy prefixes synchronously; every
// implementation in this package does so.
func appendNibble(prefix []byte, nb uint8) []byte {
	return append(prefix, nb)
}

// keyPath hashes lenPrefixed(key). pathNibble exposes its pathLen high-first
// nibbles without materializing a second expanded array.
func keyPath(key []byte) common.Hash {
	h := borrowKeccak()
	defer returnKeccak(h)
	return keyPathWithHasher(h, key)
}

func keyPathWithHasher(h *pooledKeccak, key []byte) common.Hash {
	h.Reset()
	writeLen8Prefixed(h, key)
	return readKeccakHash(h)
}

// leafValueHash is the value hash of a key: keccak256(0x00 || lenPrefixed(key) ||
// lenPrefixed(value)).
func leafValueHash(key, value []byte) common.Hash {
	h := borrowKeccak()
	defer returnKeccak(h)
	return leafValueHashWithHasher(h, key, value)
}

func leafValueHashWithHasher(h *pooledKeccak, key, value []byte) common.Hash {
	h.Reset()
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
