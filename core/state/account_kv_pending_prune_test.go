package state

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"testing"

	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/blockbuffer"
	"github.com/tronprotocol/go-tron/core/state/kvdomains"
	corepb "github.com/tronprotocol/go-tron/proto/core"
)

// These tests pin the fix for the deep-async-commit (GTRON_ASYNC_COMMIT_DEPTH>2)
// read-your-writes overlay leak in accountKVLatestBatch: the three pending maps
// (pending / accountPending / generationPending) used to be cleared only when a
// layer-batch flush drained to zero remaining ops. Under a deep pipeline there
// are always in-flight ops, so the maps accumulated every put for the whole
// session. The fix tags each overlay entry with the block number that produced
// it and prunes entries whose puts are now durable in the buffer's committed
// layers after each partial flush.

func pruneBlockHash(n int) tcommon.Hash {
	var h tcommon.Hash
	binary.BigEndian.PutUint64(h[:8], uint64(n))
	return h
}

func pruneTestOwner(n int) tcommon.Address {
	var a tcommon.Address
	a[0] = 0x41
	binary.BigEndian.PutUint32(a[16:], uint32(n))
	return a
}

func pruneTestVal(n int) []byte {
	return []byte(fmt.Sprintf("val-%d", n))
}

func zeroGeneration(tcommon.Address) (uint64, error) { return 0, nil }

// newPruneTestWriter builds a latest-domain writer backed by a real block buffer
// so flushUpTo/flushCommitted exercise the genuine WriteUpTo/WriteCommitted/
// NewestCommittedNumber layer-batch path.
func newPruneTestWriter(t *testing.T, maxInflight int) (*accountKVLatestBatch, *blockbuffer.Buffer) {
	t.Helper()
	sdb := newTestStateDB(t)
	buf := blockbuffer.New(sdb.db.DiskDB())
	buf.SetMaxInflight(maxInflight)
	writer := newAccountKVLatestDomainBatch(buf, zeroGeneration, nil, nil)
	return writer, buf
}

func pruneKVKey(owner tcommon.Address, domain kvdomains.KVDomain, key []byte) string {
	return string(accountKVLatestPendingKey(owner, 0, domain, key))
}

// TestAccountKVLatestBatchBoundsPendingUnderDeepPipeline drives a deep commit
// pipeline (depth in-flight layers) across many blocks and asserts the overlay
// maps stay bounded by the in-flight window instead of growing with total
// blocks. This FAILS on the pre-fix code (pending grows to total blocks).
func TestAccountKVLatestBatchBoundsPendingUnderDeepPipeline(t *testing.T) {
	const (
		depth = 4
		total = 240
	)
	writer, buf := newPruneTestWriter(t, depth)
	domain := kvdomains.SystemDelegation
	key := []byte("k")

	inflight := 0
	committed := uint64(0)
	for n := 1; n <= total; n++ {
		for inflight >= depth {
			buf.CommitBlock()
			inflight--
			committed++
			if err := writer.flushUpTo(committed); err != nil {
				t.Fatalf("flushUpTo(%d): %v", committed, err)
			}
		}
		buf.BeginBlock(pruneBlockHash(n), uint64(n))
		inflight++
		writer.commitBlock = uint64(n)
		owner := pruneTestOwner(n)
		if err := writer.DomainPut(owner, domain, key, pruneTestVal(n)); err != nil {
			t.Fatalf("DomainPut block %d: %v", n, err)
		}
		if err := writer.writeAccountLatest(owner, pruneTestVal(n)); err != nil {
			t.Fatalf("writeAccountLatest block %d: %v", n, err)
		}
		if err := writer.writeKVGeneration(owner, uint64(n)); err != nil {
			t.Fatalf("writeKVGeneration block %d: %v", n, err)
		}
	}

	// At loop end exactly `depth` blocks remain in flight; only their entries
	// should survive in each overlay map.
	if got := len(writer.pending); got > depth {
		t.Fatalf("KV pending map size = %d after %d blocks, want <= %d (in-flight window); overlay is leaking", got, total, depth)
	}
	if got := len(writer.accountPending); got > depth {
		t.Fatalf("account pending map size = %d after %d blocks, want <= %d", got, total, depth)
	}
	if got := len(writer.generationPending); got > depth {
		t.Fatalf("generation pending map size = %d after %d blocks, want <= %d", got, total, depth)
	}
}

// TestAccountKVLatestBatchPruneIsInclusiveOfCutoff pins that an entry whose
// tagged block number exactly equals the flush cutoff is pruned (its put is
// durable once its layer is committed and flushed up to that number). Guards the
// <= vs < boundary of the prune predicate.
func TestAccountKVLatestBatchPruneIsInclusiveOfCutoff(t *testing.T) {
	writer, buf := newPruneTestWriter(t, 8)
	domain := kvdomains.SystemDelegation
	key := []byte("k")

	o1 := pruneTestOwner(1)
	buf.BeginBlock(pruneBlockHash(1), 1)
	writer.commitBlock = 1
	if err := writer.DomainPut(o1, domain, key, pruneTestVal(1)); err != nil {
		t.Fatalf("put block 1: %v", err)
	}

	o2 := pruneTestOwner(2)
	buf.BeginBlock(pruneBlockHash(2), 2)
	writer.commitBlock = 2
	if err := writer.DomainPut(o2, domain, key, pruneTestVal(2)); err != nil {
		t.Fatalf("put block 2: %v", err)
	}

	buf.CommitBlock() // promote block 1
	if err := writer.flushUpTo(1); err != nil {
		t.Fatalf("flushUpTo(1): %v", err)
	}

	if _, ok := writer.pending[pruneKVKey(o1, domain, key)]; ok {
		t.Fatal("entry tagged number==cutoff was not pruned; prune must be inclusive of cutoff")
	}
	if _, ok := writer.pending[pruneKVKey(o2, domain, key)]; !ok {
		t.Fatal("entry tagged number>cutoff was pruned; prune must keep in-flight entries")
	}

	// Byte-identical: block 1 now reads through the committed buffer layer,
	// block 2 still reads from the retained overlay entry.
	if got, ok, err := writer.readLatest(o1, 0, domain, key); err != nil || !ok || !bytes.Equal(got, pruneTestVal(1)) {
		t.Fatalf("readLatest(block1) after prune = %q ok=%v err=%v, want %q", got, ok, err, pruneTestVal(1))
	}
	if got, ok, err := writer.readLatest(o2, 0, domain, key); err != nil || !ok || !bytes.Equal(got, pruneTestVal(2)) {
		t.Fatalf("readLatest(block2) = %q ok=%v err=%v, want %q", got, ok, err, pruneTestVal(2))
	}
}

// TestAccountKVLatestBatchPruneByteIdentical proves pruning never changes a
// read. A mirror map records the ground-truth current value of every owner; at
// the end of a deep-pipeline run every readLatest / readAccountLatest /
// readKVGeneration / iterateLatestPrefix must match the mirror exactly. An
// over-aggressive prune (dropping an entry whose put is not yet durable) would
// make a read fall through to the buffer and miss, diverging from the mirror.
func TestAccountKVLatestBatchPruneByteIdentical(t *testing.T) {
	const (
		depth = 4
		total = 90
	)
	writer, buf := newPruneTestWriter(t, depth)
	domain := kvdomains.SystemDelegation
	key := []byte("k")

	type want struct {
		val []byte
		gen uint64
	}
	mirror := make(map[int]want)

	inflight := 0
	committed := uint64(0)
	for n := 1; n <= total; n++ {
		for inflight >= depth {
			buf.CommitBlock()
			inflight--
			committed++
			if err := writer.flushUpTo(committed); err != nil {
				t.Fatalf("flushUpTo(%d): %v", committed, err)
			}
		}
		buf.BeginBlock(pruneBlockHash(n), uint64(n))
		inflight++
		writer.commitBlock = uint64(n)
		owner := pruneTestOwner(n)
		if err := writer.DomainPut(owner, domain, key, pruneTestVal(n)); err != nil {
			t.Fatalf("DomainPut block %d: %v", n, err)
		}
		if err := writer.writeAccountLatest(owner, pruneTestVal(n)); err != nil {
			t.Fatalf("writeAccountLatest block %d: %v", n, err)
		}
		if err := writer.writeKVGeneration(owner, uint64(n)); err != nil {
			t.Fatalf("writeKVGeneration block %d: %v", n, err)
		}
		mirror[n] = want{val: pruneTestVal(n), gen: uint64(n)}
	}

	for n := 1; n <= total; n++ {
		owner := pruneTestOwner(n)
		exp := mirror[n]
		got, ok, err := writer.readLatest(owner, 0, domain, key)
		if err != nil || !ok || !bytes.Equal(got, exp.val) {
			t.Fatalf("readLatest(block %d) = %q ok=%v err=%v, want %q", n, got, ok, err, exp.val)
		}
		gotAcc, ok, err := writer.readAccountLatest(owner)
		if err != nil || !ok || !bytes.Equal(gotAcc, exp.val) {
			t.Fatalf("readAccountLatest(block %d) = %q ok=%v err=%v, want %q", n, gotAcc, ok, err, exp.val)
		}
		gotGen, ok, err := writer.readKVGeneration(owner)
		if err != nil || !ok || gotGen != exp.gen {
			t.Fatalf("readKVGeneration(block %d) = %d ok=%v err=%v, want %d", n, gotGen, ok, err, exp.gen)
		}
		// Prefix iteration must surface the single key for this owner with the
		// correct value whether it lives in the overlay or a committed layer.
		var iterated [][2][]byte
		if err := writer.iterateLatestPrefix(owner, 0, domain, nil, func(k, v []byte) (bool, error) {
			iterated = append(iterated, [2][]byte{append([]byte(nil), k...), append([]byte(nil), v...)})
			return true, nil
		}); err != nil {
			t.Fatalf("iterateLatestPrefix(block %d): %v", n, err)
		}
		if len(iterated) != 1 || !bytes.Equal(iterated[0][0], key) || !bytes.Equal(iterated[0][1], exp.val) {
			t.Fatalf("iterateLatestPrefix(block %d) = %v, want single %q=%q", n, iterated, key, exp.val)
		}
	}
}

// TestAccountKVLatestBatchPruneRetainsUncommitted asserts that when nothing is
// committed (no CommitBlock), a flushUpTo cannot prune: every in-flight entry
// must be retained and remain readable from the overlay.
func TestAccountKVLatestBatchPruneRetainsUncommitted(t *testing.T) {
	const blocks = 6
	writer, buf := newPruneTestWriter(t, blocks)
	domain := kvdomains.SystemDelegation
	key := []byte("k")

	for n := 1; n <= blocks; n++ {
		buf.BeginBlock(pruneBlockHash(n), uint64(n))
		writer.commitBlock = uint64(n)
		if err := writer.DomainPut(pruneTestOwner(n), domain, key, pruneTestVal(n)); err != nil {
			t.Fatalf("put block %d: %v", n, err)
		}
	}

	// Nothing is committed yet; a flush at any cutoff must keep every entry.
	if err := writer.flushUpTo(blocks); err != nil {
		t.Fatalf("flushUpTo(%d): %v", blocks, err)
	}
	if got := len(writer.pending); got != blocks {
		t.Fatalf("pending size = %d after flush with no committed layers, want %d (all retained)", got, blocks)
	}
	for n := 1; n <= blocks; n++ {
		if got, ok, err := writer.readLatest(pruneTestOwner(n), 0, domain, key); err != nil || !ok || !bytes.Equal(got, pruneTestVal(n)) {
			t.Fatalf("readLatest(block %d) = %q ok=%v err=%v, want %q", n, got, ok, err, pruneTestVal(n))
		}
	}
}

// TestAccountKVLatestBatchPruneRewindCutoffKeepsAbove asserts a cutoff that
// rewinds (decreases) never removes entries above it: prune is monotonic in the
// entry's tagged block number and idempotent for a repeated/lower cutoff.
func TestAccountKVLatestBatchPruneRewindCutoffKeepsAbove(t *testing.T) {
	writer, buf := newPruneTestWriter(t, 8)
	domain := kvdomains.SystemDelegation
	key := []byte("k")

	for n := 1; n <= 4; n++ {
		buf.BeginBlock(pruneBlockHash(n), uint64(n))
		writer.commitBlock = uint64(n)
		if err := writer.DomainPut(pruneTestOwner(n), domain, key, pruneTestVal(n)); err != nil {
			t.Fatalf("put block %d: %v", n, err)
		}
	}
	// Commit blocks 1 and 2, flush up to 2.
	buf.CommitBlock()
	buf.CommitBlock()
	if err := writer.flushUpTo(2); err != nil {
		t.Fatalf("flushUpTo(2): %v", err)
	}
	if _, ok := writer.pending[pruneKVKey(pruneTestOwner(1), domain, key)]; ok {
		t.Fatal("block 1 entry not pruned at cutoff 2")
	}
	if _, ok := writer.pending[pruneKVKey(pruneTestOwner(3), domain, key)]; !ok {
		t.Fatal("block 3 entry incorrectly pruned at cutoff 2")
	}

	// A rewound/lower cutoff must not remove anything above it (idempotent).
	if err := writer.flushUpTo(1); err != nil {
		t.Fatalf("flushUpTo(1) after rewind: %v", err)
	}
	for _, n := range []int{3, 4} {
		if _, ok := writer.pending[pruneKVKey(pruneTestOwner(n), domain, key)]; !ok {
			t.Fatalf("block %d entry removed by a rewound cutoff", n)
		}
		if got, ok, err := writer.readLatest(pruneTestOwner(n), 0, domain, key); err != nil || !ok || !bytes.Equal(got, pruneTestVal(n)) {
			t.Fatalf("readLatest(block %d) after rewind = %q ok=%v err=%v, want %q", n, got, ok, err, pruneTestVal(n))
		}
	}
}

// TestAccountKVLatestBatchFlushCommittedPrunes covers the second flush path:
// flushCommitted applies every committed layer's ops, so it must prune every
// overlay entry up to the newest committed block while keeping in-flight ones.
// FAILS on the pre-fix code (committed entries leak because remaining != 0).
func TestAccountKVLatestBatchFlushCommittedPrunes(t *testing.T) {
	const (
		begun     = 10
		toCommit  = 8
		maxInflit = 16
	)
	writer, buf := newPruneTestWriter(t, maxInflit)
	domain := kvdomains.SystemDelegation
	key := []byte("k")

	for n := 1; n <= begun; n++ {
		buf.BeginBlock(pruneBlockHash(n), uint64(n))
		writer.commitBlock = uint64(n)
		owner := pruneTestOwner(n)
		if err := writer.DomainPut(owner, domain, key, pruneTestVal(n)); err != nil {
			t.Fatalf("DomainPut block %d: %v", n, err)
		}
		if err := writer.writeAccountLatest(owner, pruneTestVal(n)); err != nil {
			t.Fatalf("writeAccountLatest block %d: %v", n, err)
		}
		if err := writer.writeKVGeneration(owner, uint64(n)); err != nil {
			t.Fatalf("writeKVGeneration block %d: %v", n, err)
		}
	}
	for i := 0; i < toCommit; i++ {
		buf.CommitBlock()
	}

	if err := writer.flushCommitted(true); err != nil {
		t.Fatalf("flushCommitted: %v", err)
	}

	wantRemaining := begun - toCommit
	if got := len(writer.pending); got != wantRemaining {
		t.Fatalf("KV pending after flushCommitted = %d, want %d (in-flight only)", got, wantRemaining)
	}
	if got := len(writer.accountPending); got != wantRemaining {
		t.Fatalf("account pending after flushCommitted = %d, want %d", got, wantRemaining)
	}
	if got := len(writer.generationPending); got != wantRemaining {
		t.Fatalf("generation pending after flushCommitted = %d, want %d", got, wantRemaining)
	}

	// Byte-identical: committed owners read through the buffer, in-flight owners
	// read from the retained overlay.
	for n := 1; n <= begun; n++ {
		owner := pruneTestOwner(n)
		if got, ok, err := writer.readLatest(owner, 0, domain, key); err != nil || !ok || !bytes.Equal(got, pruneTestVal(n)) {
			t.Fatalf("readLatest(block %d) after flushCommitted = %q ok=%v err=%v, want %q", n, got, ok, err, pruneTestVal(n))
		}
		if got, ok, err := writer.readAccountLatest(owner); err != nil || !ok || !bytes.Equal(got, pruneTestVal(n)) {
			t.Fatalf("readAccountLatest(block %d) = %q ok=%v err=%v, want %q", n, got, ok, err, pruneTestVal(n))
		}
		if got, ok, err := writer.readKVGeneration(owner); err != nil || !ok || got != uint64(n) {
			t.Fatalf("readKVGeneration(block %d) = %d ok=%v err=%v, want %d", n, got, ok, err, n)
		}
	}
}

// TestCommitScopeThreadsBlockNumberToLatestWriter pins the production threading:
// CommitOptions.BlockNumber must reach the scope's reused latest writer so its
// overlay entries are tagged with the committing block (and thus prunable).
func TestCommitScopeThreadsBlockNumberToLatestWriter(t *testing.T) {
	sdb := newTestStateDB(t)
	addr := testAddr(0x77)
	sdb.CreateAccount(addr, corepb.AccountType_Normal)

	scope := sdb.NewCommitScope()
	defer scope.Close()

	if err := sdb.SetAccountKV(addr, kvdomains.SystemDynamicProperty, []byte("k"), []byte("v")); err != nil {
		t.Fatalf("set kv: %v", err)
	}
	if _, _, err := sdb.CommitWithStatsOptionsInScope(scope, CommitOptions{BlockNumber: 7}); err != nil {
		t.Fatalf("scoped commit: %v", err)
	}
	if scope.latestWriter.commitBlock != 7 {
		t.Fatalf("scope latest writer commitBlock = %d, want 7 (CommitOptions.BlockNumber must thread through)", scope.latestWriter.commitBlock)
	}
}
