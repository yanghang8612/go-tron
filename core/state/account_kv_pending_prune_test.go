package state

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"testing"
	"unsafe"

	ethrawdb "github.com/ethereum/go-ethereum/core/rawdb"
	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/blockbuffer"
	"github.com/tronprotocol/go-tron/core/rawdb"
	statedomains "github.com/tronprotocol/go-tron/core/state/domains"
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

var benchmarkAccountKVLatestPending accountKVLatestPending

func BenchmarkAccountKVLatestPendingOverlay(b *testing.B) {
	owner := pruneTestOwner(1)
	domain := kvdomains.ContractStorage
	logicalKey := bytes.Repeat([]byte{0x7f}, 32)
	value := bytes.Repeat([]byte{0x42}, 32)

	b.Run("overwrite", func(b *testing.B) {
		writer := &accountKVLatestBatch{}
		b.ReportAllocs()
		for range b.N {
			writer.rememberPut(owner, 7, domain, logicalKey, value)
		}
	})

	b.Run("lookup", func(b *testing.B) {
		writer := &accountKVLatestBatch{}
		writer.rememberPut(owner, 7, domain, logicalKey, value)
		b.ReportAllocs()
		b.ResetTimer()
		for range b.N {
			benchmarkAccountKVLatestPending = writer.pending[pruneKVKey(owner, domain, logicalKey)]
		}
	})
}

func BenchmarkAccountKVLatestTemporalMutationBatch(b *testing.B) {
	const mutationsPerBatch = 256
	owner := pruneTestOwner(1)
	keys := make([][]byte, mutationsPerBatch)
	values := make([][]byte, mutationsPerBatch)
	encodedValues := make([][]byte, mutationsPerBatch)
	for i := range mutationsPerBatch {
		keys[i] = bytes.Repeat([]byte{byte(i)}, 32)
		values[i] = bytes.Repeat([]byte{byte(i + 1)}, 32)
		encodedValues[i] = rawdb.EncodeStateKVLatestValue(values[i])
	}
	for _, mode := range []string{"defensive", "owned", "encoded-owned"} {
		b.Run(mode, func(b *testing.B) {
			buf := blockbuffer.New(ethrawdb.NewMemoryDatabase())
			buf.BeginBlock(pruneBlockHash(1), 1)
			writer := newAccountKVLatestDomainBatch(buf, zeroGeneration, nil, nil)
			tx := statedomains.NewSharedDomainTx(statedomains.SharedDomainTxConfig{Writer: writer})
			defer tx.Close()
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				for i := range mutationsPerBatch {
					var err error
					switch mode {
					case "owned":
						err = tx.DomainPutOwned(owner, kvdomains.ContractStorage, keys[i], values[i])
					case "encoded-owned":
						err = tx.DomainPutEncodedOwned(owner, kvdomains.ContractStorage, keys[i], values[i], encodedValues[i])
					default:
						err = tx.DomainPut(owner, kvdomains.ContractStorage, keys[i], values[i])
					}
					if err != nil {
						b.Fatal(err)
					}
				}
				if err := tx.Flush(context.Background()); err != nil {
					b.Fatal(err)
				}
				if err := writer.flush(); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func TestAccountKVLatestPendingStructuredKeysPreserveIdentity(t *testing.T) {
	writer := &accountKVLatestBatch{}
	owner := pruneTestOwner(1)
	alias := owner
	alias[0] = 0xa0 // rooted state identifies accounts by the 20-byte AccountID
	domain := kvdomains.ContractStorage
	logicalKey := []byte("slot/original")
	originalKey := append([]byte(nil), logicalKey...)
	value := []byte("value/original")
	originalValue := append([]byte(nil), value...)

	writer.rememberPut(owner, 7, domain, logicalKey, value)
	writer.rememberAccountLatestPut(owner, value)
	writer.rememberKVGenerationPut(owner, 7)
	logicalKey[0] = 'X'
	value[0] = 'X'

	if got, ok, err := writer.readLatest(alias, 7, domain, originalKey); err != nil || !ok || !bytes.Equal(got, originalValue) {
		t.Fatalf("readLatest via AccountID alias = %q ok=%v err=%v, want %q", got, ok, err, originalValue)
	}
	if _, ok, err := writer.readLatest(owner, 7, domain, logicalKey); err != nil || ok {
		t.Fatalf("readLatest via mutated caller key ok=%v err=%v, want absent", ok, err)
	}
	if got, ok, err := writer.readAccountLatest(alias); err != nil || !ok || !bytes.Equal(got, originalValue) {
		t.Fatalf("readAccountLatest via AccountID alias = %q ok=%v err=%v, want %q", got, ok, err, originalValue)
	}
	if got, ok, err := writer.readKVGeneration(alias); err != nil || !ok || got != 7 {
		t.Fatalf("readKVGeneration via AccountID alias = %d ok=%v err=%v, want 7", got, ok, err)
	}
}

func TestAccountKVLatestCommitmentReadBorrowsImmutablePendingValue(t *testing.T) {
	writer := &accountKVLatestBatch{}
	owner := pruneTestOwner(1)
	value := []byte("immutable-account-envelope")
	writer.rememberAccountLatestPutOwned(owner, value)

	ordinary, ok, err := writer.readAccountLatest(owner)
	if err != nil || !ok {
		t.Fatalf("readAccountLatest = (%q,%v,%v)", ordinary, ok, err)
	}
	if &ordinary[0] == &value[0] {
		t.Fatal("ordinary account read exposed the pending value backing array")
	}
	ordinary[0] ^= 0xff
	if !bytes.Equal(writer.accountPending[owner.AccountID()].value, value) {
		t.Fatal("mutating ordinary account read changed the pending value")
	}

	borrowed, ok, err := writer.readAccountLatestForCommitment(owner)
	if err != nil || !ok {
		t.Fatalf("readAccountLatestForCommitment = (%q,%v,%v)", borrowed, ok, err)
	}
	if &borrowed[0] != &value[0] {
		t.Fatal("commitment account read copied its immutable pending value")
	}
}

func TestAccountKVLatestOwnedPutRetainsTransferredPendingValue(t *testing.T) {
	writer := newAccountKVLatestDomainBatch(ethrawdb.NewMemoryDatabase(), zeroGeneration, nil, nil)
	owner := pruneTestOwner(2)
	key := []byte("owned-pending-key")
	value := []byte("owned-pending-value")
	if err := writer.DomainPutOwned(owner, kvdomains.SystemReward, key, value); err != nil {
		t.Fatal(err)
	}
	pending := writer.pending[pruneKVKey(owner, kvdomains.SystemReward, key)]
	if &pending.value[0] != &value[0] {
		t.Fatal("owned domain put copied pending value")
	}
	for mapKey := range writer.pending {
		if unsafe.StringData(mapKey.logicalKey) != unsafe.SliceData(key) {
			t.Fatal("owned domain put copied pending logical key")
		}
	}
}

func TestAccountKVLatestOwnedDeleteRetainsTransferredPendingKey(t *testing.T) {
	writer := newAccountKVLatestDomainBatch(ethrawdb.NewMemoryDatabase(), zeroGeneration, nil, nil)
	owner := pruneTestOwner(3)
	key := []byte("owned-delete-key")
	if err := writer.DomainDelOwned(owner, kvdomains.SystemReward, key); err != nil {
		t.Fatal(err)
	}
	for mapKey, pending := range writer.pending {
		if !pending.deleted {
			t.Fatal("owned delete did not record tombstone")
		}
		if unsafe.StringData(mapKey.logicalKey) != unsafe.SliceData(key) {
			t.Fatal("owned domain delete copied pending logical key")
		}
	}
}

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

func pruneKVKey(owner tcommon.Address, domain kvdomains.KVDomain, key []byte) accountKVLatestPendingMapKey {
	return accountKVLatestPendingKey(owner, 0, domain, key)
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

// TestAccountKVLatestBatchDurabilityPruneGuardsLostWrite pins the deep-async
// (GTRON_ASYNC_COMMIT_DEPTH>2) lost-write guard. It reproduces the loss engine:
// an overlay entry tagged with block 1 whose buffer op actually binds to layer 2
// (the commitBlock==op-layer assumption broken). A flush at cutoff 1 promotes and
// applies only layer 1, so the entry's op is NOT yet durable. With the durability
// gate (deepAsync) prunePending must KEEP the entry so read-your-writes still
// returns the value; the fast tag-based prune drops it and the value is lost —
// which is exactly the Nile 45,490,766 symptom (a committed SSTORE that vanished
// from the latest domain). Running both modes proves the gate is what prevents
// the loss, not incidental test setup.
func TestAccountKVLatestBatchDurabilityPruneGuardsLostWrite(t *testing.T) {
	domain := kvdomains.SystemDelegation
	key := []byte("k")
	o2 := pruneTestOwner(2)

	// kept reports, for the over-prune scenario, whether each overlay type still
	// serves o2's value/generation after the flush. All three overlays (KV,
	// account-latest, generation) tag with commitBlock and prune together, so the
	// durability gate must protect all three.
	type kept struct{ kv, acct, gen bool }
	run := func(deepAsync bool) kept {
		writer, buf := newPruneTestWriter(t, 8)
		writer.deepAsync = deepAsync

		// Block 1: a normal entry — tag 1, op binds to layer 1.
		buf.BeginBlock(pruneBlockHash(1), 1)
		writer.commitBlock = 1
		if err := writer.DomainPut(pruneTestOwner(1), domain, key, pruneTestVal(1)); err != nil {
			t.Fatalf("put o1: %v", err)
		}
		// Block 2 is now the active layer, but the writer keeps tagging with
		// commitBlock 1 — the invariant break. o2's ops bind to layer 2 while their
		// overlay entries are tagged 1.
		buf.BeginBlock(pruneBlockHash(2), 2)
		if err := writer.DomainPut(o2, domain, key, pruneTestVal(2)); err != nil {
			t.Fatalf("put o2 kv: %v", err)
		}
		if err := writer.writeAccountLatest(o2, pruneTestVal(2)); err != nil {
			t.Fatalf("put o2 account: %v", err)
		}
		if err := writer.writeKVGeneration(o2, 99); err != nil {
			t.Fatalf("put o2 generation: %v", err)
		}
		// Commit only layer 1; layer 2 stays in-flight. WriteUpTo(1) applies o1's
		// op (layer 1) but NOT o2's (layer 2 > cutoff). prunePending then sees the
		// o2 entries tagged 1 <= cutoff 1.
		buf.CommitBlock()
		if err := writer.flushUpTo(1); err != nil {
			t.Fatalf("flushUpTo(1): %v", err)
		}
		kvVal, kvOK, err := writer.readLatest(o2, 0, domain, key)
		if err != nil {
			t.Fatalf("readLatest(o2): %v", err)
		}
		acctVal, acctOK, err := writer.readAccountLatest(o2)
		if err != nil {
			t.Fatalf("readAccountLatest(o2): %v", err)
		}
		genVal, genOK, err := writer.readKVGeneration(o2)
		if err != nil {
			t.Fatalf("readKVGeneration(o2): %v", err)
		}
		return kept{
			kv:   kvOK && string(kvVal) == string(pruneTestVal(2)),
			acct: acctOK && string(acctVal) == string(pruneTestVal(2)),
			gen:  genOK && genVal == 99,
		}
	}

	// With the durability gate: o2's ops are not durable (layer 2 unflushed), so
	// all three overlay entries are retained and still read.
	if got := run(true); !got.kv || !got.acct || !got.gen {
		t.Fatalf("deepAsync prune lost a write: kept=%+v, want all true", got)
	}
	// Without it (fast tag-based prune): all three are dropped and lost — confirming
	// the test exercises the real loss engine across all overlays and the gate is
	// the fix.
	if got := run(false); got.kv || got.acct || got.gen {
		t.Fatalf("fast tag-based prune unexpectedly retained a write: kept=%+v, want all false", got)
	}
}

// TestAccountKVLatestBatchDurabilityPruneGuardsLostDelete is the tombstone twin
// of the lost-write guard: a delete tombstone tagged with block 1 whose delete op
// binds to layer 2. Flushing at cutoff 1 does not apply the delete, so the key is
// still present in the underlying store. The durability gate must KEEP the
// tombstone (so reads see the deletion); the fast prune drops it and the read
// falls through to the stale pre-delete value (a lost delete).
func TestAccountKVLatestBatchDurabilityPruneGuardsLostDelete(t *testing.T) {
	domain := kvdomains.SystemDelegation
	key := []byte("k")
	o := pruneTestOwner(1)

	run := func(deepAsync bool) (string, bool) {
		writer, buf := newPruneTestWriter(t, 8)
		writer.deepAsync = deepAsync

		// Block 1: write the key and make it durable (commit + flush layer 1). Its
		// put overlay entry is pruned here (durable), leaving the value on disk.
		buf.BeginBlock(pruneBlockHash(1), 1)
		writer.commitBlock = 1
		if err := writer.DomainPut(o, domain, key, pruneTestVal(1)); err != nil {
			t.Fatalf("put: %v", err)
		}
		buf.CommitBlock()
		if err := writer.flushUpTo(1); err != nil {
			t.Fatalf("flushUpTo(1): %v", err)
		}
		// Block 2 active; tag the delete with block 1 while its op binds to layer 2
		// — the invariant break for a tombstone. layer 2 is left in-flight.
		buf.BeginBlock(pruneBlockHash(2), 2)
		if err := writer.DomainDel(o, domain, key); err != nil {
			t.Fatalf("del: %v", err)
		}
		// Flush at cutoff 1: the delete op (layer 2) is NOT applied, but the
		// tombstone is tagged 1 <= 1, so it is a prune candidate.
		if err := writer.flushUpTo(1); err != nil {
			t.Fatalf("flushUpTo(1) post-delete: %v", err)
		}
		got, ok, err := writer.readLatest(o, 0, domain, key)
		if err != nil {
			t.Fatalf("readLatest: %v", err)
		}
		return string(got), ok
	}

	// With the gate: the delete is not durable, so the tombstone is kept → the key
	// reads as absent.
	if _, ok := run(true); ok {
		t.Fatal("deepAsync prune lost the delete: key reads as present, want deleted")
	}
	// Without it: the tombstone is dropped and the read falls through to the stale
	// pre-delete value — confirming the lost-delete engine and the gate's effect.
	if got, ok := run(false); !ok || got != string(pruneTestVal(1)) {
		t.Fatalf("fast prune unexpectedly hid the stale value: got %q ok=%v, want stale %q present", got, ok, pruneTestVal(1))
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
	if _, _, err := sdb.CommitWithStatsOptionsInScope(scope, CommitOptions{BlockNumber: 7, DeepAsync: true}); err != nil {
		t.Fatalf("scoped commit: %v", err)
	}
	if scope.latestWriter.commitBlock != 7 {
		t.Fatalf("scope latest writer commitBlock = %d, want 7 (CommitOptions.BlockNumber must thread through)", scope.latestWriter.commitBlock)
	}
	if !scope.latestWriter.deepAsync {
		t.Fatal("scope latest writer deepAsync = false, want true (CommitOptions.DeepAsync must thread through)")
	}
}
