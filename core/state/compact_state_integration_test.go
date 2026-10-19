package state

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"strings"
	"testing"

	"github.com/ethereum/go-ethereum/ethdb"
	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/state/domains"
	"github.com/tronprotocol/go-tron/core/state/kvdomains"
	"github.com/tronprotocol/go-tron/core/state/snapshots"
	corepb "github.com/tronprotocol/go-tron/proto/core"
)

func assertCompactFullMuHash(t *testing.T, disk ethdb.KeyValueStore) rawdb.StateCommitmentHead {
	t.Helper()
	head, ok, err := rawdb.ReadStateCommitmentHead(disk)
	if err != nil || !ok {
		t.Fatal(ok, err)
	}
	full, err := snapshots.RebuildLatestMuHash(disk)
	if err != nil || tcommon.Hash(full.Digest) != head.Root || full.State != head.State {
		t.Fatalf("full MuHash mismatch at %d: %v", head.BlockNum, err)
	}
	return head
}

// This fixture exercises physical encoding and logical APIs together. The root
// oracle enumerates and folds the complete physical set after every mutation.
func TestCompactStateHistoryUnwindSnapshotAndContinue(t *testing.T) {
	disk := rawdb.NewMemoryDatabase()
	defer disk.Close()
	// A raw trie hash can share the compact namespace without being a source.
	trieHash := append(rawdb.StateCommitmentSourcePrefixes()[2], make([]byte, 28)...)
	if err := disk.Put(trieHash, []byte("unrelated hash-addressed node")); err != nil {
		t.Fatal(err)
	}
	db := NewDatabase(rawdb.WrapKeyValueStore(disk))
	defer db.Close()
	s, err := New(tcommon.Hash{}, db)
	if err != nil {
		t.Fatal(err)
	}
	owner := testAddr(0xb3)
	slot := tcommon.Hash{31: 7}
	a, b := bytes.Repeat([]byte{0x71}, 32), bytes.Repeat([]byte{0x82}, 32)
	rootFor := func(h uint64) []byte {
		if h == 3 || h == 4 {
			return b
		}
		return a
	}
	wordFor := func(h uint64) tcommon.Hash {
		switch h {
		case 1:
			return tcommon.Hash{31: 7}
		case 2:
			return tcommon.Hash{28: 1}
		case 3:
			return tcommon.Hash{}
		case 4:
			return tcommon.Hash{0: 1, 31: 2}
		default:
			return tcommon.Hash{31: 9}
		}
	}
	commit := func(s *StateDB, disk ethdb.KeyValueStore, h uint64) tcommon.Hash {
		t.Helper()
		hash := tcommon.Hash{byte(h)}
		s.BeginDomainChangeJournalCapture(disk, h, hash, h, h)
		mark := s.DomainChangeJournalMark()
		if h == 1 {
			s.CreateAccount(owner, corepb.AccountType_Contract)
		}
		s.dynProps.SetLatestBlockHeaderNumber(int64(h))
		s.SetState(owner, slot, wordFor(h))
		s.FinalizeTransaction()
		// Distinct full logical slots sharing a suffix must never alias.
		for first := byte(1); first <= 4; first++ {
			key := make([]byte, 32)
			key[0] = first
			key[31] = 11
			v := make([]byte, 32)
			if first <= 2 {
				v[31] = first
			} else if first == 4 {
				v = nil
			}
			if err := s.SetAccountKV(owner, kvdomains.ContractStorage, key, v); err != nil {
				t.Fatal(err)
			}
		}
		if h == 1 {
			for i := 0; i < votesIndexPageEntries+1; i++ {
				voter := testAddr(byte(i))
				if err := s.AppendVotesIndex(voter); err != nil {
					t.Fatal(err)
				}
			}
		} else if h == 3 {
			if err := s.WriteVotesIndex(nil); err != nil {
				t.Fatal(err)
			}
		} else {
			if err := s.AppendVotesIndex(testAddr(byte(h))); err != nil {
				t.Fatal(err)
			}
		}
		for i, value := range [][]byte{nil, make([]byte, 8), binary.BigEndian.AppendUint64(nil, h), binary.BigEndian.AppendUint64(nil, ^uint64(0))} {
			if err := s.SetAccountKV(owner, kvdomains.AccountAssetV2, []byte{byte(i)}, value); err != nil {
				t.Fatal(err)
			}
		}
		if err := s.WriteMerkleTreeRootByBlock(int64(h), rootFor(h)); err != nil {
			t.Fatal(err)
		}
		if h == 3 {
			mark := s.Snapshot()
			if err := s.WriteMerkleTreeRootByBlock(int64(h), a); err != nil {
				t.Fatal(err)
			}
			s.RevertToSnapshot(mark)
		}
		if err := s.FlushDomainChangesSince(mark, h); err != nil {
			t.Fatal(err)
		}
		root, _, err := s.CommitWithStatsOptions(CommitOptions{BlockNumber: h, BlockHash: hash, ParentHash: tcommon.Hash{byte(h - 1)}, CommitmentTxNum: h})
		if err != nil {
			t.Fatal(err)
		}
		if err := rawdb.WriteBlockNumber(disk, hash, h); err != nil {
			t.Fatal(err)
		}
		if err := rawdb.WriteStateExecutionProperties(disk, rawdb.StateExecutionProperties{BlockNum: int64(h), Timestamp: int64(h * 3000), BlockHash: hash}); err != nil {
			t.Fatal(err)
		}
		assertCompactFullMuHash(t, disk)
		return root
	}
	roots := make([]tcommon.Hash, 0, 6)
	for h := uint64(1); h <= 5; h++ {
		roots = append(roots, commit(s, disk, h))
	}
	// Latest export and verified restore must recreate exactly the same physical
	// bytes, full accumulator, sparse runs and logical storage values.
	dir := t.TempDir()
	built, err := snapshots.NewAggregator(dir).BuildLatest(disk, snapshots.AggregatorBuildOptions{FromTxNum: 1, ToTxNum: 5})
	if err != nil {
		t.Fatal(err)
	}
	identity := snapshots.ChainIdentity{ChainID: 1, NetworkID: 1, GenesisHash: strings.Repeat("01", 32), ForkConfigHash: "sha256:" + strings.Repeat("02", 32)}
	built.Manifest.Chain = &identity
	if err := snapshots.PublishManifest(dir, built.Manifest); err != nil {
		t.Fatal(err)
	}
	restored := rawdb.NewMemoryDatabase()
	defer restored.Close()
	if _, err := snapshots.RestoreLatestFromVerifiedManifest(restored, dir, identity); err != nil {
		t.Fatal(err)
	}
	if h := assertCompactFullMuHash(t, restored); h.Root != roots[4] {
		t.Fatal("restored root differs")
	}
	restoredDB := NewDatabase(rawdb.WrapKeyValueStore(restored))
	defer restoredDB.Close()
	rs, err := New(roots[4], restoredDB)
	if err != nil {
		t.Fatal(err)
	}
	for h := int64(1); h <= 5; h++ {
		if got, ok, err := rs.ReadMerkleTreeRootByBlockStrict(h); err != nil || !ok || !bytes.Equal(got, rootFor(uint64(h))) {
			t.Fatal("restored shielded", h, got, ok, err)
		}
	}
	for first := byte(3); first <= 4; first++ {
		key := make([]byte, 32)
		key[0] = first
		key[31] = 11
		got, exists, err := rs.GetAccountKV(owner, kvdomains.ContractStorage, key)
		wantLen := 32
		if first == 4 {
			wantLen = 0
		}
		if err != nil || !exists || len(got) != wantLen || !bytes.Equal(got, make([]byte, wantLen)) {
			t.Fatal("restore lost zero/empty physical presence", first, got, exists, err)
		}
	}
	if got := rs.GetState(owner, slot); got != wordFor(5) {
		t.Fatal("restored storage", got)
	}
	next := commit(s, disk, 6)
	roots = append(roots, next)
	if other := commit(rs, restored, 6); other != next {
		t.Fatal("continued commit differs after restore")
	}
	// Historical views must select the predecessor in their as-of image and
	// reject future/genesis requests despite there being a current run.
	checkHistory := func(reader *PersistentHistoryReader) {
		t.Helper()
		for cut := uint64(1); cut <= 6; cut++ {
			view, err := New(roots[5], db)
			if err != nil {
				t.Fatal(err)
			}
			view.SetHistoricalLatestView(reader, cut)
			voters, present, err := view.ReadVotesIndexStrict()
			want := []tcommon.Address{}
			if cut < 3 {
				for i := 0; i <= votesIndexPageEntries; i++ {
					want = append(want, testAddr(byte(i)))
				}
			} else {
				for h := uint64(4); h <= cut; h++ {
					want = append(want, testAddr(byte(h)))
				}
			}
			if err != nil || !present || len(voters) != len(want) {
				t.Fatalf("votes as-of %d: len=%d want=%d present=%v err=%v", cut, len(voters), len(want), present, err)
			}
			for i := range want {
				if voters[i] != want[i] {
					t.Fatalf("votes as-of %d order at %d", cut, i)
				}
			}
			for i, value := range [][]byte{nil, make([]byte, 8), binary.BigEndian.AppendUint64(nil, cut), binary.BigEndian.AppendUint64(nil, ^uint64(0))} {
				got, exists, err := view.GetAccountKV(owner, kvdomains.AccountAssetV2, []byte{byte(i)})
				if err != nil || !exists || !bytes.Equal(got, value) {
					t.Fatalf("scalar as-of %d key %d: %x/%v/%v", cut, i, got, exists, err)
				}
			}
			for h := uint64(1); h <= cut; h++ {
				got, ok, err := view.ReadMerkleTreeRootByBlockStrict(int64(h))
				if err != nil || !ok || !bytes.Equal(got, rootFor(h)) {
					t.Fatalf("cut=%d height=%d root=%x/%v/%v", cut, h, got, ok, err)
				}
			}
			if _, ok, err := view.ReadMerkleTreeRootByBlockStrict(int64(cut + 1)); err != nil || ok {
				t.Fatal("historical future fabricated", cut, ok, err)
			}
			if got := view.GetState(owner, slot); got != wordFor(cut) {
				t.Fatalf("storage as-of %d got %x want %x", cut, got, wordFor(cut))
			}
		}
	}
	checkHistory(NewPersistentHistoryReader(disk, nil, 6))
	var changeRows int
	if err := s.IterateAccountKV(tcommon.SystemAccountAddress, kvdomains.SystemShielded, rawdb.ShieldedRootChangePrefix(), func(k, v []byte) (bool, error) { changeRows++; return true, nil }); err != nil || changeRows != 3 {
		t.Fatal("expected three changes for six blocks", changeRows, err)
	}
	for _, h := range []uint64{2, 4, 6} {
		if err := rawdb.IterateStateDomainChanges(disk, h, func(c *rawdb.StateDomainChange) (bool, error) {
			if c.Domain == kvdomains.SystemShielded && bytes.HasPrefix(c.Key, rawdb.ShieldedRootChangePrefix()) {
				t.Fatalf("unchanged root generated history at %d", h)
			}
			return true, nil
		}); err != nil {
			t.Fatal(err)
		}
	}
	updates, err := rawdb.CollectStateUnwind(disk, 6, 2)
	if err != nil {
		t.Fatal(err)
	}
	deltas, err := rawdb.StateCommitmentDeltasFromUpdates(updates)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := domains.ApplyMuHashDeltas(disk, deltas, domains.CommitmentIdentity{BlockNum: 2, BlockHash: tcommon.Hash{2}, TxNum: 2}, roots[1]); err != nil {
		t.Fatal(err)
	}
	assertCompactFullMuHash(t, disk)
	s, err = New(roots[1], db)
	if err != nil {
		t.Fatal(err)
	}
	if got, ok, err := s.ReadMerkleTreeRootByBlockStrict(3); err != nil || ok {
		t.Fatal("unwound future root fabricated", got, ok, err)
	}
	for h := uint64(3); h <= 6; h++ {
		if err := rawdb.DeleteStateDomainChanges(disk, h); err != nil {
			t.Fatal(err)
		}
		if err := rawdb.DeleteStateTxRange(disk, h); err != nil {
			t.Fatal(err)
		}
		if got := commit(s, disk, h); got != roots[h-1] {
			t.Fatalf("replay root differs at %d", h)
		}
	}
	coldDir := t.TempDir()
	refs, err := snapshots.BuildStateDomainChangeHistorySegmentsFromDB(disk, coldDir, 1, 6, "history/state-domain-change-compact.seg")
	if err != nil {
		t.Fatal(err)
	}
	if err := snapshots.PublishManifest(coldDir, snapshots.NewManifest(1, 6, refs)); err != nil {
		t.Fatal(err)
	}
	manager, err := snapshots.OpenManager(coldDir)
	if err != nil {
		t.Fatal(err)
	}
	for h := uint64(1); h <= 6; h++ {
		if err := rawdb.DeleteStateDomainChanges(disk, h); err != nil {
			t.Fatal(err)
		}
		if err := rawdb.DeleteStateTxRange(disk, h); err != nil {
			t.Fatal(err)
		}
	}
	checkHistory(NewPersistentHistoryReaderWithColdHistory(disk, nil, 6, manager))
}

func TestShieldedChangePointsNoopOverwriteAndBounds(t *testing.T) {
	s := newTestStateDB(t)
	a, b := make([]byte, 32), bytes.Repeat([]byte{3}, 32)
	for h := int64(1); h <= 10000; h++ {
		if err := s.WriteMerkleTreeRootByBlock(h, a); err != nil {
			t.Fatal(err)
		}
	}
	s.dynProps.SetLatestBlockHeaderNumber(10000)
	count := 0
	if err := s.IterateAccountKV(tcommon.SystemAccountAddress, kvdomains.SystemShielded, rawdb.ShieldedRootChangePrefix(), func(k, v []byte) (bool, error) { count++; return true, nil }); err != nil || count != 1 {
		t.Fatal("constant run grew", count, err)
	}
	for _, h := range []int64{-1, 0, 10001} {
		if _, ok, err := s.ReadMerkleTreeRootByBlockStrict(h); err != nil || ok {
			t.Fatal("out-of-range root", h, ok, err)
		}
	}
	if err := s.WriteMerkleTreeRootByBlock(10000, b); err != nil {
		t.Fatal(err)
	}
	if err := s.WriteMerkleTreeRootByBlock(10000, a); err != nil {
		t.Fatal(err)
	}
	if _, ok, err := s.GetAccountKV(tcommon.SystemAccountAddress, kvdomains.SystemShielded, rawdb.ShieldedRootChangeStateKey(10000)); err != nil || ok {
		t.Fatal("redundant overwrite not removed", ok, err)
	}
	if err := s.WriteMerkleTreeRootByBlock(9999, b); err == nil {
		t.Fatal("past canonical run rewrite accepted")
	}
	for _, bad := range []struct {
		h int64
		v []byte
	}{{0, a}, {-1, a}, {1, nil}, {1, []byte{1}}} {
		if err := s.WriteMerkleTreeRootByBlock(bad.h, bad.v); err == nil {
			t.Fatal(fmt.Sprint("bad change accepted ", bad.h))
		}
	}
}
