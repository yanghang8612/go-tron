package snapshots

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/state/kvdomains"
)

func TestLatestSegmentIteratorWriterMatchesMaterializedJSON(t *testing.T) {
	owner1 := common.BytesToAddress(append([]byte{common.AddressPrefixMainnet}, bytes.Repeat([]byte{0x11}, common.AccountIDLength)...))
	owner2 := common.BytesToAddress(append([]byte{common.AddressPrefixMainnet}, bytes.Repeat([]byte{0x22}, common.AccountIDLength)...))
	entries := []LatestEntry{
		{Key: AccountKVSnapshotKey(owner1, 7, []byte("alpha")), Value: []byte("one")},
		{Key: AccountKVSnapshotKey(owner2, 9, []byte("beta")), Value: []byte("two")},
	}
	ref := SegmentRef{
		Dataset:   SegmentDatasetKVLatest,
		Domain:    kvdomains.SystemDynamicProperty,
		Kind:      SegmentLatest,
		FromTxNum: 1,
		ToTxNum:   10,
		Path:      "latest/system-dp.json",
	}

	materializedDir := t.TempDir()
	materializedRef, err := WriteLatestSegment(materializedDir, ref, entries)
	if err != nil {
		t.Fatalf("materialized write: %v", err)
	}
	streamingDir := t.TempDir()
	streamingRef, err := writeLatestSegmentFromIterator(streamingDir, ref, func(yield func(LatestEntry) error) error {
		for _, entry := range entries {
			if err := yield(entry); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("streaming write: %v", err)
	}
	if streamingRef.Size != materializedRef.Size || streamingRef.Checksum != materializedRef.Checksum {
		t.Fatalf("streaming metadata = size %d checksum %s, materialized size %d checksum %s", streamingRef.Size, streamingRef.Checksum, materializedRef.Size, materializedRef.Checksum)
	}
	streamingBytes, err := os.ReadFile(filepath.Join(streamingDir, streamingRef.Path))
	if err != nil {
		t.Fatalf("read streaming segment: %v", err)
	}
	materializedBytes, err := os.ReadFile(filepath.Join(materializedDir, materializedRef.Path))
	if err != nil {
		t.Fatalf("read materialized segment: %v", err)
	}
	if !bytes.Equal(streamingBytes, materializedBytes) {
		t.Fatal("streaming JSON segment bytes differ from materialized writer")
	}
}

func TestLatestSegmentIteratorWriterRejectsUnsortedStream(t *testing.T) {
	owner1 := common.BytesToAddress(append([]byte{common.AddressPrefixMainnet}, bytes.Repeat([]byte{0x11}, common.AccountIDLength)...))
	owner2 := common.BytesToAddress(append([]byte{common.AddressPrefixMainnet}, bytes.Repeat([]byte{0x22}, common.AccountIDLength)...))
	ref := SegmentRef{
		Dataset:   SegmentDatasetKVLatest,
		Domain:    kvdomains.SystemDynamicProperty,
		Kind:      SegmentLatest,
		FromTxNum: 1,
		ToTxNum:   10,
		Path:      "latest/system-dp.json",
	}

	_, err := writeLatestSegmentFromIterator(t.TempDir(), ref, func(yield func(LatestEntry) error) error {
		if err := yield(LatestEntry{Key: AccountKVSnapshotKey(owner2, 9, []byte("beta")), Value: []byte("two")}); err != nil {
			return err
		}
		return yield(LatestEntry{Key: AccountKVSnapshotKey(owner1, 7, []byte("alpha")), Value: []byte("one")})
	})
	if err == nil || !strings.Contains(err.Error(), "not strictly sorted") {
		t.Fatalf("unsorted stream err = %v, want sortedness error", err)
	}
}

func TestLatestSegmentBuildsAccountKVDomainsFromHotStore(t *testing.T) {
	dir := t.TempDir()
	owner1 := latestStoreTestAddress(0x13)
	owner2 := latestStoreTestAddress(0x24)
	code := []byte{0x60, 0x01, 0x60, 0x00}
	codeHash := common.Keccak256(code)
	root := common.BytesToHash(bytes.Repeat([]byte{0x56}, common.HashLength))
	nodeKey := append(rawdb.LatestDomainCommitmentNodeLogicalPrefix(), []byte("typed-node")...)
	nodeValue := common.BytesToHash(bytes.Repeat([]byte{0x57}, common.HashLength)).Bytes()
	checkpointKey := append(rawdb.StateCommitmentCheckpointLogicalPrefix(), []byte{0, 0, 0, 0, 0, 0, 0, 9}...)
	store := &recordingLatestHotStore{
		accounts: []latestHotAccountRow{
			{owner: owner1, value: []byte("account-1")},
			{owner: owner2, value: []byte("account-2")},
		},
		kvs: []latestHotKVRow{
			{owner: owner1, generation: 7, domain: kvdomains.SystemReward, key: []byte("reward/a"), value: []byte("reward-a")},
			{owner: owner2, generation: 8, domain: kvdomains.SystemDynamicProperty, key: []byte("ignored"), value: []byte("ignored")},
		},
		generations: []latestHotGenerationRow{
			{owner: owner1, generation: 7},
			{owner: owner2, generation: 8},
		},
		codes: []latestHotCodeRow{
			{hash: codeHash, code: code},
		},
		commitmentRoot: root,
		commitmentRows: []latestHotCommitmentRow{
			{key: nodeKey, value: nodeValue},
			{key: checkpointKey, value: []byte("checkpoint")},
		},
	}

	accountRef, err := buildAccountLatestSegmentFromStore(store, dir, 1, 10, "latest/accounts.json")
	if err != nil {
		t.Fatalf("build account latest from store: %v", err)
	}
	accountSeg, err := OpenLatestSegment(dir, accountRef)
	if err != nil {
		t.Fatalf("open account latest: %v", err)
	}
	if got, ok, err := accountSeg.Get(AccountSnapshotKey(owner2)); err != nil || !ok || string(got) != "account-2" {
		t.Fatalf("account latest from store = %q ok=%v err=%v, want account-2,true,nil", got, ok, err)
	}

	kvRef, err := buildLatestDomainSegmentFromStore(store, dir, kvdomains.SystemReward, 1, 10, "latest/reward.json")
	if err != nil {
		t.Fatalf("build kv latest from store: %v", err)
	}
	kvSeg, err := OpenLatestSegment(dir, kvRef)
	if err != nil {
		t.Fatalf("open kv latest: %v", err)
	}
	if got, ok, err := kvSeg.Get(AccountKVSnapshotKey(owner1, 7, []byte("reward/a"))); err != nil || !ok || string(got) != "reward-a" {
		t.Fatalf("kv latest from store = %q ok=%v err=%v, want reward-a,true,nil", got, ok, err)
	}
	if _, ok, err := kvSeg.Get(AccountKVSnapshotKey(owner2, 8, []byte("ignored"))); err != nil || ok {
		t.Fatalf("wrong-domain kv latest ok=%v err=%v, want false,nil", ok, err)
	}

	generationRef, err := buildKVGenerationSegmentFromStore(store, dir, 1, 10, "latest/generation.json")
	if err != nil {
		t.Fatalf("build generation from store: %v", err)
	}
	generationSeg, err := OpenLatestSegment(dir, generationRef)
	if err != nil {
		t.Fatalf("open generation latest: %v", err)
	}
	if got, ok, err := generationSeg.Get(KVGenerationSnapshotKey(owner2)); err != nil || !ok {
		t.Fatalf("generation get ok=%v err=%v, want true,nil", ok, err)
	} else if generation, err := rawdb.DecodeStateKVGenerationValue(got); err != nil || generation != 8 {
		t.Fatalf("generation entry = %d err=%v, want 8,nil", generation, err)
	}
	if store.accountIterations != 1 || store.kvIterations[kvdomains.SystemReward] != 1 || store.generationIterations != 1 {
		t.Fatalf("store iterations account=%d kv=%v generation=%d", store.accountIterations, store.kvIterations, store.generationIterations)
	}

	codeRef, err := buildCodeSegmentFromStore(store, dir, 1, 10, "latest/code.json")
	if err != nil {
		t.Fatalf("build code from store: %v", err)
	}
	codeSeg, err := OpenLatestSegment(dir, codeRef)
	if err != nil {
		t.Fatalf("open code latest: %v", err)
	}
	if got, ok, err := codeSeg.Get(CodeSnapshotKey(codeHash)); err != nil || !ok || !bytes.Equal(got, code) {
		t.Fatalf("code latest from store = %x ok=%v err=%v, want %x,true,nil", got, ok, err, code)
	}

	rootRef, err := buildCommitmentRootSegmentFromStore(store, dir, 1, 10, "commitment/root.json")
	if err != nil {
		t.Fatalf("build commitment root from store: %v", err)
	}
	rootSeg, err := OpenLatestSegment(dir, rootRef)
	if err != nil {
		t.Fatalf("open commitment root: %v", err)
	}
	if got, ok, err := rootSeg.Get(rawdb.LatestDomainCommitmentRootLogicalKey()); err != nil || !ok || !bytes.Equal(got, root.Bytes()) {
		t.Fatalf("commitment root from store = %x ok=%v err=%v, want %x,true,nil", got, ok, err, root.Bytes())
	}

	nodeRef, err := buildCommitmentDomainSegmentFromStore(store, SegmentDatasetCommitmentNode, rawdb.LatestDomainCommitmentNodeLogicalPrefix(), dir, 1, 10, "commitment/nodes.json")
	if err != nil {
		t.Fatalf("build commitment nodes from store: %v", err)
	}
	nodeSeg, err := OpenLatestSegment(dir, nodeRef)
	if err != nil {
		t.Fatalf("open commitment nodes: %v", err)
	}
	if got, ok, err := nodeSeg.Get(nodeKey); err != nil || !ok || !bytes.Equal(got, nodeValue) {
		t.Fatalf("commitment node from store = %x ok=%v err=%v, want %x,true,nil", got, ok, err, nodeValue)
	}
	if _, ok, err := nodeSeg.Get(checkpointKey); err != nil || ok {
		t.Fatalf("checkpoint leaked into node segment ok=%v err=%v", ok, err)
	}
}

func TestCommitmentCheckpointSegmentSynthesizesLatestPointer(t *testing.T) {
	dir := t.TempDir()
	db := rawdb.NewMemoryDatabase()
	root := common.Hash{0xaa}
	for _, checkpoint := range []*rawdb.StateCommitmentCheckpoint{
		{BlockNum: 3, BlockHash: common.Hash{0x03}, Root: root, Scheme: rawdb.LatestDomainCommitmentScheme},
		{BlockNum: 5, BlockHash: common.Hash{0x05}, Root: root, Scheme: rawdb.LatestDomainCommitmentScheme},
	} {
		if err := rawdb.WriteStateCommitmentCheckpoint(db, checkpoint); err != nil {
			t.Fatalf("write checkpoint %d: %v", checkpoint.BlockNum, err)
		}
	}
	if err := rawdb.DeleteStateCommitmentDomain(db, rawdb.LatestStateCommitmentCheckpointLogicalKey()); err != nil {
		t.Fatalf("delete latest checkpoint pointer: %v", err)
	}

	ref, _, _, err := BuildCommitmentCheckpointSegmentFilesFromDB(db, dir, 10, 20, "commitment/checkpoints.seg")
	if err != nil {
		t.Fatalf("build checkpoint segment: %v", err)
	}
	seg, err := OpenLatestSegment(dir, ref)
	if err != nil {
		t.Fatalf("open checkpoint segment: %v", err)
	}
	value, ok, err := seg.Get(rawdb.LatestStateCommitmentCheckpointLogicalKey())
	if err != nil || !ok {
		t.Fatalf("latest checkpoint pointer from segment ok=%v err=%v", ok, err)
	}
	latest, err := rawdb.DecodeStateCommitmentCheckpointValue(value)
	if err != nil {
		t.Fatalf("decode latest checkpoint pointer: %v", err)
	}
	if latest.BlockNum != 5 {
		t.Fatalf("latest checkpoint pointer block = %d, want 5", latest.BlockNum)
	}

	restored := rawdb.NewMemoryDatabase()
	if err := seg.restoreToStore(newRawDBLatestHotRestoreStore(restored)); err != nil {
		t.Fatalf("restore checkpoint segment: %v", err)
	}
	if got, ok, err := rawdb.ReadLatestStateCommitmentCheckpoint(restored); err != nil || !ok || got.BlockNum != 5 {
		t.Fatalf("restored latest checkpoint = %+v ok=%v err=%v, want block 5", got, ok, err)
	}
	if got, ok, err := rawdb.ReadStateCommitmentCheckpoint(restored, 3); err != nil || !ok || got.BlockNum != 3 {
		t.Fatalf("restored checkpoint 3 = %+v ok=%v err=%v", got, ok, err)
	}
	if got, ok, err := rawdb.ReadStateCommitmentCheckpoint(restored, 5); err != nil || !ok || got.BlockNum != 5 {
		t.Fatalf("restored checkpoint 5 = %+v ok=%v err=%v", got, ok, err)
	}
}

func TestLatestSegmentRestoreUsesHotStore(t *testing.T) {
	owner := latestStoreTestAddress(0x35)
	store := &recordingLatestHotStore{}

	accountSeg := &LatestSegment{
		Version: LatestSegmentVersion,
		Dataset: SegmentDatasetAccountLatest,
		Entries: []LatestEntry{{Key: AccountSnapshotKey(owner), Value: []byte("account")}},
	}
	if err := accountSeg.restoreToStore(store); err != nil {
		t.Fatalf("restore account through store: %v", err)
	}
	if got := store.writtenAccount[owner.Hex()]; string(got) != "account" {
		t.Fatalf("written account = %q, want account", got)
	}

	kvSeg := &LatestSegment{
		Version: LatestSegmentVersion,
		Dataset: SegmentDatasetKVLatest,
		Domain:  kvdomains.SystemReward,
		Entries: []LatestEntry{{Key: AccountKVSnapshotKey(owner, 9, []byte("reward/a")), Value: []byte("reward")}},
	}
	if err := kvSeg.restoreToStore(store); err != nil {
		t.Fatalf("restore kv through store: %v", err)
	}
	if got := store.writtenKV[recordingLatestKVKey(owner, 9, kvdomains.SystemReward, []byte("reward/a"))]; string(got) != "reward" {
		t.Fatalf("written kv = %q, want reward", got)
	}

	generationSeg := &LatestSegment{
		Version: LatestSegmentVersion,
		Dataset: SegmentDatasetKVGeneration,
		Entries: []LatestEntry{{Key: KVGenerationSnapshotKey(owner), Value: rawdb.EncodeStateKVGenerationValue(9)}},
	}
	if err := generationSeg.restoreToStore(store); err != nil {
		t.Fatalf("restore generation through store: %v", err)
	}
	if got := store.writtenGeneration[owner.Hex()]; got != 9 {
		t.Fatalf("written generation = %d, want 9", got)
	}

	code := []byte{0x60, 0x02, 0x60, 0x00}
	codeHash := common.Keccak256(code)
	codeSeg := &LatestSegment{
		Version: LatestSegmentVersion,
		Dataset: SegmentDatasetCode,
		Entries: []LatestEntry{{Key: CodeSnapshotKey(codeHash), Value: code}},
	}
	if err := codeSeg.restoreToStore(store); err != nil {
		t.Fatalf("restore code through store: %v", err)
	}
	if got := store.writtenCode[codeHash]; !bytes.Equal(got, code) {
		t.Fatalf("written code = %x, want %x", got, code)
	}

	nodeKey := append(rawdb.LatestDomainCommitmentNodeLogicalPrefix(), []byte("typed-restore")...)
	nodeValue := common.BytesToHash(bytes.Repeat([]byte{0x58}, common.HashLength)).Bytes()
	commitmentSeg := &LatestSegment{
		Version: LatestSegmentVersion,
		Dataset: SegmentDatasetCommitmentNode,
		Entries: []LatestEntry{{Key: nodeKey, Value: nodeValue}},
	}
	if err := commitmentSeg.restoreToStore(store); err != nil {
		t.Fatalf("restore commitment through store: %v", err)
	}
	if got := store.writtenCommitment[string(nodeKey)]; !bytes.Equal(got, nodeValue) {
		t.Fatalf("written commitment = %x, want %x", got, nodeValue)
	}
}

func TestLatestSegmentBuildPublishAndRead(t *testing.T) {
	dir := t.TempDir()
	db := rawdb.NewMemoryDatabase()
	owner1 := common.BytesToAddress(append([]byte{common.AddressPrefixMainnet}, bytes.Repeat([]byte{0x11}, common.AccountIDLength)...))
	owner2 := common.BytesToAddress(append([]byte{common.AddressPrefixMainnet}, bytes.Repeat([]byte{0x22}, common.AccountIDLength)...))
	if err := rawdb.WriteStateKVLatest(db, owner2, 3, kvdomains.SystemReward, []byte("cycle/2"), []byte("ignored")); err != nil {
		t.Fatal(err)
	}
	if err := rawdb.WriteStateKVLatest(db, owner1, 7, kvdomains.SystemDynamicProperty, []byte("latest_block"), []byte("12")); err != nil {
		t.Fatal(err)
	}
	if err := rawdb.WriteStateKVLatest(db, owner2, 9, kvdomains.SystemDynamicProperty, []byte("next_maintenance"), []byte("42")); err != nil {
		t.Fatal(err)
	}

	ref, err := BuildLatestDomainSegmentFromDB(db, dir, kvdomains.SystemDynamicProperty, 1, 10, "latest/system-dp.json")
	if err != nil {
		t.Fatalf("build latest segment: %v", err)
	}
	if ref.Size == 0 || ref.Checksum == "" {
		t.Fatalf("segment metadata not filled: %+v", ref)
	}
	manifest := NewManifest(1, 10, []SegmentRef{ref})
	if err := PublishManifest(dir, manifest); err != nil {
		t.Fatalf("publish manifest: %v", err)
	}

	mgr, err := OpenManager(dir)
	if err != nil {
		t.Fatalf("open manager: %v", err)
	}
	key := AccountKVSnapshotKey(owner1, 7, []byte("latest_block"))
	got, ok, err := mgr.GetLatest(kvdomains.SystemDynamicProperty, key, 5)
	if err != nil || !ok || string(got) != "12" {
		t.Fatalf("GetLatest = %q ok:%v err:%v", got, ok, err)
	}
	if _, ok, err := mgr.GetLatest(kvdomains.SystemReward, key, 5); err != nil || ok {
		t.Fatalf("wrong domain hit ok:%v err:%v", ok, err)
	}

	var keys [][]byte
	prefix := owner2.AccountID().Bytes()
	if err := mgr.IterateLatestPrefix(kvdomains.SystemDynamicProperty, prefix, 5, func(key, value []byte) (bool, error) {
		keys = append(keys, key)
		if string(value) != "42" {
			t.Fatalf("prefix value = %q", value)
		}
		return true, nil
	}); err != nil {
		t.Fatalf("iterate prefix: %v", err)
	}
	if len(keys) != 1 {
		t.Fatalf("prefix keys = %d, want 1", len(keys))
	}
	if _, err := OpenLatestSegment(dir, SegmentRef{Domain: ref.Domain, Kind: ref.Kind, FromTxNum: ref.FromTxNum, ToTxNum: ref.ToTxNum, Path: ref.Path, Size: ref.Size, Checksum: "sha256:bad"}); err == nil {
		t.Fatal("bad checksum accepted")
	}
	if _, err := OpenLatestSegment(filepath.Join(dir, "missing"), ref); err == nil {
		t.Fatal("missing segment accepted")
	}
}

func TestLatestBinaryManagerReadsWithoutMaterializingSegment(t *testing.T) {
	dir := t.TempDir()
	db := rawdb.NewMemoryDatabase()
	owner1 := common.BytesToAddress(append([]byte{common.AddressPrefixMainnet}, bytes.Repeat([]byte{0x51}, common.AccountIDLength)...))
	owner2 := common.BytesToAddress(append([]byte{common.AddressPrefixMainnet}, bytes.Repeat([]byte{0x52}, common.AccountIDLength)...))
	if err := rawdb.WriteStateKVLatest(db, owner1, 7, kvdomains.SystemDynamicProperty, []byte("a"), []byte("value-a")); err != nil {
		t.Fatal(err)
	}
	if err := rawdb.WriteStateKVLatest(db, owner2, 7, kvdomains.SystemDynamicProperty, []byte("b"), []byte("value-b")); err != nil {
		t.Fatal(err)
	}
	ref, accessorRef, btreeRef, err := BuildLatestDomainSegmentFilesFromDB(db, dir, kvdomains.SystemDynamicProperty, 1, 10, "latest/system-dp.seg")
	if err != nil {
		t.Fatalf("build latest binary segment: %v", err)
	}
	if accessorRef.Kind != SegmentAccessor || accessorRef.Path != latestBinaryAccessorPath(ref.Path) {
		t.Fatalf("accessor ref = %+v", accessorRef)
	}
	if btreeRef.Kind != SegmentBTree || btreeRef.Path != latestBinaryBTreePath(ref.Path) {
		t.Fatalf("btree ref = %+v", btreeRef)
	}
	if err := PublishManifest(dir, NewManifest(1, 10, []SegmentRef{ref, accessorRef, btreeRef})); err != nil {
		t.Fatalf("publish manifest: %v", err)
	}
	mgr, err := OpenManager(dir)
	if err != nil {
		t.Fatalf("open manager: %v", err)
	}
	key := AccountKVSnapshotKey(owner2, 7, []byte("b"))
	got, ok, err := mgr.GetLatest(kvdomains.SystemDynamicProperty, key, 5)
	if err != nil || !ok || string(got) != "value-b" {
		t.Fatalf("GetLatest binary = %q ok:%v err:%v", got, ok, err)
	}
	if len(mgr.cache) != 0 {
		t.Fatalf("binary GetLatest materialized segment cache entries = %d", len(mgr.cache))
	}
	var values []string
	prefix := owner1.AccountID().Bytes()
	if err := mgr.IterateLatestPrefix(kvdomains.SystemDynamicProperty, prefix, 5, func(key, value []byte) (bool, error) {
		values = append(values, string(value))
		return true, nil
	}); err != nil {
		t.Fatalf("IterateLatestPrefix binary: %v", err)
	}
	if len(values) != 1 || values[0] != "value-a" {
		t.Fatalf("values = %v, want [value-a]", values)
	}
	if len(mgr.cache) != 0 {
		t.Fatalf("binary IterateLatestPrefix materialized segment cache entries = %d", len(mgr.cache))
	}

	restored := rawdb.NewMemoryDatabase()
	if err := mgr.RestoreLatest(restored, 5); err != nil {
		t.Fatalf("RestoreLatest binary: %v", err)
	}
	if got, ok, err := rawdb.ReadStateKVLatest(restored, owner2, 7, kvdomains.SystemDynamicProperty, []byte("b")); err != nil || !ok || string(got) != "value-b" {
		t.Fatalf("restored binary kv latest = %q ok:%v err:%v", got, ok, err)
	}
	if len(mgr.cache) != 0 {
		t.Fatalf("binary RestoreLatest materialized segment cache entries = %d", len(mgr.cache))
	}
}

func TestLatestBinaryManagerReadsWithBTreeAccessor(t *testing.T) {
	dir := t.TempDir()
	db := rawdb.NewMemoryDatabase()
	owner := common.BytesToAddress(append([]byte{common.AddressPrefixMainnet}, bytes.Repeat([]byte{0x53}, common.AccountIDLength)...))
	entryCount := int(latestBinaryBTreeBlockSize) + 17
	for i := 0; i < entryCount; i++ {
		key := []byte(fmt.Sprintf("slot/%03d", i))
		value := []byte(fmt.Sprintf("value-%03d", i))
		if err := rawdb.WriteStateKVLatest(db, owner, 7, kvdomains.ContractStorage, key, value); err != nil {
			t.Fatalf("write kv latest %d: %v", i, err)
		}
	}
	ref, accessorRef, btreeRef, err := BuildLatestDomainSegmentFilesFromDB(db, dir, kvdomains.ContractStorage, 1, 20, "latest/contract-storage.seg")
	if err != nil {
		t.Fatalf("build latest binary segment files: %v", err)
	}
	if accessorRef.Kind != SegmentAccessor || accessorRef.Path != latestBinaryAccessorPath(ref.Path) {
		t.Fatalf("accessor ref = %+v", accessorRef)
	}
	if btreeRef.Kind != SegmentBTree || btreeRef.Path != latestBinaryBTreePath(ref.Path) {
		t.Fatalf("btree ref = %+v", btreeRef)
	}
	if err := CheckLatestBTreeSegment(dir, btreeRef); err != nil {
		t.Fatalf("check latest btree: %v", err)
	}
	if err := PublishManifest(dir, NewManifest(1, 20, []SegmentRef{ref, accessorRef, btreeRef})); err != nil {
		t.Fatalf("publish manifest: %v", err)
	}

	mgr, err := OpenManager(dir)
	if err != nil {
		t.Fatalf("open manager: %v", err)
	}
	key := AccountKVSnapshotKey(owner, 7, []byte("slot/139"))
	got, ok, err := mgr.GetLatest(kvdomains.ContractStorage, key, 10)
	if err != nil || !ok || string(got) != "value-139" {
		t.Fatalf("GetLatest btree = %q ok:%v err:%v", got, ok, err)
	}
	if len(mgr.cache) != 0 {
		t.Fatalf("btree GetLatest materialized segment cache entries = %d", len(mgr.cache))
	}

	var count int
	prefix := owner.AccountID().Bytes()
	if err := mgr.IterateLatestPrefix(kvdomains.ContractStorage, prefix, 10, func(key, value []byte) (bool, error) {
		count++
		return true, nil
	}); err != nil {
		t.Fatalf("IterateLatestPrefix btree: %v", err)
	}
	if count != entryCount {
		t.Fatalf("prefix count = %d, want %d", count, entryCount)
	}
	if len(mgr.cache) != 0 {
		t.Fatalf("btree IterateLatestPrefix materialized segment cache entries = %d", len(mgr.cache))
	}
}

func TestFlatLatestSegmentsBuildServeAndRestore(t *testing.T) {
	dir := t.TempDir()
	db := rawdb.NewMemoryDatabase()
	owner := common.BytesToAddress(append([]byte{common.AddressPrefixMainnet}, bytes.Repeat([]byte{0x33}, common.AccountIDLength)...))
	other := common.BytesToAddress(append([]byte{common.AddressPrefixMainnet}, bytes.Repeat([]byte{0x44}, common.AccountIDLength)...))

	if err := rawdb.WriteStateAccountLatest(db, owner, []byte("account-envelope")); err != nil {
		t.Fatal(err)
	}
	if err := rawdb.WriteStateAccountLatest(db, other, []byte("other-account")); err != nil {
		t.Fatal(err)
	}
	if err := rawdb.WriteStateKVGeneration(db, owner, 7); err != nil {
		t.Fatal(err)
	}
	if err := rawdb.WriteStateKVLatest(db, owner, 7, kvdomains.ContractStorage, []byte("slot/a"), []byte("value-a")); err != nil {
		t.Fatal(err)
	}
	if err := rawdb.WriteStateKVLatest(db, owner, 7, kvdomains.SystemReward, []byte("ignored"), []byte("wrong-domain")); err != nil {
		t.Fatal(err)
	}
	root := common.BytesToHash(bytes.Repeat([]byte{0xab}, common.HashLength))
	if err := rawdb.WriteLatestDomainCommitmentRoot(db, root); err != nil {
		t.Fatal(err)
	}
	nodeKey := append(rawdb.LatestDomainCommitmentNodeLogicalPrefix(), []byte{0x00, 0x80}...)
	nodeHash := common.BytesToHash(bytes.Repeat([]byte{0xcd}, common.HashLength))
	if err := rawdb.WriteStateCommitmentDomain(db, nodeKey, nodeHash.Bytes()); err != nil {
		t.Fatal(err)
	}

	var refs []SegmentRef
	builders := []func() (SegmentRef, error){
		func() (SegmentRef, error) {
			return BuildAccountLatestSegmentFromDB(db, dir, 100, 120, "latest/accounts.json")
		},
		func() (SegmentRef, error) {
			return BuildLatestDomainSegmentFromDB(db, dir, kvdomains.ContractStorage, 100, 120, "latest/contract-storage.json")
		},
		func() (SegmentRef, error) {
			return BuildKVGenerationSegmentFromDB(db, dir, 100, 120, "latest/kv-generation.json")
		},
		func() (SegmentRef, error) {
			return BuildCommitmentRootSegmentFromDB(db, dir, 100, 120, "commitment/root.json")
		},
		func() (SegmentRef, error) {
			return BuildCommitmentNodeSegmentFromDB(db, dir, 100, 120, "commitment/nodes.json")
		},
	}
	for _, build := range builders {
		ref, err := build()
		if err != nil {
			t.Fatalf("build segment: %v", err)
		}
		refs = append(refs, ref)
	}
	if err := PublishManifest(dir, NewManifest(100, 120, refs)); err != nil {
		t.Fatalf("publish manifest: %v", err)
	}

	mgr, err := OpenManager(dir)
	if err != nil {
		t.Fatalf("open manager: %v", err)
	}
	if got, ok, err := mgr.GetAccountLatest(owner, 110); err != nil || !ok || string(got) != "account-envelope" {
		t.Fatalf("account latest = %q ok=%v err=%v", got, ok, err)
	}
	if got, ok, err := mgr.GetKVLatest(kvdomains.ContractStorage, owner, 7, []byte("slot/a"), 110); err != nil || !ok || string(got) != "value-a" {
		t.Fatalf("kv latest = %q ok=%v err=%v", got, ok, err)
	}
	if got, ok, err := mgr.GetKVGeneration(owner, 110); err != nil || !ok || got != 7 {
		t.Fatalf("kv generation = %d ok=%v err=%v", got, ok, err)
	}
	if got, ok, err := mgr.GetCommitmentRoot(110); err != nil || !ok || got != root {
		t.Fatalf("commitment root = %x ok=%v err=%v", got, ok, err)
	}
	if got, ok, err := mgr.GetCommitmentNode(nodeKey, 110); err != nil || !ok || !bytes.Equal(got, nodeHash.Bytes()) {
		t.Fatalf("commitment node = %x ok=%v err=%v", got, ok, err)
	}
	if _, ok, err := mgr.GetKVLatest(kvdomains.SystemReward, owner, 7, []byte("ignored"), 110); err != nil || ok {
		t.Fatalf("unbuilt kv domain hit ok=%v err=%v", ok, err)
	}

	restored := rawdb.NewMemoryDatabase()
	if err := mgr.RestoreLatest(restored, 110); err != nil {
		t.Fatalf("restore latest: %v", err)
	}
	if got, ok, err := rawdb.ReadStateAccountLatest(restored, owner); err != nil || !ok || string(got) != "account-envelope" {
		t.Fatalf("restored account = %q ok=%v err=%v", got, ok, err)
	}
	if got, ok, err := rawdb.ReadStateKVLatest(restored, owner, 7, kvdomains.ContractStorage, []byte("slot/a")); err != nil || !ok || string(got) != "value-a" {
		t.Fatalf("restored kv latest = %q ok=%v err=%v", got, ok, err)
	}
	if got, ok, err := rawdb.ReadStateKVGeneration(restored, owner); err != nil || !ok || got != 7 {
		t.Fatalf("restored generation = %d ok=%v err=%v", got, ok, err)
	}
	if got, ok, err := rawdb.ReadLatestDomainCommitmentRoot(restored); err != nil || !ok || got != root {
		t.Fatalf("restored root = %x ok=%v err=%v", got, ok, err)
	}
	if got, ok, err := rawdb.ReadStateCommitmentDomain(restored, nodeKey); err != nil || !ok || !bytes.Equal(got, nodeHash.Bytes()) {
		t.Fatalf("restored node = %x ok=%v err=%v", got, ok, err)
	}
	if _, ok, err := rawdb.ReadStateKVLatest(restored, owner, 7, kvdomains.SystemReward, []byte("ignored")); err != nil || ok {
		t.Fatalf("restored unbuilt domain ok=%v err=%v", ok, err)
	}
}

func TestManagerReloadsManifestAfterOpen(t *testing.T) {
	dir := t.TempDir()
	mgr, err := OpenManager(dir)
	if err != nil {
		t.Fatalf("open empty manager: %v", err)
	}
	db := rawdb.NewMemoryDatabase()
	owner := common.BytesToAddress(append([]byte{common.AddressPrefixMainnet}, bytes.Repeat([]byte{0x77}, common.AccountIDLength)...))
	if err := rawdb.WriteStateAccountLatest(db, owner, []byte("account-v1")); err != nil {
		t.Fatal(err)
	}
	ref, err := BuildAccountLatestSegmentFromDB(db, dir, 1, 1, "latest/account-latest-1-1.json")
	if err != nil {
		t.Fatalf("build account latest: %v", err)
	}
	if err := PublishManifest(dir, NewManifest(1, 1, []SegmentRef{ref})); err != nil {
		t.Fatalf("publish manifest: %v", err)
	}
	got, ok, err := mgr.GetAccountLatest(owner, 1)
	if err != nil || !ok || string(got) != "account-v1" {
		t.Fatalf("reloaded account latest = %q ok=%v err=%v", got, ok, err)
	}
	if manifest := mgr.Manifest(); manifest == nil || len(manifest.Segments) != 1 {
		t.Fatalf("reloaded manifest = %+v", manifest)
	}
}

type latestHotAccountRow struct {
	owner common.Address
	value []byte
}

type latestHotKVRow struct {
	owner      common.Address
	generation uint64
	domain     kvdomains.KVDomain
	key        []byte
	value      []byte
}

type latestHotGenerationRow struct {
	owner      common.Address
	generation uint64
}

type latestHotCodeRow struct {
	hash common.Hash
	code []byte
}

type latestHotCommitmentRow struct {
	key   []byte
	value []byte
}

type recordingLatestHotStore struct {
	accounts       []latestHotAccountRow
	kvs            []latestHotKVRow
	generations    []latestHotGenerationRow
	codes          []latestHotCodeRow
	commitmentRoot common.Hash
	commitmentRows []latestHotCommitmentRow

	accountIterations    int
	kvIterations         map[kvdomains.KVDomain]int
	generationIterations int
	codeIterations       int
	commitmentIterations map[string]int

	writtenAccount    map[string][]byte
	writtenKV         map[string][]byte
	writtenGeneration map[string]uint64
	writtenCode       map[common.Hash][]byte
	writtenCommitment map[string][]byte
}

func (s *recordingLatestHotStore) IterateAccountLatest(fn func(owner common.Address, value []byte) (bool, error)) error {
	s.accountIterations++
	rows := append([]latestHotAccountRow(nil), s.accounts...)
	sort.Slice(rows, func(i, j int) bool {
		return bytes.Compare(AccountSnapshotKey(rows[i].owner), AccountSnapshotKey(rows[j].owner)) < 0
	})
	for _, row := range rows {
		cont, err := fn(row.owner, append([]byte(nil), row.value...))
		if err != nil || !cont {
			return err
		}
	}
	return nil
}

func (s *recordingLatestHotStore) WriteAccountLatest(owner common.Address, value []byte) error {
	if s.writtenAccount == nil {
		s.writtenAccount = make(map[string][]byte)
	}
	s.writtenAccount[owner.Hex()] = append([]byte(nil), value...)
	return nil
}

func (s *recordingLatestHotStore) IterateKVLatestDomain(domain kvdomains.KVDomain, fn func(owner common.Address, generation uint64, key, value []byte) (bool, error)) error {
	if s.kvIterations == nil {
		s.kvIterations = make(map[kvdomains.KVDomain]int)
	}
	s.kvIterations[domain]++
	var rows []latestHotKVRow
	for _, row := range s.kvs {
		if row.domain == domain {
			rows = append(rows, row)
		}
	}
	sort.Slice(rows, func(i, j int) bool {
		return bytes.Compare(AccountKVSnapshotKey(rows[i].owner, rows[i].generation, rows[i].key), AccountKVSnapshotKey(rows[j].owner, rows[j].generation, rows[j].key)) < 0
	})
	for _, row := range rows {
		cont, err := fn(row.owner, row.generation, append([]byte(nil), row.key...), append([]byte(nil), row.value...))
		if err != nil || !cont {
			return err
		}
	}
	return nil
}

func (s *recordingLatestHotStore) WriteKVLatest(owner common.Address, generation uint64, domain kvdomains.KVDomain, key, value []byte) error {
	if s.writtenKV == nil {
		s.writtenKV = make(map[string][]byte)
	}
	s.writtenKV[recordingLatestKVKey(owner, generation, domain, key)] = append([]byte(nil), value...)
	return nil
}

func (s *recordingLatestHotStore) IterateKVGeneration(fn func(owner common.Address, generation uint64) (bool, error)) error {
	s.generationIterations++
	rows := append([]latestHotGenerationRow(nil), s.generations...)
	sort.Slice(rows, func(i, j int) bool {
		return bytes.Compare(KVGenerationSnapshotKey(rows[i].owner), KVGenerationSnapshotKey(rows[j].owner)) < 0
	})
	for _, row := range rows {
		cont, err := fn(row.owner, row.generation)
		if err != nil || !cont {
			return err
		}
	}
	return nil
}

func (s *recordingLatestHotStore) WriteKVGeneration(owner common.Address, generation uint64) error {
	if s.writtenGeneration == nil {
		s.writtenGeneration = make(map[string]uint64)
	}
	s.writtenGeneration[owner.Hex()] = generation
	return nil
}

func (s *recordingLatestHotStore) IterateCode(fn func(hash common.Hash, code []byte) (bool, error)) error {
	s.codeIterations++
	rows := append([]latestHotCodeRow(nil), s.codes...)
	sort.Slice(rows, func(i, j int) bool {
		return bytes.Compare(CodeSnapshotKey(rows[i].hash), CodeSnapshotKey(rows[j].hash)) < 0
	})
	for _, row := range rows {
		cont, err := fn(row.hash, append([]byte(nil), row.code...))
		if err != nil || !cont {
			return err
		}
	}
	return nil
}

func (s *recordingLatestHotStore) WriteCode(hash common.Hash, code []byte) error {
	if s.writtenCode == nil {
		s.writtenCode = make(map[common.Hash][]byte)
	}
	s.writtenCode[hash] = append([]byte(nil), code...)
	return nil
}

func (s *recordingLatestHotStore) ReadCommitmentRoot() (common.Hash, bool, error) {
	if s.commitmentRoot == (common.Hash{}) {
		return common.Hash{}, false, nil
	}
	return s.commitmentRoot, true, nil
}

func (s *recordingLatestHotStore) IterateCommitmentDomain(logicalPrefix []byte, fn func(logicalKey, value []byte) (bool, error)) error {
	if s.commitmentIterations == nil {
		s.commitmentIterations = make(map[string]int)
	}
	s.commitmentIterations[string(logicalPrefix)]++
	var rows []latestHotCommitmentRow
	for _, row := range s.commitmentRows {
		if bytes.HasPrefix(row.key, logicalPrefix) {
			rows = append(rows, row)
		}
	}
	sort.Slice(rows, func(i, j int) bool {
		return bytes.Compare(rows[i].key, rows[j].key) < 0
	})
	for _, row := range rows {
		cont, err := fn(append([]byte(nil), row.key...), append([]byte(nil), row.value...))
		if err != nil || !cont {
			return err
		}
	}
	return nil
}

func (s *recordingLatestHotStore) WriteCommitmentDomain(logicalKey, value []byte) error {
	if s.writtenCommitment == nil {
		s.writtenCommitment = make(map[string][]byte)
	}
	s.writtenCommitment[string(logicalKey)] = append([]byte(nil), value...)
	return nil
}

func recordingLatestKVKey(owner common.Address, generation uint64, domain kvdomains.KVDomain, key []byte) string {
	return owner.Hex() + "/" + fmt.Sprintf("%d/%d/%s", generation, domain, key)
}

func latestStoreTestAddress(seed byte) common.Address {
	return common.BytesToAddress(append([]byte{common.AddressPrefixMainnet}, bytes.Repeat([]byte{seed}, common.AccountIDLength)...))
}
