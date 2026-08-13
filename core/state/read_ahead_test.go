package state

import (
	"bytes"
	"sync/atomic"
	"testing"

	"github.com/ethereum/go-ethereum/ethdb"
	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/blockbuffer"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/state/kvdomains"
	"github.com/tronprotocol/go-tron/core/types"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	contractpb "github.com/tronprotocol/go-tron/proto/core/contract"
	"google.golang.org/protobuf/types/known/anypb"
)

type readAheadCountingReader struct {
	ethdb.KeyValueReader
	gets atomic.Int64
}

func (r *readAheadCountingReader) Get(key []byte) ([]byte, error) {
	r.gets.Add(1)
	return r.KeyValueReader.Get(key)
}

type readAheadBlockingReader struct {
	ethdb.KeyValueReader
	started chan struct{}
	release chan struct{}
	blocked atomic.Bool
}

func (r *readAheadBlockingReader) Get(key []byte) ([]byte, error) {
	if r.blocked.CompareAndSwap(false, true) {
		close(r.started)
		<-r.release
	}
	return r.KeyValueReader.Get(key)
}

func TestStateReadAheadWarmsCanonicalAccountAndContractReads(t *testing.T) {
	disk := rawdb.NewMemoryDatabase()
	owner := readAheadAddress(0x11)
	to := readAheadAddress(0x22)
	contract := readAheadAddress(0x33)
	witness := readAheadAddress(0x44)
	code := []byte{0x60, 0x00, 0x60, 0x00}
	codeHash := tcommon.Keccak256(code)

	for addr, envelope := range map[tcommon.Address]*StateAccountV3{
		owner:    {Version: StateAccountVersion},
		to:       {Version: StateAccountVersion},
		contract: {Version: StateAccountVersion, AccountKVGeneration: 7, CodeHash: codeHash},
		witness:  {Version: StateAccountVersion},
	} {
		encoded, err := envelope.Encode()
		if err != nil {
			t.Fatal(err)
		}
		if err := rawdb.WriteStateAccountLatest(disk, addr, encoded); err != nil {
			t.Fatal(err)
		}
	}
	if err := rawdb.WriteStateKVLatest(disk, contract, 7, kvdomains.ContractMetadata, contractMetaKVKey, []byte("contract-meta")); err != nil {
		t.Fatal(err)
	}
	if err := rawdb.WriteStateCode(disk, codeHash, code); err != nil {
		t.Fatal(err)
	}

	base := &readAheadCountingReader{KeyValueReader: disk}
	buffer := blockbuffer.New(base)
	buffer.SetBaseReadCacheSize(1 << 20)
	prefetcher := NewStateReadAhead(buffer, StateReadAheadConfig{Workers: 1, QueueBlocks: 4, QueueBytes: 1 << 20})
	prefetcher.Start()
	prefetcher.EnqueueBlocks([]*types.Block{readAheadTestBlock(t, owner, to, contract, witness)})
	prefetcher.Wait()
	defer prefetcher.Close()

	readsAfterWarmup := base.gets.Load()
	if readsAfterWarmup != 6 {
		t.Fatalf("durable warmup reads = %d, want 4 deduplicated accounts + metadata + code", readsAfterWarmup)
	}
	for _, addr := range []tcommon.Address{owner, to, contract, witness} {
		if _, ok, err := rawdb.ReadStateAccountLatestNoCopy(buffer, addr); err != nil || !ok {
			t.Fatalf("account %s = ok:%t err:%v", addr.Hex(), ok, err)
		}
	}
	if got := rawdb.ReadStateCodeImmutable(buffer, codeHash); !bytes.Equal(got, code) {
		t.Fatalf("code = %x, want %x", got, code)
	}
	if got, ok, err := rawdb.ReadStateKVLatestNoCopy(buffer, contract, 7, kvdomains.ContractMetadata, contractMetaKVKey); err != nil || !ok || string(got) != "contract-meta" {
		t.Fatalf("metadata = %q/%t/%v", got, ok, err)
	}
	if got := base.gets.Load(); got != readsAfterWarmup {
		t.Fatalf("canonical reads reached durable base after warmup: before=%d after=%d", readsAfterWarmup, got)
	}

	stats := prefetcher.Stats()
	if stats.EnqueuedBlocks != 1 || stats.ProcessedBlocks != 1 || stats.Rows != 6 || stats.Present != 6 || stats.Missing != 0 || stats.QueuedBytes != 0 || stats.Errors != 0 {
		t.Fatalf("stats = %+v", stats)
	}
}

func TestStateReadAheadWarmsTransferAssetPointReads(t *testing.T) {
	disk := rawdb.NewMemoryDatabase()
	owner := readAheadAddress(0x51)
	to := readAheadAddress(0x52)
	witness := readAheadAddress(0x53)
	const generation = uint64(9)
	for _, addr := range []tcommon.Address{owner, to, witness, tcommon.SystemAccountAddress} {
		encoded, err := (&StateAccountV3{Version: StateAccountVersion, AccountKVGeneration: generation}).Encode()
		if err != nil {
			t.Fatal(err)
		}
		if err := rawdb.WriteStateAccountLatest(disk, addr, encoded); err != nil {
			t.Fatal(err)
		}
	}

	assetName := []byte("DENSE")
	asset := testAssetIssueContract()
	asset.Name = assetName
	metadata, err := encodeAssetIssue(asset)
	if err != nil {
		t.Fatal(err)
	}
	legacyMeta := assetBytesKey(assetLegacyTag, assetName)
	rows := []struct {
		address tcommon.Address
		domain  kvdomains.KVDomain
		key     []byte
		value   []byte
	}{
		{tcommon.SystemAccountAddress, kvdomains.SystemAsset, legacyMeta, metadata},
		{tcommon.SystemAccountAddress, kvdomains.SystemAsset, assetBandwidthKey(legacyMeta), encodeAssetBandwidth(3, 4)},
		{owner, kvdomains.AccountAsset, assetName, encodeAccountAuxInt64(100)},
		{to, kvdomains.AccountAsset, assetName, encodeAccountAuxInt64(20)},
		{owner, kvdomains.AccountFreeAssetNetUsage, assetName, encodeAccountAuxInt64(5)},
		{owner, kvdomains.AccountAssetOperationTime, assetName, encodeAccountAuxInt64(6)},
	}
	for _, row := range rows {
		if err := rawdb.WriteStateKVLatest(disk, row.address, generation, row.domain, row.key, row.value); err != nil {
			t.Fatal(err)
		}
	}

	base := &readAheadCountingReader{KeyValueReader: disk}
	buffer := blockbuffer.New(base)
	buffer.SetBaseReadCacheSize(1 << 20)
	prefetcher := NewStateReadAhead(buffer, StateReadAheadConfig{Workers: 1, QueueBlocks: 4, QueueBytes: 1 << 20})
	prefetcher.Start()
	prefetcher.EnqueueBlocks([]*types.Block{readAheadTransferAssetBlock(t, owner, to, witness, assetName)})
	prefetcher.Wait()
	defer prefetcher.Close()

	readsAfterWarmup := base.gets.Load()
	if readsAfterWarmup != 10 {
		t.Fatalf("durable warmup reads = %d, want 4 account rows + 6 TransferAsset point rows", readsAfterWarmup)
	}
	for _, row := range rows {
		if _, ok, err := rawdb.ReadStateKVLatestNoCopy(buffer, row.address, generation, row.domain, row.key); err != nil || !ok {
			t.Fatalf("prefetched row address=%s domain=%#x key=%x ok=%t err=%v", row.address.Hex(), row.domain, row.key, ok, err)
		}
	}
	if got := base.gets.Load(); got != readsAfterWarmup {
		t.Fatalf("canonical TransferAsset reads reached durable base after warmup: before=%d after=%d", readsAfterWarmup, got)
	}
	stats := prefetcher.Stats()
	if stats.Rows != 10 || stats.Present != 10 || stats.Missing != 0 || stats.Errors != 0 {
		t.Fatalf("stats = %+v", stats)
	}
}

func TestStateReadAheadResetRejectsQueuedAndInFlightForkWork(t *testing.T) {
	disk := rawdb.NewMemoryDatabase()
	base := &readAheadBlockingReader{
		KeyValueReader: disk,
		started:        make(chan struct{}),
		release:        make(chan struct{}),
	}
	buffer := blockbuffer.New(base)
	buffer.SetBaseReadCacheSize(1 << 20)
	prefetcher := NewStateReadAhead(buffer, StateReadAheadConfig{Workers: 1, QueueBlocks: 4, QueueBytes: 1 << 20})
	prefetcher.Start()

	owner := readAheadAddress(0x11)
	to := readAheadAddress(0x22)
	contract := readAheadAddress(0x33)
	witness := readAheadAddress(0x44)
	block := readAheadTestBlock(t, owner, to, contract, witness)
	if got := prefetcher.EnqueueBlocks([]*types.Block{block, block}); got != 2 {
		t.Fatalf("enqueued blocks = %d, want 2", got)
	}
	<-base.started
	prefetcher.Reset()
	close(base.release)
	prefetcher.Wait()
	prefetcher.Close()

	stats := prefetcher.Stats()
	if stats.ProcessedBlocks != 0 || stats.StaleBlocks != 2 {
		t.Fatalf("reset stats = %+v, want processed=0 stale=2", stats)
	}
}

func TestStateReadAheadUsesRetainedWireSizeForQueueBudget(t *testing.T) {
	disk := rawdb.NewMemoryDatabase()
	prefetcher := NewStateReadAhead(disk, StateReadAheadConfig{Workers: 1, QueueBlocks: 1, QueueBytes: 1})
	block := readAheadTestBlock(t, readAheadAddress(0x11), readAheadAddress(0x22), readAheadAddress(0x33), readAheadAddress(0x44))
	if !prefetcher.EnqueueBlock(block, 1) {
		t.Fatal("one-byte retained payload was rejected")
	}
	prefetcher.Wait()
	if prefetcher.EnqueueBlock(block, 2) {
		t.Fatal("payload larger than queue byte budget was accepted")
	}
	prefetcher.Close()

	stats := prefetcher.Stats()
	if stats.EnqueuedBlocks != 1 || stats.EnqueuedBytes != 1 || stats.DroppedBlocks != 1 || stats.DroppedBytes != 2 || stats.QueuedBytes != 0 {
		t.Fatalf("wire-size stats = %+v", stats)
	}
}

func readAheadAddress(last byte) tcommon.Address {
	var addr tcommon.Address
	addr[0] = tcommon.AddressPrefixMainnet
	addr[len(addr)-1] = last
	return addr
}

func readAheadTestBlock(t *testing.T, owner, to, contract, witness tcommon.Address) *types.Block {
	t.Helper()
	transfer, err := anypb.New(&contractpb.TransferContract{OwnerAddress: owner.Bytes(), ToAddress: to.Bytes(), Amount: 1})
	if err != nil {
		t.Fatal(err)
	}
	trigger, err := anypb.New(&contractpb.TriggerSmartContract{OwnerAddress: owner.Bytes(), ContractAddress: contract.Bytes()})
	if err != nil {
		t.Fatal(err)
	}
	return types.NewBlockFromPB(&corepb.Block{
		BlockHeader: &corepb.BlockHeader{RawData: &corepb.BlockHeaderRaw{Number: 1, WitnessAddress: witness.Bytes()}},
		Transactions: []*corepb.Transaction{
			{RawData: &corepb.TransactionRaw{Contract: []*corepb.Transaction_Contract{{Type: corepb.Transaction_Contract_TransferContract, Parameter: transfer}}}},
			{RawData: &corepb.TransactionRaw{Contract: []*corepb.Transaction_Contract{{Type: corepb.Transaction_Contract_TriggerSmartContract, Parameter: trigger}}}},
		},
	})
}

func readAheadTransferAssetBlock(t *testing.T, owner, to, witness tcommon.Address, assetName []byte) *types.Block {
	t.Helper()
	transfer, err := anypb.New(&contractpb.TransferAssetContract{OwnerAddress: owner.Bytes(), ToAddress: to.Bytes(), AssetName: assetName, Amount: 1})
	if err != nil {
		t.Fatal(err)
	}
	return types.NewBlockFromPB(&corepb.Block{
		BlockHeader: &corepb.BlockHeader{RawData: &corepb.BlockHeaderRaw{Number: 1, WitnessAddress: witness.Bytes()}},
		Transactions: []*corepb.Transaction{{RawData: &corepb.TransactionRaw{Contract: []*corepb.Transaction_Contract{{
			Type: corepb.Transaction_Contract_TransferAssetContract, Parameter: transfer,
		}}}}},
	})
}
