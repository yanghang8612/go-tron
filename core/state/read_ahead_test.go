package state

import (
	"bytes"
	"strconv"
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
	if readsAfterWarmup != 10 {
		t.Fatalf("durable warmup reads = %d, want 4 accounts + metadata + code + 4 permission/resource rows", readsAfterWarmup)
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
	if stats.EnqueuedBlocks != 1 || stats.ProcessedBlocks != 1 || stats.Rows != 10 || stats.Present != 6 || stats.Missing != 4 || stats.QueuedBytes != 0 || stats.Errors != 0 {
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
	if readsAfterWarmup != 14 {
		t.Fatalf("durable warmup reads = %d, want 4 accounts + 6 asset rows + 4 permission/resource rows", readsAfterWarmup)
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
	if stats.Rows != 14 || stats.Present != 10 || stats.Missing != 4 || stats.Errors != 0 {
		t.Fatalf("stats = %+v", stats)
	}
}

func TestStateReadAheadTransferAssetCapPrioritizesV2Balances(t *testing.T) {
	const transfers = 120 // 240 V2 balance rows fit the new cap but not the old 128-row cap.
	const previousAssetRowLimit = 128
	disk := rawdb.NewMemoryDatabase()
	to, witness := readAheadAddress(0xe0), readAheadAddress(0xe1)
	owners := make([]tcommon.Address, transfers)
	transactions := make([]*corepb.Transaction, 0, transfers)
	for i := range owners {
		owners[i] = readAheadAddress(byte(i + 1))
	}
	addresses := append(append([]tcommon.Address(nil), owners...), to, witness, tcommon.SystemAccountAddress)
	for _, address := range addresses {
		encoded, err := (&StateAccountV3{Version: StateAccountVersion, AccountKVGeneration: 7}).Encode()
		if err != nil {
			t.Fatal(err)
		}
		if err := rawdb.WriteStateAccountLatest(disk, address, encoded); err != nil {
			t.Fatal(err)
		}
	}
	for i, owner := range owners {
		name := []byte(strconv.Itoa(1000000 + i))
		transfer, err := anypb.New(&contractpb.TransferAssetContract{OwnerAddress: owner.Bytes(), ToAddress: to.Bytes(), AssetName: name, Amount: 1})
		if err != nil {
			t.Fatal(err)
		}
		transactions = append(transactions, &corepb.Transaction{RawData: &corepb.TransactionRaw{Contract: []*corepb.Transaction_Contract{{Type: corepb.Transaction_Contract_TransferAssetContract, Parameter: transfer}}}})
		for _, address := range []tcommon.Address{owner, to} {
			for _, domain := range []kvdomains.KVDomain{kvdomains.AccountAsset, kvdomains.AccountAssetV2} {
				if err := rawdb.WriteStateKVLatest(disk, address, 7, domain, name, encodeAccountAuxInt64(10)); err != nil {
					t.Fatal(err)
				}
			}
		}
	}
	block := types.NewBlockFromPB(&corepb.Block{
		BlockHeader:  &corepb.BlockHeader{RawData: &corepb.BlockHeaderRaw{Number: 1, WitnessAddress: witness.Bytes()}},
		Transactions: transactions,
	})
	// Compare against the previous V2-first order at its 128-row asset cap.
	// The same canonical reads then expose how many V2 balances remained cold.
	oldBase := &readAheadCountingReader{KeyValueReader: disk}
	oldBuffer := blockbuffer.New(oldBase)
	oldBuffer.SetBaseReadCacheSize(1 << 20)
	oldPlan := rawdb.NewStatePrefetchPlan(previousAssetRowLimit)
	for i, owner := range owners {
		name := []byte(strconv.Itoa(1000000 + i))
		oldPlan.AddKV(owner, 7, kvdomains.AccountAssetV2, name)
		oldPlan.AddKV(to, 7, kvdomains.AccountAssetV2, name)
	}
	if err := oldPlan.Execute(oldBuffer, func(_ int, _ []byte, _ bool, _ error) error { return nil }); err != nil {
		t.Fatal(err)
	}
	oldBefore := oldBase.gets.Load()
	for i, owner := range owners {
		name := []byte(strconv.Itoa(1000000 + i))
		for _, address := range []tcommon.Address{owner, to} {
			if _, ok, err := rawdb.ReadStateKVLatestNoCopy(oldBuffer, address, 7, kvdomains.AccountAssetV2, name); err != nil || !ok {
				t.Fatalf("baseline V2 balance %d %s: ok=%t err=%v", i, address.Hex(), ok, err)
			}
		}
	}
	oldDurableReads := oldBase.gets.Load() - oldBefore
	if oldDurableReads != 2*transfers-previousAssetRowLimit {
		t.Fatalf("canonical V2 balance durable reads with previous cap = %d, want %d", oldDurableReads, 2*transfers-previousAssetRowLimit)
	}
	base := &readAheadCountingReader{KeyValueReader: disk}
	buffer := blockbuffer.New(base)
	buffer.SetBaseReadCacheSize(1 << 20)
	p := NewStateReadAhead(buffer, StateReadAheadConfig{Workers: 1})
	defer p.Close()
	if !p.EnqueueBlock(block, 1) {
		t.Fatal("read-ahead block rejected")
	}
	p.Wait()
	before := base.gets.Load()
	for i, owner := range owners {
		name := []byte(strconv.Itoa(1000000 + i))
		for _, address := range []tcommon.Address{owner, to} {
			value, ok, err := rawdb.ReadStateKVLatestNoCopy(buffer, address, 7, kvdomains.AccountAssetV2, name)
			if err != nil || !ok || !bytes.Equal(value, encodeAccountAuxInt64(10)) {
				t.Fatalf("V2 balance %d %s: %x/%t/%v", i, address.Hex(), value, ok, err)
			}
		}
	}
	if got := base.gets.Load(); got != before {
		t.Fatalf("canonical V2 balance reads reached durable base: before=%d after=%d", before, got)
	}
	t.Logf("canonical V2 balance durable reads: previous cap=%d, new cap=0", oldDurableReads)
	legacyName := []byte(strconv.Itoa(1000000))
	if _, ok, err := rawdb.ReadStateKVLatestNoCopy(buffer, owners[0], 7, kvdomains.AccountAsset, legacyName); err != nil || !ok || base.gets.Load() != before {
		t.Fatalf("first numeric legacy balance was not warmed: ok=%t err=%v", ok, err)
	}
	stats := p.Stats()
	if stats.AssetBalanceRows != maxTransferAssetPrefetchRows || stats.AssetV2Rows != 2*transfers || stats.AssetLegacyRows != maxTransferAssetPrefetchRows-2*transfers || stats.AssetCapBlocks != 1 || stats.AssetCappedAttempts == 0 || stats.AssetPlanFullRows != 0 || stats.Rows > 2*maxStateReadAheadPlanRows || stats.ProcessedBlocks != 1 || stats.CompletedBeforeApply != 1 || stats.QueuedBytes != 0 {
		t.Fatalf("cap and balance coverage stats = %+v", stats)
	}
}

func TestStateReadAheadTransferAssetRepeatedHintsDoNotConsumeCap(t *testing.T) {
	const transfers = 120
	disk := rawdb.NewMemoryDatabase()
	owner, to, witness := readAheadAddress(0x51), readAheadAddress(0x52), readAheadAddress(0x53)
	for _, address := range []tcommon.Address{owner, to, witness, tcommon.SystemAccountAddress} {
		encoded, err := (&StateAccountV3{Version: StateAccountVersion, AccountKVGeneration: 7}).Encode()
		if err != nil {
			t.Fatal(err)
		}
		if err := rawdb.WriteStateAccountLatest(disk, address, encoded); err != nil {
			t.Fatal(err)
		}
	}

	first := readAheadTransferAssetBlock(t, owner, to, witness, []byte("1000000"))
	transactions := make([]*corepb.Transaction, transfers)
	for i := range transactions {
		transactions[i] = first.Proto().Transactions[0]
	}
	block := types.NewBlockFromPB(&corepb.Block{
		BlockHeader:  &corepb.BlockHeader{RawData: &corepb.BlockHeaderRaw{Number: 1, WitnessAddress: witness.Bytes()}},
		Transactions: transactions,
	})
	p := NewStateReadAhead(disk, StateReadAheadConfig{Workers: 1})
	defer p.Close()
	if !p.EnqueueBlock(block, 1) {
		t.Fatal("read-ahead block rejected")
	}
	p.Wait()
	stats := p.Stats()
	if stats.AssetBalanceRows != 4 || stats.AssetV2Rows != 6 || stats.AssetLegacyRows != 6 || stats.AssetCapBlocks != 0 || stats.AssetCappedAttempts != 0 || stats.AssetPlanFullRows != 0 || stats.ProcessedBlocks != 1 {
		t.Fatalf("repeated transfer hints should result in only 12 distinct asset rows: %+v", stats)
	}
}

func TestStateReadAheadTransferAssetRespectsFullSubrowPlan(t *testing.T) {
	const owners = 1400 // Each owner contributes permission and resource subrows.
	disk := rawdb.NewMemoryDatabase()
	to, witness := readAheadAddress(0xe0), readAheadAddress(0xe1)
	to[len(to)-3], witness[len(witness)-3] = 1, 1
	transactions := make([]*corepb.Transaction, 0, owners+1)
	writeAccount := func(address tcommon.Address) {
		encoded, err := (&StateAccountV3{Version: StateAccountVersion, AccountKVGeneration: 7}).Encode()
		if err != nil {
			t.Fatal(err)
		}
		if err := rawdb.WriteStateAccountLatest(disk, address, encoded); err != nil {
			t.Fatal(err)
		}
	}
	for i := range owners {
		var owner tcommon.Address
		owner[0] = tcommon.AddressPrefixMainnet
		owner[len(owner)-2] = byte(i >> 8)
		owner[len(owner)-1] = byte(i)
		writeAccount(owner)
		transfer, err := anypb.New(&contractpb.TransferContract{OwnerAddress: owner.Bytes(), ToAddress: to.Bytes(), Amount: 1})
		if err != nil {
			t.Fatal(err)
		}
		transactions = append(transactions, &corepb.Transaction{RawData: &corepb.TransactionRaw{Contract: []*corepb.Transaction_Contract{{Type: corepb.Transaction_Contract_TransferContract, Parameter: transfer}}}})
	}
	for _, address := range []tcommon.Address{to, witness, tcommon.SystemAccountAddress} {
		writeAccount(address)
	}
	asset := readAheadTransferAssetBlock(t, readAheadAddress(0), to, witness, []byte("1000000"))
	transactions = append(transactions, asset.Proto().Transactions[0])
	block := types.NewBlockFromPB(&corepb.Block{
		BlockHeader:  &corepb.BlockHeader{RawData: &corepb.BlockHeaderRaw{Number: 1, WitnessAddress: witness.Bytes()}},
		Transactions: transactions,
	})
	p := NewStateReadAhead(disk, StateReadAheadConfig{Workers: 1})
	defer p.Close()
	if !p.EnqueueBlock(block, 1) {
		t.Fatal("read-ahead block rejected")
	}
	p.Wait()
	stats := p.Stats()
	if stats.Rows != owners+3+maxStateReadAheadPlanRows || stats.AssetBalanceRows != 0 || stats.AssetPlanFullRows == 0 || stats.AssetCapBlocks != 0 || stats.ProcessedBlocks != 1 {
		t.Fatalf("full subrow plan exceeded its existing bound or admitted asset reads: %+v", stats)
	}
}

func TestStateReadAheadCompletionTiming(t *testing.T) {
	for _, tc := range []struct {
		name       string
		markBefore bool
		wantBefore uint64
		wantAfter  uint64
	}{
		{name: "before apply", wantBefore: 1},
		{name: "after apply", markBefore: true, wantAfter: 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p := NewStateReadAhead(rawdb.NewMemoryDatabase(), StateReadAheadConfig{Workers: 1})
			defer p.Close()
			block := readAheadTestBlock(t, readAheadAddress(1), readAheadAddress(2), readAheadAddress(3), readAheadAddress(4))
			if tc.markBefore {
				p.MarkApplying(block.Number())
			}
			if !p.EnqueueBlock(block, 1) {
				t.Fatal("read-ahead block rejected")
			}
			p.Wait()
			stats := p.Stats()
			if stats.CompletedBeforeApply != tc.wantBefore || stats.CompletedAfterApply != tc.wantAfter {
				t.Fatalf("completion timing stats = %+v", stats)
			}
		})
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
