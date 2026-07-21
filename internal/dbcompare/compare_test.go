package dbcompare

import (
	"encoding/binary"
	"fmt"
	"slices"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/ethdb"
	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/state"
	"github.com/tronprotocol/go-tron/core/types"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	contractpb "github.com/tronprotocol/go-tron/proto/core/contract"
	"google.golang.org/protobuf/proto"
)

func TestNormalizePropertyKey(t *testing.T) {
	tests := map[string]string{
		"latest_block_header_number":  "latest_block_header_number",
		"LATEST_SOLIDIFIED_BLOCK_NUM": "latest_solidified_block_num",
		" ALLOW_SAME_TOKEN_NAME":      "allow_same_token_name",
		"TOTAL_CREATE_WITNESS_FEE":    "total_create_witness_cost",
		"ALLOW_TVM_SOLIDITY_059":      "allow_tvm_solidity059",
	}
	for input, want := range tests {
		if got := normalizePropertyKey([]byte(input)); got != want {
			t.Errorf("normalizePropertyKey(%q) = %q, want %q", input, got, want)
		}
	}
}

func TestNormalizeJavaBlockFilledSlots(t *testing.T) {
	got := normalizeJavaPropertyValue("block_filled_slots", []byte("1010"))
	if !slices.Equal(got, []byte{1, 0, 1, 0}) {
		t.Fatalf("normalized slots = %v", got)
	}
	invalid := []byte{'1', 2}
	if got := normalizeJavaPropertyValue("block_filled_slots", invalid); !slices.Equal(got, invalid) {
		t.Fatalf("invalid slots changed: %v", got)
	}
}

func TestCompareByteStoreReportsProgressLifecycle(t *testing.T) {
	java := rawdb.NewMemoryDatabase()
	for _, key := range []string{"key-1", "key-2"} {
		if err := java.Put([]byte(key), []byte("value")); err != nil {
			t.Fatal(err)
		}
	}
	var events []ProgressEvent
	c := &comparer{
		opts: Options{
			ProgressInterval: time.Nanosecond,
			Progress: func(event ProgressEvent) {
				events = append(events, event)
			},
		},
		report: new(Report),
	}
	if err := c.compareByteStore("test-store", "state", java, func(key []byte) ([]byte, bool, error) {
		return []byte("value"), true, nil
	}); err != nil {
		t.Fatal(err)
	}

	phases := make([]string, len(events))
	for i, event := range events {
		phases[i] = event.Phase
	}
	if !slices.Equal(phases, []string{"start", "progress", "done"}) {
		t.Fatalf("progress phases = %v, want start, progress, done", phases)
	}
	if events[1].Rows != 1 || events[1].Result.Equal != 1 || events[2].Result.Equal != 2 {
		t.Fatalf("progress events = %+v", events)
	}
	if events[1].Snapshot == nil || events[1].Snapshot.Progress == nil ||
		events[1].Snapshot.Progress.CurrentResult == nil || events[1].Snapshot.Progress.CurrentResult.Equal != 1 {
		t.Fatalf("progress snapshot = %+v", events[1].Snapshot)
	}
}

func TestProgressSnapshotCapsDifferencesBeforeCopyAndSort(t *testing.T) {
	const retained = 100_000
	differences := make([]Difference, retained)
	for i := range differences {
		differences[i] = Difference{Store: "delegation", Key: fmt.Sprintf("%06d", retained-i)}
	}
	limit := 10
	c := &comparer{
		opts:   Options{ProgressMaxDifferences: &limit},
		report: &Report{Differences: differences},
	}

	snapshot := c.progressSnapshot(ProgressEvent{Phase: "progress", Store: "delegation"})
	if len(snapshot.Differences) != limit {
		t.Fatalf("snapshot differences=%d, want %d", len(snapshot.Differences), limit)
	}
	if len(c.report.Differences) != retained {
		t.Fatalf("source differences=%d, want %d", len(c.report.Differences), retained)
	}
	if snapshot.Differences[0].Key != "099991" || snapshot.Differences[limit-1].Key != "100000" {
		t.Fatalf("bounded snapshot was not deterministically sorted: first=%s last=%s",
			snapshot.Differences[0].Key, snapshot.Differences[limit-1].Key)
	}
}

func TestCompareByteStoreParallelPreservesCounts(t *testing.T) {
	java := rawdb.NewMemoryDatabase()
	for i := 0; i < 8; i++ {
		if err := java.Put([]byte{byte(i)}, []byte{byte(i)}); err != nil {
			t.Fatal(err)
		}
	}

	started := make(chan struct{}, 4)
	release := make(chan struct{})
	go func() {
		for i := 0; i < 4; i++ {
			<-started
		}
		close(release)
	}()
	var calls atomic.Int32
	c := &comparer{opts: Options{MaxDifferences: 10, Workers: 4}, report: new(Report)}
	err := c.compareByteStoreParallel("parallel", "state", java, func(key []byte) ([]byte, bool, error) {
		if calls.Add(1) <= 4 {
			started <- struct{}{}
			<-release
		}
		switch key[0] {
		case 1:
			return []byte("different"), true, nil
		case 2:
			return nil, false, nil
		case 3:
			return nil, false, fmt.Errorf("bad key")
		default:
			return []byte{key[0]}, true, nil
		}
	})
	if err != nil {
		t.Fatal(err)
	}
	result := c.report.Stores[0]
	if result.Compared != 6 || result.Equal != 5 || result.Different != 1 ||
		result.MissingGtron != 1 || result.Invalid != 1 {
		t.Fatalf("parallel result=%+v", result)
	}
	if len(c.report.Differences) != 3 {
		t.Fatalf("differences=%d, want 3", len(c.report.Differences))
	}
}

func TestCompareDelegationParallelStateReads(t *testing.T) {
	gtron := rawdb.NewMemoryDatabase()
	disk := state.NewDatabase(rawdb.WrapKeyValueStore(gtron))
	sdb, err := state.New(tcommon.Hash{}, disk)
	if err != nil {
		t.Fatal(err)
	}
	java := rawdb.NewMemoryDatabase()
	const rows = 256
	for i := 0; i < rows; i++ {
		cycle := int64(i + 1)
		addr := address(byte(i))
		if err := sdb.WriteCycleReward(cycle, addr, cycle*10); err != nil {
			t.Fatal(err)
		}
		var value [8]byte
		binary.BigEndian.PutUint64(value[:], uint64(cycle*10))
		key := []byte(fmt.Sprintf("%d-%x-reward", cycle, addr))
		if err := java.Put(key, value[:]); err != nil {
			t.Fatal(err)
		}
	}
	root, err := sdb.Commit()
	if err != nil {
		t.Fatal(err)
	}
	reopened, err := state.New(root, disk)
	if err != nil {
		t.Fatal(err)
	}

	c := &comparer{opts: Options{MaxDifferences: 10, Workers: 8}, report: new(Report)}
	if err := c.compareDelegation(reopened, java); err != nil {
		t.Fatal(err)
	}
	result := c.report.Stores[0]
	if result.Compared != rows || result.Equal != rows || result.Mismatches() != 0 {
		t.Fatalf("delegation result=%+v", result)
	}
}

func TestNormalizeAccountIgnoresSplitAssetStorageFields(t *testing.T) {
	inline := &corepb.Account{
		Address: address(1), Asset: map[string]int64{"OLD": 7}, AssetV2: map[string]int64{"1000001": 9},
	}
	optimized := proto.Clone(inline).(*corepb.Account)
	optimized.Asset = nil
	optimized.AssetV2 = nil
	optimized.AssetOptimized = true
	if !proto.Equal(normalizeAccountForStoreComparison(inline), normalizeAccountForStoreComparison(optimized)) {
		t.Fatal("account asset physical split was not normalized")
	}
}

func TestNormalizeAccountIgnoresEmptyAccountResourcePresence(t *testing.T) {
	without := &corepb.Account{Address: address(1)}
	withEmpty := proto.Clone(without).(*corepb.Account)
	withEmpty.AccountResource = &corepb.Account_AccountResource{}
	if !proto.Equal(normalizeAccountForStoreComparison(without), normalizeAccountForStoreComparison(withEmpty)) {
		t.Fatal("empty account_resource presence was not normalized")
	}
}

func TestUnknownJavaPropertyIsMismatchNotSkip(t *testing.T) {
	gtron := rawdb.NewMemoryDatabase()
	disk := state.NewDatabase(rawdb.WrapKeyValueStore(gtron))
	sdb, err := state.New([32]byte{}, disk)
	if err != nil {
		t.Fatal(err)
	}
	java := rawdb.NewMemoryDatabase()
	if err := java.Put([]byte("FUTURE_CONSENSUS_FLAG"), make([]byte, 8)); err != nil {
		t.Fatal(err)
	}
	c := &comparer{opts: Options{MaxDifferences: 10}, report: new(Report)}
	if err := c.compareProperties(gtron, sdb, state.NewDynamicProperties(), java); err != nil {
		t.Fatal(err)
	}
	result := c.report.Stores[0]
	if result.MissingGtron != 1 || result.Skipped != 0 {
		t.Fatalf("property result = %+v, want one missing_gtron and zero skipped", result)
	}
}

func TestCompareAccountsDetectsValueAndReverseSetDifferences(t *testing.T) {
	gtron := rawdb.NewMemoryDatabase()
	java := rawdb.NewMemoryDatabase()

	matching := &corepb.Account{Address: address(1), Balance: 10}
	differentJava := &corepb.Account{Address: address(2), Balance: 20}
	differentGtron := &corepb.Account{Address: address(2), Balance: 21}
	extraGtron := &corepb.Account{Address: address(3), Balance: 30}
	writeGtronAccount(t, gtron, matching)
	writeGtronAccount(t, gtron, differentGtron)
	writeGtronAccount(t, gtron, extraGtron)
	writeJavaProto(t, java, matching.Address, matching)
	writeJavaProto(t, java, differentJava.Address, differentJava)

	disk := state.NewDatabase(rawdb.WrapKeyValueStore(gtron))
	sdb, err := state.New([32]byte{}, disk)
	if err != nil {
		t.Fatal(err)
	}
	c := &comparer{opts: Options{MaxDifferences: 10, ReverseAccounts: true}, report: new(Report)}
	if err := c.compareAccounts(gtron, sdb, java); err != nil {
		t.Fatal(err)
	}
	got := c.report.Stores[0]
	if got.Compared != 2 || got.Equal != 1 || got.Different != 1 || got.MissingJava != 1 {
		t.Fatalf("account result = %+v", got)
	}
}

func TestCompareContractsUsesSerializedEqualityFastPath(t *testing.T) {
	gtron := rawdb.NewMemoryDatabase()
	disk := state.NewDatabase(rawdb.WrapKeyValueStore(gtron))
	sdb, err := state.New([32]byte{}, disk)
	if err != nil {
		t.Fatal(err)
	}
	java := rawdb.NewMemoryDatabase()
	const contracts = 64
	for i := 1; i <= contracts; i++ {
		addr := tcommon.BytesToAddress(address(byte(i)))
		contract := &contractpb.SmartContract{
			OriginAddress: addr.Bytes(), ContractAddress: addr.Bytes(), Name: fmt.Sprintf("fast-path-%d", i),
		}
		sdb.SetContract(addr, contract)
		writeJavaProto(t, java, addr.Bytes(), contract)
	}
	if _, err := sdb.Commit(); err != nil {
		t.Fatal(err)
	}

	c := &comparer{opts: Options{MaxDifferences: 10, Workers: 4}, report: new(Report)}
	if err := c.compareContracts(gtron, java); err != nil {
		t.Fatal(err)
	}
	got := c.report.Stores[0]
	if got.Compared != contracts || got.Equal != contracts || got.Mismatches() != 0 {
		t.Fatalf("contract result = %+v", got)
	}
}

func TestCompareContractsIgnoresInlineABIPlacement(t *testing.T) {
	gtron := rawdb.NewMemoryDatabase()
	disk := state.NewDatabase(rawdb.WrapKeyValueStore(gtron))
	sdb, err := state.New([32]byte{}, disk)
	if err != nil {
		t.Fatal(err)
	}
	java := rawdb.NewMemoryDatabase()
	addr := tcommon.BytesToAddress(address(1))
	contract := &contractpb.SmartContract{
		OriginAddress:   addr.Bytes(),
		ContractAddress: addr.Bytes(),
		Name:            "abi-placement",
		Abi:             &contractpb.SmartContract_ABI{Entrys: []*contractpb.SmartContract_ABI_Entry{{Name: "f"}}},
	}
	sdb.SetContract(addr, contract)
	javaContract := proto.Clone(contract).(*contractpb.SmartContract)
	javaContract.Abi = nil // java ContractStore moves ABI to the separate store.
	writeJavaProto(t, java, addr.Bytes(), javaContract)
	if _, err := sdb.Commit(); err != nil {
		t.Fatal(err)
	}

	c := &comparer{opts: Options{MaxDifferences: 10, Workers: 2}, report: new(Report)}
	if err := c.compareContracts(gtron, java); err != nil {
		t.Fatal(err)
	}
	got := c.report.Stores[0]
	if got.Compared != 1 || got.Equal != 1 || got.Mismatches() != 0 {
		t.Fatalf("contract result = %+v", got)
	}
}

func TestCompareContractsProjectsCodeHashFromAccountEnvelope(t *testing.T) {
	gtron := rawdb.NewMemoryDatabase()
	disk := state.NewDatabase(rawdb.WrapKeyValueStore(gtron))
	sdb, err := state.New([32]byte{}, disk)
	if err != nil {
		t.Fatal(err)
	}
	java := rawdb.NewMemoryDatabase()
	addr := tcommon.BytesToAddress(address(1))
	contract := &contractpb.SmartContract{
		OriginAddress: addr.Bytes(), ContractAddress: addr.Bytes(), Name: "code-hash-placement",
	}
	sdb.SetContract(addr, contract)
	code := []byte{0x60, 0x00, 0x60, 0x01}
	sdb.SetCode(addr, code)
	javaContract := proto.Clone(contract).(*contractpb.SmartContract)
	javaContract.CodeHash = tcommon.Keccak256(code).Bytes()
	writeJavaProto(t, java, addr.Bytes(), javaContract)
	if _, err := sdb.Commit(); err != nil {
		t.Fatal(err)
	}

	c := &comparer{opts: Options{MaxDifferences: 10, Workers: 2}, report: new(Report)}
	if err := c.compareContracts(gtron, java); err != nil {
		t.Fatal(err)
	}
	got := c.report.Stores[0]
	if got.Compared != 1 || got.Equal != 1 || got.Mismatches() != 0 {
		t.Fatalf("contract result = %+v", got)
	}
}

func TestAddProtoDiffSkipsFormattingAfterLimit(t *testing.T) {
	c := &comparer{
		opts:   Options{MaxDifferences: 1},
		report: &Report{Differences: []Difference{{Store: "existing"}}},
	}
	// Nil messages would panic inside cmp/protocmp transformation. Reaching
	// this assertion proves the cap is checked before expensive formatting.
	c.addProtoDiff("contract", "key", nil, nil)
	if len(c.report.Differences) != 1 {
		t.Fatalf("differences = %d, want 1", len(c.report.Differences))
	}
}

func TestJavaHeadAcceptsJavaMixedCasePropertyKey(t *testing.T) {
	db := rawdb.NewMemoryDatabase()
	var value [8]byte
	binary.BigEndian.PutUint64(value[:], 123)
	if err := db.Put([]byte("latest_block_header_number"), value[:]); err != nil {
		t.Fatal(err)
	}
	got, err := javaHead(db)
	if err != nil || got != 123 {
		t.Fatalf("javaHead = %d, %v", got, err)
	}
}

func TestEverySupportedStateStoreHasAnAdapter(t *testing.T) {
	gtron := rawdb.NewMemoryDatabase()
	disk := state.NewDatabase(rawdb.WrapKeyValueStore(gtron))
	sdb, err := state.New([32]byte{}, disk)
	if err != nil {
		t.Fatal(err)
	}
	c := &comparer{opts: Options{MaxDifferences: 10}, report: new(Report)}
	java := &JavaStores{stores: make(map[string]ethdb.KeyValueStore)}
	if err := c.compareAdditionalStateStores(gtron, sdb, java); err != nil {
		t.Fatal(err)
	}
	// The original core adapters run directly from Compare.
	for _, name := range []string{"abi", "account", "code", "contract", "properties", "witness"} {
		c.report.Stores = append(c.report.Stores, StoreResult{Name: name})
	}
	got := make([]string, 0, len(c.report.Stores))
	for _, result := range c.report.Stores {
		got = append(got, result.Name)
	}
	for _, spec := range javaStoreSpecs {
		if spec.State && spec.Compare && !slices.Contains(got, spec.Name) {
			t.Errorf("supported state store %q has no invoked adapter", spec.Name)
		}
	}
}

func TestCoverageGateRejectsUnsupportedAndUnknownStores(t *testing.T) {
	java := &JavaStores{
		stores:     map[string]ethdb.KeyValueStore{"account": rawdb.NewMemoryDatabase()},
		discovered: []string{"account", "staker", "future-consensus-store"},
	}
	c := &comparer{report: &Report{Stores: []StoreResult{{Name: "account"}}}}
	c.auditJavaStoreCoverage(java)
	c.finalizeStateCoverage(java)
	if c.report.StateCoverageComplete {
		t.Fatal("coverage unexpectedly complete")
	}
	if !slices.Contains(c.report.UnsupportedStateStores, "staker") {
		t.Fatalf("unsupported stores = %v, want staker", c.report.UnsupportedStateStores)
	}
	if !slices.Contains(c.report.UnclassifiedStores, "future-consensus-store") {
		t.Fatalf("unclassified stores = %v, want future-consensus-store", c.report.UnclassifiedStores)
	}
}

func TestDecodeJavaMarketKey(t *testing.T) {
	key := make([]byte, 54)
	copy(key[:19], []byte("1000001"))
	copy(key[19:38], []byte("_"))
	binary.BigEndian.PutUint64(key[38:46], 2)
	binary.BigEndian.PutUint64(key[46:54], 3)
	sell, buy, price, err := decodeJavaMarketKey(key, true)
	if err != nil {
		t.Fatal(err)
	}
	if string(sell) != "1000001" || string(buy) != "_" || binary.BigEndian.Uint64(price[:8]) != 2 || binary.BigEndian.Uint64(price[8:]) != 3 {
		t.Fatalf("decoded market key = sell=%q buy=%q price=%x", sell, buy, price)
	}
}

func TestDelegationLogicalKeyAcceptsNegativeBrokerageCycle(t *testing.T) {
	addr := address(0x7a)
	key := []byte(fmt.Sprintf("-1-%x-brokerage", addr))
	got, err := delegationLogicalKey(key)
	if err != nil {
		t.Fatal(err)
	}
	want := rawdb.CycleBrokerageStateKey(-1, addr)
	if string(got) != string(want) {
		t.Fatalf("logical key = %x, want %x", got, want)
	}
}

func TestDifferenceRetentionCapsEachStoreIndependently(t *testing.T) {
	c := &comparer{
		opts:   Options{MaxDifferences: 3, MaxDifferencesPerStore: 2},
		report: new(Report),
	}
	for i := range 4 {
		c.addDiff("delegation", fmt.Sprint(i), "different", "")
	}
	for i := range 3 {
		c.addDiff("contract-state", fmt.Sprint(i), "different", "")
	}
	if len(c.report.Differences) != 4 {
		t.Fatalf("retained differences = %d, want 4", len(c.report.Differences))
	}
	if c.differencesByStore["delegation"] != 2 || c.differencesByStore["contract-state"] != 2 {
		t.Fatalf("per-store counts = %v", c.differencesByStore)
	}
	c.finalizeDifferenceSamples()
	if len(c.report.Differences) != 3 {
		t.Fatalf("globally retained differences = %d, want 3", len(c.report.Differences))
	}
	stores := map[string]int{}
	for _, difference := range c.report.Differences {
		stores[difference.Store]++
	}
	if stores["delegation"] == 0 || stores["contract-state"] == 0 {
		t.Fatalf("global samples did not preserve both stores: %v", stores)
	}
}

func address(last byte) []byte {
	addr := make([]byte, 21)
	addr[0] = 0x41
	addr[20] = last
	return addr
}

func writeGtronAccount(t *testing.T, db ethdb.KeyValueWriter, account *corepb.Account) {
	t.Helper()
	protoBytes, err := proto.Marshal(account)
	if err != nil {
		t.Fatal(err)
	}
	envelope, err := (&state.StateAccountV2{
		Version:       state.StateAccountVersion,
		AccountProto:  protoBytes,
		AccountKVRoot: state.EmptyKVRoot,
	}).Encode()
	if err != nil {
		t.Fatal(err)
	}
	if err := rawdb.WriteStateAccountLatest(db, types.NewAccountFromPB(account).Address(), envelope); err != nil {
		t.Fatal(err)
	}
}

func writeJavaProto(t *testing.T, db ethdb.KeyValueWriter, key []byte, message proto.Message) {
	t.Helper()
	value, err := proto.Marshal(message)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Put(key, value); err != nil {
		t.Fatal(err)
	}
}
