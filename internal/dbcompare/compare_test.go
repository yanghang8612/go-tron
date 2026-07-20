package dbcompare

import (
	"encoding/binary"
	"slices"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/state"
	"github.com/tronprotocol/go-tron/core/types"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	"google.golang.org/protobuf/proto"
)

func TestNormalizePropertyKey(t *testing.T) {
	tests := map[string]string{
		"latest_block_header_number":  "latest_block_header_number",
		"LATEST_SOLIDIFIED_BLOCK_NUM": "latest_solidified_block_num",
		" ALLOW_SAME_TOKEN_NAME":      "allow_same_token_name",
		"TOTAL_CREATE_WITNESS_FEE":    "total_create_witness_cost",
	}
	for input, want := range tests {
		if got := normalizePropertyKey([]byte(input)); got != want {
			t.Errorf("normalizePropertyKey(%q) = %q, want %q", input, got, want)
		}
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
	if err := c.compareProperties(sdb, state.NewDynamicProperties(), java); err != nil {
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
