package core

import (
	"bytes"
	"encoding/binary"
	"errors"
	"math/big"
	"math/rand"
	"reflect"
	"strings"
	"testing"

	ethrawdb "github.com/ethereum/go-ethereum/core/rawdb"
	ethtypes "github.com/ethereum/go-ethereum/core/types"
	"github.com/tronprotocol/go-tron/actuator"
	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/forks"
	tronrawdb "github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/state"
	"github.com/tronprotocol/go-tron/core/state/kvdomains"
	"github.com/tronprotocol/go-tron/core/types"
	"github.com/tronprotocol/go-tron/params"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	contractpb "github.com/tronprotocol/go-tron/proto/core/contract"
	"github.com/tronprotocol/go-tron/vm"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
)

var transactionInfoBenchmarkSink *corepb.TransactionInfo

// TestActuatorResultFieldPolicy is deliberately exhaustive. TransactionInfo is
// a consensus-adjacent persisted carrier consumed by RPC, replay diagnostics,
// and the parallel publication oracle. A new actuator.Result field must be
// explicitly classified here so it cannot silently disappear from the receipt
// path like InternalTransactions once did.
func TestActuatorResultFieldPolicy(t *testing.T) {
	policies := map[string]string{
		"Fee":                           "transaction-info",
		"EnergyUsageTotal":              "resource-receipt",
		"EnergyPenaltyTotal":            "resource-receipt",
		"EnergyUsed":                    "resource-receipt",
		"EnergyFee":                     "resource-receipt-and-packing-fee",
		"OriginEnergyUsage":             "resource-receipt",
		"VMExecutionDuration":           "execution-only-telemetry",
		"VMRawEnergyUsage":              "execution-only-telemetry",
		"CallerEnergyLeft":              "execution-only",
		"OriginEnergyLeft":              "execution-only",
		"HasCallerEnergyLeft":           "execution-only",
		"HasOriginEnergyLeft":           "execution-only",
		"energyPreCharges":              "execution-only",
		"NetUsage":                      "resource-receipt",
		"NetFee":                        "resource-receipt-and-transaction-info",
		"NetFeeForBandwidth":            "packing-fee-policy",
		"AssetIssueID":                  "transaction-info-and-result",
		"WithdrawAmount":                "transaction-info-and-result",
		"UnfreezeAmount":                "transaction-info-and-result",
		"WithdrawExpireAmount":          "transaction-info-and-result",
		"CancelUnfreezeV2Amount":        "transaction-info-and-result",
		"ExchangeReceivedAmount":        "transaction-info-and-result",
		"ExchangeInjectAnotherAmount":   "transaction-info-and-result",
		"ExchangeWithdrawAnotherAmount": "transaction-info-and-result",
		"ShieldedTransactionFee":        "transaction-info-and-result",
		"ExchangeID":                    "transaction-info-and-result",
		"OrderID":                       "transaction-info-and-result",
		"OrderDetails":                  "transaction-info-and-result",
		"ContractResult":                "transaction-info",
		"ContractResultPresent":         "transaction-info-presence",
		"ContractAddress":               "transaction-info",
		"contractAddress":               "result-owned-address-storage",
		"Logs":                          "transaction-info",
		"InternalTransactions":          "transaction-info",
		"ContractRet":                   "resource-receipt-and-status",
		"ResMessage":                    "transaction-info-on-failure",
	}
	typ := reflect.TypeOf(actuator.Result{})
	if len(policies) != typ.NumField() {
		t.Fatalf("actuator.Result policy covers %d fields, struct has %d", len(policies), typ.NumField())
	}
	for i := 0; i < typ.NumField(); i++ {
		field := typ.Field(i).Name
		if policies[field] == "" {
			t.Fatalf("actuator.Result field %q has no receipt policy", field)
		}
	}
}

// TestPersistedReceiptFieldPolicy complements the Go result policy with the
// wire schema. Upstream can add receipt fields without changing actuator.Result;
// requiring an explicit policy here prevents a generated-protobuf update from
// silently creating another always-zero persisted field.
func TestPersistedReceiptFieldPolicy(t *testing.T) {
	tests := []struct {
		name     string
		message  proto.Message
		policies map[string]string
	}{
		{
			name:    "TransactionInfo",
			message: &corepb.TransactionInfo{},
			policies: map[string]string{
				"id":                               "transaction-hash",
				"fee":                              "actuator-fee-plus-net-fee",
				"blockNumber":                      "block-context",
				"blockTimeStamp":                   "block-context",
				"contractResult":                   "actuator-result",
				"contract_address":                 "actuator-result",
				"receipt":                          "resource-receipt",
				"log":                              "actuator-result",
				"result":                           "derived-status",
				"resMessage":                       "actuator-result-on-failure",
				"assetIssueID":                     "actuator-result",
				"withdraw_amount":                  "actuator-result",
				"unfreeze_amount":                  "actuator-result",
				"internal_transactions":            "actuator-result",
				"exchange_received_amount":         "actuator-result",
				"exchange_inject_another_amount":   "actuator-result",
				"exchange_withdraw_another_amount": "actuator-result",
				"exchange_id":                      "actuator-result",
				"shielded_transaction_fee":         "actuator-result",
				"orderId":                          "actuator-result",
				"orderDetails":                     "actuator-result",
				"packingFee":                       "derived-fee-pool-policy",
				"withdraw_expire_amount":           "actuator-result",
				"cancel_unfreezeV2_amount":         "actuator-result",
			},
		},
		{
			name:    "ResourceReceipt",
			message: &corepb.ResourceReceipt{},
			policies: map[string]string{
				"energy_usage":         "actuator-result",
				"energy_fee":           "actuator-result",
				"origin_energy_usage":  "actuator-result",
				"energy_usage_total":   "actuator-result",
				"net_usage":            "bandwidth-result",
				"net_fee":              "bandwidth-result",
				"result":               "vm-contract-ret",
				"energy_penalty_total": "actuator-result",
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fields := test.message.ProtoReflect().Descriptor().Fields()
			if len(test.policies) != fields.Len() {
				t.Fatalf("policy covers %d fields, wire message has %d", len(test.policies), fields.Len())
			}
			for i := 0; i < fields.Len(); i++ {
				name := string(fields.Get(i).Name())
				if test.policies[name] == "" {
					t.Fatalf("wire field %q has no persisted receipt policy", name)
				}
			}
		})
	}
}

func BenchmarkTransactionInfoLogBuild(b *testing.B) {
	contractAddr := testProcessorAddr(2)
	tx := makeTestTriggerTx(1, contractAddr, nil)
	for _, tc := range []struct {
		name       string
		logCount   int
		topicCount int
	}{
		{name: "logs_1_topics_1", logCount: 1, topicCount: 1},
		{name: "logs_4_topics_4", logCount: 4, topicCount: 4},
	} {
		b.Run(tc.name, func(b *testing.B) {
			result := &actuator.Result{ContractRet: int32(corepb.Transaction_Result_SUCCESS)}
			result.Logs = make([]vm.Log, tc.logCount)
			for i := range result.Logs {
				result.Logs[i] = vm.Log{
					Address: contractAddr,
					Data:    bytes.Repeat([]byte{byte(i + 1)}, 64),
					Topics:  make([][]byte, tc.topicCount),
				}
				for topic := range result.Logs[i].Topics {
					result.Logs[i].Topics[topic] = bytes.Repeat([]byte{byte(topic + 1)}, 32)
				}
			}
			slot := new(transactionInfoSlot)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				transactionInfoBenchmarkSink = slot.build(tx, result, 1, 3000, false)
			}
		})
	}
}

func TestBuildTransactionInfoFromOpcodeLogTopics(t *testing.T) {
	statedb := newTestState(t)
	caller := testProcessorAddr(1)
	contractAddr := testProcessorAddr(0x80)
	statedb.GetOrCreateAccount(caller)
	// Push topic 4 through topic 1, followed by zero data size and offset, so
	// LOG4 pops the topics back in canonical topic 1 through topic 4 order.
	statedb.SetCode(contractAddr, []byte{
		0x60, 0x04,
		0x60, 0x03,
		0x60, 0x02,
		0x60, 0x01,
		0x60, 0x00,
		0x60, 0x00,
		0xa4,
		0x00,
	})

	slot := new(transactionInfoSlot)
	slot.executionLogArena.Reset()
	tvm := vm.NewTVM(statedb, nil, caller, 1, 1000, tcommon.Address{}, 1, vm.TVMConfig{})
	tvm.SetExecutionLogArena(&slot.executionLogArena)
	_, _, err := tvm.Call(caller, contractAddr, nil, 1_000_000, 0)
	if err != nil {
		t.Fatalf("execute LOG4: %v", err)
	}
	if len(tvm.Logs) != 1 {
		t.Fatalf("opcode logs = %d, want 1", len(tvm.Logs))
	}
	result := &actuator.Result{
		ContractRet: int32(corepb.Transaction_Result_SUCCESS),
		Logs:        tvm.Logs,
	}
	vm.ReleaseTVM(tvm)

	tx := makeTestTriggerTx(1, contractAddr, nil)
	info := slot.build(tx, result, 1, 3000, false)
	// The block path releases the VM's Log structs immediately after copying
	// their slice headers into TransactionInfo. Receipt topics must retain the
	// shared immutable payload without depending on that backing array.
	vm.ReleaseExecutionLogs(result.Logs)
	result.Logs = nil
	if len(info.Log) != 1 {
		t.Fatalf("receipt logs = %d, want 1", len(info.Log))
	}
	if len(info.Log[0].Topics) != 4 {
		t.Fatalf("receipt topics = %d, want 4", len(info.Log[0].Topics))
	}
	for i, topic := range info.Log[0].Topics {
		if len(topic) != 32 || topic[31] != byte(i+1) {
			t.Fatalf("topic %d = %x, want 32-byte value %d", i, topic, i+1)
		}
	}
}

func newTestState(t testing.TB) *state.StateDB {
	t.Helper()
	diskdb := ethrawdb.NewMemoryDatabase()
	sdb := state.NewDatabase(diskdb)
	statedb, err := state.New(tcommon.Hash(ethtypes.EmptyRootHash), sdb)
	if err != nil {
		t.Fatal(err)
	}
	return statedb
}

func TestRepairMainnetCreateTransferFailureOvercharge(t *testing.T) {
	statedb := newTestState(t)
	statedb.CreateAccount(mainnetCreateTransferFailurePayer, corepb.AccountType_Normal)
	statedb.AddBalance(mainnetCreateTransferFailurePayer, mainnetCreateTransferFailureBadBalance)
	snapshot := statedb.Snapshot()

	if repaired := repairMainnetCreateTransferFailureOvercharge(
		statedb,
		mainnetCreateTransferFailureRepairBlock,
		mainnetCreateTransferFailureRepairBlockID,
	); !repaired {
		t.Fatal("legacy bad balance was not repaired")
	}
	if got := statedb.GetBalance(mainnetCreateTransferFailurePayer); got != mainnetCreateTransferFailureCanonicalBalance {
		t.Fatalf("repaired balance = %d, want %d", got, mainnetCreateTransferFailureCanonicalBalance)
	}
	statedb.RevertToSnapshot(snapshot)
	if got := statedb.GetBalance(mainnetCreateTransferFailurePayer); got != mainnetCreateTransferFailureBadBalance {
		t.Fatalf("balance after block snapshot rollback = %d, want %d", got, mainnetCreateTransferFailureBadBalance)
	}
	if repaired := repairMainnetCreateTransferFailureOvercharge(
		statedb,
		mainnetCreateTransferFailureRepairBlock,
		mainnetCreateTransferFailureRepairBlockID,
	); !repaired {
		t.Fatal("legacy bad balance was not repaired after block retry")
	}
	if repaired := repairMainnetCreateTransferFailureOvercharge(
		statedb,
		mainnetCreateTransferFailureRepairBlock,
		mainnetCreateTransferFailureRepairBlockID,
	); repaired {
		t.Fatal("canonical balance must not be repaired twice")
	}

	badHashState := newTestState(t)
	badHashState.CreateAccount(mainnetCreateTransferFailurePayer, corepb.AccountType_Normal)
	badHashState.AddBalance(mainnetCreateTransferFailurePayer, mainnetCreateTransferFailureBadBalance)
	if repaired := repairMainnetCreateTransferFailureOvercharge(
		badHashState,
		mainnetCreateTransferFailureRepairBlock,
		tcommon.Hash{0xff},
	); repaired {
		t.Fatal("non-canonical block hash activated the repair")
	}
	if got := badHashState.GetBalance(mainnetCreateTransferFailurePayer); got != mainnetCreateTransferFailureBadBalance {
		t.Fatalf("non-canonical block changed balance to %d", got)
	}
}

func TestApplyMainnetLegacyStateRepairsIsObservableAndIdempotent(t *testing.T) {
	statedb := newTestState(t)
	statedb.CreateAccount(mainnetCreateTransferFailurePayer, corepb.AccountType_Normal)
	statedb.AddBalance(mainnetCreateTransferFailurePayer, mainnetCreateTransferFailureBadBalance)

	createBefore := mainnetCreateTransferFailureRepairCounter.Snapshot().Count()
	vmBefore := mainnetParallelVMMissedPaymentRepairCounter.Snapshot().Count()
	costBefore := mainnetCOSTMissedRewardRepairCounter.Snapshot().Count()
	winkBefore := mainnetWINKMissingCodeRepairCounter.Snapshot().Count()

	var incidents []tronrawdb.ExecutionSafetyIncident
	activated, err := applyMainnetLegacyStateRepairs(
		statedb,
		mainnetCreateTransferFailureRepairBlock,
		mainnetCreateTransferFailureRepairBlockID,
		func(incident tronrawdb.ExecutionSafetyIncident) error {
			incidents = append(incidents, incident)
			return nil
		},
	)
	if err != nil || !activated {
		t.Fatalf("apply repair: activated=%t err=%v", activated, err)
	}
	wantIncident := tronrawdb.ExecutionSafetyIncident{
		Kind:      tronrawdb.ExecutionSafetyIncidentCreateTransferRepair,
		BlockNum:  mainnetCreateTransferFailureRepairBlock,
		BlockHash: mainnetCreateTransferFailureRepairBlockID,
	}
	if len(incidents) != 1 || incidents[0] != wantIncident {
		t.Fatalf("repair incidents = %+v, want [%+v]", incidents, wantIncident)
	}
	if got := statedb.GetBalance(mainnetCreateTransferFailurePayer); got != mainnetCreateTransferFailureCanonicalBalance {
		t.Fatalf("repaired balance = %d, want %d", got, mainnetCreateTransferFailureCanonicalBalance)
	}
	if got := mainnetCreateTransferFailureRepairCounter.Snapshot().Count() - createBefore; got != 1 {
		t.Fatalf("create-transfer repair metric delta = %d, want 1", got)
	}
	if got := mainnetParallelVMMissedPaymentRepairCounter.Snapshot().Count() - vmBefore; got != 0 {
		t.Fatalf("parallel-VM repair metric delta = %d, want 0", got)
	}
	if got := mainnetCOSTMissedRewardRepairCounter.Snapshot().Count() - costBefore; got != 0 {
		t.Fatalf("COST repair metric delta = %d, want 0", got)
	}
	if got := mainnetWINKMissingCodeRepairCounter.Snapshot().Count() - winkBefore; got != 0 {
		t.Fatalf("WINK repair metric delta = %d, want 0", got)
	}

	activated, err = applyMainnetLegacyStateRepairs(
		statedb,
		mainnetCreateTransferFailureRepairBlock,
		mainnetCreateTransferFailureRepairBlockID,
		func(incident tronrawdb.ExecutionSafetyIncident) error {
			incidents = append(incidents, incident)
			return nil
		},
	)
	if err != nil || activated {
		t.Fatalf("reapply repair: activated=%t err=%v", activated, err)
	}
	if len(incidents) != 1 {
		t.Fatalf("idempotent repair emitted %d incidents, want 1", len(incidents))
	}
	if got := mainnetCreateTransferFailureRepairCounter.Snapshot().Count() - createBefore; got != 1 {
		t.Fatalf("idempotent repair metric delta = %d, want 1", got)
	}
}

func TestApplyMainnetLegacyStateRepairsPropagatesMarkerFailure(t *testing.T) {
	statedb := newTestState(t)
	statedb.CreateAccount(mainnetCreateTransferFailurePayer, corepb.AccountType_Normal)
	statedb.AddBalance(mainnetCreateTransferFailurePayer, mainnetCreateTransferFailureBadBalance)
	wantErr := errors.New("persist marker boom")
	activated, err := applyMainnetLegacyStateRepairs(
		statedb,
		mainnetCreateTransferFailureRepairBlock,
		mainnetCreateTransferFailureRepairBlockID,
		func(tronrawdb.ExecutionSafetyIncident) error { return wantErr },
	)
	if !activated || !errors.Is(err, wantErr) {
		t.Fatalf("repair marker failure: activated=%t err=%v, want true/%v", activated, err, wantErr)
	}
}

func TestRepairMainnetParallelVMMissedPayment(t *testing.T) {
	statedb := newTestState(t)
	statedb.CreateAccount(mainnetParallelVMMissedPaymentRecipient, corepb.AccountType_Normal)
	statedb.AddBalance(mainnetParallelVMMissedPaymentRecipient, mainnetParallelVMMissedPaymentBadBalance)
	statedb.CreateAccount(mainnetParallelVMMissedPaymentContract, corepb.AccountType_Contract)
	statedb.AddBalance(mainnetParallelVMMissedPaymentContract, mainnetParallelVMMissedPaymentContractBalance)
	statedb.CreateAccount(mainnetParallelVMMissedPaymentPayer, corepb.AccountType_Normal)
	statedb.AddBalance(mainnetParallelVMMissedPaymentPayer, mainnetParallelVMMissedPaymentPayerBalance)
	blackhole := statedb.BlackholeAddress()
	statedb.CreateAccount(blackhole, corepb.AccountType_Normal)
	statedb.AddBalance(blackhole, mainnetParallelVMMissedPaymentBlackholeBalance)
	snapshot := statedb.Snapshot()

	if repaired := repairMainnetParallelVMMissedPayment(
		statedb,
		mainnetParallelVMMissedPaymentRepairBlock,
		mainnetParallelVMMissedPaymentRepairBlockID,
	); !repaired {
		t.Fatal("legacy missed-payment balance was not repaired")
	}
	if got := statedb.GetBalance(mainnetParallelVMMissedPaymentRecipient); got != mainnetParallelVMMissedPaymentCanonicalBalance {
		t.Fatalf("repaired balance = %d, want %d", got, mainnetParallelVMMissedPaymentCanonicalBalance)
	}
	if got := statedb.GetBalance(mainnetParallelVMMissedPaymentContract); got != mainnetParallelVMMissedPaymentContractBalance-mainnetParallelVMMissedPaymentAmount {
		t.Fatalf("repaired contract balance = %d", got)
	}
	if got := statedb.GetBalance(mainnetParallelVMMissedPaymentPayer); got != mainnetParallelVMMissedPaymentPayerBalance-mainnetParallelVMMissedPaymentEnergyFee {
		t.Fatalf("repaired payer balance = %d", got)
	}
	if got := statedb.GetBalance(blackhole); got != mainnetParallelVMMissedPaymentBlackholeBalance+mainnetParallelVMMissedPaymentEnergyFee {
		t.Fatalf("repaired blackhole balance = %d", got)
	}
	if repaired := repairMainnetParallelVMMissedPayment(
		statedb,
		mainnetParallelVMMissedPaymentRepairBlock,
		mainnetParallelVMMissedPaymentRepairBlockID,
	); repaired {
		t.Fatal("canonical balance must not be repaired twice")
	}

	statedb.RevertToSnapshot(snapshot)
	if got := statedb.GetBalance(mainnetParallelVMMissedPaymentRecipient); got != mainnetParallelVMMissedPaymentBadBalance {
		t.Fatalf("balance after block snapshot rollback = %d, want %d", got, mainnetParallelVMMissedPaymentBadBalance)
	}
	if repaired := repairMainnetParallelVMMissedPayment(
		statedb,
		mainnetParallelVMMissedPaymentRepairBlock,
		tcommon.Hash{0xff},
	); repaired {
		t.Fatal("non-canonical block hash activated the repair")
	}
}

func TestRepairMainnetCOSTMissedReward(t *testing.T) {
	statedb := newTestState(t)
	statedb.CreateAccount(mainnetCOSTMissedRewardRecipient, corepb.AccountType_Normal)
	statedb.AddBalance(mainnetCOSTMissedRewardRecipient, mainnetCOSTMissedRewardRecipientBadBalance)
	statedb.CreateAccount(mainnetCOSTMissedRewardContract, corepb.AccountType_Contract)
	statedb.AddBalance(mainnetCOSTMissedRewardContract, mainnetCOSTMissedRewardContractBalance)
	statedb.SetState(mainnetCOSTMissedRewardContract, mainnetCOSTMissedRewardStorageSlot, mainnetCOSTMissedRewardBadStorageValue)
	statedb.CreateAccount(mainnetCOSTMissedRewardPayer, corepb.AccountType_Normal)
	statedb.AddBalance(mainnetCOSTMissedRewardPayer, mainnetCOSTMissedRewardPayerBalance)
	blackhole := statedb.BlackholeAddress()
	statedb.CreateAccount(blackhole, corepb.AccountType_Normal)
	statedb.AddBalance(blackhole, mainnetCOSTMissedRewardBlackholeBalance)
	snapshot := statedb.Snapshot()

	if repaired := repairMainnetCOSTMissedReward(
		statedb,
		mainnetCOSTMissedRewardRepairBlock,
		mainnetCOSTMissedRewardRepairBlockID,
	); !repaired {
		t.Fatal("legacy COST missed reward was not repaired")
	}
	if got := statedb.GetBalance(mainnetCOSTMissedRewardRecipient); got != mainnetCOSTMissedRewardRecipientBalance {
		t.Fatalf("repaired recipient balance = %d, want %d", got, mainnetCOSTMissedRewardRecipientBalance)
	}
	if got := statedb.GetBalance(mainnetCOSTMissedRewardContract); got != mainnetCOSTMissedRewardContractBalance-mainnetCOSTMissedRewardAmount {
		t.Fatalf("repaired contract balance = %d", got)
	}
	if got := statedb.GetBalance(mainnetCOSTMissedRewardPayer); got != mainnetCOSTMissedRewardPayerBalance-mainnetCOSTMissedRewardEnergyFee {
		t.Fatalf("repaired payer balance = %d", got)
	}
	if got := statedb.GetBalance(blackhole); got != mainnetCOSTMissedRewardBlackholeBalance+mainnetCOSTMissedRewardEnergyFee {
		t.Fatalf("repaired blackhole balance = %d", got)
	}
	if got := statedb.GetState(mainnetCOSTMissedRewardContract, mainnetCOSTMissedRewardStorageSlot); got != mainnetCOSTMissedRewardStorageValue {
		t.Fatalf("repaired reward storage = %x, want %x", got, mainnetCOSTMissedRewardStorageValue)
	}
	if repaired := repairMainnetCOSTMissedReward(
		statedb,
		mainnetCOSTMissedRewardRepairBlock,
		mainnetCOSTMissedRewardRepairBlockID,
	); repaired {
		t.Fatal("canonical state must not be repaired twice")
	}

	statedb.RevertToSnapshot(snapshot)
	if got := statedb.GetBalance(mainnetCOSTMissedRewardRecipient); got != mainnetCOSTMissedRewardRecipientBadBalance {
		t.Fatalf("recipient after block snapshot rollback = %d, want %d", got, mainnetCOSTMissedRewardRecipientBadBalance)
	}
	if got := statedb.GetState(mainnetCOSTMissedRewardContract, mainnetCOSTMissedRewardStorageSlot); got != mainnetCOSTMissedRewardBadStorageValue {
		t.Fatalf("reward storage after block snapshot rollback = %x, want %x", got, mainnetCOSTMissedRewardBadStorageValue)
	}
	if repaired := repairMainnetCOSTMissedReward(
		statedb,
		mainnetCOSTMissedRewardRepairBlock,
		tcommon.Hash{0xff},
	); repaired {
		t.Fatal("non-canonical block hash activated the repair")
	}

	statedb.SetState(mainnetCOSTMissedRewardContract, mainnetCOSTMissedRewardStorageSlot, mainnetCOSTMissedRewardStorageValue)
	if repaired := repairMainnetCOSTMissedReward(
		statedb,
		mainnetCOSTMissedRewardRepairBlock,
		mainnetCOSTMissedRewardRepairBlockID,
	); repaired {
		t.Fatal("unexpected storage pre-image activated the repair")
	}
	if got := statedb.GetBalance(mainnetCOSTMissedRewardRecipient); got != mainnetCOSTMissedRewardRecipientBadBalance {
		t.Fatalf("failed repair changed recipient balance to %d", got)
	}
}

func TestRepairMissingRuntimeCodeFromMetadata(t *testing.T) {
	diskdb := ethrawdb.NewMemoryDatabase()
	database := state.NewDatabase(diskdb)
	statedb, err := state.New(tcommon.Hash(ethtypes.EmptyRootHash), database)
	if err != nil {
		t.Fatal(err)
	}
	contract := testProcessorAddr(0x91)
	prefix := []byte{0x60, 0x01, 0x60}
	runtime := []byte{byte(vm.PUSH1), 0x00, byte(vm.PUSH1), 0x00, byte(vm.REVERT)}
	creation := append(append(append([]byte(nil), prefix...), runtime...), 0xaa, 0xbb)
	spec := missingRuntimeCodeRepairSpec{
		blockNum:         99,
		blockID:          tcommon.HexToHash("0123"),
		contract:         contract,
		creationCodeHash: tcommon.Keccak256(creation),
		runtimeCodeHash:  tcommon.Keccak256(runtime),
		runtimeOffset:    len(prefix),
		runtimeSize:      len(runtime),
	}

	statedb.CreateAccount(contract, corepb.AccountType_Contract)
	statedb.SetContract(contract, &contractpb.SmartContract{
		ContractAddress: contract.Bytes(),
		Bytecode:        creation,
	})
	statedb.SetCode(contract, runtime)
	root, err := statedb.Commit()
	if err != nil {
		t.Fatal(err)
	}
	if err := tronrawdb.DeleteStateCode(diskdb, spec.runtimeCodeHash); err != nil {
		t.Fatal(err)
	}
	statedb, err = state.New(root, database)
	if err != nil {
		t.Fatal(err)
	}
	if got := statedb.GetCodeHash(contract); got != spec.runtimeCodeHash {
		t.Fatalf("pre-repair code hash = %x, want %x", got, spec.runtimeCodeHash)
	}
	if got := statedb.GetCode(contract); len(got) != 0 {
		t.Fatalf("pre-repair code = %x, want missing", got)
	}
	snapshot := statedb.Snapshot()

	if repaired := repairMissingRuntimeCodeFromMetadata(statedb, spec.blockNum, spec.blockID, spec); !repaired {
		t.Fatal("missing runtime code was not repaired")
	}
	if got := statedb.GetCode(contract); !bytes.Equal(got, runtime) {
		t.Fatalf("repaired runtime = %x, want %x", got, runtime)
	}
	if repaired := repairMissingRuntimeCodeFromMetadata(statedb, spec.blockNum, spec.blockID, spec); repaired {
		t.Fatal("present runtime code must not be repaired twice")
	}

	statedb.RevertToSnapshot(snapshot)
	if got := statedb.GetCode(contract); len(got) != 0 {
		t.Fatalf("runtime after block snapshot rollback = %x, want missing", got)
	}
	if repaired := repairMissingRuntimeCodeFromMetadata(statedb, spec.blockNum, tcommon.Hash{0xff}, spec); repaired {
		t.Fatal("non-canonical block hash activated the repair")
	}
	badMetadataSpec := spec
	badMetadataSpec.creationCodeHash = tcommon.Hash{0xff}
	if repaired := repairMissingRuntimeCodeFromMetadata(statedb, spec.blockNum, spec.blockID, badMetadataSpec); repaired {
		t.Fatal("unexpected creation metadata activated the repair")
	}

	if repaired := repairMissingRuntimeCodeFromMetadata(statedb, spec.blockNum, spec.blockID, spec); !repaired {
		t.Fatal("missing runtime code was not repaired after block retry")
	}
	root, err = statedb.Commit()
	if err != nil {
		t.Fatal(err)
	}
	reloaded, err := state.New(root, database)
	if err != nil {
		t.Fatal(err)
	}
	if got := reloaded.GetCode(contract); !bytes.Equal(got, runtime) {
		t.Fatalf("persisted repaired runtime = %x, want %x", got, runtime)
	}
}

func testProcessorAddr(b byte) tcommon.Address {
	var addr tcommon.Address
	addr[0] = 0x41
	addr[20] = b
	return addr
}

func makeTestTransferTx(from, to byte, amount int64) *types.Transaction {
	tc := &contractpb.TransferContract{
		OwnerAddress: testProcessorAddr(from).Bytes(),
		ToAddress:    testProcessorAddr(to).Bytes(),
		Amount:       amount,
	}
	param, _ := anypb.New(tc)
	return types.NewTransactionFromPB(&corepb.Transaction{
		RawData: &corepb.TransactionRaw{
			Expiration: 60_000,
			Contract: []*corepb.Transaction_Contract{{
				Type:      corepb.Transaction_Contract_TransferContract,
				Parameter: param,
			}},
		},
	})
}

func makeTestTransferAssetTx(from, to byte, assetName []byte, amount int64) *types.Transaction {
	contract := &contractpb.TransferAssetContract{
		OwnerAddress: testProcessorAddr(from).Bytes(),
		ToAddress:    testProcessorAddr(to).Bytes(),
		AssetName:    append([]byte(nil), assetName...),
		Amount:       amount,
	}
	param, _ := anypb.New(contract)
	return types.NewTransactionFromPB(&corepb.Transaction{
		RawData: &corepb.TransactionRaw{
			Expiration: 60_000,
			Contract: []*corepb.Transaction_Contract{{
				Type:      corepb.Transaction_Contract_TransferAssetContract,
				Parameter: param,
			}},
		},
	})
}

func TestProcessBlockParallelTransfersMatchesSerial(t *testing.T) {
	base := newTestState(t)
	for _, id := range []byte{1, 2, 3, 4} {
		base.CreateAccount(testProcessorAddr(id), corepb.AccountType_Normal)
	}
	base.AddBalance(testProcessorAddr(1), 10_000_000)
	base.AddBalance(testProcessorAddr(3), 20_000_000)
	base.DynamicProperties().SetLatestBlockHeaderTimestamp(30_000)
	base.DynamicProperties().SetPublicNetUsage(1_000)
	base.DynamicProperties().SetPublicNetTime(0)
	if _, err := base.Commit(); err != nil {
		t.Fatal(err)
	}
	serialState, err := base.Copy()
	if err != nil {
		t.Fatal(err)
	}
	serialState.SetDynamicProperties(base.DynamicProperties().Copy())
	parallelState, err := base.Copy()
	if err != nil {
		t.Fatal(err)
	}
	parallelState.SetDynamicProperties(base.DynamicProperties().Copy())
	transactions := []*types.Transaction{
		makeTestTransferTx(1, 2, 1_000_000),
		makeTestTransferTx(3, 4, 2_000_000),
	}
	block := types.NewBlockFromPB(&corepb.Block{
		BlockHeader:  &corepb.BlockHeader{RawData: &corepb.BlockHeaderRaw{Number: 1, Timestamp: 33_000}},
		Transactions: []*corepb.Transaction{transactions[0].Proto(), transactions[1].Proto()},
	})
	run := func(statedb *state.StateDB, options processBlockOptions) ([]*corepb.TransactionInfo, error) {
		infos, _, processErr := processBlockWithOptions(
			statedb, statedb.DynamicProperties(), block, nil, nil, 0,
			params.DefaultBlockNumForEnergyLimit, false, tcommon.Hash{}, nil, nil,
			nil, forks.NewVersionPassCache(), new(transactionInfoBatch), true, -1, nil,
			options,
		)
		return infos, processErr
	}
	serialInfos, err := run(serialState, processBlockOptions{})
	if err != nil {
		t.Fatalf("serial process: %v", err)
	}
	publishedBefore := parallelTransferPublishedCounter.Snapshot().Count()
	publicNetRebasedBefore := parallelTransferPublicNetRebasedCounter.Snapshot().Count()
	candidatesBefore := parallelTransferCandidatesCounter.Snapshot().Count()
	conflictsBefore := parallelTransferConflictFallbackCounter.Snapshot().Count()
	unavailableBefore := parallelTransferUnavailableFallbackCounter.Snapshot().Count()
	preflightBefore := parallelTransferPreflightFallbackCounter.Snapshot().Count()
	parallelInfos, err := run(parallelState, processBlockOptions{parallelTransfers: true})
	if err != nil {
		t.Fatalf("parallel process: %v", err)
	}
	if published := parallelTransferPublishedCounter.Snapshot().Count() - publishedBefore; published != 2 {
		t.Fatalf("published transfers = %d, want 2 (candidates=%d conflicts=%d unavailable=%d preflight=%d)",
			published,
			parallelTransferCandidatesCounter.Snapshot().Count()-candidatesBefore,
			parallelTransferConflictFallbackCounter.Snapshot().Count()-conflictsBefore,
			parallelTransferUnavailableFallbackCounter.Snapshot().Count()-unavailableBefore,
			parallelTransferPreflightFallbackCounter.Snapshot().Count()-preflightBefore)
	}
	if conflicts := parallelTransferConflictFallbackCounter.Snapshot().Count() - conflictsBefore; conflicts != 0 {
		t.Fatalf("serial conflict fallbacks = %d, want 0", conflicts)
	}
	if rebased := parallelTransferPublicNetRebasedCounter.Snapshot().Count() - publicNetRebasedBefore; rebased != 1 {
		t.Fatalf("rebased public-net publications = %d, want 1", rebased)
	}
	if len(serialInfos) != len(parallelInfos) {
		t.Fatalf("info count serial=%d parallel=%d", len(serialInfos), len(parallelInfos))
	}
	for i := range serialInfos {
		if !proto.Equal(serialInfos[i], parallelInfos[i]) {
			t.Fatalf("tx %d info mismatch\nserial=%v\nparallel=%v", i, serialInfos[i], parallelInfos[i])
		}
	}
	for _, id := range []byte{1, 2, 3, 4} {
		address := testProcessorAddr(id)
		if serial, parallel := serialState.GetBalance(address), parallelState.GetBalance(address); serial != parallel {
			t.Fatalf("account %d balance serial=%d parallel=%d", id, serial, parallel)
		}
	}
	if serial, parallel := serialState.DynamicProperties().PublicNetUsage(), parallelState.DynamicProperties().PublicNetUsage(); serial != parallel {
		t.Fatalf("public net usage serial=%d parallel=%d", serial, parallel)
	}
	if serial, parallel := serialState.DynamicProperties().PublicNetTime(), parallelState.DynamicProperties().PublicNetTime(); serial != parallel {
		t.Fatalf("public net time serial=%d parallel=%d", serial, parallel)
	}
	serialRoot, err := serialState.Commit()
	if err != nil {
		t.Fatal(err)
	}
	parallelRoot, err := parallelState.Commit()
	if err != nil {
		t.Fatal(err)
	}
	if serialRoot != parallelRoot {
		t.Fatalf("state roots differ: serial=%x parallel=%x", serialRoot, parallelRoot)
	}
}

// Block 22,097,772 first exposed the production failure shape: two funded
// sender chains each paid the same recipient more than once, and an async
// retry published a stale recipient post-image. The lost 4,455 SUN was only
// observed 26,087 blocks later when that recipient spent its full balance.
func TestProcessBlockParallelTransfersPreservesBlock22097772SharedRecipient(t *testing.T) {
	mustAddress := func(encoded string) tcommon.Address {
		address := tcommon.BytesToAddress(tcommon.FromHex(encoded))
		if !address.ValidPrefix() || address.Hex() != encoded {
			t.Fatalf("invalid test address %q", encoded)
		}
		return address
	}
	fundingSource := mustAddress("41733f5f424de3a0ec4c928d10507fb1461be119a5")
	firstSender := mustAddress("4117fe31d8d3dfc39742e2e755d5be115e291b7f46")
	secondSender := mustAddress("41718bd518333befb2b1c0d6414324039852a666ba")
	sharedRecipient := mustAddress("419f45f2203271e3f9131cf1bb31deb46f60fa9986")
	fillerAddress := func(txIndex int, recipient bool) tcommon.Address {
		var address tcommon.Address
		address[0] = tcommon.AddressPrefixMainnet
		if recipient {
			address[1] = 0xfd
		} else {
			address[1] = 0xfe
		}
		address[19] = byte(txIndex >> 8)
		address[20] = byte(txIndex)
		return address
	}
	makeTransfer := func(from, to tcommon.Address, amount int64) *types.Transaction {
		parameter, err := anypb.New(&contractpb.TransferContract{
			OwnerAddress: from.Bytes(), ToAddress: to.Bytes(), Amount: amount,
		})
		if err != nil {
			t.Fatal(err)
		}
		return types.NewTransactionFromPB(&corepb.Transaction{RawData: &corepb.TransactionRaw{
			Expiration: 60_000,
			Contract: []*corepb.Transaction_Contract{{
				Type: corepb.Transaction_Contract_TransferContract, Parameter: parameter,
			}},
		}})
	}
	const transactionCount = 148
	transactions := make([]*types.Transaction, transactionCount)
	for txIndex := range transactions {
		transactions[txIndex] = makeTransfer(fillerAddress(txIndex, false), fillerAddress(txIndex, true), 1)
	}
	// Exact positions, addresses and amounts from mainnet block 22,097,772.
	// The corresponding tx IDs are ab96600f (fund sender 1), 7d6788fb and
	// acb8d8b7 (sender 1), c0cf8ecf (fund sender 2), and ab5aea80/f4e588a5
	// (sender 2). Keeping the intervening independent work preserves the real
	// async-retry scheduling window instead of compressing the six transfers.
	transactions[0] = makeTransfer(fundingSource, firstSender, 450_060)
	transactions[25] = makeTransfer(firstSender, sharedRecipient, 445_561)
	transactions[58] = makeTransfer(firstSender, sharedRecipient, 4_455)
	transactions[69] = makeTransfer(fundingSource, secondSender, 337_545)
	transactions[87] = makeTransfer(secondSender, sharedRecipient, 334_171)
	transactions[129] = makeTransfer(secondSender, sharedRecipient, 3_342)

	newBase := func() *state.StateDB {
		base := newTestState(t)
		for _, address := range []tcommon.Address{fundingSource, firstSender, secondSender, sharedRecipient} {
			base.CreateAccount(address, corepb.AccountType_Normal)
		}
		base.AddBalance(fundingSource, 1_000_000)
		base.AddBalance(firstSender, 2)
		base.AddBalance(secondSender, 2)
		base.AddBalance(sharedRecipient, 12_084_877_502)
		for txIndex := range transactionCount {
			if txIndex == 0 || txIndex == 25 || txIndex == 58 || txIndex == 69 || txIndex == 87 || txIndex == 129 {
				continue
			}
			owner := fillerAddress(txIndex, false)
			recipient := fillerAddress(txIndex, true)
			base.CreateAccount(owner, corepb.AccountType_Normal)
			base.AddBalance(owner, 1_000_000)
			base.CreateAccount(recipient, corepb.AccountType_Normal)
		}
		if _, err := base.Commit(); err != nil {
			t.Fatal(err)
		}
		return base
	}
	transactionProtos := make([]*corepb.Transaction, len(transactions))
	for txIndex, tx := range transactions {
		transactionProtos[txIndex] = tx.Proto()
	}
	block := types.NewBlockFromPB(&corepb.Block{
		BlockHeader: &corepb.BlockHeader{RawData: &corepb.BlockHeaderRaw{
			Number: 22_097_772, Timestamp: 3_000,
		}},
		Transactions: transactionProtos,
	})
	run := func(statedb *state.StateDB, options processBlockOptions) ([]*corepb.TransactionInfo, error) {
		infos, _, processErr := processBlockWithOptions(
			statedb, statedb.DynamicProperties(), block, ethrawdb.NewMemoryDatabase(), nil, 0,
			params.DefaultBlockNumForEnergyLimit, false, tcommon.Hash{}, nil, nil,
			nil, forks.NewVersionPassCache(), new(transactionInfoBatch), true, -1, nil,
			options,
		)
		return infos, processErr
	}

	serialState := newBase()
	serialInfos, err := run(serialState, processBlockOptions{captureBalanceTrace: true})
	if err != nil {
		t.Fatalf("serial process: %v", err)
	}
	serialRoot, err := serialState.Commit()
	if err != nil {
		t.Fatal(err)
	}
	if got := serialState.GetBalance(sharedRecipient); got != 12_085_665_031 {
		t.Fatalf("serial recipient balance = %d, want 12085665031", got)
	}

	balanceOracleBefore := parallelTransferBalanceOracleCandidatesCounter.Snapshot().Count()
	balanceOracleMatchesBefore := parallelTransferBalanceOracleMatchesCounter.Snapshot().Count()
	serialVerifyBefore := parallelTransferSerialVerifyCandidatesCounter.Snapshot().Count()
	serialVerifyMatchesBefore := parallelTransferSerialVerifyMatchesCounter.Snapshot().Count()
	serialVerifyInfoMismatchBefore := parallelTransferSerialVerifyInfoMismatchCounter.Snapshot().Count()
	serialVerifyWriteMismatchBefore := parallelTransferSerialVerifyWriteMismatchCounter.Snapshot().Count()
	serialVerifyBalanceMismatchBefore := parallelTransferSerialVerifyBalanceMismatchCounter.Snapshot().Count()
	serialVerifyErrorsBefore := parallelTransferSerialVerifyErrorsCounter.Snapshot().Count()
	publishedBefore := parallelTransferPublishedCounter.Snapshot().Count()
	writeSealCandidatesBefore := parallelTransferWriteSealCandidatesCounter.Snapshot().Count()
	writeSealMatchesBefore := parallelTransferWriteSealMatchesCounter.Snapshot().Count()
	writeSealMismatchesBefore := parallelTransferWriteSealMismatchesCounter.Snapshot().Count()
	publishAuditBefore := parallelTransferPublishAuditCandidatesCounter.Snapshot().Count()
	publishAuditMatchesBefore := parallelTransferPublishAuditMatchesCounter.Snapshot().Count()
	publishAuditMismatchesBefore := parallelTransferPublishAuditMismatchesCounter.Snapshot().Count()
	publishAuditErrorsBefore := parallelTransferPublishAuditErrorsCounter.Snapshot().Count()
	for iteration := 0; iteration < 32; iteration++ {
		parallelState := newBase()
		parallelInfos, processErr := run(parallelState, processBlockOptions{parallelTransfers: true, captureBalanceTrace: true})
		if processErr != nil {
			t.Fatalf("iteration %d parallel process: %v", iteration, processErr)
		}
		for txIndex := range serialInfos {
			if !proto.Equal(serialInfos[txIndex], parallelInfos[txIndex]) {
				t.Fatalf("iteration %d tx %d info mismatch\nserial=%v\nparallel=%v", iteration, txIndex, serialInfos[txIndex], parallelInfos[txIndex])
			}
		}
		if got := parallelState.GetBalance(sharedRecipient); got != 12_085_665_031 {
			t.Fatalf("iteration %d recipient balance = %d, want 12085665031", iteration, got)
		}
		parallelRoot, commitErr := parallelState.Commit()
		if commitErr != nil {
			t.Fatal(commitErr)
		}
		if parallelRoot != serialRoot {
			t.Fatalf("iteration %d state roots differ: serial=%x parallel=%x", iteration, serialRoot, parallelRoot)
		}
	}
	balanceCandidates := parallelTransferBalanceOracleCandidatesCounter.Snapshot().Count() - balanceOracleBefore
	balanceMatches := parallelTransferBalanceOracleMatchesCounter.Snapshot().Count() - balanceOracleMatchesBefore
	if balanceCandidates == 0 || balanceMatches != balanceCandidates {
		t.Fatalf("historical transfer balance oracle candidates/matches = %d/%d, want non-zero equality", balanceCandidates, balanceMatches)
	}
	serialCandidates := parallelTransferSerialVerifyCandidatesCounter.Snapshot().Count() - serialVerifyBefore
	serialMatches := parallelTransferSerialVerifyMatchesCounter.Snapshot().Count() - serialVerifyMatchesBefore
	if serialCandidates == 0 || serialMatches != serialCandidates {
		t.Fatalf("historical transfer serial oracle candidates/matches = %d/%d mismatches(info/write/balance)=%d/%d/%d errors=%d",
			serialCandidates,
			serialMatches,
			parallelTransferSerialVerifyInfoMismatchCounter.Snapshot().Count()-serialVerifyInfoMismatchBefore,
			parallelTransferSerialVerifyWriteMismatchCounter.Snapshot().Count()-serialVerifyWriteMismatchBefore,
			parallelTransferSerialVerifyBalanceMismatchCounter.Snapshot().Count()-serialVerifyBalanceMismatchBefore,
			parallelTransferSerialVerifyErrorsCounter.Snapshot().Count()-serialVerifyErrorsBefore,
		)
	}
	published := parallelTransferPublishedCounter.Snapshot().Count() - publishedBefore
	sealCandidates := parallelTransferWriteSealCandidatesCounter.Snapshot().Count() - writeSealCandidatesBefore
	sealMatches := parallelTransferWriteSealMatchesCounter.Snapshot().Count() - writeSealMatchesBefore
	if sealCandidates == 0 || sealCandidates != sealMatches || sealMatches != published {
		t.Fatalf("historical transfer WriteSet seals candidates/matches/published = %d/%d/%d, want non-zero equality", sealCandidates, sealMatches, published)
	}
	if sealMismatches := parallelTransferWriteSealMismatchesCounter.Snapshot().Count() - writeSealMismatchesBefore; sealMismatches != 0 {
		t.Fatalf("historical transfer WriteSet seal mismatches = %d, want 0", sealMismatches)
	}
	audited := parallelTransferPublishAuditCandidatesCounter.Snapshot().Count() - publishAuditBefore
	matches := parallelTransferPublishAuditMatchesCounter.Snapshot().Count() - publishAuditMatchesBefore
	if audited == 0 || matches != audited {
		t.Fatalf("historical transfer publication audits candidates/matches = %d/%d, want non-zero equality", audited, matches)
	}
	if mismatches := parallelTransferPublishAuditMismatchesCounter.Snapshot().Count() - publishAuditMismatchesBefore; mismatches != 0 {
		t.Fatalf("historical transfer publication audit mismatches = %d, want 0", mismatches)
	}
	if auditErrors := parallelTransferPublishAuditErrorsCounter.Snapshot().Count() - publishAuditErrorsBefore; auditErrors != 0 {
		t.Fatalf("historical transfer publication audit errors = %d, want 0", auditErrors)
	}
}

func TestProcessBlockParallelTransferToBlackholeFallsBackSerially(t *testing.T) {
	owner := testProcessorAddr(0x31)
	blackhole := params.BlackholeAddress
	parameter, err := anypb.New(&contractpb.TransferContract{
		OwnerAddress: owner.Bytes(),
		ToAddress:    blackhole.Bytes(),
		Amount:       1_000,
	})
	if err != nil {
		t.Fatal(err)
	}
	tx := types.NewTransactionFromPB(&corepb.Transaction{RawData: &corepb.TransactionRaw{
		Expiration: 60_000,
		Contract: []*corepb.Transaction_Contract{{
			Type:      corepb.Transaction_Contract_TransferContract,
			Parameter: parameter,
		}},
	}})
	block := types.NewBlockFromPB(&corepb.Block{
		BlockHeader:  &corepb.BlockHeader{RawData: &corepb.BlockHeaderRaw{Number: 1, Timestamp: 3_000}},
		Transactions: []*corepb.Transaction{tx.Proto()},
	})
	newBase := func() *state.StateDB {
		base := newTestState(t)
		base.CreateAccount(owner, corepb.AccountType_Normal)
		base.AddBalance(owner, 1_000_000)
		base.CreateAccount(blackhole, corepb.AccountType_Normal)
		// Force paid bandwidth through the legacy Blackhole settlement route.
		base.DynamicProperties().Set("free_net_limit", 0)
		base.DynamicProperties().Set("public_net_limit", 0)
		base.DynamicProperties().Set("total_net_limit", 0)
		base.DynamicProperties().Set("transaction_fee", 1)
		base.DynamicProperties().SetAllowBlackHoleOptimization(false)
		base.DynamicProperties().SetAllowTransactionFeePool(false)
		if _, commitErr := base.Commit(); commitErr != nil {
			t.Fatal(commitErr)
		}
		return base
	}
	run := func(statedb *state.StateDB, options processBlockOptions) ([]*corepb.TransactionInfo, error) {
		infos, _, processErr := processBlockWithOptions(
			statedb, statedb.DynamicProperties(), block, ethrawdb.NewMemoryDatabase(), nil, 0,
			params.DefaultBlockNumForEnergyLimit, false, tcommon.Hash{}, nil, nil,
			nil, forks.NewVersionPassCache(), new(transactionInfoBatch), true, -1, nil,
			options,
		)
		return infos, processErr
	}

	serialState := newBase()
	serialInfos, err := run(serialState, processBlockOptions{})
	if err != nil {
		t.Fatalf("serial process: %v", err)
	}
	fee := serialInfos[0].GetFee()
	if fee <= 0 {
		t.Fatalf("serial fee = %d, want paid bandwidth", fee)
	}
	if got := serialState.GetBalance(blackhole); got != 1_000+fee {
		t.Fatalf("serial Blackhole balance = %d, want %d", got, 1_000+fee)
	}
	serialRoot, err := serialState.Commit()
	if err != nil {
		t.Fatal(err)
	}

	candidatesBefore := parallelTransferBalanceOracleCandidatesCounter.Snapshot().Count()
	fallbacksBefore := parallelTransferBalanceOracleFallbacksCounter.Snapshot().Count()
	mismatchesBefore := parallelTransferBalanceOracleMismatchesCounter.Snapshot().Count()
	errorsBefore := parallelTransferBalanceOracleErrorsCounter.Snapshot().Count()
	publishedBefore := parallelTransferPublishedCounter.Snapshot().Count()
	encodeBalance := func(value int64) []byte {
		encoded := make([]byte, 8)
		binary.BigEndian.PutUint64(encoded, uint64(value))
		return encoded
	}
	oracleState := newBase()
	oracleResult := &discardShadowTaskResult{
		info: &corepb.TransactionInfo{Fee: fee},
		writes: state.TransactionWriteSet{
			{Kind: state.TransactionAccessAccountField, Address: owner, AccountField: state.TransactionAccountFieldBalance}: {
				Exists: true, Value: encodeBalance(1_000_000 - 1_000 - fee),
			},
			{Kind: state.TransactionAccessAccountField, Address: blackhole, AccountField: state.TransactionAccountFieldBalance}: {
				Exists: true, Value: encodeBalance(1_000 + fee),
			},
		},
	}
	if matched, oracleErr := validateTransferBalancePostImages(
		oracleState, tx, oracleResult,
		discardShadowRunConfig{block: block, transactions: []*types.Transaction{tx}}, 0,
	); oracleErr != nil || matched {
		t.Fatalf("Blackhole balance oracle result = %t,%v, want false,nil fallback", matched, oracleErr)
	}
	parallelState := newBase()
	parallelInfos, err := run(parallelState, processBlockOptions{parallelTransfers: true})
	if err != nil {
		t.Fatalf("parallel process: %v", err)
	}
	if !proto.Equal(serialInfos[0], parallelInfos[0]) {
		t.Fatalf("transaction info mismatch\nserial=%v\nparallel=%v", serialInfos[0], parallelInfos[0])
	}
	parallelRoot, err := parallelState.Commit()
	if err != nil {
		t.Fatal(err)
	}
	if parallelRoot != serialRoot {
		t.Fatalf("state roots differ: serial=%x parallel=%x", serialRoot, parallelRoot)
	}
	if got := parallelTransferBalanceOracleCandidatesCounter.Snapshot().Count() - candidatesBefore; got != 1 {
		t.Fatalf("balance oracle candidates = %d, want 1", got)
	}
	if got := parallelTransferBalanceOracleFallbacksCounter.Snapshot().Count() - fallbacksBefore; got != 1 {
		t.Fatalf("balance oracle protocol-account fallbacks = %d, want 1", got)
	}
	if got := parallelTransferBalanceOracleMismatchesCounter.Snapshot().Count() - mismatchesBefore; got != 0 {
		t.Fatalf("balance oracle mismatches = %d, want 0", got)
	}
	if got := parallelTransferBalanceOracleErrorsCounter.Snapshot().Count() - errorsBefore; got != 0 {
		t.Fatalf("balance oracle errors = %d, want 0", got)
	}
	if got := parallelTransferPublishedCounter.Snapshot().Count() - publishedBefore; got != 0 {
		t.Fatalf("Blackhole transfer publications = %d, want 0", got)
	}
}

func TestProcessBlockParallelTransferFeeRoutingDifferential(t *testing.T) {
	for testIndex, tc := range []struct {
		name                string
		blackholeOptimized  bool
		transactionFeePool  bool
		wantBlackholeFee    bool
		wantBandwidthInPool bool
	}{
		{name: "legacy-blackhole", wantBlackholeFee: true},
		{name: "burn", blackholeOptimized: true},
		{name: "legacy-blackhole-with-fee-pool", transactionFeePool: true, wantBlackholeFee: true, wantBandwidthInPool: true},
		{name: "burn-with-fee-pool", blackholeOptimized: true, transactionFeePool: true, wantBandwidthInPool: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			newBase := func() *state.StateDB {
				base := newTestState(t)
				for _, id := range []byte{1, 2} {
					base.CreateAccount(testProcessorAddr(id), corepb.AccountType_Normal)
				}
				base.CreateAccount(params.BlackholeAddress, corepb.AccountType_Normal)
				base.AddBalance(testProcessorAddr(1), 1_000_000_000)
				base.AddBalance(testProcessorAddr(2), 1_000_000)
				base.AddBalance(params.BlackholeAddress, 100)
				dp := base.DynamicProperties()
				// Force paid bandwidth while also charging memo and multisig fees.
				// These three components cover every settlement destination used by
				// a successful existing-recipient Transfer.
				dp.Set("free_net_limit", 0)
				dp.SetPublicNetLimit(0)
				dp.Set("transaction_fee", 2)
				dp.SetAllowMultiSign(true)
				dp.SetMultiSignFee(11)
				dp.SetMemoFee(13)
				dp.SetAllowBlackHoleOptimization(tc.blackholeOptimized)
				dp.SetAllowTransactionFeePool(tc.transactionFeePool)
				if _, err := base.Commit(); err != nil {
					t.Fatal(err)
				}
				return base
			}

			tx := makeTestTransferTx(1, 2, 123_456)
			tx.Proto().Signature = [][]byte{make([]byte, 65), make([]byte, 65)}
			tx.Proto().RawData.Data = []byte("parallel-fee-routing")
			block := types.NewBlockFromPB(&corepb.Block{
				BlockHeader: &corepb.BlockHeader{RawData: &corepb.BlockHeaderRaw{
					Number: int64(101 + testIndex), Timestamp: 3_000,
				}},
				Transactions: []*corepb.Transaction{tx.Proto()},
			})
			run := func(statedb *state.StateDB, options processBlockOptions) ([]*corepb.TransactionInfo, *contractpb.BlockBalanceTrace, map[tcommon.Address]int64, error) {
				statedb.BeginBalanceTrace(int64(block.Number()), block.Hash().Bytes(), block.Timestamp())
				infos, _, processErr := processBlockWithOptions(
					statedb, statedb.DynamicProperties(), block, ethrawdb.NewMemoryDatabase(), nil, 0,
					params.DefaultBlockNumForEnergyLimit, false, tcommon.Hash{}, nil, nil,
					nil, forks.NewVersionPassCache(), new(transactionInfoBatch), true, -1, nil,
					options,
				)
				trace, finalBalances := statedb.FinishBalanceTrace()
				return infos, trace, finalBalances, processErr
			}

			publishedBefore := parallelTransferPublishedCounter.Snapshot().Count()
			balanceMatchesBefore := parallelTransferBalanceOracleMatchesCounter.Snapshot().Count()
			balanceFallbacksBefore := parallelTransferBalanceOracleFallbacksCounter.Snapshot().Count()
			serialMatchesBefore := parallelTransferSerialVerifyMatchesCounter.Snapshot().Count()
			base := newBase()
			serialState, err := base.Copy()
			if err != nil {
				t.Fatal(err)
			}
			serialState.SetDynamicProperties(base.DynamicProperties().Copy())
			parallelState, err := base.Copy()
			if err != nil {
				t.Fatal(err)
			}
			parallelState.SetDynamicProperties(base.DynamicProperties().Copy())

			serialInfos, serialTrace, serialBalances, err := run(serialState, processBlockOptions{captureBalanceTrace: true})
			if err != nil {
				t.Fatalf("serial process: %v", err)
			}
			parallelInfos, parallelTrace, parallelBalances, err := run(parallelState, processBlockOptions{parallelTransfers: true, captureBalanceTrace: true})
			if err != nil {
				t.Fatalf("parallel process: %v", err)
			}
			if len(serialInfos) != 1 || len(parallelInfos) != 1 || !proto.Equal(serialInfos[0], parallelInfos[0]) {
				t.Fatalf("transaction info mismatch\nserial=%v\nparallel=%v", serialInfos, parallelInfos)
			}
			if !proto.Equal(serialTrace, parallelTrace) {
				t.Fatalf("block balance trace mismatch\nserial=%v\nparallel=%v", serialTrace, parallelTrace)
			}
			if len(serialBalances) != len(parallelBalances) {
				t.Fatalf("final balance count serial=%d parallel=%d", len(serialBalances), len(parallelBalances))
			}
			for address, serialBalance := range serialBalances {
				if parallelBalance, ok := parallelBalances[address]; !ok || parallelBalance != serialBalance {
					t.Fatalf("final balance %s serial=%d parallel=%d present=%t", address.Hex(), serialBalance, parallelBalance, ok)
				}
			}
			serialRoot, err := serialState.Commit()
			if err != nil {
				t.Fatal(err)
			}
			parallelRoot, err := parallelState.Commit()
			if err != nil {
				t.Fatal(err)
			}
			if parallelRoot != serialRoot {
				t.Fatalf("state roots differ: serial=%x parallel=%x", serialRoot, parallelRoot)
			}

			info := parallelInfos[0]
			if info.GetFee() <= 24 {
				t.Fatalf("total fee = %d, want paid bandwidth plus 24 memo/multisig", info.GetFee())
			}
			bandwidthFee := info.GetFee() - 24
			if got := parallelState.GetBalance(testProcessorAddr(1)); got != 1_000_000_000-123_456-info.GetFee() {
				t.Fatalf("owner balance = %d", got)
			}
			if got := parallelState.GetBalance(testProcessorAddr(2)); got != 1_000_000+123_456 {
				t.Fatalf("recipient balance = %d", got)
			}
			wantBlackhole := int64(100)
			if tc.wantBlackholeFee {
				wantBlackhole += 24
				if !tc.wantBandwidthInPool {
					wantBlackhole += bandwidthFee
				}
			}
			if got := parallelState.GetBalance(params.BlackholeAddress); got != wantBlackhole {
				t.Fatalf("Blackhole balance = %d, want %d", got, wantBlackhole)
			}
			wantPool := int64(0)
			if tc.wantBandwidthInPool {
				wantPool = bandwidthFee
			}
			if got := parallelState.DynamicProperties().TransactionFeePool(); got != wantPool {
				t.Fatalf("transaction fee pool = %d, want %d", got, wantPool)
			}
			wantBurned := int64(0)
			if tc.blackholeOptimized {
				wantBurned = 24
				if !tc.wantBandwidthInPool {
					wantBurned += bandwidthFee
				}
			}
			if got := parallelState.DynamicProperties().BurnTrxAmount(); got != wantBurned {
				t.Fatalf("burned TRX = %d, want %d", got, wantBurned)
			}
			if got := parallelTransferPublishedCounter.Snapshot().Count() - publishedBefore; got != 1 {
				t.Fatalf("published transfers = %d, want 1", got)
			}
			if got := parallelTransferBalanceOracleMatchesCounter.Snapshot().Count() - balanceMatchesBefore; got != 1 {
				t.Fatalf("balance-oracle matches = %d, want 1", got)
			}
			if got := parallelTransferBalanceOracleFallbacksCounter.Snapshot().Count() - balanceFallbacksBefore; got != 0 {
				t.Fatalf("balance-oracle fallbacks = %d, want 0", got)
			}
			if got := parallelTransferSerialVerifyMatchesCounter.Snapshot().Count() - serialMatchesBefore; got != 1 {
				t.Fatalf("serial-oracle matches = %d, want 1", got)
			}
		})
	}
}

func TestProcessBlockParallelTransfersRandomizedDifferential(t *testing.T) {
	const (
		seedCount        = 16
		transactionCount = 96
	)
	newBase := func(seed int64) *state.StateDB {
		base := newTestState(t)
		for id := byte(1); id <= 60; id++ {
			base.CreateAccount(testProcessorAddr(id), corepb.AccountType_Normal)
			base.AddBalance(testProcessorAddr(id), 1_000_000_000)
		}
		// Two sender suffixes are invalid on the block-start view and become
		// executable only after the first sender funds them. This is the shape
		// that exercises the async incarnation path rather than only independent
		// block-start publications.
		base.AddBalance(testProcessorAddr(2), -999_999_999)
		base.AddBalance(testProcessorAddr(3), -999_999_999)
		base.CreateAccount(params.BlackholeAddress, corepb.AccountType_Normal)
		dp := base.DynamicProperties()
		dp.Set("free_net_limit", 1_000_000_000)
		if seed%3 == 0 {
			// Exhaust the global free-bandwidth pool part-way through the block;
			// later candidates must fall back to canonical paid bandwidth.
			dp.SetPublicNetLimit(5_000)
		} else {
			dp.SetPublicNetLimit(1_000_000_000)
		}
		dp.SetPublicNetUsage(seed % 97)
		dp.SetPublicNetTime(0)
		dp.Set("transaction_fee", 1)
		dp.SetAllowBlackHoleOptimization(true)
		dp.SetAllowTransactionFeePool(false)
		if _, err := base.Commit(); err != nil {
			t.Fatal(err)
		}
		return base
	}
	makeTransaction := func(seed int64, txIndex int, from, to byte, amount int64) *types.Transaction {
		tx := makeTestTransferTx(from, to, amount)
		tx.Proto().RawData.Expiration = 60_000 + seed*1_000 + int64(txIndex)
		return tx
	}

	publishedBefore := parallelTransferPublishedCounter.Snapshot().Count()
	retryExecutedBefore := discardShadowRetryActualExecutedCounter.Snapshot().Count()
	rebasedBefore := parallelTransferPublicNetRebasedCounter.Snapshot().Count()
	limitFallbackBefore := parallelTransferPublicNetLimitFallbackCounter.Snapshot().Count()
	balanceCandidatesBefore := parallelTransferBalanceOracleCandidatesCounter.Snapshot().Count()
	balanceMatchesBefore := parallelTransferBalanceOracleMatchesCounter.Snapshot().Count()
	balanceFallbacksBefore := parallelTransferBalanceOracleFallbacksCounter.Snapshot().Count()
	balanceMismatchesBefore := parallelTransferBalanceOracleMismatchesCounter.Snapshot().Count()
	balanceErrorsBefore := parallelTransferBalanceOracleErrorsCounter.Snapshot().Count()
	serialCandidatesBefore := parallelTransferSerialVerifyCandidatesCounter.Snapshot().Count()
	serialMatchesBefore := parallelTransferSerialVerifyMatchesCounter.Snapshot().Count()

	for seed := int64(1); seed <= seedCount; seed++ {
		seed := seed
		t.Run("seed-"+string(rune('A'+seed-1)), func(t *testing.T) {
			rng := rand.New(rand.NewSource(seed))
			transactions := make([]*types.Transaction, transactionCount)
			transactions[0] = makeTransaction(seed, 0, 1, 2, 20_000_000)
			transactions[1] = makeTransaction(seed, 1, 1, 3, 20_000_000)
			for txIndex := 2; txIndex < 90; txIndex++ {
				owner := byte(10 + rng.Intn(30))
				recipient := byte(41 + rng.Intn(19))
				if txIndex%4 == 0 {
					recipient = 4 // deliberately hot shared recipient
				}
				transactions[txIndex] = makeTransaction(seed, txIndex, owner, recipient, int64(1+rng.Intn(10_000)))
			}
			transactions[90] = makeTransaction(seed, 90, 2, 4, 7_000_000)
			transactions[91] = makeTransaction(seed, 91, 3, 4, 8_000_000)
			transactions[92] = makeTransaction(seed, 92, 2, 4, 6_000_000)
			transactions[93] = makeTransaction(seed, 93, 3, 5, 5_000_000)
			transactions[94] = makeTransaction(seed, 94, 12, 4, int64(1+rng.Intn(10_000)))
			transactions[95] = makeTransaction(seed, 95, 2, 6, 1_000_000)

			transactionProtos := make([]*corepb.Transaction, len(transactions))
			for txIndex, tx := range transactions {
				transactionProtos[txIndex] = tx.Proto()
			}
			blockNumber := uint64(10_000 + seed*2)
			if blockNumber%discardShadowSampleInterval == 0 {
				blockNumber++
			}
			block := types.NewBlockFromPB(&corepb.Block{
				BlockHeader: &corepb.BlockHeader{RawData: &corepb.BlockHeaderRaw{
					Number: int64(blockNumber), Timestamp: 3_000,
				}},
				Transactions: transactionProtos,
			})
			run := func(statedb *state.StateDB, options processBlockOptions) ([]*corepb.TransactionInfo, *contractpb.BlockBalanceTrace, map[tcommon.Address]int64, error) {
				statedb.BeginBalanceTrace(int64(block.Number()), block.Hash().Bytes(), block.Timestamp())
				infos, _, processErr := processBlockWithOptions(
					statedb, statedb.DynamicProperties(), block, ethrawdb.NewMemoryDatabase(), nil, 0,
					params.DefaultBlockNumForEnergyLimit, false, tcommon.Hash{}, nil, nil,
					nil, forks.NewVersionPassCache(), new(transactionInfoBatch), true, -1, nil,
					options,
				)
				trace, finalBalances := statedb.FinishBalanceTrace()
				return infos, trace, finalBalances, processErr
			}

			base := newBase(seed)
			serialState, err := base.Copy()
			if err != nil {
				t.Fatal(err)
			}
			serialState.SetDynamicProperties(base.DynamicProperties().Copy())
			parallelState, err := base.Copy()
			if err != nil {
				t.Fatal(err)
			}
			parallelState.SetDynamicProperties(base.DynamicProperties().Copy())
			serialInfos, serialTrace, serialBalances, err := run(serialState, processBlockOptions{captureBalanceTrace: true})
			if err != nil {
				t.Fatalf("serial process: %v", err)
			}
			parallelInfos, parallelTrace, parallelBalances, err := run(parallelState, processBlockOptions{parallelTransfers: true, captureBalanceTrace: true})
			if err != nil {
				t.Fatalf("parallel process: %v", err)
			}
			if len(serialInfos) != len(parallelInfos) {
				t.Fatalf("transaction-info count serial=%d parallel=%d", len(serialInfos), len(parallelInfos))
			}
			for txIndex := range serialInfos {
				if !proto.Equal(serialInfos[txIndex], parallelInfos[txIndex]) {
					t.Fatalf("tx %d info mismatch\nserial=%v\nparallel=%v", txIndex, serialInfos[txIndex], parallelInfos[txIndex])
				}
			}
			if !proto.Equal(serialTrace, parallelTrace) {
				t.Fatalf("block balance trace mismatch\nserial=%v\nparallel=%v", serialTrace, parallelTrace)
			}
			if len(serialBalances) != len(parallelBalances) {
				t.Fatalf("final balance count serial=%d parallel=%d", len(serialBalances), len(parallelBalances))
			}
			for address, serialBalance := range serialBalances {
				if parallelBalance, ok := parallelBalances[address]; !ok || parallelBalance != serialBalance {
					t.Fatalf("final balance %s serial=%d parallel=%d present=%t", address.Hex(), serialBalance, parallelBalance, ok)
				}
			}
			serialRoot, err := serialState.Commit()
			if err != nil {
				t.Fatal(err)
			}
			parallelRoot, err := parallelState.Commit()
			if err != nil {
				t.Fatal(err)
			}
			if parallelRoot != serialRoot {
				t.Fatalf("state roots differ: serial=%x parallel=%x", serialRoot, parallelRoot)
			}
		})
	}

	published := parallelTransferPublishedCounter.Snapshot().Count() - publishedBefore
	if published == 0 {
		t.Fatal("randomized differential matrix did not publish a Transfer")
	}
	if executed := discardShadowRetryActualExecutedCounter.Snapshot().Count() - retryExecutedBefore; executed == 0 {
		t.Fatal("randomized differential matrix did not execute an async sender retry")
	}
	if rebased := parallelTransferPublicNetRebasedCounter.Snapshot().Count() - rebasedBefore; rebased == 0 {
		t.Fatal("randomized differential matrix did not exercise public-net rebasing")
	}
	if fallbacks := parallelTransferPublicNetLimitFallbackCounter.Snapshot().Count() - limitFallbackBefore; fallbacks == 0 {
		t.Fatal("randomized differential matrix did not exercise public-net exhaustion fallback")
	}
	balanceCandidates := parallelTransferBalanceOracleCandidatesCounter.Snapshot().Count() - balanceCandidatesBefore
	balanceMatches := parallelTransferBalanceOracleMatchesCounter.Snapshot().Count() - balanceMatchesBefore
	balanceFallbacks := parallelTransferBalanceOracleFallbacksCounter.Snapshot().Count() - balanceFallbacksBefore
	if mismatches := parallelTransferBalanceOracleMismatchesCounter.Snapshot().Count() - balanceMismatchesBefore; mismatches != 0 {
		t.Fatalf("randomized balance-oracle mismatches = %d", mismatches)
	}
	if oracleErrors := parallelTransferBalanceOracleErrorsCounter.Snapshot().Count() - balanceErrorsBefore; oracleErrors != 0 {
		t.Fatalf("randomized balance-oracle errors = %d", oracleErrors)
	}
	if balanceCandidates != balanceMatches+balanceFallbacks || balanceMatches != published {
		t.Fatalf("randomized balance oracle candidates/matches/fallbacks/published = %d/%d/%d/%d", balanceCandidates, balanceMatches, balanceFallbacks, published)
	}
	serialCandidates := parallelTransferSerialVerifyCandidatesCounter.Snapshot().Count() - serialCandidatesBefore
	serialMatches := parallelTransferSerialVerifyMatchesCounter.Snapshot().Count() - serialMatchesBefore
	if serialCandidates != published || serialMatches != published {
		t.Fatalf("randomized serial verification candidates/matches/published = %d/%d/%d", serialCandidates, serialMatches, published)
	}
}

func TestProcessBlockParallelTransfersPreservesRepeatedRecipientBalance(t *testing.T) {
	base := newTestState(t)
	for _, id := range []byte{1, 2, 3} {
		base.CreateAccount(testProcessorAddr(id), corepb.AccountType_Normal)
	}
	base.AddBalance(testProcessorAddr(1), 11_708)
	base.SetAllowance(testProcessorAddr(1), 38_047_331_075)
	base.AddBalance(testProcessorAddr(2), 401_984_861)
	base.AddBalance(testProcessorAddr(3), 1_000_000_000_000_000)
	base.CreateAccount(params.BlackholeAddress, corepb.AccountType_Normal)
	contractAddr := testProcessorAddr(0x80)
	base.CreateAccount(contractAddr, corepb.AccountType_Contract)
	base.SetContract(contractAddr, &contractpb.SmartContract{
		OriginAddress: testProcessorAddr(3).Bytes(), ContractAddress: contractAddr.Bytes(),
	})
	base.SetCode(contractAddr, []byte{0x00})
	base.DynamicProperties().SetAllowCreationOfContracts(true)
	base.DynamicProperties().SetAllowAdaptiveEnergy(true)
	base.DynamicProperties().SetAllowBlackHoleOptimization(true)
	base.DynamicProperties().SetLatestBlockHeaderTimestamp(100_000_000)
	base.DynamicProperties().SetPublicNetUsage(1_000)
	base.DynamicProperties().SetPublicNetTime(0)
	passVersion3_6_5(base, 27)
	if _, err := base.Commit(); err != nil {
		t.Fatal(err)
	}

	amounts := []int64{1_000_000_000, 1_000_000_000, 1_000_000_000, 5_000_000_000, 12_000_000_000, 5_000_000_000, 12_000_000_000}
	transferIndices := []int{8, 38, 80, 81, 90, 95, 100}
	transactions := make([]*corepb.Transaction, 129)
	for i := range transactions {
		trigger := makeTestTriggerTx(3, contractAddr, nil).Proto()
		trigger.RawData.Expiration = 100_060_000
		trigger.RawData.FeeLimit = 10_000_000
		trigger.Ret = []*corepb.Transaction_Result{{ContractRet: corepb.Transaction_Result_SUCCESS}}
		transactions[i] = trigger
	}
	withdraw, err := anypb.New(&contractpb.WithdrawBalanceContract{OwnerAddress: testProcessorAddr(1).Bytes()})
	if err != nil {
		t.Fatal(err)
	}
	transactions[6] = &corepb.Transaction{RawData: &corepb.TransactionRaw{
		Expiration: 100_060_000,
		Contract: []*corepb.Transaction_Contract{{
			Type: corepb.Transaction_Contract_WithdrawBalanceContract, Parameter: withdraw,
		}},
	}}
	for i, amount := range amounts {
		transactions[transferIndices[i]] = makeTestTransferTx(1, 2, amount).Proto()
		transactions[transferIndices[i]].RawData.Expiration = 100_060_000
	}
	block := types.NewBlockFromPB(&corepb.Block{
		BlockHeader: &corepb.BlockHeader{RawData: &corepb.BlockHeaderRaw{
			Number: 32, Timestamp: 100_003_000,
		}},
		Transactions: transactions,
	})
	run := func(statedb *state.StateDB, db actuator.BufferedKVStore, options processBlockOptions) ([]*corepb.TransactionInfo, error) {
		infos, _, processErr := processBlockWithOptions(
			statedb, statedb.DynamicProperties(), block, db, nil, 0,
			params.DefaultBlockNumForEnergyLimit, false, tcommon.Hash{}, nil, nil,
			nil, forks.NewVersionPassCache(), new(transactionInfoBatch), true, -1, nil,
			options,
		)
		return infos, processErr
	}
	serialState, err := base.Copy()
	if err != nil {
		t.Fatal(err)
	}
	serialState.SetDynamicProperties(base.DynamicProperties().Copy())
	parallelState, err := base.Copy()
	if err != nil {
		t.Fatal(err)
	}
	parallelState.SetDynamicProperties(base.DynamicProperties().Copy())

	serialInfos, err := run(serialState, ethrawdb.NewMemoryDatabase(), processBlockOptions{})
	if err != nil {
		t.Fatalf("serial process: %v", err)
	}
	publishedBefore := parallelTransferPublishedCounter.Snapshot().Count()
	parallelInfos, err := run(parallelState, ethrawdb.NewMemoryDatabase(), processBlockOptions{parallelTransfers: true})
	if err != nil {
		t.Fatalf("parallel process: %v", err)
	}
	if published := parallelTransferPublishedCounter.Snapshot().Count() - publishedBefore; published != 0 {
		t.Fatalf("cross-family sender suffix published %d transfers, want serial fallback", published)
	}
	for i := range serialInfos {
		if !proto.Equal(serialInfos[i], parallelInfos[i]) {
			t.Fatalf("tx %d info mismatch\nserial=%v\nparallel=%v", i, serialInfos[i], parallelInfos[i])
		}
	}
	for _, id := range []byte{1, 2} {
		address := testProcessorAddr(id)
		if serial, parallel := serialState.GetBalance(address), parallelState.GetBalance(address); serial != parallel {
			t.Fatalf("account %d balance serial=%d parallel=%d", id, serial, parallel)
		}
	}
	serialRoot, err := serialState.Commit()
	if err != nil {
		t.Fatal(err)
	}
	parallelRoot, err := parallelState.Commit()
	if err != nil {
		t.Fatal(err)
	}
	if serialRoot != parallelRoot {
		t.Fatalf("state roots differ: serial=%x parallel=%x", serialRoot, parallelRoot)
	}
}

func TestProcessBlockPublishesVMSenderChainCohort(t *testing.T) {
	base := newTestState(t)
	dynProps := base.DynamicProperties()
	dynProps.SetAllowCreationOfContracts(true)
	dynProps.SetAllowAdaptiveEnergy(true)
	dynProps.SetAllowBlackHoleOptimization(true)
	dynProps.SetLatestBlockHeaderTimestamp(30_000)
	passVersion3_6_5(base, 27)

	owner1 := testProcessorAddr(1)
	owner2 := testProcessorAddr(3)
	contract1 := testProcessorAddr(0x80)
	contract2 := testProcessorAddr(0x81)
	for _, owner := range []tcommon.Address{owner1, owner2} {
		base.CreateAccount(owner, corepb.AccountType_Normal)
		base.AddBalance(owner, 100_000_000)
	}
	base.CreateAccount(params.BlackholeAddress, corepb.AccountType_Normal)
	for _, contract := range []struct {
		address tcommon.Address
		origin  tcommon.Address
	}{{address: contract1, origin: owner1}, {address: contract2, origin: owner2}} {
		base.CreateAccount(contract.address, corepb.AccountType_Contract)
		base.SetContract(contract.address, &contractpb.SmartContract{
			OriginAddress: contract.origin.Bytes(), ContractAddress: contract.address.Bytes(),
		})
		// PUSH1 0; CALLDATALOAD; PUSH1 0; SSTORE; STOP. This makes the
		// publication fixture exercise a persistent storage post-image instead
		// of proving equivalence only for resource and balance settlement.
		base.SetCode(contract.address, []byte{0x60, 0x00, 0x35, 0x60, 0x00, 0x55, 0x00})
	}
	// Interleaving a second sender forces the final owner1 result to cross a
	// forwarded storage boundary and an independently ordered public-net
	// boundary. Its second call overwrites the same slot written by the first.
	if _, err := base.Commit(); err != nil {
		t.Fatal(err)
	}

	serialState, err := base.Copy()
	if err != nil {
		t.Fatal(err)
	}
	serialState.SetDynamicProperties(base.DynamicProperties().Copy())
	parallelState, err := base.Copy()
	if err != nil {
		t.Fatal(err)
	}
	parallelState.SetDynamicProperties(base.DynamicProperties().Copy())
	transferOnlyState, err := base.Copy()
	if err != nil {
		t.Fatal(err)
	}
	transferOnlyState.SetDynamicProperties(base.DynamicProperties().Copy())
	corruptState, err := base.Copy()
	if err != nil {
		t.Fatal(err)
	}
	corruptState.SetDynamicProperties(base.DynamicProperties().Copy())
	preApplyCorruptState, err := base.Copy()
	if err != nil {
		t.Fatal(err)
	}
	preApplyCorruptState.SetDynamicProperties(base.DynamicProperties().Copy())
	payloadCorruptState, err := base.Copy()
	if err != nil {
		t.Fatal(err)
	}
	payloadCorruptState.SetDynamicProperties(base.DynamicProperties().Copy())

	storageInput := func(value byte) []byte {
		input := make([]byte, tcommon.HashLength)
		input[len(input)-1] = value
		return input
	}
	transactions := []*types.Transaction{
		makeTestTriggerTx(1, contract1, storageInput(1)),
		makeTestTriggerTx(3, contract2, storageInput(2)),
		makeTestTriggerTx(1, contract1, storageInput(3)),
		makeTestTransferTx(1, 3, 1_000),
	}
	transactionProtos := make([]*corepb.Transaction, len(transactions))
	for txIndex, tx := range transactions {
		tx.Proto().RawData.FeeLimit = 10_000_000
		tx.Proto().Ret = []*corepb.Transaction_Result{{ContractRet: corepb.Transaction_Result_SUCCESS}}
		transactionProtos[txIndex] = tx.Proto()
	}
	block := types.NewBlockFromPB(&corepb.Block{
		BlockHeader: &corepb.BlockHeader{RawData: &corepb.BlockHeaderRaw{
			Number: int64(vmSenderChainPublishInterval), Timestamp: 33_000,
		}},
		Transactions: transactionProtos,
	})

	run := func(statedb *state.StateDB, db actuator.BufferedKVStore, options processBlockOptions) ([]*corepb.TransactionInfo, *contractpb.BlockBalanceTrace, map[tcommon.Address]int64, error) {
		statedb.BeginBalanceTrace(int64(block.Number()), block.Hash().Bytes(), block.Timestamp())
		infos, _, processErr := processBlockWithOptions(
			statedb, statedb.DynamicProperties(), block, db, nil, 0,
			params.DefaultBlockNumForEnergyLimit, false, tcommon.Hash{}, nil, nil,
			nil, forks.NewVersionPassCache(), new(transactionInfoBatch), true, -1, nil,
			options,
		)
		trace, finalBalances := statedb.FinishBalanceTrace()
		return infos, trace, finalBalances, processErr
	}
	var serialTiming processBlockTiming
	serialInfos, serialTrace, serialFinalBalances, err := run(serialState, ethrawdb.NewMemoryDatabase(), processBlockOptions{captureBalanceTrace: true, timing: &serialTiming})
	if err != nil {
		t.Fatalf("serial VM process: %v", err)
	}
	var preApplyCorruptions int
	_, _, _, err = run(preApplyCorruptState, ethrawdb.NewMemoryDatabase(), processBlockOptions{
		parallelVM:          true,
		captureBalanceTrace: true,
		speculativePreApplyTestHook: func(family string, txIndex int, writes state.TransactionWriteSet) {
			if family != "VM" || txIndex != 0 {
				return
			}
			for key, value := range writes {
				if key.Kind != state.TransactionAccessStorage || len(value.Value) != tcommon.HashLength {
					continue
				}
				value.Value = append([]byte(nil), value.Value...)
				value.Value[len(value.Value)-1] ^= 1
				writes[key] = value
				preApplyCorruptions++
				return
			}
		},
	})
	if !errors.Is(err, errSpeculativePublicationAudit) {
		t.Fatalf("valid VM pre-apply mutation error = %v, want speculative safety sentinel", err)
	}
	if preApplyCorruptions != 1 {
		t.Fatalf("VM pre-apply corruptions = %d, want 1", preApplyCorruptions)
	}
	if _, exists := preApplyCorruptState.GetStateWithExist(contract1, tcommon.Hash{}); exists {
		t.Fatal("validly mutated VM attempt left storage behind after block rollback")
	}
	for _, address := range []tcommon.Address{owner1, owner2, contract1, contract2, params.BlackholeAddress} {
		if got, want := preApplyCorruptState.GetBalance(address), base.GetBalance(address); got != want {
			t.Fatalf("validly mutated VM attempt balance %s = %d, want rolled-back %d", address.Hex(), got, want)
		}
	}
	if got, want := preApplyCorruptState.DynamicProperties().BlockEnergyUsage(), base.DynamicProperties().BlockEnergyUsage(); got != want {
		t.Fatalf("validly mutated VM attempt block energy = %d, want rolled-back %d", got, want)
	}
	var payloadCorruptions int
	_, _, _, err = run(payloadCorruptState, ethrawdb.NewMemoryDatabase(), processBlockOptions{
		parallelVM:          true,
		captureBalanceTrace: true,
		speculativePostOracleTestHook: func(family string, txIndex int, result *discardShadowTaskResult) {
			if family != "VM" || txIndex != 0 || result == nil {
				return
			}
			if result.info == nil || result.balanceTrace == nil {
				t.Fatal("published VM fixture did not retain info and balance trace")
			}
			result.info.Fee++
			result.balanceTrace.TransactionIdentifier[0] ^= 1
			result.reads.Unsupported = !result.reads.Unsupported
			payloadCorruptions++
		},
	})
	if !errors.Is(err, errSpeculativePublicationAudit) {
		t.Fatalf("mutated VM payload error = %v, want speculative safety sentinel", err)
	}
	if payloadCorruptions != 1 {
		t.Fatalf("VM payload corruptions = %d, want 1", payloadCorruptions)
	}
	if _, exists := payloadCorruptState.GetStateWithExist(contract1, tcommon.Hash{}); exists {
		t.Fatal("mutated VM payload attempt left storage behind after block rollback")
	}
	for _, address := range []tcommon.Address{owner1, owner2, contract1, contract2, params.BlackholeAddress} {
		if got, want := payloadCorruptState.GetBalance(address), base.GetBalance(address); got != want {
			t.Fatalf("mutated VM payload attempt balance %s = %d, want rolled-back %d", address.Hex(), got, want)
		}
	}
	if got, want := payloadCorruptState.DynamicProperties().BlockEnergyUsage(), base.DynamicProperties().BlockEnergyUsage(); got != want {
		t.Fatalf("mutated VM payload attempt block energy = %d, want rolled-back %d", got, want)
	}
	var postApplyCorruptions int
	_, _, _, err = run(corruptState, ethrawdb.NewMemoryDatabase(), processBlockOptions{
		parallelVM:          true,
		captureBalanceTrace: true,
		speculativePostApplyTestHook: func(family string, txIndex int, writes state.TransactionWriteSet) {
			if family != "VM" || txIndex != 0 {
				return
			}
			for key := range writes {
				if key.Kind == state.TransactionAccessStorage {
					delete(writes, key)
					postApplyCorruptions++
					return
				}
			}
		},
	})
	if !errors.Is(err, errSpeculativePublicationAudit) {
		t.Fatalf("corrupted VM post-apply audit error = %v, want speculative safety sentinel", err)
	}
	if postApplyCorruptions != 1 {
		t.Fatalf("VM post-apply corruptions = %d, want 1", postApplyCorruptions)
	}
	if _, exists := corruptState.GetStateWithExist(contract1, tcommon.Hash{}); exists {
		t.Fatal("corrupted VM attempt left storage behind after block rollback")
	}
	for _, address := range []tcommon.Address{owner1, owner2, contract1, contract2, params.BlackholeAddress} {
		if got, want := corruptState.GetBalance(address), base.GetBalance(address); got != want {
			t.Fatalf("corrupted VM attempt balance %s = %d, want rolled-back %d", address.Hex(), got, want)
		}
	}
	if got, want := corruptState.DynamicProperties().BlockEnergyUsage(), base.DynamicProperties().BlockEnergyUsage(); got != want {
		t.Fatalf("corrupted VM attempt block energy = %d, want rolled-back %d", got, want)
	}
	vmPublishedWithTransferOnlyBefore := parallelVMPublishedCounter.Snapshot().Count()
	transferOnlyInfos, transferOnlyTrace, transferOnlyFinalBalances, err := run(transferOnlyState, ethrawdb.NewMemoryDatabase(), processBlockOptions{parallelTransfers: true, captureBalanceTrace: true})
	if err != nil {
		t.Fatalf("Transfer-only process: %v", err)
	}
	if published := parallelVMPublishedCounter.Snapshot().Count() - vmPublishedWithTransferOnlyBefore; published != 0 {
		t.Fatalf("Transfer-only option published %d VM results, want 0", published)
	}
	for txIndex := range serialInfos {
		if !proto.Equal(serialInfos[txIndex], transferOnlyInfos[txIndex]) {
			t.Fatalf("Transfer-only tx %d info mismatch\nserial=%v\ntransfer-only=%v", txIndex, serialInfos[txIndex], transferOnlyInfos[txIndex])
		}
	}
	if !proto.Equal(serialTrace, transferOnlyTrace) {
		t.Fatalf("Transfer-only block balance trace mismatch\nserial=%v\ntransfer-only=%v", serialTrace, transferOnlyTrace)
	}
	if len(serialFinalBalances) != len(transferOnlyFinalBalances) {
		t.Fatalf("Transfer-only final balance count serial=%d transfer-only=%d", len(serialFinalBalances), len(transferOnlyFinalBalances))
	}
	for address, serialBalance := range serialFinalBalances {
		if transferOnlyBalance, ok := transferOnlyFinalBalances[address]; !ok || transferOnlyBalance != serialBalance {
			t.Fatalf("Transfer-only final balance %s serial=%d transfer-only=%d present=%t", address.Hex(), serialBalance, transferOnlyBalance, ok)
		}
	}
	blocksBefore := parallelVMBlocksCounter.Snapshot().Count()
	preexecutedBefore := parallelVMPreexecutedCounter.Snapshot().Count()
	candidatesBefore := parallelVMCandidatesCounter.Snapshot().Count()
	publishedBefore := parallelVMPublishedCounter.Snapshot().Count()
	chainCandidatesBefore := parallelVMChainCandidatesCounter.Snapshot().Count()
	chainPublishedBefore := parallelVMChainPublishedCounter.Snapshot().Count()
	energyPublishedBefore := parallelVMBlockEnergyPublishedCounter.Snapshot().Count()
	publicNetReservationsBefore := parallelVMPublicNetReservationsCounter.Snapshot().Count()
	publicNetPublishedBefore := parallelVMPublicNetPublishedCounter.Snapshot().Count()
	publicNetRebasedBefore := parallelVMPublicNetRebasedCounter.Snapshot().Count()
	projectionMatchesBefore := discardShadowVMPublicNetProjectionMatchesCounter.Snapshot().Count()
	projectionMismatchesBefore := discardShadowVMPublicNetProjectionMismatchCounter.Snapshot().Count()
	projectionMissingBefore := discardShadowVMPublicNetProjectionMissingCounter.Snapshot().Count()
	energyMatchesBefore := discardShadowVMBlockEnergyMatchesCounter.Snapshot().Count()
	energyMismatchesBefore := discardShadowVMBlockEnergyMismatchesCounter.Snapshot().Count()
	energyMissingBefore := discardShadowVMBlockEnergyMissingCounter.Snapshot().Count()
	serialVerifyCandidatesBefore := parallelVMSerialVerifyCandidatesCounter.Snapshot().Count()
	serialVerifyMatchesBefore := parallelVMSerialVerifyMatchesCounter.Snapshot().Count()
	serialVerifyInfoMismatchesBefore := parallelVMSerialVerifyInfoMismatchCounter.Snapshot().Count()
	serialVerifyWriteMismatchesBefore := parallelVMSerialVerifyWriteMismatchCounter.Snapshot().Count()
	serialVerifyBalanceMismatchesBefore := parallelVMSerialVerifyBalanceMismatchCounter.Snapshot().Count()
	serialVerifyErrorsBefore := parallelVMSerialVerifyErrorsCounter.Snapshot().Count()
	dualOracleCandidatesBefore := parallelVMDualOracleCandidatesCounter.Snapshot().Count()
	dualOracleMatchesBefore := parallelVMDualOracleMatchesCounter.Snapshot().Count()
	dualOracleInfoMismatchesBefore := parallelVMDualOracleInfoMismatchCounter.Snapshot().Count()
	dualOracleWriteMismatchesBefore := parallelVMDualOracleWriteMismatchCounter.Snapshot().Count()
	dualOracleBalanceMismatchesBefore := parallelVMDualOracleBalanceMismatchCounter.Snapshot().Count()
	dualOracleErrorsBefore := parallelVMDualOracleErrorsCounter.Snapshot().Count()
	writeSealCandidatesBefore := parallelVMWriteSealCandidatesCounter.Snapshot().Count()
	writeSealMatchesBefore := parallelVMWriteSealMatchesCounter.Snapshot().Count()
	writeSealMismatchesBefore := parallelVMWriteSealMismatchesCounter.Snapshot().Count()
	errorsBefore := parallelVMErrorsCounter.Snapshot().Count()
	fallbacksBefore := parallelVMUnavailableFallbackCounter.Snapshot().Count() +
		parallelVMConflictFallbackCounter.Snapshot().Count() +
		parallelVMPreflightFallbackCounter.Snapshot().Count() +
		parallelVMPublicNetFallbackCounter.Snapshot().Count() +
		parallelVMBlockEnergyFallbackCounter.Snapshot().Count()
	var parallelTiming processBlockTiming
	parallelInfos, parallelTrace, parallelFinalBalances, err := run(parallelState, ethrawdb.NewMemoryDatabase(), processBlockOptions{parallelVM: true, captureBalanceTrace: true, timing: &parallelTiming})
	if err != nil {
		t.Fatalf("parallel VM process: %v", err)
	}
	if serialTiming.VMTransactions != 3 || serialTiming.NativeTransactions != 1 {
		t.Fatalf("serial transaction telemetry = %d VM/%d native, want 3/1", serialTiming.VMTransactions, serialTiming.NativeTransactions)
	}
	if parallelTiming.VMTransactions != 3 || parallelTiming.NativeTransactions != 1 {
		t.Fatalf("parallel transaction telemetry = %d VM/%d native, want 3/1", parallelTiming.VMTransactions, parallelTiming.NativeTransactions)
	}
	if serialTiming.VMExecution <= 0 || parallelTiming.VMExecution <= 0 {
		t.Fatalf("VM execution telemetry = serial %s, parallel %s; want both positive", serialTiming.VMExecution, parallelTiming.VMExecution)
	}
	if serialTiming.VMRawEnergyUsage <= 0 || parallelTiming.VMRawEnergyUsage != serialTiming.VMRawEnergyUsage {
		t.Fatalf("VM raw energy telemetry = serial %d, parallel %d; want equal positive canonical totals", serialTiming.VMRawEnergyUsage, parallelTiming.VMRawEnergyUsage)
	}
	if blocks := parallelVMBlocksCounter.Snapshot().Count() - blocksBefore; blocks != 1 {
		t.Fatalf("parallel VM blocks = %d, want 1", blocks)
	}
	if preexecuted := parallelVMPreexecutedCounter.Snapshot().Count() - preexecutedBefore; preexecuted != 3 {
		t.Fatalf("parallel VM preexecuted = %d, want 3", preexecuted)
	}
	if candidates := parallelVMCandidatesCounter.Snapshot().Count() - candidatesBefore; candidates != 3 {
		t.Fatalf("parallel VM candidates = %d, want 3 (published=%d unavailable=%d conflict=%d preflight=%d public=%d energy=%d)",
			candidates,
			parallelVMPublishedCounter.Snapshot().Count()-publishedBefore,
			parallelVMUnavailableFallbackCounter.Snapshot().Count(),
			parallelVMConflictFallbackCounter.Snapshot().Count(),
			parallelVMPreflightFallbackCounter.Snapshot().Count(),
			parallelVMPublicNetFallbackCounter.Snapshot().Count(),
			parallelVMBlockEnergyFallbackCounter.Snapshot().Count())
	}
	if published := parallelVMPublishedCounter.Snapshot().Count() - publishedBefore; published != 3 {
		t.Fatalf("parallel VM published = %d, want 3", published)
	}
	if candidates := parallelVMChainCandidatesCounter.Snapshot().Count() - chainCandidatesBefore; candidates != 1 {
		t.Fatalf("parallel VM sender-chain candidates = %d, want 1", candidates)
	}
	if published := parallelVMChainPublishedCounter.Snapshot().Count() - chainPublishedBefore; published != 1 {
		t.Fatalf("parallel VM sender-chain published = %d, want 1", published)
	}
	if published := parallelVMBlockEnergyPublishedCounter.Snapshot().Count() - energyPublishedBefore; published != 3 {
		t.Fatalf("parallel VM block-energy publications = %d, want 3", published)
	}
	if reservations := parallelVMPublicNetReservationsCounter.Snapshot().Count() - publicNetReservationsBefore; reservations != 3 {
		t.Fatalf("parallel VM public-net reservations = %d, want 3", reservations)
	}
	if published := parallelVMPublicNetPublishedCounter.Snapshot().Count() - publicNetPublishedBefore; published != 3 {
		t.Fatalf("parallel VM public-net publications = %d, want 3", published)
	}
	if rebased := parallelVMPublicNetRebasedCounter.Snapshot().Count() - publicNetRebasedBefore; rebased != 2 {
		t.Fatalf("parallel VM public-net rebases = %d, want 2", rebased)
	}
	if matches := discardShadowVMPublicNetProjectionMatchesCounter.Snapshot().Count() - projectionMatchesBefore; matches != 3 {
		t.Fatalf("parallel VM projected public-net matches = %d, want 3", matches)
	}
	if mismatches := discardShadowVMPublicNetProjectionMismatchCounter.Snapshot().Count() - projectionMismatchesBefore; mismatches != 0 {
		t.Fatalf("parallel VM projected public-net mismatches = %d, want 0", mismatches)
	}
	if missing := discardShadowVMPublicNetProjectionMissingCounter.Snapshot().Count() - projectionMissingBefore; missing != 0 {
		t.Fatalf("parallel VM projected public-net missing = %d, want 0", missing)
	}
	if matches := discardShadowVMBlockEnergyMatchesCounter.Snapshot().Count() - energyMatchesBefore; matches != 3 {
		t.Fatalf("parallel VM projected block-energy matches = %d, want 3", matches)
	}
	if mismatches := discardShadowVMBlockEnergyMismatchesCounter.Snapshot().Count() - energyMismatchesBefore; mismatches != 0 {
		t.Fatalf("parallel VM projected block-energy mismatches = %d, want 0", mismatches)
	}
	if missing := discardShadowVMBlockEnergyMissingCounter.Snapshot().Count() - energyMissingBefore; missing != 0 {
		t.Fatalf("parallel VM projected block-energy missing = %d, want 0", missing)
	}
	if failures := parallelVMErrorsCounter.Snapshot().Count() - errorsBefore; failures != 0 {
		t.Fatalf("parallel VM errors = %d, want 0", failures)
	}
	if candidates := parallelVMSerialVerifyCandidatesCounter.Snapshot().Count() - serialVerifyCandidatesBefore; candidates != 3 {
		t.Fatalf("parallel VM boundary serial verification candidates = %d, want 3", candidates)
	}
	if matches := parallelVMSerialVerifyMatchesCounter.Snapshot().Count() - serialVerifyMatchesBefore; matches != 3 {
		t.Fatalf("parallel VM boundary serial verification matches = %d, want 3", matches)
	}
	if mismatches := parallelVMSerialVerifyInfoMismatchCounter.Snapshot().Count() - serialVerifyInfoMismatchesBefore; mismatches != 0 {
		t.Fatalf("parallel VM boundary serial info mismatches = %d, want 0", mismatches)
	}
	if mismatches := parallelVMSerialVerifyWriteMismatchCounter.Snapshot().Count() - serialVerifyWriteMismatchesBefore; mismatches != 0 {
		t.Fatalf("parallel VM boundary serial write mismatches = %d, want 0", mismatches)
	}
	if mismatches := parallelVMSerialVerifyBalanceMismatchCounter.Snapshot().Count() - serialVerifyBalanceMismatchesBefore; mismatches != 0 {
		t.Fatalf("parallel VM boundary serial balance mismatches = %d, want 0", mismatches)
	}
	if failures := parallelVMSerialVerifyErrorsCounter.Snapshot().Count() - serialVerifyErrorsBefore; failures != 0 {
		t.Fatalf("parallel VM boundary serial verification errors = %d, want 0", failures)
	}
	if candidates := parallelVMDualOracleCandidatesCounter.Snapshot().Count() - dualOracleCandidatesBefore; candidates != 3 {
		t.Fatalf("parallel VM dual-oracle candidates = %d, want 3", candidates)
	}
	if matches := parallelVMDualOracleMatchesCounter.Snapshot().Count() - dualOracleMatchesBefore; matches != 3 {
		t.Fatalf("parallel VM dual-oracle matches = %d, want 3", matches)
	}
	if mismatches := parallelVMDualOracleInfoMismatchCounter.Snapshot().Count() - dualOracleInfoMismatchesBefore; mismatches != 0 {
		t.Fatalf("parallel VM dual-oracle info mismatches = %d, want 0", mismatches)
	}
	if mismatches := parallelVMDualOracleWriteMismatchCounter.Snapshot().Count() - dualOracleWriteMismatchesBefore; mismatches != 0 {
		t.Fatalf("parallel VM dual-oracle write mismatches = %d, want 0", mismatches)
	}
	if mismatches := parallelVMDualOracleBalanceMismatchCounter.Snapshot().Count() - dualOracleBalanceMismatchesBefore; mismatches != 0 {
		t.Fatalf("parallel VM dual-oracle balance mismatches = %d, want 0", mismatches)
	}
	if failures := parallelVMDualOracleErrorsCounter.Snapshot().Count() - dualOracleErrorsBefore; failures != 0 {
		t.Fatalf("parallel VM dual-oracle errors = %d, want 0", failures)
	}
	if candidates := parallelVMWriteSealCandidatesCounter.Snapshot().Count() - writeSealCandidatesBefore; candidates != 3 {
		t.Fatalf("parallel VM WriteSet seal candidates = %d, want 3", candidates)
	}
	if matches := parallelVMWriteSealMatchesCounter.Snapshot().Count() - writeSealMatchesBefore; matches != 3 {
		t.Fatalf("parallel VM WriteSet seal matches = %d, want 3", matches)
	}
	if mismatches := parallelVMWriteSealMismatchesCounter.Snapshot().Count() - writeSealMismatchesBefore; mismatches != 0 {
		t.Fatalf("parallel VM WriteSet seal mismatches = %d, want 0", mismatches)
	}
	fallbacksAfter := parallelVMUnavailableFallbackCounter.Snapshot().Count() +
		parallelVMConflictFallbackCounter.Snapshot().Count() +
		parallelVMPreflightFallbackCounter.Snapshot().Count() +
		parallelVMPublicNetFallbackCounter.Snapshot().Count() +
		parallelVMBlockEnergyFallbackCounter.Snapshot().Count()
	if fallbacks := fallbacksAfter - fallbacksBefore; fallbacks != 0 {
		t.Fatalf("parallel VM fallbacks = %d, want 0", fallbacks)
	}

	for txIndex := range serialInfos {
		if !proto.Equal(serialInfos[txIndex], parallelInfos[txIndex]) {
			t.Fatalf("tx %d info mismatch\nserial=%v\nparallel=%v", txIndex, serialInfos[txIndex], parallelInfos[txIndex])
		}
	}
	if !proto.Equal(serialTrace, parallelTrace) {
		t.Fatalf("block balance trace mismatch\nserial=%v\nparallel=%v", serialTrace, parallelTrace)
	}
	if len(serialFinalBalances) != len(parallelFinalBalances) {
		t.Fatalf("final balance count serial=%d parallel=%d", len(serialFinalBalances), len(parallelFinalBalances))
	}
	for address, serialBalance := range serialFinalBalances {
		if parallelBalance, ok := parallelFinalBalances[address]; !ok || parallelBalance != serialBalance {
			t.Fatalf("final balance %s serial=%d parallel=%d present=%t", address.Hex(), serialBalance, parallelBalance, ok)
		}
	}
	for _, address := range []tcommon.Address{owner1, owner2, contract1, contract2} {
		if serialBalance, parallelBalance := serialState.GetBalance(address), parallelState.GetBalance(address); serialBalance != parallelBalance {
			t.Fatalf("balance %s serial=%d parallel=%d", address.Hex(), serialBalance, parallelBalance)
		}
	}
	storageSlot := tcommon.Hash{}
	for _, contract := range []struct {
		address tcommon.Address
		want    byte
	}{{address: contract1, want: 3}, {address: contract2, want: 2}} {
		serialValue, serialExists := serialState.GetStateWithExist(contract.address, storageSlot)
		parallelValue, parallelExists := parallelState.GetStateWithExist(contract.address, storageSlot)
		if !serialExists || !parallelExists || serialValue != parallelValue || serialValue[31] != contract.want {
			t.Fatalf("storage %s serial=%x/%t parallel=%x/%t, want ...%02x",
				contract.address.Hex(), serialValue, serialExists, parallelValue, parallelExists, contract.want)
		}
	}
	for _, property := range []struct {
		name     string
		serial   int64
		parallel int64
	}{
		{name: "public_net_usage", serial: serialState.DynamicProperties().PublicNetUsage(), parallel: parallelState.DynamicProperties().PublicNetUsage()},
		{name: "public_net_time", serial: serialState.DynamicProperties().PublicNetTime(), parallel: parallelState.DynamicProperties().PublicNetTime()},
		{name: "block_energy_usage", serial: serialState.DynamicProperties().BlockEnergyUsage(), parallel: parallelState.DynamicProperties().BlockEnergyUsage()},
	} {
		if property.serial != property.parallel {
			t.Fatalf("%s serial=%d parallel=%d", property.name, property.serial, property.parallel)
		}
	}
	serialRoot, err := serialState.Commit()
	if err != nil {
		t.Fatal(err)
	}
	parallelRoot, err := parallelState.Commit()
	if err != nil {
		t.Fatal(err)
	}
	transferOnlyRoot, err := transferOnlyState.Commit()
	if err != nil {
		t.Fatal(err)
	}
	if serialRoot != parallelRoot {
		t.Fatalf("VM state roots differ: serial=%x parallel=%x", serialRoot, parallelRoot)
	}
	if serialRoot != transferOnlyRoot {
		t.Fatalf("Transfer-only state root differs: serial=%x transfer-only=%x", serialRoot, transferOnlyRoot)
	}
}

func TestProcessBlockPublishesVMInternalCreate(t *testing.T) {
	base := newTestState(t)
	dynProps := base.DynamicProperties()
	dynProps.SetAllowCreationOfContracts(true)
	dynProps.SetAllowAdaptiveEnergy(true)
	dynProps.SetAllowBlackHoleOptimization(true)
	dynProps.SetAllowMultiSign(true)
	dynProps.SetLatestBlockHeaderTimestamp(30_000)
	passVersion3_6_5(base, 27)

	owner := testProcessorAddr(1)
	parent := testProcessorAddr(0x8a)
	base.CreateAccount(owner, corepb.AccountType_Normal)
	base.AddBalance(owner, 100_000_000)
	base.CreateAccount(params.BlackholeAddress, corepb.AccountType_Normal)
	base.CreateAccount(parent, corepb.AccountType_Contract)
	base.SetContract(parent, &contractpb.SmartContract{
		OriginAddress: owner.Bytes(), ContractAddress: parent.Bytes(),
	})
	// The child constructor returns one STOP byte as its runtime. The parent
	// copies that constructor from its own code, CREATEs the child, discards the
	// returned address and stops. A successful publication must therefore carry
	// one fresh account envelope, code and contract metadata plus a LOG payload
	// and the internal CREATE record in addition to ordinary resource settlement.
	// The child address is also derived independently from java-tron's root-
	// tx/nonce formula below and its complete canonical post-state is checked
	// directly.
	childInit := []byte{
		0x60, 0x01, 0x60, 0x0c, 0x60, 0x00, 0x39,
		0x60, 0x01, 0x60, 0x00, 0xf3,
		0x00,
	}
	parentCode := []byte{
		0x60, byte(len(childInit)), 0x60, 0x00, 0x60, 0x00, 0x39,
		0x60, byte(len(childInit)), 0x60, 0x00, 0x60, 0x00, 0xf0, 0x50,
		0x60, 0x01, 0x60, 0x00, 0x52,
		0x60, 0x20, 0x60, 0x00, 0xa0,
		0x00,
	}
	parentCode[3] = byte(len(parentCode))
	base.SetCode(parent, append(parentCode, childInit...))
	if _, err := base.Commit(); err != nil {
		t.Fatal(err)
	}

	serialState, err := base.Copy()
	if err != nil {
		t.Fatal(err)
	}
	serialState.SetDynamicProperties(base.DynamicProperties().Copy())
	parallelState, err := base.Copy()
	if err != nil {
		t.Fatal(err)
	}
	parallelState.SetDynamicProperties(base.DynamicProperties().Copy())

	tx := makeTestTriggerTx(1, parent, nil)
	tx.Proto().RawData.FeeLimit = 10_000_000
	tx.Proto().Ret = []*corepb.Transaction_Result{{ContractRet: corepb.Transaction_Result_SUCCESS}}
	block := types.NewBlockFromPB(&corepb.Block{
		BlockHeader: &corepb.BlockHeader{RawData: &corepb.BlockHeaderRaw{
			Number: int64(vmSenderChainPublishInterval), Timestamp: 33_000,
		}},
		Transactions: []*corepb.Transaction{tx.Proto()},
	})
	run := func(statedb *state.StateDB, options processBlockOptions) ([]*corepb.TransactionInfo, error) {
		infos, _, processErr := processBlockWithOptions(
			statedb, statedb.DynamicProperties(), block, ethrawdb.NewMemoryDatabase(), nil, 0,
			params.DefaultBlockNumForEnergyLimit, false, tcommon.Hash{}, nil, nil,
			nil, forks.NewVersionPassCache(), new(transactionInfoBatch), true, -1, nil,
			options,
		)
		return infos, processErr
	}
	serialInfos, err := run(serialState, processBlockOptions{saveInternalTx: true})
	if err != nil {
		t.Fatalf("serial internal CREATE: %v", err)
	}
	publishedBefore := parallelVMPublishedCounter.Snapshot().Count()
	dualCandidatesBefore := parallelVMDualOracleCandidatesCounter.Snapshot().Count()
	dualMatchesBefore := parallelVMDualOracleMatchesCounter.Snapshot().Count()
	dualErrorsBefore := parallelVMDualOracleErrorsCounter.Snapshot().Count()
	parallelInfos, err := run(parallelState, processBlockOptions{parallelVM: true, saveInternalTx: true})
	if err != nil {
		t.Fatalf("parallel internal CREATE: %v", err)
	}
	if published := parallelVMPublishedCounter.Snapshot().Count() - publishedBefore; published != 1 {
		t.Fatalf("internal CREATE VM publications = %d, want 1", published)
	}
	if candidates := parallelVMDualOracleCandidatesCounter.Snapshot().Count() - dualCandidatesBefore; candidates != 1 {
		t.Fatalf("internal CREATE dual-oracle candidates = %d, want 1", candidates)
	}
	if matches := parallelVMDualOracleMatchesCounter.Snapshot().Count() - dualMatchesBefore; matches != 1 {
		t.Fatalf("internal CREATE dual-oracle matches = %d, want 1", matches)
	}
	if failures := parallelVMDualOracleErrorsCounter.Snapshot().Count() - dualErrorsBefore; failures != 0 {
		t.Fatalf("internal CREATE dual-oracle errors = %d, want 0", failures)
	}
	if len(serialInfos) != 1 || len(parallelInfos) != 1 || !proto.Equal(serialInfos[0], parallelInfos[0]) {
		t.Fatalf("internal CREATE receipt mismatch\nserial=%v\nparallel=%v", serialInfos, parallelInfos)
	}
	if logs := serialInfos[0].GetLog(); len(logs) != 1 || len(logs[0].GetData()) != tcommon.HashLength || logs[0].GetData()[tcommon.HashLength-1] != 1 {
		t.Fatalf("internal CREATE log = %v, want one 32-byte ...01 payload", logs)
	}
	var createSeed [tcommon.HashLength + 8]byte
	txHash := tx.Hash()
	copy(createSeed[:tcommon.HashLength], txHash[:])
	binary.BigEndian.PutUint64(createSeed[tcommon.HashLength:], 0)
	childHash := tcommon.Keccak256(createSeed[:])
	var child tcommon.Address
	child[0] = 0x41
	copy(child[1:], childHash[12:])
	for name, infos := range map[string][]*corepb.TransactionInfo{"serial": serialInfos, "parallel": parallelInfos} {
		internal := infos[0].GetInternalTransactions()
		if len(internal) != 1 || string(internal[0].GetNote()) != "create" || internal[0].GetRejected() ||
			!bytes.Equal(internal[0].GetCallerAddress(), parent.Bytes()) ||
			!bytes.Equal(internal[0].GetTransferToAddress(), child.Bytes()) || len(internal[0].GetHash()) != tcommon.HashLength {
			t.Fatalf("%s internal CREATE record = %+v", name, internal)
		}
	}
	for name, statedb := range map[string]*state.StateDB{"serial": serialState, "parallel": parallelState} {
		if !statedb.AccountExists(child) {
			t.Fatalf("%s child account is absent", name)
		}
		if code := statedb.GetCode(child); !bytes.Equal(code, []byte{0x00}) {
			t.Fatalf("%s child code = %x, want STOP", name, code)
		}
		metadata := statedb.GetContract(child)
		if metadata == nil || !bytes.Equal(metadata.GetContractAddress(), child.Bytes()) {
			t.Fatalf("%s child metadata = %v", name, metadata)
		}
	}
	serialRoot, err := serialState.Commit()
	if err != nil {
		t.Fatal(err)
	}
	parallelRoot, err := parallelState.Commit()
	if err != nil {
		t.Fatal(err)
	}
	if serialRoot != parallelRoot {
		t.Fatalf("internal CREATE roots differ: serial=%x parallel=%x", serialRoot, parallelRoot)
	}
}

func TestProcessBlockPublishesVMCallTokenAccountKV(t *testing.T) {
	base := newTestState(t)
	dynProps := base.DynamicProperties()
	dynProps.SetAllowCreationOfContracts(true)
	dynProps.SetAllowAdaptiveEnergy(true)
	dynProps.SetAllowBlackHoleOptimization(true)
	dynProps.SetAllowTvmTransferTrc10(true)
	dynProps.SetAllowMultiSign(true)
	dynProps.SetAllowSameTokenName(true)
	dynProps.SetLatestBlockHeaderTimestamp(30_000)
	passVersion3_6_5(base, 27)

	const tokenID = int64(1_000_001)
	owner := testProcessorAddr(1)
	contractAddr := testProcessorAddr(0x8f)
	recipient := testProcessorAddr(0x90)
	base.CreateAccount(owner, corepb.AccountType_Normal)
	base.AddBalance(owner, 100_000_000)
	base.CreateAccount(params.BlackholeAddress, corepb.AccountType_Normal)
	base.CreateAccount(contractAddr, corepb.AccountType_Contract)
	base.SetContract(contractAddr, &contractpb.SmartContract{
		OriginAddress: owner.Bytes(), ContractAddress: contractAddr.Bytes(),
	})
	base.CreateAccount(recipient, corepb.AccountType_Normal)
	base.SetTRC10Balance(contractAddr, tokenID, 100)
	base.SetTRC10Balance(recipient, tokenID, 10)
	// retSize, retOffset, inSize, inOffset, tokenID, tokenValue, recipient,
	// forwarded energy; CALLTOKEN; POP; STOP. The successful nested token
	// transfer writes two AccountAssetV2 AccountKV rows, a state family that
	// must be carried by the canonical publication seal rather than inferred
	// from the compact account envelope.
	code := []byte{
		byte(vm.PUSH1), 0x00,
		byte(vm.PUSH1), 0x00,
		byte(vm.PUSH1), 0x00,
		byte(vm.PUSH1), 0x00,
		byte(vm.PUSH3), 0x0f, 0x42, 0x41,
		byte(vm.PUSH1), 0x07,
		byte(vm.PUSH20),
	}
	code = append(code, recipient[1:]...)
	code = append(code,
		byte(vm.PUSH2), 0xff, 0xff,
		byte(vm.CALLTOKEN), byte(vm.POP), byte(vm.STOP),
	)
	base.SetCode(contractAddr, code)
	if _, err := base.Commit(); err != nil {
		t.Fatal(err)
	}

	serialState, err := base.Copy()
	if err != nil {
		t.Fatal(err)
	}
	serialState.SetDynamicProperties(base.DynamicProperties().Copy())
	parallelState, err := base.Copy()
	if err != nil {
		t.Fatal(err)
	}
	parallelState.SetDynamicProperties(base.DynamicProperties().Copy())

	tx := makeTestTriggerTx(1, contractAddr, nil)
	tx.Proto().RawData.FeeLimit = 10_000_000
	tx.Proto().Ret = []*corepb.Transaction_Result{{ContractRet: corepb.Transaction_Result_SUCCESS}}
	block := types.NewBlockFromPB(&corepb.Block{
		BlockHeader: &corepb.BlockHeader{RawData: &corepb.BlockHeaderRaw{
			Number: int64(vmSenderChainPublishInterval), Timestamp: 33_000,
		}},
		Transactions: []*corepb.Transaction{tx.Proto()},
	})
	run := func(statedb *state.StateDB, options processBlockOptions) ([]*corepb.TransactionInfo, error) {
		infos, _, processErr := processBlockWithOptions(
			statedb, statedb.DynamicProperties(), block, ethrawdb.NewMemoryDatabase(), nil, 0,
			params.DefaultBlockNumForEnergyLimit, false, tcommon.Hash{}, nil, nil,
			nil, forks.NewVersionPassCache(), new(transactionInfoBatch), true, -1, nil,
			options,
		)
		return infos, processErr
	}
	serialInfos, err := run(serialState, processBlockOptions{})
	if err != nil {
		t.Fatalf("serial VM CALLTOKEN: %v", err)
	}

	publishedBefore := parallelVMPublishedCounter.Snapshot().Count()
	dualCandidatesBefore := parallelVMDualOracleCandidatesCounter.Snapshot().Count()
	dualMatchesBefore := parallelVMDualOracleMatchesCounter.Snapshot().Count()
	dualInfoMismatchBefore := parallelVMDualOracleInfoMismatchCounter.Snapshot().Count()
	dualWriteMismatchBefore := parallelVMDualOracleWriteMismatchCounter.Snapshot().Count()
	dualBalanceMismatchBefore := parallelVMDualOracleBalanceMismatchCounter.Snapshot().Count()
	dualErrorsBefore := parallelVMDualOracleErrorsCounter.Snapshot().Count()
	sealCandidatesBefore := parallelVMWriteSealCandidatesCounter.Snapshot().Count()
	sealMatchesBefore := parallelVMWriteSealMatchesCounter.Snapshot().Count()
	sealMismatchesBefore := parallelVMWriteSealMismatchesCounter.Snapshot().Count()
	auditCandidatesBefore := parallelVMPublishAuditCandidatesCounter.Snapshot().Count()
	auditMatchesBefore := parallelVMPublishAuditMatchesCounter.Snapshot().Count()
	auditMismatchesBefore := parallelVMPublishAuditMismatchesCounter.Snapshot().Count()
	auditErrorsBefore := parallelVMPublishAuditErrorsCounter.Snapshot().Count()
	fallbacksBefore := parallelVMUnavailableFallbackCounter.Snapshot().Count() +
		parallelVMConflictFallbackCounter.Snapshot().Count() +
		parallelVMPreflightFallbackCounter.Snapshot().Count() +
		parallelVMPublicNetFallbackCounter.Snapshot().Count() +
		parallelVMBlockEnergyFallbackCounter.Snapshot().Count()
	parallelInfos, err := run(parallelState, processBlockOptions{parallelVM: true})
	if err != nil {
		t.Fatalf("parallel VM CALLTOKEN: %v", err)
	}
	if published := parallelVMPublishedCounter.Snapshot().Count() - publishedBefore; published != 1 {
		t.Fatalf("CALLTOKEN VM publications = %d, want 1", published)
	}
	if candidates := parallelVMDualOracleCandidatesCounter.Snapshot().Count() - dualCandidatesBefore; candidates != 1 {
		t.Fatalf("CALLTOKEN dual-oracle candidates = %d, want 1", candidates)
	}
	if matches := parallelVMDualOracleMatchesCounter.Snapshot().Count() - dualMatchesBefore; matches != 1 {
		t.Fatalf("CALLTOKEN dual-oracle matches = %d, want 1", matches)
	}
	if failures := parallelVMDualOracleInfoMismatchCounter.Snapshot().Count() - dualInfoMismatchBefore +
		parallelVMDualOracleWriteMismatchCounter.Snapshot().Count() - dualWriteMismatchBefore +
		parallelVMDualOracleBalanceMismatchCounter.Snapshot().Count() - dualBalanceMismatchBefore +
		parallelVMDualOracleErrorsCounter.Snapshot().Count() - dualErrorsBefore; failures != 0 {
		t.Fatalf("CALLTOKEN dual-oracle mismatches/errors = %d, want 0", failures)
	}
	if candidates := parallelVMWriteSealCandidatesCounter.Snapshot().Count() - sealCandidatesBefore; candidates != 1 {
		t.Fatalf("CALLTOKEN write-seal candidates = %d, want 1", candidates)
	}
	if matches := parallelVMWriteSealMatchesCounter.Snapshot().Count() - sealMatchesBefore; matches != 1 {
		t.Fatalf("CALLTOKEN write-seal matches = %d, want 1", matches)
	}
	if mismatches := parallelVMWriteSealMismatchesCounter.Snapshot().Count() - sealMismatchesBefore; mismatches != 0 {
		t.Fatalf("CALLTOKEN write-seal mismatches = %d, want 0", mismatches)
	}
	if candidates := parallelVMPublishAuditCandidatesCounter.Snapshot().Count() - auditCandidatesBefore; candidates != 1 {
		t.Fatalf("CALLTOKEN publish-audit candidates = %d, want 1", candidates)
	}
	if matches := parallelVMPublishAuditMatchesCounter.Snapshot().Count() - auditMatchesBefore; matches != 1 {
		t.Fatalf("CALLTOKEN publish-audit matches = %d, want 1", matches)
	}
	if failures := parallelVMPublishAuditMismatchesCounter.Snapshot().Count() - auditMismatchesBefore +
		parallelVMPublishAuditErrorsCounter.Snapshot().Count() - auditErrorsBefore; failures != 0 {
		t.Fatalf("CALLTOKEN publish-audit mismatches/errors = %d, want 0", failures)
	}
	fallbacksAfter := parallelVMUnavailableFallbackCounter.Snapshot().Count() +
		parallelVMConflictFallbackCounter.Snapshot().Count() +
		parallelVMPreflightFallbackCounter.Snapshot().Count() +
		parallelVMPublicNetFallbackCounter.Snapshot().Count() +
		parallelVMBlockEnergyFallbackCounter.Snapshot().Count()
	if fallbacks := fallbacksAfter - fallbacksBefore; fallbacks != 0 {
		t.Fatalf("CALLTOKEN VM fallbacks = %d, want 0", fallbacks)
	}
	if len(serialInfos) != 1 || len(parallelInfos) != 1 || !proto.Equal(serialInfos[0], parallelInfos[0]) {
		t.Fatalf("CALLTOKEN receipt mismatch\nserial=%v\nparallel=%v", serialInfos, parallelInfos)
	}
	for name, statedb := range map[string]*state.StateDB{"serial": serialState, "parallel": parallelState} {
		if got := statedb.GetTRC10Balance(contractAddr, tokenID); got != 93 {
			t.Fatalf("%s contract TRC10 balance = %d, want 93", name, got)
		}
		if got := statedb.GetTRC10Balance(recipient, tokenID); got != 17 {
			t.Fatalf("%s recipient TRC10 balance = %d, want 17", name, got)
		}
	}
	serialRoot, err := serialState.Commit()
	if err != nil {
		t.Fatal(err)
	}
	parallelRoot, err := parallelState.Commit()
	if err != nil {
		t.Fatal(err)
	}
	if serialRoot != parallelRoot {
		t.Fatalf("CALLTOKEN roots differ: serial=%x parallel=%x", serialRoot, parallelRoot)
	}
}

func TestProcessBlockPublishesVMFreezeResourceAccountKV(t *testing.T) {
	base := newTestState(t)
	dynProps := base.DynamicProperties()
	dynProps.SetAllowCreationOfContracts(true)
	dynProps.SetAllowAdaptiveEnergy(true)
	dynProps.SetAllowBlackHoleOptimization(true)
	dynProps.SetAllowTvmFreeze(true)
	dynProps.SetLatestBlockHeaderTimestamp(30_000)
	passVersion3_6_5(base, 27)

	owner := testProcessorAddr(1)
	contractAddr := testProcessorAddr(0x91)
	base.CreateAccount(owner, corepb.AccountType_Normal)
	base.AddBalance(owner, 100_000_000)
	base.CreateAccount(params.BlackholeAddress, corepb.AccountType_Normal)
	base.CreateAccount(contractAddr, corepb.AccountType_Contract)
	base.AddBalance(contractAddr, 2_000_000)
	base.SetContract(contractAddr, &contractpb.SmartContract{
		OriginAddress: owner.Bytes(), ContractAddress: contractAddr.Bytes(),
	})
	// Freeze 1 TRX of the contract's own balance for energy. The mutable
	// resource payload is split into exact AccountKV rows; publication must carry
	// those rows together with the balance and total-energy-weight post-images.
	code := []byte{byte(vm.PUSH20)}
	code = append(code, contractAddr[1:]...)
	code = append(code,
		byte(vm.PUSH3), 0x0f, 0x42, 0x40,
		byte(vm.PUSH1), 0x01,
		byte(vm.FREEZE), byte(vm.POP), byte(vm.STOP),
	)
	base.SetCode(contractAddr, code)
	if _, err := base.Commit(); err != nil {
		t.Fatal(err)
	}

	serialState, err := base.Copy()
	if err != nil {
		t.Fatal(err)
	}
	serialState.SetDynamicProperties(base.DynamicProperties().Copy())
	parallelState, err := base.Copy()
	if err != nil {
		t.Fatal(err)
	}
	parallelState.SetDynamicProperties(base.DynamicProperties().Copy())

	tx := makeTestTriggerTx(1, contractAddr, nil)
	tx.Proto().RawData.FeeLimit = 10_000_000
	tx.Proto().Ret = []*corepb.Transaction_Result{{ContractRet: corepb.Transaction_Result_SUCCESS}}
	block := types.NewBlockFromPB(&corepb.Block{
		BlockHeader: &corepb.BlockHeader{RawData: &corepb.BlockHeaderRaw{
			Number: int64(vmSenderChainPublishInterval), Timestamp: 33_000,
		}},
		Transactions: []*corepb.Transaction{tx.Proto()},
	})
	run := func(statedb *state.StateDB, options processBlockOptions) ([]*corepb.TransactionInfo, error) {
		infos, _, processErr := processBlockWithOptions(
			statedb, statedb.DynamicProperties(), block, ethrawdb.NewMemoryDatabase(), nil, 0,
			params.DefaultBlockNumForEnergyLimit, false, tcommon.Hash{}, nil, nil,
			nil, forks.NewVersionPassCache(), new(transactionInfoBatch), true, -1, nil,
			options,
		)
		return infos, processErr
	}
	serialInfos, err := run(serialState, processBlockOptions{})
	if err != nil {
		t.Fatalf("serial VM FREEZE: %v", err)
	}
	publishedBefore := parallelVMPublishedCounter.Snapshot().Count()
	dualCandidatesBefore := parallelVMDualOracleCandidatesCounter.Snapshot().Count()
	dualMatchesBefore := parallelVMDualOracleMatchesCounter.Snapshot().Count()
	dualInfoMismatchBefore := parallelVMDualOracleInfoMismatchCounter.Snapshot().Count()
	dualWriteMismatchBefore := parallelVMDualOracleWriteMismatchCounter.Snapshot().Count()
	dualBalanceMismatchBefore := parallelVMDualOracleBalanceMismatchCounter.Snapshot().Count()
	dualErrorsBefore := parallelVMDualOracleErrorsCounter.Snapshot().Count()
	sealCandidatesBefore := parallelVMWriteSealCandidatesCounter.Snapshot().Count()
	sealMatchesBefore := parallelVMWriteSealMatchesCounter.Snapshot().Count()
	sealMismatchesBefore := parallelVMWriteSealMismatchesCounter.Snapshot().Count()
	auditCandidatesBefore := parallelVMPublishAuditCandidatesCounter.Snapshot().Count()
	auditMatchesBefore := parallelVMPublishAuditMatchesCounter.Snapshot().Count()
	auditMismatchesBefore := parallelVMPublishAuditMismatchesCounter.Snapshot().Count()
	auditErrorsBefore := parallelVMPublishAuditErrorsCounter.Snapshot().Count()
	fallbacksBefore := parallelVMUnavailableFallbackCounter.Snapshot().Count() +
		parallelVMConflictFallbackCounter.Snapshot().Count() +
		parallelVMPreflightFallbackCounter.Snapshot().Count() +
		parallelVMPublicNetFallbackCounter.Snapshot().Count() +
		parallelVMBlockEnergyFallbackCounter.Snapshot().Count()
	parallelInfos, err := run(parallelState, processBlockOptions{parallelVM: true})
	if err != nil {
		t.Fatalf("parallel VM FREEZE: %v", err)
	}
	if published := parallelVMPublishedCounter.Snapshot().Count() - publishedBefore; published != 1 {
		t.Fatalf("FREEZE VM publications = %d, want 1", published)
	}
	if candidates := parallelVMDualOracleCandidatesCounter.Snapshot().Count() - dualCandidatesBefore; candidates != 1 {
		t.Fatalf("FREEZE dual-oracle candidates = %d, want 1", candidates)
	}
	if matches := parallelVMDualOracleMatchesCounter.Snapshot().Count() - dualMatchesBefore; matches != 1 {
		t.Fatalf("FREEZE dual-oracle matches = %d, want 1", matches)
	}
	if failures := parallelVMDualOracleInfoMismatchCounter.Snapshot().Count() - dualInfoMismatchBefore +
		parallelVMDualOracleWriteMismatchCounter.Snapshot().Count() - dualWriteMismatchBefore +
		parallelVMDualOracleBalanceMismatchCounter.Snapshot().Count() - dualBalanceMismatchBefore +
		parallelVMDualOracleErrorsCounter.Snapshot().Count() - dualErrorsBefore; failures != 0 {
		t.Fatalf("FREEZE dual-oracle mismatches/errors = %d, want 0", failures)
	}
	if candidates := parallelVMWriteSealCandidatesCounter.Snapshot().Count() - sealCandidatesBefore; candidates != 1 {
		t.Fatalf("FREEZE write-seal candidates = %d, want 1", candidates)
	}
	if matches := parallelVMWriteSealMatchesCounter.Snapshot().Count() - sealMatchesBefore; matches != 1 {
		t.Fatalf("FREEZE write-seal matches = %d, want 1", matches)
	}
	if mismatches := parallelVMWriteSealMismatchesCounter.Snapshot().Count() - sealMismatchesBefore; mismatches != 0 {
		t.Fatalf("FREEZE write-seal mismatches = %d, want 0", mismatches)
	}
	if candidates := parallelVMPublishAuditCandidatesCounter.Snapshot().Count() - auditCandidatesBefore; candidates != 1 {
		t.Fatalf("FREEZE publish-audit candidates = %d, want 1", candidates)
	}
	if matches := parallelVMPublishAuditMatchesCounter.Snapshot().Count() - auditMatchesBefore; matches != 1 {
		t.Fatalf("FREEZE publish-audit matches = %d, want 1", matches)
	}
	if failures := parallelVMPublishAuditMismatchesCounter.Snapshot().Count() - auditMismatchesBefore +
		parallelVMPublishAuditErrorsCounter.Snapshot().Count() - auditErrorsBefore; failures != 0 {
		t.Fatalf("FREEZE publish-audit mismatches/errors = %d, want 0", failures)
	}
	fallbacksAfter := parallelVMUnavailableFallbackCounter.Snapshot().Count() +
		parallelVMConflictFallbackCounter.Snapshot().Count() +
		parallelVMPreflightFallbackCounter.Snapshot().Count() +
		parallelVMPublicNetFallbackCounter.Snapshot().Count() +
		parallelVMBlockEnergyFallbackCounter.Snapshot().Count()
	if fallbacks := fallbacksAfter - fallbacksBefore; fallbacks != 0 {
		t.Fatalf("FREEZE VM fallbacks = %d, want 0", fallbacks)
	}
	if len(serialInfos) != 1 || len(parallelInfos) != 1 || !proto.Equal(serialInfos[0], parallelInfos[0]) {
		t.Fatalf("FREEZE receipt mismatch\nserial=%v\nparallel=%v", serialInfos, parallelInfos)
	}
	for name, statedb := range map[string]*state.StateDB{"serial": serialState, "parallel": parallelState} {
		if got := statedb.GetBalance(contractAddr); got != 1_000_000 {
			t.Fatalf("%s contract balance = %d, want 1000000", name, got)
		}
		frozen, freezeErr := statedb.GetAccountFrozenEnergyV1(contractAddr)
		if freezeErr != nil || frozen != 1_000_000 {
			t.Fatalf("%s frozen energy = %d, err=%v, want 1000000", name, frozen, freezeErr)
		}
		if got := statedb.DynamicProperties().TotalEnergyWeight(); got != 1 {
			t.Fatalf("%s total energy weight = %d, want 1", name, got)
		}
	}
	serialRoot, err := serialState.Commit()
	if err != nil {
		t.Fatal(err)
	}
	parallelRoot, err := parallelState.Commit()
	if err != nil {
		t.Fatal(err)
	}
	if serialRoot != parallelRoot {
		t.Fatalf("FREEZE roots differ: serial=%x parallel=%x", serialRoot, parallelRoot)
	}
}

func TestProcessBlockVMFreezeSharedWeightSerializesConflict(t *testing.T) {
	base := newTestState(t)
	dynProps := base.DynamicProperties()
	dynProps.SetAllowCreationOfContracts(true)
	dynProps.SetAllowAdaptiveEnergy(true)
	dynProps.SetAllowBlackHoleOptimization(true)
	dynProps.SetAllowTvmFreeze(true)
	dynProps.SetLatestBlockHeaderTimestamp(30_000)
	passVersion3_6_5(base, 27)
	base.CreateAccount(params.BlackholeAddress, corepb.AccountType_Normal)

	owners := []byte{1, 2}
	contracts := []tcommon.Address{testProcessorAddr(0x92), testProcessorAddr(0x93)}
	transactions := make([]*corepb.Transaction, len(contracts))
	for index, contractAddr := range contracts {
		owner := testProcessorAddr(owners[index])
		base.CreateAccount(owner, corepb.AccountType_Normal)
		base.AddBalance(owner, 100_000_000)
		base.CreateAccount(contractAddr, corepb.AccountType_Contract)
		base.AddBalance(contractAddr, 2_000_000)
		base.SetContract(contractAddr, &contractpb.SmartContract{
			OriginAddress: owner.Bytes(), ContractAddress: contractAddr.Bytes(),
		})
		code := []byte{byte(vm.PUSH20)}
		code = append(code, contractAddr[1:]...)
		code = append(code,
			byte(vm.PUSH3), 0x0f, 0x42, 0x40,
			byte(vm.PUSH1), 0x01,
			byte(vm.FREEZE), byte(vm.POP), byte(vm.STOP),
		)
		base.SetCode(contractAddr, code)
		tx := makeTestTriggerTx(owners[index], contractAddr, nil)
		tx.Proto().RawData.FeeLimit = 10_000_000
		tx.Proto().Ret = []*corepb.Transaction_Result{{ContractRet: corepb.Transaction_Result_SUCCESS}}
		transactions[index] = tx.Proto()
	}
	if _, err := base.Commit(); err != nil {
		t.Fatal(err)
	}

	serialState, err := base.Copy()
	if err != nil {
		t.Fatal(err)
	}
	serialState.SetDynamicProperties(base.DynamicProperties().Copy())
	parallelState, err := base.Copy()
	if err != nil {
		t.Fatal(err)
	}
	parallelState.SetDynamicProperties(base.DynamicProperties().Copy())
	block := types.NewBlockFromPB(&corepb.Block{
		BlockHeader: &corepb.BlockHeader{RawData: &corepb.BlockHeaderRaw{
			Number: int64(vmSenderChainPublishInterval), Timestamp: 33_000,
		}},
		Transactions: transactions,
	})
	run := func(statedb *state.StateDB, options processBlockOptions) ([]*corepb.TransactionInfo, error) {
		infos, _, processErr := processBlockWithOptions(
			statedb, statedb.DynamicProperties(), block, ethrawdb.NewMemoryDatabase(), nil, 0,
			params.DefaultBlockNumForEnergyLimit, false, tcommon.Hash{}, nil, nil,
			nil, forks.NewVersionPassCache(), new(transactionInfoBatch), true, -1, nil,
			options,
		)
		return infos, processErr
	}
	serialInfos, err := run(serialState, processBlockOptions{})
	if err != nil {
		t.Fatalf("serial shared-weight FREEZE: %v", err)
	}
	publishedBefore := parallelVMPublishedCounter.Snapshot().Count()
	conflictBefore := parallelVMConflictFallbackCounter.Snapshot().Count()
	dualCandidatesBefore := parallelVMDualOracleCandidatesCounter.Snapshot().Count()
	dualMatchesBefore := parallelVMDualOracleMatchesCounter.Snapshot().Count()
	dualErrorsBefore := parallelVMDualOracleErrorsCounter.Snapshot().Count()
	parallelInfos, err := run(parallelState, processBlockOptions{parallelVM: true})
	if err != nil {
		t.Fatalf("parallel shared-weight FREEZE: %v", err)
	}
	if published := parallelVMPublishedCounter.Snapshot().Count() - publishedBefore; published != 1 {
		t.Fatalf("shared-weight FREEZE publications = %d, want 1", published)
	}
	if conflicts := parallelVMConflictFallbackCounter.Snapshot().Count() - conflictBefore; conflicts != 1 {
		t.Fatalf("shared-weight FREEZE conflict fallbacks = %d, want 1", conflicts)
	}
	if candidates := parallelVMDualOracleCandidatesCounter.Snapshot().Count() - dualCandidatesBefore; candidates != 1 {
		t.Fatalf("shared-weight FREEZE dual-oracle candidates = %d, want 1", candidates)
	}
	if matches := parallelVMDualOracleMatchesCounter.Snapshot().Count() - dualMatchesBefore; matches != 1 {
		t.Fatalf("shared-weight FREEZE dual-oracle matches = %d, want 1", matches)
	}
	if failures := parallelVMDualOracleErrorsCounter.Snapshot().Count() - dualErrorsBefore; failures != 0 {
		t.Fatalf("shared-weight FREEZE dual-oracle errors = %d, want 0", failures)
	}
	if len(serialInfos) != len(parallelInfos) {
		t.Fatalf("shared-weight FREEZE info count serial=%d parallel=%d", len(serialInfos), len(parallelInfos))
	}
	for index := range serialInfos {
		if !proto.Equal(serialInfos[index], parallelInfos[index]) {
			t.Fatalf("shared-weight FREEZE tx %d receipt mismatch\nserial=%v\nparallel=%v", index, serialInfos[index], parallelInfos[index])
		}
	}
	for name, statedb := range map[string]*state.StateDB{"serial": serialState, "parallel": parallelState} {
		if got := statedb.DynamicProperties().TotalEnergyWeight(); got != 2 {
			t.Fatalf("%s total energy weight = %d, want 2", name, got)
		}
		for _, contractAddr := range contracts {
			if got := statedb.GetBalance(contractAddr); got != 1_000_000 {
				t.Fatalf("%s contract %s balance = %d, want 1000000", name, contractAddr.Hex(), got)
			}
			frozen, freezeErr := statedb.GetAccountFrozenEnergyV1(contractAddr)
			if freezeErr != nil || frozen != 1_000_000 {
				t.Fatalf("%s contract %s frozen energy = %d, err=%v, want 1000000", name, contractAddr.Hex(), frozen, freezeErr)
			}
		}
	}
	serialRoot, err := serialState.Commit()
	if err != nil {
		t.Fatal(err)
	}
	parallelRoot, err := parallelState.Commit()
	if err != nil {
		t.Fatal(err)
	}
	if serialRoot != parallelRoot {
		t.Fatalf("shared-weight FREEZE roots differ: serial=%x parallel=%x", serialRoot, parallelRoot)
	}
}

func TestProcessBlockVMWithdrawRewardAccountKVFallsBackBeforeOracle(t *testing.T) {
	base := newTestState(t)
	dynProps := base.DynamicProperties()
	dynProps.SetAllowCreationOfContracts(true)
	dynProps.SetAllowAdaptiveEnergy(true)
	dynProps.SetAllowBlackHoleOptimization(true)
	dynProps.SetAllowTvmVote(true)
	dynProps.SetCurrentCycleNumber(10)
	dynProps.SetNewRewardAlgorithmEffectiveCycle(0)
	dynProps.SetLatestBlockHeaderTimestamp(30_000)
	passVersion3_6_5(base, 27)

	owner := testProcessorAddr(1)
	contractAddr := testProcessorAddr(0x8b)
	witness := testProcessorAddr(0x8c)
	base.CreateAccount(owner, corepb.AccountType_Normal)
	base.AddBalance(owner, 100_000_000)
	base.CreateAccount(params.BlackholeAddress, corepb.AccountType_Normal)
	base.CreateAccount(contractAddr, corepb.AccountType_Contract)
	base.AddBalance(contractAddr, 1_000)
	base.SetAllowance(contractAddr, 50)
	base.SetVotes(contractAddr, []*corepb.Vote{{VoteAddress: witness.Bytes(), VoteCount: 100}})
	base.SetContract(contractAddr, &contractpb.SmartContract{
		OriginAddress: owner.Bytes(), ContractAddress: contractAddr.Bytes(),
	})
	// WITHDRAWREWARD; POP; STOP. This writes exact account scalar fields and
	// reward-cursor/account-vote AccountKV rows from inside TVM execution.
	base.SetCode(contractAddr, []byte{0xd9, 0x50, 0x00})
	if err := base.WriteBeginCycle(contractAddr.Bytes(), 1); err != nil {
		t.Fatal(err)
	}
	if err := base.WriteWitnessVI(0, witness.Bytes(), new(big.Int)); err != nil {
		t.Fatal(err)
	}
	if err := base.WriteWitnessVI(9, witness.Bytes(), big.NewInt(3_000_000_000_000_000_000)); err != nil {
		t.Fatal(err)
	}
	if _, err := base.Commit(); err != nil {
		t.Fatal(err)
	}

	serialState, err := base.Copy()
	if err != nil {
		t.Fatal(err)
	}
	serialState.SetDynamicProperties(base.DynamicProperties().Copy())
	parallelState, err := base.Copy()
	if err != nil {
		t.Fatal(err)
	}
	parallelState.SetDynamicProperties(base.DynamicProperties().Copy())

	tx := makeTestTriggerTx(1, contractAddr, nil)
	tx.Proto().RawData.FeeLimit = 10_000_000
	tx.Proto().Ret = []*corepb.Transaction_Result{{ContractRet: corepb.Transaction_Result_SUCCESS}}
	block := types.NewBlockFromPB(&corepb.Block{
		BlockHeader: &corepb.BlockHeader{RawData: &corepb.BlockHeaderRaw{
			Number: int64(vmSenderChainPublishInterval), Timestamp: 33_000,
		}},
		Transactions: []*corepb.Transaction{tx.Proto()},
	})
	run := func(statedb *state.StateDB, options processBlockOptions) ([]*corepb.TransactionInfo, error) {
		infos, _, processErr := processBlockWithOptions(
			statedb, statedb.DynamicProperties(), block, ethrawdb.NewMemoryDatabase(), nil, 0,
			params.DefaultBlockNumForEnergyLimit, false, tcommon.Hash{}, nil, nil,
			nil, forks.NewVersionPassCache(), new(transactionInfoBatch), true, -1, nil,
			options,
		)
		return infos, processErr
	}
	serialInfos, err := run(serialState, processBlockOptions{})
	if err != nil {
		t.Fatalf("serial VM reward withdrawal: %v", err)
	}
	publishedBefore := parallelVMPublishedCounter.Snapshot().Count()
	unavailableBefore := parallelVMUnavailableFallbackCounter.Snapshot().Count()
	conflictBefore := parallelVMConflictFallbackCounter.Snapshot().Count()
	preflightBefore := parallelVMPreflightFallbackCounter.Snapshot().Count()
	publicNetBefore := parallelVMPublicNetFallbackCounter.Snapshot().Count()
	blockEnergyBefore := parallelVMBlockEnergyFallbackCounter.Snapshot().Count()
	dualCandidatesBefore := parallelVMDualOracleCandidatesCounter.Snapshot().Count()
	dualMatchesBefore := parallelVMDualOracleMatchesCounter.Snapshot().Count()
	dualErrorsBefore := parallelVMDualOracleErrorsCounter.Snapshot().Count()
	parallelInfos, err := run(parallelState, processBlockOptions{parallelVM: true})
	if err != nil {
		t.Fatalf("parallel VM reward withdrawal: %v", err)
	}
	if published := parallelVMPublishedCounter.Snapshot().Count() - publishedBefore; published != 0 {
		t.Fatalf("VM reward withdrawal publications = %d, want 0; fallback unavailable/conflict/preflight/public-net/energy=%d/%d/%d/%d/%d",
			published,
			parallelVMUnavailableFallbackCounter.Snapshot().Count()-unavailableBefore,
			parallelVMConflictFallbackCounter.Snapshot().Count()-conflictBefore,
			parallelVMPreflightFallbackCounter.Snapshot().Count()-preflightBefore,
			parallelVMPublicNetFallbackCounter.Snapshot().Count()-publicNetBefore,
			parallelVMBlockEnergyFallbackCounter.Snapshot().Count()-blockEnergyBefore)
	}
	if fallbacks := parallelVMConflictFallbackCounter.Snapshot().Count() - conflictBefore; fallbacks != 1 {
		t.Fatalf("VM reward withdrawal version-gate fallbacks = %d, want 1", fallbacks)
	}
	if otherFallbacks := parallelVMUnavailableFallbackCounter.Snapshot().Count() - unavailableBefore +
		parallelVMPreflightFallbackCounter.Snapshot().Count() - preflightBefore +
		parallelVMPublicNetFallbackCounter.Snapshot().Count() - publicNetBefore +
		parallelVMBlockEnergyFallbackCounter.Snapshot().Count() - blockEnergyBefore; otherFallbacks != 0 {
		t.Fatalf("VM reward withdrawal other fallbacks = %d, want 0", otherFallbacks)
	}
	if candidates := parallelVMDualOracleCandidatesCounter.Snapshot().Count() - dualCandidatesBefore; candidates != 0 {
		t.Fatalf("VM reward withdrawal dual-oracle candidates = %d, want 0 before admission", candidates)
	}
	if matches := parallelVMDualOracleMatchesCounter.Snapshot().Count() - dualMatchesBefore; matches != 0 {
		t.Fatalf("VM reward withdrawal dual-oracle matches = %d, want 0 before admission", matches)
	}
	if failures := parallelVMDualOracleErrorsCounter.Snapshot().Count() - dualErrorsBefore; failures != 0 {
		t.Fatalf("VM reward withdrawal dual-oracle errors = %d, want 0", failures)
	}
	if len(serialInfos) != 1 || len(parallelInfos) != 1 || !proto.Equal(serialInfos[0], parallelInfos[0]) {
		t.Fatalf("VM reward withdrawal receipt mismatch\nserial=%v\nparallel=%v", serialInfos, parallelInfos)
	}
	for name, statedb := range map[string]*state.StateDB{"serial": serialState, "parallel": parallelState} {
		if got := statedb.GetBalance(contractAddr); got != 1_350 {
			t.Fatalf("%s contract balance = %d, want 1350", name, got)
		}
		if got := statedb.GetAllowance(contractAddr); got != 0 {
			t.Fatalf("%s contract allowance = %d, want 0", name, got)
		}
		if got := statedb.GetLatestWithdrawTime(contractAddr); got != block.Timestamp() {
			t.Fatalf("%s latest withdraw time = %d, want %d", name, got, block.Timestamp())
		}
		if got := statedb.ReadBeginCycle(contractAddr.Bytes()); got != 10 {
			t.Fatalf("%s begin cycle = %d, want 10", name, got)
		}
		if got := statedb.ReadEndCycle(contractAddr.Bytes()); got != 11 {
			t.Fatalf("%s end cycle = %d, want 11", name, got)
		}
		if vote := statedb.ReadCycleAccountVote(10, contractAddr.Bytes()); vote == nil {
			t.Fatalf("%s cycle account-vote snapshot is absent", name)
		}
	}
	serialRoot, err := serialState.Commit()
	if err != nil {
		t.Fatal(err)
	}
	parallelRoot, err := parallelState.Commit()
	if err != nil {
		t.Fatal(err)
	}
	if serialRoot != parallelRoot {
		t.Fatalf("VM reward withdrawal roots differ: serial=%x parallel=%x", serialRoot, parallelRoot)
	}
}

func TestProcessBlockVMSelfDestructFallsBackBeforeOracle(t *testing.T) {
	base := newTestState(t)
	dynProps := base.DynamicProperties()
	dynProps.SetAllowCreationOfContracts(true)
	dynProps.SetAllowAdaptiveEnergy(true)
	dynProps.SetAllowBlackHoleOptimization(true)
	dynProps.SetLatestBlockHeaderTimestamp(30_000)
	passVersion3_6_5(base, 27)

	owner := testProcessorAddr(1)
	contractAddr := testProcessorAddr(0x8d)
	beneficiary := testProcessorAddr(0x8e)
	// Before allow_tvm_energy_adjustment, java-tron compares only the first
	// 20 bytes of the 21-byte TRON address when deciding whether SELFDESTRUCT
	// targets the contract itself. testProcessorAddr normally differs only in
	// byte 20, so make the beneficiary distinct inside that legacy comparison
	// range; otherwise this fixture exercises the blackhole self-target path.
	beneficiary[19] = 0x8e
	base.CreateAccount(owner, corepb.AccountType_Normal)
	base.AddBalance(owner, 100_000_000)
	base.CreateAccount(params.BlackholeAddress, corepb.AccountType_Normal)
	base.CreateAccount(beneficiary, corepb.AccountType_Normal)
	base.AddBalance(beneficiary, 10)
	base.CreateAccount(contractAddr, corepb.AccountType_Contract)
	base.AddBalance(contractAddr, 7_777)
	base.SetContract(contractAddr, &contractpb.SmartContract{
		OriginAddress: owner.Bytes(), ContractAddress: contractAddr.Bytes(),
	})
	code := append([]byte{0x73}, beneficiary[1:]...)
	base.SetCode(contractAddr, append(code, 0xff))
	if _, err := base.Commit(); err != nil {
		t.Fatal(err)
	}

	serialState, err := base.Copy()
	if err != nil {
		t.Fatal(err)
	}
	serialState.SetDynamicProperties(base.DynamicProperties().Copy())
	parallelState, err := base.Copy()
	if err != nil {
		t.Fatal(err)
	}
	parallelState.SetDynamicProperties(base.DynamicProperties().Copy())

	tx := makeTestTriggerTx(1, contractAddr, nil)
	tx.Proto().RawData.FeeLimit = 10_000_000
	tx.Proto().Ret = []*corepb.Transaction_Result{{ContractRet: corepb.Transaction_Result_SUCCESS}}
	block := types.NewBlockFromPB(&corepb.Block{
		BlockHeader: &corepb.BlockHeader{RawData: &corepb.BlockHeaderRaw{
			Number: int64(vmSenderChainPublishInterval), Timestamp: 33_000,
		}},
		Transactions: []*corepb.Transaction{tx.Proto()},
	})
	run := func(statedb *state.StateDB, options processBlockOptions) ([]*corepb.TransactionInfo, error) {
		infos, _, processErr := processBlockWithOptions(
			statedb, statedb.DynamicProperties(), block, ethrawdb.NewMemoryDatabase(), nil, 0,
			params.DefaultBlockNumForEnergyLimit, false, tcommon.Hash{}, nil, nil,
			nil, forks.NewVersionPassCache(), new(transactionInfoBatch), true, -1, nil,
			options,
		)
		return infos, processErr
	}
	serialInfos, err := run(serialState, processBlockOptions{})
	if err != nil {
		t.Fatalf("serial VM selfdestruct: %v", err)
	}
	publishedBefore := parallelVMPublishedCounter.Snapshot().Count()
	unavailableBefore := parallelVMUnavailableFallbackCounter.Snapshot().Count()
	dualCandidatesBefore := parallelVMDualOracleCandidatesCounter.Snapshot().Count()
	dualMatchesBefore := parallelVMDualOracleMatchesCounter.Snapshot().Count()
	dualErrorsBefore := parallelVMDualOracleErrorsCounter.Snapshot().Count()
	parallelInfos, err := run(parallelState, processBlockOptions{parallelVM: true})
	if err != nil {
		t.Fatalf("parallel VM selfdestruct fallback: %v", err)
	}
	if published := parallelVMPublishedCounter.Snapshot().Count() - publishedBefore; published != 0 {
		t.Fatalf("VM selfdestruct publications = %d, want 0", published)
	}
	if fallbacks := parallelVMUnavailableFallbackCounter.Snapshot().Count() - unavailableBefore; fallbacks != 1 {
		t.Fatalf("VM selfdestruct unavailable fallbacks = %d, want 1", fallbacks)
	}
	if candidates := parallelVMDualOracleCandidatesCounter.Snapshot().Count() - dualCandidatesBefore; candidates != 0 {
		t.Fatalf("VM selfdestruct dual-oracle candidates = %d, want 0 before admission", candidates)
	}
	if matches := parallelVMDualOracleMatchesCounter.Snapshot().Count() - dualMatchesBefore; matches != 0 {
		t.Fatalf("VM selfdestruct dual-oracle matches = %d, want 0 before admission", matches)
	}
	if failures := parallelVMDualOracleErrorsCounter.Snapshot().Count() - dualErrorsBefore; failures != 0 {
		t.Fatalf("VM selfdestruct dual-oracle errors = %d, want 0", failures)
	}
	if len(serialInfos) != 1 || len(parallelInfos) != 1 || !proto.Equal(serialInfos[0], parallelInfos[0]) {
		t.Fatalf("VM selfdestruct receipt mismatch\nserial=%v\nparallel=%v", serialInfos, parallelInfos)
	}
	for name, statedb := range map[string]*state.StateDB{"serial": serialState, "parallel": parallelState} {
		if statedb.AccountExists(contractAddr) {
			t.Fatalf("%s selfdestructed contract still exists", name)
		}
		if got := statedb.GetBalance(beneficiary); got != 7_787 {
			t.Fatalf("%s beneficiary balance = %d, want 7787", name, got)
		}
		if got := statedb.GetBalance(params.BlackholeAddress); got != 0 {
			t.Fatalf("%s blackhole balance = %d, want 0 for distinct beneficiary", name, got)
		}
	}
	serialRoot, err := serialState.Commit()
	if err != nil {
		t.Fatal(err)
	}
	parallelRoot, err := parallelState.Commit()
	if err != nil {
		t.Fatal(err)
	}
	if serialRoot != parallelRoot {
		t.Fatalf("VM selfdestruct roots differ: serial=%x parallel=%x", serialRoot, parallelRoot)
	}
}

func TestProcessBlockVMPublisherFallsBackOnStorageConflict(t *testing.T) {
	base := newTestState(t)
	dynProps := base.DynamicProperties()
	dynProps.SetAllowCreationOfContracts(true)
	dynProps.SetAllowAdaptiveEnergy(true)
	dynProps.SetAllowBlackHoleOptimization(true)
	dynProps.SetLatestBlockHeaderTimestamp(30_000)
	passVersion3_6_5(base, 27)

	owner1 := testProcessorAddr(1)
	owner2 := testProcessorAddr(3)
	contractAddr := testProcessorAddr(0x83)
	for _, owner := range []tcommon.Address{owner1, owner2} {
		base.CreateAccount(owner, corepb.AccountType_Normal)
		base.AddBalance(owner, 100_000_000)
	}
	base.CreateAccount(params.BlackholeAddress, corepb.AccountType_Normal)
	base.CreateAccount(contractAddr, corepb.AccountType_Contract)
	base.SetContract(contractAddr, &contractpb.SmartContract{
		OriginAddress: owner1.Bytes(), ContractAddress: contractAddr.Bytes(),
	})
	// Both callers write calldata to slot zero. The second speculative worker
	// sees an empty block-start slot, whereas serial execution sees the first
	// transaction's value and charges RESET rather than SET energy. Exact
	// storage read-version validation must reject that stale result.
	base.SetCode(contractAddr, []byte{0x60, 0x00, 0x35, 0x60, 0x00, 0x55, 0x00})
	if _, err := base.Commit(); err != nil {
		t.Fatal(err)
	}

	serialState, err := base.Copy()
	if err != nil {
		t.Fatal(err)
	}
	serialState.SetDynamicProperties(base.DynamicProperties().Copy())
	parallelState, err := base.Copy()
	if err != nil {
		t.Fatal(err)
	}
	parallelState.SetDynamicProperties(base.DynamicProperties().Copy())

	storageInput := func(value byte) []byte {
		input := make([]byte, tcommon.HashLength)
		input[len(input)-1] = value
		return input
	}
	transactions := []*types.Transaction{
		makeTestTriggerTx(1, contractAddr, storageInput(1)),
		makeTestTriggerTx(3, contractAddr, storageInput(2)),
	}
	transactionProtos := make([]*corepb.Transaction, len(transactions))
	for txIndex, tx := range transactions {
		tx.Proto().RawData.FeeLimit = 10_000_000
		tx.Proto().Ret = []*corepb.Transaction_Result{{ContractRet: corepb.Transaction_Result_SUCCESS}}
		transactionProtos[txIndex] = tx.Proto()
	}
	block := types.NewBlockFromPB(&corepb.Block{
		BlockHeader: &corepb.BlockHeader{RawData: &corepb.BlockHeaderRaw{
			Number: int64(vmSenderChainPublishInterval), Timestamp: 33_000,
		}},
		Transactions: transactionProtos,
	})
	run := func(statedb *state.StateDB, options processBlockOptions) ([]*corepb.TransactionInfo, error) {
		infos, _, processErr := processBlockWithOptions(
			statedb, statedb.DynamicProperties(), block, ethrawdb.NewMemoryDatabase(), nil, 0,
			params.DefaultBlockNumForEnergyLimit, false, tcommon.Hash{}, nil, nil,
			nil, forks.NewVersionPassCache(), new(transactionInfoBatch), true, -1, nil,
			options,
		)
		return infos, processErr
	}
	var serialTiming processBlockTiming
	serialInfos, err := run(serialState, processBlockOptions{timing: &serialTiming})
	if err != nil {
		t.Fatalf("serial VM process: %v", err)
	}
	publishedBefore := parallelVMPublishedCounter.Snapshot().Count()
	conflictsBefore := parallelVMConflictFallbackCounter.Snapshot().Count()
	serialVerifyCandidatesBefore := parallelVMSerialVerifyCandidatesCounter.Snapshot().Count()
	serialVerifyMatchesBefore := parallelVMSerialVerifyMatchesCounter.Snapshot().Count()
	var parallelTiming processBlockTiming
	parallelInfos, err := run(parallelState, processBlockOptions{parallelVM: true, timing: &parallelTiming})
	if err != nil {
		t.Fatalf("parallel VM process: %v", err)
	}
	if published := parallelVMPublishedCounter.Snapshot().Count() - publishedBefore; published != 1 {
		t.Fatalf("parallel VM publications = %d, want first transaction only", published)
	}
	if conflicts := parallelVMConflictFallbackCounter.Snapshot().Count() - conflictsBefore; conflicts != 1 {
		t.Fatalf("parallel VM storage-conflict fallbacks = %d, want 1", conflicts)
	}
	if serialTiming.VMTransactions != 2 || serialTiming.NativeTransactions != 0 ||
		parallelTiming.VMTransactions != 2 || parallelTiming.NativeTransactions != 0 {
		t.Fatalf("fallback transaction telemetry = serial %d/%d parallel %d/%d, want 2 VM/0 native each",
			serialTiming.VMTransactions, serialTiming.NativeTransactions,
			parallelTiming.VMTransactions, parallelTiming.NativeTransactions)
	}
	if serialTiming.VMExecution <= 0 || parallelTiming.VMExecution <= 0 {
		t.Fatalf("fallback VM execution telemetry = serial %s, parallel %s; want both positive", serialTiming.VMExecution, parallelTiming.VMExecution)
	}
	if serialTiming.VMRawEnergyUsage <= 0 || parallelTiming.VMRawEnergyUsage != serialTiming.VMRawEnergyUsage {
		t.Fatalf("fallback VM raw energy telemetry = serial %d, parallel %d; want equal positive canonical totals", serialTiming.VMRawEnergyUsage, parallelTiming.VMRawEnergyUsage)
	}
	if candidates := parallelVMSerialVerifyCandidatesCounter.Snapshot().Count() - serialVerifyCandidatesBefore; candidates != 1 {
		t.Fatalf("parallel VM boundary serial verification candidates = %d, want first transaction only", candidates)
	}
	if matches := parallelVMSerialVerifyMatchesCounter.Snapshot().Count() - serialVerifyMatchesBefore; matches != 1 {
		t.Fatalf("parallel VM boundary serial verification matches = %d, want 1", matches)
	}
	for txIndex := range serialInfos {
		if !proto.Equal(serialInfos[txIndex], parallelInfos[txIndex]) {
			t.Fatalf("tx %d info mismatch\nserial=%v\nparallel=%v", txIndex, serialInfos[txIndex], parallelInfos[txIndex])
		}
	}
	serialValue, serialExists := serialState.GetStateWithExist(contractAddr, tcommon.Hash{})
	parallelValue, parallelExists := parallelState.GetStateWithExist(contractAddr, tcommon.Hash{})
	if !serialExists || !parallelExists || serialValue != parallelValue || serialValue[31] != 2 {
		t.Fatalf("storage serial=%x/%t parallel=%x/%t, want ...02",
			serialValue, serialExists, parallelValue, parallelExists)
	}
	for _, property := range []struct {
		name     string
		serial   int64
		parallel int64
	}{
		{name: "public_net_usage", serial: serialState.DynamicProperties().PublicNetUsage(), parallel: parallelState.DynamicProperties().PublicNetUsage()},
		{name: "block_energy_usage", serial: serialState.DynamicProperties().BlockEnergyUsage(), parallel: parallelState.DynamicProperties().BlockEnergyUsage()},
	} {
		if property.serial != property.parallel {
			t.Fatalf("%s serial=%d parallel=%d", property.name, property.serial, property.parallel)
		}
	}
	serialRoot, err := serialState.Commit()
	if err != nil {
		t.Fatal(err)
	}
	parallelRoot, err := parallelState.Commit()
	if err != nil {
		t.Fatal(err)
	}
	if serialRoot != parallelRoot {
		t.Fatalf("storage-conflict roots differ: serial=%x parallel=%x", serialRoot, parallelRoot)
	}
}

func TestProcessBlockVMPublisherRetainsCachedCodeAfterHotPrune(t *testing.T) {
	diskdb := ethrawdb.NewMemoryDatabase()
	base, err := state.New(tcommon.Hash(ethtypes.EmptyRootHash), state.NewDatabase(diskdb))
	if err != nil {
		t.Fatal(err)
	}
	dynProps := base.DynamicProperties()
	dynProps.SetAllowCreationOfContracts(true)
	dynProps.SetAllowAdaptiveEnergy(true)
	dynProps.SetAllowDynamicEnergy(true)
	dynProps.SetCurrentCycleNumber(10)
	dynProps.SetDynamicEnergyIncreaseFactor(2_000)
	dynProps.SetDynamicEnergyMaxFactor(10_000)
	dynProps.SetAllowBlackHoleOptimization(true)
	dynProps.SetLatestBlockHeaderTimestamp(30_000)
	passVersion3_6_5(base, 27)

	owner := testProcessorAddr(1)
	contractAddr := testProcessorAddr(0x82)
	base.CreateAccount(owner, corepb.AccountType_Normal)
	base.AddBalance(owner, 100_000_000)
	base.CreateAccount(params.BlackholeAddress, corepb.AccountType_Normal)
	base.CreateAccount(contractAddr, corepb.AccountType_Contract)
	base.SetContract(contractAddr, &contractpb.SmartContract{
		OriginAddress: owner.Bytes(), ContractAddress: contractAddr.Bytes(),
	})
	code := []byte{0x60, 0x01, 0x60, 0x02, 0x01, 0x50, 0x00}
	base.SetCode(contractAddr, code)
	contractState := types.NewContractState(10)
	contractState.SetEnergyFactor(5_000)
	if err := base.WriteContractState(contractAddr, contractState); err != nil {
		t.Fatal(err)
	}
	if _, err := base.Commit(); err != nil {
		t.Fatal(err)
	}

	serialState, err := base.Copy()
	if err != nil {
		t.Fatal(err)
	}
	serialState.SetDynamicProperties(base.DynamicProperties().Copy())
	parallelState, err := base.Copy()
	if err != nil {
		t.Fatal(err)
	}
	parallelState.SetDynamicProperties(base.DynamicProperties().Copy())

	// Keep code available only through the canonical StateDB caches. This is
	// the production failure shape that previously let the block-start VM
	// publisher execute an existing contract as empty code.
	if err := tronrawdb.DeleteStateCode(diskdb, tcommon.Keccak256(code)); err != nil {
		t.Fatal(err)
	}
	if got := serialState.GetCode(contractAddr); !bytes.Equal(got, code) {
		t.Fatalf("serial cached code = %x, want %x", got, code)
	}
	if got := parallelState.GetCode(contractAddr); !bytes.Equal(got, code) {
		t.Fatalf("parallel cached code = %x, want %x", got, code)
	}

	tx := makeTestTriggerTx(1, contractAddr, []byte{0x01})
	tx.Proto().RawData.FeeLimit = 10_000_000
	tx.Proto().Ret = []*corepb.Transaction_Result{{ContractRet: corepb.Transaction_Result_SUCCESS}}
	block := types.NewBlockFromPB(&corepb.Block{
		BlockHeader: &corepb.BlockHeader{RawData: &corepb.BlockHeaderRaw{
			Number: int64(vmSenderChainPublishInterval), Timestamp: 33_000,
		}},
		Transactions: []*corepb.Transaction{tx.Proto()},
	})
	run := func(statedb *state.StateDB, options processBlockOptions) ([]*corepb.TransactionInfo, error) {
		infos, _, processErr := processBlockWithOptions(
			statedb, statedb.DynamicProperties(), block, ethrawdb.NewMemoryDatabase(), nil, 0,
			params.DefaultBlockNumForEnergyLimit, false, tcommon.Hash{}, nil, nil,
			nil, forks.NewVersionPassCache(), new(transactionInfoBatch), true, -1, nil,
			options,
		)
		return infos, processErr
	}
	serialInfos, err := run(serialState, processBlockOptions{})
	if err != nil {
		t.Fatalf("serial VM process: %v", err)
	}
	publishedBefore := parallelVMPublishedCounter.Snapshot().Count()
	parallelInfos, err := run(parallelState, processBlockOptions{parallelVM: true})
	if err != nil {
		t.Fatalf("parallel VM process: %v", err)
	}
	if published := parallelVMPublishedCounter.Snapshot().Count() - publishedBefore; published != 1 {
		t.Fatalf("parallel VM publications = %d, want 1", published)
	}
	if serialInfos[0].GetReceipt().GetEnergyUsageTotal() == 0 {
		t.Fatal("serial contract execution consumed no energy")
	}
	if serialInfos[0].GetReceipt().GetEnergyPenaltyTotal() == 0 {
		t.Fatal("serial contract execution omitted its dynamic-energy penalty")
	}
	if !proto.Equal(serialInfos[0], parallelInfos[0]) {
		t.Fatalf("cached-code VM info mismatch\nserial=%v\nparallel=%v", serialInfos[0], parallelInfos[0])
	}
	serialRoot, err := serialState.Commit()
	if err != nil {
		t.Fatal(err)
	}
	parallelRoot, err := parallelState.Commit()
	if err != nil {
		t.Fatal(err)
	}
	if serialRoot != parallelRoot {
		t.Fatalf("cached-code VM roots differ: serial=%x parallel=%x", serialRoot, parallelRoot)
	}
}

func TestProcessBlockPublishesBoundaryReadyAsyncVMRetry(t *testing.T) {
	base := newTestState(t)
	dynProps := base.DynamicProperties()
	dynProps.SetAllowCreationOfContracts(true)
	dynProps.SetAllowAdaptiveEnergy(true)
	dynProps.SetAllowBlackHoleOptimization(true)
	dynProps.SetLatestBlockHeaderTimestamp(30_000)
	passVersion3_6_5(base, 27)

	owner := testProcessorAddr(1)
	funder := testProcessorAddr(3)
	churnOwner := testProcessorAddr(5)
	churnRecipient := testProcessorAddr(6)
	contractAddr := testProcessorAddr(0x80)
	for _, address := range []tcommon.Address{owner, funder, churnOwner, churnRecipient, params.BlackholeAddress} {
		base.CreateAccount(address, corepb.AccountType_Normal)
	}
	base.AddBalance(owner, 100_000_000)
	base.AddBalance(funder, 100_000_000)
	base.AddBalance(churnOwner, 100_000_000)
	base.CreateAccount(contractAddr, corepb.AccountType_Contract)
	base.SetContract(contractAddr, &contractpb.SmartContract{
		OriginAddress: owner.Bytes(), ContractAddress: contractAddr.Bytes(),
	})
	base.SetCode(contractAddr, []byte{0x60, 0x01, 0x60, 0x02, 0x01, 0x50, 0x00})
	if _, err := base.Commit(); err != nil {
		t.Fatal(err)
	}

	serialState, err := base.Copy()
	if err != nil {
		t.Fatal(err)
	}
	serialState.SetDynamicProperties(base.DynamicProperties().Copy())
	parallelState, err := base.Copy()
	if err != nil {
		t.Fatal(err)
	}
	parallelState.SetDynamicProperties(base.DynamicProperties().Copy())

	transactions := []*types.Transaction{
		makeTestTriggerTx(1, contractAddr, []byte{0x01}),
		makeTestTransferTx(3, 1, 2_000_000),
		makeTestTriggerTx(1, contractAddr, []byte{0x02}),
	}
	// The conflict at tx 2 launches a two-result sender suffix. Unrelated
	// canonical work gives its descendant a deterministic opportunity to finish
	// before its own boundary without introducing any wait in production code.
	for range 256 {
		transactions = append(transactions, makeTestTransferTx(5, 6, 1))
	}
	transactions = append(transactions, makeTestTriggerTx(1, contractAddr, []byte{0x03}))
	transactionProtos := make([]*corepb.Transaction, len(transactions))
	for txIndex, tx := range transactions {
		if tx.ContractType() == corepb.Transaction_Contract_TriggerSmartContract {
			tx.Proto().RawData.FeeLimit = 10_000_000
			tx.Proto().Ret = []*corepb.Transaction_Result{{ContractRet: corepb.Transaction_Result_SUCCESS}}
		}
		transactionProtos[txIndex] = tx.Proto()
	}
	block := types.NewBlockFromPB(&corepb.Block{
		BlockHeader: &corepb.BlockHeader{RawData: &corepb.BlockHeaderRaw{
			Number: int64(vmSenderRetryPublishInterval + vmSenderRetryCoScheduleOffset), Timestamp: 33_000,
		}},
		Transactions: transactionProtos,
	})
	run := func(statedb *state.StateDB, options processBlockOptions) ([]*corepb.TransactionInfo, error) {
		infos, _, processErr := processBlockWithOptions(
			statedb, statedb.DynamicProperties(), block, ethrawdb.NewMemoryDatabase(), nil, 0,
			params.DefaultBlockNumForEnergyLimit, false, tcommon.Hash{}, nil, nil,
			nil, forks.NewVersionPassCache(), new(transactionInfoBatch), true, -1, nil,
			options,
		)
		return infos, processErr
	}
	serialInfos, err := run(serialState, processBlockOptions{})
	if err != nil {
		t.Fatalf("serial VM retry process: %v", err)
	}
	blocksBefore := parallelVMAsyncRetryBlocksCounter.Snapshot().Count()
	attemptsBefore := parallelVMAsyncRetryAttemptsCounter.Snapshot().Count()
	jobsBefore := parallelVMAsyncRetryJobsCounter.Snapshot().Count()
	executedBefore := parallelVMAsyncRetryExecutedCounter.Snapshot().Count()
	readyBefore := parallelVMAsyncRetryReadyCounter.Snapshot().Count()
	lateBefore := parallelVMAsyncRetryLateCounter.Snapshot().Count()
	staleBefore := parallelVMAsyncRetryStaleCounter.Snapshot().Count()
	candidatesBefore := parallelVMAsyncRetryCandidatesCounter.Snapshot().Count()
	validatedBefore := parallelVMAsyncRetryValidatedCounter.Snapshot().Count()
	recoveredBefore := parallelVMAsyncRetryRecoveredCounter.Snapshot().Count()
	errorsBefore := parallelVMAsyncRetryErrorsCounter.Snapshot().Count()
	prewarmedBefore := parallelVMAsyncRetryPrewarmedCounter.Snapshot().Count()
	capacityBefore := parallelVMAsyncRetryCapacityCounter.Snapshot().Count()
	sharedJobsBefore := parallelVMAsyncRetrySharedJobsCounter.Snapshot().Count()
	sharedErrorsBefore := parallelVMAsyncRetrySharedErrorsCounter.Snapshot().Count()
	sharedValueBlocksBefore := versionedShadowSharedValueBlocksCounter.Snapshot().Count()
	sharedValueHitsBefore := versionedShadowSharedValueHitsCounter.Snapshot().Count()
	publishBlocksBefore := parallelVMAsyncRetryPublishBlocksCounter.Snapshot().Count()
	publishCandidatesBefore := parallelVMAsyncRetryPublishCandidatesCounter.Snapshot().Count()
	retryPublishedBefore := parallelVMAsyncRetryPublishedCounter.Snapshot().Count()
	publishErrorsBefore := parallelVMAsyncRetryPublishErrorsCounter.Snapshot().Count()
	publishFallbacksBefore := parallelVMAsyncRetryPublishEnergyFallbackCounter.Snapshot().Count() +
		parallelVMAsyncRetryPublishNetFallbackCounter.Snapshot().Count() +
		parallelVMAsyncRetryPublishPreflightCounter.Snapshot().Count()
	publishWriteOKBefore := parallelVMAsyncRetryPublishWriteOKCounter.Snapshot().Count()
	publishWriteMismatchBefore := parallelVMAsyncRetryPublishWriteMismatchCounter.Snapshot().Count()
	serialVerifyCandidatesBefore := parallelVMSerialVerifyCandidatesCounter.Snapshot().Count()
	serialVerifyMatchesBefore := parallelVMSerialVerifyMatchesCounter.Snapshot().Count()
	serialVerifyErrorsBefore := parallelVMSerialVerifyErrorsCounter.Snapshot().Count()
	dualOracleCandidatesBefore := parallelVMDualOracleCandidatesCounter.Snapshot().Count()
	dualOracleMatchesBefore := parallelVMDualOracleMatchesCounter.Snapshot().Count()
	dualOracleInfoMismatchesBefore := parallelVMDualOracleInfoMismatchCounter.Snapshot().Count()
	dualOracleWriteMismatchesBefore := parallelVMDualOracleWriteMismatchCounter.Snapshot().Count()
	dualOracleBalanceMismatchesBefore := parallelVMDualOracleBalanceMismatchCounter.Snapshot().Count()
	dualOracleErrorsBefore := parallelVMDualOracleErrorsCounter.Snapshot().Count()
	mismatchesBefore := parallelVMAsyncRetryInfoMismatchCounter.Snapshot().Count() +
		parallelVMAsyncRetryWriteMismatchCounter.Snapshot().Count() +
		parallelVMAsyncRetryBalanceMismatchCounter.Snapshot().Count()
	vmPublishedBefore := parallelVMPublishedCounter.Snapshot().Count()
	parallelInfos, err := run(parallelState, processBlockOptions{parallelVM: true})
	if err != nil {
		t.Fatalf("parallel VM retry process: %v", err)
	}
	if blocks := parallelVMAsyncRetryBlocksCounter.Snapshot().Count() - blocksBefore; blocks != 1 {
		t.Fatalf("async VM retry blocks = %d, want 1", blocks)
	}
	if attempts := parallelVMAsyncRetryAttemptsCounter.Snapshot().Count() - attemptsBefore; attempts != 1 {
		t.Fatalf("async VM retry attempts = %d, want 1", attempts)
	}
	if jobs := parallelVMAsyncRetryJobsCounter.Snapshot().Count() - jobsBefore; jobs != 1 {
		t.Fatalf("async VM retry jobs = %d, want 1", jobs)
	}
	executed := parallelVMAsyncRetryExecutedCounter.Snapshot().Count() - executedBefore
	if executed != 2 {
		t.Fatalf("async VM retry executions = %d, want 2", executed)
	}
	classified := parallelVMAsyncRetryReadyCounter.Snapshot().Count() - readyBefore +
		parallelVMAsyncRetryLateCounter.Snapshot().Count() - lateBefore +
		parallelVMAsyncRetryStaleCounter.Snapshot().Count() - staleBefore
	if classified != executed {
		t.Fatalf("async VM retry classifications = %d, executions = %d", classified, executed)
	}
	candidates := parallelVMAsyncRetryCandidatesCounter.Snapshot().Count() - candidatesBefore
	validated := parallelVMAsyncRetryValidatedCounter.Snapshot().Count() - validatedBefore
	recovered := parallelVMAsyncRetryRecoveredCounter.Snapshot().Count() - recoveredBefore
	if candidates != 1 || validated != 0 || recovered != 0 {
		t.Fatalf("async VM retry candidates=%d validated=%d recovered=%d, want 1/0/0 for a published result", candidates, validated, recovered)
	}
	if failures := parallelVMAsyncRetryErrorsCounter.Snapshot().Count() - errorsBefore; failures != 0 {
		t.Fatalf("async VM retry errors = %d, want 0", failures)
	}
	if prewarmed := parallelVMAsyncRetryPrewarmedCounter.Snapshot().Count() - prewarmedBefore; prewarmed != 1 {
		t.Fatalf("async VM retry prewarmed runners = %d, want 1", prewarmed)
	}
	if capacity := parallelVMAsyncRetryCapacityCounter.Snapshot().Count() - capacityBefore; capacity != 1 {
		t.Fatalf("async VM retry runner capacity = %d, want 1", capacity)
	}
	if jobs := parallelVMAsyncRetrySharedJobsCounter.Snapshot().Count() - sharedJobsBefore; jobs != 1 {
		t.Fatalf("async VM retry shared-state jobs = %d, want 1", jobs)
	}
	if failures := parallelVMAsyncRetrySharedErrorsCounter.Snapshot().Count() - sharedErrorsBefore; failures != 0 {
		t.Fatalf("async VM retry shared-state errors = %d, want 0", failures)
	}
	if blocks := versionedShadowSharedValueBlocksCounter.Snapshot().Count() - sharedValueBlocksBefore; blocks != 1 {
		t.Fatalf("async VM retry shared-version blocks = %d, want 1", blocks)
	}
	if hits := versionedShadowSharedValueHitsCounter.Snapshot().Count() - sharedValueHitsBefore; hits <= 0 {
		t.Fatalf("async VM retry shared-version hits = %d, want > 0", hits)
	}
	mismatchesAfter := parallelVMAsyncRetryInfoMismatchCounter.Snapshot().Count() +
		parallelVMAsyncRetryWriteMismatchCounter.Snapshot().Count() +
		parallelVMAsyncRetryBalanceMismatchCounter.Snapshot().Count()
	if mismatches := mismatchesAfter - mismatchesBefore; mismatches != 0 {
		t.Fatalf("VM retry mismatches = %d, want 0", mismatches)
	}
	if blocks := parallelVMAsyncRetryPublishBlocksCounter.Snapshot().Count() - publishBlocksBefore; blocks != 1 {
		t.Fatalf("async VM retry publish blocks = %d, want 1", blocks)
	}
	if candidates := parallelVMAsyncRetryPublishCandidatesCounter.Snapshot().Count() - publishCandidatesBefore; candidates != 1 {
		t.Fatalf("async VM retry publish candidates = %d, want 1", candidates)
	}
	if published := parallelVMAsyncRetryPublishedCounter.Snapshot().Count() - retryPublishedBefore; published != 1 {
		t.Fatalf("async VM retry publications = %d, want 1", published)
	}
	if failures := parallelVMAsyncRetryPublishErrorsCounter.Snapshot().Count() - publishErrorsBefore; failures != 0 {
		t.Fatalf("async VM retry publish errors = %d, want 0", failures)
	}
	publishFallbacksAfter := parallelVMAsyncRetryPublishEnergyFallbackCounter.Snapshot().Count() +
		parallelVMAsyncRetryPublishNetFallbackCounter.Snapshot().Count() +
		parallelVMAsyncRetryPublishPreflightCounter.Snapshot().Count()
	if fallbacks := publishFallbacksAfter - publishFallbacksBefore; fallbacks != 0 {
		t.Fatalf("async VM retry publish fallbacks = %d, want 0", fallbacks)
	}
	if matches := parallelVMAsyncRetryPublishWriteOKCounter.Snapshot().Count() - publishWriteOKBefore; matches != 1 {
		t.Fatalf("async VM retry publish write-set matches = %d, want 1", matches)
	}
	if mismatches := parallelVMAsyncRetryPublishWriteMismatchCounter.Snapshot().Count() - publishWriteMismatchBefore; mismatches != 0 {
		t.Fatalf("async VM retry publish write-set mismatches = %d, want 0", mismatches)
	}
	if candidates := parallelVMSerialVerifyCandidatesCounter.Snapshot().Count() - serialVerifyCandidatesBefore; candidates != 1 {
		t.Fatalf("async VM retry boundary serial verification candidates = %d, want 1", candidates)
	}
	if matches := parallelVMSerialVerifyMatchesCounter.Snapshot().Count() - serialVerifyMatchesBefore; matches != 1 {
		t.Fatalf("async VM retry boundary serial verification matches = %d, want 1", matches)
	}
	if failures := parallelVMSerialVerifyErrorsCounter.Snapshot().Count() - serialVerifyErrorsBefore; failures != 0 {
		t.Fatalf("async VM retry boundary serial verification errors = %d, want 0", failures)
	}
	if candidates := parallelVMDualOracleCandidatesCounter.Snapshot().Count() - dualOracleCandidatesBefore; candidates != 1 {
		t.Fatalf("async VM retry dual-oracle candidates = %d, want 1", candidates)
	}
	if matches := parallelVMDualOracleMatchesCounter.Snapshot().Count() - dualOracleMatchesBefore; matches != 1 {
		t.Fatalf("async VM retry dual-oracle matches = %d, want 1", matches)
	}
	if mismatches := parallelVMDualOracleInfoMismatchCounter.Snapshot().Count() - dualOracleInfoMismatchesBefore; mismatches != 0 {
		t.Fatalf("async VM retry dual-oracle info mismatches = %d, want 0", mismatches)
	}
	if mismatches := parallelVMDualOracleWriteMismatchCounter.Snapshot().Count() - dualOracleWriteMismatchesBefore; mismatches != 0 {
		t.Fatalf("async VM retry dual-oracle write mismatches = %d, want 0", mismatches)
	}
	if mismatches := parallelVMDualOracleBalanceMismatchCounter.Snapshot().Count() - dualOracleBalanceMismatchesBefore; mismatches != 0 {
		t.Fatalf("async VM retry dual-oracle balance mismatches = %d, want 0", mismatches)
	}
	if failures := parallelVMDualOracleErrorsCounter.Snapshot().Count() - dualOracleErrorsBefore; failures != 0 {
		t.Fatalf("async VM retry dual-oracle errors = %d, want 0", failures)
	}
	if published := parallelVMPublishedCounter.Snapshot().Count() - vmPublishedBefore; published != 1 {
		t.Fatalf("VM publications = %d, want 1 async retry descendant", published)
	}
	for txIndex := range serialInfos {
		if !proto.Equal(serialInfos[txIndex], parallelInfos[txIndex]) {
			t.Fatalf("tx %d info mismatch\nserial=%v\nparallel=%v", txIndex, serialInfos[txIndex], parallelInfos[txIndex])
		}
	}
	for _, property := range []struct {
		name     string
		serial   int64
		parallel int64
	}{
		{name: "public_net_usage", serial: serialState.DynamicProperties().PublicNetUsage(), parallel: parallelState.DynamicProperties().PublicNetUsage()},
		{name: "public_net_time", serial: serialState.DynamicProperties().PublicNetTime(), parallel: parallelState.DynamicProperties().PublicNetTime()},
		{name: "block_energy_usage", serial: serialState.DynamicProperties().BlockEnergyUsage(), parallel: parallelState.DynamicProperties().BlockEnergyUsage()},
	} {
		if property.serial != property.parallel {
			t.Fatalf("%s serial=%d parallel=%d", property.name, property.serial, property.parallel)
		}
	}
	serialRoot, err := serialState.Commit()
	if err != nil {
		t.Fatal(err)
	}
	parallelRoot, err := parallelState.Commit()
	if err != nil {
		t.Fatal(err)
	}
	if serialRoot != parallelRoot {
		t.Fatalf("VM retry state roots differ: serial=%x parallel=%x", serialRoot, parallelRoot)
	}
}

func TestProcessBlockSamplesSenderChainForwarding(t *testing.T) {
	statedb := newTestState(t)
	for _, id := range []byte{1, 2, 3} {
		statedb.CreateAccount(testProcessorAddr(id), corepb.AccountType_Normal)
	}
	statedb.AddBalance(testProcessorAddr(1), 10_000_000)
	if _, err := statedb.Commit(); err != nil {
		t.Fatal(err)
	}
	transactions := []*types.Transaction{
		makeTestTransferTx(1, 2, 1_000_000),
		makeTestTransferTx(1, 3, 2_000_000),
	}
	block := types.NewBlockFromPB(&corepb.Block{
		BlockHeader: &corepb.BlockHeader{RawData: &corepb.BlockHeaderRaw{
			Number: int64(discardShadowAsyncRetryInterval), Timestamp: 3_000,
		}},
		Transactions: []*corepb.Transaction{transactions[0].Proto(), transactions[1].Proto()},
	})
	forwardedBefore := discardShadowSenderChainForwardedCounter.Snapshot().Count()
	validatedBefore := discardShadowSenderChainForwardedOKCounter.Snapshot().Count()
	mismatchBefore := discardShadowSenderChainInfoMismatchesCounter.Snapshot().Count() +
		discardShadowSenderChainWriteMismatchesCounter.Snapshot().Count() +
		discardShadowSenderChainBalanceMismatchesCounter.Snapshot().Count()
	if _, _, err := processBlockWithOptions(
		statedb, statedb.DynamicProperties(), block, nil, nil, 0,
		params.DefaultBlockNumForEnergyLimit, false, tcommon.Hash{}, nil, nil,
		nil, forks.NewVersionPassCache(), new(transactionInfoBatch), true, -1, nil,
		processBlockOptions{parallelTransfers: true},
	); err != nil {
		t.Fatalf("process sampled sender chain: %v", err)
	}
	if forwarded := discardShadowSenderChainForwardedCounter.Snapshot().Count() - forwardedBefore; forwarded != 1 {
		t.Fatalf("forwarded sender-chain results = %d, want 1", forwarded)
	}
	if validated := discardShadowSenderChainForwardedOKCounter.Snapshot().Count() - validatedBefore; validated != 1 {
		t.Fatalf("validated forwarded results = %d, want 1", validated)
	}
	mismatchAfter := discardShadowSenderChainInfoMismatchesCounter.Snapshot().Count() +
		discardShadowSenderChainWriteMismatchesCounter.Snapshot().Count() +
		discardShadowSenderChainBalanceMismatchesCounter.Snapshot().Count()
	if mismatches := mismatchAfter - mismatchBefore; mismatches != 0 {
		t.Fatalf("sender-chain mismatches = %d, want 0", mismatches)
	}
	if balance := statedb.GetBalance(testProcessorAddr(1)); balance != 7_000_000 {
		t.Fatalf("owner balance = %d, want 7000000", balance)
	}
}

func TestProcessBlockSamplesSenderRetryIncarnation(t *testing.T) {
	statedb := newTestState(t)
	for _, id := range []byte{1, 2, 3, 4, 5} {
		statedb.CreateAccount(testProcessorAddr(id), corepb.AccountType_Normal)
	}
	statedb.AddBalance(testProcessorAddr(1), 10_000_000)
	statedb.AddBalance(testProcessorAddr(3), 10_000_000)
	if _, err := statedb.Commit(); err != nil {
		t.Fatal(err)
	}
	transactions := []*types.Transaction{
		makeTestTransferTx(1, 2, 1_000_000),
		makeTestTransferTx(3, 1, 2_000_000),
		makeTestTransferTx(1, 4, 3_000_000),
		makeTestTransferTx(1, 5, 1_000_000),
	}
	block := types.NewBlockFromPB(&corepb.Block{
		BlockHeader: &corepb.BlockHeader{RawData: &corepb.BlockHeaderRaw{
			Number: int64(discardShadowAsyncRetryInterval), Timestamp: 3_000,
		}},
		Transactions: []*corepb.Transaction{
			transactions[0].Proto(), transactions[1].Proto(), transactions[2].Proto(), transactions[3].Proto(),
		},
	})
	attemptsBefore := discardShadowRetryAttemptsCounter.Snapshot().Count()
	executedBefore := discardShadowRetryExecutedCounter.Snapshot().Count()
	candidatesBefore := discardShadowRetryCandidatesCounter.Snapshot().Count()
	recoveredBefore := discardShadowRetryRecoveredCounter.Snapshot().Count()
	validatedBefore := discardShadowRetryValidatedCounter.Snapshot().Count()
	prefixRefreshBefore := discardShadowRetryPrefixRefreshCounter.Snapshot().Count()
	prefixReuseBefore := discardShadowRetryPrefixReuseCounter.Snapshot().Count()
	prefixAdvanceBefore := discardShadowRetryPrefixAdvanceCounter.Snapshot().Count()
	asyncCandidatesBefore := discardShadowRetryAsyncCandidatesCounter.Snapshot().Count()
	asyncReadyBefore := discardShadowRetryAsyncReadyCounter.Snapshot().Count()
	asyncLateBefore := discardShadowRetryAsyncLateCounter.Snapshot().Count()
	asyncUnknownBefore := discardShadowRetryAsyncUnknownCounter.Snapshot().Count()
	mismatchBefore := discardShadowRetryInfoMismatchCounter.Snapshot().Count() +
		discardShadowRetryWriteMismatchCounter.Snapshot().Count() +
		discardShadowRetryBalanceMismatchCounter.Snapshot().Count() +
		discardShadowRetryErrorsCounter.Snapshot().Count()
	if _, _, err := processBlockWithOptions(
		statedb, statedb.DynamicProperties(), block, nil, nil, 0,
		params.DefaultBlockNumForEnergyLimit, false, tcommon.Hash{}, nil, nil,
		nil, forks.NewVersionPassCache(), new(transactionInfoBatch), true, -1, nil,
		processBlockOptions{parallelTransfers: true},
	); err != nil {
		t.Fatalf("process sampled sender retry: %v", err)
	}
	if attempts := discardShadowRetryAttemptsCounter.Snapshot().Count() - attemptsBefore; attempts != 1 {
		t.Fatalf("sender retry attempts = %d, want 1 (executed=%d candidates=%d recovered=%d validated=%d)", attempts,
			discardShadowRetryExecutedCounter.Snapshot().Count()-executedBefore,
			discardShadowRetryCandidatesCounter.Snapshot().Count()-candidatesBefore,
			discardShadowRetryRecoveredCounter.Snapshot().Count()-recoveredBefore,
			discardShadowRetryValidatedCounter.Snapshot().Count()-validatedBefore)
	}
	if executed := discardShadowRetryExecutedCounter.Snapshot().Count() - executedBefore; executed != 2 {
		t.Fatalf("sender retry executions = %d, want 2", executed)
	}
	if candidates := discardShadowRetryCandidatesCounter.Snapshot().Count() - candidatesBefore; candidates != 2 {
		t.Fatalf("sender retry candidates = %d, want 2", candidates)
	}
	if recovered := discardShadowRetryRecoveredCounter.Snapshot().Count() - recoveredBefore; recovered != 2 {
		t.Fatalf("sender retry recovered = %d, want 2", recovered)
	}
	if validated := discardShadowRetryValidatedCounter.Snapshot().Count() - validatedBefore; validated != 2 {
		t.Fatalf("sender retry validated = %d, want 2", validated)
	}
	if refreshes := discardShadowRetryPrefixRefreshCounter.Snapshot().Count() - prefixRefreshBefore; refreshes != 1 {
		t.Fatalf("sender retry prefix refreshes = %d, want 1", refreshes)
	}
	if reuses := discardShadowRetryPrefixReuseCounter.Snapshot().Count() - prefixReuseBefore; reuses != 0 {
		t.Fatalf("sender retry prefix reuses = %d, want 0", reuses)
	}
	if advances := discardShadowRetryPrefixAdvanceCounter.Snapshot().Count() - prefixAdvanceBefore; advances != 0 {
		t.Fatalf("sender retry prefix advances = %d, want 0", advances)
	}
	asyncCandidates := discardShadowRetryAsyncCandidatesCounter.Snapshot().Count() - asyncCandidatesBefore
	asyncClassified := discardShadowRetryAsyncReadyCounter.Snapshot().Count() - asyncReadyBefore +
		discardShadowRetryAsyncLateCounter.Snapshot().Count() - asyncLateBefore +
		discardShadowRetryAsyncUnknownCounter.Snapshot().Count() - asyncUnknownBefore
	if asyncCandidates != 2 || asyncClassified != asyncCandidates {
		t.Fatalf("sender retry async projection candidates=%d classified=%d, want 2", asyncCandidates, asyncClassified)
	}
	mismatchAfter := discardShadowRetryInfoMismatchCounter.Snapshot().Count() +
		discardShadowRetryWriteMismatchCounter.Snapshot().Count() +
		discardShadowRetryBalanceMismatchCounter.Snapshot().Count() +
		discardShadowRetryErrorsCounter.Snapshot().Count()
	if mismatches := mismatchAfter - mismatchBefore; mismatches != 0 {
		t.Fatalf("sender retry mismatches/errors = %d, want 0", mismatches)
	}
	if balance := statedb.GetBalance(testProcessorAddr(1)); balance != 7_000_000 {
		t.Fatalf("owner balance = %d, want 7000000", balance)
	}
}

func TestProcessBlockRunsActualAsyncSenderRetryCanary(t *testing.T) {
	statedb := newTestState(t)
	for _, id := range []byte{1, 2, 3, 4, 5} {
		statedb.CreateAccount(testProcessorAddr(id), corepb.AccountType_Normal)
	}
	statedb.AddBalance(testProcessorAddr(1), 10_000_000)
	statedb.AddBalance(testProcessorAddr(3), 10_000_000)
	if _, err := statedb.Commit(); err != nil {
		t.Fatal(err)
	}
	transactions := []*types.Transaction{
		makeTestTransferTx(1, 2, 1_000_000),
		makeTestTransferTx(3, 1, 2_000_000),
		makeTestTransferTx(1, 4, 3_000_000),
		makeTestTransferTx(1, 5, 1_000_000),
	}
	block := types.NewBlockFromPB(&corepb.Block{
		BlockHeader: &corepb.BlockHeader{RawData: &corepb.BlockHeaderRaw{
			Number: int64(discardShadowAsyncRetryFirstOffset), Timestamp: 3_000,
		}},
		Transactions: []*corepb.Transaction{
			transactions[0].Proto(), transactions[1].Proto(), transactions[2].Proto(), transactions[3].Proto(),
		},
	})
	blocksBefore := discardShadowRetryActualBlocksCounter.Snapshot().Count()
	prewarmedBefore := discardShadowRetryActualPrewarmedCounter.Snapshot().Count()
	capacityBefore := discardShadowRetryActualRunnerCapacityCounter.Snapshot().Count()
	maxInflightBefore := discardShadowRetryActualMaxInflightCounter.Snapshot().Count()
	deferredBefore := discardShadowRetryActualDeferredCounter.Snapshot().Count()
	supersededBefore := discardShadowRetryActualSupersededCounter.Snapshot().Count()
	queueEnqueuedBefore := discardShadowRetryActualQueueEnqueuedCounter.Snapshot().Count()
	queueDequeuedBefore := discardShadowRetryActualQueueDequeuedCounter.Snapshot().Count()
	queueDroppedBefore := discardShadowRetryActualQueueDroppedCounter.Snapshot().Count()
	publishedBefore := parallelTransferRetryPublishedCounter.Snapshot().Count()
	workerPrefixJobsBefore := discardShadowRetryActualWorkerPrefixJobsCounter.Snapshot().Count()
	workerPrefixAdvancesBefore := discardShadowRetryActualWorkerPrefixAdvanceCounter.Snapshot().Count()
	workerPrefixNanosBefore := discardShadowRetryActualWorkerPrefixNanosCounter.Snapshot().Count()
	workerPrefixErrorsBefore := discardShadowRetryActualWorkerPrefixErrorsCounter.Snapshot().Count()
	sharedStateJobsBefore := discardShadowRetrySharedStateJobsCounter.Snapshot().Count()
	sharedStateCopyNanosBefore := discardShadowRetrySharedStateCopyNanosCounter.Snapshot().Count()
	sharedStateErrorsBefore := discardShadowRetrySharedStateErrorsCounter.Snapshot().Count()
	sharedValueBlocksBefore := versionedShadowSharedValueBlocksCounter.Snapshot().Count()
	sharedValueHitsBefore := versionedShadowSharedValueHitsCounter.Snapshot().Count()
	jobsBefore := discardShadowRetryActualJobsCounter.Snapshot().Count()
	executedBefore := discardShadowRetryActualExecutedCounter.Snapshot().Count()
	readyBefore := discardShadowRetryActualReadyCounter.Snapshot().Count()
	lateBefore := discardShadowRetryActualLateCounter.Snapshot().Count()
	staleBefore := discardShadowRetryActualStaleCounter.Snapshot().Count()
	errorsBefore := discardShadowRetryActualErrorsCounter.Snapshot().Count()
	prefixRefreshBefore := discardShadowRetryPrefixRefreshCounter.Snapshot().Count()
	prefixAdvanceBefore := discardShadowRetryPrefixAdvanceCounter.Snapshot().Count()
	actualPrefixRefreshBefore := discardShadowRetryActualPrefixRefreshCounter.Snapshot().Count()
	actualPrefixAdvanceBefore := discardShadowRetryActualPrefixAdvanceCounter.Snapshot().Count()
	if _, _, err := processBlockWithOptions(
		statedb, statedb.DynamicProperties(), block, nil, nil, 0,
		params.DefaultBlockNumForEnergyLimit, false, tcommon.Hash{}, nil, nil,
		nil, forks.NewVersionPassCache(), new(transactionInfoBatch), true, -1, nil,
		processBlockOptions{parallelTransfers: true},
	); err != nil {
		t.Fatalf("process actual async sender retry: %v", err)
	}
	if blocks := discardShadowRetryActualBlocksCounter.Snapshot().Count() - blocksBefore; blocks != 1 {
		t.Fatalf("actual async blocks = %d, want 1", blocks)
	}
	if jobs := discardShadowRetryActualJobsCounter.Snapshot().Count() - jobsBefore; jobs != 1 {
		t.Fatalf("actual async jobs = %d, want 1", jobs)
	}
	if prewarmed := discardShadowRetryActualPrewarmedCounter.Snapshot().Count() - prewarmedBefore; prewarmed != 1 {
		t.Fatalf("actual async prewarmed runners = %d, want 1", prewarmed)
	}
	if capacity := discardShadowRetryActualRunnerCapacityCounter.Snapshot().Count() - capacityBefore; capacity != 1 {
		t.Fatalf("actual async runner capacity = %d, want 1", capacity)
	}
	if maxInflight := discardShadowRetryActualMaxInflightCounter.Snapshot().Count() - maxInflightBefore; maxInflight != 1 {
		t.Fatalf("actual async max inflight = %d, want 1", maxInflight)
	}
	if deferred := discardShadowRetryActualDeferredCounter.Snapshot().Count() - deferredBefore; deferred != 0 {
		t.Fatalf("actual async deferred suffix = %d, want 0", deferred)
	}
	if superseded := discardShadowRetryActualSupersededCounter.Snapshot().Count() - supersededBefore; superseded != 0 {
		t.Fatalf("actual async superseded suffix = %d, want 0", superseded)
	}
	if enqueued := discardShadowRetryActualQueueEnqueuedCounter.Snapshot().Count() - queueEnqueuedBefore; enqueued != 1 {
		t.Fatalf("actual async queued requests = %d, want 1", enqueued)
	}
	if dequeued := discardShadowRetryActualQueueDequeuedCounter.Snapshot().Count() - queueDequeuedBefore; dequeued != 1 {
		t.Fatalf("actual async dequeued requests = %d, want 1", dequeued)
	}
	if dropped := discardShadowRetryActualQueueDroppedCounter.Snapshot().Count() - queueDroppedBefore; dropped != 0 {
		t.Fatalf("actual async dropped queued tasks = %d, want 0", dropped)
	}
	if published := parallelTransferRetryPublishedCounter.Snapshot().Count() - publishedBefore; published != 0 {
		t.Fatalf("non-publication async cohort published %d retries", published)
	}
	if jobs := discardShadowRetryActualWorkerPrefixJobsCounter.Snapshot().Count() - workerPrefixJobsBefore; jobs != 0 {
		t.Fatalf("actual async worker prefix jobs = %d, want 0", jobs)
	}
	if advances := discardShadowRetryActualWorkerPrefixAdvanceCounter.Snapshot().Count() - workerPrefixAdvancesBefore; advances != 0 {
		t.Fatalf("actual async worker prefix advances = %d, want 0", advances)
	}
	if nanos := discardShadowRetryActualWorkerPrefixNanosCounter.Snapshot().Count() - workerPrefixNanosBefore; nanos != 0 {
		t.Fatalf("actual async worker prefix nanos = %d, want 0", nanos)
	}
	if errors := discardShadowRetryActualWorkerPrefixErrorsCounter.Snapshot().Count() - workerPrefixErrorsBefore; errors != 0 {
		t.Fatalf("actual async worker prefix errors = %d, want 0", errors)
	}
	if refreshes := discardShadowRetryPrefixRefreshCounter.Snapshot().Count() - prefixRefreshBefore; refreshes != 0 {
		t.Fatalf("actual async prefix refreshes = %d, want 0", refreshes)
	}
	if advances := discardShadowRetryPrefixAdvanceCounter.Snapshot().Count() - prefixAdvanceBefore; advances != 0 {
		t.Fatalf("actual async prefix advances = %d, want 0", advances)
	}
	if refreshes := discardShadowRetryActualPrefixRefreshCounter.Snapshot().Count() - actualPrefixRefreshBefore; refreshes != 0 {
		t.Fatalf("actual-only async prefix refreshes = %d, want 0", refreshes)
	}
	if advances := discardShadowRetryActualPrefixAdvanceCounter.Snapshot().Count() - actualPrefixAdvanceBefore; advances != 0 {
		t.Fatalf("actual-only async prefix advances = %d, want 0", advances)
	}
	if jobs := discardShadowRetrySharedStateJobsCounter.Snapshot().Count() - sharedStateJobsBefore; jobs != 1 {
		t.Fatalf("actual async shared-state jobs = %d, want 1", jobs)
	}
	if nanos := discardShadowRetrySharedStateCopyNanosCounter.Snapshot().Count() - sharedStateCopyNanosBefore; nanos <= 0 {
		t.Fatalf("actual async shared-state copy nanos = %d, want > 0", nanos)
	}
	if errors := discardShadowRetrySharedStateErrorsCounter.Snapshot().Count() - sharedStateErrorsBefore; errors != 0 {
		t.Fatalf("actual async shared-state errors = %d, want 0", errors)
	}
	if blocks := versionedShadowSharedValueBlocksCounter.Snapshot().Count() - sharedValueBlocksBefore; blocks != 1 {
		t.Fatalf("shared version blocks = %d, want 1", blocks)
	}
	if hits := versionedShadowSharedValueHitsCounter.Snapshot().Count() - sharedValueHitsBefore; hits <= 0 {
		t.Fatalf("shared version hits = %d, want > 0", hits)
	}
	executed := discardShadowRetryActualExecutedCounter.Snapshot().Count() - executedBefore
	if executed != 2 {
		t.Fatalf("actual async executions = %d, want 2", executed)
	}
	classified := discardShadowRetryActualReadyCounter.Snapshot().Count() - readyBefore +
		discardShadowRetryActualLateCounter.Snapshot().Count() - lateBefore +
		discardShadowRetryActualStaleCounter.Snapshot().Count() - staleBefore
	if classified != executed {
		t.Fatalf("actual async classified results = %d, executions = %d", classified, executed)
	}
	if errors := discardShadowRetryActualErrorsCounter.Snapshot().Count() - errorsBefore; errors != 0 {
		t.Fatalf("actual async errors = %d, want 0", errors)
	}
	if balance := statedb.GetBalance(testProcessorAddr(1)); balance != 7_000_000 {
		t.Fatalf("owner balance = %d, want 7000000", balance)
	}
}

func TestProcessBlockPublishesAsyncSenderRetryCohort(t *testing.T) {
	testProcessBlockPublishesAsyncSenderRetry(t, discardShadowAsyncPublishOffset)
}

func TestProcessBlockPublishesAsyncSenderRetryOnOrdinaryBlock(t *testing.T) {
	testProcessBlockPublishesAsyncSenderRetry(t, discardShadowAsyncPublishOffset+1)
}

func testProcessBlockPublishesAsyncSenderRetry(t *testing.T, blockNumber uint64) {
	t.Helper()
	base := newTestState(t)
	for id := byte(1); id <= 250; id++ {
		base.CreateAccount(testProcessorAddr(id), corepb.AccountType_Normal)
	}
	base.AddBalance(testProcessorAddr(1), 20_000_000)
	base.AddBalance(testProcessorAddr(3), 10_000_000)
	for id := byte(10); id <= 99; id++ {
		base.AddBalance(testProcessorAddr(id), 1_000_000)
	}
	if _, err := base.Commit(); err != nil {
		t.Fatal(err)
	}
	serialState, err := base.Copy()
	if err != nil {
		t.Fatal(err)
	}
	serialState.SetDynamicProperties(base.DynamicProperties().Copy())
	parallelState, err := base.Copy()
	if err != nil {
		t.Fatal(err)
	}
	parallelState.SetDynamicProperties(base.DynamicProperties().Copy())
	transactions := []*types.Transaction{
		makeTestTransferTx(1, 2, 1_000_000),
		makeTestTransferTx(3, 1, 2_000_000),
		makeTestTransferTx(1, 4, 3_000_000),
	}
	// Leave enough independent canonical work between the conflicted sender
	// retry and its next publication boundary for the background incarnation
	// to complete even under the race detector. Production never waits for the
	// worker: a result that misses its boundary still falls back to serial.
	for id := byte(10); id <= 99; id++ {
		transactions = append(transactions, makeTestTransferTx(id, id+140, 1_000))
	}
	transactions = append(transactions, makeTestTransferTx(1, 5, 1_000_000))
	transactionProtos := make([]*corepb.Transaction, len(transactions))
	for txIndex, tx := range transactions {
		transactionProtos[txIndex] = tx.Proto()
	}
	block := types.NewBlockFromPB(&corepb.Block{
		BlockHeader: &corepb.BlockHeader{RawData: &corepb.BlockHeaderRaw{
			Number: int64(blockNumber), Timestamp: 3_000,
		}},
		Transactions: transactionProtos,
	})
	run := func(statedb *state.StateDB, options processBlockOptions) ([]*corepb.TransactionInfo, error) {
		infos, _, processErr := processBlockWithOptions(
			statedb, statedb.DynamicProperties(), block, nil, nil, 0,
			params.DefaultBlockNumForEnergyLimit, false, tcommon.Hash{}, nil, nil,
			nil, forks.NewVersionPassCache(), new(transactionInfoBatch), true, -1, nil,
			options,
		)
		return infos, processErr
	}
	serialInfos, err := run(serialState, processBlockOptions{captureBalanceTrace: true})
	if err != nil {
		t.Fatalf("serial process: %v", err)
	}
	retryCandidatesBefore := parallelTransferRetryCandidatesCounter.Snapshot().Count()
	retryPublishedBefore := parallelTransferRetryPublishedCounter.Snapshot().Count()
	allPublishedBefore := parallelTransferPublishedCounter.Snapshot().Count()
	serialVerifyBefore := parallelTransferSerialVerifyCandidatesCounter.Snapshot().Count()
	serialVerifyMatchesBefore := parallelTransferSerialVerifyMatchesCounter.Snapshot().Count()
	publishedBefore := discardShadowRetryActualPublishedCounter.Snapshot().Count()
	writeMatchesBefore := discardShadowRetryActualPublishedWriteOKCounter.Snapshot().Count()
	writeMismatchesBefore := discardShadowRetryActualPublishedWriteMismatchCounter.Snapshot().Count()
	captureBlocksBefore := versionedShadowWriteCaptureBlocksCounter.Snapshot().Count()
	captureTransactionsBefore := versionedShadowWriteCaptureTransactionsCounter.Snapshot().Count()
	captureFullBefore := versionedShadowWriteCaptureFullTransactionsCounter.Snapshot().Count()
	captureFilteredBefore := versionedShadowWriteCaptureFilteredTransactionsCounter.Snapshot().Count()
	captureCellsBefore := versionedShadowWriteCaptureCellsCounter.Snapshot().Count()
	captureNanosBefore := versionedShadowWriteCaptureNanosCounter.Snapshot().Count()
	captureFullCellsBefore := versionedShadowWriteCaptureFullCellsCounter.Snapshot().Count()
	captureFilteredCellsBefore := versionedShadowWriteCaptureFilteredCellsCounter.Snapshot().Count()
	captureFullNanosBefore := versionedShadowWriteCaptureFullNanosCounter.Snapshot().Count()
	captureFilteredNanosBefore := versionedShadowWriteCaptureFilteredNanosCounter.Snapshot().Count()
	captureRecorderTransactionsBefore := versionedShadowWriteCaptureRecorderTransactionsCounter.Snapshot().Count()
	captureRecorderNanosBefore := versionedShadowWriteCaptureRecorderNanosCounter.Snapshot().Count()
	captureRecorderFullTransactionsBefore := versionedShadowRecorderFullTxCounter.Snapshot().Count()
	captureRecorderFullNanosBefore := versionedShadowRecorderFullNanosCounter.Snapshot().Count()
	captureFilteredEmptyBefore := versionedShadowWriteCaptureFilteredEmptyCounter.Snapshot().Count()
	captureUnsupportedBefore := versionedShadowWriteCaptureUnsupportedCounter.Snapshot().Count()
	captureErrorsBefore := versionedShadowWriteCaptureErrorsCounter.Snapshot().Count()
	parallelInfos, err := run(parallelState, processBlockOptions{parallelTransfers: true, captureBalanceTrace: true})
	if err != nil {
		t.Fatalf("parallel process: %v", err)
	}
	if candidates := parallelTransferRetryCandidatesCounter.Snapshot().Count() - retryCandidatesBefore; candidates != 1 {
		t.Fatalf("async retry publication candidates = %d, want 1", candidates)
	}
	if published := parallelTransferRetryPublishedCounter.Snapshot().Count() - retryPublishedBefore; published != 1 {
		t.Fatalf("async retry publications = %d, want 1", published)
	}
	serialCandidates := parallelTransferSerialVerifyCandidatesCounter.Snapshot().Count() - serialVerifyBefore
	serialMatches := parallelTransferSerialVerifyMatchesCounter.Snapshot().Count() - serialVerifyMatchesBefore
	allPublished := parallelTransferPublishedCounter.Snapshot().Count() - allPublishedBefore
	if allPublished < 1 || serialCandidates != allPublished || serialMatches != allPublished {
		t.Fatalf("async block transfer publications/serial candidates/matches = %d/%d/%d, want equal non-zero counts", allPublished, serialCandidates, serialMatches)
	}
	if published := discardShadowRetryActualPublishedCounter.Snapshot().Count() - publishedBefore; published != 1 {
		t.Fatalf("async retry published results = %d, want 1", published)
	}
	if matches := discardShadowRetryActualPublishedWriteOKCounter.Snapshot().Count() - writeMatchesBefore; matches != 1 {
		mismatches := discardShadowRetryActualPublishedWriteMismatchCounter.Snapshot().Count() - writeMismatchesBefore
		t.Fatalf("async retry published write matches = %d, mismatches = %d, want 1/0", matches, mismatches)
	}
	if mismatches := discardShadowRetryActualPublishedWriteMismatchCounter.Snapshot().Count() - writeMismatchesBefore; mismatches != 0 {
		t.Fatalf("async retry published write mismatches = %d, want 0", mismatches)
	}
	if blocks := versionedShadowWriteCaptureBlocksCounter.Snapshot().Count() - captureBlocksBefore; blocks != 1 {
		t.Fatalf("write capture blocks = %d, want 1", blocks)
	}
	if captured := versionedShadowWriteCaptureTransactionsCounter.Snapshot().Count() - captureTransactionsBefore; captured != int64(len(transactions)) {
		t.Fatalf("write capture transactions = %d, want %d", captured, len(transactions))
	}
	fullCaptured := versionedShadowWriteCaptureFullTransactionsCounter.Snapshot().Count() - captureFullBefore
	filteredCaptured := versionedShadowWriteCaptureFilteredTransactionsCounter.Snapshot().Count() - captureFilteredBefore
	if blockNumber%discardShadowSampleInterval == 0 {
		if fullCaptured != int64(len(transactions)) || filteredCaptured != 0 {
			t.Fatalf("sampled write capture full/filtered = %d/%d, want %d/0", fullCaptured, filteredCaptured, len(transactions))
		}
	} else if fullCaptured != int64(len(transactions)) || filteredCaptured != 0 {
		// Every transaction in this fixture is a plain-transfer publication
		// candidate. Production publication now requires a complete canonical
		// WriteSet capture for the immediate fail-closed apply audit.
		t.Fatalf("ordinary write capture full/filtered = %d/%d, want %d/0", fullCaptured, filteredCaptured, len(transactions))
	}
	if cells := versionedShadowWriteCaptureCellsCounter.Snapshot().Count() - captureCellsBefore; cells <= 0 {
		t.Fatalf("write capture cells = %d, want > 0", cells)
	}
	if nanos := versionedShadowWriteCaptureNanosCounter.Snapshot().Count() - captureNanosBefore; nanos <= 0 {
		t.Fatalf("write capture nanos = %d, want > 0", nanos)
	}
	fullCells := versionedShadowWriteCaptureFullCellsCounter.Snapshot().Count() - captureFullCellsBefore
	filteredCells := versionedShadowWriteCaptureFilteredCellsCounter.Snapshot().Count() - captureFilteredCellsBefore
	if cells := versionedShadowWriteCaptureCellsCounter.Snapshot().Count() - captureCellsBefore; fullCells+filteredCells != cells {
		t.Fatalf("write capture full/filtered cells = %d/%d, total %d", fullCells, filteredCells, cells)
	}
	fullNanos := versionedShadowWriteCaptureFullNanosCounter.Snapshot().Count() - captureFullNanosBefore
	filteredNanos := versionedShadowWriteCaptureFilteredNanosCounter.Snapshot().Count() - captureFilteredNanosBefore
	if nanos := versionedShadowWriteCaptureNanosCounter.Snapshot().Count() - captureNanosBefore; fullNanos+filteredNanos != nanos {
		t.Fatalf("write capture full/filtered nanos = %d/%d, total %d", fullNanos, filteredNanos, nanos)
	}
	if filteredCaptured == 0 && (filteredCells != 0 || filteredNanos != 0) {
		t.Fatalf("zero filtered captures produced cells/nanos = %d/%d", filteredCells, filteredNanos)
	}
	recorderTransactions := versionedShadowWriteCaptureRecorderTransactionsCounter.Snapshot().Count() - captureRecorderTransactionsBefore
	recorderNanos := versionedShadowWriteCaptureRecorderNanosCounter.Snapshot().Count() - captureRecorderNanosBefore
	recorderFullTransactions := versionedShadowRecorderFullTxCounter.Snapshot().Count() - captureRecorderFullTransactionsBefore
	recorderFullNanos := versionedShadowRecorderFullNanosCounter.Snapshot().Count() - captureRecorderFullNanosBefore
	if blockNumber%discardShadowSampleInterval == 0 {
		if recorderTransactions != 0 || recorderNanos != 0 || recorderFullTransactions != 0 || recorderFullNanos != 0 {
			t.Fatalf("sampled recorder-only capture total/full transactions/nanos = %d/%d %d/%d, want zero", recorderTransactions, recorderNanos, recorderFullTransactions, recorderFullNanos)
		}
	} else if recorderTransactions != int64(len(transactions)) || recorderNanos <= 0 || recorderFullTransactions != fullCaptured || recorderFullNanos <= 0 {
		t.Fatalf("ordinary recorder-only capture total/full transactions/nanos = %d/%d %d/%d, want %d/>0 %d/>0", recorderTransactions, recorderNanos, recorderFullTransactions, recorderFullNanos, len(transactions), fullCaptured)
	}
	filteredEmpty := versionedShadowWriteCaptureFilteredEmptyCounter.Snapshot().Count() - captureFilteredEmptyBefore
	if blockNumber%discardShadowSampleInterval == 0 && filteredEmpty != 0 {
		t.Fatalf("sampled filtered empty captures = %d, want 0", filteredEmpty)
	}
	if filteredCaptured == 0 && filteredEmpty != 0 {
		t.Fatalf("zero filtered captures produced %d empty captures", filteredEmpty)
	}
	if unsupported := versionedShadowWriteCaptureUnsupportedCounter.Snapshot().Count() - captureUnsupportedBefore; unsupported != 0 {
		t.Fatalf("write capture unsupported = %d, want 0", unsupported)
	}
	if captureErrors := versionedShadowWriteCaptureErrorsCounter.Snapshot().Count() - captureErrorsBefore; captureErrors != 0 {
		t.Fatalf("write capture errors = %d, want 0", captureErrors)
	}
	for txIndex := range serialInfos {
		if !proto.Equal(serialInfos[txIndex], parallelInfos[txIndex]) {
			t.Fatalf("tx %d info mismatch\nserial=%v\nparallel=%v", txIndex, serialInfos[txIndex], parallelInfos[txIndex])
		}
	}
	serialRoot, err := serialState.Commit()
	if err != nil {
		t.Fatal(err)
	}
	parallelRoot, err := parallelState.Commit()
	if err != nil {
		t.Fatal(err)
	}
	if serialRoot != parallelRoot {
		t.Fatalf("state roots differ: serial=%x parallel=%x", serialRoot, parallelRoot)
	}
}

func TestProcessBlockReincarnatesSenderRetryAfterLaterConflict(t *testing.T) {
	statedb := newTestState(t)
	for _, id := range []byte{1, 2, 3, 4, 5, 6, 7} {
		statedb.CreateAccount(testProcessorAddr(id), corepb.AccountType_Normal)
	}
	statedb.AddBalance(testProcessorAddr(1), 20_000_000)
	statedb.AddBalance(testProcessorAddr(6), 10_000_000)
	statedb.AddBalance(testProcessorAddr(7), 10_000_000)
	if _, err := statedb.Commit(); err != nil {
		t.Fatal(err)
	}
	transactions := []*types.Transaction{
		makeTestTransferTx(1, 2, 1_000_000),
		makeTestTransferTx(6, 1, 2_000_000),
		makeTestTransferTx(1, 3, 3_000_000),
		makeTestTransferTx(7, 1, 2_000_000),
		makeTestTransferTx(1, 4, 4_000_000),
		makeTestTransferTx(1, 5, 1_000_000),
	}
	transactionProtos := make([]*corepb.Transaction, len(transactions))
	for index, tx := range transactions {
		transactionProtos[index] = tx.Proto()
	}
	block := types.NewBlockFromPB(&corepb.Block{
		BlockHeader: &corepb.BlockHeader{RawData: &corepb.BlockHeaderRaw{
			Number: int64(discardShadowAsyncRetryInterval), Timestamp: 3_000,
		}},
		Transactions: transactionProtos,
	})
	attemptsBefore := discardShadowRetryAttemptsCounter.Snapshot().Count()
	executedBefore := discardShadowRetryExecutedCounter.Snapshot().Count()
	candidatesBefore := discardShadowRetryCandidatesCounter.Snapshot().Count()
	recoveredBefore := discardShadowRetryRecoveredCounter.Snapshot().Count()
	validatedBefore := discardShadowRetryValidatedCounter.Snapshot().Count()
	prefixRefreshBefore := discardShadowRetryPrefixRefreshCounter.Snapshot().Count()
	prefixReuseBefore := discardShadowRetryPrefixReuseCounter.Snapshot().Count()
	prefixAdvanceBefore := discardShadowRetryPrefixAdvanceCounter.Snapshot().Count()
	mismatchBefore := discardShadowRetryInfoMismatchCounter.Snapshot().Count() +
		discardShadowRetryWriteMismatchCounter.Snapshot().Count() +
		discardShadowRetryBalanceMismatchCounter.Snapshot().Count() +
		discardShadowRetryErrorsCounter.Snapshot().Count()
	if _, _, err := processBlockWithOptions(
		statedb, statedb.DynamicProperties(), block, nil, nil, 0,
		params.DefaultBlockNumForEnergyLimit, false, tcommon.Hash{}, nil, nil,
		nil, forks.NewVersionPassCache(), new(transactionInfoBatch), true, -1, nil,
		processBlockOptions{parallelTransfers: true},
	); err != nil {
		t.Fatalf("process reincarnated sender retry: %v", err)
	}
	if attempts := discardShadowRetryAttemptsCounter.Snapshot().Count() - attemptsBefore; attempts != 2 {
		t.Fatalf("sender retry attempts = %d, want 2", attempts)
	}
	if executed := discardShadowRetryExecutedCounter.Snapshot().Count() - executedBefore; executed != 5 {
		t.Fatalf("sender retry executions = %d, want 5", executed)
	}
	if candidates := discardShadowRetryCandidatesCounter.Snapshot().Count() - candidatesBefore; candidates != 3 {
		t.Fatalf("sender retry candidates = %d, want 3", candidates)
	}
	if recovered := discardShadowRetryRecoveredCounter.Snapshot().Count() - recoveredBefore; recovered != 3 {
		t.Fatalf("sender retry recovered = %d, want 3", recovered)
	}
	if validated := discardShadowRetryValidatedCounter.Snapshot().Count() - validatedBefore; validated != 3 {
		t.Fatalf("sender retry validated = %d, want 3", validated)
	}
	if refreshes := discardShadowRetryPrefixRefreshCounter.Snapshot().Count() - prefixRefreshBefore; refreshes != 1 {
		t.Fatalf("sender retry prefix refreshes = %d, want 1", refreshes)
	}
	if reuses := discardShadowRetryPrefixReuseCounter.Snapshot().Count() - prefixReuseBefore; reuses != 1 {
		t.Fatalf("sender retry prefix reuses = %d, want 1", reuses)
	}
	if advances := discardShadowRetryPrefixAdvanceCounter.Snapshot().Count() - prefixAdvanceBefore; advances != 2 {
		t.Fatalf("sender retry prefix advances = %d, want 2", advances)
	}
	mismatchAfter := discardShadowRetryInfoMismatchCounter.Snapshot().Count() +
		discardShadowRetryWriteMismatchCounter.Snapshot().Count() +
		discardShadowRetryBalanceMismatchCounter.Snapshot().Count() +
		discardShadowRetryErrorsCounter.Snapshot().Count()
	if mismatches := mismatchAfter - mismatchBefore; mismatches != 0 {
		t.Fatalf("sender retry mismatches/errors = %d, want 0", mismatches)
	}
	if balance := statedb.GetBalance(testProcessorAddr(1)); balance != 15_000_000 {
		t.Fatalf("owner balance = %d, want 15000000", balance)
	}
}

func TestProcessBlockPublishesVersionedSenderChain(t *testing.T) {
	base := newTestState(t)
	for _, id := range []byte{1, 2, 3} {
		base.CreateAccount(testProcessorAddr(id), corepb.AccountType_Normal)
	}
	base.AddBalance(testProcessorAddr(1), 10_000_000)
	if _, err := base.Commit(); err != nil {
		t.Fatal(err)
	}
	serialState, err := base.Copy()
	if err != nil {
		t.Fatal(err)
	}
	serialState.SetDynamicProperties(base.DynamicProperties().Copy())
	parallelState, err := base.Copy()
	if err != nil {
		t.Fatal(err)
	}
	parallelState.SetDynamicProperties(base.DynamicProperties().Copy())
	transactions := []*types.Transaction{
		makeTestTransferTx(1, 2, 1_000_000),
		makeTestTransferTx(1, 3, 2_000_000),
	}
	block := types.NewBlockFromPB(&corepb.Block{
		BlockHeader:  &corepb.BlockHeader{RawData: &corepb.BlockHeaderRaw{Number: 1, Timestamp: 3_000}},
		Transactions: []*corepb.Transaction{transactions[0].Proto(), transactions[1].Proto()},
	})
	run := func(statedb *state.StateDB, options processBlockOptions) ([]*corepb.TransactionInfo, error) {
		infos, _, processErr := processBlockWithOptions(
			statedb, statedb.DynamicProperties(), block, nil, nil, 0,
			params.DefaultBlockNumForEnergyLimit, false, tcommon.Hash{}, nil, nil,
			nil, forks.NewVersionPassCache(), new(transactionInfoBatch), true, -1, nil,
			options,
		)
		return infos, processErr
	}
	serialInfos, err := run(serialState, processBlockOptions{})
	if err != nil {
		t.Fatalf("serial process: %v", err)
	}
	publishedBefore := parallelTransferPublishedCounter.Snapshot().Count()
	conflictsBefore := parallelTransferConflictFallbackCounter.Snapshot().Count()
	chainPreexecutedBefore := parallelTransferChainPreexecutedCounter.Snapshot().Count()
	chainCandidatesBefore := parallelTransferChainCandidatesCounter.Snapshot().Count()
	chainPublishedBefore := parallelTransferChainPublishedCounter.Snapshot().Count()
	parallelInfos, err := run(parallelState, processBlockOptions{parallelTransfers: true})
	if err != nil {
		t.Fatalf("parallel process: %v", err)
	}
	if published := parallelTransferPublishedCounter.Snapshot().Count() - publishedBefore; published != 2 {
		t.Fatalf("published sender-chain transfers = %d, want 2", published)
	}
	if conflicts := parallelTransferConflictFallbackCounter.Snapshot().Count() - conflictsBefore; conflicts != 0 {
		t.Fatalf("sender-chain conflict fallbacks = %d, want 0", conflicts)
	}
	if preexecuted := parallelTransferChainPreexecutedCounter.Snapshot().Count() - chainPreexecutedBefore; preexecuted != 1 {
		t.Fatalf("preexecuted sender-chain dependents = %d, want 1", preexecuted)
	}
	if candidates := parallelTransferChainCandidatesCounter.Snapshot().Count() - chainCandidatesBefore; candidates != 1 {
		t.Fatalf("sender-chain candidates = %d, want 1", candidates)
	}
	if published := parallelTransferChainPublishedCounter.Snapshot().Count() - chainPublishedBefore; published != 1 {
		t.Fatalf("published sender-chain dependents = %d, want 1", published)
	}
	for txIndex := range serialInfos {
		if !proto.Equal(serialInfos[txIndex], parallelInfos[txIndex]) {
			t.Fatalf("tx %d info mismatch\nserial=%v\nparallel=%v", txIndex, serialInfos[txIndex], parallelInfos[txIndex])
		}
	}
	for _, id := range []byte{1, 2, 3} {
		address := testProcessorAddr(id)
		if serial, parallel := serialState.GetBalance(address), parallelState.GetBalance(address); serial != parallel {
			t.Fatalf("account %d balance serial=%d parallel=%d", id, serial, parallel)
		}
	}
	serialRoot, err := serialState.Commit()
	if err != nil {
		t.Fatal(err)
	}
	parallelRoot, err := parallelState.Commit()
	if err != nil {
		t.Fatal(err)
	}
	if serialRoot != parallelRoot {
		t.Fatalf("state roots differ: serial=%x parallel=%x", serialRoot, parallelRoot)
	}
}

func TestProcessBlockParallelTransfersFallsBackWhenPublicNetLimitIsExhausted(t *testing.T) {
	base := newTestState(t)
	for _, id := range []byte{1, 2, 3, 4} {
		base.CreateAccount(testProcessorAddr(id), corepb.AccountType_Normal)
	}
	base.AddBalance(testProcessorAddr(1), 10_000_000)
	base.AddBalance(testProcessorAddr(3), 20_000_000)
	base.DynamicProperties().SetPublicNetLimit(150)
	if _, err := base.Commit(); err != nil {
		t.Fatal(err)
	}
	serialState, err := base.Copy()
	if err != nil {
		t.Fatal(err)
	}
	serialState.SetDynamicProperties(base.DynamicProperties().Copy())
	parallelState, err := base.Copy()
	if err != nil {
		t.Fatal(err)
	}
	parallelState.SetDynamicProperties(base.DynamicProperties().Copy())
	transactions := []*types.Transaction{
		makeTestTransferTx(1, 2, 1_000_000),
		makeTestTransferTx(3, 4, 2_000_000),
	}
	block := types.NewBlockFromPB(&corepb.Block{
		BlockHeader:  &corepb.BlockHeader{RawData: &corepb.BlockHeaderRaw{Number: 1, Timestamp: 3_000}},
		Transactions: []*corepb.Transaction{transactions[0].Proto(), transactions[1].Proto()},
	})
	run := func(statedb *state.StateDB, options processBlockOptions) ([]*corepb.TransactionInfo, error) {
		infos, _, processErr := processBlockWithOptions(
			statedb, statedb.DynamicProperties(), block, nil, nil, 0,
			params.DefaultBlockNumForEnergyLimit, false, tcommon.Hash{}, nil, nil,
			nil, forks.NewVersionPassCache(), new(transactionInfoBatch), true, -1, nil,
			options,
		)
		return infos, processErr
	}
	serialInfos, err := run(serialState, processBlockOptions{})
	if err != nil {
		t.Fatalf("serial process: %v", err)
	}
	publishedBefore := parallelTransferPublishedCounter.Snapshot().Count()
	limitFallbackBefore := parallelTransferPublicNetLimitFallbackCounter.Snapshot().Count()
	parallelInfos, err := run(parallelState, processBlockOptions{parallelTransfers: true})
	if err != nil {
		t.Fatalf("parallel process: %v", err)
	}
	if published := parallelTransferPublishedCounter.Snapshot().Count() - publishedBefore; published != 1 {
		t.Fatalf("published transfers = %d, want 1", published)
	}
	if fallbacks := parallelTransferPublicNetLimitFallbackCounter.Snapshot().Count() - limitFallbackBefore; fallbacks != 1 {
		t.Fatalf("public-net limit fallbacks = %d, want 1", fallbacks)
	}
	for i := range serialInfos {
		if !proto.Equal(serialInfos[i], parallelInfos[i]) {
			t.Fatalf("tx %d info mismatch\nserial=%v\nparallel=%v", i, serialInfos[i], parallelInfos[i])
		}
	}
	if serial, parallel := serialState.DynamicProperties().PublicNetUsage(), parallelState.DynamicProperties().PublicNetUsage(); serial != parallel {
		t.Fatalf("public net usage serial=%d parallel=%d", serial, parallel)
	}
	if serial, parallel := serialState.DynamicProperties().TotalTransactionCost(), parallelState.DynamicProperties().TotalTransactionCost(); serial != parallel {
		t.Fatalf("transaction cost serial=%d parallel=%d", serial, parallel)
	}
	serialRoot, err := serialState.Commit()
	if err != nil {
		t.Fatal(err)
	}
	parallelRoot, err := parallelState.Commit()
	if err != nil {
		t.Fatal(err)
	}
	if serialRoot != parallelRoot {
		t.Fatalf("state roots differ: serial=%x parallel=%x", serialRoot, parallelRoot)
	}
}

func makeTestTriggerTx(owner byte, contractAddr tcommon.Address, data []byte) *types.Transaction {
	tsc := &contractpb.TriggerSmartContract{
		OwnerAddress:    testProcessorAddr(owner).Bytes(),
		ContractAddress: contractAddr.Bytes(),
		Data:            data,
	}
	param, _ := anypb.New(tsc)
	return types.NewTransactionFromPB(&corepb.Transaction{
		RawData: &corepb.TransactionRaw{
			Expiration: 60_000,
			Contract: []*corepb.Transaction_Contract{{
				Type:      corepb.Transaction_Contract_TriggerSmartContract,
				Parameter: param,
			}},
		},
	})
}

func makeTestProposalCreateTx(owner tcommon.Address, params map[int64]int64) *types.Transaction {
	pc := &contractpb.ProposalCreateContract{
		OwnerAddress: owner.Bytes(),
		Parameters:   params,
	}
	param, _ := anypb.New(pc)
	return types.NewTransactionFromPB(&corepb.Transaction{
		RawData: &corepb.TransactionRaw{
			Expiration: 60_000,
			Contract: []*corepb.Transaction_Contract{{
				Type:      corepb.Transaction_Contract_ProposalCreateContract,
				Parameter: param,
			}},
		},
	})
}

func TestApplyTransaction_Transfer(t *testing.T) {
	statedb := newTestState(t)
	dynProps := state.NewDynamicProperties()

	statedb.CreateAccount(testProcessorAddr(1), corepb.AccountType_Normal)
	statedb.AddBalance(testProcessorAddr(1), 1_000_000)
	// Pre-create the recipient so this stays on the regular bandwidth path.
	statedb.CreateAccount(testProcessorAddr(2), corepb.AccountType_Normal)

	tx := makeTestTransferTx(1, 2, 300_000)
	result, err := ApplyTransaction(statedb, dynProps, tx, 3000, 3000, 1, nil, nil, true, false)
	if err != nil {
		t.Fatal(err)
	}
	if result.Fee != 0 {
		t.Fatalf("fee: got %d, want 0", result.Fee)
	}
	if got := statedb.GetBalance(testProcessorAddr(1)); got != 700_000 {
		t.Fatalf("sender: got %d, want 700000", got)
	}
	if got := statedb.GetBalance(testProcessorAddr(2)); got != 300_000 {
		t.Fatalf("recipient: got %d, want 300000", got)
	}
}

func TestApplyTransaction_ValidationFails(t *testing.T) {
	statedb := newTestState(t)
	dynProps := state.NewDynamicProperties()

	// No account seeded — validation should fail
	tx := makeTestTransferTx(1, 2, 100)
	_, err := ApplyTransaction(statedb, dynProps, tx, 3000, 3000, 1, nil, nil, true, false)
	if err == nil {
		t.Fatal("expected validation error")
	}
}

func TestApplyTransaction_SurfacesCorruptRootedAccount(t *testing.T) {
	disk := ethrawdb.NewMemoryDatabase()
	statedb, err := state.New(tcommon.Hash(ethtypes.EmptyRootHash), state.NewDatabase(disk))
	if err != nil {
		t.Fatal(err)
	}
	owner, recipient := testProcessorAddr(1), testProcessorAddr(2)
	statedb.CreateAccount(owner, corepb.AccountType_Normal)
	statedb.AddBalance(owner, 1_000_000)
	if err := tronrawdb.WriteStateAccountLatest(disk, recipient, []byte{0x80}); err != nil {
		t.Fatal(err)
	}

	_, err = ApplyTransaction(statedb, state.NewDynamicProperties(), makeTestTransferTx(1, 2, 100), 3000, 3000, 1, nil, nil, true, false)
	if err == nil || !strings.Contains(err.Error(), "rooted state access failed") || !strings.Contains(err.Error(), "decode rooted account envelope") {
		t.Fatalf("apply error = %v, want rooted account corruption", err)
	}
}

func TestApplyTransactionRejectsTruncatedLegacyAssetForNewRecipient(t *testing.T) {
	const tokenID = int64(1_000_001)
	assetName := []byte("TRUNC")
	owner, recipient := testProcessorAddr(1), testProcessorAddr(2)
	statedb := newTestState(t)
	statedb.CreateAccount(owner, corepb.AccountType_Normal)
	statedb.AddBalance(owner, 10_000_000)
	statedb.SetTRC10BalanceLegacyAndV2(owner, assetName, tokenID, 500)
	asset := &contractpb.AssetIssueContract{
		Id:           "1000001",
		OwnerAddress: owner.Bytes(),
		Name:         assetName,
	}
	if err := statedb.WriteAssetIssueByName(assetName, asset); err != nil {
		t.Fatal(err)
	}
	legacyKey := append([]byte{0x02}, assetName...)
	raw, ok, err := statedb.SystemKVGet(kvdomains.SystemAsset, legacyKey)
	if err != nil || !ok || len(raw) < 2 {
		t.Fatalf("legacy metadata: ok=%v len=%d err=%v", ok, len(raw), err)
	}
	if err := statedb.SystemKVPut(kvdomains.SystemAsset, legacyKey, append([]byte(nil), raw[:len(raw)-1]...)); err != nil {
		t.Fatal(err)
	}

	_, err = ApplyTransaction(statedb, state.NewDynamicProperties(), makeTestTransferAssetTx(1, 2, assetName, 100), 3000, 3000, 1, nil, nil, true, false)
	if err == nil || !strings.Contains(err.Error(), "rooted state access failed") || !strings.Contains(err.Error(), "decode legacy asset issue") {
		t.Fatalf("apply error = %v, want truncated asset corruption", err)
	}
	if statedb.AccountExists(recipient) {
		t.Fatal("failed transfer created the recipient")
	}
}

func TestApplyTransactionRejectsCorruptExistingWitness(t *testing.T) {
	owner := testProcessorAddr(1)
	statedb := newTestState(t)
	statedb.CreateAccount(owner, corepb.AccountType_Normal)
	statedb.AddBalance(owner, 10_000_000)
	key := tronrawdb.WitnessCapsuleStateKey(owner)
	corrupt := []byte{0x80}
	if err := statedb.SetAccountKV(owner, kvdomains.WitnessCapsule, key, corrupt); err != nil {
		t.Fatal(err)
	}
	contract := &contractpb.WitnessCreateContract{OwnerAddress: owner.Bytes(), Url: []byte("https://witness.invalid")}
	param, _ := anypb.New(contract)
	tx := types.NewTransactionFromPB(&corepb.Transaction{RawData: &corepb.TransactionRaw{
		Expiration: 60_000,
		Contract: []*corepb.Transaction_Contract{{
			Type:      corepb.Transaction_Contract_WitnessCreateContract,
			Parameter: param,
		}},
	}})

	_, err := ApplyTransaction(statedb, state.NewDynamicProperties(), tx, 3000, 3000, 1, nil, nil, true, false)
	if err == nil || !strings.Contains(err.Error(), "rooted state access failed") || !strings.Contains(err.Error(), "read witness") {
		t.Fatalf("apply error = %v, want witness corruption", err)
	}
	raw, ok, readErr := statedb.GetAccountKV(owner, kvdomains.WitnessCapsule, key)
	if readErr != nil || !ok || !bytes.Equal(raw, corrupt) {
		t.Fatalf("witness row after rejection = %x ok=%v err=%v", raw, ok, readErr)
	}
}

func TestApplyTransaction_InBlockPreConsensusSkipsResultSizeGate(t *testing.T) {
	run := func(consensusLogicOptimization bool) error {
		statedb := newTestState(t)
		dynProps := state.NewDynamicProperties()
		dynProps.SetConsensusLogicOptimization(consensusLogicOptimization)

		statedb.CreateAccount(testProcessorAddr(1), corepb.AccountType_Normal)
		statedb.AddBalance(testProcessorAddr(1), 20_000_000)
		statedb.CreateAccount(testProcessorAddr(2), corepb.AccountType_Normal)

		tx := makeTestTransferTx(1, 2, 1)
		tx.Proto().RawData.Expiration = 1001
		padTxDataToLargestValidSize(t, tx)

		_, err := applyTransaction(
			statedb, dynProps, tx, 1000, true, HeadSlot(1000, 0), 2000, 1,
			nil, nil, params.DefaultBlockNumForEnergyLimit, tcommon.Hash{}, tcommon.Address{}, true, false, true, nil, nil,
		)
		return err
	}

	if err := run(false); err != nil {
		t.Fatalf("pre-consensus in-block transaction rejected: %v", err)
	}
	if err := run(true); !errors.Is(err, ErrTransactionTooLarge) {
		t.Fatalf("expected post-consensus result-size rejection, got %v", err)
	}
}

// TestApplyTransaction_InBlockExpirationLowerBound pins java Manager.validateCommon's
// in-block expiration LOWER bound (active once consensus_logic_optimization is on):
// the tx must not be expired as of the next block slot. With prevBlockTime=1000 and
// StateFlag=0, nextSlotTime = 1000 + 1*3000 = 4000, so an expiration in (1000, 4000)
// is accepted with the flag off but rejected with it on.
func TestApplyTransaction_InBlockExpirationLowerBound(t *testing.T) {
	run := func(clo bool, expiration int64) error {
		statedb := newTestState(t)
		dynProps := state.NewDynamicProperties()
		dynProps.SetConsensusLogicOptimization(clo)
		statedb.CreateAccount(testProcessorAddr(1), corepb.AccountType_Normal)
		statedb.AddBalance(testProcessorAddr(1), 20_000_000)
		statedb.CreateAccount(testProcessorAddr(2), corepb.AccountType_Normal)

		tx := makeTestTransferTx(1, 2, 1)
		tx.Proto().RawData.Expiration = expiration
		_, err := applyTransaction(
			statedb, dynProps, tx, 1000, true, HeadSlot(1000, 0), 2000, 1,
			nil, nil, params.DefaultBlockNumForEnergyLimit, tcommon.Hash{}, tcommon.Address{}, true, false, true, nil, nil,
		)
		return err
	}

	if err := run(false, 2000); err != nil {
		t.Fatalf("CLO off: sub-slot expiration must pass (base bound only), got %v", err)
	}
	if err := run(true, 2000); !errors.Is(err, ErrTransactionExpiration) {
		t.Fatalf("CLO on: expiration < nextSlotTime must be rejected, got %v", err)
	}
	if err := run(true, 5000); err != nil {
		t.Fatalf("CLO on: expiration >= nextSlotTime must pass, got %v", err)
	}
}

// TestApplyTransaction_RejectsOversizedResult pins java BandwidthProcessor
// .consume's always-on (in-block) getResultSerializedSize() > 64*contractCount
// reject. A normal/no ret passes; a result padded past 64 bytes is rejected.
func TestApplyTransaction_RejectsOversizedResult(t *testing.T) {
	run := func(orderIDLen int) error {
		statedb := newTestState(t)
		dynProps := state.NewDynamicProperties()
		statedb.CreateAccount(testProcessorAddr(1), corepb.AccountType_Normal)
		statedb.AddBalance(testProcessorAddr(1), 20_000_000)
		statedb.CreateAccount(testProcessorAddr(2), corepb.AccountType_Normal)

		tx := makeTestTransferTx(1, 2, 1)
		if orderIDLen > 0 {
			tx.Proto().Ret = []*corepb.Transaction_Result{{OrderId: make([]byte, orderIDLen)}}
		}
		_, err := applyTransaction(
			statedb, dynProps, tx, 1000, true, HeadSlot(1000, 0), 2000, 1,
			nil, nil, params.DefaultBlockNumForEnergyLimit, tcommon.Hash{}, tcommon.Address{}, true, false, true, nil, nil,
		)
		return err
	}

	if err := run(0); err != nil {
		t.Fatalf("no ret: expected accept, got %v", err)
	}
	if err := run(100); !errors.Is(err, ErrTransactionResultTooLarge) {
		t.Fatalf("oversized ret (100-byte OrderId > 64): expected ErrTransactionResultTooLarge, got %v", err)
	}
}

func TestProcessBlock_WithTransactions(t *testing.T) {
	statedb := newTestState(t)
	dynProps := state.NewDynamicProperties()

	statedb.CreateAccount(testProcessorAddr(1), corepb.AccountType_Normal)
	statedb.AddBalance(testProcessorAddr(1), 10_000_000)
	statedb.CreateAccount(testProcessorAddr(2), corepb.AccountType_Normal)
	statedb.CreateAccount(testProcessorAddr(3), corepb.AccountType_Normal)

	// Commit the initial state so we have a clean base
	_, err := statedb.Commit()
	if err != nil {
		t.Fatal(err)
	}

	tx1 := makeTestTransferTx(1, 2, 1_000_000)
	tx2 := makeTestTransferTx(1, 3, 2_000_000)

	witnessAddr := testProcessorAddr(0xFF)
	// Witnesses always have an account in practice (created before becoming witness)
	statedb.CreateAccount(witnessAddr, corepb.AccountType_Normal)

	block := types.NewBlockFromPB(&corepb.Block{
		BlockHeader: &corepb.BlockHeader{
			RawData: &corepb.BlockHeaderRaw{
				Number:         1,
				Timestamp:      3000,
				WitnessAddress: witnessAddr.Bytes(),
			},
		},
		Transactions: []*corepb.Transaction{tx1.Proto(), tx2.Proto()},
	})

	txInfos, err := ProcessBlock(statedb, dynProps, block, nil, nil, 0, false)
	if err != nil {
		t.Fatal(err)
	}
	_ = txInfos

	// Verify: sender lost 3M, recipients got 1M and 2M
	if got := statedb.GetBalance(testProcessorAddr(1)); got != 7_000_000 {
		t.Fatalf("sender: got %d, want 7000000", got)
	}
	if got := statedb.GetBalance(testProcessorAddr(2)); got != 1_000_000 {
		t.Fatalf("recipient 2: got %d, want 1000000", got)
	}
	if got := statedb.GetBalance(testProcessorAddr(3)); got != 2_000_000 {
		t.Fatalf("recipient 3: got %d, want 2000000", got)
	}

	// Verify witness reward
	reward := dynProps.WitnessPayPerBlock()
	if got := statedb.GetAllowance(witnessAddr); got != reward {
		t.Fatalf("witness reward: got %d, want %d", got, reward)
	}
}

func TestProcessBlock_PassesGenesisHashToProposalValidation(t *testing.T) {
	nileGenesisHash := tcommon.HexToHash("0000000000000000d698d4192c56cb6be724a558448e2684802de4d6cd8690dc")
	type historicalProposalCase struct {
		name        string
		blockNumber int64
		proposal    map[int64]int64
	}
	cases := []historicalProposalCase{
		{
			name:        "shielded transaction",
			blockNumber: 1_628_391,
			proposal:    map[int64]int64{27: 1},
		},
		{
			name:        "shielded TRC20",
			blockNumber: 6_360_101,
			proposal:    map[int64]int64{39: 1},
		},
	}

	run := func(tc historicalProposalCase, genesisHash tcommon.Hash) error {
		diskdb := ethrawdb.NewMemoryDatabase()
		statedb, err := state.New(tcommon.Hash(ethtypes.EmptyRootHash), state.NewDatabase(diskdb))
		if err != nil {
			t.Fatal(err)
		}
		dynProps := state.NewDynamicProperties()
		owner := testProcessorAddr(1)
		statedb.CreateAccount(owner, corepb.AccountType_Normal)
		statedb.PutWitness(owner, "http://w.com")

		tx := makeTestProposalCreateTx(owner, tc.proposal)
		block := types.NewBlockFromPB(&corepb.Block{
			BlockHeader: &corepb.BlockHeader{
				RawData: &corepb.BlockHeaderRaw{
					Number:    tc.blockNumber,
					Timestamp: 3_000,
				},
			},
			Transactions: []*corepb.Transaction{tx.Proto()},
		})
		_, err = ProcessBlock(statedb, dynProps, block, diskdb, []tcommon.Address{owner}, 0, false, genesisHash)
		return err
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if err := run(tc, tcommon.Hash{}); err == nil {
				t.Fatal("expected historical proposal to fail without the Nile genesis hash")
			}
			if err := run(tc, nileGenesisHash); err != nil {
				t.Fatalf("Nile historical proposal rejected: %v", err)
			}
		})
	}
}

func TestProcessBlock_FailingTxRevertsState(t *testing.T) {
	statedb := newTestState(t)
	dynProps := state.NewDynamicProperties()

	statedb.CreateAccount(testProcessorAddr(1), corepb.AccountType_Normal)
	statedb.AddBalance(testProcessorAddr(1), 100)

	// tx tries to transfer 200 — should fail validation
	tx := makeTestTransferTx(1, 2, 200)

	block := types.NewBlockFromPB(&corepb.Block{
		BlockHeader: &corepb.BlockHeader{
			RawData: &corepb.BlockHeaderRaw{
				Number:    1,
				Timestamp: 3000,
			},
		},
		Transactions: []*corepb.Transaction{tx.Proto()},
	})

	_, err := ProcessBlock(statedb, dynProps, block, nil, nil, 0, false)
	if err == nil {
		t.Fatal("expected error for invalid transaction")
	}

	// Balance should be unchanged
	if got := statedb.GetBalance(testProcessorAddr(1)); got != 100 {
		t.Fatalf("balance should be unchanged: got %d, want 100", got)
	}
}

func TestApplyTransaction_ReturnsResult(t *testing.T) {
	statedb := newTestState(t)
	dynProps := state.NewDynamicProperties()

	statedb.CreateAccount(testProcessorAddr(1), corepb.AccountType_Normal)
	statedb.AddBalance(testProcessorAddr(1), 1_000_000)

	tx := makeTestTransferTx(1, 2, 300_000)
	result, err := ApplyTransaction(statedb, dynProps, tx, 3000, 3000, 1, nil, nil, true, false)
	if err != nil {
		t.Fatal(err)
	}
	if result == nil {
		t.Fatal("expected non-nil result")
	}
	if result.ContractRet != 1 {
		t.Fatalf("expected ContractRet=1, got %d", result.ContractRet)
	}
}

// makeExchangeTransactionTx builds a syntactically valid
// ExchangeTransactionContract transaction. Used by the v33 fork-gated
// reject tests below.
func makeExchangeTransactionTx(owner byte) *types.Transaction {
	tc := &contractpb.ExchangeTransactionContract{
		OwnerAddress: testProcessorAddr(owner).Bytes(),
		ExchangeId:   1,
		TokenId:      []byte("_"),
		Quant:        1,
		Expected:     1,
	}
	param, _ := anypb.New(tc)
	return types.NewTransactionFromPB(&corepb.Transaction{
		RawData: &corepb.TransactionRaw{
			Expiration: 1_700_000_060_000,
			Contract: []*corepb.Transaction_Contract{{
				Type:      corepb.Transaction_Contract_ExchangeTransactionContract,
				Parameter: param,
			}},
		},
	})
}

// TestApplyTransaction_ExchangeRejectedAfterFork seeds the v33 fork bitmap
// at quorum and asserts that an ExchangeTransactionContract is rejected at
// the block-apply path with the master-aligned error string. Mirrors
// java-tron Manager.rejectExchangeTransaction (PR #6507).
func TestApplyTransaction_ExchangeRejectedAfterFork(t *testing.T) {
	statedb := newTestState(t)
	dynProps := state.NewDynamicProperties() // maintenance_time_interval defaults to 21_600_000

	db := ethrawdb.NewMemoryDatabase()
	// Seed v33 votes at quorum: 70% of 27 witnesses = ceil(18.9) = 19.
	stats := make([]byte, 27)
	for i := 0; i < 19; i++ {
		stats[i] = forks.VoteUpgrade
	}
	statedb.WriteForkStats(33, stats)

	tx := makeExchangeTransactionTx(1)
	// blockTime well past the v33 HardForkTime ceiling.
	_, err := ApplyTransaction(statedb, dynProps, tx, 1_700_000_000_000, 1_700_000_000_000, 1, db, nil, false, false)
	if !errors.Is(err, ErrExchangeRejected) {
		t.Fatalf("expected ErrExchangeRejected, got %v", err)
	}
}

func TestApplyTransaction_ExchangeNileUsesVersion34(t *testing.T) {
	run := func(t *testing.T, passedVersion int32) error {
		t.Helper()
		statedb := newTestState(t)
		dynProps := state.NewDynamicProperties()
		db := ethrawdb.NewMemoryDatabase()
		stats := make([]byte, 27)
		for i := 0; i < 22; i++ { // v34 requires ceil(80% * 27) = 22
			stats[i] = forks.VoteUpgrade
		}
		statedb.WriteForkStats(passedVersion, stats)

		_, err := applyTransaction(
			statedb, dynProps, makeExchangeTransactionTx(1),
			1_700_000_000_000, true, 0, 1_700_000_000_000, 1,
			db, nil, params.DefaultBlockNumForEnergyLimit, params.NileGenesisHash,
			tcommon.Address{}, false, false, true, nil, nil,
		)
		return err
	}

	// Historical Nile version 33 was release-v4.8.1, before the exchange
	// disable patch. The transaction may fail later because this focused test
	// does not seed exchange state, but it must not hit the fork rejection.
	if err := run(t, 33); errors.Is(err, ErrExchangeRejected) {
		t.Fatalf("historical Nile v33 must allow exchange transaction, got %v", err)
	}

	if err := run(t, 34); !errors.Is(err, ErrExchangeRejected) {
		t.Fatalf("Nile v34 must reject exchange transaction, got %v", err)
	}
}

// TestApplyTransaction_ExchangePassesPreFork asserts that with no v33
// votes, the early reject does not fire — preserving replay safety for
// historical pre-fork blocks. Whether the actuator itself succeeds is
// unrelated to this gate; the test only locks in that the early-return
// path is gated.
func TestApplyTransaction_ExchangePassesPreFork(t *testing.T) {
	statedb := newTestState(t)
	dynProps := state.NewDynamicProperties()

	db := ethrawdb.NewMemoryDatabase()
	// No fork stats written → PassVersion returns false.

	tx := makeExchangeTransactionTx(1)
	_, err := ApplyTransaction(statedb, dynProps, tx, 1_700_000_000_000, 1_700_000_000_000, 1, db, nil, false, false)
	// The actuator can fail later for unrelated reasons (no exchange
	// state seeded); the only thing we care about here is that the
	// failure mode is NOT the v33 early reject.
	if errors.Is(err, ErrExchangeRejected) {
		t.Fatalf("pre-fork exchange tx must not hit the v33 early reject; got %v", err)
	}
}

func TestProcessBlock_RejectsRetCountGreaterThanContractCountWhenOptimized(t *testing.T) {
	statedb := newTestState(t)
	dynProps := state.NewDynamicProperties()
	dynProps.SetConsensusLogicOptimization(true)

	statedb.CreateAccount(testProcessorAddr(1), corepb.AccountType_Normal)
	statedb.AddBalance(testProcessorAddr(1), 1_000_000)
	statedb.CreateAccount(testProcessorAddr(2), corepb.AccountType_Normal)

	tx := makeTestTransferTx(1, 2, 1)
	tx.Proto().Ret = []*corepb.Transaction_Result{
		{Ret: corepb.Transaction_Result_SUCESS},
		{Ret: corepb.Transaction_Result_SUCESS},
	}

	block := types.NewBlockFromPB(&corepb.Block{
		BlockHeader: &corepb.BlockHeader{
			RawData: &corepb.BlockHeaderRaw{
				Number:    1,
				Timestamp: 3000,
			},
		},
		Transactions: []*corepb.Transaction{tx.Proto()},
	})

	_, err := ProcessBlock(statedb, dynProps, block, nil, nil, 0, false)
	if !errors.Is(err, ErrTransactionRetCount) {
		t.Fatalf("expected ErrTransactionRetCount, got %v", err)
	}
}

func TestProcessBlock_RejectsVMContractRetMismatch(t *testing.T) {
	statedb := newTestState(t)
	dynProps := state.NewDynamicProperties()
	dynProps.SetAllowCreationOfContracts(true)

	owner := testProcessorAddr(1)
	contractAddr := testProcessorAddr(2)
	statedb.CreateAccount(owner, corepb.AccountType_Normal)
	statedb.AddBalance(owner, 100_000_000)
	statedb.SetContract(contractAddr, &contractpb.SmartContract{
		OriginAddress:   owner.Bytes(),
		ContractAddress: contractAddr.Bytes(),
	})
	statedb.SetCode(contractAddr, []byte{0x00}) // STOP

	tsc := &contractpb.TriggerSmartContract{
		OwnerAddress:    owner.Bytes(),
		ContractAddress: contractAddr.Bytes(),
	}
	param, err := anypb.New(tsc)
	if err != nil {
		t.Fatal(err)
	}
	tx := types.NewTransactionFromPB(&corepb.Transaction{
		RawData: &corepb.TransactionRaw{
			Expiration: 60_000,
			FeeLimit:   10_000_000,
			Contract: []*corepb.Transaction_Contract{{
				Type:      corepb.Transaction_Contract_TriggerSmartContract,
				Parameter: param,
			}},
		},
		Ret: []*corepb.Transaction_Result{{
			ContractRet: corepb.Transaction_Result_REVERT,
		}},
	})

	block := types.NewBlockFromPB(&corepb.Block{
		BlockHeader: &corepb.BlockHeader{
			RawData: &corepb.BlockHeaderRaw{
				Number:    1,
				Timestamp: 3000,
			},
		},
		Transactions: []*corepb.Transaction{tx.Proto()},
	})

	_, err = ProcessBlock(statedb, dynProps, block, nil, nil, 0, false)
	if !errors.Is(err, ErrTransactionRetMismatch) {
		t.Fatalf("expected ErrTransactionRetMismatch, got %v", err)
	}
	if got := statedb.GetBalance(owner); got != 100_000_000 {
		t.Fatalf("failed block must roll back state: owner balance got %d, want 100000000", got)
	}
}

func TestProcessBlock_ReturnsTransactionInfos(t *testing.T) {
	statedb := newTestState(t)
	dynProps := state.NewDynamicProperties()

	statedb.CreateAccount(testProcessorAddr(1), corepb.AccountType_Normal)
	statedb.AddBalance(testProcessorAddr(1), 10_000_000)
	_, err := statedb.Commit()
	if err != nil {
		t.Fatal(err)
	}

	tx1 := makeTestTransferTx(1, 2, 1_000_000)
	tx2 := makeTestTransferTx(1, 3, 2_000_000)
	witnessAddr := testProcessorAddr(0xFF)
	statedb.CreateAccount(witnessAddr, corepb.AccountType_Normal)

	block := types.NewBlockFromPB(&corepb.Block{
		BlockHeader: &corepb.BlockHeader{
			RawData: &corepb.BlockHeaderRaw{
				Number:         1,
				Timestamp:      3000,
				WitnessAddress: witnessAddr.Bytes(),
			},
		},
		Transactions: []*corepb.Transaction{tx1.Proto(), tx2.Proto()},
	})

	txInfos, err := ProcessBlock(statedb, dynProps, block, nil, nil, 0, false)
	if err != nil {
		t.Fatal(err)
	}
	if len(txInfos) != 2 {
		t.Fatalf("expected 2 txInfos, got %d", len(txInfos))
	}
	for i, info := range txInfos {
		if info.BlockNumber != 1 {
			t.Fatalf("txInfo[%d] blockNumber: got %d, want 1", i, info.BlockNumber)
		}
		if info.BlockTimeStamp != 3000 {
			t.Fatalf("txInfo[%d] blockTimeStamp: got %d, want 3000", i, info.BlockTimeStamp)
		}
		if len(info.Id) == 0 {
			t.Fatalf("txInfo[%d] has empty ID", i)
		}
	}
	if txInfos[0] == txInfos[1] || txInfos[0].Receipt == txInfos[1].Receipt {
		t.Fatal("transaction info slots alias each other's fixed-size messages")
	}
	secondIDFirstByte := txInfos[1].Id[0]
	txInfos[0].Id[0] ^= 0xff
	if txInfos[1].Id[0] != secondIDFirstByte {
		t.Fatal("transaction IDs share mutable backing storage")
	}
	txInfos[0].ContractResult[0] = []byte{1}
	if len(txInfos[1].ContractResult[0]) != 0 {
		t.Fatal("contract-result cells share mutable backing storage")
	}
	txInfos[0].Receipt.EnergyFee = 99
	if txInfos[1].Receipt.EnergyFee == 99 {
		t.Fatal("resource receipts share mutable backing storage")
	}
}

func TestTransactionInfoSlotOwnsContractAddress(t *testing.T) {
	tx := makeTestTriggerTx(1, testProcessorAddr(2), nil)
	firstAddress := testProcessorAddr(3)
	secondAddress := testProcessorAddr(4)
	sharedResultAddress := firstAddress.Bytes()
	result := &actuator.Result{ContractAddress: sharedResultAddress}
	var firstSlot, secondSlot transactionInfoSlot
	firstInfo := firstSlot.build(tx, result, 1, 3_000, false)

	copy(sharedResultAddress, secondAddress.Bytes())
	secondInfo := secondSlot.build(tx, result, 1, 3_000, false)
	if !bytes.Equal(firstInfo.ContractAddress, firstAddress.Bytes()) {
		t.Fatalf("first slot contract address = %x, want %x", firstInfo.ContractAddress, firstAddress)
	}
	if !bytes.Equal(secondInfo.ContractAddress, secondAddress.Bytes()) {
		t.Fatalf("second slot contract address = %x, want %x", secondInfo.ContractAddress, secondAddress)
	}
}

func TestProcessBlock_CanDiscardTransactionInfosAfterValidation(t *testing.T) {
	statedb := newTestState(t)
	dynProps := state.NewDynamicProperties()

	owner := testProcessorAddr(1)
	receiver := testProcessorAddr(2)
	witness := testProcessorAddr(0xff)
	statedb.CreateAccount(owner, corepb.AccountType_Normal)
	statedb.AddBalance(owner, 10_000_000)
	statedb.CreateAccount(receiver, corepb.AccountType_Normal)
	statedb.CreateAccount(witness, corepb.AccountType_Normal)
	if _, err := statedb.Commit(); err != nil {
		t.Fatal(err)
	}

	tx := makeTestTransferTx(1, 2, 1_000_000)
	block := types.NewBlockFromPB(&corepb.Block{
		BlockHeader: &corepb.BlockHeader{RawData: &corepb.BlockHeaderRaw{
			Number:         1,
			Timestamp:      3000,
			WitnessAddress: witness.Bytes(),
		}},
		Transactions: []*corepb.Transaction{tx.Proto()},
	})
	batch := new(transactionInfoBatch)
	txInfos, _, err := processBlock(
		statedb, dynProps, block, nil, nil, 0,
		params.DefaultBlockNumForEnergyLimit, false, tcommon.Hash{}, nil, nil,
		nil, nil, batch, false, -1, nil,
	)
	if err != nil {
		t.Fatal(err)
	}
	if txInfos != nil {
		t.Fatalf("discarded transaction infos = %+v, want nil", txInfos)
	}
	if len(batch.slots) != 1 {
		t.Fatalf("execution scratch slots = %d, want 1", len(batch.slots))
	}
	if got := statedb.GetBalance(owner); got != 9_000_000 {
		t.Fatalf("owner balance = %d, want 9000000", got)
	}
	if got := statedb.GetBalance(receiver); got != 1_000_000 {
		t.Fatalf("receiver balance = %d, want 1000000", got)
	}
}

func TestBuildTransactionInfo_PackingFee(t *testing.T) {
	tx := makeTestTransferTx(1, 2, 100)

	result := &actuator.Result{
		NetFee:             123,
		NetFeeForBandwidth: true,
		EnergyFee:          456,
		ContractRet:        int32(corepb.Transaction_Result_SUCCESS),
	}
	info := buildTransactionInfo(tx, result, 1, 3000, true)
	if info.PackingFee != 579 {
		t.Fatalf("packingFee: got %d, want 579", info.PackingFee)
	}

	info = buildTransactionInfo(tx, result, 1, 3000, false)
	if info.PackingFee != 0 {
		t.Fatalf("packingFee without support_transaction_fee_pool: got %d, want 0", info.PackingFee)
	}

	result.NetFeeForBandwidth = false
	info = buildTransactionInfo(tx, result, 1, 3000, true)
	if info.PackingFee != 456 {
		t.Fatalf("packingFee must exclude create-account net fee: got %d, want 456", info.PackingFee)
	}

	result.ContractRet = int32(corepb.Transaction_Result_OUT_OF_TIME)
	info = buildTransactionInfo(tx, result, 1, 3000, true)
	if info.PackingFee != 0 {
		t.Fatalf("packingFee must exclude OUT_OF_TIME energy fee: got %d, want 0", info.PackingFee)
	}
}

func TestBuildTransactionInfo_IncludesEmptyVMContractResult(t *testing.T) {
	contractAddr := testProcessorAddr(2)
	tx := makeTestTriggerTx(1, contractAddr, nil)
	result := &actuator.Result{
		ContractRet:           int32(corepb.Transaction_Result_OUT_OF_TIME),
		ContractResult:        []byte{},
		ContractResultPresent: true,
		ResMessage:            []byte("Already Time Out"),
	}

	info := buildTransactionInfo(tx, result, 1, 3000, false)
	if len(info.ContractResult) != 1 {
		t.Fatalf("contractResult entries: got %d, want 1", len(info.ContractResult))
	}
	if len(info.ContractResult[0]) != 0 {
		t.Fatalf("contractResult[0] length: got %d, want 0", len(info.ContractResult[0]))
	}
	if got := info.Receipt.Result; got != corepb.Transaction_Result_OUT_OF_TIME {
		t.Fatalf("receipt result: got %s, want OUT_OF_TIME", got)
	}
	if got := string(info.ResMessage); got != "Already Time Out" {
		t.Fatalf("resMessage: got %q", got)
	}
}

func TestBuildTransactionInfo_NonVMReceiptShapeMatchesJavaTron(t *testing.T) {
	tx := makeTestTransferTx(1, 2, 100)
	result := &actuator.Result{
		ContractRet: int32(corepb.Transaction_Result_SUCCESS),
	}

	info := buildTransactionInfo(tx, result, 1, 3000, false)
	if got := info.Receipt.Result; got != corepb.Transaction_Result_DEFAULT {
		t.Fatalf("receipt result: got %s, want DEFAULT", got)
	}
	if len(info.ContractResult) != 1 {
		t.Fatalf("contractResult entries: got %d, want 1", len(info.ContractResult))
	}
	if len(info.ContractResult[0]) != 0 {
		t.Fatalf("contractResult[0] length: got %d, want 0", len(info.ContractResult[0]))
	}
}

func TestBuildTransactionInfo_VMReceiptAndLogShapeMatchesJavaTron(t *testing.T) {
	contractAddr := testProcessorAddr(2)
	tx := makeTestTriggerTx(1, contractAddr, []byte{0x12, 0x34})
	result := &actuator.Result{
		ContractRet:           int32(corepb.Transaction_Result_SUCCESS),
		EnergyPenaltyTotal:    19,
		ContractResultPresent: true,
		ContractResult:        []byte{0xab},
		ContractAddress:       contractAddr.Bytes(),
		Logs: []vm.Log{{
			Address: contractAddr,
			Data:    []byte{0xcd},
			Topics:  [][]byte{{0x01}},
		}},
		InternalTransactions: []*corepb.InternalTransaction{{
			Hash:              []byte{0x01},
			CallerAddress:     testProcessorAddr(1).Bytes(),
			TransferToAddress: contractAddr.Bytes(),
			CallValueInfo: []*corepb.InternalTransaction_CallValueInfo{{
				CallValue: 7,
			}},
			Note: []byte("call"),
		}},
	}

	info := buildTransactionInfo(tx, result, 1, 3000, false)
	if got := info.Receipt.Result; got != corepb.Transaction_Result_SUCCESS {
		t.Fatalf("receipt result: got %s, want SUCCESS", got)
	}
	if got := info.Receipt.EnergyPenaltyTotal; got != 19 {
		t.Fatalf("receipt energy penalty total: got %d, want 19", got)
	}
	if len(info.ContractResult) != 1 || string(info.ContractResult[0]) != string([]byte{0xab}) {
		t.Fatalf("contractResult: got %x, want ab", info.ContractResult)
	}
	if string(info.ContractAddress) != string(contractAddr.Bytes()) {
		t.Fatalf("contract_address: got %x, want %x", info.ContractAddress, contractAddr.Bytes())
	}
	if len(info.Log) != 1 {
		t.Fatalf("logs: got %d, want 1", len(info.Log))
	}
	wantLogAddress := contractAddr.Bytes()[1:]
	if string(info.Log[0].Address) != string(wantLogAddress) {
		t.Fatalf("log address: got %x, want %x", info.Log[0].Address, wantLogAddress)
	}
	if len(info.Log[0].Topics) != 1 || string(info.Log[0].Topics[0]) != string([]byte{0x01}) {
		t.Fatalf("log topics: got %x, want 01", info.Log[0].Topics)
	}
	if len(info.InternalTransactions) != 1 || !proto.Equal(info.InternalTransactions[0], result.InternalTransactions[0]) {
		t.Fatalf("internal_transactions: got %+v, want exact VM result", info.InternalTransactions)
	}
	if cap(info.InternalTransactions) != len(info.InternalTransactions) {
		t.Fatal("internal transaction view exposes arena spare capacity")
	}
}

func TestFilterTransactionInfoInternalTransactionsMatchesJavaConfig(t *testing.T) {
	makeInfo := func() []*corepb.TransactionInfo {
		return []*corepb.TransactionInfo{{
			InternalTransactions: []*corepb.InternalTransaction{
				{Note: []byte("call")},
				{Note: []byte("freezeForEnergy")},
				{Note: []byte("cancelAllUnfreezeV2"), Extra: `{"BANDWIDTH":1,"ENERGY":2,"TRON_POWER":3}`},
				{Note: []byte("suicide")},
			},
		}}
	}

	t.Run("disabled", func(t *testing.T) {
		infos := makeInfo()
		filterTransactionInfoInternalTransactions(infos, false, true, true)
		if infos[0].InternalTransactions != nil {
			t.Fatalf("disabled persistence retained %+v", infos[0].InternalTransactions)
		}
	})

	t.Run("basic_only", func(t *testing.T) {
		infos := makeInfo()
		filterTransactionInfoInternalTransactions(infos, true, false, true)
		got := infos[0].InternalTransactions
		if len(got) != 2 || string(got[0].Note) != "call" || string(got[1].Note) != "suicide" {
			t.Fatalf("basic filter = %+v, want call/suicide", got)
		}
		if cap(got) != len(got) {
			t.Fatalf("basic filtered view exposes spare capacity: len=%d cap=%d", len(got), cap(got))
		}
	})

	t.Run("featured_without_cancel_details", func(t *testing.T) {
		infos := makeInfo()
		filterTransactionInfoInternalTransactions(infos, true, true, false)
		got := infos[0].InternalTransactions
		if len(got) != 4 || got[2].Extra != "" {
			t.Fatalf("featured filter = %+v, want all records and empty cancel Extra", got)
		}
	})

	t.Run("featured_with_cancel_details", func(t *testing.T) {
		infos := makeInfo()
		filterTransactionInfoInternalTransactions(infos, true, true, true)
		got := infos[0].InternalTransactions
		if len(got) != 4 || got[2].Extra != `{"BANDWIDTH":1,"ENERGY":2,"TRON_POWER":3}` {
			t.Fatalf("featured detailed filter = %+v", got)
		}
	})
}

func TestBuildTransactionInfoMapsEveryActuatorCarrier(t *testing.T) {
	contractAddr := testProcessorAddr(2)
	tx := makeTestTriggerTx(1, contractAddr, []byte{0x12})
	internal := &corepb.InternalTransaction{Hash: []byte{0x44}, Note: []byte("call")}
	detail := &corepb.MarketOrderDetail{
		MakerOrderId:     []byte{0x51},
		TakerOrderId:     []byte{0x52},
		FillSellQuantity: 53,
		FillBuyQuantity:  54,
	}
	result := &actuator.Result{
		Fee:                           101,
		EnergyUsageTotal:              4,
		EnergyPenaltyTotal:            8,
		EnergyUsed:                    1,
		EnergyFee:                     2,
		OriginEnergyUsage:             3,
		NetUsage:                      5,
		NetFee:                        6,
		NetFeeForBandwidth:            true,
		AssetIssueID:                  "7",
		WithdrawAmount:                15,
		UnfreezeAmount:                16,
		WithdrawExpireAmount:          28,
		CancelUnfreezeV2Amount:        map[string]int64{"ENERGY": 29},
		ExchangeReceivedAmount:        18,
		ExchangeInjectAnotherAmount:   19,
		ExchangeWithdrawAnotherAmount: 20,
		ShieldedTransactionFee:        22,
		ExchangeID:                    21,
		OrderID:                       []byte{0x25},
		OrderDetails:                  []*corepb.MarketOrderDetail{detail},
		ContractResult:                []byte{0xaa},
		ContractResultPresent:         true,
		ContractAddress:               contractAddr.Bytes(),
		Logs: []vm.Log{{
			Address: contractAddr,
			Data:    []byte{0xbb},
			Topics:  [][]byte{{0xcc}},
		}},
		InternalTransactions: []*corepb.InternalTransaction{internal},
		ContractRet:          int32(corepb.Transaction_Result_REVERT),
		ResMessage:           []byte("failed"),
	}
	hash := tx.Hash()
	want := &corepb.TransactionInfo{
		Id:              hash[:],
		Fee:             107,
		BlockNumber:     30,
		BlockTimeStamp:  31,
		ContractResult:  [][]byte{{0xaa}},
		ContractAddress: contractAddr.Bytes(),
		Receipt: &corepb.ResourceReceipt{
			EnergyUsage:        1,
			EnergyFee:          2,
			OriginEnergyUsage:  3,
			EnergyUsageTotal:   4,
			NetUsage:           5,
			NetFee:             6,
			Result:             corepb.Transaction_Result_REVERT,
			EnergyPenaltyTotal: 8,
		},
		Log: []*corepb.TransactionInfo_Log{{
			Address: contractAddr.Bytes()[1:],
			Data:    []byte{0xbb},
			Topics:  [][]byte{{0xcc}},
		}},
		Result:                        corepb.TransactionInfo_FAILED,
		ResMessage:                    []byte("failed"),
		AssetIssueID:                  "7",
		WithdrawAmount:                15,
		UnfreezeAmount:                16,
		InternalTransactions:          []*corepb.InternalTransaction{internal},
		ExchangeReceivedAmount:        18,
		ExchangeInjectAnotherAmount:   19,
		ExchangeWithdrawAnotherAmount: 20,
		ExchangeId:                    21,
		ShieldedTransactionFee:        22,
		OrderId:                       []byte{0x25},
		OrderDetails:                  []*corepb.MarketOrderDetail{detail},
		PackingFee:                    8,
		WithdrawExpireAmount:          28,
		CancelUnfreezeV2Amount:        map[string]int64{"ENERGY": 29},
	}
	if got := buildTransactionInfo(tx, result, 30, 31, true); !proto.Equal(got, want) {
		t.Fatalf("transaction info mapping mismatch:\n got: %v\nwant: %v", got, want)
	}
}

func TestTransactionInfoSlotReuseClearsVariableFields(t *testing.T) {
	contractAddr := testProcessorAddr(2)
	tx := makeTestTriggerTx(1, contractAddr, nil)
	slot := new(transactionInfoSlot)

	first := &actuator.Result{
		ContractRet: int32(corepb.Transaction_Result_SUCCESS),
		Logs: []vm.Log{
			{Address: contractAddr, Topics: [][]byte{{0x01}, {0x02}}, Data: []byte{0xa1}},
			{Address: contractAddr, Topics: [][]byte{{0x03}}, Data: []byte{0xa2}},
		},
		InternalTransactions: []*corepb.InternalTransaction{{Note: []byte("a")}},
	}
	info := slot.build(tx, first, 1, 3000, false)
	if len(info.Log) != 2 || len(info.InternalTransactions) != 1 || string(info.InternalTransactions[0].Note) != "a" {
		t.Fatalf("first build shape: logs=%d internal=%v", len(info.Log), info.InternalTransactions)
	}

	info = slot.build(tx, &actuator.Result{ContractRet: int32(corepb.Transaction_Result_SUCCESS)}, 2, 6000, false)
	if info.Log != nil || info.InternalTransactions != nil {
		t.Fatalf("empty reuse retained variable fields: logs=%v internal=%v", info.Log, info.InternalTransactions)
	}

	nonMainnet := tcommon.Address{0x42, 0x11}
	third := &actuator.Result{
		ContractRet: int32(corepb.Transaction_Result_SUCCESS),
		Logs: []vm.Log{{
			Address: nonMainnet,
			Data:    []byte{0xb1},
		}},
		InternalTransactions: []*corepb.InternalTransaction{{Note: []byte("b")}},
	}
	info = slot.build(tx, third, 3, 9000, false)
	if len(info.Log) != 1 || len(info.Log[0].Topics) != 0 || len(info.Log[0].Address) != tcommon.AddressLength {
		t.Fatalf("third build log shape: %+v", info.Log)
	}
	if !bytes.Equal(info.Log[0].Address, nonMainnet[:]) {
		t.Fatalf("non-mainnet log address = %x, want %x", info.Log[0].Address, nonMainnet)
	}
	if len(info.InternalTransactions) != 1 || string(info.InternalTransactions[0].Note) != "b" {
		t.Fatalf("third build internal transactions = %+v, want b", info.InternalTransactions)
	}
	if cap(info.Log) != len(info.Log) || cap(info.InternalTransactions) != len(info.InternalTransactions) {
		t.Fatal("receipt repeated fields expose spare reusable capacity")
	}
}

func TestTransactionInfoVariableFieldsDoNotAliasAcrossSlots(t *testing.T) {
	tx := makeTestTriggerTx(1, testProcessorAddr(2), nil)
	results := [2]*actuator.Result{
		{
			ContractRet: int32(corepb.Transaction_Result_SUCCESS),
			Logs:        []vm.Log{{Address: testProcessorAddr(2), Topics: [][]byte{{0x01}}}},
			InternalTransactions: []*corepb.InternalTransaction{{
				Hash: []byte{0x11}, Note: []byte("first"),
			}},
		},
		{
			ContractRet: int32(corepb.Transaction_Result_SUCCESS),
			Logs:        []vm.Log{{Address: testProcessorAddr(3), Topics: [][]byte{{0x02}}}},
			InternalTransactions: []*corepb.InternalTransaction{{
				Hash: []byte{0x22}, Note: []byte("second"),
			}},
		},
	}
	slots := make([]transactionInfoSlot, 2)
	first := slots[0].build(tx, results[0], 1, 3000, false)
	second := slots[1].build(tx, results[1], 1, 3000, false)
	secondAddress := append([]byte(nil), second.Log[0].Address...)
	first.Log[0].Address[0] ^= 0xff
	if !bytes.Equal(second.Log[0].Address, secondAddress) {
		t.Fatal("receipt log address buffers alias across transaction slots")
	}
	secondInternalHash := append([]byte(nil), second.InternalTransactions[0].Hash...)
	first.InternalTransactions[0].Hash[0] ^= 0xff
	if !bytes.Equal(second.InternalTransactions[0].Hash, secondInternalHash) {
		t.Fatal("receipt internal transactions alias across transaction slots")
	}
}

func TestApplyTransaction_IncludesMemoAndMultiSignFees(t *testing.T) {
	statedb := newTestState(t)
	dp := state.NewDynamicProperties()
	dp.SetAllowMultiSign(true)
	dp.SetMultiSignFee(10)
	dp.SetMemoFee(20)

	owner := testProcessorAddr(1)
	to := testProcessorAddr(2)
	statedb.CreateAccount(owner, corepb.AccountType_Normal)
	statedb.CreateAccount(to, corepb.AccountType_Normal)
	statedb.AddBalance(owner, 1_000_000)

	tx := makeTestTransferTx(1, 2, 100)
	tx.Proto().Signature = [][]byte{make([]byte, 65), make([]byte, 65)}
	tx.Proto().RawData.Data = []byte("memo")

	db := ethrawdb.NewMemoryDatabase()
	result, err := ApplyTransaction(statedb, dp, tx, 0, 3000, 1, db, nil, true, false)
	if err != nil {
		t.Fatalf("ApplyTransaction: %v", err)
	}
	if result.Fee != 30 {
		t.Fatalf("result fee: got %d, want 30", result.Fee)
	}
	info := buildTransactionInfo(tx, result, 1, 3000, false)
	if info.Fee != 30 {
		t.Fatalf("transaction info fee: got %d, want 30", info.Fee)
	}
	if got := statedb.GetBalance(owner); got != 1_000_000-100-30 {
		t.Fatalf("owner balance: got %d, want %d", got, int64(1_000_000-100-30))
	}
}

func TestApplyTransaction_RollsBackPreExecutionFeesOnMemoFailure(t *testing.T) {
	statedb := newTestState(t)
	dp := state.NewDynamicProperties()
	dp.SetAllowMultiSign(true)
	dp.SetMultiSignFee(100)
	dp.SetMemoFee(100)
	dp.SetAllowBlackHoleOptimization(true)

	owner := testProcessorAddr(1)
	to := testProcessorAddr(2)
	statedb.CreateAccount(owner, corepb.AccountType_Normal)
	statedb.CreateAccount(to, corepb.AccountType_Normal)
	statedb.AddBalance(owner, 150)

	tx := makeTestTransferTx(1, 2, 1)
	tx.Proto().Signature = [][]byte{make([]byte, 65), make([]byte, 65)}
	tx.Proto().RawData.Data = []byte("memo")

	db := ethrawdb.NewMemoryDatabase()
	if _, err := ApplyTransaction(statedb, dp, tx, 0, 3000, 1, db, nil, true, false); err == nil {
		t.Fatal("expected memo fee failure")
	}
	if got := statedb.GetBalance(owner); got != 150 {
		t.Fatalf("owner balance should be rolled back, got %d want 150", got)
	}
	if got := dp.BurnTrxAmount(); got != 0 {
		t.Fatalf("burn_trx_amount should be rolled back, got %d", got)
	}
}
