package actuator

import (
	"bytes"
	"testing"

	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/state"
	"github.com/tronprotocol/go-tron/core/state/kvdomains"
	"github.com/tronprotocol/go-tron/core/types"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	contractpb "github.com/tronprotocol/go-tron/proto/core/contract"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
)

func TestPrefetchKeysForTransferDedupesAccountAndContractMetadata(t *testing.T) {
	owner := makeTestAddr(0x01)
	tx := newPrefetchTestTx(t, corepb.Transaction_Contract_TransferContract, &contractpb.TransferContract{
		OwnerAddress: owner.Bytes(),
		ToAddress:    owner.Bytes(),
		Amount:       1,
	})

	keys := PrefetchKeysFor(tx)
	assertPrefetchHas(t, keys, state.AccountPrefetchKey(owner))
	assertPrefetchHas(t, keys, state.ContractMetadataPrefetchKey(owner))
	if len(keys) != 2 {
		t.Fatalf("len(keys) = %d, want 2 deduplicated keys: %#v", len(keys), keys)
	}
}

func TestPrefetchKeysForTriggerSmartContract(t *testing.T) {
	owner := makeTestAddr(0x02)
	contract := makeTestAddr(0x03)
	tx := newPrefetchTestTx(t, corepb.Transaction_Contract_TriggerSmartContract, &contractpb.TriggerSmartContract{
		OwnerAddress:    owner.Bytes(),
		ContractAddress: contract.Bytes(),
	})

	keys := PrefetchKeysFor(tx)
	assertPrefetchHas(t, keys, state.AccountPrefetchKey(owner))
	assertPrefetchHas(t, keys, state.AccountPrefetchKey(contract))
	assertPrefetchHas(t, keys, state.ContractMetadataPrefetchKey(contract))
}

func TestPrefetchKeysForCreateSmartContract(t *testing.T) {
	owner := makeTestAddr(0x04)
	origin := makeTestAddr(0x05)
	tx := newPrefetchTestTx(t, corepb.Transaction_Contract_CreateSmartContract, &contractpb.CreateSmartContract{
		OwnerAddress: owner.Bytes(),
		NewContract: &contractpb.SmartContract{
			OriginAddress: origin.Bytes(),
		},
	})
	created := generateContractAddress(tx, owner)

	keys := PrefetchKeysFor(tx)
	assertPrefetchHas(t, keys, state.AccountPrefetchKey(owner))
	assertPrefetchHas(t, keys, state.AccountPrefetchKey(origin))
	assertPrefetchHas(t, keys, state.AccountPrefetchKey(created))
	assertPrefetchHas(t, keys, state.ContractMetadataPrefetchKey(created))
}

func TestPrefetchKeysForDelegateResourceSystemRows(t *testing.T) {
	owner := makeTestAddr(0x06)
	receiver := makeTestAddr(0x07)
	tx := newPrefetchTestTx(t, corepb.Transaction_Contract_DelegateResourceContract, &contractpb.DelegateResourceContract{
		OwnerAddress:    owner.Bytes(),
		ReceiverAddress: receiver.Bytes(),
	})

	keys := PrefetchKeysFor(tx)
	assertPrefetchHas(t, keys, state.AccountPrefetchKey(owner))
	assertPrefetchHas(t, keys, state.AccountPrefetchKey(receiver))
	assertPrefetchHas(t, keys, systemDelegationPrefetchKey(rawdb.DelegatedResourceV2StateKey(owner, receiver, false)))
	assertPrefetchHas(t, keys, systemDelegationPrefetchKey(rawdb.DelegatedResourceV2StateKey(owner, receiver, true)))
	assertPrefetchHas(t, keys, systemDelegationPrefetchKey(rawdb.DelegationIndexStateKey(owner)))
}

func TestPrefetchKeysForMalformedOrInvalidInputs(t *testing.T) {
	if keys := PrefetchKeysFor(nil); len(keys) != 0 {
		t.Fatalf("nil tx keys = %#v, want none", keys)
	}

	txWithoutContract := types.NewTransactionFromPB(&corepb.Transaction{RawData: &corepb.TransactionRaw{}})
	if keys := PrefetchKeysFor(txWithoutContract); len(keys) != 0 {
		t.Fatalf("tx without contract keys = %#v, want none", keys)
	}

	malformed := types.NewTransactionFromPB(&corepb.Transaction{
		RawData: &corepb.TransactionRaw{Contract: []*corepb.Transaction_Contract{{
			Type: corepb.Transaction_Contract_TransferContract,
		}}},
	})
	if keys := PrefetchKeysFor(malformed); len(keys) != 0 {
		t.Fatalf("malformed tx keys = %#v, want none", keys)
	}

	invalidPrefix := makeTestAddr(0x08)
	invalidPrefix[0] = 0xa0
	validTo := makeTestAddr(0x09)
	invalidAddrTx := newPrefetchTestTx(t, corepb.Transaction_Contract_TransferContract, &contractpb.TransferContract{
		OwnerAddress: invalidPrefix.Bytes(),
		ToAddress:    validTo.Bytes(),
		Amount:       1,
	})
	keys := PrefetchKeysFor(invalidAddrTx)
	assertPrefetchMissing(t, keys, state.AccountPrefetchKey(invalidPrefix))
	assertPrefetchHas(t, keys, state.AccountPrefetchKey(validTo))
}

func newPrefetchTestTx(t *testing.T, typ corepb.Transaction_Contract_ContractType, msg proto.Message) *types.Transaction {
	t.Helper()
	param, err := anypb.New(msg)
	if err != nil {
		t.Fatal(err)
	}
	return types.NewTransactionFromPB(&corepb.Transaction{
		RawData: &corepb.TransactionRaw{
			Timestamp: 1,
			Contract: []*corepb.Transaction_Contract{{
				Type:      typ,
				Parameter: param,
			}},
		},
	})
}

func systemDelegationPrefetchKey(key []byte) state.PrefetchKey {
	return state.AccountKVPrefetchKey(tcommon.SystemAccountAddress, kvdomains.SystemDelegation, key)
}

func assertPrefetchHas(t *testing.T, keys []state.PrefetchKey, want state.PrefetchKey) {
	t.Helper()
	if !prefetchKeySliceContains(keys, want) {
		t.Fatalf("missing prefetch key %#v in %#v", want, keys)
	}
}

func assertPrefetchMissing(t *testing.T, keys []state.PrefetchKey, want state.PrefetchKey) {
	t.Helper()
	if prefetchKeySliceContains(keys, want) {
		t.Fatalf("unexpected prefetch key %#v in %#v", want, keys)
	}
}

func prefetchKeySliceContains(keys []state.PrefetchKey, want state.PrefetchKey) bool {
	for _, got := range keys {
		if got.Kind == want.Kind &&
			got.Owner == want.Owner &&
			got.Domain == want.Domain &&
			got.Slot == want.Slot &&
			got.Generation == want.Generation &&
			got.HasGeneration == want.HasGeneration &&
			bytes.Equal(got.Key, want.Key) {
			return true
		}
	}
	return false
}
