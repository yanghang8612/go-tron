package vm

import (
	"encoding/hex"
	"encoding/json"
	"os"
	"reflect"
	"testing"

	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/state"
	"github.com/tronprotocol/go-tron/core/types"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	contractpb "github.com/tronprotocol/go-tron/proto/core/contract"
)

type javaFrameInput struct {
	Name, Family                          string
	Dynamic, London, Compatibility, Osaka bool
	Version                               int32
	MessageValue                          int64 `json:"messageValue"`
	Energy                                uint64
	Code                                  map[string]string
	Balances                              map[string]int64
	States                                map[string][]int64
}

type javaFrameInternal struct {
	Note     string `json:"note"`
	Rejected bool   `json:"rejected"`
	Value    int64  `json:"value"`
}

type javaFrameOutput struct {
	Return     string              `json:"return"`
	EnergyLeft uint64              `json:"energy_left"`
	Penalty    uint64              `json:"penalty"`
	Nonce      uint64              `json:"nonce"`
	Internal   []javaFrameInternal `json:"internal"`
	States     map[string][]int64  `json:"states"`
	Balances   map[string]int64    `json:"balances"`
	Storage    []string            `json:"storage"`
}

func javaFrameAddress(t *testing.T, encoded string) tcommon.Address {
	t.Helper()
	value, err := hex.DecodeString(encoded)
	if err != nil || len(value) != 21 || value[0] != 0x41 {
		t.Fatalf("invalid fixture address %q: %v", encoded, err)
	}
	return tcommon.BytesToAddress(value)
}

func TestJavaFrameBoundaryParity(t *testing.T) {
	data, err := os.ReadFile("testdata/frame_boundaries/java_golden.json")
	if err != nil {
		t.Fatal(err)
	}
	var corpus struct {
		JavaCommit string `json:"java_commit"`
		Cases      []struct {
			Input  javaFrameInput  `json:"input"`
			Output javaFrameOutput `json:"output"`
		} `json:"cases"`
	}
	if err := json.Unmarshal(data, &corpus); err != nil {
		t.Fatal(err)
	}
	if corpus.JavaCommit != "4e8389f159d0782f1ebcd9dab2fd88388c0e6fe9" || len(corpus.Cases) != 40 {
		t.Fatalf("unrecognized Java VM corpus: commit=%s cases=%d", corpus.JavaCommit, len(corpus.Cases))
	}
	families := make(map[string]int)
	for _, fixture := range corpus.Cases {
		families[fixture.Input.Family]++
	}
	if !reflect.DeepEqual(families, map[string]int{"static": 12, "callcode": 8, "delegate": 2, "create": 18}) {
		t.Fatalf("incomplete Java VM fixture families: %v", families)
	}
	for _, fixture := range corpus.Cases {
		for _, traced := range []bool{false, true} {
			name := "plain/" + fixture.Input.Name
			if traced {
				name = "traced/" + fixture.Input.Name
			}
			t.Run(name, func(t *testing.T) {
				input := fixture.Input
				cfg := TVMConfig{MultiSign: true, Constantinople: true,
					DynamicEnergy: input.Dynamic, London: input.London,
					Compatibility: input.Compatibility, Osaka: input.Osaka}
				if traced {
					cfg.Tracer = &recorderTracer{}
				}
				tvm, st, _ := newTestTVMForCreate(t, cfg, func(dp *state.DynamicProperties) {
					dp.SetCurrentCycleNumber(2)
					dp.SetAllowMultiSign(true)
					dp.SetLatestBlockHeaderTimestamp(1000)
					dp.SetDynamicEnergyThreshold(1_000_000)
					dp.SetDynamicEnergyIncreaseFactor(2000)
					dp.SetDynamicEnergyMaxFactor(34000)
				})
				tvm.SetRootTransactionID(tcommon.Hash{31: 1})
				for encoded, balance := range input.Balances {
					addr := javaFrameAddress(t, encoded)
					st.CreateAccount(addr, corepb.AccountType_Contract)
					st.AddBalance(addr, balance)
					st.SetContract(addr, &contractpb.SmartContract{ContractAddress: addr.Bytes(), Version: input.Version})
				}
				for encoded, codeHex := range input.Code {
					code, err := hex.DecodeString(codeHex)
					if err != nil {
						t.Fatal(err)
					}
					st.SetCode(javaFrameAddress(t, encoded), code)
				}
				for encoded, initial := range input.States {
					if len(initial) != 3 {
						t.Fatal("invalid fixture ContractState")
					}
					cs := types.NewContractState(initial[0])
					cs.AddEnergyUsage(initial[1])
					cs.SetEnergyFactor(initial[2])
					if err := st.WriteContractState(javaFrameAddress(t, encoded), cs); err != nil {
						t.Fatal(err)
					}
				}
				parent, caller := tcommon.Address{0x41, 0xc1}, tcommon.Address{0x41, 0xc0}
				contract := NewContract(caller, parent, input.MessageValue, input.Energy)
				contract.Version = input.Version
				contract.SetCode(parent, st.GetCode(parent))
				ret, err := tvm.runContract(contract)
				if err != nil {
					t.Fatalf("unexpected root failure: %v", err)
				}
				got := javaFrameOutput{Return: hex.EncodeToString(ret), EnergyLeft: contract.Energy,
					Penalty: tvm.EnergyPenaltyTotal, Nonce: tvm.Nonce,
					Internal: []javaFrameInternal{}, States: map[string][]int64{}, Balances: map[string]int64{}, Storage: []string{}}
				for _, tx := range tvm.InternalTransactions {
					row := javaFrameInternal{Note: string(tx.Note), Rejected: tx.Rejected}
					if len(tx.CallValueInfo) != 0 {
						row.Value = tx.CallValueInfo[0].CallValue
					}
					got.Internal = append(got.Internal, row)
				}
				for encoded := range input.Balances {
					addr := javaFrameAddress(t, encoded)
					got.Balances[encoded] = st.GetBalance(addr)
					if cs := st.ReadContractState(addr); cs != nil {
						got.States[encoded] = []int64{cs.UpdateCycle(), cs.EnergyUsage(), cs.EnergyFactor()}
					}
				}
				for slot := byte(0); slot < 2; slot++ {
					word := st.GetState(parent, tcommon.Hash{31: slot})
					got.Storage = append(got.Storage, hex.EncodeToString(word[:]))
				}
				if err := st.Error(); err != nil {
					t.Fatal(err)
				}
				if !reflect.DeepEqual(got, fixture.Output) {
					actual, _ := json.Marshal(got)
					expected, _ := json.Marshal(fixture.Output)
					t.Fatalf("Java VM mismatch:\n got %s\nwant %s", actual, expected)
				}
			})
		}
	}
}
