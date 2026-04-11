package actuator

import (
	"errors"

	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/state"
	"github.com/tronprotocol/go-tron/core/types"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	"github.com/tronprotocol/go-tron/vm"
)

type Context struct {
	State           *state.StateDB
	DynProps        *state.DynamicProperties
	Tx              *types.Transaction
	BlockTime       int64
	BlockNumber     uint64
	DB              ethdb.KeyValueStore // rawdb access for governance/brokerage
	ActiveWitnesses []common.Address // active witness set for governance checks
}

type Result struct {
	Fee               int64
	EnergyUsed        int64
	EnergyFee         int64
	OriginEnergyUsage int64
	NetUsage          int64
	NetFee            int64
	ContractResult    []byte
	ContractAddress   []byte
	Logs              []vm.Log
	ContractRet       int32
}

type Actuator interface {
	Validate(ctx *Context) error
	Execute(ctx *Context) (*Result, error)
}

func CreateActuator(tx *types.Transaction) (Actuator, error) {
	ct := tx.ContractType()
	switch ct {
	case corepb.Transaction_Contract_AssetIssueContract:
		return &AssetIssueActuator{}, nil
	case corepb.Transaction_Contract_TransferContract:
		return &TransferActuator{}, nil
	case corepb.Transaction_Contract_TransferAssetContract:
		return &TransferAssetActuator{}, nil
	case corepb.Transaction_Contract_AccountCreateContract:
		return &CreateAccountActuator{}, nil
	case corepb.Transaction_Contract_WitnessCreateContract:
		return &WitnessCreateActuator{}, nil
	case corepb.Transaction_Contract_FreezeBalanceV2Contract:
		return &FreezeBalanceV2Actuator{}, nil
	case corepb.Transaction_Contract_UnfreezeBalanceV2Contract:
		return &UnfreezeBalanceV2Actuator{}, nil
	case corepb.Transaction_Contract_VoteWitnessContract:
		return &VoteWitnessActuator{}, nil
	case corepb.Transaction_Contract_WithdrawBalanceContract:
		return &WithdrawBalanceActuator{}, nil
	case corepb.Transaction_Contract_WithdrawExpireUnfreezeContract:
		return &WithdrawExpireUnfreezeActuator{}, nil
	case corepb.Transaction_Contract_CreateSmartContract:
		return &VMActuator{}, nil
	case corepb.Transaction_Contract_TriggerSmartContract:
		return &VMActuator{}, nil
	case corepb.Transaction_Contract_WitnessUpdateContract:
		return &WitnessUpdateActuator{}, nil
	case corepb.Transaction_Contract_UpdateSettingContract:
		return &UpdateSettingActuator{}, nil
	case corepb.Transaction_Contract_AccountUpdateContract:
		return &AccountUpdateActuator{}, nil
	case corepb.Transaction_Contract_SetAccountIdContract:
		return &SetAccountIdActuator{}, nil
	case corepb.Transaction_Contract_AccountPermissionUpdateContract:
		return &AccountPermissionUpdateActuator{}, nil
	case corepb.Transaction_Contract_UpdateEnergyLimitContract:
		return &UpdateEnergyLimitActuator{}, nil
	case corepb.Transaction_Contract_UpdateBrokerageContract:
		return &UpdateBrokerageActuator{}, nil
	case corepb.Transaction_Contract_ClearABIContract:
		return &ClearABIActuator{}, nil
	case corepb.Transaction_Contract_ProposalCreateContract:
		return &ProposalCreateActuator{}, nil
	case corepb.Transaction_Contract_ProposalApproveContract:
		return &ProposalApproveActuator{}, nil
	case corepb.Transaction_Contract_ProposalDeleteContract:
		return &ProposalDeleteActuator{}, nil
	case corepb.Transaction_Contract_DelegateResourceContract:
		return &DelegateResourceActuator{}, nil
	case corepb.Transaction_Contract_UnDelegateResourceContract:
		return &UnDelegateResourceActuator{}, nil
	case corepb.Transaction_Contract_CancelAllUnfreezeV2Contract:
		return &CancelAllUnfreezeV2Actuator{}, nil
	case corepb.Transaction_Contract_ParticipateAssetIssueContract:
		return &ParticipateAssetIssueActuator{}, nil
	case corepb.Transaction_Contract_UpdateAssetContract:
		return &UpdateAssetActuator{}, nil
	case corepb.Transaction_Contract_VoteAssetContract:
		return &VoteAssetActuator{}, nil
	default:
		return nil, errors.New("unsupported contract type")
	}
}
