package actuator

import (
	"errors"
	"fmt"

	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/forks"
	"github.com/tronprotocol/go-tron/core/rawdb"
)

const (
	proposalLongValue                    int64  = 100_000_000_000_000_000
	proposalMaxSupply                    int64  = 100_000_000_000
	proposalOneYearBlockNumbers          int64  = 10_512_000
	proposalCreateAccountTxMinSize       int64  = 500
	proposalCreateAccountTxMaxSize       int64  = 10_000
	proposalMinExpireTime                int64  = 0
	proposalMaxExpireTime                int64  = 31_536_003_000
	proposalDynamicEnergyIncreaseMax     int64  = 10_000
	proposalDynamicEnergyMaxFactorMax    int64  = 100_000
	proposalMarketFeeMax                 int64  = 10_000_000_000
	proposalMemoFeeMax                   int64  = 1_000_000_000
	proposalTotalNetLimitMax             int64  = 1_000_000_000_000
	proposalMaintenanceIntervalMinMillis int64  = 3 * 27 * 1000
	proposalMaintenanceIntervalMaxMillis int64  = 24 * 3600 * 1000
	proposalNileShieldedActivationBlock  uint64 = 1_628_391
)

var proposalNileGenesisHash = common.HexToHash("0000000000000000d698d4192c56cb6be724a558448e2684802de4d6cd8690dc")

func validateProposalParameter(ctx *Context, id, value int64) error {
	if ctx == nil || ctx.DynProps == nil {
		return errors.New("dynamic properties are required")
	}
	if forks.ProposalParamKey(id) == "" {
		return fmt.Errorf("bad chain parameter id %d", id)
	}
	if err := validateProposalForkGate(ctx, id); err != nil {
		return err
	}
	dp := ctx.DynProps

	switch id {
	case 0:
		return validateProposalRange(id, value, proposalMaintenanceIntervalMinMillis, proposalMaintenanceIntervalMaxMillis)
	case 1, 2, 3, 4, 5, 6, 7, 8, 17, 19, 31, 73:
		return validateProposalRange(id, value, 0, proposalLongValue)
	case 9, 14, 15, 16, 20, 21, 40, 41, 44, 51, 53, 60, 63, 65, 66, 69, 71, 76:
		return validateProposalOne(id, value)
	case 10:
		if dp.RemoveThePowerOfTheGr() == -1 {
			return errors.New("remove_the_power_of_the_gr proposal has already been executed")
		}
		return validateProposalOne(id, value)
	case 11, 12:
		return nil
	case 13:
		if dp.AllowHigherLimitForMaxCpuTimeOfOneTx() {
			return validateProposalRange(id, value, 10, 400)
		}
		return validateProposalRange(id, value, 10, 100)
	case 18:
		if err := validateProposalOne(id, value); err != nil {
			return err
		}
		return validateProposalRequires(dp.AllowSameTokenName(), "allow_same_token_name", id)
	case 22, 23:
		return validateProposalRange(id, value, 0, proposalMaxSupply)
	case 24, 25, 30, 39, 48, 49:
		return validateProposalZeroOrOne(id, value)
	case 26:
		if err := validateProposalOne(id, value); err != nil {
			return err
		}
		return validateProposalRequires(dp.AllowTvmTransferTrc10(), "allow_tvm_transfer_trc10", id)
	case 27:
		if !isHistoricalNileShieldedActivation(ctx) {
			return fmt.Errorf("bad chain parameter id %d", id)
		}
		return validateProposalOne(id, value)
	case 29:
		return validateProposalRange(id, value, 1, 10_000)
	case 32:
		if err := validateProposalOne(id, value); err != nil {
			return err
		}
		return validateProposalRequires(dp.AllowCreationOfContracts(), "allow_creation_of_contracts", id)
	case 33:
		return validateProposalRange(id, value, 1, 1_000)
	case 35:
		if err := validateProposalOne(id, value); err != nil {
			return err
		}
		return validateProposalRequires(dp.AllowCreationOfContracts(), "allow_creation_of_contracts", id)
	case 45, 46:
		if err := validateProposalRequires(dp.AllowMarketTransaction(), "allow_market_transaction", id); err != nil {
			return err
		}
		return validateProposalRange(id, value, 0, proposalMarketFeeMax)
	case 47:
		if value < 0 {
			return fmt.Errorf("bad chain parameter value for id %d, value must not be negative", id)
		}
		if value > proposalMarketFeeMax && !dp.AllowTvmLondon() {
			return fmt.Errorf("bad chain parameter value for id %d, valid range is [0,%d]", id, proposalMarketFeeMax)
		}
		if value > proposalLongValue {
			return validateProposalRange(id, value, 0, proposalLongValue)
		}
		return nil
	case 52:
		if err := validateProposalOne(id, value); err != nil {
			return err
		}
		if err := validateProposalRequires(dp.AllowDelegateResource(), "allow_delegate_resource", id); err != nil {
			return err
		}
		if err := validateProposalRequires(dp.AllowMultiSign(), "allow_multi_sign", id); err != nil {
			return err
		}
		if err := validateProposalRequires(dp.AllowTvmConstantinople(), "allow_tvm_constantinople", id); err != nil {
			return err
		}
		return validateProposalRequires(dp.AllowTvmSolidity059(), "allow_tvm_solidity059", id)
	case 59:
		if err := validateProposalOne(id, value); err != nil {
			return err
		}
		return validateProposalRequires(dp.ChangeDelegation(), "change_delegation", id)
	case 61:
		return validateProposalRange(id, value, 0, 100_000)
	case 62:
		return validateProposalRange(id, value, 0, proposalTotalNetLimitMax)
	case 67:
		if dp.AllowNewReward() {
			return errors.New("allow_new_reward has already been activated")
		}
		return validateProposalOne(id, value)
	case 68:
		return validateProposalRange(id, value, 0, proposalMemoFeeMax)
	case 70:
		return validateProposalRange(id, value, 1, 365)
	case 72:
		if err := validateProposalZeroOrOne(id, value); err != nil {
			return err
		}
		if value == 1 {
			return validateProposalRequires(dp.ChangeDelegation(), "change_delegation", id)
		}
		return nil
	case 74:
		return validateProposalRange(id, value, 0, proposalDynamicEnergyIncreaseMax)
	case 75:
		return validateProposalRange(id, value, 0, proposalDynamicEnergyMaxFactorMax)
	case 77:
		if err := validateProposalOne(id, value); err != nil {
			return err
		}
		return validateProposalRequires(dp.UnfreezeDelayDays() != 0, "unfreeze_delay_days", id)
	case 78:
		if err := validateProposalRequires(dp.UnfreezeDelayDays() != 0, "unfreeze_delay_days", id); err != nil {
			return err
		}
		if value <= dp.MaxDelegateLockPeriod() || value > proposalOneYearBlockNumbers {
			return fmt.Errorf("bad chain parameter value for id %d, valid range is (%d,%d]", id, dp.MaxDelegateLockPeriod(), proposalOneYearBlockNumbers)
		}
		return nil
	case 79:
		if dp.AllowOldRewardOpt() {
			return errors.New("allow_old_reward_opt has already been activated")
		}
		if err := validateProposalOne(id, value); err != nil {
			return err
		}
		return validateProposalRequires(dp.UseNewRewardAlgorithm(), "allow_new_reward", id)
	case 81:
		if dp.AllowEnergyAdjustment() {
			return errors.New("allow_energy_adjustment has already been activated")
		}
		return validateProposalOne(id, value)
	case 82:
		return validateProposalRange(id, value, proposalCreateAccountTxMinSize, proposalCreateAccountTxMaxSize)
	case 83:
		if dp.AllowTvmCancun() {
			return errors.New("allow_tvm_cancun has already been activated")
		}
		return validateProposalOne(id, value)
	case 87:
		if dp.AllowStrictMath() {
			return errors.New("allow_strict_math has already been activated")
		}
		return validateProposalOne(id, value)
	case 88:
		if dp.ConsensusLogicOptimization() {
			return errors.New("consensus_logic_optimization has already been activated")
		}
		return validateProposalOne(id, value)
	case 89:
		if dp.AllowTvmBlob() {
			return errors.New("allow_tvm_blob has already been activated")
		}
		return validateProposalOne(id, value)
	case 92:
		if value <= proposalMinExpireTime || value >= proposalMaxExpireTime {
			return fmt.Errorf("bad chain parameter value for id %d, valid range is (%d,%d)", id, proposalMinExpireTime, proposalMaxExpireTime)
		}
		return nil
	case 94:
		if dp.AllowTvmSelfdestructRestriction() {
			return errors.New("allow_tvm_selfdestruct_restriction has already been activated")
		}
		return validateProposalOne(id, value)
	case 95:
		if err := validateProposalRequires(dp.AllowTvmShanghai(), "allow_tvm_shanghai", id); err != nil {
			return err
		}
		if dp.AllowTvmPrague() {
			return errors.New("allow_tvm_prague has already been activated")
		}
		return validateProposalOne(id, value)
	case 96:
		if dp.AllowTvmOsaka() {
			return errors.New("allow_tvm_osaka has already been activated")
		}
		return validateProposalOne(id, value)
	case 97:
		if dp.AllowHardenResourceCalculation() {
			return errors.New("allow_harden_resource_calculation has already been activated")
		}
		return validateProposalOne(id, value)
	case 98:
		if err := validateProposalZeroOrOne(id, value); err != nil {
			return err
		}
		if dp.AllowHardenExchangeCalculation() == (value == 1) {
			return fmt.Errorf("allow_harden_exchange_calculation has already been set to %d", value)
		}
		return nil
	default:
		return nil
	}
}

func isHistoricalNileShieldedActivation(ctx *Context) bool {
	if ctx == nil || ctx.BlockNumber != proposalNileShieldedActivationBlock {
		return false
	}
	genesisHash := ctx.GenesisHash
	if genesisHash == (common.Hash{}) && ctx.DB != nil {
		genesisHash = rawdb.ReadBlockHashByNumber(ctx.DB, 0)
	}
	return genesisHash == proposalNileGenesisHash
}

var proposalRequiredVersion = map[int64]int32{
	19: 6,
	20: 7, 21: 7, 22: 7, 23: 7,
	24: 8, 25: 8, 26: 8,
	29: 9, 30: 9, 31: 9, 32: 9, 33: 9,
	35: 10,
	39: 17,
	40: 19, 41: 19, 45: 19, 46: 19,
	47: 20, 48: 20, 49: 20,
	51: 21, 52: 21,
	53: 22, 59: 22, 61: 22, 62: 22,
	60: 23, 63: 23,
	65: 24, 66: 24,
	67: 25, 68: 25, 69: 25,
	70: 26, 71: 26, 72: 26, 73: 26, 74: 26, 75: 26,
	76: 28, 77: 28, 78: 28,
	79: 29,
	81: 30, 82: 30,
	87: 31,
	83: 32, 88: 32, 89: 32,
	92: 34, 94: 34,
	95: 35, 96: 35, 97: 35, 98: 35,
}

func validateProposalForkGate(ctx *Context, id int64) error {
	switch id {
	case 17:
		if !proposalPassVersion(ctx, 5) || proposalPassVersion(ctx, 6) {
			return fmt.Errorf("bad chain parameter id %d", id)
		}
		return nil
	case 44:
		if !proposalPassVersion(ctx, 19) || proposalPassVersion(ctx, 34) {
			return fmt.Errorf("bad chain parameter id %d", id)
		}
		return nil
	}
	version, ok := proposalRequiredVersion[id]
	if !ok {
		return nil
	}
	if !proposalPassVersion(ctx, version) {
		return fmt.Errorf("bad chain parameter id %d", id)
	}
	return nil
}

func proposalPassVersion(ctx *Context, version int32) bool {
	if ctx.State == nil {
		return false
	}
	return forks.PassVersionFromStore(ctx.State, version, ctx.PrevBlockTime, ctx.DynProps.MaintenanceTimeInterval())
}

func validateProposalOne(id, value int64) error {
	if value != 1 {
		return fmt.Errorf("bad chain parameter value for id %d, value is only allowed to be 1", id)
	}
	return nil
}

func validateProposalZeroOrOne(id, value int64) error {
	if value != 0 && value != 1 {
		return fmt.Errorf("bad chain parameter value for id %d, value is only allowed to be 0 or 1", id)
	}
	return nil
}

func validateProposalRange(id, value, min, max int64) error {
	if value < min || value > max {
		return fmt.Errorf("bad chain parameter value for id %d, valid range is [%d,%d]", id, min, max)
	}
	return nil
}

func validateProposalRequires(ok bool, requirement string, id int64) error {
	if !ok {
		return fmt.Errorf("proposal id %d requires %s to be approved first", id, requirement)
	}
	return nil
}
