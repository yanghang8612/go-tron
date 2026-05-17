package actuator

import (
	"errors"
	"fmt"
	"math"

	"github.com/tronprotocol/go-tron/core/forks"
	"github.com/tronprotocol/go-tron/core/state"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	contractpb "github.com/tronprotocol/go-tron/proto/core/contract"
	"google.golang.org/protobuf/proto"
)

type AccountPermissionUpdateActuator struct{}

func (a *AccountPermissionUpdateActuator) getContract(ctx *Context) (*contractpb.AccountPermissionUpdateContract, error) {
	contract := ctx.Tx.Contract()
	if contract == nil {
		return nil, errors.New("no contract in transaction")
	}
	c := &contractpb.AccountPermissionUpdateContract{}
	if err := contract.Parameter.UnmarshalTo(c); err != nil {
		return nil, errors.New("failed to unmarshal AccountPermissionUpdateContract")
	}
	return c, nil
}

func (a *AccountPermissionUpdateActuator) Validate(ctx *Context) error {
	if !forks.IsActive(forks.AllowMultiSign, ctx.BlockNumber, ctx.DynProps) {
		return errors.New("multi-sign not yet enabled")
	}
	c, err := a.getContract(ctx)
	if err != nil {
		return err
	}
	ownerAddr, err := checkedAddress(c.OwnerAddress, "ownerAddress")
	if err != nil {
		return err
	}
	account := ctx.State.GetAccount(ownerAddr)
	if account == nil {
		return errors.New("ownerAddress account does not exist")
	}
	if c.Owner == nil {
		return errors.New("owner permission is missed")
	}
	if account.IsWitness() {
		if c.Witness == nil {
			return errors.New("witness permission is missed")
		}
	} else if c.Witness != nil {
		return errors.New("account isn't witness can't set witness permission")
	}
	if len(c.Actives) == 0 {
		return errors.New("active permission is missed")
	}
	if len(c.Actives) > 8 {
		return errors.New("active permission is too many")
	}
	if c.Owner.Type != corepb.Permission_Owner {
		return errors.New("owner permission type is error")
	}
	if err := validatePermission(c.Owner, ctx.DynProps); err != nil {
		return err
	}
	if account.IsWitness() {
		if c.Witness.Type != corepb.Permission_Witness {
			return errors.New("witness permission type is error")
		}
		if err := validatePermission(c.Witness, ctx.DynProps); err != nil {
			return err
		}
	}
	for _, active := range c.Actives {
		if active == nil || active.Type != corepb.Permission_Active {
			return errors.New("active permission type is error")
		}
		if err := validatePermission(active, ctx.DynProps); err != nil {
			return err
		}
	}
	return nil
}

// validateOperationsBits enforces that every bit set in the active
// permission's operations bitmap corresponds to a contract type marked
// available in dp.AvailableContractType. Mirrors java-tron
// AccountPermissionUpdateActuator's per-bit check.
func validateOperationsBits(operations []byte, dp *state.DynamicProperties) error {
	avail := dp.AvailableContractType()
	for i := 0; i < state.ContractTypeBitmapBytes*8; i++ {
		opBit := operations[i/8]&(1<<(i%8)) != 0
		availBit := avail[i/8]&(1<<(i%8)) != 0
		if opBit && !availBit {
			return fmt.Errorf("%d isn't a validate ContractType", i)
		}
	}
	return nil
}

func validatePermission(p *corepb.Permission, dp *state.DynamicProperties) error {
	if p == nil {
		return errors.New("permission is missed")
	}
	if len(p.Keys) > int(dp.TotalSignNum()) {
		return fmt.Errorf("number of keys in permission should not be greater than %d", dp.TotalSignNum())
	}
	if len(p.Keys) == 0 {
		return errors.New("key's count should be greater than 0")
	}
	if p.Type == corepb.Permission_Witness && len(p.Keys) != 1 {
		return errors.New("Witness permission's key count should be 1")
	}
	if p.Threshold <= 0 {
		return errors.New("permission's threshold should be greater than 0")
	}
	if p.PermissionName != "" && len(p.PermissionName) > 32 {
		return errors.New("permission's name is too long")
	}
	if p.ParentId != 0 {
		return errors.New("permission's parent should be owner")
	}
	var totalWeight int64
	seen := make(map[string]struct{}, len(p.Keys))
	for _, k := range p.Keys {
		if !validAddressBytes(k.Address) {
			return errors.New("key is not a validate address")
		}
		key := string(k.Address)
		if _, ok := seen[key]; ok {
			return fmt.Errorf("address should be distinct in permission %s", p.Type)
		}
		seen[key] = struct{}{}
		if k.Weight <= 0 {
			return errors.New("key's weight should be greater than 0")
		}
		if k.Weight > math.MaxInt64-totalWeight {
			return errors.New("integer overflow")
		}
		totalWeight += k.Weight
	}
	if p.Threshold > totalWeight {
		return fmt.Errorf("sum of all key's weight should not be less than threshold in permission %s", p.Type)
	}
	if p.Type != corepb.Permission_Active {
		if len(p.Operations) != 0 {
			return fmt.Errorf("%s permission needn't operations", p.Type)
		}
		return nil
	}
	if len(p.Operations) == 0 || len(p.Operations) != state.ContractTypeBitmapBytes {
		return errors.New("operations size must 32")
	}
	return validateOperationsBits(p.Operations, dp)
}

func clonePermissionWithID(p *corepb.Permission, id int32) *corepb.Permission {
	if p == nil {
		return nil
	}
	cp := proto.Clone(p).(*corepb.Permission)
	cp.Id = id
	return cp
}

func normalizeUpdatedPermissions(owner, witness *corepb.Permission, actives []*corepb.Permission, isWitness bool) (*corepb.Permission, *corepb.Permission, []*corepb.Permission) {
	owner = clonePermissionWithID(owner, 0)
	if isWitness {
		witness = clonePermissionWithID(witness, 1)
	} else {
		witness = nil
	}

	activeCopies := make([]*corepb.Permission, 0, len(actives))
	for i, active := range actives {
		activeCopies = append(activeCopies, clonePermissionWithID(active, int32(i+2)))
	}
	return owner, witness, activeCopies
}

func (a *AccountPermissionUpdateActuator) Execute(ctx *Context) (*Result, error) {
	c, err := a.getContract(ctx)
	if err != nil {
		return nil, err
	}
	ownerAddr, err := checkedAddress(c.OwnerAddress, "ownerAddress")
	if err != nil {
		return nil, err
	}
	fee := ctx.DynProps.UpdateAccountPermissionFee()
	account := ctx.State.GetAccount(ownerAddr)
	isWitness := account != nil && account.IsWitness()
	owner, witness, actives := normalizeUpdatedPermissions(c.Owner, c.Witness, c.Actives, isWitness)
	ctx.State.SetPermissions(ownerAddr, owner, witness, actives)
	if err := burnFee(ctx, ownerAddr, fee); err != nil {
		return nil, err
	}
	return &Result{Fee: fee, ContractRet: 1}, nil
}
