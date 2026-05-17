package actuator

import (
	"errors"

	"github.com/tronprotocol/go-tron/core/rawdb"
	contractpb "github.com/tronprotocol/go-tron/proto/core/contract"
)

type WitnessUpdateActuator struct{}

func (a *WitnessUpdateActuator) getContract(ctx *Context) (*contractpb.WitnessUpdateContract, error) {
	contract := ctx.Tx.Contract()
	if contract == nil {
		return nil, errors.New("no contract in transaction")
	}
	c := &contractpb.WitnessUpdateContract{}
	if err := contract.Parameter.UnmarshalTo(c); err != nil {
		return nil, errors.New("failed to unmarshal WitnessUpdateContract")
	}
	return c, nil
}

func (a *WitnessUpdateActuator) Validate(ctx *Context) error {
	c, err := a.getContract(ctx)
	if err != nil {
		return err
	}
	ownerAddr, err := checkedAddress(c.OwnerAddress, "ownerAddress")
	if err != nil {
		return err
	}
	if !ctx.State.AccountExists(ownerAddr) {
		return errors.New("owner account does not exist")
	}
	if !witnessExists(ctx, ownerAddr) {
		return errors.New("owner is not a witness")
	}
	if !validBytesLen(c.UpdateUrl, 256, false) {
		return errors.New("invalid witness URL")
	}
	return nil
}

func (a *WitnessUpdateActuator) Execute(ctx *Context) (*Result, error) {
	c, err := a.getContract(ctx)
	if err != nil {
		return nil, err
	}
	ownerAddr, err := checkedAddress(c.OwnerAddress, "ownerAddress")
	if err != nil {
		return nil, err
	}
	url := string(c.UpdateUrl)

	// Persist the URL change through ctx.DB. In applyBlock this is the
	// fork-rewindable block buffer; ApplyBlockStatistics reads from the
	// same buffer when updating production counters, so the new URL is
	// preserved across that read-merge-write. In BuildBlock ctx.DB is the
	// disk DB and the write is idempotent with the upcoming applyBlock.
	w := rawdb.ReadWitness(ctx.DB, ownerAddr)
	if w == nil {
		// Validate already required GetWitness != nil; fall back to the
		// in-memory record if disk has nothing yet (e.g. a witness created
		// earlier in this same block hasn't been flushed).
		if mw := ctx.State.GetWitness(ownerAddr); mw != nil {
			w = mw.Copy()
		} else {
			return nil, errors.New("witness record missing")
		}
	}
	w.Proto().Url = url
	rawdb.WriteWitness(ctx.DB, ownerAddr, w)

	// Mirror in-memory so any later read in this block sees the new URL.
	ctx.State.SetWitnessURL(ownerAddr, url)
	return &Result{Fee: 0, ContractRet: 1}, nil
}
