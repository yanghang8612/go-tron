package actuator

import (
	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
)

func witnessExists(ctx *Context, addr common.Address) bool {
	if ctx.State.GetWitness(addr) != nil {
		return true
	}
	return ctx.DB != nil && rawdb.ReadWitness(ctx.DB, addr) != nil
}
