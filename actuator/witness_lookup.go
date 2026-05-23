package actuator

import (
	"github.com/tronprotocol/go-tron/common"
)

func witnessExists(ctx *Context, addr common.Address) bool {
	return ctx.State.GetWitness(addr) != nil
}
