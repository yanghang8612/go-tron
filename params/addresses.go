package params

import (
	"encoding/hex"

	"github.com/tronprotocol/go-tron/common"
)

// BlackholeAddress is the TRON genesis "Blackhole" account (address
// TLsV52sRDL79HXGGm9yzwKibb6BeruhUzy, hex 41b0a14f...). Before proposal #49
// (AllowBlackholeOptimization) is activated, protocol fees are credited to
// this account rather than being burned. Source: java-tron genesis config.
var BlackholeAddress = func() common.Address {
	b, _ := hex.DecodeString("41b0a14fb448b324ca992f2ddcb7d7b49470da3cf8")
	return common.BytesToAddress(b)
}()
