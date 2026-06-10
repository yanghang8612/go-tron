package core

import (
	tcommon "github.com/tronprotocol/go-tron/common"
	contractpb "github.com/tronprotocol/go-tron/proto/core/contract"
)

type blockBalanceTraceData struct {
	trace           *contractpb.BlockBalanceTrace
	accountBalances map[tcommon.Address]int64
}
