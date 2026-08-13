package core

import (
	"testing"

	corepb "github.com/tronprotocol/go-tron/proto/core"
)

func TestTransactionInfosEnergyUsageTotal(t *testing.T) {
	infos := []*corepb.TransactionInfo{
		nil,
		{},
		{Receipt: &corepb.ResourceReceipt{EnergyUsageTotal: 1_250}},
		{Receipt: &corepb.ResourceReceipt{EnergyUsageTotal: 3_750}},
	}
	if got := transactionInfosEnergyUsageTotal(infos); got != 5_000 {
		t.Fatalf("energy usage total = %d, want 5000", got)
	}
}
