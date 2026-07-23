package vm

import (
	"testing"

	tcommon "github.com/tronprotocol/go-tron/common"
)

func TestExecutionContractPoolClearsFrameState(t *testing.T) {
	caller := tcommon.Address{0x41, 0x01}
	addr := tcommon.Address{0x41, 0x02}
	c := acquireExecutionContract(caller, addr, 7, 100)
	c.Code = []byte{byte(STOP)}
	c.Input = []byte{1, 2, 3}
	c.CodeAddr = tcommon.Address{0x41, 0x03}
	c.CodeHash = tcommon.HexToHash("deadbeef")
	c.InternalTxHash = tcommon.HexToHash("cafe")
	c.TokenID = 1
	c.TokenValue = 2
	c.EnergyUsed = 3
	c.Version = 1
	c.jumpdests = bitvec{0xff}
	releaseExecutionContract(c)

	got := acquireExecutionContract(caller, addr, 9, 200)
	defer releaseExecutionContract(got)
	if got.Caller != caller || got.Address != addr || got.Value != 9 || got.Energy != 200 {
		t.Fatalf("initialized fields = %+v", got)
	}
	if got.Code != nil || got.Input != nil || got.jumpdests != nil || got.CodeAddr != (tcommon.Address{}) ||
		got.CodeHash != (tcommon.Hash{}) || got.InternalTxHash != (tcommon.Hash{}) || got.TokenID != 0 ||
		got.TokenValue != 0 || got.EnergyUsed != 0 || got.Version != 0 {
		t.Fatalf("pooled frame retained state: %+v", got)
	}
}

var contractBenchmarkSink *Contract

func BenchmarkExecutionContractLifecycle(b *testing.B) {
	caller := tcommon.Address{0x41, 0x01}
	addr := tcommon.Address{0x41, 0x02}
	b.Run("Allocate", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			contractBenchmarkSink = NewContract(caller, addr, 7, 100_000)
		}
	})
	b.Run("Pool", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			c := acquireExecutionContract(caller, addr, 7, 100_000)
			contractBenchmarkSink = c
			releaseExecutionContract(c)
		}
	})
}
