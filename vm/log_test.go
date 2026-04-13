package vm

import (
	"testing"

	tcommon "github.com/tronprotocol/go-tron/common"
)

func TestLogSnapshotRevert(t *testing.T) {
	evm := &TVM{}

	evm.Logs = append(evm.Logs, Log{
		Address: tcommon.Address{0x41, 0x01},
		Topics:  [][]byte{{0x01}},
		Data:    []byte{0xAA},
	})
	if len(evm.Logs) != 1 {
		t.Fatalf("expected 1 log, got %d", len(evm.Logs))
	}

	snap := evm.LogSnapshot()
	if snap != 1 {
		t.Fatalf("expected snapshot 1, got %d", snap)
	}

	evm.Logs = append(evm.Logs, Log{
		Address: tcommon.Address{0x41, 0x02},
		Topics:  [][]byte{{0x02}},
		Data:    []byte{0xBB},
	})
	if len(evm.Logs) != 2 {
		t.Fatalf("expected 2 logs, got %d", len(evm.Logs))
	}

	evm.RevertLogs(snap)
	if len(evm.Logs) != 1 {
		t.Fatalf("expected 1 log after revert, got %d", len(evm.Logs))
	}
	if evm.Logs[0].Data[0] != 0xAA {
		t.Fatal("wrong log after revert")
	}
}
