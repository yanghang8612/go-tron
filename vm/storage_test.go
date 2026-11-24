package vm

import (
	"bytes"
	"testing"

	tcommon "github.com/tronprotocol/go-tron/common"
	contractpb "github.com/tronprotocol/go-tron/proto/core/contract"
)

func TestExtCodeHashMetadataBeforeAndAfterProposal102(t *testing.T) {
	for _, tc := range []struct {
		name          string
		code          []byte
		initialHash   []byte
		cacheAfter102 bool
	}{
		{"empty", nil, nil, false},
		{"stop", []byte{byte(STOP)}, nil, true},
		{"nonzero", []byte{byte(JUMPDEST), byte(STOP)}, nil, true},
		{"zero hash", []byte{byte(STOP)}, make([]byte, 32), true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for _, protected := range []bool{false, true} {
				tvm := newTestEVMWithConfig(t, TVMConfig{ProtectLegacyStorageReads: protected})
				addr := tcommon.Address{0: 0x41, 20: 0x42}
				tvm.StateDB.SetContract(addr, &contractpb.SmartContract{
					ContractAddress: addr.Bytes(),
					CodeHash:        tc.initialHash,
				})
				tvm.StateDB.SetCode(addr, tc.code)
				want := tcommon.Keccak256(tc.code)
				if len(tc.initialHash) > 0 {
					want = tcommon.BytesToHash(tc.initialHash)
				}
				if got := tvm.extCodeHash(addr); got != want {
					t.Fatalf("protected=%v: hash=%x, want=%x", protected, got, want)
				}
				var wantCached []byte
				if !protected || tc.cacheAfter102 {
					wantCached = want.Bytes()
				}
				if got := tvm.StateDB.GetContract(addr).GetCodeHash(); !bytes.Equal(got, wantCached) {
					t.Fatalf("protected=%v: cached hash=%x, want=%x", protected, got, wantCached)
				}
			}
		})
	}
}
