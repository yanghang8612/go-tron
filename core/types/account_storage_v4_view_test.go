package types

import (
	"bytes"
	"encoding/binary"
	"math"
	"testing"

	corepb "github.com/tronprotocol/go-tron/proto/core"
)

func TestAccountStorageCoreV4CodeHashViewMatchesFullDecoder(t *testing.T) {
	pb := &corepb.Account{
		AccountName: []byte("name"), Type: corepb.AccountType_Contract, Address: []byte{0x41, 1},
		Balance: math.MinInt64, NetUsage: math.MaxInt64, AcquiredDelegatedFrozenBalanceForBandwidth: 9,
		DelegatedFrozenBalanceForBandwidth: 10, OldTronPower: -1, AssetOptimized: true,
		CreateTime: 11, LatestOprationTime: 12, Allowance: 13, LatestWithdrawTime: 14,
		Code: []byte{1, 2}, IsWitness: true, IsCommittee: true, AssetIssuedName: []byte("asset"),
		AssetIssued_ID: []byte("1000001"), FreeNetUsage: 15, LatestConsumeTime: 16,
		LatestConsumeFreeTime: 17, AccountId: []byte("id"), NetWindowSize: 18, NetWindowOptimized: true,
		CodeHash: bytes.Repeat([]byte{0x55}, 32), DelegatedFrozenV2BalanceForBandwidth: 19,
		AcquiredDelegatedFrozenV2BalanceForBandwidth: 20,
	}
	pb.ProtoReflect().SetUnknown([]byte{0xa0, 6, 1})
	full, err := NewAccountFromPB(pb).MarshalStorageCoreV4()
	if err != nil {
		t.Fatal(err)
	}
	empty, _ := NewAccountFromPB(new(corepb.Account)).MarshalStorageCoreV4()
	short, _ := NewAccountFromPB(&corepb.Account{CodeHash: []byte{1, 2}}).MarshalStorageCoreV4()
	corpus := [][]byte{full, empty, short, nil}
	for n := 0; n < len(full); n++ {
		corpus = append(corpus, full[:n])
	}
	for n := range full {
		v := bytes.Clone(full)
		v[n] ^= 0x80
		corpus = append(corpus, v)
	}
	for _, bitmap := range []uint32{1, 1 << 3, 1 << 1, 1 << 29} {
		v := append(bytes.Clone(full[:5]), make([]byte, 4)...)
		binary.BigEndian.PutUint32(v[5:], bitmap)
		corpus = append(corpus, append(v, 0))
	}
	corpus = append(corpus, append(bytes.Clone(full), 0))
	for i, encoded := range corpus {
		view, viewErr := AccountStorageCoreV4CodeHash(encoded)
		account, fullErr := UnmarshalAccountStorageCoreV4(encoded)
		if (viewErr == nil) != (fullErr == nil) {
			t.Fatalf("case %d acceptance: view=%v full=%v", i, viewErr, fullErr)
		}
		if fullErr == nil && !bytes.Equal(view, account.Proto().CodeHash) {
			t.Fatalf("case %d hash mismatch", i)
		}
	}
	var view []byte
	allocs := testing.AllocsPerRun(100, func() { view, err = AccountStorageCoreV4CodeHash(full) })
	if err != nil || allocs != 0 || !bytes.Equal(view, pb.CodeHash) {
		t.Fatalf("hash view err=%v allocs=%v", err, allocs)
	}
	view[0] = 0x77
	if !bytes.Contains(full, view) {
		t.Fatal("view does not borrow its input")
	}
}
