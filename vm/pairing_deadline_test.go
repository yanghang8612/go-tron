package vm

import (
	"errors"
	"testing"
	"time"

	"github.com/consensys/gnark-crypto/ecc/bn254"
	tcommon "github.com/tronprotocol/go-tron/common"
)

func TestBN128PairingDeadline(t *testing.T) {
	_, _, g1, g2 := bn254.Generators()
	var negG1 bn254.G1Affine
	negG1.Neg(&g1)
	g1Bytes, negG1Bytes, g2Bytes := g1.RawBytes(), negG1.RawBytes(), g2.RawBytes()
	single := append(g1Bytes[:], g2Bytes[:]...)
	inverse := append(negG1Bytes[:], g2Bytes[:]...)
	paired := append(append([]byte{}, single...), inverse...)
	precompile := &bn128Pairing{istanbul: true}

	for _, test := range []struct {
		name  string
		input []byte
		want  byte
	}{
		{"empty", nil, 1},
		{"infinity", make([]byte, bn128PairSize), 1},
		{"nonidentity", single, 0},
		{"identity", paired, 1},
	} {
		t.Run(test.name, func(t *testing.T) {
			for _, deadline := range []time.Time{{}, time.Now().Add(time.Minute)} {
				tvm := &TVM{cfg: TVMConfig{Deadline: deadline}}
				output, used, ok, err := precompile.RunWithStatus(tvm, tcommon.Address{}, test.input, 200_000)
				wantUsed := 45_000 + 34_000*uint64(len(test.input)/bn128PairSize)
				if err != nil || !ok || len(output) != 32 || output[31] != test.want || used != wantUsed {
					t.Fatalf("output %x, energy %d, success %t, error %v", output, used, ok, err)
				}
			}
		})
	}
	tvm := &TVM{cfg: TVMConfig{Deadline: time.Now().Add(-time.Second)}}
	if _, _, ok, err := precompile.RunWithStatus(tvm, tcommon.Address{}, single, 200_000); ok || !errors.Is(err, ErrAlreadyTimeOut) {
		t.Fatalf("expired pairing success %t, error %v", ok, err)
	}
}
