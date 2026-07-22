package crypto

import (
	"bytes"
	"encoding/hex"
	"testing"

	ethcrypto "github.com/ethereum/go-ethereum/crypto"

	"github.com/tronprotocol/go-tron/common"
)

// Nile block 18,278,266: a TransferContract owned by the keccak256("") ghost
// address, "signed" by a constructed signature whose recId=0 recovery is the
// point at infinity. java-tron's recover returns keccak256("")[12:] for it;
// gtron must do the same instead of rejecting the block.
func TestSigToAddressJavaCompatInfinityGhost(t *testing.T) {
	hash, _ := hex.DecodeString("6ca2cf5c23523f6f5eb8283164c93bc039bc263ef4afb26cdfc18aea2cc33f27")
	// recoverySig = [r||s||v] with v already a geth recovery id. The on-chain
	// v byte is 0x00, which signatureForRecovery maps to recid 0.
	recoverySig, _ := hex.DecodeString(
		"bdd60d53b9876fd4c2d3d142d43b60350e70dbc0554d4ec34c977518bda2db24" +
			"4cf392d2900d507cec8bcc9e7aae71f9267069c8a454aca0f637c6470e6cc2be" +
			"00")

	addr, err := SigToAddressJavaCompat(hash, recoverySig)
	if err != nil {
		t.Fatalf("java-compat recover must not error on the infinity case: %v", err)
	}

	want := make([]byte, 21)
	want[0] = common.AddressPrefixMainnet
	copy(want[1:], ethcrypto.Keccak256(nil)[12:])
	if !bytes.Equal(addr.Bytes(), want) {
		t.Fatalf("ghost signer: got %x want %x (0x41 || keccak256(\"\")[12:])", addr.Bytes(), want)
	}
	// And concretely: the owner of Nile 18,278,266.
	if got := hex.EncodeToString(addr.Bytes()); got != "41dcc703c0e500b653ca82273b7bfad8045d85a470" {
		t.Fatalf("ghost signer addr: got %s want 41dcc703…", got)
	}
}

// A normal, valid signature must still recover its real signer unchanged.
func TestSigToAddressJavaCompatNormal(t *testing.T) {
	key, _ := ethcrypto.GenerateKey()
	hash := ethcrypto.Keccak256([]byte("hello tron"))
	sig, _ := ethcrypto.Sign(hash, key)

	addr, err := SigToAddressJavaCompat(hash, sig)
	if err != nil {
		t.Fatalf("normal recover: %v", err)
	}
	if addr != PubkeyToAddress(&key.PublicKey) {
		t.Fatalf("normal recover mismatch: got %x want %x", addr, PubkeyToAddress(&key.PublicKey))
	}
}

// A genuinely malformed signature (zero r/s) is not the infinity case and
// must still error — java rejects these too.
func TestSigToAddressJavaCompatGarbageStillErrors(t *testing.T) {
	hash := ethcrypto.Keccak256([]byte("x"))
	garbage := make([]byte, 65) // all-zero r/s/v
	if _, err := SigToAddressJavaCompat(hash, garbage); err == nil {
		t.Fatal("all-zero signature must error, not resolve to the ghost address")
	}
}

func BenchmarkSigToAddressJavaCompat(b *testing.B) {
	key, err := ethcrypto.GenerateKey()
	if err != nil {
		b.Fatal(err)
	}
	hash := ethcrypto.Keccak256([]byte("gtron signature recovery benchmark"))
	sig, err := ethcrypto.Sign(hash, key)
	if err != nil {
		b.Fatal(err)
	}
	want := PubkeyToAddress(&key.PublicKey)

	for _, tc := range []struct {
		name string
		fn   func([]byte, []byte) (common.Address, error)
	}{
		{name: "direct-address", fn: SigToAddressJavaCompat},
		{name: "legacy-pubkey-roundtrip", fn: func(hash, sig []byte) (common.Address, error) {
			pub, err := SigToPub(hash, sig)
			if err != nil {
				return common.Address{}, err
			}
			return PubkeyToAddress(pub), nil
		}},
	} {
		b.Run(tc.name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				got, err := tc.fn(hash, sig)
				if err != nil {
					b.Fatal(err)
				}
				if got != want {
					b.Fatalf("recovered address = %x, want %x", got, want)
				}
			}
		})
	}
}
