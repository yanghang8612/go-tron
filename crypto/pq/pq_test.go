package pq

import (
	"crypto/sha256"
	"encoding/hex"
	"testing"

	"github.com/cloudflare/circl/sign/mldsa/mldsa44"
	corepb "github.com/tronprotocol/go-tron/proto/core"
)

// TestMLDSA44JavaTronKAT pins the public-key encoding shared with
// java-tron's BouncyCastle 1.84 implementation. The expected digest is from
// MLDSA44KatTest at GreatVoyage-Nile-v4.8.2-PQ1-build2.
func TestMLDSA44JavaTronKAT(t *testing.T) {
	var seed [mldsa44.SeedSize]byte
	for i := range seed {
		seed[i] = byte(i)
	}
	pk, _ := mldsa44.NewKeyFromSeed(&seed)
	digest := sha256.Sum256(pk.Bytes())
	if got, want := hex.EncodeToString(digest[:]), "9f107644c1084526af3bc8098680b05499a2325a644e388fb4f970e058d19d46"; got != want {
		t.Fatalf("ML-DSA-44 public-key encoding differs from java-tron: got %s want %s", got, want)
	}
}

// TestNileFNDSA512Block69200000 is a real java-tron PQ1 block signature.
// It pins the original Falcon transcript and java's headerless public-key
// encoding, which differ from the later fixed-size FN-DSA API defaults.
func TestNileFNDSA512Block69200000(t *testing.T) {
	digest := mustDecodeHex(t, "c26dedbf7d48eb74fdfe824039fc96a702127be6cf42e6a26feb8909929fe227")
	pk := mustDecodeHex(t, "3acc915ab2eea42afc94bb536e802e200c902a0bd41a4a6a54caeb7a6cbc9632c798c3369055b1079ddfb189dea81d2f9c47464593c5c10a5aa9e5226be4c3a4b61ee82e076348187dfa97d64e6e883a0977ae9724599f5b2e2d1e9dfe57fbe10d6404bc09a50415e44bee1d7a6ed3ae6794fe1aa5976e4ab9e661571cca083d3949610d730c826b25e4c4153d3a852b51a4586d05a9fae51ab889da79f01ac76e42222943424f28478c52312245d033cd7a4a06098d34d60433dec4f22d6c7658ef1ce5b5fdf9b6052da103604ba71c22776bc66ff52568b116e45e08e18c35353a347417ac01841601db74cc3a4a9a6d999d0dd014d1374b88cc38657285c9d38784466533b74da78d8a08bf84c3125f6795fc32d6182f122d3a7204e4258592a1facd94cf1fde5530c64ce4b4ce1a36d088db637908a8c0016a56383983a61325217ea543e393405e7dfaa2f31428ac4cde4157144a1b8c01e6d48c3d728406b5a1524b56900a45e0cab6b29819b9570bb8a3ec89bbe6e12c4e618513ba96ccb71131290802ac954c449645147b6ab500e7e814f55d2978e0d9c582ae22e5929ed8209685f2e5b409a23b9144153758a32956fde0a0281031821c30ab532314860b57aad70271d67e1784daa39aa5301b451c438a5ce6a4211bfb8fd0bb623987e01f4ee2a8c8ebb65f31c78a6208b64f9943e9c0dc6b40c2259ae60d6aa35442a229e6cc88ca8184bb5c2072f18609d88ef7b961b9217b5f3c94d0057457d78968ecd706ba88b0614e752a62d8f464c876e324c3e715764ed8f08b429a11d589d3d2bb84152e22c62493587ab4b809d2fa3d54bfb36e0941b174b2d610c0ab00b50598dc856e605caf48544d35b3a9acb053e66c3c3a287b731aa3036949a423207b28096c184e4e4d4df55c63830b7c9e2961a42cab1d732472314c67e7a7d446d65c6cd6701ed494a3a2e31fe6b972350b3d917a9201e9dedc0f60a1c50a67febb8cb2c296e1401a44de79c69fa8a781dbe88ad25037a2370a6b695a795511e763c48f7ec95a86a6eac3b16a356717a12935c138e0decd2b312a5ea09ec6d33a3e8e235fdae66d1610eb1a154867681e26af95ba9bed45e27d9e5a08e1a7190da92146f04014191ae89fc63f5be3dc5c8109db1b1ce3a0469d04b9055b4d0415fa166b85ce0896e3da8ee8d061e54aebd583d2709c635252994b65bdbe22c4cdb0072673e010c16f3fccd7d84dc11d553e51e0c15e622c62a896b140d3")
	sig := mustDecodeHex(t, "39a1560347528bac6de5090abbf1bd36c024e528dc1c33669bed70597d6c63fd66068cbd62f732acea81c01b0d55f592d3f3b1de65caf0d4314e8654d3b5baf45a516b99efe527991b536b538dfd672dac7726d1b1399839fbdeb531a9ebbf7f1dc5524e85b1769eb5b5bfb014cc2222dc13e95ad9768ca1d9163e9ddb7e35f71f0dea9a6f7e6d2c804ad054995bc2b284c4fa629154d8528c62cd164f92686a4a8c7796fbf1ca171388f5518bf7c7d5baa63f905a1dc21d1158255adb4d0dc591d5a5ab6f9d5251916c7acd09bd2153b7cf02b2226708acbeaa847a02809ae4a0eb2b1566e2351559b51a7a3a0b04a439d97262accab45cf9079918b0f0533c1e05aef3f758c68cdda1880fae26a047139f6de280d8ee2672cfe7fb128c369b828ff8681132c25a5d1ba45270a956fc6bd51e6d82da5dda2463217bdd5dc901a681b33b388b34b326afe70728d04f2b96c972b35cdae0048219942e03511f74db1687df7c7b18ad77a2c1e5f9d939087fdf06b96f10b67c912284c32852caeb03264e60944c6c9e64c49134d6e90c95274c651767d747a00b2ad73f47d181e855797d8f5f61daae0cda90fd238c9b6b897838c5a9469790991a26703f753e3550e4af52b5266ec95de2eb5b9fbbf96eca4fc5134e98bcfc6591475aeac2712be2c05d77eb34ff4db3862b215a85cd788564fe329844f3ff5d44f72eec354f9927ec4c7d2b7f2af50d50521ae1873b6ea360df71aca6c7c0fe72b9fc5268d59c1e4fcf81d0fe7c947638d828afd43444384d01fc44f8cafb712afd4c20a89efdbb7b210b57abc8bc492d5616cf999a9961911134812572e4939ccb9e6bbcf1b753e1c55fa38212c386b0ede83c5f02caccee308926d1cec7bb943c721eb71c4a756345d0ab319be4bb7ea943f493aef4")
	if !Verify(corepb.PQScheme_FN_DSA_512, pk, digest, sig) {
		t.Fatal("real Nile FN-DSA-512 signature rejected")
	}
	addr, err := Address(corepb.PQScheme_FN_DSA_512, pk)
	if err != nil {
		t.Fatal(err)
	}
	// This is the account's delegated WitnessPermission key; the block raw
	// witness_address remains the SR account (411d2984...).
	if got, want := hex.EncodeToString(addr.Bytes()), "4191774ded82d13959fa7a2059d027f95538300cd3"; got != want {
		t.Fatalf("derived witness address: got %s want %s", got, want)
	}
	digest[0] ^= 1
	if Verify(corepb.PQScheme_FN_DSA_512, pk, digest, sig) {
		t.Fatal("mutated digest accepted")
	}
}

func mustDecodeHex(t *testing.T, value string) []byte {
	t.Helper()
	b, err := hex.DecodeString(value)
	if err != nil {
		t.Fatal(err)
	}
	return b
}
