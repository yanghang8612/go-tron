package state

import (
	"bytes"
	"fmt"
	"math"
	"reflect"
	"testing"

	"github.com/ethereum/go-ethereum/rlp"
	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/types"
	corepb "github.com/tronprotocol/go-tron/proto/core"
)

func accountV5Core(t testing.TB, hash []byte) []byte {
	t.Helper()
	pb := &corepb.Account{Address: bytes.Repeat([]byte{0x41}, 21), AccountName: []byte("account"),
		Type: corepb.AccountType_Contract, Balance: math.MinInt64, NetUsage: math.MaxInt64,
		CreateTime: 123456789, LatestOprationTime: 123456799, OldTronPower: -1,
		AssetOptimized: true, IsWitness: true, IsCommittee: true, NetWindowOptimized: true,
		AccountId: []byte("id"), CodeHash: hash}
	pb.ProtoReflect().SetUnknown([]byte{0xa0, 6, 1, 0xaa, 6, 0})
	core, err := types.NewAccountFromPB(pb).MarshalStorageCoreV4()
	if err != nil {
		t.Fatal(err)
	}
	return core
}

func TestStateAccountV5RoundTripAndExactSavings(t *testing.T) {
	hash := tcommon.Hash{0x55, 0x66}
	for _, root := range []tcommon.Hash{EmptyKVRoot, {}, {0x77}} {
		for _, generation := range []uint64{0, 1, 127, 128, math.MaxUint64} {
			for _, tc := range []struct {
				name string
				core []byte
				hash tcommon.Hash
				code []byte
			}{
				{"zero", accountV5Core(t, nil), tcommon.Hash{}, nil},
				{"empty-code", accountV5Core(t, nil), stateAccountV5EmptyCodeHash, []byte{2}},
				{"reference", accountV5Core(t, hash[:]), hash, []byte{1}},
				{"different", accountV5Core(t, []byte{0x11}), hash, hash[:]},
				{"missing", accountV5Core(t, nil), hash, hash[:]},
				{"empty-core", nil, hash, hash[:]},
				{"opaque-core", []byte{0x01}, hash, hash[:]},
			} {
				t.Run(fmt.Sprintf("%s/%x/%d", tc.name, root[:2], generation), func(t *testing.T) {
					in := StateAccountV3{StateAccountVersion, tc.core, root, generation, tc.hash}
					got, err := in.Encode()
					if err != nil {
						t.Fatal(err)
					}
					rootField := root[:]
					if root == EmptyKVRoot {
						rootField = nil
					}
					// Independent generic RLP oracle for the new explicit schema.
					want, err := rlp.EncodeToBytes([]any{uint64(5), tc.core, rootField, generation, tc.code})
					if err != nil || !bytes.Equal(got, want) {
						t.Fatalf("encoding differs: %x != %x (%v)", got, want, err)
					}
					if len(got) != cap(got) || len(got) != stateAccountV5EncodedSize(tc.core, root, generation, tc.hash) {
						t.Fatal("inexact arena sizing")
					}
					out, err := DecodeStateAccountV3(got)
					if err != nil {
						t.Fatal(err)
					}
					if out.Version != 5 || !bytes.Equal(out.AccountProto, in.AccountProto) || out.AccountKVRoot != root || out.AccountKVGeneration != generation || out.CodeHash != tc.hash {
						t.Fatalf("roundtrip differs: %+v != %+v", out, in)
					}
					var readHash tcommon.Hash
					allocs := testing.AllocsPerRun(100, func() { readHash, err = DecodeStateAccountCodeHash(got) })
					if err != nil || readHash != tc.hash || allocs != 0 {
						t.Fatalf("hash=%x err=%v allocs=%v", readHash, err, allocs)
					}
					old := encodeStateAccountV2Fields(4, tc.core, root, generation, tc.hash)
					if root == EmptyKVRoot && len(tc.code) < 32 && len(old)-len(got) < 64 {
						t.Fatalf("saved only %d, want >=64", len(old)-len(got))
					}
					oldDecoded, err := DecodeStateAccountV3(old)
					if err != nil || oldDecoded.Version != 4 || !bytes.Equal(oldDecoded.AccountProto, tc.core) || oldDecoded.CodeHash != tc.hash || oldDecoded.AccountKVRoot != root {
						t.Fatalf("explicit V4 reader failed: %v", err)
					}
					before := bytes.Clone(out.AccountProto)
					clear(got)
					if !bytes.Equal(before, out.AccountProto) || out.CodeHash != tc.hash {
						t.Fatal("decoded fields retained borrowed input")
					}
				})
			}
		}
	}
}

func TestStateAccountV5RejectsMalformedReferenceAndEnvelope(t *testing.T) {
	hash := tcommon.Hash{0x11}
	core := accountV5Core(t, hash[:])
	for _, tc := range []struct {
		version          uint64
		core, root, code []byte
	}{
		{5, core, nil, []byte{3}}, {5, core, nil, []byte{0}},
		{5, core, []byte{1}, nil}, {5, core, make([]byte, 31), nil},
		{5, nil, nil, []byte{1}}, {5, accountV5Core(t, nil), nil, []byte{1}},
		{5, accountV5Core(t, []byte{1}), nil, []byte{1}},
		{5, append(bytes.Clone(core), 0), nil, []byte{1}},
		{4, core, nil, nil}, {4, core, EmptyKVRoot[:], []byte{1}},
		{3, core, EmptyKVRoot[:], hash[:]}, {6, core, nil, nil},
	} {
		encoded, err := rlp.EncodeToBytes([]any{tc.version, tc.core, tc.root, uint64(0), tc.code})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := DecodeStateAccountV3(encoded); err == nil {
			t.Fatalf("accepted malformed envelope %x", encoded)
		}
		if _, err := DecodeStateAccountCodeHash(encoded); err == nil {
			t.Fatal("hash-only reader accepted malformed envelope")
		}
	}
	valid, _ := (&StateAccountV3{Version: 5, AccountProto: core, AccountKVRoot: EmptyKVRoot, CodeHash: hash}).Encode()
	for n := range valid {
		if _, err := DecodeStateAccountV3(valid[:n]); err == nil {
			t.Fatalf("accepted truncation %d", n)
		}
	}
	if _, err := DecodeStateAccountV3(append(bytes.Clone(valid), 0)); err == nil {
		t.Fatal("accepted trailing byte")
	}
	// A valid V5 reference restores all fields to caller-owned storage, including
	// clearing previously nonzero root/hash/generation when the next row is default.
	var dst StateAccountV3
	if err := decodeStateAccountV3Into(valid, &dst); err != nil {
		t.Fatal(err)
	}
	empty, _ := (&StateAccountV3{Version: 5, AccountKVRoot: EmptyKVRoot}).Encode()
	if err := decodeStateAccountV3Into(empty, &dst); err != nil {
		t.Fatal(err)
	}
	want := StateAccountV3{Version: 5, AccountKVRoot: EmptyKVRoot, AccountProto: []byte{}}
	if !reflect.DeepEqual(dst, want) {
		t.Fatalf("stale fields after reuse: %+v", dst)
	}
}

func TestStateAccountV5PreparedWriterMatchesOrdinaryBothRoots(t *testing.T) {
	hash := tcommon.Hash{0x33}
	for _, flatRoot := range []bool{false, true} {
		for _, inner := range [][]byte{nil, hash[:], {1}} {
			account, err := types.UnmarshalAccountStorageCoreV4(accountV5Core(t, inner))
			if err != nil {
				t.Fatal(err)
			}
			obj := &stateObject{account: account, accountKVRoot: tcommon.Hash{0x77}, accountKVGeneration: math.MaxUint64, codeHash: hash}
			core, _ := account.MarshalStorageCoreV4()
			root := obj.accountKVRoot
			if flatRoot {
				root = EmptyKVRoot
			}
			want := appendStateAccountV2Fields(nil, 5, core, root, obj.accountKVGeneration, hash)
			for _, cached := range []bool{false, true} {
				obj.accountProto = nil
				if cached {
					obj.accountProto = bytes.Clone(core)
				}
				size, coreSize, ok, err := accountLatestObjectEncodedSize(obj, flatRoot)
				if err != nil || !ok || size != len(want) {
					t.Fatalf("size=%d want=%d err=%v", size, len(want), err)
				}
				prefix := []byte{1, 2, 3}
				arena := make([]byte, len(prefix), len(prefix)+size)
				copy(arena, prefix)
				out, ok, err := appendAccountLatestObjectPrepared(arena, obj, flatRoot, coreSize)
				if err != nil || !ok || !bytes.Equal(out[len(prefix):], want) || !bytes.Equal(out[:len(prefix)], prefix) || cap(out) != len(out) {
					t.Fatalf("prepared writer mismatch: %v", err)
				}
				if !bytes.Equal(obj.accountProto, core) {
					t.Fatal("core cache changed")
				}
			}
		}
	}
}

func BenchmarkStateAccountV5Envelope(b *testing.B) {
	hash := tcommon.Hash{0x55}
	for _, contract := range []bool{false, true} {
		var inner []byte
		var codeHash tcommon.Hash
		if contract {
			inner, codeHash = hash[:], hash
		}
		core := accountV5Core(b, inner)
		for _, version := range []uint64{4, 5} {
			v := StateAccountV3{Version: version, AccountProto: core, AccountKVRoot: EmptyKVRoot, CodeHash: codeHash}
			encoded, _ := v.Encode()
			name := fmt.Sprintf("contract=%t/v%d", contract, version)
			b.Run(name+"/encode", func(b *testing.B) {
				b.ReportAllocs()
				b.ReportMetric(float64(len(encoded)), "encoded-B")
				for i := 0; i < b.N; i++ {
					stateAccountV5BytesSink, _ = v.Encode()
				}
			})
			b.Run(name+"/decode", func(b *testing.B) {
				b.ReportAllocs()
				for i := 0; i < b.N; i++ {
					stateAccountV2DecodeSink, _ = DecodeStateAccountV3(encoded)
				}
			})
			b.Run(name+"/hash", func(b *testing.B) {
				b.ReportAllocs()
				for i := 0; i < b.N; i++ {
					stateAccountV2CodeHashSink, _ = DecodeStateAccountCodeHash(encoded)
				}
			})
		}
	}
}

var stateAccountV5BytesSink []byte

// Compare the uncached commit-arena path, including core sizing and encoding.
// V4 uses the previous fixed-hash prefix/trailer; V5 uses the production entry
// points. These synthetic accounts measure codec work, not block import TPS.
func BenchmarkStateAccountV5PreparedCommit(b *testing.B) {
	hash := tcommon.Hash{0x55}
	for _, reference := range []bool{false, true} {
		var inner []byte
		var codeHash tcommon.Hash
		if reference {
			inner, codeHash = hash[:], hash
		}
		account, err := types.UnmarshalAccountStorageCoreV4(accountV5Core(b, inner))
		if err != nil {
			b.Fatal(err)
		}
		for _, version := range []uint64{4, 5} {
			b.Run(fmt.Sprintf("reference=%t/v%d", reference, version), func(b *testing.B) {
				b.ReportAllocs()
				obj := &stateObject{account: account, codeHash: codeHash, accountKVRoot: EmptyKVRoot}
				for i := 0; i < b.N; i++ {
					obj.accountProto = nil
					if version == 5 {
						size, coreSize, ok, err := accountLatestObjectEncodedSize(obj, true)
						if err != nil || !ok {
							b.Fatal(err)
						}
						out, ok, err := appendAccountLatestObjectPrepared(make([]byte, 0, size), obj, true, coreSize)
						if err != nil || !ok {
							b.Fatal(err)
						}
						stateAccountV5BytesSink = out
						continue
					}
					coreSize, err := account.StorageCoreV4Size()
					if err != nil {
						b.Fatal(err)
					}
					size := stateAccountV2EncodedSizeFromProtoSize(4, coreSize, 0)
					out := appendStateAccountV2StorageCorePrefix(make([]byte, 0, size), 4, coreSize, 0)
					start := len(out)
					out, err = account.AppendStorageCoreV4(out)
					if err != nil || len(out)-start != coreSize {
						b.Fatal(err)
					}
					end := len(out)
					out = appendStateAccountV2StorageCoreTrailer(out, EmptyKVRoot, 0, codeHash)
					obj.accountProto = out[start:end:end]
					stateAccountV5BytesSink = out
				}
				b.ReportMetric(float64(len(stateAccountV5BytesSink)), "encoded-B")
			})
		}
	}
}
