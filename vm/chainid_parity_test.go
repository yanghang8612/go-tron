package vm

import (
	"bytes"
	"testing"

	ethrawdb "github.com/ethereum/go-ethereum/core/rawdb"
	ethdb "github.com/ethereum/go-ethereum/ethdb"
	"github.com/holiman/uint256"
	tcommon "github.com/tronprotocol/go-tron/common"
	tronrawdb "github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/state"
	"github.com/tronprotocol/go-tron/core/types"
	corepb "github.com/tronprotocol/go-tron/proto/core"
)

// chainIDProgram returns CHAINID; MSTORE@0; RETURN 32@0.
func chainIDProgram() []byte {
	return []byte{byte(CHAINID), byte(PUSH1), 0x00, byte(MSTORE), byte(PUSH1), 0x20, byte(PUSH1), 0x00, byte(RETURN)}
}

// fakeBlockHashReader is a KVReadWriter that also implements
// rawdb.BlockHashReader, returning a fully-controlled genesis hash so the
// CHAINID parity tests can pin the low 4 bytes (mainnet 0x2b6653dc, Nile
// 0xcd8690dc) independently of how a real block header would hash.
type fakeBlockHashReader struct {
	ethdb.Database
	genesis tcommon.Hash
}

func (f *fakeBlockHashReader) BlockHashByNumber(number uint64) (tcommon.Hash, bool) {
	if number == 0 {
		return f.genesis, true
	}
	return tcommon.Hash{}, false
}

func newChainIDTVM(t *testing.T, cfg TVMConfig, chainID int64) *TVM {
	t.Helper()
	diskdb := ethrawdb.NewMemoryDatabase()
	db := state.NewDatabase(diskdb)
	sdb, err := state.New(tcommon.Hash{}, db)
	if err != nil {
		t.Fatal(err)
	}
	return NewTVM(sdb, nil, tcommon.Address{}, 1, 1000, tcommon.Address{}, chainID, cfg)
}

func runChainID(t *testing.T, evm *TVM) []byte {
	t.Helper()
	contract := NewContract(tcommon.Address{0x41, 0x01}, tcommon.Address{0x41, 0x02}, 0, 100000)
	contract.SetCode(tcommon.Address{0x41, 0x02}, chainIDProgram())
	ret, err := evm.interpreter.Run(contract)
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	return ret
}

// genesisWithHashTail builds a genesis hash whose low 4 bytes equal tail. The
// leading bytes are arbitrary (they must NOT influence the post-fork result,
// which is copyOfRange(hash, len-4, len) per java Program.getChainId).
func genesisWithHashTail(tail [4]byte) tcommon.Hash {
	var h tcommon.Hash
	for i := range h {
		h[i] = byte(0xA0 + (i % 7)) // distinctive non-zero high bytes
	}
	copy(h[len(h)-4:], tail[:])
	return h
}

// TestChainIDPostForkReturnsGenesisLowFourBytes is the parity golden:
// java-tron Program.getChainId (actuator/.../vm/program/Program.java:1391-1397)
// post-fork returns Arrays.copyOfRange(genesisBlockId, len-4, len) — the LOW 4
// bytes of the genesis block hash, NOT the numeric chain id. Mainnet genesis
// hash tail is 0x2b6653dc (728126428); Nile is 0xcd8690dc (3448148188).
//
// The numeric ChainID is set to a sentinel that differs from the hash tail, so
// a regression to `uint256(tvm.ChainID)` makes this fail.
func TestChainIDPostForkReturnsGenesisLowFourBytes(t *testing.T) {
	cases := []struct {
		name string
		tail [4]byte
		want uint64
		cfg  TVMConfig
	}{
		{"mainnet/compatible", [4]byte{0x2b, 0x66, 0x53, 0xdc}, 728126428, TVMConfig{Istanbul: true, Compatibility: true}},
		{"mainnet/optimized", [4]byte{0x2b, 0x66, 0x53, 0xdc}, 728126428, TVMConfig{Istanbul: true, OptimizedReturnValueOfChainId: true}},
		{"nile/compatible", [4]byte{0xcd, 0x86, 0x90, 0xdc}, 3448148188, TVMConfig{Istanbul: true, Compatibility: true}},
		{"nile/optimized", [4]byte{0xcd, 0x86, 0x90, 0xdc}, 3448148188, TVMConfig{Istanbul: true, OptimizedReturnValueOfChainId: true}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			// Sentinel numeric chain id that is NOT the hash tail.
			const sentinelChainID int64 = 0x77777777
			evm := newChainIDTVM(t, tc.cfg, sentinelChainID)
			store := &fakeBlockHashReader{
				Database: ethrawdb.NewMemoryDatabase(),
				genesis:  genesisWithHashTail(tc.tail),
			}
			evm.SetDB(store)

			got := new(uint256.Int).SetBytes(runChainID(t, evm)).Uint64()
			if got != tc.want {
				t.Fatalf("post-fork CHAINID: got %#x (%d) want %#x (%d) — must be genesis-hash low 4 bytes, not numeric ChainID %#x",
					got, got, tc.want, tc.want, sentinelChainID)
			}
		})
	}
}

// TestChainIDPreForkReturnsFullGenesisHash pins the pre-fork (both gates off)
// behavior: java returns the UNTRUNCATED genesis block id (full 32 bytes). This
// must NOT regress when the post-fork branch is fixed.
func TestChainIDPreForkReturnsFullGenesisHash(t *testing.T) {
	genesis := genesisWithHashTail([4]byte{0x2b, 0x66, 0x53, 0xdc})
	store := &fakeBlockHashReader{
		Database: ethrawdb.NewMemoryDatabase(),
		genesis:  genesis,
	}
	// Both Compatibility and OptimizedReturnValueOfChainId are false.
	evm := newChainIDTVM(t, TVMConfig{Istanbul: true}, 0x77777777)
	evm.SetDB(store)

	got := runChainID(t, evm)
	if want := genesis.Bytes(); !bytes.Equal(got, want) {
		t.Fatalf("pre-fork CHAINID:\n got  %x\n want %x (full genesis hash)", got, want)
	}
}

// TestChainIDPostForkReadBlockKVFallback verifies the freezer-safe secondary
// path: when the store does NOT implement BlockHashReader, the genesis hash is
// read via ReadBlockKV (a seeded b-<0> row) and the post-fork low-4-bytes
// truncation still applies. This is the path the existing memdb-backed tests
// exercise; here we assert the truncation, not the numeric fallback.
func TestChainIDPostForkReadBlockKVFallback(t *testing.T) {
	diskdb := ethrawdb.NewMemoryDatabase()
	db := state.NewDatabase(diskdb)
	sdb, err := state.New(tcommon.Hash{}, db)
	if err != nil {
		t.Fatal(err)
	}
	genesis := types.NewBlockFromPB(&corepb.Block{
		BlockHeader: &corepb.BlockHeader{
			RawData: &corepb.BlockHeaderRaw{
				Number:     0,
				Timestamp:  123,
				ParentHash: []byte("parent-hash-for-chainid-parity"),
			},
		},
	})
	if err := tronrawdb.WriteBlock(diskdb, genesis); err != nil {
		t.Fatal(err)
	}
	// memdb does NOT implement BlockHashReader → ReadBlockKV path.
	evm := NewTVM(sdb, nil, tcommon.Address{}, 1, 1000, tcommon.Address{}, 0x77777777, TVMConfig{Istanbul: true, OptimizedReturnValueOfChainId: true})
	evm.SetDB(diskdb)

	// The RETURN pushes the value as a 32-byte big-endian word; compare the
	// numeric low-4-bytes (leading zeros normalized), mirroring the optimized
	// test above.
	got := new(uint256.Int).SetBytes(runChainID(t, evm))
	want := new(uint256.Int).SetBytes(genesis.Hash().Bytes()[len(genesis.Hash())-4:])
	if got.Cmp(want) != 0 {
		t.Fatalf("post-fork CHAINID via ReadBlockKV:\n got  %x\n want %x (genesis hash low 4 bytes)", got.Bytes(), want.Bytes())
	}
}

// TestChainIDPostForkNumericFallbackWhenNoDB documents the last-resort: with no
// DB at all, java's getContractState().getBlockByNum(0) would itself be
// unavailable; go falls back to the numeric ChainID so the opcode still
// produces a value rather than panicking. (Production always has a DB.)
func TestChainIDPostForkNumericFallbackWhenNoDB(t *testing.T) {
	evm := newChainIDTVM(t, TVMConfig{Istanbul: true, OptimizedReturnValueOfChainId: true}, 0x01020304)
	// No SetDB → tvm.DB == nil.
	got := new(uint256.Int).SetBytes(runChainID(t, evm)).Uint64()
	if got != 0x01020304 {
		t.Fatalf("post-fork CHAINID no-DB fallback: got %#x want %#x", got, uint64(0x01020304))
	}
}
