package jsonrpc

import (
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/crypto"
)

// clientVersion is the value reported by web3_clientVersion. The legacy
// dispatch handler (api.go) still inlines this literal; the two converge when
// the hand-rolled web3 cases are deleted at cutover.
const clientVersion = "go-tron/v0.3.0-dev"

// Web3API implements the "web3" JSON-RPC namespace on the reflection-based
// internal/rpc framework. It is the migration target for the hand-rolled
// web3_* arms of api.go's dispatch switch (jsonrpc-reflection Slice 3).
//
// The framework maps method names by reflection: an exported method M on a
// service registered under name "web3" is invoked as web3_<m>, where <m> is M
// with its first letter lowered. So ClientVersion -> web3_clientVersion and
// Sha3 -> web3_sha3. Positional params are decoded into the method arguments
// by their Go types; a single 0-or-1-or-2-value return is (result[, error]).
type Web3API struct{}

// ClientVersion serves web3_clientVersion.
func (Web3API) ClientVersion() string { return clientVersion }

// Sha3 serves web3_sha3: keccak256 of the input, returned as 0x-hex. The input
// arrives as a 0x-hex string positional argument, mirroring the legacy handler
// (which json-decodes params[0] then common.FromHex's it); common.FromHex is
// lenient (no error on malformed hex), so no error return is needed.
func (Web3API) Sha3(input string) string {
	return hexBytes(crypto.Keccak256(common.FromHex(input)))
}
