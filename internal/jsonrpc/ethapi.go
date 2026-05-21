package jsonrpc

// EthAPI implements the "eth" JSON-RPC namespace on the reflection-based
// internal/rpc framework. It is the migration target for the eth_* arms of
// api.go's dispatch switch (jsonrpc-reflection).
//
// This first increment covers the no-parameter, no-hash methods that migrate
// zero-diff against the frozen jsonrpc-corpus. The param-bearing methods
// (getBalance/getCode/getStorageAt/call/estimateGas), the block/tx/receipt
// readers (which additionally FIX the legacy double-hex-hash bug, so their
// corpus entries get regenerated at that point), and the filter methods land
// in follow-up increments.
//
// Method names map by the framework's reflection rule (first letter lowered):
// ChainId -> eth_chainId, BlockNumber -> eth_blockNumber, Syncing ->
// eth_syncing, GasPrice -> eth_gasPrice, Accounts -> eth_accounts.
type EthAPI struct {
	backend Backend
}

// NewEthAPI builds an EthAPI over the given backend.
func NewEthAPI(backend Backend) *EthAPI { return &EthAPI{backend: backend} }

// ChainId serves eth_chainId. It is deliberately named ChainId (not ChainID)
// so the framework's first-letter-lowering yields the canonical method name
// eth_chainId, matching go-ethereum's own EthereumAPI.ChainId.
func (e *EthAPI) ChainId() string { return hexUint64(uint64(e.backend.ChainID())) }

// BlockNumber serves eth_blockNumber: the current head height as 0x-hex.
func (e *EthAPI) BlockNumber() string { return hexUint64(e.backend.BlockNumber()) }

// Syncing serves eth_syncing. go-tron always reports false here, mirroring the
// legacy handler; sync progress is surfaced through the TRON HTTP API instead.
func (e *EthAPI) Syncing() bool { return false }

// GasPrice serves eth_gasPrice: the energy fee in SUN as 0x-hex.
func (e *EthAPI) GasPrice() string { return hexUint64(uint64(e.backend.GasPrice())) }

// Accounts serves eth_accounts: always empty (the node holds no managed keys).
func (e *EthAPI) Accounts() []string { return []string{} }
