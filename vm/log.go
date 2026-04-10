package vm

import tcommon "github.com/tronprotocol/go-tron/common"

// Log represents a contract log event emitted by LOG0-LOG4.
type Log struct {
	Address tcommon.Address
	Topics  [][]byte
	Data    []byte
}
