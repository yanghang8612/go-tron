package domains

import (
	"github.com/ethereum/go-ethereum/ethdb"
)

type CommitmentDB interface {
	ethdb.KeyValueReader
	ethdb.KeyValueWriter
	ethdb.Iteratee
}
