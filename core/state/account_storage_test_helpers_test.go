package state

import (
	"testing"

	"github.com/tronprotocol/go-tron/core/types"
	corepb "github.com/tronprotocol/go-tron/proto/core"
)

func decodeStoredAccountCore(t testing.TB, envelope *StateAccountV3) *corepb.Account {
	t.Helper()
	account, err := types.UnmarshalAccountStorageCoreV4(envelope.AccountProto)
	if err != nil {
		t.Fatal(err)
	}
	return account.Proto()
}
