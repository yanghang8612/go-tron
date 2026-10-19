package domains

import (
	"errors"
	"strings"
	"testing"

	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
)

func TestMuHashUpdatesAndDeltasPreserveValidationOrder(t *testing.T) {
	db := rawdb.NewMemoryDatabase()
	defer db.Close()
	bad := []byte("invalid physical key")
	if _, err := ApplyMuHashDeltas(db, []rawdb.StateCommitmentDelta{{Key: bad}}, CommitmentIdentity{}, common.Hash{}); !errors.Is(err, rawdb.ErrStateCommitmentHead) {
		t.Fatal("public Deltas did not check HEAD before delta", err)
	}
	if _, err := ApplyMuHashCommitment(db, []rawdb.StateCommitmentUpdate{{Key: bad}}, CommitmentIdentity{}); err == nil || !strings.Contains(err.Error(), "lacks pre-write") {
		t.Fatal("Updates did not reject unknown before before HEAD", err)
	}
	if _, err := ApplyMuHashCommitment(db, []rawdb.StateCommitmentUpdate{{Key: bad, BeforeKnown: true}}, CommitmentIdentity{}); err == nil || !strings.Contains(err.Error(), "invalid latest-domain") {
		t.Fatal("Updates did not validate physical key before HEAD", err)
	}
	if err := rawdb.InitializeStateCommitment(db); err != nil {
		t.Fatal(err)
	}
	key := rawdb.StateAccountLatestCommitmentKey(common.Address{0x41, 1})
	updates := []rawdb.StateCommitmentUpdate{{Key: key, BeforeKnown: true, Value: []byte("a")}, {Key: key, BeforeKnown: true, BeforeExists: true, Before: []byte("wrong"), Value: []byte("b")}}
	if _, err := ApplyMuHashCommitment(db, updates, CommitmentIdentity{}); err == nil || !strings.Contains(err.Error(), "intermediate") {
		t.Fatal("private owned path skipped continuity validation", err)
	}
}
