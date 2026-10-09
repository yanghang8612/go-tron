package snapshots

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/tronprotocol/go-tron/core/rawdb"
)

func TestHistoryStagingReceiptAuditRechecksEveryAuthenticatedTrio(t *testing.T) {
	dir, manifest, _, binding := stagingReceiptFixture(t)
	pinned, err := OpenPinnedManager(dir, manifest)
	if err != nil {
		t.Fatal(err)
	}
	audit, err := NewHistoryStagingReceiptAudit(pinned)
	if err != nil {
		t.Fatal(err)
	}
	if err := audit.VerifyBinding(context.Background(), binding); err != nil {
		t.Fatal(err)
	}
	if err := audit.VerifyBinding(context.Background(), binding); err != nil {
		t.Fatal(err)
	}
	if audit.AuthenticatedTrios() != 1 {
		t.Fatalf("expected one task-local authenticated trio, got %d", audit.AuthenticatedTrios())
	}
	if err := audit.RecheckAll(context.Background()); err != nil {
		t.Fatal(err)
	}
	var changed string
	for _, ref := range manifest.Segments {
		if ref.Kind == SegmentHistory && ref.Dataset == SegmentDatasetStateDomainChange {
			changed = filepath.Join(dir, ref.Path)
			break
		}
	}
	if changed == "" {
		t.Fatal("fixture has no cold history file")
	}
	data, err := os.ReadFile(changed)
	if err != nil {
		t.Fatal(err)
	}
	data[8] ^= 0xff
	if err := os.WriteFile(changed, data, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := audit.RecheckAll(context.Background()); err == nil {
		t.Fatal("changed authenticated trio passed final fingerprint check")
	}
	if err := audit.VerifyBinding(context.Background(), binding); err == nil {
		t.Fatal("changed trio passed task-local receipt reuse")
	}
}

func TestHistoryStagingReceiptAuditRequiresPinnedManager(t *testing.T) {
	if _, err := NewHistoryStagingReceiptAudit(nil); err == nil {
		t.Fatal("nil manager accepted")
	}
	var audit *HistoryStagingReceiptAudit
	if err := audit.VerifyBinding(context.Background(), rawdb.HistoryStagingColdBinding{}); err == nil {
		t.Fatal("nil audit accepted")
	}
}
