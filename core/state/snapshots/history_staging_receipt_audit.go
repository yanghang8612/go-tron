package snapshots

import (
	"context"
	"errors"
	"fmt"

	"github.com/tronprotocol/go-tron/core/rawdb"
)

// HistoryStagingReceiptAudit is a task-local, pinned-manifest authentication
// session for startup or offline cleanup. It reuses the durable receipt
// verifier while retaining every authenticated trio's strong file fingerprint,
// independent of the bounded process-wide cache. It is single-goroutine only.
type HistoryStagingReceiptAudit struct {
	manager *Manager
	auth    *historyStagingReceiptAuthenticator
	refs    map[[32]byte][3]SegmentRef
}

func NewHistoryStagingReceiptAudit(manager *Manager) (*HistoryStagingReceiptAudit, error) {
	if manager == nil || !manager.pinned {
		return nil, errors.New("snapshots: receipt audit requires pinned manager")
	}
	manifest, err := manager.currentManifest()
	if err != nil || manifest == nil {
		return nil, errors.New("snapshots: receipt audit has no pinned manifest")
	}
	refs, err := historyStagingManifestTrioIndex(manager.dir, manifest)
	if err != nil {
		return nil, err
	}
	return &HistoryStagingReceiptAudit{manager: manager,
		auth: &historyStagingReceiptAuthenticator{dir: manager.dir, manifest: manifest,
			taskVerified: make(map[[32]byte][3]historyStagingFileState)}, refs: refs}, nil
}

// VerifyBinding checks an existing durable semantic receipt and its physical
// trio. The caller must load the binding through a validated current-epoch
// route/binding; SOURCE or TARGET routes may also carry a certified cold span.
// This method cannot certify newly supplied semantic commitments for
// publication. A separate TARGET migration receipt is not required.
func (a *HistoryStagingReceiptAudit) VerifyBinding(ctx context.Context, binding rawdb.HistoryStagingColdBinding) error {
	if a == nil || a.manager == nil || ctx == nil {
		return errors.New("snapshots: missing receipt audit")
	}
	return a.manager.verifyHistoryStagingPinnedBindingReceipt(ctx, binding, a.auth)
}

// RecheckAll rejects any physical-file change since the first successful
// checksum authentication. It performs metadata checks only, so the original
// checksum work is not repeated before the writer transition or after close.
func (a *HistoryStagingReceiptAudit) RecheckAll(ctx context.Context) error {
	if a == nil || a.auth == nil || ctx == nil {
		return errors.New("snapshots: missing receipt audit")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	for id, states := range a.auth.taskVerified {
		if err := ctx.Err(); err != nil {
			return err
		}
		refs, ok := a.refs[id]
		if !ok {
			return errors.New("snapshots: authenticated trio disappeared from pinned manifest")
		}
		if err := historyStagingCheckFileStates(a.auth.dir, refs, states); err != nil {
			return fmt.Errorf("snapshots: receipt audit trio changed: %w", err)
		}
	}
	return nil
}

func (a *HistoryStagingReceiptAudit) AuthenticatedTrios() int {
	if a == nil || a.auth == nil {
		return 0
	}
	return len(a.auth.taskVerified)
}
