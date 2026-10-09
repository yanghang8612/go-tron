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

// HistoryStagingReceiptAuditStats counts distinct trios handled by this one
// audit. Task-local duplicate references do not increment either counter.
// Process-memory checksum hits count in neither field.
type HistoryStagingReceiptAuditStats struct {
	PersistentHits   uint64
	PhysicalSHATrios uint64
}

func (a *HistoryStagingReceiptAudit) Stats() HistoryStagingReceiptAuditStats {
	if a == nil || a.auth == nil {
		return HistoryStagingReceiptAuditStats{}
	}
	return HistoryStagingReceiptAuditStats{PersistentHits: a.auth.persistentHits,
		PhysicalSHATrios: a.auth.physicalSHATrios}
}

func NewHistoryStagingReceiptAudit(manager *Manager) (*HistoryStagingReceiptAudit, error) {
	return newHistoryStagingReceiptAudit(manager, false)
}

// NewHistoryStagingReceiptAuditForceFull ignores persistent and process
// checksum caches and SHA-verifies each unique trio once in this audit.
func NewHistoryStagingReceiptAuditForceFull(manager *Manager) (*HistoryStagingReceiptAudit, error) {
	return newHistoryStagingReceiptAudit(manager, true)
}

func newHistoryStagingReceiptAudit(manager *Manager, forceFull bool) (*HistoryStagingReceiptAudit, error) {
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
			taskVerified: make(map[[32]byte][3]historyStagingFileState),
			persistent:   historyStagingReadCertificates(manager.dir), forceFull: forceFull}, refs: refs}, nil
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

// CommitPhysicalCertificates records the physical checksum facts from a
// completed pinned audit. It deliberately does not persist semantic or RPC
// proof caches. Callers treat write failure as advisory after strong auth.
func (a *HistoryStagingReceiptAudit) CommitPhysicalCertificates(ctx context.Context) error {
	if err := a.RecheckAll(ctx); err != nil {
		return err
	}
	additions := make(map[[32]byte]historyStagingReceiptCertificate, len(a.auth.taskVerified))
	for id, states := range a.auth.taskVerified {
		refs, ok := a.refs[id]
		if !ok {
			return errors.New("snapshots: certificate trio absent from pinned manifest")
		}
		cert, ok := historyStagingMakeCertificate(id, refs, states)
		if !ok {
			return errors.New("snapshots: certificate lacks strong file identity")
		}
		additions[id] = cert
	}
	writeErr := historyStagingWriteCertificates(ctx, a.auth.dir, additions, a.auth.persistent, a.refs)
	if err := a.RecheckAll(ctx); err != nil {
		return err
	}
	return historyStagingAdvisoryWriteError(ctx, writeErr)
}
