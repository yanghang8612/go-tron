package core

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/state/snapshots"
)

// VerifyHistoryStagingRuntimeReady is the final startup barrier before API,
// P2P, producer or background lifecycles. It checks every route through the
// materialized head and authenticates each persisted cold binding against one
// pinned manifest. The frozen offline CandidateSHA is not compared with a
// later, legitimately advancing canonical database.
func (bc *BlockChain) VerifyHistoryStagingRuntimeReady(ctx context.Context) (result error) {
	if bc == nil {
		return errors.New("history staging startup: missing blockchain or context")
	}
	// A repeated verification must never preserve an earlier ready bit when
	// any subsequent identity, file, or cancellation check fails.
	bc.historyStagingReady.Store(false)
	if ctx == nil {
		return errors.New("history staging startup: missing blockchain or context")
	}
	manager := bc.HistoryStagingManager()
	if manager == nil {
		return errors.New("history staging startup: manager is not installed")
	}
	current := bc.CurrentBlock()
	if current == nil {
		return errors.New("history staging startup: head is unavailable")
	}
	coldManager, ok := bc.stateCodeColdHistory.(*snapshots.Manager)
	if !ok || coldManager == nil {
		return errors.New("history staging startup: cold manager is unavailable")
	}
	identity, present, err := rawdb.ReadHistoryStagingIdentity(bc.db)
	if err != nil || !present || bc.config == nil || bc.genesisBlock == nil {
		return fmt.Errorf("history staging startup: verified chain identity unavailable: %w", err)
	}
	if identity.NetworkID > uint64(^uint32(0)>>1) {
		return errors.New("history staging startup: network ID exceeds signed manifest field")
	}
	expectedChain := snapshots.ChainIdentity{ChainID: bc.config.ChainID, NetworkID: int32(identity.NetworkID),
		GenesisHash: bc.genesisBlock.Hash().Hex()}
	// A completed replay first removes the previous epoch's active cold tail
	// from the serving manifest under the publication writer gate. Old files
	// remain retired until their durable references can be swept safely.
	if err := snapshots.IsolateHistoryStagingResetColdTail(ctx, coldManager.HistoryStagingDir(), manager, expectedChain); err != nil {
		return fmt.Errorf("history staging startup: isolate reset cold tail: %w", err)
	}
	pinned, release, err := coldManager.PinHistoryReadView()
	if err != nil {
		return err
	}
	if release != nil {
		defer release()
	}
	var receiptAudit *snapshots.HistoryStagingReceiptAudit
	if pinned != nil {
		manifest := pinned.Manifest()
		if manifest == nil {
			return errors.New("history staging startup: pinned cold manifest is unavailable")
		}
		// Legacy unbound manifests are admitted only for staging state history
		// through durable route/binding receipts and true ContentID file checks.
		// Runtime knows the canonical chain, network and genesis, but not the
		// operator's optional fork-config hash. Check only these primary fields;
		// offline admission still compares the complete configured identity.
		if manifest.Chain != nil {
			if manifest.Chain.ChainID != expectedChain.ChainID ||
				manifest.Chain.NetworkID != expectedChain.NetworkID ||
				common.HexToHash(manifest.Chain.GenesisHash) != bc.genesisBlock.Hash() {
				return errors.New("history staging startup: cold manifest primary chain identity mismatch")
			}
		}
		// Keep strong fingerprints for every authenticated trio for this one
		// startup. The process-wide receipt cache is bounded and may evict an
		// earlier trio before a later bucket references it again.
		receiptAudit, err = snapshots.NewHistoryStagingReceiptAudit(pinned)
		if err != nil {
			return fmt.Errorf("history staging startup: create receipt audit: %w", err)
		}
	}
	// A single large trio can take much longer than a bucket to authenticate.
	// Report the current bucket on a timer so a silent pre-API startup does not
	// look hung while the full physical checksum and coverage scan is running.
	started := time.Now()
	headBucket := current.Number() / rawdb.StateHistoryChunkBucketBlocks
	var currentBucket, verifiedBuckets, coldBindings atomic.Uint64
	stop := make(chan struct{})
	stopped := make(chan struct{})
	log.Info("History staging startup audit started", "headBucket", headBucket)
	go func() {
		defer close(stopped)
		ticker := time.NewTicker(30 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-stop:
				return
			case <-ticker.C:
				log.Info("History staging startup audit in progress",
					"currentBucket", currentBucket.Load(), "verifiedBuckets", verifiedBuckets.Load(),
					"headBucket", headBucket, "coldBindings", coldBindings.Load(),
					"elapsed", time.Since(started))
			}
		}
	}()
	defer func() {
		close(stop)
		<-stopped
		var authenticatedTrios int
		if receiptAudit != nil {
			authenticatedTrios = receiptAudit.AuthenticatedTrios()
		}
		log.Info("History staging startup audit finished", "verifiedBuckets", verifiedBuckets.Load(),
			"headBucket", headBucket, "coldBindings", coldBindings.Load(), "authenticatedTrios", authenticatedTrios,
			"elapsed", time.Since(started), "err", result)
	}()
	if err := manager.VerifyHistoryStagingStartup(ctx, current.Number(), func(bucket uint64, route rawdb.HistoryStagingRoute) error {
		currentBucket.Store(bucket)
		if route.ColdBindingEpoch == 0 {
			verifiedBuckets.Add(1)
			return nil
		}
		if pinned == nil {
			return fmt.Errorf("history staging startup: cold binding bucket %d has no manifest", bucket)
		}
		binding, present, err := manager.ReadColdBindingAt(route.Epoch, bucket)
		if err != nil || !present || binding.BindingEpoch != route.ColdBindingEpoch {
			return fmt.Errorf("history staging startup: cold binding bucket %d is missing: %w", bucket, err)
		}
		if receiptAudit == nil {
			return errors.New("history staging startup: cold verifier is unavailable")
		}
		if err := receiptAudit.VerifyBinding(ctx, binding); err != nil {
			return err
		}
		coldBindings.Add(1)
		verifiedBuckets.Add(1)
		return nil
	}); err != nil {
		return err
	}
	// The pinned lease prevents normal retirement, but fail closed if any file
	// authenticated early in this long audit changed before API/P2P is opened.
	if receiptAudit != nil {
		if err := receiptAudit.RecheckAll(ctx); err != nil {
			return fmt.Errorf("history staging startup: recheck authenticated trios: %w", err)
		}
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	bc.historyStagingReady.Store(true)
	return nil
}
