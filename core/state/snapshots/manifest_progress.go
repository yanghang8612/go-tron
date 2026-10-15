package snapshots

import (
	"encoding/json"
	"os"
	"unicode/utf8"
)

// The only mutations allowed here are monotonic progress fields. The production
// loader authenticates the complete current file and validates its catalog.
// Sharing that immutable catalog avoids cloning/sorting/revalidating it; this
// private generation never escapes to an API that permits caller mutation.
func updateManifestPruneProgress(dir string, blockNum, txNum, indexBlock uint64) error {
	return updateManifestPruneProgressWithSync(dir, blockNum, txNum, indexBlock, syncSnapshotDir)
}

// An explicit operation dependency permits fault tests without mutable hooks.
func updateManifestPruneProgressWithSync(dir string, blockNum, txNum, indexBlock uint64, syncDir func(string) error) error {
	if dir == "" {
		return nil
	}
	manifestPublicationMu.Lock()
	defer manifestPublicationMu.Unlock()
	prior, err := loadProductionManifestReadOnly(dir)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return err
	}
	progress := Progress{}
	if prior.Progress != nil {
		progress = *prior.Progress
	}
	next := progress
	next.HotPruneBlockNum = max(next.HotPruneBlockNum, blockNum)
	next.HotPruneTxNum = max(next.HotPruneTxNum, txNum)
	next.StateChangeIndexPruneBlockNum = max(next.StateChangeIndexPruneBlockNum, indexBlock)
	if next == progress {
		// A previous rename may have succeeded before directory fsync failed.
		// Reconfirm durability even when current bytes already contain this cursor.
		return syncDir(dir)
	}
	// Trusted reuse is optional: disabled/rejected/replaced cache generations
	// retain the ordinary public validation/ownership path.
	budget, configErr := manifestCacheConfiguredBudget()
	c := &loadedManifestCache
	c.mu.Lock()
	trusted := configErr == nil && budget != 0 && prior.Generation != 0 && prior.PublishedUnix != 0 && c.manifest == prior && c.productionErr == nil && utf8.Valid(c.data)
	c.mu.Unlock()
	if !trusted {
		owned := cloneDecodedManifest(prior)
		owned.Progress = &next
		return publishManifestLockedWithSync(dir, owned, syncDir)
	}
	m := *prior
	m.Progress = &next
	// The old lookup is valid for exactly these unchanged catalogs.
	data, err := json.Marshal(&m)
	if err != nil {
		return err
	}
	if err := writeManifestBytesWithSync(dir, data, syncDir); err != nil {
		return err
	}
	rememberManifestProgress(data, &m)
	return nil
}

// Unlike public publication, m and every catalog it references are already
// immutable owned generations. No caller owns these containers or strings.
func rememberManifestProgress(data []byte, m *Manifest) {
	budget, err := manifestCacheConfiguredBudget()
	if err != nil {
		return
	}
	c := &loadedManifestCache
	c.mu.Lock()
	defer c.mu.Unlock()
	c.resizeLocked(budget)
	charge := manifestCacheCharge(data, m)
	manifestCacheCandidate.Update(manifestCacheMetricBytes(charge))
	if budget == 0 || charge > budget {
		c.clearLocked()
		if budget != 0 {
			manifestCacheBypasses.Inc(1)
			manifestCacheRejections.Inc(1)
			manifestCacheRejectedCharge.Update(manifestCacheMetricBytes(charge))
		}
		manifestCachePublicationBypasses.Inc(1)
		return
	}
	c.data, c.manifest, c.productionErr, c.charge = data, m, nil, charge
	c.publication = true
	manifestCacheResident.Update(int64(charge))
	manifestCacheHeadroom.Update(int64(budget - charge))
	manifestCachePublicationSeeds.Inc(1)
}
