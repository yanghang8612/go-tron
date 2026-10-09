package main

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"time"

	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/rawdb/etl"
	"github.com/tronprotocol/go-tron/core/state/snapshots"
)

func (e repairExecution) fence(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := errors.Join(e.hold.Recheck(), checkCleanupOperationLock(e.lock, e.lockPath)); err != nil {
		return err
	}
	for _, path := range []string{e.hotPath, e.stagePath, e.coldPath} {
		free, err := cleanupFreeBytes(path)
		if err != nil || free < e.minFree {
			return fmt.Errorf("repair: free-space floor on %s: have %d: %w", path, free, errors.Join(err, rawdb.ErrHistoryStagingIncomplete))
		}
	}
	return nil
}

func (e repairExecution) manifestSHA(want [32]byte) error {
	got, err := cleanupFileSHA256(filepath.Join(e.coldPath, snapshots.ManifestFile))
	if err != nil || got != want {
		return errors.Join(fmt.Errorf("repair: manifest SHA changed: actual=%x expected=%x", got, want), err)
	}
	return nil
}

func (e repairExecution) checkpoint(phase string) error {
	if e.phaseHook != nil {
		return e.phaseHook(phase)
	}
	return nil
}

func repairNewHistoryPath(tag string, from, to uint64) (string, error) {
	var nonce [8]byte
	if _, err := rand.Read(nonce[:]); err != nil {
		return "", err
	}
	return fmt.Sprintf("history/state-domain-change-repair-%s-%d-%d-%x.seg", tag, from, to, nonce), nil
}

func repairBuildReplacements(ctx context.Context, e repairExecution, slices []repairTargetSlice) ([]snapshots.SegmentRef, error) {
	opts := etl.Options{TempDir: filepath.Join(e.coldPath, "etl"), BufferLimit: 32 << 20, BatchSize: 4 << 20}
	var refs []snapshots.SegmentRef
	if e.plan.Left != nil {
		left, err := repairCopyBoundary(ctx, e, e.plan.Left, "left", opts)
		if err != nil {
			return nil, err
		}
		refs = append(refs, left...)
	}
	for _, item := range slices {
		if err := e.fence(ctx); err != nil {
			return nil, err
		}
		blocks, err := e.manager.ReadBucketBlockProofs(ctx, item.Bucket)
		if err != nil {
			return nil, err
		}
		first := item.FromBlock - item.Bucket*rawdb.StateHistoryChunkBucketBlocks
		last := item.ToBlock - item.Bucket*rawdb.StateHistoryChunkBucketBlocks
		if last >= uint64(len(blocks)) || blocks[first].BeginTxNum != item.FromTxNum || blocks[last].EndTxNum != item.ToTxNum {
			return nil, fmt.Errorf("repair: TARGET bucket %d slice differs from pinned tx ranges", item.Bucket)
		}
		// Bound the 256 MiB metadata spool by keeping each immutable output
		// small. Very dense 128-block slices may still exceed its hard limit;
		// that fails closed without truncating a canonical block.
		for start := first; start <= last; {
			end := min(start+127, last)
			fromTx, toTx := blocks[start].BeginTxNum, blocks[end].EndTxNum
			view, err := e.manager.AcquireTargetBucketView(e.routes.Epoch, item.Bucket)
			if err != nil {
				return nil, err
			}
			path, err := repairNewHistoryPath("target", fromTx, toTx)
			if err != nil {
				_ = view.Close()
				return nil, err
			}
			part, _, buildErr := snapshots.BuildStateHistoryReferenceTrioReadContext(ctx, view, e.coldPath, fromTx, toTx, blocks[start].Number, blocks[end].Number, path, opts, 0)
			if err := errors.Join(buildErr, view.Close()); err != nil {
				return nil, fmt.Errorf("repair: build TARGET bucket %d blocks [%d,%d]: %w", item.Bucket, blocks[start].Number, blocks[end].Number, err)
			}
			refs = append(refs, part...)
			start = end + 1
		}
	}
	if e.plan.Right != nil {
		right, err := repairCopyBoundary(ctx, e, e.plan.Right, "right", opts)
		if err != nil {
			return nil, err
		}
		refs = append(refs, right...)
	}
	if err := snapshots.SyncHistorySegmentDirectories(e.coldPath, refs); err != nil {
		return nil, err
	}
	return refs, nil
}

func repairCopyBoundary(ctx context.Context, e repairExecution, boundary *snapshots.OfflineHistoryRepairSlice, tag string, opts etl.Options) ([]snapshots.SegmentRef, error) {
	parts, err := snapshots.PlanOfflineHistoryRepairBoundaryBlockSlices(ctx, e.coldPath, boundary.SourceRefs, boundary.FromTxNum, boundary.ToTxNum, 128)
	if err != nil {
		return nil, fmt.Errorf("repair: plan %s immutable boundary: %w", tag, err)
	}
	var refs []snapshots.SegmentRef
	for _, part := range parts {
		if err := e.fence(ctx); err != nil {
			return nil, err
		}
		path, err := repairNewHistoryPath(tag, part.FromTxNum, part.ToTxNum)
		if err != nil {
			return nil, err
		}
		copied, err := snapshots.CopyStateHistoryReferenceTrioRangeContext(ctx, e.coldPath, boundary.SourceRefs, part.FromTxNum, part.ToTxNum, path, opts)
		if err != nil {
			return nil, fmt.Errorf("repair: copy %s immutable blocks [%d,%d]: %w", tag, part.FromBlock, part.ToBlock, err)
		}
		refs = append(refs, copied...)
	}
	return refs, nil
}

func repairVerifyNewRefs(ctx context.Context, dir string, manifest *snapshots.Manifest, refs []snapshots.SegmentRef) error {
	if len(refs)%3 != 0 {
		return errors.New("repair: incomplete replacement trios")
	}
	if manifest == nil {
		return errors.New("repair: replacement manifest missing")
	}
	if err := snapshots.AuthenticateOfflineHistoryRepairTrios(ctx, dir, refs); err != nil {
		return fmt.Errorf("repair: replacement trio authentication: %w", err)
	}
	return nil
}

func repairCandidateBindings(ctx context.Context, e repairExecution, chain *rawdb.ChainDB, candidate *snapshots.Manifest, slices []repairTargetSlice, expected *repairJournal, audit *snapshots.HistoryStagingReceiptAudit) ([]rawdb.HistoryStagingColdBinding, error) {
	if len(slices) != len(e.routes.Buckets) {
		return nil, errors.New("repair: incomplete TARGET slice list")
	}
	if expected != nil && audit == nil {
		return nil, errors.New("repair: resumed durable binding needs a pinned receipt audit")
	}
	bindings := make([]rawdb.HistoryStagingColdBinding, 0, len(e.routes.Buckets))
	for i, row := range e.routes.Buckets {
		blocks, err := retireCanonicalBlocks(ctx, e.manager, chain, row.Bucket)
		if err != nil {
			return nil, err
		}
		item := slices[i]
		if item.Bucket != row.Bucket {
			return nil, fmt.Errorf("repair: TARGET slice bucket %d differs from route %d", item.Bucket, row.Bucket)
		}
		var prior *rawdb.HistoryStagingColdBinding
		if expected != nil {
			prior = &expected.Bindings[i]
			if prior.Bucket != row.Bucket || prior.ManifestEpoch != candidate.Generation {
				return nil, fmt.Errorf("repair: frozen bucket %d proof identity differs", row.Bucket)
			}
			// A preceding interrupted pass may already have durably certified
			// this whole prepared union. In that case the authenticated route
			// receipt, not the private journal, is the semantic authority.
			if row.OldBinding != nil && row.OldBinding.ManifestEpoch == candidate.Generation && reflect.DeepEqual(row.OldBinding.Spans, prior.Spans) {
				if err := audit.VerifyBinding(ctx, *row.OldBinding); err != nil {
					return nil, err
				}
				bindings = append(bindings, *row.OldBinding)
				continue
			}
		}
		mask := make([]bool, len(blocks))
		first := item.FromBlock - blocks[0].Number
		last := item.ToBlock - blocks[0].Number
		if last >= uint64(len(blocks)) {
			return nil, fmt.Errorf("repair: TARGET slice bucket %d escaped canonical proof", row.Bucket)
		}
		for b := first; b <= last; b++ {
			mask[b] = true
		}
		view, err := e.manager.AcquireTargetBucketView(e.routes.Epoch, row.Bucket)
		if err != nil {
			return nil, err
		}
		newSpans, proofErr := snapshots.VerifyHistoryStagingTargetColdRange(ctx, view, e.coldPath, candidate, blocks, mask)
		if err := errors.Join(proofErr, view.Close()); err != nil {
			return nil, fmt.Errorf("repair: TARGET/cold equivalence bucket %d: %w", row.Bucket, err)
		}
		var oldSpans []rawdb.HistoryStagingColdSpan
		if row.OldBinding != nil {
			rebound, err := snapshots.RebindHistoryStagingColdBinding(ctx, e.coldPath, candidate, *row.OldBinding, blocks)
			if err != nil {
				return nil, fmt.Errorf("repair: old certified span changed in bucket %d: %w", row.Bucket, err)
			}
			oldSpans = rebound.Spans
		}
		// The union is precisely the previously durable spans plus this
		// batch's TARGET block mask. No other block, including an unrepaired
		// bad tail in the same bucket, may enter the new semantic binding.
		spans := append(append([]rawdb.HistoryStagingColdSpan(nil), oldSpans...), newSpans...)
		sort.Slice(spans, func(i, j int) bool { return spans[i].From < spans[j].From })
		marked := make([]bool, len(blocks))
		for _, span := range oldSpans {
			for b := span.From - blocks[0].Number; b <= span.To-blocks[0].Number; b++ {
				if b >= uint64(len(blocks)) || marked[b] || mask[b] {
					return nil, fmt.Errorf("repair: old cold span overlaps current TARGET mask in bucket %d", row.Bucket)
				}
				marked[b] = true
			}
		}
		for _, span := range newSpans {
			for b := span.From - blocks[0].Number; b <= span.To-blocks[0].Number; b++ {
				if b >= uint64(len(blocks)) || marked[b] || !mask[b] {
					return nil, fmt.Errorf("repair: new cold span escaped TARGET mask in bucket %d", row.Bucket)
				}
				marked[b] = true
			}
		}
		for b, needed := range mask {
			if needed && !marked[b] {
				return nil, fmt.Errorf("repair: TARGET block %d was not semantically certified", blocks[b].Number)
			}
		}
		bindingEpoch := uint64(1)
		if row.OldBinding != nil {
			if row.OldBinding.BindingEpoch == ^uint64(0) {
				return nil, errors.New("repair: binding epoch overflow")
			}
			bindingEpoch = row.OldBinding.BindingEpoch + 1
		}
		binding := rawdb.HistoryStagingColdBinding{Version: rawdb.HistoryStagingFormatVersion, Bucket: row.Bucket, Epoch: e.routes.Epoch, BindingEpoch: bindingEpoch, ManifestEpoch: candidate.Generation, Spans: spans}
		if expected != nil {
			if !reflect.DeepEqual(prior.Spans, binding.Spans) {
				return nil, fmt.Errorf("repair: resumed semantic proof differs for bucket %d", binding.Bucket)
			}
		}
		bindings = append(bindings, binding)
	}
	return bindings, nil
}

func repairCanonicalDigests(ctx context.Context, p *retirePlan, m *rawdb.HistoryStagingManager, chain *rawdb.ChainDB) error {
	for i := range p.Buckets {
		blocks, err := retireCanonicalBlocks(ctx, m, chain, p.Buckets[i].Bucket)
		if err != nil {
			return err
		}
		p.Buckets[i].CanonicalDigest, err = retireBlocksDigest(p.Buckets[i].Bucket, blocks)
		if err != nil {
			return err
		}
	}
	return nil
}

func recheckRepairCanonical(ctx context.Context, p retirePlan, m *rawdb.HistoryStagingManager, chain *rawdb.ChainDB) error {
	for _, row := range p.Buckets {
		blocks, err := retireCanonicalBlocks(ctx, m, chain, row.Bucket)
		if err != nil {
			return err
		}
		digest, err := retireBlocksDigest(row.Bucket, blocks)
		if err != nil || digest != row.CanonicalDigest {
			return errors.Join(fmt.Errorf("repair: bucket %d canonical proof changed", row.Bucket), err)
		}
	}
	return nil
}

func executeRepairTargetCold(ctx context.Context, report *repairReport, e repairExecution) (retErr error) {
	if e.ancientPath == "" {
		return errors.New("repair: --ancient-dir is required for canonical proofs")
	}
	ancient, closeAncient, ancientPath, err := openRetireAncient(e.ancientPath)
	if err != nil {
		return err
	}
	defer func() { retErr = errors.Join(retErr, closeAncient()) }()
	if ancientPath == "" {
		return errors.New("repair: missing canonical freezer")
	}
	if err := rawdb.ValidateHistoryStagingPaths(e.hotPath, e.stagePath, ancientPath); err != nil {
		return err
	}
	chain := rawdb.NewChainDB(*e.hotDB, ancient)
	if err := repairCanonicalDigests(ctx, &e.routes, e.manager, chain); err != nil {
		return err
	}
	if err := e.fence(ctx); err != nil {
		return err
	}
	proofCtx, facts, err := snapshots.WithHistoryStagingPhysicalFacts(ctx, e.coldPath)
	if err != nil {
		return err
	}
	var candidate *snapshots.Manifest
	var j repairJournal
	var receiptAudit *snapshots.HistoryStagingReceiptAudit
	if e.journalPresent {
		j = e.journal
		report.TargetSlices = j.Slices
		if err := validateRepairTargetSlices(proofCtx, j.Slices, e.routes, e.manager, chain, e.fromTx, e.toTx); err != nil {
			return err
		}
		if e.currentSHA == j.OldSHA {
			candidate, err = snapshots.PrepareOfflineHistoryRepairManifest(e.manifest, e.plan, j.NewRefs, j.PublishedUnix)
			if err != nil {
				return err
			}
		} else {
			candidate = e.manifest
		}
		serialized, err := json.Marshal(candidate)
		if err != nil || sha256.Sum256(serialized) != j.CandidateSHA {
			return errors.Join(errors.New("repair: resumed candidate manifest differs from journal"), err)
		}
		if err := repairVerifyNewRefs(proofCtx, e.coldPath, candidate, j.NewRefs); err != nil {
			return err
		}
		// The private journal says which boundary outputs were copied; it is
		// not itself evidence that a prior process copied them correctly.
		if err := snapshots.VerifyOfflineHistoryRepairBoundaryCopies(proofCtx, e.coldPath, e.plan, j.NewRefs); err != nil {
			return fmt.Errorf("repair: resumed old-cold boundary copy differs: %w", err)
		}
	} else {
		slices, err := selectRepairTargetSlices(proofCtx, e.routes, e.manager, chain, e.fromTx, e.toTx)
		if err != nil {
			return err
		}
		if err := validateRepairTargetSlices(proofCtx, slices, e.routes, e.manager, chain, e.fromTx, e.toTx); err != nil {
			return err
		}
		report.TargetSlices = slices
		if err := snapshots.AuthenticateOfflineHistoryRepairTrios(proofCtx, e.coldPath, e.plan.SourceRefs); err != nil {
			return fmt.Errorf("repair: original trio authentication: %w", err)
		}
		report.Phase = "build"
		refs, err := repairBuildReplacements(proofCtx, e, slices)
		if err != nil {
			return err
		}
		candidate, err = snapshots.PrepareOfflineHistoryRepairManifest(e.manifest, e.plan, refs, time.Now().UTC().Unix())
		if err != nil {
			return err
		}
		if err := repairVerifyNewRefs(proofCtx, e.coldPath, candidate, refs); err != nil {
			return err
		}
		bindings, err := repairCandidateBindings(proofCtx, e, chain, candidate, slices, nil, nil)
		if err != nil {
			return err
		}
		serialized, err := json.Marshal(candidate)
		if err != nil {
			return err
		}
		planSHA, err := repairPlanSHA(e.plan, e.routes)
		if err != nil {
			return err
		}
		otherSHA, err := repairOtherRefsSHA(e.manifest, e.plan.SourceRefs, refs)
		if err != nil {
			return err
		}
		j = repairJournal{Version: 1, Phase: journalPrepared, OldSHA: e.currentSHA, CandidateSHA: sha256.Sum256(serialized), PlanSHA: planSHA, Epoch: e.routes.Epoch, PublishedUnix: candidate.PublishedUnix,
			Protected: e.routes.Protected, Barrier: e.routes.Barrier, Plan: *e.plan, NewRefs: refs, Bindings: bindings, Slices: slices, OtherRefsSHA: otherSHA}
		if err := errors.Join(facts.RecheckAll(proofCtx), e.fence(proofCtx), e.manifestSHA(e.currentSHA)); err != nil {
			return err
		}
		if err := writeRepairJournal(e.coldPath, j); err != nil {
			return err
		}
		if err := e.checkpoint(journalPrepared); err != nil {
			return err
		}
	}
	// A resumed publication cannot borrow the private journal's semantic
	// assertion. Recheck the exact immutable candidate against the pinned
	// TARGET rows (or a previously durable full binding) before reopening hot
	// writable. This also authenticates candidate files after a process crash.
	if e.journalPresent {
		pinned, err := snapshots.OpenPinnedManager(e.coldPath, candidate)
		if err != nil {
			return err
		}
		receiptAudit, err = snapshots.NewHistoryStagingReceiptAudit(pinned)
		if err != nil {
			return err
		}
		if _, err := repairCandidateBindings(proofCtx, e, chain, candidate, j.Slices, &j, receiptAudit); err != nil {
			return err
		}
	}
	report.CandidateSHA256 = hex.EncodeToString(j.CandidateSHA[:])
	report.JournalPhase = j.Phase
	if err := e.fence(ctx); err != nil {
		return err
	}
	if e.currentSHA == j.OldSHA {
		if err := e.manifestSHA(j.OldSHA); err != nil {
			return err
		}
	}
	// All immutable output names are synced before any catalog write. The
	// operation journal is durable first, so a crash at either manifest handoff
	// can only resume the exact old or exact candidate SHA.
	if err := snapshots.SyncHistorySegmentDirectories(e.coldPath, j.NewRefs); err != nil {
		return err
	}
	if err := (*e.hotDB).Close(); err != nil {
		return err
	}
	*e.hotDB = nil
	// The stage payload store remains read-only and locked for the entire
	// operation. Only hot route/binding metadata needs a writable handle.
	defer func() {
		if *e.hotDB != nil {
			retErr = errors.Join(retErr, (*e.hotDB).Close())
			*e.hotDB = nil
		}
		if *e.stageDB != nil {
			retErr = errors.Join(retErr, (*e.stageDB).Close())
			*e.stageDB = nil
		}
		verifyCtx, cancel := context.WithTimeout(context.Background(), 2*time.Hour)
		defer cancel()
		finalProof := func(checkCtx context.Context) error {
			checks := []error{facts.RecheckAll(checkCtx)}
			if receiptAudit != nil {
				checks = append(checks, receiptAudit.RecheckAll(checkCtx))
			}
			return errors.Join(checks...)
		}
		retErr = errors.Join(retErr, verifyRepairReopen(verifyCtx, report, e, j, retErr == nil, finalProof))
	}()
	if err := e.fence(ctx); err != nil {
		return err
	}
	tune := rawdb.DefaultPebbleOptions()
	tune.MemTableSizeBytes = 64 << 20
	tune.MaxConcurrentCompactions = 1
	tune.DisableAutomaticCompactions = false
	*e.hotDB, err = rawdb.NewPebbleDBWithOptions(e.hotPath, 128, 128, tune)
	if err != nil {
		return err
	}
	current, manager, err := inspectRetirePlan(ctx, *e.hotDB, *e.stageDB, e.fromBucket, e.throughBucket, e.window)
	if err != nil {
		return err
	}
	if !reflect.DeepEqual(current.Protected, j.Protected) || current.Epoch != j.Epoch || current.Barrier != j.Barrier {
		return errors.New("repair: protected chain or route barrier changed at writer handoff")
	}
	if err := repairUnchangedMetadata(e.routes, current); err != nil {
		return err
	}
	if err := recheckRepairCanonical(ctx, e.routes, manager, rawdb.NewChainDB(*e.hotDB, ancient)); err != nil {
		return err
	}
	if err := e.fence(ctx); err != nil {
		return err
	}
	if err := facts.RecheckAll(ctx); err != nil {
		return err
	}
	if receiptAudit != nil {
		if err := receiptAudit.RecheckAll(ctx); err != nil {
			return err
		}
	}
	if e.currentSHA == j.OldSHA {
		if err := e.manifestSHA(j.OldSHA); err != nil {
			return err
		}
		report.Phase = "publish"
		if err := snapshots.PublishManifest(e.coldPath, candidate); err != nil {
			return err
		}
		if err := e.manifestSHA(j.CandidateSHA); err != nil {
			return err
		}
		if err := e.checkpoint("manifest-published"); err != nil {
			return err
		}
	}
	if j.Phase == journalPrepared {
		j.Phase = journalPublished
		if err := writeRepairJournal(e.coldPath, j); err != nil {
			return err
		}
		report.JournalPhase = j.Phase
		if err := e.checkpoint(journalPublished); err != nil {
			return err
		}
	}
	if err := e.manifestSHA(j.CandidateSHA); err != nil {
		return err
	}
	report.Phase = "certify"
	for _, prepared := range j.Bindings {
		currentBinding, present, err := manager.ReadColdBindingAt(j.Epoch, prepared.Bucket)
		if err != nil {
			return err
		}
		if present && reflect.DeepEqual(currentBinding.Spans, prepared.Spans) && currentBinding.ManifestEpoch == candidate.Generation {
			continue
		}
		binding := prepared
		if present {
			if currentBinding.BindingEpoch == ^uint64(0) {
				return errors.New("repair: binding epoch overflow")
			}
			binding.BindingEpoch = currentBinding.BindingEpoch + 1
		}
		if err := manager.CertifyColdRange(ctx, binding, func() error {
			checks := []error{e.fence(ctx), e.manifestSHA(j.CandidateSHA), facts.RecheckAll(ctx)}
			if receiptAudit != nil {
				checks = append(checks, receiptAudit.RecheckAll(ctx))
			}
			return errors.Join(checks...)
		}); err != nil {
			return err
		}
		if err := e.checkpoint(fmt.Sprintf("certified-bucket-%d", binding.Bucket)); err != nil {
			return err
		}
	}
	// Candidate TARGET spans are certified before old ContentIDs are rebound;
	// this keeps journal epochs stable across a partially certified retry.
	// Rebind only the source trios replaced in this batch, never all Retired.
	report.Phase = "rebind"
	if err := snapshots.OfflineRebindHistoryStagingRepair(proofCtx, e.coldPath, manager, candidate, j.Plan.SourceRefs, func() error {
		checks := []error{e.fence(ctx), e.manifestSHA(j.CandidateSHA), facts.RecheckAll(ctx)}
		if receiptAudit != nil {
			checks = append(checks, receiptAudit.RecheckAll(ctx))
		}
		return errors.Join(checks...)
	}); err != nil {
		return err
	}
	if err := e.manifestSHA(j.CandidateSHA); err != nil {
		return err
	}
	for _, binding := range j.Bindings {
		actual, present, err := manager.ReadColdBindingAt(j.Epoch, binding.Bucket)
		if err != nil || !present || actual.ManifestEpoch != candidate.Generation || !reflect.DeepEqual(actual.Spans, binding.Spans) {
			return errors.Join(fmt.Errorf("repair: durable binding bucket %d differs", binding.Bucket), err)
		}
	}
	if err := facts.CommitPhysicalCertificates(ctx); err != nil {
		if !errors.Is(err, snapshots.ErrHistoryStagingReceiptSidecarWrite) {
			return err
		}
		fmt.Fprintf(os.Stderr, "repair event=physical_certificate_warning err=%v\n", err)
	}
	if receiptAudit != nil {
		if err := receiptAudit.CommitPhysicalCertificates(ctx); err != nil {
			if !errors.Is(err, snapshots.ErrHistoryStagingReceiptSidecarWrite) {
				return err
			}
			fmt.Fprintf(os.Stderr, "repair event=receipt_certificate_warning err=%v\n", err)
		}
	}
	if j.Phase != journalRebound {
		j.Phase = journalRebound
		if err := writeRepairJournal(e.coldPath, j); err != nil {
			return err
		}
		if err := e.checkpoint(journalRebound); err != nil {
			return err
		}
	}
	report.JournalPhase = j.Phase
	report.Phase = "verify_reopen"
	return nil
}

func verifyRepairReopen(ctx context.Context, report *repairReport, e repairExecution, j repairJournal, complete bool, finalProof func(context.Context) error) error {
	if err := e.fence(ctx); err != nil {
		return err
	}
	hot, err := rawdb.NewPebbleDBReadOnly(e.hotPath, 64, 128)
	if err != nil {
		return err
	}
	defer hot.Close()
	stage, err := rawdb.NewHistoryStagingPebbleDB(e.stagePath, 64, 128, true)
	if err != nil {
		return err
	}
	defer stage.Close()
	after, manager, err := inspectRetirePlan(ctx, hot, stage, e.fromBucket, e.throughBucket, e.window)
	if err != nil || !reflect.DeepEqual(after.Protected, j.Protected) || after.Epoch != j.Epoch || after.Barrier != j.Barrier {
		return errors.Join(errors.New("repair: protected state differs after reopen"), err)
	}
	report.ProtectedStateVerified = true
	manifestSHA, err := cleanupFileSHA256(filepath.Join(e.coldPath, snapshots.ManifestFile))
	if err != nil || manifestSHA != j.OldSHA && manifestSHA != j.CandidateSHA {
		return errors.Join(errors.New("repair: reopened manifest is neither original nor candidate"), err)
	}
	if finalProof == nil {
		return errors.New("repair: final immutable-file proof missing")
	}
	if err := finalProof(ctx); err != nil {
		return fmt.Errorf("repair: cold files changed before final reopen: %w", err)
	}
	if !complete {
		// The original error remains authoritative. The exact old/candidate
		// manifest and protected state were verified after all handles closed;
		// a later explicit --resume must finish certification/rebinding.
		return nil
	}
	if err := e.manifestSHA(j.CandidateSHA); err != nil {
		return err
	}
	manifest, err := loadCleanupManifest(e.coldPath, j.CandidateSHA)
	if err != nil {
		return err
	}
	if err := verifyRepairCandidateManifest(manifest, j); err != nil {
		return err
	}
	if len(after.Buckets) != len(e.routes.Buckets) || len(j.Bindings) != len(after.Buckets) {
		return errors.New("repair: reopened route range changed")
	}
	for i, binding := range j.Bindings {
		actual, present, err := manager.ReadColdBindingAt(j.Epoch, binding.Bucket)
		if err != nil || !present || actual.ManifestEpoch != manifest.Generation || !reflect.DeepEqual(actual.Spans, binding.Spans) {
			return errors.Join(fmt.Errorf("repair: cold binding bucket %d differs after reopen", binding.Bucket), err)
		}
		oldRow, newRow := e.routes.Buckets[i], after.Buckets[i]
		wantRoute := oldRow.Route
		wantRoute.ColdBindingEpoch = actual.BindingEpoch
		if oldRow.Bucket != binding.Bucket || newRow.Bucket != binding.Bucket || newRow.Route != wantRoute ||
			!reflect.DeepEqual(newRow.Receipt, oldRow.Receipt) || newRow.OldBinding == nil || !reflect.DeepEqual(*newRow.OldBinding, actual) {
			return fmt.Errorf("repair: TARGET route or receipt changed outside cold binding epoch in bucket %d", binding.Bucket)
		}
	}
	if j.Phase != journalRebound {
		return errors.New("repair: reopen completed before durable rebound journal")
	}
	stored, found, err := readRepairJournal(e.coldPath)
	if err != nil || !found {
		return errors.Join(errors.New("repair: completed journal missing after reopen"), err)
	}
	if err := archiveRepairJournal(e.coldPath, stored); err != nil {
		return err
	}
	report.Phase = "complete"
	report.JournalPhase = "archived"
	return nil
}
