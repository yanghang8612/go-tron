package main

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"reflect"
	"time"

	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/tronprotocol/go-tron/core/rawdb"
	statesnapshots "github.com/tronprotocol/go-tron/core/state/snapshots"
	"github.com/urfave/cli/v2"
)

type historyStagingApplySession struct {
	cli      *historyStagingCLIContext
	reader   *historyStagingPlanReader
	source   ethdb.KeyValueStore
	stage    ethdb.KeyValueStore
	manager  *rawdb.HistoryStagingManager
	manifest *statesnapshots.Manifest
	prover   *statesnapshots.HistoryStagingColdProver
	limits   rawdb.HistoryStagingLimits
}

func openHistoryStagingApply(ctx *cli.Context) (_ *historyStagingApplySession, err error) {
	c, err := newHistoryStagingCLIContext(ctx)
	if err != nil {
		return nil, err
	}
	reader, err := openHistoryStagingPlan(ctx, c)
	if err != nil {
		return nil, err
	}
	s := &historyStagingApplySession{cli: c, reader: reader}
	defer func() {
		if err != nil {
			s.Close()
		}
	}()
	s.source, err = rawdb.NewPebbleDB(c.paths.Source, 64, 128)
	if err != nil {
		return nil, err
	}
	if err = verifyHistoryStagingPlanInputs(ctx, c, s.source, reader.header); err != nil {
		return nil, err
	}
	s.manifest, err = c.inspectCold(ctx, s.source)
	if err != nil {
		return nil, err
	}
	s.prover, err = statesnapshots.NewHistoryStagingColdProver(c.paths.Cold, s.manifest)
	if err != nil {
		return nil, err
	}
	s.prover.EnableReaderReuse()
	s.limits, _, err = historyStagingCLIWorkLimits(ctx, c.paths.Source, c.paths.Target)
	if err != nil {
		return nil, err
	}
	if !reflect.DeepEqual(historyStagingFrozenLimits(s.limits), reader.header.Limits) {
		return nil, errors.New("history staging apply work limits differ from frozen plan")
	}
	s.stage, err = rawdb.NewHistoryStagingPebbleDB(c.paths.Target, 64, 128, false)
	if err != nil {
		return nil, err
	}
	s.manager, err = rawdb.NewHistoryStagingManager(s.source, s.stage,
		historyStagingIdentity(c.paths, reader.header.GenesisHash, reader.header.NetworkID))
	if err != nil {
		return nil, err
	}
	if err = s.manager.Initialize(c.ctx); err != nil {
		return nil, err
	}
	if err = s.manager.VerifyIdentity(); err != nil {
		return nil, err
	}
	return s, nil
}

func (s *historyStagingApplySession) Close() {
	if s == nil {
		return
	}
	if s.prover != nil {
		_ = s.prover.Close()
	}
	if s.stage != nil {
		s.stage.Close()
	}
	if s.source != nil {
		s.source.Close()
	}
	if s.reader != nil {
		s.reader.Close()
	}
}

func historyStagingFrozenLimits(l rawdb.HistoryStagingLimits) historyStagingPlanLimits {
	return historyStagingPlanLimits{l.MaxRowBytes, l.MaxBucketBytes, l.MaxBatchBytes,
		l.MaxWorkBytes, l.MaxDecodedBytes, l.MinFreeBytes}
}

func (s *historyStagingApplySession) seedRoutes(ctx context.Context, progress *historyStagingCLIProgress) error {
	through := s.reader.header.Head.HeadBlock / rawdb.StateHistoryChunkBucketBlocks
	for bucket := uint64(1); bucket <= through; {
		progress.bucket.Store(bucket)
		if err := ctx.Err(); err != nil {
			return err
		}
		state, err := s.manager.InspectBucket(bucket)
		if err != nil {
			return err
		}
		if state.HasRoute {
			if state.Route.Epoch != 1 {
				return fmt.Errorf("history staging bucket %d has a different epoch", bucket)
			}
			bucket++
			continue
		}
		end := bucket
		for end < through && end-bucket+1 < 128 {
			next, err := s.manager.InspectBucket(end + 1)
			if err != nil {
				return err
			}
			if next.HasRoute {
				break
			}
			end++
		}
		if err := s.manager.InitializeOfflineSourceRoutes(ctx, 1, bucket, end); err != nil {
			return err
		}
		bucket = end + 1
	}
	return nil
}

func historyStagingColdCoversWholeBucket(proof rawdb.HistoryStagingProof) bool {
	first, last, err := rawdb.StateHistoryChunkBucketBounds(proof.Bucket)
	if err != nil || len(proof.ColdSpans) == 0 {
		return false
	}
	next := first
	for _, span := range proof.ColdSpans {
		if span.From != next || span.To < span.From {
			return false
		}
		next = span.To + 1
	}
	return next == last+1
}

func (s *historyStagingApplySession) verifyCold(ctx context.Context, proof rawdb.HistoryStagingProof) error {
	if len(proof.ColdSpans) == 0 {
		return nil
	}
	return s.prover.VerifyBinding(ctx, rawdb.HistoryStagingColdBinding{
		Bucket: proof.Bucket, Spans: proof.ColdSpans,
	}, proof.Blocks)
}

func (s *historyStagingApplySession) applyBucket(ctx context.Context, plan historyStagingPlanBucket, planID string) error {
	proof := plan.Proof
	state, err := s.manager.InspectBucket(proof.Bucket)
	if err != nil {
		return err
	}
	if !state.HasRoute || state.Route.Epoch != proof.Epoch {
		return errors.New("history staging bucket missing current source route")
	}
	if state.Route.Owner == rawdb.HistoryStagingOwnerCold {
		if !historyStagingColdCoversWholeBucket(proof) || state.HasClaim {
			return rawdb.ErrHistoryStagingConflict
		}
		return s.verifyCold(ctx, proof)
	}
	if state.Route.Owner == rawdb.HistoryStagingOwnerTarget && state.Route.SourceCleared {
		if state.HasClaim || !state.HasReceipt {
			return rawdb.ErrHistoryStagingIncomplete
		}
		return s.verifyCold(ctx, proof)
	}
	if state.Route.Owner != rawdb.HistoryStagingOwnerSource && state.Route.Owner != rawdb.HistoryStagingOwnerTarget {
		return rawdb.ErrHistoryStagingConflict
	}
	if err := s.verifyCold(ctx, proof); err != nil {
		return err
	}
	if state.Route.Owner == rawdb.HistoryStagingOwnerSource && len(proof.ColdSpans) > 0 {
		binding := rawdb.HistoryStagingColdBinding{Version: rawdb.HistoryStagingFormatVersion,
			Bucket: proof.Bucket, Epoch: proof.Epoch, BindingEpoch: 1,
			ManifestEpoch: s.manifest.Generation, Spans: proof.ColdSpans}
		if err := s.manager.CertifyColdRange(ctx, binding, func() error { return s.verifyCold(ctx, proof) }); err != nil {
			return err
		}
	}
	if state.Route.Owner == rawdb.HistoryStagingOwnerSource && historyStagingColdCoversWholeBucket(proof) {
		first, last, _ := rawdb.StateHistoryChunkBucketBounds(proof.Bucket)
		return s.manager.ClearCertifiedSourceRange(ctx, proof.Bucket, first, last, s.limits)
	}
	claimID, err := historyStagingClaimID(planID, proof.Bucket)
	if err != nil {
		return err
	}
	var claim rawdb.HistoryStagingClaim
	if state.Route.Owner == rawdb.HistoryStagingOwnerSource {
		claim, err = s.manager.BeginClaim(ctx, proof, claimID)
		if err != nil {
			return err
		}
		if _, err = s.manager.CopyClaim(ctx, claim, s.limits); err != nil {
			return err
		}
		if _, err = s.manager.AdoptClaim(ctx, claim, proof); err != nil {
			return err
		}
	} else {
		if !state.HasClaim || state.Claim.ClaimID != claimID {
			return rawdb.ErrHistoryStagingConflict
		}
		claim = state.Claim
	}
	return s.manager.ClearSource(ctx, claim, s.limits)
}

func historyStagingPlanIDBytes(planID string) ([32]byte, error) {
	var digest [32]byte
	decoded, err := hex.DecodeString(planID)
	if err != nil || len(decoded) != len(digest) {
		return digest, errors.New("history staging invalid plan ID")
	}
	copy(digest[:], decoded)
	return digest, nil
}

func historyStagingCandidateBytes(sha string) ([32]byte, error) {
	return historyStagingPlanIDBytes(sha)
}

func runHistoryStagingApply(ctx *cli.Context, action string) error {
	if action != "apply" && action != "resume" {
		return errors.New("invalid history staging migration action")
	}
	progress := startHistoryStagingCLIProgress(ctx.App.ErrWriter, action, "open-plan", 30*time.Second)
	defer progress.close()
	s, err := openHistoryStagingApply(ctx)
	if err != nil {
		return err
	}
	defer s.Close()
	progress.total.Store(s.reader.header.LastBucket)
	progress.stage.Store("seed-routes")
	if err := s.seedRoutes(s.cli.ctx, progress); err != nil {
		return err
	}
	progress.stage.Store("apply-buckets")
	for {
		row, more, err := s.reader.Next(s.cli.ctx)
		if err != nil {
			return err
		}
		if !more {
			break
		}
		progress.bucket.Store(row.Proof.Bucket)
		if err := s.applyBucket(s.cli.ctx, row, s.cli.event.PlanID); err != nil {
			return fmt.Errorf("history staging bucket %d: %w", row.Proof.Bucket, err)
		}
		s.cli.event.Bucket = row.Proof.Bucket
		progress.completed.Add(1)
	}
	if err := verifyHistoryStagingPlanInputs(ctx, s.cli, s.source, s.reader.header); err != nil {
		return err
	}
	progress.stage.Store("emit-durable-result")
	s.cli.event.Head = s.reader.header.Head.HeadBlock
	s.cli.event.Solid = s.reader.header.Head.SolidifiedBlock
	s.cli.event.EligibleThrough = s.reader.header.EligibleThrough
	s.cli.event.Durable = true
	return s.cli.emit(action)
}

func verifyHistoryStagingComplete(ctx *cli.Context) error {
	c, err := newHistoryStagingCLIContext(ctx)
	if err != nil {
		return err
	}
	progress := startHistoryStagingCLIProgress(ctx.App.ErrWriter, "inspect", "verify-complete", 30*time.Second)
	defer progress.close()
	preflight, err := openHistoryStagingPlan(ctx, c)
	if err != nil {
		return err
	}
	full := preflight.CompleteEligibleCoverage()
	if err := preflight.Close(); err != nil {
		return err
	}
	if !full {
		return errors.New("history staging partial plan cannot publish the reader activation barrier")
	}
	if info, err := os.Stat(c.paths.Target); err != nil || !info.IsDir() {
		return errors.New("history staging target store is absent")
	}
	s, err := openHistoryStagingApply(ctx)
	if err != nil {
		return err
	}
	defer s.Close()
	planDigest, err := historyStagingPlanIDBytes(s.cli.event.PlanID)
	if err != nil {
		return err
	}
	candidate, err := historyStagingCandidateBytes(s.cli.event.CandidateSHA256)
	if err != nil {
		return err
	}
	header := s.reader.header
	progress.total.Store(header.LastBucket)
	barrier := rawdb.HistoryStagingRouteBarrier{
		Version: rawdb.HistoryStagingFormatVersion, Epoch: 1,
		ThroughBucket:   header.Head.HeadBlock / rawdb.StateHistoryChunkBucketBlocks,
		EligibleThrough: header.FullEligibleLastBucket,
		PlanDigest:      planDigest, CandidateSHA: candidate,
	}
	if err := s.manager.PublishHistoryStagingRouteBarrier(s.cli.ctx, barrier, func() error {
		return s.verifyCompleteRows(ctx, progress)
	}); err != nil {
		return err
	}
	s.cli.event.Head = header.Head.HeadBlock
	s.cli.event.Solid = header.Head.SolidifiedBlock
	s.cli.event.EligibleThrough = header.EligibleThrough
	s.cli.event.Durable = true
	s.cli.event.VerifiedComplete = true
	s.cli.event.HistoryWindow = header.HistoryWindow
	s.cli.event.PruneMode = header.PruneMode
	return s.cli.emit("inspect")
}

func (s *historyStagingApplySession) verifyCompleteRows(ctx *cli.Context, progress *historyStagingCLIProgress) error {
	if !s.reader.CompleteEligibleCoverage() {
		return errors.New("history staging partial plan cannot verify complete")
	}
	if err := verifyHistoryStagingPlanInputs(ctx, s.cli, s.source, s.reader.header); err != nil {
		return err
	}
	ancient, closeAncient, err := openSnapshotPruneAncientReader(ctx.String("datadir"))
	if err != nil {
		return err
	}
	defer closeAncient()
	canonical := rawdb.NewChainDB(s.source, ancient)
	hot, release, err := rawdb.AcquireStateHistoryReadView(s.source)
	if err != nil {
		return err
	}
	defer release()
	progress.stage.Store("verify-buckets")
	for {
		row, more, err := s.reader.Next(s.cli.ctx)
		if err != nil {
			return err
		}
		if !more {
			break
		}
		proof := row.Proof
		progress.bucket.Store(proof.Bucket)
		for _, block := range proof.Blocks {
			hash, present, err := rawdb.ReadBlockHashByNumberStrict(canonical, block.Number)
			if err != nil || !present || hash != block.Hash {
				return fmt.Errorf("history staging canonical block %d changed", block.Number)
			}
			rangeRow, present, err := rawdb.ReadStateTxRange(hot, block.Number)
			if err != nil || !present || rangeRow == nil || rangeRow.BlockHash != block.Hash ||
				rangeRow.BeginTxNum != block.BeginTxNum || rangeRow.EndTxNum != block.EndTxNum {
				return fmt.Errorf("history staging tx range %d changed", block.Number)
			}
		}
		if err := s.verifyCold(s.cli.ctx, proof); err != nil {
			return fmt.Errorf("history staging bucket %d cold: %w", proof.Bucket, err)
		}
		state, err := s.manager.InspectBucket(proof.Bucket)
		if err != nil || !state.HasRoute || state.Route.Epoch != proof.Epoch || state.HasClaim {
			return fmt.Errorf("history staging bucket %d route/claim incomplete", proof.Bucket)
		}
		if len(proof.ColdSpans) > 0 {
			binding, present, err := s.manager.ReadColdBindingAt(proof.Epoch, proof.Bucket)
			if err != nil || !present || binding.BindingEpoch != state.Route.ColdBindingEpoch ||
				!reflect.DeepEqual(binding.Spans, proof.ColdSpans) {
				return fmt.Errorf("history staging bucket %d cold binding incomplete", proof.Bucket)
			}
		}
		switch state.Route.Owner {
		case rawdb.HistoryStagingOwnerTarget:
			if !state.Route.SourceCleared || !state.HasReceipt {
				return fmt.Errorf("history staging bucket %d target receipt incomplete", proof.Bucket)
			}
			physical, err := s.manager.InspectTargetBucket(s.cli.ctx, proof, s.limits)
			if err != nil || physical != row.Physical || state.Receipt.DataDigest != physical.Digest {
				return fmt.Errorf("history staging bucket %d target physical inventory differs: %w", proof.Bucket, err)
			}
		case rawdb.HistoryStagingOwnerCold:
			if !historyStagingColdCoversWholeBucket(proof) || state.HasReceipt {
				return fmt.Errorf("history staging bucket %d cold route lacks full proof", proof.Bucket)
			}
		default:
			return fmt.Errorf("history staging bucket %d remains source-owned", proof.Bucket)
		}
		progress.completed.Add(1)
	}
	if err := verifyHistoryStagingPlanInputs(ctx, s.cli, s.source, s.reader.header); err != nil {
		return err
	}
	progress.stage.Store("verify-routes")
	_, err = s.manager.VerifyOfflineRouteCoverage(s.cli.ctx, 1,
		s.reader.header.FullEligibleLastBucket,
		s.reader.header.Head.HeadBlock/rawdb.StateHistoryChunkBucketBlocks)
	return err
}
