package main

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"hash"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"time"

	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/state/snapshots"
)

const cleanupFirstBlock = rawdb.StateHistoryChunkBucketBlocks

type cleanupCanary struct {
	KeyHash   [32]byte
	ValueHash [32]byte
}

type cleanupProtected struct {
	Boundary chainBoundary
	Guard    guard
	Canaries map[string][]cleanupCanary
}

type cleanupAuthorization struct {
	Epoch             uint64
	Barrier           rawdb.HistoryStagingRouteBarrier
	ThroughBlock      uint64
	ThroughBucket     uint64
	RouteBindingHash  [32]byte
	ColdBindings      uint64
	AuthenticatedTrio int
	ManifestSHA256    [32]byte
	Protected         cleanupProtected
}

type cleanupRouteReader interface {
	CurrentEpoch() (uint64, error)
	ReadResetIntent() (rawdb.HistoryStagingResetIntent, bool, error)
	ReadHistoryStagingRouteBarrier() (rawdb.HistoryStagingRouteBarrier, bool, error)
	ReadRoute(uint64) (rawdb.HistoryStagingRoute, bool, error)
	ReadClaim(uint64) (rawdb.HistoryStagingClaim, bool, error)
	ReadColdBindingAt(uint64, uint64) (rawdb.HistoryStagingColdBinding, bool, error)
}

type cleanupBindingAuditor interface {
	VerifyBinding(context.Context, rawdb.HistoryStagingColdBinding) error
	RecheckAll(context.Context) error
	AuthenticatedTrios() int
}

func cleanupFileSHA256(path string) ([32]byte, error) {
	var sum [32]byte
	f, err := os.Open(path)
	if err != nil {
		return sum, err
	}
	defer f.Close()
	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return sum, err
	}
	copy(sum[:], h.Sum(nil))
	return sum, nil
}

func captureCleanupProtected(db ethdb.KeyValueStore) (cleanupProtected, error) {
	var out cleanupProtected
	var err error
	out.Boundary, err = strictBoundary(db)
	if err != nil {
		return out, err
	}
	out.Guard, err = readGuard(db)
	if err != nil {
		return out, err
	}
	if !out.Guard.LatestCommitmentRoot.Present || !out.Guard.StagingIdentity.Present {
		return out, errors.New("cleanup requires durable commitment root and staging identity")
	}
	out.Canaries = make(map[string][]cleanupCanary)
	for _, r := range rawdb.HotMaintenanceCanaryRanges() {
		it := db.NewIterator(nil, r.Start)
		var samples []cleanupCanary
		for it.Next() && len(samples) < 4 {
			if len(r.End) > 0 && string(it.Key()) >= string(r.End) {
				break
			}
			samples = append(samples, cleanupCanary{KeyHash: sha256.Sum256(it.Key()), ValueHash: sha256.Sum256(it.Value())})
		}
		err := it.Error()
		it.Release()
		if err != nil {
			return out, err
		}
		out.Canaries[r.Name] = samples
	}
	return out, nil
}

func validateCleanupBounds(protected cleanupProtected, through uint64) error {
	if through < cleanupFirstBlock || through%rawdb.StateHistoryChunkBucketBlocks != rawdb.StateHistoryChunkBucketBlocks-1 {
		return errors.New("cleanup through height must end a complete bucket at or after bucket one")
	}
	if through > protected.Boundary.SolidNumber || !protected.Guard.Finish.Present || !protected.Guard.HistoryIndex.Present ||
		through > protected.Guard.Finish.Value.BlockNum || through > protected.Guard.HistoryIndex.Value.BlockNum {
		return errors.New("cleanup boundary exceeds canonical solid, Finish, or history index")
	}
	return nil
}

func cleanupBindingCovers(bucket uint64, binding rawdb.HistoryStagingColdBinding) bool {
	first, last, err := rawdb.StateHistoryChunkBucketBounds(bucket)
	if err != nil || len(binding.Spans) == 0 {
		return false
	}
	next := first
	for i, span := range binding.Spans {
		if span.From != next || span.To < span.From || span.To > last {
			return false
		}
		if span.To == last {
			return i == len(binding.Spans)-1
		}
		next = span.To + 1
	}
	return false
}

func cleanupHashRow(h hash.Hash, bucket uint64, value any) error {
	data, err := json.Marshal(value)
	if err != nil {
		return err
	}
	var length [8]byte
	binary.BigEndian.PutUint64(length[:], bucket)
	_, _ = h.Write(length[:])
	binary.BigEndian.PutUint64(length[:], uint64(len(data)))
	_, _ = h.Write(length[:])
	_, _ = h.Write(data)
	return nil
}

// proveCleanupPrefix reads only durable schema-owned route and cold-binding
// records. A COLD route's binding itself is the semantic receipt; a separate
// TARGET migration receipt may legitimately be absent. Current-epoch claims
// are explicitly rejected because startup route verification omits them.
func proveCleanupPrefix(ctx context.Context, manager cleanupRouteReader, audit cleanupBindingAuditor, through uint64, progress func(uint64)) (cleanupAuthorization, error) {
	var out cleanupAuthorization
	if ctx == nil || manager == nil || audit == nil {
		return out, errors.New("cleanup requires pinned route and cold audit")
	}
	epoch, err := manager.CurrentEpoch()
	if err != nil {
		return out, err
	}
	if intent, present, err := manager.ReadResetIntent(); err != nil {
		return out, err
	} else if present {
		return out, fmt.Errorf("cleanup refuses reset intent at epoch %d complete=%t", intent.NewEpoch, intent.Complete)
	}
	barrier, present, err := manager.ReadHistoryStagingRouteBarrier()
	if err != nil || !present || barrier.Epoch != epoch {
		return out, errors.New("cleanup requires current-epoch route barrier")
	}
	lastBucket := through / rawdb.StateHistoryChunkBucketBlocks
	if lastBucket > barrier.EligibleThrough || lastBucket > barrier.ThroughBucket {
		return out, errors.New("cleanup boundary exceeds durable route barrier")
	}
	h := sha256.New()
	_, _ = h.Write([]byte("go-tron-offline-cold-prefix-v1\x00"))
	for bucket := uint64(1); bucket <= lastBucket; bucket++ {
		if err := ctx.Err(); err != nil {
			return out, err
		}
		route, found, err := manager.ReadRoute(bucket)
		if err != nil || !found || route.Epoch != epoch || route.Owner != rawdb.HistoryStagingOwnerCold {
			return out, fmt.Errorf("cleanup bucket %d is not current-epoch COLD: %w", bucket, errors.Join(err, rawdb.ErrHistoryStagingIncomplete))
		}
		if claim, found, err := manager.ReadClaim(bucket); err != nil {
			return out, err
		} else if found && claim.Epoch == epoch {
			return out, fmt.Errorf("cleanup bucket %d has current-epoch claim", bucket)
		}
		binding, found, err := manager.ReadColdBindingAt(epoch, bucket)
		if err != nil || !found || binding.BindingEpoch != route.ColdBindingEpoch || !cleanupBindingCovers(bucket, binding) {
			return out, fmt.Errorf("cleanup bucket %d lacks complete route-bound COLD semantic receipt: %w", bucket, errors.Join(err, rawdb.ErrHistoryStagingIncomplete))
		}
		if err := audit.VerifyBinding(ctx, binding); err != nil {
			return out, fmt.Errorf("cleanup bucket %d cold trio authentication: %w", bucket, err)
		}
		if err := cleanupHashRow(h, bucket, route); err != nil {
			return out, err
		}
		if err := cleanupHashRow(h, bucket, binding); err != nil {
			return out, err
		}
		if progress != nil {
			progress(bucket)
		}
	}
	if err := audit.RecheckAll(ctx); err != nil {
		return out, err
	}
	copy(out.RouteBindingHash[:], h.Sum(nil))
	out.Epoch, out.Barrier, out.ThroughBlock, out.ThroughBucket = epoch, barrier, through, lastBucket
	out.ColdBindings, out.AuthenticatedTrio = lastBucket, audit.AuthenticatedTrios()
	return out, nil
}

func compareCleanupAuthorization(before, after cleanupAuthorization) error {
	if before.Epoch != after.Epoch || before.Barrier != after.Barrier || before.ThroughBlock != after.ThroughBlock || before.ThroughBucket != after.ThroughBucket ||
		before.RouteBindingHash != after.RouteBindingHash || before.ManifestSHA256 != after.ManifestSHA256 || !reflect.DeepEqual(before.Protected, after.Protected) {
		return errors.New("cleanup protected chain, route binding, or manifest changed")
	}
	return nil
}

func loadCleanupManifest(cold string, expected [32]byte) (*snapshots.Manifest, error) {
	path := filepath.Join(cold, snapshots.ManifestFile)
	actual, err := cleanupFileSHA256(path)
	if err != nil {
		return nil, err
	}
	if actual != expected {
		return nil, fmt.Errorf("cleanup manifest SHA mismatch: actual=%s expected=%s", hex.EncodeToString(actual[:]), hex.EncodeToString(expected[:]))
	}
	manifest, err := snapshots.LoadProductionManifest(cold)
	if err != nil {
		return nil, err
	}
	after, err := cleanupFileSHA256(path)
	if err != nil || after != expected {
		return nil, errors.New("cleanup manifest changed during load")
	}
	return manifest, nil
}

func cleanupProgress(label string, completed, total uint64, last *time.Time) {
	if last == nil || time.Since(*last) < 30*time.Second {
		return
	}
	fmt.Fprintf(os.Stderr, "cleanup phase=%s completed=%d total=%d at=%s\n", label, completed, total, time.Now().UTC().Format(time.RFC3339))
	*last = time.Now()
}
