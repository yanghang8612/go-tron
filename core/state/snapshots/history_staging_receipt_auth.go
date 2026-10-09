package snapshots

import (
	"context"
	"crypto/sha256"
	"errors"
	"sync"

	"github.com/tronprotocol/go-tron/core/maintenance"
)

// Receipt authentication only establishes that immutable bytes still match
// the ContentID of an already durable semantic certification. This cache and
// its flights are deliberately separate from full trio and per-range proofs:
// checksum success must never authorize Build, offline adoption or RPC reads.
var historyStagingReceiptChecksumCache = struct {
	sync.Mutex
	trios   map[[32]byte][3]historyStagingFileState
	flights map[[32]byte]chan struct{}
}{trios: make(map[[32]byte][3]historyStagingFileState),
	flights: make(map[[32]byte]chan struct{})}

type historyStagingReceiptAuthenticator struct {
	dir      string
	manifest *Manifest
	// taskVerified is owned by one offline audit. Unlike the process-wide
	// bounded cache, it cannot evict an already authenticated trio midway
	// through a large route census. A changed fingerprint is an error.
	taskVerified map[[32]byte][3]historyStagingFileState
	// Test hooks wrap only physical authentication and singleflight waiting.
	// Production always uses the strong companion checksum verifier.
	testAuth func(context.Context, [3]SegmentRef) error
	testWait func()
}

func (p *historyStagingReceiptAuthenticator) authenticate(ctx context.Context, refs [3]SegmentRef, id [32]byte) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	// Independently reject weak or mismatched identities even on cache hits.
	gotID, err := historyStagingTrioID(refs)
	if err != nil {
		return err
	}
	if gotID != id {
		return errors.New("snapshots: receipt trio ContentID mismatch")
	}
	var states [3]historyStagingFileState
	for i, ref := range refs {
		states[i], err = historyStagingFileFingerprint(p.dir, ref)
		if err != nil {
			return err
		}
		if p.taskVerified != nil && !states[i].hasChangeTime {
			return errors.New("snapshots: offline receipt audit requires strong file change timestamps")
		}
	}
	if p.taskVerified != nil {
		if previous, ok := p.taskVerified[id]; ok {
			for i := range previous {
				if !previous[i].unchanged(states[i]) {
					return errors.New("snapshots: authenticated cold trio changed during offline audit")
				}
			}
			return nil
		}
	}
	key := sha256.Sum256(append(append([]byte("gtron-history-staging-receipt-checksum-v1\x00"), p.dir...), id[:]...))
	// A cooperative owner may release the shared maintenance token. Never
	// own or wait on a global flight here: a flight waiter may hold that token
	// while the owner tries to reacquire it. Cache hits remain safe; misses run
	// the identical full proof independently, publishing only authenticated bytes.
	if maintenance.HasWorkCheckpoint(ctx) {
		historyStagingReceiptChecksumCache.Lock()
		cached, hit := historyStagingReceiptChecksumCache.trios[key]
		historyStagingReceiptChecksumCache.Unlock()
		if hit && cached[0].same(states[0]) && cached[1].same(states[1]) && cached[2].same(states[2]) {
			if p.taskVerified != nil {
				p.taskVerified[id] = states
			}
			return nil
		}

		if p.testAuth != nil {
			err = p.testAuth(ctx, refs)
		} else {
			err = VerifyHistorySegmentCompanionChecksumsContext(ctx, p.dir, p.manifest, refs[0])
		}
		if err == nil {
			err = historyStagingCheckFileStates(p.dir, refs, states)
		}
		if err == nil {
			err = ctx.Err()
		}
		if err != nil {
			return err
		}
		if p.taskVerified != nil {
			p.taskVerified[id] = states
		}
		historyStagingReceiptChecksumCache.Lock()
		if len(historyStagingReceiptChecksumCache.trios) >= historyStagingPinnedProofCacheEntries {
			for old := range historyStagingReceiptChecksumCache.trios {
				delete(historyStagingReceiptChecksumCache.trios, old)
				break
			}
		}
		historyStagingReceiptChecksumCache.trios[key] = states
		historyStagingReceiptChecksumCache.Unlock()

		return nil
	}
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		historyStagingReceiptChecksumCache.Lock()
		cached, hit := historyStagingReceiptChecksumCache.trios[key]
		if hit && cached[0].same(states[0]) && cached[1].same(states[1]) && cached[2].same(states[2]) {
			historyStagingReceiptChecksumCache.Unlock()
			if p.taskVerified != nil {
				p.taskVerified[id] = states
			}
			return nil
		}
		flight := historyStagingReceiptChecksumCache.flights[key]
		// Keep physical audits and their open descriptors bounded even when
		// independent callers encounter many different trios concurrently.
		if flight == nil && len(historyStagingReceiptChecksumCache.flights) >= 8 {
			for _, pending := range historyStagingReceiptChecksumCache.flights {
				flight = pending
				break
			}
		}
		if flight != nil {
			historyStagingReceiptChecksumCache.Unlock()
			if p.testWait != nil {
				p.testWait()
			}
			select {
			case <-flight:
				if err := historyStagingCheckFileStates(p.dir, refs, states); err != nil {
					return err
				}
				continue
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		flight = make(chan struct{})
		historyStagingReceiptChecksumCache.flights[key] = flight
		historyStagingReceiptChecksumCache.Unlock()

		if p.testAuth != nil {
			err = p.testAuth(ctx, refs)
		} else {
			err = VerifyHistorySegmentCompanionChecksumsContext(ctx, p.dir, p.manifest, refs[0])
		}
		if err == nil {
			err = historyStagingCheckFileStates(p.dir, refs, states)
		}
		if err == nil {
			err = ctx.Err()
		}
		historyStagingReceiptChecksumCache.Lock()
		if err == nil {
			if len(historyStagingReceiptChecksumCache.trios) >= historyStagingPinnedProofCacheEntries {
				for old := range historyStagingReceiptChecksumCache.trios {
					delete(historyStagingReceiptChecksumCache.trios, old)
					break
				}
			}
			historyStagingReceiptChecksumCache.trios[key] = states
		}
		delete(historyStagingReceiptChecksumCache.flights, key)
		close(flight)
		historyStagingReceiptChecksumCache.Unlock()
		if err == nil && p.taskVerified != nil {
			p.taskVerified[id] = states
		}
		return err
	}
}
