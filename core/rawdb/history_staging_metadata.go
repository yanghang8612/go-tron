package rawdb

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/rlp"
)

const historyStagingMetadataMaxBytes = 1 << 20

func encodeHistoryStaging(v any) ([]byte, error) { return rlp.EncodeToBytes(v) }

func decodeHistoryStaging(data []byte, out any) error {
	if len(data) == 0 || len(data) > historyStagingMetadataMaxBytes {
		return errors.New("rawdb: invalid history staging metadata length")
	}
	if err := rlp.DecodeBytes(data, out); err != nil {
		return fmt.Errorf("rawdb: decode history staging metadata: %w", err)
	}
	return nil
}

func readHistoryStagingValue(db ethdb.KeyValueReader, key []byte, out any) (bool, error) {
	value, present, err := readPresentValue(db, key, "history staging metadata")
	if err != nil || !present {
		return present, err
	}
	return true, decodeHistoryStaging(value, out)
}

func writeHistoryStagingValue(db ethdb.KeyValueWriter, key []byte, value any) error {
	encoded, err := encodeHistoryStaging(value)
	if err != nil {
		return err
	}
	if len(encoded) > historyStagingMetadataMaxBytes {
		return errors.New("rawdb: history staging metadata exceeds maximum length")
	}
	return db.Put(key, encoded)
}

func (m *HistoryStagingManager) syncHot() error {
	return m.hot.(historyStagingSyncStore).SyncKeyValue()
}
func (m *HistoryStagingManager) syncStage() error {
	return m.stage.(historyStagingSyncStore).SyncKeyValue()
}

// Initialize writes the target identity first, then the source activation
// marker. A crash between the syncs may be resumed only when the target marker
// matches and no source route has yet been published.
func (m *HistoryStagingManager) Initialize(ctx context.Context) error {
	if m == nil || ctx == nil {
		return errors.New("rawdb: missing history staging manager or context")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	m.initMu.Lock()
	defer m.initMu.Unlock()
	var source, target HistoryStagingIdentity
	hasSource, err := readHistoryStagingValue(m.hot, historyStagingIdentityKey, &source)
	if err != nil {
		return err
	}
	hasTarget, err := readHistoryStagingValue(m.stage, historyStagingIdentityKey, &target)
	if err != nil {
		return err
	}
	if hasSource && (!hasTarget || source != m.identity || target != m.identity) {
		return ErrHistoryStagingConflict
	}
	if hasTarget && target != m.identity {
		return ErrHistoryStagingConflict
	}
	if !hasTarget {
		if err := writeHistoryStagingValue(m.stage, historyStagingIdentityKey, m.identity); err != nil {
			return err
		}
		if err := m.syncStage(); err != nil {
			return err
		}
	}
	if !hasSource {
		batch := m.hot.NewBatch()
		defer batch.Reset()
		if err := writeHistoryStagingValue(batch, historyStagingIdentityKey, m.identity); err != nil {
			return err
		}
		var epoch [8]byte
		binary.BigEndian.PutUint64(epoch[:], 1)
		if err := batch.Put(historyStagingEpochKey, epoch[:]); err != nil {
			return err
		}
		// Genesis belongs to the fixed [0,1023] bucket. It is never
		// transferred, but it still needs an explicit source route.
		if err := writeHistoryStagingValue(batch, historyStagingBucketKey(historyStagingRoutePrefix, 0), HistoryStagingRoute{
			Version: HistoryStagingFormatVersion, Bucket: 0, Epoch: 1,
			WriteVersion: 1, Owner: HistoryStagingOwnerSource,
		}); err != nil {
			return err
		}
		if err := batch.Write(); err != nil {
			return err
		}
		if err := m.syncHot(); err != nil {
			return err
		}
	}
	return nil
}

func (m *HistoryStagingManager) VerifyIdentity() error {
	if m == nil {
		return errors.New("rawdb: missing history staging manager")
	}
	var source, target HistoryStagingIdentity
	hasSource, err := readHistoryStagingValue(m.hot, historyStagingIdentityKey, &source)
	if err != nil {
		return err
	}
	hasTarget, err := readHistoryStagingValue(m.stage, historyStagingIdentityKey, &target)
	if err != nil {
		return err
	}
	if !hasSource || !hasTarget {
		return ErrHistoryStagingUninitialized
	}
	if source != m.identity || target != m.identity || source.Version != HistoryStagingFormatVersion {
		return ErrHistoryStagingConflict
	}
	if _, err := m.CurrentEpoch(); err != nil {
		return err
	}
	return nil
}

// ReadHistoryStagingIdentity detects an already activated source before node
// construction. A capable binary must fail closed if its external capability
// marker/config is absent; it may not silently reopen this DB as hot-only.
func ReadHistoryStagingIdentity(db ethdb.KeyValueReader) (HistoryStagingIdentity, bool, error) {
	var identity HistoryStagingIdentity
	if db == nil {
		return identity, false, errors.New("rawdb: missing history staging source")
	}
	present, err := readHistoryStagingValue(db, historyStagingIdentityKey, &identity)
	if err != nil || !present {
		return identity, present, err
	}
	if identity.Version != HistoryStagingFormatVersion || identity.GenesisHash == ([32]byte{}) || identity.SourceID == ([32]byte{}) || identity.TargetID == ([32]byte{}) || identity.SourceID == identity.TargetID {
		return HistoryStagingIdentity{}, false, ErrHistoryStagingConflict
	}
	return identity, true, nil
}

func (m *HistoryStagingManager) CurrentEpoch() (uint64, error) {
	if m == nil {
		return 0, errors.New("rawdb: missing history staging manager")
	}
	value, present, err := readPresentValue(m.hot, historyStagingEpochKey, "history staging epoch")
	if err != nil {
		return 0, err
	}
	if !present || len(value) != 8 {
		return 0, ErrHistoryStagingIncomplete
	}
	epoch := binary.BigEndian.Uint64(value)
	if epoch == 0 {
		return 0, ErrHistoryStagingConflict
	}
	return epoch, nil
}

func (m *HistoryStagingManager) ReadRoute(bucket uint64) (HistoryStagingRoute, bool, error) {
	var route HistoryStagingRoute
	if m == nil {
		return route, false, errors.New("rawdb: missing history staging manager")
	}
	present, err := readHistoryStagingValue(m.hot, historyStagingBucketKey(historyStagingRoutePrefix, bucket), &route)
	if err != nil || !present {
		return route, present, err
	}
	if route.Version != HistoryStagingFormatVersion || route.Bucket != bucket || route.Epoch == 0 || route.Owner < HistoryStagingOwnerSource || route.Owner > HistoryStagingOwnerCold || (route.Owner == HistoryStagingOwnerTarget && route.ReceiptDigest == ([32]byte{})) {
		return HistoryStagingRoute{}, false, ErrHistoryStagingConflict
	}
	return route, true, nil
}

func (m *HistoryStagingManager) ReadClaim(bucket uint64) (HistoryStagingClaim, bool, error) {
	var claim HistoryStagingClaim
	if m == nil {
		return claim, false, errors.New("rawdb: missing history staging manager")
	}
	present, err := readHistoryStagingValue(m.hot, historyStagingBucketKey(historyStagingClaimPrefix, bucket), &claim)
	if err != nil || !present {
		return claim, present, err
	}
	if claim.Version != HistoryStagingFormatVersion || claim.Bucket != bucket || claim.Epoch == 0 || claim.ClaimID == ([32]byte{}) || claim.ProofDigest == ([32]byte{}) {
		return HistoryStagingClaim{}, false, ErrHistoryStagingConflict
	}
	digest, err := claim.Proof.digest()
	if err != nil || digest != claim.ProofDigest || claim.Proof.Epoch != claim.Epoch {
		return HistoryStagingClaim{}, false, ErrHistoryStagingConflict
	}
	return claim, true, nil
}

func (m *HistoryStagingManager) ReadReceipt(bucket uint64) (HistoryStagingReceipt, bool, error) {
	route, hasRoute, err := m.ReadRoute(bucket)
	if err != nil {
		return HistoryStagingReceipt{}, false, err
	}
	if hasRoute {
		return m.ReadReceiptAt(route.Epoch, bucket)
	}
	claim, hasClaim, err := m.ReadClaim(bucket)
	if err != nil {
		return HistoryStagingReceipt{}, false, err
	}
	if hasClaim {
		return m.ReadReceiptAt(claim.Epoch, bucket)
	}
	return HistoryStagingReceipt{}, false, nil
}

func (m *HistoryStagingManager) ReadReceiptAt(epoch, bucket uint64) (HistoryStagingReceipt, bool, error) {
	var receipt HistoryStagingReceipt
	if m == nil {
		return receipt, false, errors.New("rawdb: missing history staging manager")
	}
	present, err := readHistoryStagingValue(m.stage, historyStagingReceiptKey(epoch, bucket), &receipt)
	if err != nil || !present {
		return receipt, present, err
	}
	if receipt.Version != HistoryStagingFormatVersion || receipt.Bucket != bucket || receipt.Epoch != epoch || receipt.ClaimID == ([32]byte{}) || receipt.ProofDigest == ([32]byte{}) || receipt.DataDigest == ([32]byte{}) {
		return HistoryStagingReceipt{}, false, ErrHistoryStagingConflict
	}
	return receipt, true, nil
}

// InspectBucket is schema-owned and avoids exposing private key prefixes to
// CLI and runtime packages. It does not itself certify canonical/cold proofs.
type HistoryStagingBucketState struct {
	Bucket     uint64
	Route      HistoryStagingRoute
	HasRoute   bool
	Claim      HistoryStagingClaim
	HasClaim   bool
	Receipt    HistoryStagingReceipt
	HasReceipt bool
}

func (m *HistoryStagingManager) InspectBucket(bucket uint64) (HistoryStagingBucketState, error) {
	state := HistoryStagingBucketState{Bucket: bucket}
	var err error
	state.Route, state.HasRoute, err = m.ReadRoute(bucket)
	if err != nil {
		return HistoryStagingBucketState{}, err
	}
	state.Claim, state.HasClaim, err = m.ReadClaim(bucket)
	if err != nil {
		return HistoryStagingBucketState{}, err
	}
	state.Receipt, state.HasReceipt, err = m.ReadReceipt(bucket)
	return state, err
}

// ScanBuckets merges the route and claim keyspaces in bucket order. after is
// an opaque exclusive 8-byte cursor; nil starts before bucket zero. A complete
// page returns next=nil. Neither family can starve the other when paginated.
func (m *HistoryStagingManager) ScanBuckets(ctx context.Context, after []byte, maxRows uint64) ([]HistoryStagingBucketState, []byte, bool, error) {
	if m == nil || ctx == nil || maxRows == 0 || maxRows > 256 || (len(after) != 0 && len(after) != 8) {
		return nil, nil, false, errors.New("rawdb: invalid staging scan")
	}
	type cursor struct {
		it     ethdb.Iterator
		prefix []byte
		bucket uint64
		valid  bool
	}
	routes := cursor{it: m.hot.NewIterator(historyStagingRoutePrefix, after), prefix: historyStagingRoutePrefix}
	claims := cursor{it: m.hot.NewIterator(historyStagingClaimPrefix, after), prefix: historyStagingClaimPrefix}
	defer routes.it.Release()
	defer claims.it.Release()
	advance := func(c *cursor) error {
		for c.it.Next() {
			if err := ctx.Err(); err != nil {
				return err
			}
			key := c.it.Key()
			if !bytes.HasPrefix(key, c.prefix) || len(key) != len(c.prefix)+8 {
				return ErrHistoryStagingConflict
			}
			bucket := binary.BigEndian.Uint64(key[len(c.prefix):])
			if len(after) == 0 || bucket > binary.BigEndian.Uint64(after) {
				c.bucket, c.valid = bucket, true
				return nil
			}
		}
		c.valid = false
		return c.it.Error()
	}
	if err := advance(&routes); err != nil {
		return nil, nil, false, err
	}
	if err := advance(&claims); err != nil {
		return nil, nil, false, err
	}
	states := make([]HistoryStagingBucketState, 0, maxRows)
	for (routes.valid || claims.valid) && uint64(len(states)) < maxRows {
		if err := ctx.Err(); err != nil {
			return nil, nil, false, err
		}
		bucket := uint64(0)
		if !routes.valid || (claims.valid && claims.bucket < routes.bucket) {
			bucket = claims.bucket
		} else {
			bucket = routes.bucket
		}
		state, err := m.InspectBucket(bucket)
		if err != nil {
			return nil, nil, false, err
		}
		states = append(states, state)
		if routes.valid && routes.bucket == bucket {
			if err := advance(&routes); err != nil {
				return nil, nil, false, err
			}
		}
		if claims.valid && claims.bucket == bucket {
			if err := advance(&claims); err != nil {
				return nil, nil, false, err
			}
		}
	}
	if !routes.valid && !claims.valid {
		return states, nil, true, nil
	}
	next := make([]byte, 8)
	binary.BigEndian.PutUint64(next, states[len(states)-1].Bucket)
	return states, next, false, nil
}

func historyStagingReceiptDigest(receipt HistoryStagingReceipt) ([32]byte, error) {
	encoded, err := encodeHistoryStaging(receipt)
	if err != nil {
		return [32]byte{}, err
	}
	return sha256.Sum256(encoded), nil
}
