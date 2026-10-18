package state

import (
	"bytes"
	"fmt"

	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/state/kvdomains"
)

func (d *DomainCommitmentState) hasPhysicalBefore(key []byte) bool {
	if d == nil {
		return false
	}
	_, ok := d.before[string(key)]
	return ok
}

// recordPhysicalBefore is called BEFORE the corresponding source write. Values
// are owned independently of scope overlays, read cursors and StateDB reuse.
func (d *DomainCommitmentState) recordPhysicalBefore(key, value []byte, exists bool) error {
	if d == nil {
		return nil
	}
	if _, err := rawdb.StateCommitmentElementFamily(key); err != nil {
		return err
	}
	if d.before == nil {
		d.before = make(map[string]rawdb.StateCommitmentDelta)
	}
	if _, ok := d.before[string(key)]; ok {
		return nil
	}
	d.before[string(key)] = rawdb.CloneStateCommitmentDelta(rawdb.StateCommitmentDelta{Key: key, BeforeExists: exists, Before: value})
	if owner, ok := rawdb.DecodeStateKVGenerationKey(key); ok {
		d.recordKVGenerationTouch(owner)
		return nil
	}
	// KV touch identity and owned post-image are recorded by the temporal
	// mutation recorder; pre-capture must not replace its owned logical key.
	if _, _, _, _, ok := rawdb.DecodeStateKVLatestKey(key); ok {
		return nil
	}

	var owner common.Address
	owner[0] = common.AddressPrefixMainnet
	copy(owner[1:], key[len(key)-common.AccountIDLength:])
	d.recordAccountLatestTouch(owner)
	return nil
}
func (w *accountKVLatestBatch) captureKVBefore(owner common.Address, gen uint64, domain kvdomains.KVDomain, key []byte) error {
	if w == nil || w.commitment == nil {
		return nil
	}
	physical := rawdb.StateKVLatestCommitmentKey(owner, gen, domain, key)
	if w.commitment.hasPhysicalBefore(physical) {
		return nil
	}
	value, present, err := w.readLatest(owner, gen, domain, key)
	if err != nil {
		return err
	}
	var encoded []byte
	if present {
		encoded = rawdb.EncodeStateKVLatestValue(value)
	}
	return w.commitment.recordPhysicalBefore(physical, encoded, present)
}
func (w *accountKVLatestBatch) captureAccountBefore(owner common.Address) error {
	if w == nil || w.commitment == nil {
		return nil
	}
	key := rawdb.StateAccountLatestCommitmentKey(owner)
	if w.commitment.hasPhysicalBefore(key) {
		return nil
	}
	value, present, err := w.readAccountLatest(owner)
	if err != nil {
		return err
	}
	return w.commitment.recordPhysicalBefore(key, value, present)
}
func (w *accountKVLatestBatch) captureGenerationBefore(owner common.Address) error {
	if w == nil || w.commitment == nil {
		return nil
	}
	key := rawdb.StateKVGenerationCommitmentKey(owner)
	if w.commitment.hasPhysicalBefore(key) {
		return nil
	}
	value, present, err := w.readKVGeneration(owner)
	if err != nil {
		return err
	}
	var encoded []byte
	if present {
		encoded = rawdb.EncodeStateKVGenerationValue(value)
	}
	return w.commitment.recordPhysicalBefore(key, encoded, present)
}
func (d *DomainCommitmentState) attachPhysicalBefore(update rawdb.StateCommitmentUpdate) (rawdb.StateCommitmentUpdate, error) {
	before, ok := d.before[string(update.Key)]
	if !ok {
		return update, fmt.Errorf("latest-domain touch %x has no pre-write commitment capture", update.Key)
	}
	update.BeforeKnown = true
	update.BeforeExists = before.BeforeExists
	update.Before = bytes.Clone(before.Before)
	return update, nil
}
