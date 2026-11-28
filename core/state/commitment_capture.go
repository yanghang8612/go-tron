package state

import (
	"bytes"
	"fmt"

	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/state/kvdomains"
)

func (d *DomainCommitmentState) hasAccountLatestBefore(owner common.Address) bool {
	if d == nil || d.touches == nil {
		return false
	}
	idx, ok := d.touches[domainCommitmentTouch{
		flatDomain: rawdb.StateFlatDomainAccountLatest,
		owner:      owner.AccountID(),
	}]
	return ok && d.touchValues[idx].beforeKnown
}

func (d *DomainCommitmentState) hasKVGenerationBefore(owner common.Address) bool {
	if d == nil || d.touches == nil {
		return false
	}
	idx, ok := d.touches[domainCommitmentTouch{
		flatDomain: rawdb.StateFlatDomainKVGeneration,
		owner:      owner.AccountID(),
	}]
	return ok && d.touchValues[idx].beforeKnown
}

func (d *DomainCommitmentState) hasKVLatestBefore(owner common.Address, gen uint64, domain kvdomains.KVDomain, key []byte) bool {
	if d == nil {
		return false
	}
	if idx, ok := d.kvLatestTouchIndex(owner.AccountID(), gen, domain, key); ok && d.touchValues[idx].beforeKnown {
		return true
	}
	return false
}

func (d *DomainCommitmentState) recordAccountLatestBefore(owner common.Address, value []byte, exists, ownBefore bool) {
	if d == nil {
		return
	}
	d.recordStructuredBefore(domainCommitmentTouch{
		flatDomain: rawdb.StateFlatDomainAccountLatest,
		owner:      owner.AccountID(),
	}, value, exists, true, ownBefore)
}

func (d *DomainCommitmentState) recordKVGenerationBefore(owner common.Address, value []byte, exists, ownBefore bool) {
	if d == nil {
		return
	}
	d.recordStructuredBefore(domainCommitmentTouch{
		flatDomain: rawdb.StateFlatDomainKVGeneration,
		owner:      owner.AccountID(),
	}, value, exists, true, ownBefore)
}

func (d *DomainCommitmentState) recordKVLatestBefore(owner common.Address, gen uint64, domain kvdomains.KVDomain, key, value []byte, exists, ownKey, ownBefore bool) {
	if d == nil {
		return
	}
	ownerID := owner.AccountID()
	if index, ok := d.kvLatestTouchIndex(ownerID, gen, domain, key); ok {
		entry := &d.touchValues[index]
		if entry.beforeKnown {
			return
		}
		entry.beforeKnown = true
		entry.beforeExists = exists
		if exists {
			if ownBefore {
				entry.before = value
			} else {
				entry.before = bytes.Clone(value)
			}
		}
		return
	}
	var ownedBefore []byte
	if exists {
		if ownBefore {
			ownedBefore = value
		} else {
			ownedBefore = bytes.Clone(value)
		}
	}
	var keyStr string
	if ownKey {
		keyStr = ownedBytesString(key)
	} else {
		keyStr = string(key)
	}
	d.recordTouchWithValue(domainCommitmentTouch{
		flatDomain: rawdb.StateFlatDomainKVLatest,
		owner:      ownerID,
		generation: gen,
		domain:     domain,
		key:        keyStr,
	}, domainCommitmentCapturedValue{
		before:       ownedBefore,
		beforeExists: exists,
		beforeKnown:  true,
	})
}

func (d *DomainCommitmentState) recordStructuredBefore(touch domainCommitmentTouch, value []byte, exists, markTouched, ownBefore bool) {
	if d.touches == nil {
		d.touches = make(map[domainCommitmentTouch]int)
	}
	if index, ok := d.touches[touch]; ok {
		entry := &d.touchValues[index]
		if markTouched && !entry.touched {
			entry.touched = true
			d.touchedCount++
		}
		if entry.beforeKnown {
			return
		}
		entry.beforeKnown = true
		entry.beforeExists = exists
		if exists {
			if ownBefore {
				entry.before = value
			} else {
				entry.before = bytes.Clone(value)
			}
		}
		return
	}
	var ownedBefore []byte
	if exists {
		if ownBefore {
			ownedBefore = value
		} else {
			ownedBefore = bytes.Clone(value)
		}
	}
	d.touches[touch] = len(d.touchValues)
	d.touchValues = append(d.touchValues, domainCommitmentCapturedValue{
		touched:      markTouched,
		before:       ownedBefore,
		beforeExists: exists,
		beforeKnown:  true,
	})
	if markTouched {
		d.touchedCount++
	}
	if len(d.touches) > d.touchCapacityHint {
		d.touchCapacityHint = len(d.touches)
	}
}

func (d *DomainCommitmentState) hasPhysicalBefore(key []byte) bool {
	if d == nil {
		return false
	}
	if owner, ok := rawdb.DecodeStateKVGenerationKey(key); ok {
		return d.hasKVGenerationBefore(owner)
	}
	if owner, gen, domain, logicalKey, ok := rawdb.DecodeStateKVLatestKey(key); ok {
		if d.hasKVLatestBefore(owner, gen, domain, logicalKey) {
			return true
		}
	} else if len(key) >= common.AccountIDLength {
		var owner common.Address
		owner[0] = common.AddressPrefixMainnet
		copy(owner[1:], key[len(key)-common.AccountIDLength:])
		if d.hasAccountLatestBefore(owner) {
			return true
		}
	}
	_, ok := d.legacyBefore[string(key)]
	return ok
}

// recordPhysicalBefore is the physical-key compatibility wrapper preserved for
// tests. Production paths call recordAccountLatestBefore /
// recordKVGenerationBefore / recordKVLatestBefore directly without encoding or
// decoding physical keys.
func (d *DomainCommitmentState) recordPhysicalBefore(key, value []byte, exists bool) error {
	if d == nil {
		return nil
	}
	if _, err := rawdb.StateCommitmentElementFamily(key); err != nil {
		return err
	}
	if owner, ok := rawdb.DecodeStateKVGenerationKey(key); ok {
		d.recordKVGenerationBefore(owner, value, exists, false)
		return nil
	}
	if _, _, _, _, ok := rawdb.DecodeStateKVLatestKey(key); ok {
		if d.legacyBefore == nil {
			d.legacyBefore = make(map[string]rawdb.StateCommitmentDelta)
		}
		if _, present := d.legacyBefore[string(key)]; present {
			return nil
		}
		var ownedBefore []byte
		if exists {
			ownedBefore = bytes.Clone(value)
		}
		d.legacyBefore[string(key)] = rawdb.StateCommitmentDelta{BeforeExists: exists, Before: ownedBefore}
		return nil
	}
	var owner common.Address
	owner[0] = common.AddressPrefixMainnet
	copy(owner[1:], key[len(key)-common.AccountIDLength:])
	d.recordAccountLatestBefore(owner, value, exists, false)
	return nil
}

func (w *accountKVLatestBatch) captureKVBefore(owner common.Address, gen uint64, domain kvdomains.KVDomain, key []byte) error {
	if w == nil || w.commitment == nil {
		return nil
	}
	if w.commitment.hasKVLatestBefore(owner, gen, domain, key) {
		return nil
	}
	// Borrow only until presence encoding and recordKVLatestBefore own it.
	// Commitment capture is not an execution SLOAD resolution.
	value, present, err := w.readLatestBorrowed(owner, gen, domain, key, false)
	if err != nil {
		return err
	}
	var encoded []byte
	if present {
		encoded = rawdb.EncodeStateKVLatestValue(value)
	}
	w.commitment.recordKVLatestBefore(owner, gen, domain, key, encoded, present, false, true)
	return nil
}

func (w *accountKVLatestBatch) captureAccountBefore(owner common.Address) error {
	if w == nil || w.commitment == nil {
		return nil
	}
	if w.commitment.hasAccountLatestBefore(owner) {
		return nil
	}
	value, present, err := w.readAccountLatest(owner)
	if err != nil {
		return err
	}
	w.commitment.recordAccountLatestBefore(owner, value, present, true)
	return nil
}

func (w *accountKVLatestBatch) captureGenerationBefore(owner common.Address) error {
	if w == nil || w.commitment == nil {
		return nil
	}
	if w.commitment.hasKVGenerationBefore(owner) {
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
	w.commitment.recordKVGenerationBefore(owner, encoded, present, true)
	return nil
}

func (d *DomainCommitmentState) attachPhysicalBefore(update rawdb.StateCommitmentUpdate) (rawdb.StateCommitmentUpdate, error) {
	if d == nil {
		return update, fmt.Errorf("latest-domain touch %x has no pre-write commitment capture", update.Key)
	}
	if before, ok := d.legacyBefore[string(update.Key)]; ok {
		update.BeforeKnown = true
		update.BeforeExists = before.BeforeExists
		update.Before = before.Before
		return update, nil
	}
	if owner, gen, domain, logicalKey, ok := rawdb.DecodeStateKVLatestKey(update.Key); ok && d.touches != nil {
		if idx, found := d.kvLatestTouchIndex(owner.AccountID(), gen, domain, logicalKey); found && d.touchValues[idx].beforeKnown {
			entry := d.touchValues[idx]
			update.BeforeKnown = true
			update.BeforeExists = entry.beforeExists
			update.Before = entry.before
			return update, nil
		}
	} else if owner, ok := rawdb.DecodeStateKVGenerationKey(update.Key); ok && d.touches != nil {
		if idx, found := d.touches[domainCommitmentTouch{flatDomain: rawdb.StateFlatDomainKVGeneration, owner: owner.AccountID()}]; found && d.touchValues[idx].beforeKnown {
			entry := d.touchValues[idx]
			update.BeforeKnown = true
			update.BeforeExists = entry.beforeExists
			update.Before = entry.before
			return update, nil
		}
	} else if len(update.Key) >= common.AccountIDLength && d.touches != nil {
		var owner common.Address
		owner[0] = common.AddressPrefixMainnet
		copy(owner[1:], update.Key[len(update.Key)-common.AccountIDLength:])
		if idx, found := d.touches[domainCommitmentTouch{flatDomain: rawdb.StateFlatDomainAccountLatest, owner: owner.AccountID()}]; found && d.touchValues[idx].beforeKnown {
			entry := d.touchValues[idx]
			update.BeforeKnown = true
			update.BeforeExists = entry.beforeExists
			update.Before = entry.before
			return update, nil
		}
	}
	return update, fmt.Errorf("latest-domain touch %x has no pre-write commitment capture", update.Key)
}
