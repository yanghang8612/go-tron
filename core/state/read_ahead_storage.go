package state

import (
	"hash"
	"sync"

	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/state/statecodec"
	"golang.org/x/crypto/sha3"
)

const (
	maxContractLayoutCacheEntries = 4096
	maxSlotLearnerEntries         = 1024
	maxStoragePrefetchRows        = 2048
	maxLearnedMappingSlot         = 15
	maxLearnedConstSlot           = 31
	maxLearnerRejectedSamples     = 4
	maxTemplateSingleMapSlots     = 3
	maxTemplateNestedMapSlots     = 2
	maxTemplateConstSlots         = 4
)

var (
	selectorERC20Transfer     = [4]byte{0xa9, 0x05, 0x9c, 0xbb} // transfer(address,uint256)
	selectorERC20TransferFrom = [4]byte{0x23, 0xb8, 0x72, 0xdd} // transferFrom(address,address,uint256)
	selectorERC20Approve      = [4]byte{0x09, 0x5e, 0xa7, 0xb3} // approve(address,uint256)
)

const (
	slotActorCaller uint8 = 0
	slotActorArg0   uint8 = 1
	slotActorArg1   uint8 = 2
)

type contractGenKey struct {
	address    tcommon.Address
	generation uint64
}

type contractStorageLayout struct {
	prefix   [storageKeyPrefixBytes]byte
	addrHash tcommon.Hash
	hashSlot bool
}

type contractStorageLayoutCache struct {
	mu      sync.RWMutex
	entries map[contractGenKey]contractStorageLayout
}

func newContractStorageLayoutCache() *contractStorageLayoutCache {
	return &contractStorageLayoutCache{
		entries: make(map[contractGenKey]contractStorageLayout, 256),
	}
}

func (c *contractStorageLayoutCache) get(addr tcommon.Address, generation uint64) (contractStorageLayout, bool) {
	if c == nil {
		return contractStorageLayout{}, false
	}
	c.mu.RLock()
	layout, ok := c.entries[contractGenKey{address: addr, generation: generation}]
	c.mu.RUnlock()
	return layout, ok
}

func (c *contractStorageLayoutCache) setEncoded(addr tcommon.Address, generation uint64, encoded []byte) {
	if c == nil || len(encoded) == 0 {
		return
	}
	fields, err := statecodec.ReadContractRuntime(encoded)
	if err != nil {
		return
	}
	addrHash := javaStorageAddressHash(addr, fields.TrxHash)
	var prefix [storageKeyPrefixBytes]byte
	copy(prefix[:], addrHash[:storageKeyPrefixBytes])
	layout := contractStorageLayout{
		prefix:   prefix,
		addrHash: addrHash,
		hashSlot: fields.Version == 1,
	}
	key := contractGenKey{address: addr, generation: generation}
	c.mu.Lock()
	if len(c.entries) >= maxContractLayoutCacheEntries {
		clear(c.entries)
	}
	c.entries[key] = layout
	c.mu.Unlock()
}

// ExtractTriggerStorageCall extracts the 4-byte ABI selector and EVM address
// arguments for supported TRC20/ERC20 standard calls (`transfer`, `transferFrom`,
// `approve`). Calldata that is shorter or uses another selector returns ok=false.
func ExtractTriggerStorageCall(owner, data []byte) (selector [4]byte, caller20, arg0, arg1 [20]byte, ok bool) {
	if len(owner) != tcommon.AddressLength || len(data) < 68 {
		return selector, caller20, arg0, arg1, false
	}
	copy(selector[:], data[:4])
	switch selector {
	case selectorERC20Transfer, selectorERC20Approve:
		copy(caller20[:], owner[1:21])
		copy(arg0[:], data[16:36])
		return selector, caller20, arg0, arg1, true
	case selectorERC20TransferFrom:
		if len(data) < 100 {
			return selector, caller20, arg0, arg1, false
		}
		copy(caller20[:], owner[1:21])
		copy(arg0[:], data[16:36])
		copy(arg1[:], data[48:68])
		return selector, caller20, arg0, arg1, true
	default:
		return selector, caller20, arg0, arg1, false
	}
}

type slotLearnerKey struct {
	address    tcommon.Address
	generation uint64
	selector   [4]byte
}

const (
	slotLearnLearning    uint8 = 0
	slotLearnLearned     uint8 = 1
	slotLearnUnlearnable uint8 = 2
)

type nestedMapSlotSpec struct {
	outerRole uint8
	innerRole uint8
	slot      uint8
}

type storageSlotTemplate struct {
	callerMapSlots []uint8
	arg0MapSlots   []uint8
	arg1MapSlots   []uint8
	nestedMapSlots []nestedMapSlotSpec
	constSlots     []uint8
}

type slotLearnerEntry struct {
	state           uint8
	samples         uint8
	candInitialized bool
	candCallerMask  uint16
	candArg0Mask    uint16
	candArg1Mask    uint16
	candNested      []nestedMapSlotSpec
	candConstMask   uint32
	template        storageSlotTemplate
}

type contractSlotLearner struct {
	mu      sync.RWMutex
	entries map[slotLearnerKey]*slotLearnerEntry
}

func newContractSlotLearner() *contractSlotLearner {
	return &contractSlotLearner{
		entries: make(map[slotLearnerKey]*slotLearnerEntry, 128),
	}
}

func (l *contractSlotLearner) needsObservation(addr tcommon.Address, generation uint64, selector [4]byte) bool {
	if l == nil {
		return false
	}
	key := slotLearnerKey{address: addr, generation: generation, selector: selector}
	l.mu.RLock()
	entry := l.entries[key]
	needs := entry == nil || entry.state == slotLearnLearning
	l.mu.RUnlock()
	return needs
}

func (l *contractSlotLearner) learnedTemplate(addr tcommon.Address, generation uint64, selector [4]byte) (storageSlotTemplate, bool) {
	if l == nil {
		return storageSlotTemplate{}, false
	}
	key := slotLearnerKey{address: addr, generation: generation, selector: selector}
	l.mu.RLock()
	entry := l.entries[key]
	ok := entry != nil && entry.state == slotLearnLearned
	var tpl storageSlotTemplate
	if ok {
		tpl = entry.template
	}
	l.mu.RUnlock()
	return tpl, ok
}

func (l *contractSlotLearner) learnedCount() int {
	if l == nil {
		return 0
	}
	l.mu.RLock()
	count := 0
	for _, entry := range l.entries {
		if entry.state == slotLearnLearned {
			count++
		}
	}
	l.mu.RUnlock()
	return count
}

type keccakBufHasher interface {
	hash.Hash
}

func computeMappingSlotWithHasher(h hash.Hash, scratch *[64]byte, key20 [20]byte, baseSlot uint8) tcommon.Hash {
	clear(scratch[:])
	copy(scratch[12:32], key20[:])
	scratch[63] = baseSlot
	h.Reset()
	_, _ = h.Write(scratch[:])
	var out tcommon.Hash
	h.Sum(out[:0])
	return out
}

func computeNestedMappingSlotWithHasher(h hash.Hash, scratch *[64]byte, innerKey20 [20]byte, outerHash tcommon.Hash) tcommon.Hash {
	clear(scratch[:32])
	copy(scratch[12:32], innerKey20[:])
	copy(scratch[32:64], outerHash[:])
	h.Reset()
	_, _ = h.Write(scratch[:])
	var out tcommon.Hash
	h.Sum(out[:0])
	return out
}

func actorKeyByRole(role uint8, caller20, arg0, arg1 [20]byte) [20]byte {
	switch role {
	case slotActorCaller:
		return caller20
	case slotActorArg0:
		return arg0
	default:
		return arg1
	}
}

func (l *contractSlotLearner) observe(addr tcommon.Address, generation uint64, selector [4]byte, caller20, arg0, arg1 [20]byte, observed []tcommon.Hash) (newlyLearned bool) {
	if l == nil || len(observed) == 0 {
		return false
	}
	key := slotLearnerKey{address: addr, generation: generation, selector: selector}
	l.mu.RLock()
	if existing := l.entries[key]; existing != nil && existing.state != slotLearnLearning {
		l.mu.RUnlock()
		return false
	}
	l.mu.RUnlock()

	h := sha3.NewLegacyKeccak256()
	var scratch [64]byte
	var (
		callerMask uint16
		arg0Mask   uint16
		arg1Mask   uint16
		constMask  uint32
		nested     []nestedMapSlotSpec
	)
	var callerHashes [maxLearnedMappingSlot + 1]tcommon.Hash
	var arg0Hashes [maxLearnedMappingSlot + 1]tcommon.Hash
	var arg1Hashes [maxLearnedMappingSlot + 1]tcommon.Hash
	for s := uint8(0); s <= maxLearnedMappingSlot; s++ {
		callerHashes[s] = computeMappingSlotWithHasher(h, &scratch, caller20, s)
		arg0Hashes[s] = computeMappingSlotWithHasher(h, &scratch, arg0, s)
		if selector == selectorERC20TransferFrom {
			arg1Hashes[s] = computeMappingSlotWithHasher(h, &scratch, arg1, s)
		}
	}
	for _, obs := range observed {
		if isLowScalarSlot(obs) {
			constMask |= uint32(1) << obs[31]
		}
		for s := uint8(0); s <= maxLearnedMappingSlot; s++ {
			bit := uint16(1) << s
			if obs == callerHashes[s] {
				callerMask |= bit
			}
			if obs == arg0Hashes[s] {
				arg0Mask |= bit
			}
			if selector == selectorERC20TransferFrom && obs == arg1Hashes[s] {
				arg1Mask |= bit
			}
		}
		switch selector {
		case selectorERC20TransferFrom:
			for s := uint8(0); s <= maxLearnedMappingSlot; s++ {
				if obs == computeNestedMappingSlotWithHasher(h, &scratch, caller20, arg0Hashes[s]) {
					nested = appendUniqueNestedSpec(nested, nestedMapSlotSpec{outerRole: slotActorArg0, innerRole: slotActorCaller, slot: s})
				}
			}
		case selectorERC20Approve:
			for s := uint8(0); s <= maxLearnedMappingSlot; s++ {
				if obs == computeNestedMappingSlotWithHasher(h, &scratch, arg0, callerHashes[s]) {
					nested = appendUniqueNestedSpec(nested, nestedMapSlotSpec{outerRole: slotActorCaller, innerRole: slotActorArg0, slot: s})
				}
			}
		}
	}

	l.mu.Lock()
	defer l.mu.Unlock()
	if len(l.entries) >= maxSlotLearnerEntries {
		clear(l.entries)
	}
	entry := l.entries[key]
	if entry == nil {
		entry = &slotLearnerEntry{}
		l.entries[key] = entry
	}
	if entry.state != slotLearnLearning {
		return false
	}
	entry.samples++
	hasMapping := callerMask != 0 || arg0Mask != 0 || arg1Mask != 0 || len(nested) != 0
	if !entry.candInitialized {
		if hasMapping {
			entry.candInitialized = true
			entry.candCallerMask = callerMask
			entry.candArg0Mask = arg0Mask
			entry.candArg1Mask = arg1Mask
			entry.candNested = nested
			entry.candConstMask = constMask
			return false
		}
		if entry.samples >= maxLearnerRejectedSamples {
			entry.state = slotLearnUnlearnable
		}
		return false
	}

	entry.candCallerMask &= callerMask
	entry.candArg0Mask &= arg0Mask
	entry.candArg1Mask &= arg1Mask
	entry.candNested = intersectNestedSpecs(entry.candNested, nested)
	entry.candConstMask &= constMask

	if entry.candCallerMask != 0 || entry.candArg0Mask != 0 || entry.candArg1Mask != 0 || len(entry.candNested) != 0 {
		entry.template = buildStorageSlotTemplate(
			entry.candCallerMask,
			entry.candArg0Mask,
			entry.candArg1Mask,
			entry.candNested,
			entry.candConstMask,
		)
		entry.candNested = nil
		entry.state = slotLearnLearned
		return true
	}
	if entry.samples >= maxLearnerRejectedSamples {
		entry.candNested = nil
		entry.state = slotLearnUnlearnable
	}
	return false
}

func isLowScalarSlot(slot tcommon.Hash) bool {
	if slot[31] > maxLearnedConstSlot {
		return false
	}
	for i := 0; i < 31; i++ {
		if slot[i] != 0 {
			return false
		}
	}
	return true
}

func appendUniqueNestedSpec(list []nestedMapSlotSpec, spec nestedMapSlotSpec) []nestedMapSlotSpec {
	for _, existing := range list {
		if existing == spec {
			return list
		}
	}
	return append(list, spec)
}

func intersectNestedSpecs(a, b []nestedMapSlotSpec) []nestedMapSlotSpec {
	if len(a) == 0 || len(b) == 0 {
		return nil
	}
	out := a[:0]
	for _, x := range a {
		for _, y := range b {
			if x == y {
				out = append(out, x)
				break
			}
		}
	}
	return out
}

func extractMaskBits(mask uint32, maxSlots, limit int) []uint8 {
	if mask == 0 {
		return nil
	}
	var out []uint8
	for s := 0; s <= maxSlots && len(out) < limit; s++ {
		if mask&(uint32(1)<<s) != 0 {
			out = append(out, uint8(s))
		}
	}
	return out
}

func buildStorageSlotTemplate(callerMask, arg0Mask, arg1Mask uint16, nested []nestedMapSlotSpec, constMask uint32) storageSlotTemplate {
	if len(nested) > maxTemplateNestedMapSlots {
		nested = append([]nestedMapSlotSpec(nil), nested[:maxTemplateNestedMapSlots]...)
	} else if len(nested) > 0 {
		nested = append([]nestedMapSlotSpec(nil), nested...)
	}
	return storageSlotTemplate{
		callerMapSlots: extractMaskBits(uint32(callerMask), maxLearnedMappingSlot, maxTemplateSingleMapSlots),
		arg0MapSlots:   extractMaskBits(uint32(arg0Mask), maxLearnedMappingSlot, maxTemplateSingleMapSlots),
		arg1MapSlots:   extractMaskBits(uint32(arg1Mask), maxLearnedMappingSlot, maxTemplateSingleMapSlots),
		nestedMapSlots: nested,
		constSlots:     extractMaskBits(constMask, maxLearnedConstSlot, maxTemplateConstSlots),
	}
}

func appendPredictedStorageSlots(dst []tcommon.Hash, tpl storageSlotTemplate, caller20, arg0, arg1 [20]byte) []tcommon.Hash {
	h := sha3.NewLegacyKeccak256()
	var scratch [64]byte
	for _, s := range tpl.callerMapSlots {
		dst = append(dst, computeMappingSlotWithHasher(h, &scratch, caller20, s))
	}
	for _, s := range tpl.arg0MapSlots {
		dst = append(dst, computeMappingSlotWithHasher(h, &scratch, arg0, s))
	}
	for _, s := range tpl.arg1MapSlots {
		dst = append(dst, computeMappingSlotWithHasher(h, &scratch, arg1, s))
	}
	for _, spec := range tpl.nestedMapSlots {
		outerKey := actorKeyByRole(spec.outerRole, caller20, arg0, arg1)
		innerKey := actorKeyByRole(spec.innerRole, caller20, arg0, arg1)
		outerHash := computeMappingSlotWithHasher(h, &scratch, outerKey, spec.slot)
		dst = append(dst, computeNestedMappingSlotWithHasher(h, &scratch, innerKey, outerHash))
	}
	for _, s := range tpl.constSlots {
		dst = append(dst, tcommon.Hash{31: s})
	}
	return dst
}
