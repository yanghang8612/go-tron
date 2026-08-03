package state

import (
	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/state/kvdomains"
	corepb "github.com/tronprotocol/go-tron/proto/core"
)

// TransactionWriteKind identifies the logical state cell family touched by a
// journaled transaction write. It is intentionally value-free: the first P4
// shadow pass needs conflict identity, not a second copy of the post-state.
type TransactionWriteKind uint8

const (
	TransactionWriteUnknown TransactionWriteKind = iota
	TransactionWriteAccount
	TransactionWriteAccountCreate
	TransactionWriteWitness
	TransactionWriteStorage
	TransactionWriteCode
	TransactionWriteContractMetadata
	TransactionWriteAccountKV
	TransactionWriteAccountKVReset
	TransactionWriteSelfDestruct
	TransactionWriteTransientStorage
	TransactionWriteDynamicProperties
)

// TransactionWrite describes one logical write found in StateDB's undo
// journal. KVKey is the journal-owned composite key and is valid only during
// the VisitTransactionWritesSince callback. StorageKey is populated only for
// TransactionWriteStorage. Callers that retain either must copy it.
type TransactionWrite struct {
	Kind        TransactionWriteKind
	Address     tcommon.Address
	KVDomain    kvdomains.KVDomain
	KVKey       string
	StorageKey  tcommon.Hash
	PropertyKey string
}

// TransactionAccessKind identifies one versioned logical state cell. The
// layout follows Erigon's typed account paths, extended with TRON's split
// account-KV and DynamicProperties stores. Values are deliberately excluded:
// P4.2 only needs dependency identity while canonical serial execution remains
// the source of truth.
type TransactionAccessKind uint8

const (
	TransactionAccessUnknown TransactionAccessKind = iota
	TransactionAccessAccount
	TransactionAccessWitness
	TransactionAccessStorage
	TransactionAccessCode
	TransactionAccessContractMetadata
	TransactionAccessAccountKV
	TransactionAccessAccountKVGeneration
	TransactionAccessSelfDestruct
	TransactionAccessTransientStorage
	TransactionAccessDynamicInt
	TransactionAccessDynamicString
	TransactionAccessDynamicHash
	TransactionAccessAccountField
	TransactionAccessRawKV
)

// TransactionAccountField follows Erigon's per-account path model, adapted to
// the hot scalar fields in TRON's much wider Account protobuf. Full-account
// readers remain TransactionAccessAccount barriers; account creation/deletion
// invalidates every field through the shadow validator's hierarchical version.
type TransactionAccountField uint8

const (
	TransactionAccountFieldUnknown TransactionAccountField = iota
	TransactionAccountFieldExistence
	TransactionAccountFieldAccountType
	TransactionAccountFieldBalance
	TransactionAccountFieldAllowance
	TransactionAccountFieldLatestWithdrawTime
	TransactionAccountFieldNetUsage
	TransactionAccountFieldLatestOperationTime
	TransactionAccountFieldLatestConsumeTime
	TransactionAccountFieldFreeNetUsage
	TransactionAccountFieldLatestConsumeFreeTime
	TransactionAccountFieldNetWindow
	TransactionAccountFieldFrozenResource
)

// TransactionAccountFieldKey is kept smaller than TransactionAccessKey so hot
// account-path recording and validation do not hash unused storage/string
// fields. This mirrors Erigon's per-path typed maps.
type TransactionAccountFieldKey struct {
	Address tcommon.Address
	Field   TransactionAccountField
}

// TransactionAccessKey is comparable and can therefore be used directly by a
// block-local version map. LogicalKey is populated for account-KV and dynamic
// property cells. StorageKey is populated for persistent/transient storage.
type TransactionAccessKey struct {
	Kind         TransactionAccessKind
	Address      tcommon.Address
	AccountField TransactionAccountField
	KVDomain     kvdomains.KVDomain
	StorageKey   tcommon.Hash
	LogicalKey   string
}

// TransactionAccessMode is a bit set because a transaction can both read and
// write the same logical cell.
type TransactionAccessMode uint8

const (
	TransactionAccessRead TransactionAccessMode = 1 << iota
	TransactionAccessWrite
	// TransactionAccessCommutativeRead/Write mark the internal read-modify-write
	// performed by a protocol settlement accumulator. The canonical serial
	// mutation is unchanged. P4.3 uses the distinction only to model a future
	// worker returning a delta that is applied at ordered publication time.
	TransactionAccessCommutativeRead
	TransactionAccessCommutativeWrite
)

// TransactionRead records one logical path consumed by speculative execution.
// Keys are owned by the source recorder, including LogicalKey strings, so a
// captured set remains valid after the recorder is reset for another task.
// HasExpectedWriter is false for a block-start/storage read. When true,
// ExpectedWriter is the earlier transaction whose published value the worker
// consumed. This mirrors Erigon's versioned read carrier and lets an ordered
// publisher distinguish a valid dependency from a stale read.
type TransactionRead struct {
	Key               TransactionAccessKey
	Mode              TransactionAccessMode
	ExpectedWriter    int
	HasExpectedWriter bool
}

// TransactionReadSet is the version carrier returned by a speculative worker.
// Unsupported marks range/unknown reads that cannot be validated by exact
// logical-key versions and must fall back to serial replay.
type TransactionReadSet struct {
	Reads       []TransactionRead
	Unsupported bool
}

// PublicNetReservation describes one successful free-bandwidth admission.
// Unlike an unconditional commutative counter, public_net_usage is guarded by
// a global limit and is recovered against public_net_time before every use.
// Speculative workers therefore retain the complete admission input so the
// ordered publisher can repeat the java-tron limit check against the state
// left by all preceding transactions.
type PublicNetReservation struct {
	StartUsage     int64
	StartTime      int64
	RecoveredUsage int64
	ResourceTime   int64
	Delta          int64
	Limit          int64
}

// TransactionAccessRecorder is a transaction-scoped, reusable read/write set.
// Canonical execution installs one recorder on StateDB and DynamicProperties,
// resets it at the transaction boundary, and removes it before advancing. The
// map retains bounded buckets across transactions; arbitrary account-KV keys
// are copied only on the first unique access in a transaction.
type TransactionAccessRecorder struct {
	accesses             map[TransactionAccessKey]TransactionAccessMode
	accounts             map[tcommon.Address]TransactionAccessMode
	accountFields        map[TransactionAccountFieldKey]TransactionAccessMode
	accountFieldWrites   map[tcommon.Address]struct{}
	commutativeDeltas    map[TransactionAccessKey]int64
	rawKVWrites          map[TransactionAccessKey]TransactionWriteValue
	rawKVKeys            map[string]string
	unsupported          bool
	commutativeScopeKey  TransactionAccessKey
	commutativeScopeOpen bool
	publicNetReservation PublicNetReservation
	publicNetRecorded    bool
	publicNetValid       bool
}

// Reset begins a new transaction capture. capacityHint is used only for the
// first allocation; subsequent transactions reuse the existing map buckets.
func (r *TransactionAccessRecorder) Reset(capacityHint int) {
	if r == nil {
		return
	}
	if r.accesses == nil {
		if capacityHint < 16 {
			capacityHint = 16
		}
		r.accesses = make(map[TransactionAccessKey]TransactionAccessMode, capacityHint)
	} else {
		clear(r.accesses)
	}
	clear(r.accounts)
	clear(r.accountFields)
	clear(r.accountFieldWrites)
	clear(r.commutativeDeltas)
	clear(r.rawKVWrites)
	r.unsupported = false
	r.commutativeScopeKey = TransactionAccessKey{}
	r.commutativeScopeOpen = false
	r.publicNetReservation = PublicNetReservation{}
	r.publicNetRecorded = false
	r.publicNetValid = true
}

// RecordPublicNetReservation retains the conditional global-bandwidth update
// made by the authoritative serial bandwidth processor. Recording more than
// one non-identical reservation in a transaction is rejected conservatively.
func (r *TransactionAccessRecorder) RecordPublicNetReservation(reservation PublicNetReservation) {
	if r == nil {
		return
	}
	if r.publicNetRecorded && r.publicNetReservation != reservation {
		r.publicNetValid = false
		return
	}
	r.publicNetReservation = reservation
	r.publicNetRecorded = true
}

// PublicNetReservation returns the transaction-local conditional reservation.
func (r *TransactionAccessRecorder) PublicNetReservation() (PublicNetReservation, bool) {
	if r == nil || !r.publicNetRecorded || !r.publicNetValid {
		return PublicNetReservation{}, false
	}
	return r.publicNetReservation, true
}

func (r *TransactionAccessRecorder) recordCommutativeDelta(key TransactionAccessKey, delta int64) {
	if r == nil || delta == 0 {
		return
	}
	if r.commutativeDeltas == nil {
		r.commutativeDeltas = make(map[TransactionAccessKey]int64, 8)
	}
	r.commutativeDeltas[key] += delta
}

// CommutativeDelta returns the transaction-local settlement increment for key.
// The absolute state value remains canonical and serial; ordered publication
// applies this delta after validating all ordinary reads.
func (r *TransactionAccessRecorder) CommutativeDelta(key TransactionAccessKey) (int64, bool) {
	if r == nil {
		return 0, false
	}
	delta, ok := r.commutativeDeltas[key]
	return delta, ok
}

// Visit visits each unique access in unspecified order. Returning false stops
// the walk. Callers must not mutate the recorder from inside visit.
func (r *TransactionAccessRecorder) Visit(visit func(TransactionAccessKey, TransactionAccessMode) bool) {
	if r == nil || visit == nil {
		return
	}
	for key, mode := range r.accesses {
		if !visit(key, mode) {
			return
		}
	}
	for address, mode := range r.accounts {
		if !visit(TransactionAccessKey{Kind: TransactionAccessAccount, Address: address}, mode) {
			return
		}
	}
	for key, mode := range r.accountFields {
		if !visit(TransactionAccessKey{Kind: TransactionAccessAccountField, Address: key.Address, AccountField: key.Field}, mode) {
			return
		}
	}
}

func (r *TransactionAccessRecorder) Len() int {
	if r == nil {
		return 0
	}
	return len(r.accesses) + len(r.accounts) + len(r.accountFields)
}

func (r *TransactionAccessRecorder) Unsupported() bool {
	return r != nil && r.unsupported
}

// TransactionAccessRecorderCoversWrites reports whether every authoritative
// mutation of kind is registered inline. Projected write capture may skip the
// undo journal only when every retained kind returns true here.
func TransactionAccessRecorderCoversWrites(kind TransactionAccessKind) bool {
	switch kind {
	case TransactionAccessAccount,
		TransactionAccessAccountField,
		TransactionAccessAccountKV,
		TransactionAccessAccountKVGeneration:
		return true
	default:
		return false
	}
}

// CaptureReadSet freezes the exact reads observed by one worker result before
// its recorder is reused. Ordinary reads participate in version validation;
// commutative reads are retained so the publisher can audit that their write
// set carries an ordered delta instead of an absolute block-start value.
func (r *TransactionAccessRecorder) CaptureReadSet() TransactionReadSet {
	if r == nil {
		return TransactionReadSet{}
	}
	reads := make([]TransactionRead, 0, r.Len())
	r.Visit(func(key TransactionAccessKey, mode TransactionAccessMode) bool {
		readMode := mode & (TransactionAccessRead | TransactionAccessCommutativeRead)
		if readMode != 0 {
			reads = append(reads, TransactionRead{Key: key, Mode: readMode})
		}
		return true
	})
	return TransactionReadSet{Reads: reads, Unsupported: r.unsupported}
}

// RestoreReadSet installs a frozen worker read set into the recorder used for
// canonical ordered publication. The caller resets the recorder first; writes
// produced by the typed applier are then added to the same transaction view.
func (r *TransactionAccessRecorder) RestoreReadSet(reads TransactionReadSet) {
	if r == nil {
		return
	}
	if reads.Unsupported {
		r.unsupported = true
	}
	for _, read := range reads.Reads {
		r.record(read.Key, read.Mode&(TransactionAccessRead|TransactionAccessCommutativeRead))
	}
}

func (r *TransactionAccessRecorder) markUnsupported() {
	if r != nil {
		r.unsupported = true
	}
}

func (r *TransactionAccessRecorder) record(key TransactionAccessKey, mode TransactionAccessMode) {
	if r == nil {
		return
	}
	if r.commutativeScopeOpen && key == r.commutativeScopeKey {
		if mode&TransactionAccessRead != 0 {
			mode = mode&^TransactionAccessRead | TransactionAccessCommutativeRead
		}
		if mode&TransactionAccessWrite != 0 {
			mode = mode&^TransactionAccessWrite | TransactionAccessCommutativeWrite
		}
	}
	switch key.Kind {
	case TransactionAccessAccount:
		if r.accounts == nil {
			r.accounts = make(map[tcommon.Address]TransactionAccessMode, 16)
		}
		r.accounts[key.Address] |= mode
		return
	case TransactionAccessAccountField:
		if r.accountFields == nil {
			r.accountFields = make(map[TransactionAccountFieldKey]TransactionAccessMode, 32)
		}
		fieldKey := TransactionAccountFieldKey{Address: key.Address, Field: key.AccountField}
		r.accountFields[fieldKey] |= mode
		if mode&(TransactionAccessWrite|TransactionAccessCommutativeWrite) != 0 {
			if r.accountFieldWrites == nil {
				r.accountFieldWrites = make(map[tcommon.Address]struct{}, 16)
			}
			r.accountFieldWrites[key.Address] = struct{}{}
		}
		return
	}
	if r.accesses == nil {
		r.accesses = make(map[TransactionAccessKey]TransactionAccessMode, 16)
	}
	r.accesses[key] |= mode
}

// AccountWriteCoverage lets the journal observer distinguish a full Account
// mutation from an accountScalarChange represented by exact inline field
// writes. No field write means an unrecognized journal path and therefore
// remains a conservative full-account barrier.
func (r *TransactionAccessRecorder) AccountWriteCoverage(address tcommon.Address) (full, fields bool) {
	if r == nil {
		return false, false
	}
	full = r.accounts[address]&(TransactionAccessWrite|TransactionAccessCommutativeWrite) != 0
	_, fields = r.accountFieldWrites[address]
	return full, fields
}

// beginCommutativeScope reclassifies only the exact logical cell subsequently
// accessed by a settlement helper. Reads of any other cell remain ordinary
// dependencies. The returned state must be restored with endCommutativeScope;
// this explicit pair keeps the hot path allocation-free.
func (r *TransactionAccessRecorder) beginCommutativeScope(key TransactionAccessKey) (TransactionAccessKey, bool) {
	previousKey, previousOpen := r.commutativeScopeKey, r.commutativeScopeOpen
	r.commutativeScopeKey, r.commutativeScopeOpen = key, true
	return previousKey, previousOpen
}

func (r *TransactionAccessRecorder) endCommutativeScope(previousKey TransactionAccessKey, previousOpen bool) {
	r.commutativeScopeKey, r.commutativeScopeOpen = previousKey, previousOpen
}

func (r *TransactionAccessRecorder) recordAccountKV(owner tcommon.Address, domain kvdomains.KVDomain, logicalKey []byte, mode TransactionAccessMode) {
	if r == nil {
		return
	}
	if r.accesses == nil {
		r.accesses = make(map[TransactionAccessKey]TransactionAccessMode, 16)
	}
	lookup := TransactionAccessKey{
		Kind:       TransactionAccessAccountKV,
		Address:    owner,
		KVDomain:   domain,
		LogicalKey: borrowedBytesString(logicalKey),
	}
	if previous, ok := r.accesses[lookup]; ok {
		r.accesses[lookup] = previous | mode
		return
	}
	// The caller may lend stack/scratch bytes. Own the key only for the first
	// unique access; the borrowed lookup above makes repeats allocation-free.
	lookup.LogicalKey = string(logicalKey)
	r.accesses[lookup] = mode
}

func (r *TransactionAccessRecorder) recordRawKV(key []byte, mode TransactionAccessMode) string {
	if r == nil {
		return ""
	}
	if r.accesses == nil {
		r.accesses = make(map[TransactionAccessKey]TransactionAccessMode, 16)
	}
	// The recorder lives for one block and Reset only starts a new transaction.
	// Intern raw physical keys across those transactions: TAPOS and BLOCKHASH
	// repeatedly probe the same small set, so this owns each key once per block
	// instead of allocating a string for every transaction. The borrowed view is
	// lookup-only; both maps retain the stable owned string.
	if r.rawKVKeys == nil {
		r.rawKVKeys = make(map[string]string, 16)
	}
	borrowed := borrowedBytesString(key)
	stable, ok := r.rawKVKeys[borrowed]
	if !ok {
		stable = string(key)
		r.rawKVKeys[stable] = stable
	}
	accessKey := TransactionAccessKey{Kind: TransactionAccessRawKV, LogicalKey: stable}
	r.accesses[accessKey] |= mode
	return stable
}

// RecordRawKVRead adds a direct Context.DB read to the transaction's versioned
// read set. StateDB's typed accessors do not use this path; it covers only the
// remaining actuator/VM raw database surface.
func (r *TransactionAccessRecorder) RecordRawKVRead(key []byte) {
	r.recordRawKV(key, TransactionAccessRead)
}

// RecordRawKVPut records the final transaction-local value of a direct raw DB
// write. The value and key are owned because the caller may lend scratch bytes.
func (r *TransactionAccessRecorder) RecordRawKVPut(key, value []byte) {
	if r == nil {
		return
	}
	stable := r.recordRawKV(key, TransactionAccessWrite)
	if r.rawKVWrites == nil {
		r.rawKVWrites = make(map[TransactionAccessKey]TransactionWriteValue, 4)
	}
	writeKey := TransactionAccessKey{Kind: TransactionAccessRawKV, LogicalKey: stable}
	r.rawKVWrites[writeKey] = ownedTransactionWriteValue(true, value)
}

// RecordRawKVDelete records a direct raw DB deletion including its absence
// post-image, so ordered publication can distinguish it from an empty value.
func (r *TransactionAccessRecorder) RecordRawKVDelete(key []byte) {
	if r == nil {
		return
	}
	stable := r.recordRawKV(key, TransactionAccessWrite)
	if r.rawKVWrites == nil {
		r.rawKVWrites = make(map[TransactionAccessKey]TransactionWriteValue, 4)
	}
	writeKey := TransactionAccessKey{Kind: TransactionAccessRawKV, LogicalKey: stable}
	r.rawKVWrites[writeKey] = TransactionWriteValue{}
}

func (r *TransactionAccessRecorder) rawKVWrite(key TransactionAccessKey) (TransactionWriteValue, bool) {
	if r == nil {
		return TransactionWriteValue{}, false
	}
	value, ok := r.rawKVWrites[key]
	return value, ok
}

func (s *StateDB) recordAccountRead(address tcommon.Address) {
	if s != nil && s.transactionAccess != nil {
		s.transactionAccess.record(TransactionAccessKey{Kind: TransactionAccessAccount, Address: address}, TransactionAccessRead)
	}
}

func (s *StateDB) recordAccountWrite(address tcommon.Address) {
	if s != nil && s.transactionAccess != nil {
		s.transactionAccess.record(TransactionAccessKey{Kind: TransactionAccessAccount, Address: address}, TransactionAccessWrite)
	}
}

func (s *StateDB) recordAccountFieldRead(address tcommon.Address, field TransactionAccountField) {
	if s != nil && s.transactionAccess != nil {
		s.transactionAccess.record(TransactionAccessKey{Kind: TransactionAccessAccountField, Address: address, AccountField: field}, TransactionAccessRead)
	}
}

func (s *StateDB) recordAccountFieldWrite(address tcommon.Address, field TransactionAccountField) {
	if s != nil && s.transactionAccess != nil {
		s.transactionAccess.record(TransactionAccessKey{Kind: TransactionAccessAccountField, Address: address, AccountField: field}, TransactionAccessWrite)
	}
}

func (s *StateDB) recordWitnessRead(address tcommon.Address) {
	if s != nil && s.transactionAccess != nil {
		s.transactionAccess.record(TransactionAccessKey{Kind: TransactionAccessWitness, Address: address}, TransactionAccessRead)
	}
}

func (s *StateDB) recordStorageRead(address tcommon.Address, key tcommon.Hash) {
	if s != nil && s.transactionAccess != nil {
		s.transactionAccess.record(TransactionAccessKey{Kind: TransactionAccessStorage, Address: address, StorageKey: key}, TransactionAccessRead)
	}
}

func (s *StateDB) recordCodeRead(address tcommon.Address) {
	if s != nil && s.transactionAccess != nil {
		s.transactionAccess.record(TransactionAccessKey{Kind: TransactionAccessCode, Address: address}, TransactionAccessRead)
	}
}

func (s *StateDB) recordContractMetadataRead(address tcommon.Address) {
	if s != nil && s.transactionAccess != nil {
		s.transactionAccess.record(TransactionAccessKey{Kind: TransactionAccessContractMetadata, Address: address}, TransactionAccessRead)
	}
}

func (s *StateDB) recordSelfDestructRead(address tcommon.Address) {
	if s != nil && s.transactionAccess != nil {
		s.transactionAccess.record(TransactionAccessKey{Kind: TransactionAccessSelfDestruct, Address: address}, TransactionAccessRead)
	}
}

func (s *StateDB) recordTransientStorageRead(address tcommon.Address, key tcommon.Hash) {
	if s != nil && s.transactionAccess != nil {
		s.transactionAccess.record(TransactionAccessKey{Kind: TransactionAccessTransientStorage, Address: address, StorageKey: key}, TransactionAccessRead)
	}
}

func (s *StateDB) recordAccountKVRead(owner tcommon.Address, domain kvdomains.KVDomain, logicalKey []byte) {
	if s == nil || s.transactionAccess == nil {
		return
	}
	// Every logical KV read also consumes the owner's namespace generation.
	// ResetAccountKV/recreation writes that cell, preventing an old-generation
	// speculative result from validating against a new namespace.
	s.transactionAccess.record(TransactionAccessKey{Kind: TransactionAccessAccountKVGeneration, Address: owner}, TransactionAccessRead)
	s.transactionAccess.recordAccountKV(owner, domain, logicalKey, TransactionAccessRead)
}

func (s *StateDB) recordAccountKVWrite(owner tcommon.Address, domain kvdomains.KVDomain, logicalKey []byte) {
	if s == nil || s.transactionAccess == nil {
		return
	}
	s.transactionAccess.recordAccountKV(owner, domain, logicalKey, TransactionAccessWrite)
}

func (s *StateDB) recordAccountKVGenerationWrite(owner tcommon.Address) {
	if s != nil && s.transactionAccess != nil {
		s.transactionAccess.record(TransactionAccessKey{Kind: TransactionAccessAccountKVGeneration, Address: owner}, TransactionAccessWrite)
	}
}

func (s *StateDB) recordAccountKVPrefixRead(owner tcommon.Address) {
	if s == nil || s.transactionAccess == nil {
		return
	}
	s.transactionAccess.record(TransactionAccessKey{Kind: TransactionAccessAccountKVGeneration, Address: owner}, TransactionAccessRead)
	// Exact keys returned by an iterator are not sufficient: a predecessor can
	// insert a previously absent key under the prefix. Keep such transactions
	// out of speculative publication until range-version cells are implemented.
	s.transactionAccess.markUnsupported()
}

// SetTransactionAccessRecorder installs the same transaction-scoped recorder
// used by StateDB. Passing nil disables capture.
func (dp *DynamicProperties) SetTransactionAccessRecorder(recorder *TransactionAccessRecorder) {
	if dp != nil {
		dp.transactionAccess = recorder
	}
}

// RecordPublicNetReservation retains the limit-checked free-bandwidth update
// for ordered speculative publication. It is observe-only and has no effect
// when transaction access capture is disabled.
func (dp *DynamicProperties) RecordPublicNetReservation(reservation PublicNetReservation) {
	if dp != nil && dp.transactionAccess != nil {
		dp.transactionAccess.RecordPublicNetReservation(reservation)
	}
}

func (dp *DynamicProperties) recordDynamicAccess(kind TransactionAccessKind, key string, mode TransactionAccessMode) {
	if dp != nil && dp.transactionAccess != nil {
		dp.transactionAccess.record(TransactionAccessKey{Kind: kind, LogicalKey: key}, mode)
	}
}

// addCommutativeInt keeps the authoritative serial read-modify-write while
// labelling its internal dependency as a settlement delta. Any ordinary read
// of the same key elsewhere in the transaction is recorded separately and
// therefore still prevents normalized first-pass validation.
func (dp *DynamicProperties) addCommutativeInt(key string, delta int64) {
	if dp == nil {
		return
	}
	if recorder := dp.transactionAccess; recorder != nil {
		accessKey := TransactionAccessKey{Kind: TransactionAccessDynamicInt, LogicalKey: key}
		recorder.recordCommutativeDelta(accessKey, delta)
		previousKey, previousOpen := recorder.beginCommutativeScope(accessKey)
		dp.Set(key, dp.readInt(key)+delta)
		recorder.endCommutativeScope(previousKey, previousOpen)
		return
	}
	dp.Set(key, dp.readInt(key)+delta)
}

func (dp *DynamicProperties) readInt(key string) int64 {
	if dp == nil {
		return 0
	}
	dp.recordDynamicAccess(TransactionAccessDynamicInt, key, TransactionAccessRead)
	return dp.props[key]
}

func (dp *DynamicProperties) readString(key string) string {
	if dp == nil {
		return ""
	}
	dp.recordDynamicAccess(TransactionAccessDynamicString, key, TransactionAccessRead)
	return dp.stringProps[key]
}

func (dp *DynamicProperties) recordAllDynamicReadsUnsupported() {
	if dp == nil || dp.transactionAccess == nil {
		return
	}
	dp.transactionAccess.markUnsupported()
}

func transactionAccessFromWrite(write TransactionWrite) (TransactionAccessKey, bool) {
	switch write.Kind {
	case TransactionWriteAccount, TransactionWriteAccountCreate:
		return TransactionAccessKey{Kind: TransactionAccessAccount, Address: write.Address}, true
	case TransactionWriteWitness:
		return TransactionAccessKey{Kind: TransactionAccessWitness, Address: write.Address}, true
	case TransactionWriteStorage:
		return TransactionAccessKey{Kind: TransactionAccessStorage, Address: write.Address, StorageKey: write.StorageKey}, true
	case TransactionWriteCode:
		return TransactionAccessKey{Kind: TransactionAccessCode, Address: write.Address}, true
	case TransactionWriteContractMetadata:
		return TransactionAccessKey{Kind: TransactionAccessContractMetadata, Address: write.Address}, true
	case TransactionWriteAccountKV:
		if len(write.KVKey) < 2 {
			return TransactionAccessKey{}, false
		}
		return TransactionAccessKey{
			Kind:       TransactionAccessAccountKV,
			Address:    write.Address,
			KVDomain:   write.KVDomain,
			LogicalKey: write.KVKey[2:],
		}, true
	case TransactionWriteAccountKVReset:
		return TransactionAccessKey{Kind: TransactionAccessAccountKVGeneration, Address: write.Address}, true
	case TransactionWriteSelfDestruct:
		return TransactionAccessKey{Kind: TransactionAccessSelfDestruct, Address: write.Address}, true
	case TransactionWriteTransientStorage:
		return TransactionAccessKey{Kind: TransactionAccessTransientStorage, Address: write.Address, StorageKey: write.StorageKey}, true
	case TransactionWriteDynamicProperties:
		if write.PropertyKey == "" {
			return TransactionAccessKey{}, false
		}
		return TransactionAccessKey{Kind: TransactionAccessDynamicInt, LogicalKey: write.PropertyKey}, true
	default:
		return TransactionAccessKey{}, false
	}
}

// VisitTransactionWritesSince visits journaled writes at or after mark without
// allocating a second write set. It is the observation boundary for P4 shadow
// conflict analysis; canonical rollback and persistence continue to consume
// the original journal unchanged. Returning false stops the walk.
//
// Unknown journal entry types are reported conservatively as
// TransactionWriteUnknown so a future state mutation cannot accidentally be
// considered safe for speculative publication.
func (s *StateDB) VisitTransactionWritesSince(mark int, visit func(TransactionWrite) bool) {
	if s == nil || visit == nil {
		return
	}
	if mark < 0 {
		mark = 0
	}
	if mark > s.journal.length() {
		mark = s.journal.length()
	}
	for _, entry := range s.journal.entries[mark:] {
		write := transactionWriteFromJournal(entry)
		if !visit(write) {
			return
		}
	}
}

// VisitTransactionAccessWritesSince projects the authoritative undo journal
// onto version-map cell identities. It returns false when an unknown journal
// entry is encountered; already-visited writes remain valid, but the caller
// must treat the transaction as unsupported for speculative publication.
func (s *StateDB) VisitTransactionAccessWritesSince(mark int, visit func(TransactionAccessKey) bool) bool {
	known := true
	s.VisitTransactionWritesSince(mark, func(write TransactionWrite) bool {
		key, ok := transactionAccessFromWrite(write)
		if !ok {
			known = false
			return false
		}
		return visit == nil || visit(key)
	})
	return known
}

func transactionWriteFromJournal(entry journalChange) TransactionWrite {
	switch change := entry.(type) {
	case accountChange:
		kind := TransactionWriteAccount
		if change.prev == nil {
			kind = TransactionWriteAccountCreate
		}
		return TransactionWrite{Kind: kind, Address: change.address}
	case *accountScalarChange:
		return TransactionWrite{Kind: TransactionWriteAccount, Address: change.address}
	case witnessChange:
		return TransactionWrite{Kind: TransactionWriteWitness, Address: change.address}
	case *storageChange:
		return TransactionWrite{Kind: TransactionWriteStorage, Address: change.address, StorageKey: change.key}
	case codeChange:
		return TransactionWrite{Kind: TransactionWriteCode, Address: change.address}
	case contractMetaChange:
		return TransactionWrite{Kind: TransactionWriteContractMetadata, Address: change.address}
	case selfDestructChange:
		return TransactionWrite{Kind: TransactionWriteSelfDestruct, Address: change.address}
	case kvChange:
		return transactionKVWrite(change.address, change.mapKey)
	case *kvChange:
		if change == nil {
			return TransactionWrite{Kind: TransactionWriteUnknown}
		}
		return transactionKVWrite(change.address, change.mapKey)
	case kvResetChange:
		return TransactionWrite{Kind: TransactionWriteAccountKVReset, Address: change.address}
	case transientStorageChange:
		return TransactionWrite{Kind: TransactionWriteTransientStorage, Address: change.tk.addr, StorageKey: change.tk.key}
	case resourceWeightChange:
		var key string
		switch change.resource {
		case corepb.ResourceCode_BANDWIDTH:
			key = "total_net_weight"
		case corepb.ResourceCode_ENERGY:
			key = "total_energy_weight"
		case corepb.ResourceCode_TRON_POWER:
			key = "total_tron_power_weight"
		}
		return TransactionWrite{Kind: TransactionWriteDynamicProperties, PropertyKey: key}
	default:
		return TransactionWrite{Kind: TransactionWriteUnknown}
	}
}

func transactionKVWrite(address tcommon.Address, mapKey string) TransactionWrite {
	domain, _, ok := splitKVCompositeKeyView([]byte(mapKey))
	if !ok {
		return TransactionWrite{Kind: TransactionWriteUnknown, Address: address, KVKey: mapKey}
	}
	return TransactionWrite{
		Kind:     TransactionWriteAccountKV,
		Address:  address,
		KVDomain: domain,
		KVKey:    mapKey,
	}
}
