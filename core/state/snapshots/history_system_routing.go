package snapshots

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math/bits"
	"os"
	"path/filepath"
	"sort"
	"sync"

	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/state/kvdomains"
)

const (
	systemHistoryRoutingMaxBytes         = 64 << 20
	systemHistoryRoutingMaxSources       = 1 << 18
	systemHistoryRoutingMaxKeys          = 1 << 20
	systemHistoryRoutingMaxNewSources    = 64
	systemHistoryRoutingMaxAccessorBytes = 128 << 20
	systemHistoryRoutingHeaderSize       = 40
	systemHistoryRoutingSourceKeyBytes   = 16 << 20
	systemHistoryRoutingBuildKeyBytes    = 32 << 20
)

var (
	systemHistoryRoutingMagic     = [8]byte{'G', 'T', 'S', 'Y', 'S', 'R', '0', '1'}
	errSystemHistoryRoutingBudget = errors.New("snapshots: system history routing budget exceeded")
)

// One decoded immutable object, admitted by a conservative byte charge. Every
// cross-request hit still reads and hashes CURRENT physical bytes first.
var sharedSystemHistoryRouting struct {
	sync.Mutex
	digest [32]byte
	value  *systemHistoryRouting
}

type systemHistoryRouting struct {
	sources         [][sha256.Size]byte
	columns         map[[sha256.Size]byte]int
	keys            map[string][]byte
	from, to        uint64
	storagePrefixes bool
}

// Bind all pair fields, not just a range or an inode. Stale coverage can never
// prove absence for a replacement, even when the replacement has the same range.
func systemHistoryRoutingPairID(history, accessor SegmentRef) [sha256.Size]byte {
	h := sha256.New()
	h.Write([]byte("go-tron/system-history-routing/pair/v1\x00"))
	for _, ref := range []SegmentRef{history, accessor} {
		var fields [42]byte
		binary.BigEndian.PutUint64(fields[0:8], ref.FromTxNum)
		binary.BigEndian.PutUint64(fields[8:16], ref.ToTxNum)
		binary.BigEndian.PutUint64(fields[16:24], ref.effectiveAggregationSteps())
		binary.BigEndian.PutUint64(fields[24:32], ref.Size)
		binary.BigEndian.PutUint64(fields[32:40], ref.Rows)
		binary.BigEndian.PutUint16(fields[40:42], uint16(ref.Domain))
		h.Write(fields[:])
		for _, value := range []string{string(ref.normalizedDataset()), string(ref.Kind), ref.Path, ref.Checksum} {
			var size [8]byte
			binary.BigEndian.PutUint64(size[:], uint64(len(value)))
			h.Write(size[:])
			h.Write([]byte(value))
		}
	}
	var out [sha256.Size]byte
	copy(out[:], h.Sum(nil))
	return out
}

func systemHistoryRoutingKey(key []byte) bool {
	if len(key) == 21 && (key[0] == byte(rawdb.StateFlatDomainAccountLatest) || key[0] == byte(rawdb.StateFlatDomainKVGeneration)) {
		return bytes.Equal(key[1:], common.SystemAccountID[:]) || bytes.Equal(key[1:], make([]byte, 20))
	}
	if len(key) < 31 || key[0] != byte(rawdb.StateFlatDomainKVLatest) {
		return false
	}
	domain := kvdomains.KVDomain(binary.BigEndian.Uint16(key[29:31]))
	if domain == kvdomains.ContractStorage {
		return len(key) == 31 // owner/generation/domain membership, never a slot
	}
	return domain == kvdomains.ContractMetadata || bytes.Equal(key[1:21], common.SystemAccountID[:]) && (domain == kvdomains.SystemDynamicProperty || domain == kvdomains.SystemForkVote)
}

func contractStorageHistoryRoutingKey(key []byte) bool {
	return len(key) >= 31 && key[0] == byte(rawdb.StateFlatDomainKVLatest) && kvdomains.KVDomain(binary.BigEndian.Uint16(key[29:31])) == kvdomains.ContractStorage
}

func normalizedHistoryRoutingKey(key []byte) []byte {
	if contractStorageHistoryRoutingKey(key) {
		return key[:31]
	}
	return key
}

func systemHistoryRoutingQuery(q StateHistoryQuery) bool {
	if q.FlatDomain == rawdb.StateFlatDomainAccountLatest || q.FlatDomain == rawdb.StateFlatDomainKVGeneration {
		return common.IsSystemAccount(q.Owner) || q.Owner.AccountID() == (common.AccountID{})
	}
	return q.FlatDomain == rawdb.StateFlatDomainKVLatest && (q.Domain == kvdomains.ContractStorage || q.Domain == kvdomains.ContractMetadata || q.Owner == common.SystemAccountAddress && (q.Domain == kvdomains.SystemDynamicProperty || q.Domain == kvdomains.SystemForkVote))
}

// Members are either a dense bitmap (tag zero), or strictly increasing uint32
// column numbers (tag one). Sparse contract metadata never pays one bit per
// history segment for each contract that was created only once.
func systemHistoryMembersContain(members []byte, column int) bool {
	if len(members) == 0 {
		return false
	}
	if members[0] == 0 {
		return members[1+column/8]&(1<<uint(column%8)) != 0
	}
	n := (len(members) - 1) / 4
	pos := sort.Search(n, func(i int) bool { return binary.BigEndian.Uint32(members[1+4*i:]) >= uint32(column) })
	return pos < n && binary.BigEndian.Uint32(members[1+4*pos:]) == uint32(column)
}

func systemHistoryMembersIterate(members []byte, fn func(int)) {
	if members[0] == 1 {
		for off := 1; off < len(members); off += 4 {
			fn(int(binary.BigEndian.Uint32(members[off:])))
		}
		return
	}
	for i, b := range members[1:] {
		for b != 0 {
			pos := bits.TrailingZeros8(b)
			fn(i*8 + pos)
			b &= b - 1
		}
	}
}

func (r *systemHistoryRouting) mayContain(column int, lookup []byte) bool {
	if contractStorageHistoryRoutingKey(lookup) && !r.storagePrefixes {
		// V1 coverage attests only the original families. Missing storage
		// entries in that object must never prove a storage key absent.
		return true
	}
	return systemHistoryMembersContain(r.keys[string(normalizedHistoryRoutingKey(lookup))], column)
}

func loadSystemHistoryRouting(ctx context.Context, dir string, manifest *Manifest, window ...TxRange) (*systemHistoryRouting, error) {
	var found *SegmentRef
	for i := range manifest.Segments {
		if manifest.Segments[i].Kind == SegmentSystemHistoryRouting {
			if found != nil {
				return nil, errors.New("snapshots: multiple system history routing segments")
			}
			found = &manifest.Segments[i]
		}
	}
	if found == nil {
		return nil, nil
	}
	if len(window) != 0 && (found.ToTxNum < window[0].From || found.FromTxNum > window[0].To) {
		return nil, nil // The index cannot skip a pair in this request's range.
	}
	return readSystemHistoryRouting(ctx, dir, *found)
}

func readSystemHistoryRouting(ctx context.Context, dir string, ref SegmentRef) (*systemHistoryRouting, error) {
	if err := contextError(ctx); err != nil {
		return nil, err
	}
	if ref.Kind != SegmentSystemHistoryRouting || ref.normalizedDataset() != SegmentDatasetStateDomainChange || ref.Size < systemHistoryRoutingHeaderSize || ref.Size > systemHistoryRoutingMaxBytes {
		return nil, errors.New("snapshots: invalid system history routing reference")
	}
	if err := validateSegmentRef(ref); err != nil {
		return nil, err
	}
	f, err := os.Open(filepath.Join(dir, ref.Path))
	if err != nil {
		return nil, err
	}
	defer f.Close()
	stat, err := f.Stat()
	if err != nil {
		return nil, err
	}
	if stat.Size() != int64(ref.Size) {
		return nil, errors.New("snapshots: system history routing size mismatch")
	}
	data := make([]byte, ref.Size)
	if _, err := io.ReadFull(contextReader{ctx: ctx, r: f}, data); err != nil {
		return nil, err
	}
	digest := sha256.Sum256(data)
	if fmt.Sprintf("sha256:%x", digest) != ref.Checksum {
		return nil, errors.New("snapshots: system history routing checksum mismatch")
	}
	sharedSystemHistoryRouting.Lock()
	cached := sharedSystemHistoryRouting.value
	if sharedSystemHistoryRouting.digest != digest {
		cached = nil
	}
	sharedSystemHistoryRouting.Unlock()
	if cached != nil && cached.from == ref.FromTxNum && cached.to == ref.ToTxNum && uint64(len(cached.keys)) == ref.Rows {
		return cached, nil
	}
	decoded, err := decodeSystemHistoryRouting(data, ref)
	if err != nil {
		return nil, err
	}
	charge := len(decoded.sources)*128 + len(decoded.keys)*256
	for key, members := range decoded.keys {
		charge += len(key) + len(members)
	}
	if charge <= systemHistoryRoutingMaxBytes {
		sharedSystemHistoryRouting.Lock()
		sharedSystemHistoryRouting.digest = digest
		sharedSystemHistoryRouting.value = decoded
		sharedSystemHistoryRouting.Unlock()
	}
	return decoded, nil
}

func decodeSystemHistoryRouting(data []byte, ref SegmentRef) (*systemHistoryRouting, error) {
	bad := errors.New("snapshots: invalid system history routing encoding")
	if len(data) < systemHistoryRoutingHeaderSize || len(data) > systemHistoryRoutingMaxBytes || !bytes.Equal(data[:8], systemHistoryRoutingMagic[:]) || binary.BigEndian.Uint32(data[20:24]) != 0 {
		return nil, bad
	}
	version := binary.BigEndian.Uint32(data[8:12])
	if version != 1 && version != 2 {
		return nil, bad
	}
	n, keys := uint64(binary.BigEndian.Uint32(data[12:16])), uint64(binary.BigEndian.Uint32(data[16:20]))
	width := (n + 7) / 8
	if n == 0 || n > systemHistoryRoutingMaxSources || keys > systemHistoryRoutingMaxKeys || ref.Rows != keys || binary.BigEndian.Uint64(data[24:32]) != ref.FromTxNum || binary.BigEndian.Uint64(data[32:40]) != ref.ToTxNum || uint64(len(data)) < systemHistoryRoutingHeaderSize+n*32+keys*29 {
		return nil, bad
	}
	r := &systemHistoryRouting{sources: make([][32]byte, n), columns: make(map[[32]byte]int, n), keys: make(map[string][]byte, keys), from: ref.FromTxNum, to: ref.ToTxNum}
	r.storagePrefixes = version == 2
	off := systemHistoryRoutingHeaderSize
	for i := range r.sources {
		copy(r.sources[i][:], data[off:off+32])
		off += 32
		if _, exists := r.columns[r.sources[i]]; exists {
			return nil, bad
		}
		r.columns[r.sources[i]] = i
	}
	var previous []byte
	for i := uint64(0); i < keys; i++ {
		if off+2 > len(data) {
			return nil, bad
		}
		size := int(binary.BigEndian.Uint16(data[off : off+2]))
		off += 2
		if size < 21 || size > 1024 || size+5 > len(data)-off {
			return nil, bad
		}
		key := data[off : off+size]
		off += size
		mode := data[off]
		length := uint64(binary.BigEndian.Uint32(data[off+1 : off+5]))
		off += 5
		if length == 0 || length > uint64(len(data)-off) {
			return nil, bad
		}
		payload := data[off : off+int(length)]
		off += int(length)
		switch mode {
		case 0:
			if length != width || n%8 != 0 && payload[len(payload)-1]>>uint(n%8) != 0 {
				return nil, bad
			}
			anyBit := false
			for _, b := range payload {
				anyBit = anyBit || b != 0
			}
			if !anyBit {
				return nil, bad
			}
		case 1:
			if length%4 != 0 || length >= width {
				return nil, bad
			}
			var prior uint32
			for at := 0; at < len(payload); at += 4 {
				column := binary.BigEndian.Uint32(payload[at : at+4])
				if uint64(column) >= n || at > 0 && column <= prior {
					return nil, bad
				}
				prior = column
			}
		default:
			return nil, bad
		}
		if !systemHistoryRoutingKey(key) || !r.storagePrefixes && contractStorageHistoryRoutingKey(key) || previous != nil && bytes.Compare(previous, key) >= 0 {
			return nil, bad
		}
		// Keep the one-byte tag contiguous with its payload without copying it.
		// The encoded uint32 length is between them, so own this small view header.
		members := make([]byte, 1+len(payload))
		members[0] = mode
		copy(members[1:], payload)
		r.keys[string(key)] = members
		previous = key
	}
	if off != len(data) {
		return nil, bad
	}
	return r, nil
}

func iterateHistoryRoutingSourceKeys(ctx context.Context, dir string, history, accessor SegmentRef, fn func([]byte) error) error {
	// Authenticate this exact open descriptor before using dictionary absences.
	f, err := os.Open(filepath.Join(dir, accessor.Path))
	if err != nil {
		return err
	}
	defer f.Close()
	stat, err := f.Stat()
	if err != nil {
		return err
	}
	if stat.Size() < 0 || uint64(stat.Size()) != accessor.Size {
		return errors.New("snapshots: routing source accessor size mismatch")
	}
	if stat.Size() > systemHistoryRoutingMaxAccessorBytes {
		return errSystemHistoryRoutingBudget
	}
	var version [12]byte
	if _, err := f.ReadAt(version[:], 0); err != nil {
		return err
	}
	if v := binary.BigEndian.Uint32(version[8:]); !isStateDomainChangeKeyAccessor(v) {
		return errSystemHistoryRoutingBudget
	}
	hash := sha256.New()
	if _, err = io.Copy(hash, contextReader{ctx: ctx, r: f}); err != nil {
		return err
	}
	if fmt.Sprintf("sha256:%x", hash.Sum(nil)) != accessor.Checksum {
		return errors.New("snapshots: routing source accessor checksum mismatch")
	}
	r := contextReaderAt{ctx: ctx, r: f}
	size := uint64(stat.Size())
	h, err := decodeStateDomainChangeBinaryAccessorKeyHeader(r, size)
	if err != nil {
		return err
	}
	if h.fromTxNum != history.FromTxNum || h.toTxNum != history.ToTxNum {
		return errors.New("snapshots: routing source accessor range mismatch")
	}
	segment, header, _, err := openStateDomainChangeBinarySegmentSequentialReader(dir, history)
	if err != nil {
		return err
	}
	if header.version != stateDomainChangeBinaryVersionV6 {
		segment.Close()
		return errSystemHistoryRoutingBudget
	}
	if header.count != h.recordCount {
		segment.Close()
		return errors.New("snapshots: routing source record count mismatch")
	}
	err = verifyStateDomainChangeBinaryV6DictionaryPair(segment, r, size)
	closeErr := segment.Close()
	if err != nil {
		return err
	}
	if closeErr != nil {
		return closeErr
	}
	_, err = iterateStateDomainChangeBinaryAccessorV6Keys(r, size, func(_ uint32, key []byte) error { return fn(key) })
	if err != nil {
		return err
	}
	return contextError(ctx)
}

func systemHistorySourceKeys(ctx context.Context, dir string, history, accessor SegmentRef) (keys []string, err error) {
	return systemHistorySourceKeysWithStorage(ctx, dir, history, accessor, true)
}

func systemHistorySourceKeysWithStorage(ctx context.Context, dir string, history, accessor SegmentRef, storage bool) (keys []string, err error) {
	keyBytes := 0
	seen := make(map[string]bool)
	err = iterateHistoryRoutingSourceKeys(ctx, dir, history, accessor, func(key []byte) error {
		if contractStorageHistoryRoutingKey(key) {
			if !storage {
				return nil
			}
			key = normalizedHistoryRoutingKey(key)
		}
		if !systemHistoryRoutingKey(key) {
			return nil
		}
		if seen[string(key)] {
			return nil
		}
		keyBytes += len(key) + 96 // own the dedup map, key bytes and result slice
		if len(key) > 1024 || len(keys) >= systemHistoryRoutingMaxKeys || keyBytes > systemHistoryRoutingSourceKeyBytes {
			return errSystemHistoryRoutingBudget
		}
		keys = append(keys, string(key))
		seen[string(key)] = true
		return nil
	})
	if err != nil {
		return nil, err
	}
	sort.Strings(keys)
	return keys, contextError(ctx)
}

// BuildSystemHistoryRoutingContext creates at most one bounded optional index.
// It does not publish or retire anything. The caller owns a pinned manifest and
// the normal build/publication transaction. Unsupported inputs defer safely.
func BuildSystemHistoryRoutingContext(ctx context.Context, dir string, manifest *Manifest, maxNew int) (ref SegmentRef, built bool, err error) {
	if manifest == nil || maxNew < 1 {
		return ref, false, nil
	}
	maxNew = min(maxNew, systemHistoryRoutingMaxNewSources)
	old, err := loadSystemHistoryRouting(ctx, dir, manifest)
	if err != nil {
		return ref, false, err
	}
	if old != nil && !old.storagePrefixes {
		// Reauthenticate dictionaries before making the broader V2 claim.
		// Uncovered pairs retain ordinary authoritative reads during rebuild.
		old = nil
	}
	manifest = manifestLookupView(manifest)
	type pair struct {
		history, accessor SegmentRef
		id                [32]byte
		oldColumn         int
		keys              []string
	}
	var pairs []pair
	newSources, attempted, collectedBytes := 0, 0, 0
	cfg, _ := DefaultDomainRegistry().Dataset(SegmentDatasetStateDomainChange)
	for _, history := range manifest.Segments {
		if history.Kind != SegmentHistory || history.normalizedDataset() != SegmentDatasetStateDomainChange || !isStateDomainChangeBinarySegmentPath(history.Path) {
			continue
		}
		accessor, ok := cfg.HistoryAccessorRef(manifest, history)
		if !ok {
			return ref, false, errors.New("snapshots: routing source missing accessor")
		}
		id := systemHistoryRoutingPairID(history, accessor)
		column := -1
		if old != nil {
			if pos, ok := old.columns[id]; ok {
				column = pos
			}
		}
		if column < 0 && attempted >= maxNew {
			continue
		}
		p := pair{history: history, accessor: accessor, id: id, oldColumn: column}
		if column < 0 {
			attempted++
			p.keys, err = systemHistorySourceKeys(ctx, dir, history, accessor)
			if errors.Is(err, errSystemHistoryRoutingBudget) {
				continue
			}
			if err != nil {
				return ref, false, err
			}
			bytes := 0
			for _, key := range p.keys {
				bytes += len(key) + 32
			}
			if bytes > systemHistoryRoutingBuildKeyBytes-collectedBytes {
				attempted = maxNew // Keep existing coverage, defer further new inputs.
				continue
			}
			collectedBytes += bytes
			newSources++
		}
		pairs = append(pairs, p)
		if len(pairs) > systemHistoryRoutingMaxSources {
			return ref, false, nil
		}
	}
	if newSources == 0 {
		return ref, false, nil
	}
	width := (len(pairs) + 7) / 8
	rows := make(map[string][]uint32)
	estimated := systemHistoryRoutingHeaderSize + len(pairs)*32
	add := func(key string, column int) error {
		columns := rows[key]
		if len(columns) == 0 {
			if len(rows) >= systemHistoryRoutingMaxKeys {
				return errSystemHistoryRoutingBudget
			}
			estimated += 2 + len(key) + 5
		}
		if len(columns) > 0 && columns[len(columns)-1] == uint32(column) {
			return nil
		}
		before := min(len(columns)*4, width)
		after := min((len(columns)+1)*4, width)
		estimated += after - before
		if estimated > systemHistoryRoutingMaxBytes {
			return errSystemHistoryRoutingBudget
		}
		rows[key] = append(columns, uint32(column))
		return nil
	}
	oldToNew := make(map[int]int)
	for column, p := range pairs {
		if err := contextError(ctx); err != nil {
			return ref, false, err
		}
		if p.oldColumn >= 0 {
			oldToNew[p.oldColumn] = column
			continue
		}
		for _, key := range p.keys {
			if err := add(key, column); err != nil {
				return ref, false, nil
			}
		}
	}
	if old != nil {
		for key, members := range old.keys {
			var columns []int
			systemHistoryMembersIterate(members, func(oldColumn int) {
				if column, ok := oldToNew[oldColumn]; ok {
					columns = append(columns, column)
				}
			})
			sort.Ints(columns)
			for _, column := range columns {
				if err := add(key, column); err != nil {
					return ref, false, nil
				}
			}
		}
	}
	// New-source columns may precede reused columns after a range replacement.
	for key, columns := range rows {
		sort.Slice(columns, func(i, j int) bool { return columns[i] < columns[j] })
		rows[key] = columns
	}

	keys := make([]string, 0, len(rows))
	for key := range rows {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	from, to := pairs[0].history.FromTxNum, pairs[0].history.ToTxNum
	for _, p := range pairs {
		from = min(from, p.history.FromTxNum)
		to = max(to, p.history.ToTxNum)
	}
	data := make([]byte, systemHistoryRoutingHeaderSize)
	copy(data, systemHistoryRoutingMagic[:])
	binary.BigEndian.PutUint32(data[8:12], 2)
	binary.BigEndian.PutUint32(data[12:16], uint32(len(pairs)))
	binary.BigEndian.PutUint32(data[16:20], uint32(len(keys)))
	binary.BigEndian.PutUint64(data[24:32], from)
	binary.BigEndian.PutUint64(data[32:40], to)
	for _, p := range pairs {
		data = append(data, p.id[:]...)
	}
	for _, key := range keys {
		data = binary.BigEndian.AppendUint16(data, uint16(len(key)))
		data = append(data, key...)
		columns := rows[key]
		if len(columns)*4 < width {
			data = append(data, 1)
			data = binary.BigEndian.AppendUint32(data, uint32(len(columns)*4))
			for _, column := range columns {
				data = binary.BigEndian.AppendUint32(data, column)
			}
		} else {
			data = append(data, 0)
			data = binary.BigEndian.AppendUint32(data, uint32(width))
			begin := len(data)
			data = append(data, make([]byte, width)...)
			for _, column := range columns {
				data[begin+int(column)/8] |= 1 << uint(column%8)
			}
		}
	}
	sum := sha256.Sum256(data)
	ref = SegmentRef{Dataset: SegmentDatasetStateDomainChange, Kind: SegmentSystemHistoryRouting, FromTxNum: from, ToTxNum: to, Rows: uint64(len(keys)), Size: uint64(len(data)), Checksum: fmt.Sprintf("sha256:%x", sum)}
	ref.Path = fmt.Sprintf("history/system-routing-%d-%d-%x.route", from, to, sum)
	if _, err := decodeSystemHistoryRouting(data, ref); err != nil {
		return SegmentRef{}, false, err
	}
	if err := contextError(ctx); err != nil {
		return SegmentRef{}, false, err
	}
	if err := os.MkdirAll(filepath.Join(dir, "history"), 0755); err != nil {
		return SegmentRef{}, false, err
	}
	f, err := os.CreateTemp(filepath.Join(dir, "history"), "system-routing-*.tmp")
	if err != nil {
		return SegmentRef{}, false, err
	}
	name := f.Name()
	defer os.Remove(name)
	if _, err = f.Write(data); err == nil {
		err = f.Sync()
	}
	err = errors.Join(err, f.Close())
	if err != nil {
		return SegmentRef{}, false, err
	}
	if err := contextError(ctx); err != nil {
		return SegmentRef{}, false, err
	}
	if err := os.Rename(name, filepath.Join(dir, ref.Path)); err != nil {
		return SegmentRef{}, false, err
	}
	if err := syncSnapshotDir(filepath.Join(dir, "history")); err != nil {
		return SegmentRef{}, false, err
	}
	return ref, true, nil
}

// Snapshot import checks the absence index against the covered authoritative
// dictionaries as well as authenticating the index object itself.
func CheckSystemHistoryRouting(dir string, ref SegmentRef) error {
	manifest, err := LoadProductionManifest(dir)
	if err != nil {
		return err
	}
	return checkSystemHistoryRoutingContext(context.Background(), dir, manifest, ref)
}

func checkSystemHistoryRoutingContext(ctx context.Context, dir string, manifest *Manifest, ref SegmentRef) error {
	route, err := readSystemHistoryRouting(ctx, dir, ref)
	if err != nil {
		return err
	}
	manifest = manifestLookupView(manifest)
	counts := make([]int, len(route.sources))
	for _, members := range route.keys {
		systemHistoryMembersIterate(members, func(column int) { counts[column]++ })
	}
	cfg, _ := DefaultDomainRegistry().Dataset(SegmentDatasetStateDomainChange)
	for _, history := range manifest.Segments {
		if history.Kind != SegmentHistory || history.normalizedDataset() != SegmentDatasetStateDomainChange {
			continue
		}
		accessor, ok := cfg.HistoryAccessorRef(manifest, history)
		if !ok {
			continue
		}
		column, covered := route.columns[systemHistoryRoutingPairID(history, accessor)]
		if !covered {
			continue
		}
		keys, err := systemHistorySourceKeysWithStorage(ctx, dir, history, accessor, route.storagePrefixes)
		if err != nil {
			return err
		}
		if len(keys) != counts[column] {
			return errors.New("snapshots: system history routing membership count mismatch")
		}
		for _, key := range keys {
			if !route.mayContain(column, []byte(key)) {
				return errors.New("snapshots: system history routing omitted a covered key")
			}
		}
	}
	return nil
}

func refreshSystemHistoryRoutingContext(ctx context.Context, dir string, busy bool) (bool, error) {
	if !historyReadLeaseGate.TryRLock() {
		return false, nil
	}
	defer historyReadLeaseGate.RUnlock()
	manifest, err := loadProductionManifestReadOnly(dir)
	if os.IsNotExist(err) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	count, indexed := 0, false
	for _, ref := range manifest.Segments {
		if ref.Kind == SegmentSystemHistoryRouting {
			indexed = true
		}
		if ref.Kind == SegmentHistory && ref.normalizedDataset() == SegmentDatasetStateDomainChange {
			count++
		}
	}
	// Small catalogs keep the original path. Once installed, finish their tail
	// when idle; busy refreshes wait for a full bounded batch of new sources.
	if count < systemHistoryRoutingMaxNewSources && !indexed {
		return false, nil
	}
	old, err := loadSystemHistoryRouting(ctx, dir, manifest)
	if err != nil {
		return false, err
	}
	manifest = manifestLookupView(manifest)
	cfg, _ := DefaultDomainRegistry().Dataset(SegmentDatasetStateDomainChange)
	uncovered := 0
	for _, history := range manifest.Segments {
		if history.Kind != SegmentHistory || history.normalizedDataset() != SegmentDatasetStateDomainChange {
			continue
		}
		accessor, ok := cfg.HistoryAccessorRef(manifest, history)
		if !ok {
			continue
		}
		if old == nil {
			uncovered++
		} else if _, ok := old.columns[systemHistoryRoutingPairID(history, accessor)]; !ok {
			uncovered++
		}
	}
	if uncovered == 0 || busy && uncovered < systemHistoryRoutingMaxNewSources {
		return false, nil
	}
	ref, built, err := BuildSystemHistoryRoutingContext(ctx, dir, manifest, systemHistoryRoutingMaxNewSources)
	if err != nil || !built {
		return false, err
	}
	if err := contextError(ctx); err != nil {
		return false, err
	}
	_, err = NewAggregator(dir).Integrate(manifest.VisibleTxStart, manifest.VisibleTxEnd, []SegmentRef{ref})
	return err == nil, err
}
