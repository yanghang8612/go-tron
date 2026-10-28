package snapshots

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/state/kvdomains"
)

const smallSystemHistoryRoutingMaxKeys = 4096

// Working-set charge for accumulated memberships, not a peak RSS limit.
const smallSystemHistoryRoutingBuildBytes = 64 << 20

// Only an attempt-order hint. Never used as evidence of source coverage.
var smallSystemRoutingRefreshCursor struct {
	sync.Mutex
	dir   string
	start int
}

var smallSystemHistoryRoutingMagic = [8]byte{'G', 'T', 'S', 'M', 'L', 'R', '0', '1'}
var sharedSmallSystemHistoryRouting struct {
	sync.Mutex
	digest [32]byte
	value  *systemHistoryRouting
}

func smallSystemHistoryRoutingKey(key []byte) bool {
	if len(key) == 21 && (key[0] == byte(rawdb.StateFlatDomainAccountLatest) || key[0] == byte(rawdb.StateFlatDomainKVGeneration)) {
		return bytes.Equal(key[1:], common.SystemAccountID[:]) || bytes.Equal(key[1:], make([]byte, 20))
	}
	if len(key) < 31 || key[0] != byte(rawdb.StateFlatDomainKVLatest) || !bytes.Equal(key[1:21], common.SystemAccountID[:]) {
		return false
	}
	domain := kvdomains.KVDomain(binary.BigEndian.Uint16(key[29:31]))
	return domain == kvdomains.SystemDynamicProperty || domain == kvdomains.SystemForkVote
}
func smallSystemHistoryRoutingQuery(q StateHistoryQuery) bool {
	if q.FlatDomain == rawdb.StateFlatDomainAccountLatest || q.FlatDomain == rawdb.StateFlatDomainKVGeneration {
		return common.IsSystemAccount(q.Owner) || q.Owner.AccountID() == (common.AccountID{})
	}
	return q.FlatDomain == rawdb.StateFlatDomainKVLatest && q.Owner == common.SystemAccountAddress && (q.Domain == kvdomains.SystemDynamicProperty || q.Domain == kvdomains.SystemForkVote)
}

func smallSystemHistorySourceKeys(ctx context.Context, dir string, h, a SegmentRef) ([]string, error) {
	return collectSmallSystemHistorySourceKeys(ctx, dir, h, a, false)
}

func collectSmallSystemHistorySourceKeys(ctx context.Context, dir string, h, a SegmentRef, selected bool) ([]string, error) {
	seen := make(map[string]bool)
	keyBytes := 0
	visit := func(key []byte) error {
		if err := contextError(ctx); err != nil {
			return err
		}
		if !smallSystemHistoryRoutingKey(key) {
			return nil
		}
		if len(key) > 1024 {
			return errSystemHistoryRoutingBudget
		}
		s := string(key)
		if seen[s] {
			return nil
		}
		if len(seen) >= smallSystemHistoryRoutingMaxKeys || keyBytes+len(key)+96 > systemHistoryRoutingSourceKeyBytes {
			return errSystemHistoryRoutingBudget
		}
		seen[s] = true
		keyBytes += len(key) + 96
		return nil
	}
	var err error
	if selected {
		err = withAuthenticatedHistoryRoutingDictionary(ctx, dir, h, a, func(r io.ReaderAt, size uint64, header stateDomainChangeBinaryAccessorV6Header) error {
			return iterateSmallSystemRoutingDictionaryKeys(ctx, r, size, header, visit)
		})
	} else {
		// Strict import keeps the independent full dictionary walk, including
		// its ordering/commitment proof. It must not validate a selective
		// builder merely by repeating that builder's searches.
		err = iterateHistoryRoutingSourceKeys(ctx, dir, h, a, visit)
	}
	if err != nil {
		return nil, err
	}
	keys := make([]string, 0, len(seen))
	for key := range seen {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys, nil
}

func loadSmallSystemHistoryRouting(ctx context.Context, dir string, manifest *Manifest, window ...TxRange) (*systemHistoryRouting, error) {
	var found *SegmentRef
	for i := range manifest.Segments {
		if manifest.Segments[i].Kind == SegmentSmallSystemHistoryRouting {
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
	return readSmallSystemHistoryRouting(ctx, dir, *found)
}

func readSmallSystemHistoryRouting(ctx context.Context, dir string, ref SegmentRef) (*systemHistoryRouting, error) {
	if err := contextError(ctx); err != nil {
		return nil, err
	}
	if ref.Kind != SegmentSmallSystemHistoryRouting || ref.normalizedDataset() != SegmentDatasetStateDomainChange || ref.Size < systemHistoryRoutingHeaderSize || ref.Size > systemHistoryRoutingMaxBytes {
		return nil, errors.New("snapshots: invalid system history routing reference")
	}
	if err := validateSegmentRef(ref); err != nil {
		return nil, err
	}
	data, digest, err := readSystemHistoryRoutingBytes(ctx, dir, ref)
	if err != nil {
		return nil, err
	}
	sharedSmallSystemHistoryRouting.Lock()
	cached := sharedSmallSystemHistoryRouting.value
	if sharedSmallSystemHistoryRouting.digest != digest {
		cached = nil
	}
	sharedSmallSystemHistoryRouting.Unlock()
	if cached != nil && cached.from == ref.FromTxNum && cached.to == ref.ToTxNum && uint64(len(cached.keys)) == ref.Rows {
		return cached, nil
	}
	decoded, err := decodeHistoryRoutingFamily(ctx, data, ref, true)
	if err != nil {
		return nil, err
	}
	charge := len(decoded.sources)*128 + len(decoded.keys)*256
	for key, members := range decoded.keys {
		charge += len(key) + len(members)
	}
	if charge <= systemHistoryRoutingMaxBytes {
		sharedSmallSystemHistoryRouting.Lock()
		sharedSmallSystemHistoryRouting.digest = digest
		sharedSmallSystemHistoryRouting.value = decoded
		sharedSmallSystemHistoryRouting.Unlock()
	}
	return decoded, nil
}

// BuildSmallSystemHistoryRoutingContext authenticates up to maxNew uncovered
// source pairs in one pass. Maintenance passes 64; an offline build can cover
// the whole fixed catalog without repeatedly rewriting intermediate objects.
func BuildSmallSystemHistoryRoutingContext(ctx context.Context, dir string, manifest *Manifest, maxNew int) (SegmentRef, bool, error) {
	return buildSmallSystemHistoryRoutingContext(ctx, dir, manifest, maxNew, 0, nil)
}

type smallSystemRoutingWorkLimit struct {
	bytes    uint64
	duration time.Duration
}

func buildSmallSystemHistoryRoutingContext(ctx context.Context, dir string, manifest *Manifest, maxNew, start int, next *int) (ref SegmentRef, built bool, err error) {
	return buildSmallSystemHistoryRoutingWithBudget(ctx, dir, manifest, maxNew, start, next, smallSystemRoutingWorkLimit{})
}

func buildSmallSystemHistoryRoutingWithBudget(ctx context.Context, dir string, manifest *Manifest, maxNew, start int, next *int, work smallSystemRoutingWorkLimit) (ref SegmentRef, built bool, err error) {
	if manifest == nil || maxNew < 1 {
		return ref, false, nil
	}
	if err = contextError(ctx); err != nil {
		return ref, false, err
	}
	maxNew = min(maxNew, systemHistoryRoutingMaxSources)
	old, err := loadSmallSystemHistoryRouting(ctx, dir, manifest)
	if err != nil {
		return ref, false, err
	}
	manifest = manifestLookupView(manifest)
	// Reuse complete exact-pair proofs from the old authenticated broad route.
	// Range is only a cheap admission hint; membership authority uses pairID.
	var bootstrap *historyRoutingBootstrap
	if historyRoutingBootstrapNeeded(manifest, old) {
		bootstrap, err = loadHistoryRoutingBootstrap(ctx, dir, manifest)
		if err != nil {
			return ref, false, err
		}
	}
	type pair struct {
		from, to uint64
		id       [32]byte
	}
	var pairs []pair
	rows := make(map[string][]uint32)
	oldToNew := make(map[int]int)
	seedToNew := make(map[int]int)
	// Charge source identities/maps, key maps and append capacity conservatively.
	charged, newSources, attempted := 0, 0, 0
	var readBytes uint64
	started := time.Now()
	workExhausted := false
	add := func(key string, column int) error {
		cols := rows[key]
		if len(cols) > 0 && cols[len(cols)-1] == uint32(column) {
			return nil
		}
		extra := 8
		if len(cols) == 0 {
			extra += len(key) + 256
			if len(rows) >= smallSystemHistoryRoutingMaxKeys {
				return errSystemHistoryRoutingBudget
			}
		}
		if charged+extra > smallSystemHistoryRoutingBuildBytes {
			return errSystemHistoryRoutingBudget
		}
		charged += extra
		rows[key] = append(cols, uint32(column))
		return nil
	}
	cfg, _ := DefaultDomainRegistry().Dataset(SegmentDatasetStateDomainChange)
	for offset := range manifest.Segments {
		index := (start + offset) % len(manifest.Segments)
		h := manifest.Segments[index]
		if err = contextError(ctx); err != nil {
			return ref, false, err
		}
		if h.Kind != SegmentHistory || h.normalizedDataset() != SegmentDatasetStateDomainChange || !isStateDomainChangeBinarySegmentPath(h.Path) {
			continue
		}
		a, ok := cfg.HistoryAccessorRef(manifest, h)
		if !ok {
			return ref, false, errors.New("snapshots: small routing source missing accessor")
		}
		id := systemHistoryRoutingPairID(h, a)
		oldColumn, covered := 0, false
		if old != nil {
			oldColumn, covered = old.columns[id]
		}
		seedColumn, seedCovered := 0, false
		if !covered && bootstrap != nil {
			seedColumn, seedCovered = bootstrap.columns[id]
		}
		var keys []string
		if !covered && !seedCovered {
			if attempted > 0 && (work.bytes > 0 && (readBytes >= work.bytes || a.Size > work.bytes-readBytes) || work.duration > 0 && time.Since(started) >= work.duration) {
				workExhausted = true
			}
			if workExhausted || attempted >= maxNew {
				continue
			}
			attempted++
			readBytes += a.Size
			if next != nil {
				*next = (index + 1) % len(manifest.Segments)
			}
			keys, err = collectSmallSystemHistorySourceKeys(ctx, dir, h, a, true)
			if errors.Is(err, errSystemHistoryRoutingBudget) {
				continue
			}
			if err != nil {
				return ref, false, err
			}
		}
		if len(pairs) >= systemHistoryRoutingMaxSources || charged+192 > smallSystemHistoryRoutingBuildBytes {
			return ref, false, nil
		}
		charged += 192
		column := len(pairs)
		pairs = append(pairs, pair{h.FromTxNum, h.ToTxNum, id})
		if covered {
			oldToNew[oldColumn] = column
			continue
		}
		if seedCovered {
			seedToNew[seedColumn] = column
			newSources++
			continue
		}
		for _, key := range keys {
			if err = add(key, column); err != nil {
				return ref, false, nil
			}
		}
		newSources++ // Empty-family dictionaries still attest absence.
	}
	if newSources == 0 {
		return ref, false, nil
	}
	if bootstrap != nil {
		for key, members := range bootstrap.small {
			if err = contextError(ctx); err != nil {
				return ref, false, err
			}
			var addErr error
			systemHistoryMembersIterate(members, func(prior int) {
				if addErr == nil {
					if column, ok := seedToNew[prior]; ok {
						addErr = add(key, column)
					}
				}
			})
			if addErr != nil {
				if errors.Is(addErr, errSystemHistoryRoutingBudget) {
					return ref, false, nil
				}
				return ref, false, addErr
			}
		}
	}
	if old != nil {
		for key, members := range old.keys {
			if err = contextError(ctx); err != nil {
				return ref, false, err
			}
			var addErr error
			systemHistoryMembersIterate(members, func(prior int) {
				if prior%256 == 0 && addErr == nil {
					addErr = contextError(ctx)
				}
				if addErr != nil {
					return
				}
				if column, ok := oldToNew[prior]; ok {
					addErr = add(key, column)
				}
			})
			if addErr != nil {
				if !errors.Is(addErr, errSystemHistoryRoutingBudget) {
					return ref, false, addErr
				}
				return ref, false, nil
			}
		}
	}
	keys := make([]string, 0, len(rows))
	for key, cols := range rows {
		if err = contextError(ctx); err != nil {
			return ref, false, err
		}
		sort.Slice(cols, func(i, j int) bool { return cols[i] < cols[j] })
		rows[key] = cols
		keys = append(keys, key)
	}
	sort.Strings(keys)
	from, to := pairs[0].from, pairs[0].to
	for _, p := range pairs {
		from = min(from, p.from)
		to = max(to, p.to)
	}
	width := (len(pairs) + 7) / 8
	data := make([]byte, systemHistoryRoutingHeaderSize)
	copy(data, smallSystemHistoryRoutingMagic[:])
	binary.BigEndian.PutUint32(data[8:12], 1)
	binary.BigEndian.PutUint32(data[12:16], uint32(len(pairs)))
	binary.BigEndian.PutUint32(data[16:20], uint32(len(keys)))
	binary.BigEndian.PutUint64(data[24:32], from)
	binary.BigEndian.PutUint64(data[32:40], to)
	for _, p := range pairs {
		data = append(data, p.id[:]...)
	}
	for _, key := range keys {
		if err = contextError(ctx); err != nil {
			return ref, false, err
		}
		data = binary.BigEndian.AppendUint16(data, uint16(len(key)))
		data = append(data, key...)
		cols := rows[key]
		if len(cols)*4 < width {
			data = append(data, 1)
			data = binary.BigEndian.AppendUint32(data, uint32(len(cols)*4))
			for _, c := range cols {
				data = binary.BigEndian.AppendUint32(data, c)
			}
		} else {
			data = append(data, 0)
			data = binary.BigEndian.AppendUint32(data, uint32(width))
			begin := len(data)
			data = append(data, make([]byte, width)...)
			for _, c := range cols {
				data[begin+int(c)/8] |= 1 << uint(c%8)
			}
		}
		if len(data) > systemHistoryRoutingMaxBytes {
			return ref, false, nil
		}
	}
	sum := sha256.Sum256(data)
	ref = SegmentRef{Dataset: SegmentDatasetStateDomainChange, Kind: SegmentSmallSystemHistoryRouting, FromTxNum: from, ToTxNum: to, Rows: uint64(len(keys)), Size: uint64(len(data)), Checksum: fmt.Sprintf("sha256:%x", sum)}
	ref.Path = fmt.Sprintf("history/small-system-routing-%d-%d-%x.route", from, to, sum)
	if _, err := decodeHistoryRoutingFamily(ctx, data, ref, true); err != nil {
		return SegmentRef{}, false, err
	}
	if err := os.MkdirAll(filepath.Join(dir, "history"), 0755); err != nil {
		return SegmentRef{}, false, err
	}
	f, err := os.CreateTemp(filepath.Join(dir, "history"), "small-system-routing-*.tmp")
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

func CheckSmallSystemHistoryRouting(dir string, ref SegmentRef) error {
	manifest, err := LoadProductionManifest(dir)
	if err != nil {
		return err
	}
	return checkSmallSystemHistoryRoutingContext(context.Background(), dir, manifest, ref)
}

func checkSmallSystemHistoryRoutingContext(ctx context.Context, dir string, manifest *Manifest, ref SegmentRef) error {
	route, err := readSmallSystemHistoryRouting(ctx, dir, ref)
	if err != nil {
		return err
	}
	manifest = manifestLookupView(manifest)
	counts := make([]int, len(route.sources))
	for _, members := range route.keys {
		if err := contextError(ctx); err != nil {
			return err
		}
		var canceled error
		systemHistoryMembersIterate(members, func(column int) {
			if column%256 == 0 && canceled == nil {
				canceled = contextError(ctx)
			}
			counts[column]++
		})
		if canceled != nil {
			return canceled
		}
	}
	cfg, _ := DefaultDomainRegistry().Dataset(SegmentDatasetStateDomainChange)
	for _, history := range manifest.Segments {
		if err := contextError(ctx); err != nil {
			return err
		}
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
		keys, err := smallSystemHistorySourceKeys(ctx, dir, history, accessor)
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
	return contextError(ctx)
}

func refreshSmallSystemHistoryRoutingContext(ctx context.Context, dir string, busy bool) (bool, error) {
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
		if ref.Kind == SegmentSmallSystemHistoryRouting {
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
	old, err := loadSmallSystemHistoryRouting(ctx, dir, manifest)
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
	smallSystemRoutingRefreshCursor.Lock()
	start := 0
	if smallSystemRoutingRefreshCursor.dir == dir {
		start = smallSystemRoutingRefreshCursor.start
	}
	smallSystemRoutingRefreshCursor.Unlock()
	next := start
	ref, built, err := buildSmallSystemHistoryRoutingWithBudget(ctx, dir, manifest, 256, start, &next, smallSystemRoutingWorkLimit{bytes: 128 << 20, duration: 2 * time.Second})
	if err == nil {
		smallSystemRoutingRefreshCursor.Lock()
		smallSystemRoutingRefreshCursor.dir, smallSystemRoutingRefreshCursor.start = dir, next
		smallSystemRoutingRefreshCursor.Unlock()
	}
	if err != nil || !built {
		return false, err
	}
	if err := contextError(ctx); err != nil {
		return false, err
	}
	_, err = NewAggregator(dir).Integrate(manifest.VisibleTxStart, manifest.VisibleTxEnd, []SegmentRef{ref})
	return err == nil, err
}
