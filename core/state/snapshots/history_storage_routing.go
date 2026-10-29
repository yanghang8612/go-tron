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
	"time"
)

const storageRoutingPageSources = 256
const storageRoutingPageBytes = 1 << 20
const storageRoutingSourceHashes = 1 << 16
const storageRoutingProbes = 8

var storageRoutingMagic = [8]byte{'G', 'T', 'S', 'T', 'O', 'R', '0', '1'}

type storagePrefixHash struct{ first, step uint64 }
type storageSourceFilter struct {
	count uint32
	bits  []byte
}

func hashStoragePrefix(key []byte) storagePrefixHash {
	h := sha256.Sum256(normalizedHistoryRoutingKey(key))
	return storagePrefixHash{binary.BigEndian.Uint64(h[:8]), binary.BigEndian.Uint64(h[8:16]) | 1}
}

func storageFilterBytes(count uint32) int {
	if count == 0 {
		return 0
	}
	return 1 << bits.Len(uint(max(64, int(count)*2)-1))
}

func storageFilterFromHashes(hashes []storagePrefixHash) storageSourceFilter {
	f := storageSourceFilter{count: uint32(len(hashes)), bits: make([]byte, storageFilterBytes(uint32(len(hashes))))}
	for _, h := range hashes {
		for i := uint64(0); i < storageRoutingProbes; i++ {
			bit := (h.first + i*h.step) & uint64(len(f.bits)*8-1)
			f.bits[bit/8] |= 1 << (bit % 8)
		}
	}
	return f
}

func (f storageSourceFilter) mayContain(key []byte) bool {
	if !contractStorageHistoryRoutingKey(key) {
		return true
	}
	if f.count == 0 {
		return false
	}
	h := hashStoragePrefix(key)
	for i := uint64(0); i < storageRoutingProbes; i++ {
		bit := (h.first + i*h.step) & uint64(len(f.bits)*8-1)
		if f.bits[bit/8]&(1<<(bit%8)) == 0 {
			return false
		}
	}
	return true
}

func storageHistorySourceFilter(ctx context.Context, dir string, history, accessor SegmentRef) (storageSourceFilter, error) {
	var hashes []storagePrefixHash
	var prior [31]byte
	havePrior := false
	err := iterateHistoryRoutingSourceKeys(ctx, dir, history, accessor, func(key []byte) error {
		if err := contextError(ctx); err != nil {
			return err
		}
		if !contractStorageHistoryRoutingKey(key) || havePrior && bytes.Equal(prior[:], key[:31]) {
			return nil
		}
		if len(hashes) == storageRoutingSourceHashes {
			return errSystemHistoryRoutingBudget
		}
		copy(prior[:], key[:31])
		havePrior = true
		hashes = append(hashes, hashStoragePrefix(key))
		return nil
	})
	if err != nil {
		return storageSourceFilter{}, err // Never return an incomplete filter.
	}
	return storageFilterFromHashes(hashes), nil
}

func storageRoutingSize(rows map[[32]byte]storageSourceFilter) int {
	n := 32 + 40*len(rows)
	for _, f := range rows {
		n += len(f.bits)
	}
	return n
}

func readStorageHistoryRouting(ctx context.Context, dir string, ref SegmentRef) (map[[32]byte]storageSourceFilter, error) {
	if ref.Kind != SegmentStorageHistoryRouting || ref.normalizedDataset() != SegmentDatasetStateDomainChange || ref.Rows == 0 || ref.Rows > storageRoutingPageSources || ref.Size < 32 || ref.Size > storageRoutingPageBytes {
		return nil, errors.New("snapshots: invalid storage routing reference")
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
	if err != nil || uint64(stat.Size()) != ref.Size {
		return nil, errors.Join(errors.New("snapshots: storage routing size mismatch"), err)
	}
	data := make([]byte, ref.Size)
	if _, err = io.ReadFull(contextReader{ctx: ctx, r: f}, data); err != nil {
		return nil, err
	}
	sum := sha256.Sum256(data)
	if fmt.Sprintf("sha256:%x", sum) != ref.Checksum || !bytes.Equal(data[:8], storageRoutingMagic[:]) || binary.BigEndian.Uint32(data[8:12]) != 1 || binary.BigEndian.Uint32(data[12:16]) != uint32(ref.Rows) || binary.BigEndian.Uint64(data[16:24]) != ref.FromTxNum || binary.BigEndian.Uint64(data[24:32]) != ref.ToTxNum {
		return nil, errors.New("snapshots: storage routing authentication/header mismatch")
	}
	rows := make(map[[32]byte]storageSourceFilter, ref.Rows)
	pos := 32
	var prior [32]byte
	for i := uint64(0); i < ref.Rows; i++ {
		if err := contextError(ctx); err != nil {
			return nil, err
		}
		if len(data)-pos < 40 {
			return nil, io.ErrUnexpectedEOF
		}
		var id [32]byte
		copy(id[:], data[pos:pos+32])
		count, size := binary.BigEndian.Uint32(data[pos+32:pos+36]), binary.BigEndian.Uint32(data[pos+36:pos+40])
		pos += 40
		if i > 0 && bytes.Compare(prior[:], id[:]) >= 0 || count > storageRoutingSourceHashes || size != uint32(storageFilterBytes(count)) || uint64(size) > uint64(len(data)-pos) {
			return nil, errors.New("snapshots: invalid storage routing row")
		}
		rows[id] = storageSourceFilter{count, data[pos : pos+int(size)]}
		pos += int(size)
		prior = id
	}
	if pos != len(data) {
		return nil, errors.New("snapshots: trailing storage routing bytes")
	}
	return rows, nil
}

// One request-local page, loaded only when a canonical read is about to be
// submitted. Failed optional pages are unknown, including for all their peers.
type storageHistoryRoutingReader struct {
	refs   []SegmentRef
	loaded string
	rows   map[[32]byte]storageSourceFilter
	load   func(context.Context, string, SegmentRef) (map[[32]byte]storageSourceFilter, error)
}

func newStorageHistoryRoutingReader(m *Manifest) *storageHistoryRoutingReader {
	r := &storageHistoryRoutingReader{load: readStorageHistoryRouting}
	for _, ref := range m.Segments {
		if ref.Kind == SegmentStorageHistoryRouting {
			r.refs = append(r.refs, ref)
		}
	}
	sort.Slice(r.refs, func(i, j int) bool { return r.refs[i].FromTxNum < r.refs[j].FromTxNum })
	return r
}

func (r *storageHistoryRoutingReader) source(ctx context.Context, dir string, history SegmentRef, id [32]byte) (storageSourceFilter, bool, error) {
	if err := contextError(ctx); err != nil {
		return storageSourceFilter{}, false, err
	}
	i := sort.Search(len(r.refs), func(i int) bool { return r.refs[i].ToTxNum >= history.FromTxNum })
	if i == len(r.refs) || r.refs[i].FromTxNum > history.FromTxNum || r.refs[i].ToTxNum < history.ToTxNum {
		return storageSourceFilter{}, false, nil
	}
	ref := r.refs[i]
	if r.loaded != ref.Path {
		rows, err := r.load(ctx, dir, ref)
		r.loaded, r.rows = ref.Path, rows
		if err != nil {
			// Optional hints never expose a speculative later-source error.
			// The ordinary authenticated accessor remains the authority.
			return storageSourceFilter{}, false, contextError(ctx)
		}
	}
	f, covered := r.rows[id]
	return f, covered, nil
}

func writeStorageHistoryRoutingPage(ctx context.Context, dir string, from, to uint64, rows map[[32]byte]storageSourceFilter) (SegmentRef, bool, error) {
	if from > to || len(rows) == 0 || len(rows) > storageRoutingPageSources || storageRoutingSize(rows) > storageRoutingPageBytes {
		return SegmentRef{}, false, errSystemHistoryRoutingBudget
	}
	ids := make([][32]byte, 0, len(rows))
	for id := range rows {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return bytes.Compare(ids[i][:], ids[j][:]) < 0 })
	data := make([]byte, 32, storageRoutingSize(rows))
	copy(data, storageRoutingMagic[:])
	binary.BigEndian.PutUint32(data[8:12], 1)
	binary.BigEndian.PutUint32(data[12:16], uint32(len(ids)))
	binary.BigEndian.PutUint64(data[16:24], from)
	binary.BigEndian.PutUint64(data[24:32], to)
	for _, id := range ids {
		f := rows[id]
		if f.count > storageRoutingSourceHashes || len(f.bits) != storageFilterBytes(f.count) {
			return SegmentRef{}, false, errors.New("snapshots: invalid storage filter build")
		}
		data = append(data, id[:]...)
		data = binary.BigEndian.AppendUint32(data, f.count)
		data = binary.BigEndian.AppendUint32(data, uint32(len(f.bits)))
		data = append(data, f.bits...)
	}
	sum := sha256.Sum256(data)
	ref := SegmentRef{Dataset: SegmentDatasetStateDomainChange, Kind: SegmentStorageHistoryRouting, FromTxNum: from, ToTxNum: to, Rows: uint64(len(ids)), Size: uint64(len(data)), Checksum: fmt.Sprintf("sha256:%x", sum)}
	ref.Path = fmt.Sprintf("history/storage-routing-%d-%d-%x.route", from, to, sum)
	if err := os.MkdirAll(filepath.Join(dir, "history"), 0755); err != nil {
		return ref, false, err
	}
	f, err := os.CreateTemp(filepath.Join(dir, "history"), "storage-routing-*.tmp")
	if err != nil {
		return ref, false, err
	}
	name := f.Name()
	defer os.Remove(name)
	if _, err = f.Write(data); err == nil {
		err = f.Sync()
	}
	err = errors.Join(err, f.Close(), contextError(ctx))
	if err == nil {
		err = os.Rename(name, filepath.Join(dir, ref.Path))
	}
	if err == nil {
		err = syncSnapshotDir(filepath.Join(dir, "history"))
	}
	return ref, err == nil, err
}

// BuildStorageHistoryRoutingContext creates one bounded optional page without
// publishing it. On a shared store the caller holds a history read lease from
// build through guarded publication. Offline immutable copies need no writer.
func BuildStorageHistoryRoutingContext(ctx context.Context, dir string, m *Manifest, maxNew int) (SegmentRef, bool, error) {
	return buildStorageHistoryRoutingContext(ctx, dir, m, maxNew, 0, nil, smallSystemRoutingWorkLimit{})
}

// A soft work budget is checked between complete sources. One authenticated
// source may overrun it; no partial source is ever published to meet a deadline.
func buildStorageHistoryRoutingContext(ctx context.Context, dir string, m *Manifest, maxNew int, startTx uint64, nextTx *uint64, work smallSystemRoutingWorkLimit) (SegmentRef, bool, error) {
	return buildStorageHistoryRoutingWithPolicy(ctx, dir, m, maxNew, startTx, nextTx, storageRoutingBuildPolicy{work: work})
}

type storageRoutingBuildStats struct {
	sourceReads, unsupported int
	broadReads               int
	inputBytes               uint64
	repairDeferred           bool
}

// Broad coverage is an admission hint only. It suppresses redundant builds,
// never writes a proof or answers a query from this process-local ID cache.
type storageRoutingBuildPolicy struct {
	work               smallSystemRoutingWorkLimit
	broadFrom, broadTo uint64
	broadCovered       map[[32]byte]struct{}
	bootstrap          *storageRoutingBootstrap
	stats              *storageRoutingBuildStats
}

func buildStorageHistoryRoutingWithPolicy(ctx context.Context, dir string, m *Manifest, maxNew int, startTx uint64, nextTx *uint64, policy storageRoutingBuildPolicy) (SegmentRef, bool, error) {
	if nextTx != nil {
		*nextTx = 0
	}
	if m == nil || maxNew < 1 {
		return SegmentRef{}, false, contextError(ctx)
	}
	m = manifestLookupView(m)
	reader := newStorageHistoryRoutingReader(m)
	cfg, _ := DefaultDomainRegistry().Dataset(SegmentDatasetStateDomainChange)
	rows := make(map[[32]byte]storageSourceFilter)
	from, to, readBytes := uint64(0), uint64(0), uint64(0)
	started, attempts := time.Now(), 0
	work := policy.work
	for _, h := range m.Segments {
		if err := contextError(ctx); err != nil {
			return SegmentRef{}, false, err
		}
		if h.Kind != SegmentHistory || h.normalizedDataset() != SegmentDatasetStateDomainChange || h.ToTxNum < startTx || !isStateDomainChangeBinarySegmentPath(h.Path) {
			continue
		}
		a, ok := cfg.HistoryAccessorRef(m, h)
		if !ok {
			return SegmentRef{}, false, errors.New("snapshots: storage source missing accessor")
		}
		id := systemHistoryRoutingPairID(h, a)
		first := sort.Search(len(reader.refs), func(i int) bool { return reader.refs[i].ToTxNum >= h.FromTxNum })
		last := first
		for last < len(reader.refs) && reader.refs[last].FromTxNum <= h.ToTxNum {
			last++
		}
		// A skipped broad source may still own a retained storage page. Never
		// let a new page span that page and retire proofs it does not copy.
		if len(rows) > 0 && last > first {
			break
		}
		if _, covered := policy.broadCovered[id]; covered && h.FromTxNum >= policy.broadFrom && h.ToTxNum <= policy.broadTo {
			continue
		}
		_, covered, err := reader.source(ctx, dir, h, id)
		if err != nil {
			return SegmentRef{}, false, err
		}
		if covered {
			if len(rows) > 0 {
				break
			}
			continue
		}
		if attempts > 0 && (work.bytes > 0 && a.Size > work.bytes-min(readBytes, work.bytes) || work.duration > 0 && time.Since(started) >= work.duration) {
			break
		}
		var f storageSourceFilter
		if policy.bootstrap != nil {
			col, ok := policy.bootstrap.route.columns[id]
			if !ok {
				continue
			}
			f = policy.bootstrap.filters[col]
		} else {
			f, err = storageHistorySourceFilter(ctx, dir, h, a)
			readBytes += a.Size
		}
		attempts++
		if policy.stats != nil && policy.bootstrap == nil {
			policy.stats.sourceReads++
			policy.stats.inputBytes += a.Size
			if errors.Is(err, errSystemHistoryRoutingBudget) {
				policy.stats.unsupported++
			}
		}
		if nextTx != nil && h.ToTxNum != ^uint64(0) {
			*nextTx = h.ToTxNum + 1
		}
		if err != nil && !errors.Is(err, errSystemHistoryRoutingBudget) {
			return SegmentRef{}, false, err
		}
		if err == nil {
			if last > first {
				ref, built, repairErr := repairStorageHistoryRoutingPage(ctx, dir, m, reader.refs[first:last], h, id, f)
				if policy.stats != nil && !built && repairErr == nil {
					policy.stats.repairDeferred = true
				}
				return ref, built, repairErr
			}
			if storageRoutingSize(rows)+40+len(f.bits) > storageRoutingPageBytes {
				if nextTx != nil {
					*nextTx = h.FromTxNum // Retry this complete source in the next page.
				}
				break
			}
			if len(rows) == 0 {
				from = h.FromTxNum
			}
			rows[id], to = f, h.ToTxNum
		}
		if attempts >= min(maxNew, storageRoutingPageSources) {
			break
		}
	}
	if len(rows) == 0 {
		return SegmentRef{}, false, nil
	}
	return writeStorageHistoryRoutingPage(ctx, dir, from, to, rows)
}

func repairStorageHistoryRoutingPage(ctx context.Context, dir string, m *Manifest, overlaps []SegmentRef, history SegmentRef, id [32]byte, f storageSourceFilter) (SegmentRef, bool, error) {
	current := manifestHistorySources(m)
	if current == nil {
		return SegmentRef{}, false, nil
	}
	rows := map[[32]byte]storageSourceFilter{id: f}
	from, to := history.FromTxNum, history.ToTxNum
	for _, ref := range overlaps {
		prior, err := readStorageHistoryRouting(ctx, dir, ref)
		if err != nil {
			return SegmentRef{}, false, err
		}
		for id, f := range prior {
			if i, ok := current.byID[id]; ok {
				h := m.Segments[current.sources[i].row]
				if h.FromTxNum < ref.FromTxNum || h.ToTxNum > ref.ToTxNum {
					continue
				}
				rows[id] = f
				if len(rows) > storageRoutingPageSources || storageRoutingSize(rows) > storageRoutingPageBytes {
					return SegmentRef{}, false, nil
				}
				from, to = min(from, h.FromTxNum), max(to, h.ToTxNum)
			}
		}
	}
	if len(rows) > storageRoutingPageSources || storageRoutingSize(rows) > storageRoutingPageBytes {
		return SegmentRef{}, false, nil
	}
	return writeStorageHistoryRoutingPage(ctx, dir, from, to, rows)
}

func checkStorageHistoryRoutingContext(ctx context.Context, dir string, m *Manifest, ref SegmentRef) error {
	rows, err := readStorageHistoryRouting(ctx, dir, ref)
	if err != nil {
		return err
	}
	m = manifestLookupView(m)
	cfg, _ := DefaultDomainRegistry().Dataset(SegmentDatasetStateDomainChange)
	for _, h := range m.Segments {
		if h.Kind != SegmentHistory || h.normalizedDataset() != SegmentDatasetStateDomainChange {
			continue
		}
		a, ok := cfg.HistoryAccessorRef(m, h)
		if !ok {
			continue
		}
		want, covered := rows[systemHistoryRoutingPairID(h, a)]
		if !covered {
			continue
		}
		if h.FromTxNum < ref.FromTxNum || h.ToTxNum > ref.ToTxNum {
			return errors.New("snapshots: storage source outside routing page")
		}
		actual, err := storageHistorySourceFilter(ctx, dir, h, a)
		if err != nil {
			return err
		}
		if actual.count != want.count || !bytes.Equal(actual.bits, want.bits) {
			return errors.New("snapshots: storage routing source proof mismatch")
		}
	}
	return contextError(ctx)
}

func CheckStorageHistoryRouting(dir string, ref SegmentRef) error {
	m, err := LoadProductionManifest(dir)
	if err != nil {
		return err
	}
	return checkStorageHistoryRoutingContext(context.Background(), dir, m, ref)
}
