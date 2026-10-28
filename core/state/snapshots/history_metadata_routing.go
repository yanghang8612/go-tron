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

	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/state/kvdomains"
)

const metadataRoutingPageSources = 256
const metadataRoutingBloomBytes = 1024
const metadataRoutingHeaderBytes = 32
const metadataRoutingMaxPageBytes = metadataRoutingHeaderBytes + metadataRoutingPageSources*(40+metadataRoutingBloomBytes)

var metadataRoutingMagic = [8]byte{'G', 'T', 'M', 'E', 'T', 'R', '0', '1'}

var metadataRoutingRefreshCursor struct {
	sync.Mutex
	dir    string
	nextTx uint64
}

type metadataSourceFilter struct {
	count uint64
	bits  []byte
}

func metadataRoutingKey(key []byte) bool {
	return len(key) >= 31 && key[0] == byte(rawdb.StateFlatDomainKVLatest) && kvdomains.KVDomain(binary.BigEndian.Uint16(key[29:31])) == kvdomains.ContractMetadata
}

func (f metadataSourceFilter) mayContain(key []byte) bool {
	if f.count == 0 {
		return false
	}
	h := sha256.Sum256(key)
	for i := 0; i < 6; i++ {
		bit := binary.BigEndian.Uint32(h[i*4:]) % (metadataRoutingBloomBytes * 8)
		if f.bits[bit/8]&(1<<uint(bit%8)) == 0 {
			return false
		}
	}
	return true
}

func (f *metadataSourceFilter) add(key []byte) {
	if !metadataRoutingKey(key) {
		return
	}
	if f.bits == nil {
		f.bits = make([]byte, metadataRoutingBloomBytes)
	}
	f.count++
	h := sha256.Sum256(key)
	for i := 0; i < 6; i++ {
		bit := binary.BigEndian.Uint32(h[i*4:]) % (metadataRoutingBloomBytes * 8)
		f.bits[bit/8] |= 1 << uint(bit%8)
	}
}

func metadataHistorySourceFilter(ctx context.Context, dir string, h, a SegmentRef) (metadataSourceFilter, error) {
	var f metadataSourceFilter
	err := iterateHistoryRoutingSourceKeys(ctx, dir, h, a, func(key []byte) error {
		if err := contextError(ctx); err != nil {
			return err
		}
		f.add(key)
		return nil
	})
	return f, err
}

func readMetadataHistoryRouting(ctx context.Context, dir string, ref SegmentRef) (map[[32]byte]metadataSourceFilter, error) {
	if ref.Kind != SegmentMetadataHistoryRouting || ref.normalizedDataset() != SegmentDatasetStateDomainChange || ref.Rows == 0 || ref.Rows > metadataRoutingPageSources || ref.Size < metadataRoutingHeaderBytes || ref.Size > metadataRoutingMaxPageBytes {
		return nil, errors.New("snapshots: invalid metadata routing reference")
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
	if uint64(stat.Size()) != ref.Size {
		return nil, errors.New("snapshots: metadata routing size mismatch")
	}
	data := make([]byte, ref.Size)
	if _, err := io.ReadFull(contextReader{ctx: ctx, r: f}, data); err != nil {
		return nil, err
	}
	sum := sha256.Sum256(data)
	if fmt.Sprintf("sha256:%x", sum) != ref.Checksum {
		return nil, errors.New("snapshots: metadata routing checksum mismatch")
	}
	if !bytes.Equal(data[:8], metadataRoutingMagic[:]) || binary.BigEndian.Uint32(data[8:12]) != 1 || binary.BigEndian.Uint32(data[12:16]) != uint32(ref.Rows) || binary.BigEndian.Uint64(data[16:24]) != ref.FromTxNum || binary.BigEndian.Uint64(data[24:32]) != ref.ToTxNum {
		return nil, errors.New("snapshots: metadata routing header mismatch")
	}
	rows := make(map[[32]byte]metadataSourceFilter, ref.Rows)
	pos := metadataRoutingHeaderBytes
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
		if i > 0 && bytes.Compare(prior[:], id[:]) >= 0 {
			return nil, errors.New("snapshots: unordered metadata routing source")
		}
		prior = id
		row := metadataSourceFilter{count: binary.BigEndian.Uint64(data[pos+32 : pos+40])}
		pos += 40
		if row.count > 0 {
			if len(data)-pos < metadataRoutingBloomBytes {
				return nil, io.ErrUnexpectedEOF
			}
			row.bits = data[pos : pos+metadataRoutingBloomBytes]
			pos += metadataRoutingBloomBytes
		}
		rows[id] = row
	}
	if pos != len(data) {
		return nil, errors.New("snapshots: trailing metadata routing data")
	}
	return rows, nil
}

// A request keeps only one authenticated page at a time. Source history refs
// are visited in range order; worst-case resident filter bytes stay page-sized.
type metadataHistoryRoutingReader struct {
	refs   []SegmentRef
	loaded string
	rows   map[[32]byte]metadataSourceFilter
}

func newMetadataHistoryRoutingReader(manifest *Manifest) *metadataHistoryRoutingReader {
	r := &metadataHistoryRoutingReader{}
	for _, ref := range manifest.Segments {
		if ref.Kind == SegmentMetadataHistoryRouting {
			r.refs = append(r.refs, ref)
		}
	}
	sort.Slice(r.refs, func(i, j int) bool { return r.refs[i].FromTxNum < r.refs[j].FromTxNum })
	return r
}

func (r *metadataHistoryRoutingReader) source(ctx context.Context, dir string, h SegmentRef, id [32]byte) (metadataSourceFilter, bool, error) {
	i := sort.Search(len(r.refs), func(i int) bool { return r.refs[i].ToTxNum >= h.FromTxNum })
	if i == len(r.refs) || r.refs[i].FromTxNum > h.FromTxNum || r.refs[i].ToTxNum < h.ToTxNum {
		return metadataSourceFilter{}, false, nil
	}
	ref := r.refs[i]
	if r.loaded != ref.Path {
		rows, err := readMetadataHistoryRouting(ctx, dir, ref)
		if err != nil {
			return metadataSourceFilter{}, false, err
		}
		r.rows, r.loaded = rows, ref.Path
	}
	f, found := r.rows[id]
	return f, found, nil
}

// BuildMetadataHistoryRoutingContext completes one independent, bounded page.
// Existing pages are not rewritten. If a canonical merge replaces sources,
// rebuilding that range retires the old page; uncovered neighbors fall back.
func BuildMetadataHistoryRoutingContext(ctx context.Context, dir string, manifest *Manifest, maxNew int) (SegmentRef, bool, error) {
	return buildMetadataHistoryRoutingContext(ctx, dir, manifest, maxNew, 0, nil)
}

func buildMetadataHistoryRoutingContext(ctx context.Context, dir string, manifest *Manifest, maxNew int, startTx uint64, nextTx *uint64) (SegmentRef, bool, error) {
	return buildMetadataHistoryRoutingWithBudget(ctx, dir, manifest, maxNew, startTx, nextTx, smallSystemRoutingWorkLimit{})
}

func buildMetadataHistoryRoutingWithBudget(ctx context.Context, dir string, manifest *Manifest, maxNew int, startTx uint64, nextTx *uint64, work smallSystemRoutingWorkLimit) (SegmentRef, bool, error) {
	if manifest == nil || maxNew < 1 {
		return SegmentRef{}, false, nil
	}
	manifest = manifestLookupView(manifest)
	reader := newMetadataHistoryRoutingReader(manifest)
	cfg, _ := DefaultDomainRegistry().Dataset(SegmentDatasetStateDomainChange)
	rows := make(map[[32]byte]metadataSourceFilter)
	var from, to uint64
	attempts := 0
	var bootstrap *historyRoutingBootstrap
	bootstrapLoaded := false
	var readBytes uint64
	readSources := 0
	started := time.Now()
	if nextTx != nil {
		*nextTx = 0
	}
	for _, h := range manifest.Segments {
		if err := contextError(ctx); err != nil {
			return SegmentRef{}, false, err
		}
		if h.Kind != SegmentHistory || h.normalizedDataset() != SegmentDatasetStateDomainChange || !isStateDomainChangeBinarySegmentPath(h.Path) {
			continue
		}
		if h.ToTxNum < startTx {
			continue
		}
		a, ok := cfg.HistoryAccessorRef(manifest, h)
		if !ok {
			return SegmentRef{}, false, errors.New("snapshots: metadata source missing accessor")
		}
		id := systemHistoryRoutingPairID(h, a)
		_, covered, err := reader.source(ctx, dir, h, id)
		if err != nil {
			return SegmentRef{}, false, err
		}
		if covered {
			if len(rows) > 0 {
				break // Never span a retained page, even after a source replacement.
			}
			continue
		}
		if !bootstrapLoaded {
			bootstrap, err = loadHistoryRoutingBootstrap(ctx, dir, manifest)
			if err != nil {
				return SegmentRef{}, false, err
			}
			bootstrapLoaded = true
		}
		var row metadataSourceFilter
		seeded := false
		if bootstrap != nil {
			if column, ok := bootstrap.columns[id]; ok && !bootstrap.metadataNonempty[column] {
				seeded = true
			}
		}
		if !seeded {
			// Page capacity is not a heavy-I/O budget. Zero-I/O broad
			// migrations can fill a page; uncached dictionaries remain bounded.
			if readSources > 0 && (work.bytes > 0 && (readBytes >= work.bytes || a.Size > work.bytes-readBytes) || work.duration > 0 && time.Since(started) >= work.duration) {
				break
			}
			readSources++
			readBytes += a.Size
			row, err = metadataHistorySourceFilter(ctx, dir, h, a)
		}
		attempts++
		if nextTx != nil && h.ToTxNum != ^uint64(0) {
			*nextTx = h.ToTxNum + 1
		}
		if errors.Is(err, errSystemHistoryRoutingBudget) {
			// Unsupported/oversized sources remain unknown. Never repeatedly
			// block other routing families and canonical compaction on them.
			if attempts >= min(maxNew, metadataRoutingPageSources) {
				break
			}
			continue
		}
		if err != nil {
			return SegmentRef{}, false, err
		}
		if len(rows) == 0 {
			from = h.FromTxNum
		}
		to = h.ToTxNum
		rows[id] = row
		if attempts >= min(maxNew, metadataRoutingPageSources) {
			break
		}
	}
	if len(rows) == 0 {
		return SegmentRef{}, false, nil
	}
	return writeMetadataHistoryRoutingPage(ctx, dir, from, to, rows)
}

func writeMetadataHistoryRoutingPage(ctx context.Context, dir string, from, to uint64, rows map[[32]byte]metadataSourceFilter) (SegmentRef, bool, error) {
	if from > to || len(rows) < 1 || len(rows) > metadataRoutingPageSources {
		return SegmentRef{}, false, errors.New("snapshots: invalid metadata page build bounds")
	}
	ids := make([][32]byte, 0, len(rows))
	for id := range rows {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return bytes.Compare(ids[i][:], ids[j][:]) < 0 })
	data := make([]byte, metadataRoutingHeaderBytes)
	copy(data, metadataRoutingMagic[:])
	binary.BigEndian.PutUint32(data[8:12], 1)
	binary.BigEndian.PutUint32(data[12:16], uint32(len(ids)))
	binary.BigEndian.PutUint64(data[16:24], from)
	binary.BigEndian.PutUint64(data[24:32], to)
	for _, id := range ids {
		row := rows[id]
		if row.count == 0 && len(row.bits) != 0 || row.count != 0 && len(row.bits) != metadataRoutingBloomBytes {
			return SegmentRef{}, false, errors.New("snapshots: invalid metadata filter build size")
		}
		data = append(data, id[:]...)
		data = binary.BigEndian.AppendUint64(data, row.count)
		data = append(data, row.bits...)
	}
	sum := sha256.Sum256(data)
	ref := SegmentRef{Dataset: SegmentDatasetStateDomainChange, Kind: SegmentMetadataHistoryRouting, FromTxNum: from, ToTxNum: to, Rows: uint64(len(ids)), Size: uint64(len(data)), Checksum: fmt.Sprintf("sha256:%x", sum)}
	ref.Path = fmt.Sprintf("history/metadata-routing-%d-%d-%x.route", from, to, sum)
	if err := os.MkdirAll(filepath.Join(dir, "history"), 0755); err != nil {
		return SegmentRef{}, false, err
	}
	f, err := os.CreateTemp(filepath.Join(dir, "history"), "metadata-routing-*.tmp")
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
	if err = contextError(ctx); err != nil {
		return SegmentRef{}, false, err
	}
	if err = os.Rename(name, filepath.Join(dir, ref.Path)); err != nil {
		return SegmentRef{}, false, err
	}
	if err = syncSnapshotDir(filepath.Join(dir, "history")); err != nil {
		return SegmentRef{}, false, err
	}
	return ref, true, nil
}

func checkMetadataHistoryRoutingContext(ctx context.Context, dir string, manifest *Manifest, ref SegmentRef) error {
	rows, err := readMetadataHistoryRouting(ctx, dir, ref)
	if err != nil {
		return err
	}
	manifest = manifestLookupView(manifest)
	cfg, _ := DefaultDomainRegistry().Dataset(SegmentDatasetStateDomainChange)
	for _, h := range manifest.Segments {
		if h.Kind != SegmentHistory || h.normalizedDataset() != SegmentDatasetStateDomainChange {
			continue
		}
		a, ok := cfg.HistoryAccessorRef(manifest, h)
		if !ok {
			continue
		}
		id := systemHistoryRoutingPairID(h, a)
		want, covered := rows[id]
		if !covered {
			continue
		}
		if h.FromTxNum < ref.FromTxNum || h.ToTxNum > ref.ToTxNum {
			return errors.New("snapshots: metadata source outside page")
		}
		actual, err := metadataHistorySourceFilter(ctx, dir, h, a)
		if err != nil {
			return err
		}
		if actual.count != want.count || !bytes.Equal(actual.bits, want.bits) {
			return errors.New("snapshots: metadata routing source proof mismatch")
		}
	}
	return contextError(ctx)
}

func CheckMetadataHistoryRouting(dir string, ref SegmentRef) error {
	m, err := LoadProductionManifest(dir)
	if err != nil {
		return err
	}
	return checkMetadataHistoryRoutingContext(context.Background(), dir, m, ref)
}

func refreshMetadataHistoryRoutingContext(ctx context.Context, dir string, busy bool) (bool, error) {
	if !historyReadLeaseGate.TryRLock() {
		return false, nil
	}
	defer historyReadLeaseGate.RUnlock()
	m, err := loadProductionManifestReadOnly(dir)
	if os.IsNotExist(err) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	count, indexed := 0, false
	for _, ref := range m.Segments {
		if ref.Kind == SegmentMetadataHistoryRouting {
			indexed = true
		}
		if ref.Kind == SegmentHistory && ref.normalizedDataset() == SegmentDatasetStateDomainChange {
			count++
		}
	}
	if count < systemHistoryRoutingMaxNewSources && !indexed {
		return false, nil
	}
	metadataRoutingRefreshCursor.Lock()
	start := uint64(0)
	if metadataRoutingRefreshCursor.dir == dir {
		start = metadataRoutingRefreshCursor.nextTx
	}
	metadataRoutingRefreshCursor.Unlock()
	next := start
	ref, built, err := buildMetadataHistoryRoutingWithBudget(ctx, dir, m, metadataRoutingPageSources, start, &next, smallSystemRoutingWorkLimit{bytes: 128 << 20, duration: 2 * time.Second})
	if err == nil {
		metadataRoutingRefreshCursor.Lock()
		metadataRoutingRefreshCursor.dir, metadataRoutingRefreshCursor.nextTx = dir, next
		metadataRoutingRefreshCursor.Unlock()
	}
	if err != nil || !built {
		return false, err
	}
	_, err = NewAggregator(dir).Integrate(m.VisibleTxStart, m.VisibleTxEnd, []SegmentRef{ref})
	return err == nil, err
}
