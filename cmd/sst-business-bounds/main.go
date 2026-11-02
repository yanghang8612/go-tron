// Read-only SST index-bound census. This is a physical file-set observation,
// never a Pebble live-key snapshot. Data payloads are not read in census mode.
package main

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"time"

	"github.com/cockroachdb/pebble/objstorage/objstorageprovider"
	"github.com/cockroachdb/pebble/sstable"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/ethereum/go-ethereum/ethdb/memorydb"
	"github.com/golang/snappy"
	"github.com/klauspost/compress/zstd"
	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/state/kvdomains"
)

type budget struct {
	start       time.Time
	bytes, rate int64
}
type countedFile struct {
	*os.File
	budget *budget
}

func (f *countedFile) ReadAt(p []byte, off int64) (int, error) {
	n, e := f.File.ReadAt(p, off)
	f.budget.bytes += int64(n)
	if f.budget.rate > 0 {
		target := time.Duration(float64(f.budget.bytes) / float64(f.budget.rate) * float64(time.Second))
		if delay := target - time.Since(f.budget.start); delay > 0 {
			time.Sleep(delay)
		}
	}
	return n, e
}

type entry struct{ key, value []byte }

func block(f *countedFile, h sstable.BlockHandle) ([]byte, error) {
	if h.Length > 16<<20 {
		return nil, fmt.Errorf("index block too large: %d", h.Length)
	}
	b := make([]byte, h.Length+5)
	if _, e := f.ReadAt(b, int64(h.Offset)); e != nil {
		return nil, e
	}
	switch b[h.Length] {
	case 0:
		return b[:h.Length], nil
	case 1:
		return snappy.Decode(nil, b[:h.Length])
	case 7:
		size, n := binary.Uvarint(b[:h.Length])
		if n <= 0 || size > 32<<20 {
			return nil, fmt.Errorf("invalid zstd size")
		}
		d, e := zstd.NewReader(nil, zstd.WithDecoderConcurrency(1), zstd.WithDecoderMaxMemory(64<<20))
		if e != nil {
			return nil, e
		}
		defer d.Close()
		out, e := d.DecodeAll(b[n:h.Length], nil)
		if e == nil && uint64(len(out)) != size {
			return nil, fmt.Errorf("decoded size mismatch")
		}
		return out, e
	default:
		return nil, fmt.Errorf("unknown block compression %d", b[h.Length])
	}
}
func entries(b []byte) ([]entry, error) {
	if len(b) < 4 {
		return nil, io.ErrUnexpectedEOF
	}
	nr := int(binary.LittleEndian.Uint32(b[len(b)-4:]))
	end := len(b) - 4 - 4*nr
	if nr < 0 || end < 0 {
		return nil, fmt.Errorf("invalid restart array")
	}
	var out []entry
	var prev []byte
	for pos := 0; pos < end; {
		read := func() (uint64, error) {
			v, n := binary.Uvarint(b[pos:end])
			if n <= 0 {
				return 0, io.ErrUnexpectedEOF
			}
			pos += n
			return v, nil
		}
		shared, e := read()
		if e != nil {
			return nil, e
		}
		unshared, e := read()
		if e != nil {
			return nil, e
		}
		vl, e := read()
		if e != nil {
			return nil, e
		}
		if shared > uint64(len(prev)) || unshared+vl > uint64(end-pos) {
			return nil, fmt.Errorf("invalid prefix-coded entry")
		}
		k := append(append([]byte(nil), prev[:shared]...), b[pos:pos+int(unshared)]...)
		pos += int(unshared)
		v := append([]byte(nil), b[pos:pos+int(vl)]...)
		pos += int(vl)
		prev = k
		if len(k) < 8 {
			return nil, fmt.Errorf("short internal index key")
		}
		out = append(out, entry{append([]byte(nil), k[:len(k)-8]...), v})
	}
	return out, nil
}

var ranges = rawdb.DiskSpaceObservationRanges()

func family(k []byte) string {
	for _, r := range ranges {
		if bytes.Compare(k, r.Start) >= 0 && bytes.Compare(k, r.End) < 0 {
			return r.Name
		}
	}
	return "unclassified"
}
func point(k []byte) string {
	n := family(k)
	if n == "account_kv_latest" {
		_, _, d, _, ok := rawdb.DecodeStateKVLatestKey(k)
		if ok {
			return "account_kv_latest/" + kvdomains.Name(d)
		}
	}
	return n
}
func interval(lo, hi []byte) string {
	if len(lo) == 0 {
		return "mixed_or_first_block"
	}
	a, b := family(lo), family(hi)
	if a != b || a == "unclassified" {
		return "mixed_or_unclassified"
	}
	if a != "account_kv_latest" {
		return a
	}
	oa, ga, da, _, oka := rawdb.DecodeStateKVLatestKey(lo)
	ob, gb, db, _, okb := rawdb.DecodeStateKVLatestKey(hi)
	if oka && okb && oa == ob && ga == gb && da == db {
		return a + "/" + kvdomains.Name(da)
	}
	return "account_kv_latest/mixed_owner_or_domain"
}

type result struct {
	Path        string            `json:"path"`
	Size        int64             `json:"file_bytes"`
	ReadBytes   int64             `json:"metadata_read_bytes"`
	DataBytes   uint64            `json:"data_block_bytes"`
	Outside     uint64            `json:"outside_data_block_bytes"`
	Blocks      int               `json:"data_blocks"`
	Entries     uint64            `json:"stored_entries_including_versions"`
	Deletes     uint64            `json:"stored_deletions"`
	ValueBlocks uint64            `json:"value_block_bytes"`
	ByBusiness  map[string]uint64 `json:"guaranteed_interval_bytes"`
}

func inspect(f *countedFile, verifyPayload bool) (res result, err error) {
	before, e := f.Stat()
	if e != nil {
		return res, e
	}
	res.Path = f.Name()
	res.Size = before.Size()
	res.ByBusiness = map[string]uint64{}
	start := f.budget.bytes
	rd, e := sstable.NewSimpleReadable(f)
	if e != nil {
		return res, e
	}
	r, e := sstable.NewReader(rd, sstable.ReaderOptions{})
	if e != nil {
		return res, e
	}
	defer r.Close()
	l, e := r.Layout()
	if e != nil {
		return res, e
	}
	res.Entries = r.Properties.NumEntries
	res.Deletes = r.Properties.NumDeletions
	res.ValueBlocks = r.Properties.ValueBlocksSize
	var lo []byte
	index := 0
	for _, h := range l.Index {
		b, e := block(f, h)
		if e != nil {
			return res, e
		}
		es, e := entries(b)
		if e != nil {
			return res, e
		}
		for _, en := range es {
			if index >= len(l.Data) {
				return res, fmt.Errorf("too many index entries")
			}
			off, n := binary.Uvarint(en.value)
			if n <= 0 {
				return res, fmt.Errorf("bad handle")
			}
			length, n := binary.Uvarint(en.value[n:])
			if n <= 0 {
				return res, fmt.Errorf("bad length")
			}
			bh := l.Data[index].BlockHandle
			if bh.Offset != off || bh.Length != length {
				return res, fmt.Errorf("index handle differs from Pebble Layout")
			}
			if len(lo) > 0 && bytes.Compare(lo, en.key) > 0 {
				return res, fmt.Errorf("nonmonotonic separators")
			}
			group := interval(lo, en.key)
			res.ByBusiness[group] += length + 5
			res.DataBytes += length + 5
			if verifyPayload && group != "mixed_or_first_block" && group != "mixed_or_unclassified" && group != "account_kv_latest/mixed_owner_or_domain" {
				data, e := block(f, bh)
				if e != nil {
					return res, e
				}
				records, e := entries(data)
				if e != nil {
					return res, e
				}
				for _, record := range records {
					if point(record.key) != group {
						return res, fmt.Errorf("false pure attribution %s vs %s", group, point(record.key))
					}
				}
			}
			lo = append(lo[:0], en.key...)
			index++
		}
	}
	if index != len(l.Data) || uint64(index) != r.Properties.NumDataBlocks {
		return res, fmt.Errorf("block count mismatch")
	}
	res.Blocks = index
	if res.DataBytes > uint64(res.Size) {
		return res, fmt.Errorf("data bytes exceed file")
	}
	res.Outside = uint64(res.Size) - res.DataBytes
	res.ReadBytes = f.budget.bytes - start
	after, e := f.Stat()
	if e != nil {
		return res, e
	}
	if before.Size() != after.Size() || before.ModTime() != after.ModTime() {
		return res, fmt.Errorf("SST changed during read")
	}
	return res, nil
}
func fixture(dir string) error {
	m := memorydb.New()
	defer m.Close()
	var owner common.Address
	owner[0] = 0x41
	owner[20] = 1
	for _, d := range []kvdomains.KVDomain{kvdomains.SystemDelegation, kvdomains.ContractStorage, kvdomains.AccountAssetV2} {
		for j := 0; j < 300; j++ {
			k := make([]byte, 32)
			binary.BigEndian.PutUint64(k[24:], uint64(j))
			v := make([]byte, 32)
			v[31] = byte(j)
			if e := rawdb.WriteStateKVLatest(m, owner, 0, d, k, v); e != nil {
				return e
			}
		}
	}
	for i, c := range []sstable.Compression{sstable.SnappyCompression, sstable.ZstdCompression, sstable.NoCompression} {
		name := filepath.Join(dir, fmt.Sprintf("fixture-%d.sst", i))
		f, e := vfs.Default.Create(name)
		if e != nil {
			return e
		}
		w := sstable.NewWriter(objstorageprovider.NewFileWritable(f), sstable.WriterOptions{BlockSize: 256, IndexBlockSize: 256, TableFormat: sstable.TableFormatPebblev4, Compression: c})
		it := m.NewIterator(nil, nil)
		for it.Next() {
			if e = w.Set(it.Key(), it.Value()); e != nil {
				return e
			}
		}
		it.Release()
		if e = w.Close(); e != nil {
			return e
		}
		osf, e := os.Open(name)
		if e != nil {
			return e
		}
		res, e := inspect(&countedFile{osf, &budget{start: time.Now()}}, true)
		if e != nil {
			return e
		}
		fmt.Println("fixture verified", i, res.Blocks, len(res.ByBusiness))
	}
	return nil
}
func main() {
	dir := flag.String("dir", "", "SST directory (files only; no database open)")
	out := flag.String("out", "", "output JSONL")
	rate := flag.Int64("read-mib-sec", 10, "metadata read ceiling")
	self := flag.String("fixture", "", "write tiny synthetic files and independently check attribution")
	flag.Parse()
	if *self != "" {
		if e := os.MkdirAll(*self, 0755); e != nil {
			panic(e)
		}
		if e := fixture(*self); e != nil {
			panic(e)
		}
		return
	}
	if *dir == "" || *out == "" {
		panic("dir and out required")
	}
	names, e := filepath.Glob(filepath.Join(*dir, "*.sst"))
	if e != nil {
		panic(e)
	}
	sort.Strings(names)
	b := &budget{start: time.Now(), rate: *rate << 20}
	var files []*countedFile
	var missing []string
	// Open all file descriptors before analysis. This retains immutable inodes
	// even if normal compaction unlinks a name; it never acquires the DB lock.
	for _, name := range names {
		f, e := os.Open(name)
		if e != nil {
			missing = append(missing, fmt.Sprintf("%s: %v", name, e))
			continue
		}
		files = append(files, &countedFile{f, b})
	}
	f, e := os.OpenFile(*out, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0644)
	if e != nil {
		panic(e)
	}
	defer f.Close()
	enc := json.NewEncoder(f)
	for i, p := range files {
		res, e := inspect(p, false)
		if e != nil {
			p.Close()
			if e = enc.Encode(map[string]interface{}{"path": p.Name(), "error": e.Error()}); e != nil {
				panic(e)
			}
		} else {
			if e = enc.Encode(res); e != nil {
				panic(e)
			}
		}
		if i%128 == 0 {
			fmt.Printf("files %d/%d metadata_read %d\n", i+1, len(files), b.bytes)
		}
	}
	if e = f.Sync(); e != nil {
		panic(e)
	}
	summary := map[string]interface{}{"started_utc": b.start.UTC().Format(time.RFC3339Nano), "finished_utc": time.Now().UTC().Format(time.RFC3339Nano), "listed_files": len(names), "opened_files": len(files), "missing_files": missing, "metadata_read_bytes": b.bytes, "scope": "immutable SST file set, may include obsolete files and versions; index-interval physical lower bounds; no data blocks read"}
	data, _ := json.MarshalIndent(summary, "", "  ")
	if e = os.WriteFile(*out+".summary.json", data, 0644); e != nil {
		panic(e)
	}
	fmt.Println(string(data))
}
