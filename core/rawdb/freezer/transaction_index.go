package freezer

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"math"
	"os"
	"path/filepath"
	"sort"
)

const (
	transactionIndexMagic               = "gtxi0001"
	transactionIndexVersion             = uint32(1)
	transactionIndexHeaderSize          = uint32(128)
	transactionIndexBucketHeaderSize    = uint64(16)
	transactionIndexDefaultPrefixBits   = uint32(20)
	transactionIndexFingerprintBits     = uint32(64)
	transactionIndexMinPrefixBits       = uint32(8)
	transactionIndexMaxPrefixBits       = uint32(24)
	transactionIndexAdaptiveMinBits     = uint32(12)
	transactionIndexTargetBucketRows    = uint64(64)
	transactionIndexMaxBucketRows       = uint64(1 << 20)
	transactionIndexDirectoryEntryBytes = uint64(8)
	transactionLocationMarker           = uint64(1) << 63
	transactionLocationOrdinalBits      = uint64(16)
)

var transactionIndexCRC = crc32.MakeTable(crc32.Castagnoli)

// AdaptiveTransactionIndexPrefixBits treats configuredMax as a lookup-latency
// ceiling rather than forcing every immutable run to reserve the same fixed
// directory. Small 65k-block runs otherwise each spend 8 MiB on a 20-bit
// directory even when they contain only a few hundred thousand transactions.
//
// The 64-bit fingerprint is unchanged. Reducing the routed prefix only grows
// the checksummed candidate bucket; callers still verify the full transaction
// hash. A 12-bit production floor also keeps a future billion-row geometric
// merge below the on-disk bucket safety limit. Explicit test/special-purpose
// configurations below 12 bits retain their requested ceiling.
func AdaptiveTransactionIndexPrefixBits(rows uint64, configuredMax uint32) uint32 {
	if configuredMax == 0 {
		configuredMax = transactionIndexDefaultPrefixBits
	}
	if configuredMax < transactionIndexMinPrefixBits {
		return transactionIndexMinPrefixBits
	}
	if configuredMax > transactionIndexMaxPrefixBits {
		configuredMax = transactionIndexMaxPrefixBits
	}
	floor := min(configuredMax, transactionIndexAdaptiveMinBits)
	wantedBuckets := ceilDivUint64(rows, transactionIndexTargetBucketRows)
	bits := uint32(0)
	for buckets := uint64(1); buckets < wantedBuckets && bits < transactionIndexMaxPrefixBits; buckets <<= 1 {
		bits++
	}
	if bits < floor {
		bits = floor
	}
	if bits > configuredMax {
		bits = configuredMax
	}
	return bits
}

func ceilDivUint64(value, divisor uint64) uint64 {
	if value == 0 {
		return 0
	}
	return 1 + (value-1)/divisor
}

// TransactionIndexEntry is one hash-sorted immutable transaction locator.
// Location retains rawdb's eight-byte encoding so both legacy block-only and
// packed block/ordinal values can be represented without translation.
type TransactionIndexEntry struct {
	Hash     [32]byte
	Location uint64
}

// TransactionIndexIterator must emit entries in strictly increasing full-hash
// order. BuildTransactionIndexRun consumes it once and streams completed
// prefix buckets directly to the destination's reserved data region.
type TransactionIndexIterator func(yield func(TransactionIndexEntry) error) error

// TransactionIndexBuildOptions configures an immutable transaction-index run.
type TransactionIndexBuildOptions struct {
	Context    context.Context
	PrefixBits uint32
	StartBlock uint64
	EndBlock   uint64
	Iterate    TransactionIndexIterator
}

// TransactionIndexBuildResult summarizes a published immutable run.
type TransactionIndexBuildResult struct {
	Path          string
	Rows          uint64
	StartBlock    uint64
	EndBlock      uint64
	PrefixBits    uint32
	Buckets       uint64
	NonEmpty      uint64
	MaxBucketRows uint64
	FileBytes     uint64
}

// TransactionIndexRun is a concurrency-safe reader for one immutable run. The
// fixed directory is held in memory; bucket payloads are checksummed and read
// on demand from the file.
type TransactionIndexRun struct {
	path       string
	file       *os.File
	prefixBits uint32
	startBlock uint64
	endBlock   uint64
	rows       uint64
	fileBytes  uint64
	directory  []uint64
}

// BuildTransactionIndexRun writes a checksummed immutable transaction index
// and publishes it with an atomic rename. The destination must not exist.
func BuildTransactionIndexRun(path string, opts TransactionIndexBuildOptions) (TransactionIndexBuildResult, error) {
	var result TransactionIndexBuildResult
	if opts.Context == nil {
		opts.Context = context.Background()
	}
	if err := opts.Context.Err(); err != nil {
		return result, err
	}
	if opts.Iterate == nil {
		return result, errors.New("transaction index: iterator is required")
	}
	if opts.EndBlock <= opts.StartBlock {
		return result, fmt.Errorf("transaction index: invalid block range [%d,%d)", opts.StartBlock, opts.EndBlock)
	}
	prefixBits := opts.PrefixBits
	if prefixBits == 0 {
		prefixBits = transactionIndexDefaultPrefixBits
	}
	if prefixBits < transactionIndexMinPrefixBits || prefixBits > transactionIndexMaxPrefixBits {
		return result, fmt.Errorf("transaction index: prefix bits %d outside [%d,%d]", prefixBits, transactionIndexMinPrefixBits, transactionIndexMaxPrefixBits)
	}
	if _, err := os.Stat(path); err == nil {
		return result, fmt.Errorf("transaction index: destination %q already exists", path)
	} else if !os.IsNotExist(err) {
		return result, err
	}
	bucketCount := uint64(1) << prefixBits
	directoryBytes, ok := checkedMul(bucketCount+1, transactionIndexDirectoryEntryBytes)
	if !ok {
		return result, errors.New("transaction index: directory size overflow")
	}
	dataOffset, ok := checkedAdd(uint64(transactionIndexHeaderSize), directoryBytes)
	if !ok {
		return result, errors.New("transaction index: data offset overflow")
	}
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		return result, err
	}
	file, err := os.CreateTemp(filepath.Dir(path), ".tx-index-*.tmp")
	if err != nil {
		return result, err
	}
	tempName := file.Name()
	defer func() {
		_ = file.Close()
		_ = os.Remove(tempName)
	}()
	if err := file.Truncate(int64(dataOffset)); err != nil {
		return result, err
	}
	if _, err := file.Seek(int64(dataOffset), io.SeekStart); err != nil {
		return result, err
	}
	dataWriter := bufio.NewWriterSize(file, 4<<20)

	directory := make([]uint64, int(bucketCount+1))
	cursor := dataOffset
	nextDirectory := uint64(0)
	currentPrefix := uint32(0)
	haveBucket := false
	var fingerprints, locations []uint64
	var bucketData []byte
	var rows, nonEmpty, maxBucketRows uint64
	flushBucket := func() error {
		if !haveBucket {
			return nil
		}
		if len(fingerprints) != len(locations) {
			return fmt.Errorf("transaction index: bucket %d fingerprint/location mismatch", currentPrefix)
		}
		for nextDirectory <= uint64(currentPrefix) {
			directory[nextDirectory] = cursor
			nextDirectory++
		}
		bucketBytes := int(transactionIndexBucketHeaderSize) + len(fingerprints)*16
		if cap(bucketData) < bucketBytes {
			bucketData = make([]byte, bucketBytes)
		} else {
			bucketData = bucketData[:bucketBytes]
		}
		fingerprintData := bucketData[transactionIndexBucketHeaderSize : transactionIndexBucketHeaderSize+uint64(len(fingerprints))*8]
		locationData := bucketData[transactionIndexBucketHeaderSize+uint64(len(fingerprints))*8:]
		for i := range fingerprints {
			binary.BigEndian.PutUint64(fingerprintData[i*8:(i+1)*8], fingerprints[i])
			binary.BigEndian.PutUint64(locationData[i*8:(i+1)*8], locations[i])
		}
		header := bucketData[:transactionIndexBucketHeaderSize]
		binary.BigEndian.PutUint32(header[0:4], uint32(len(fingerprints)))
		binary.BigEndian.PutUint32(header[4:8], crc32.Checksum(fingerprintData, transactionIndexCRC))
		binary.BigEndian.PutUint32(header[8:12], crc32.Checksum(locationData, transactionIndexCRC))
		binary.BigEndian.PutUint32(header[12:16], crc32.Checksum(header[:12], transactionIndexCRC))
		if _, err := dataWriter.Write(bucketData); err != nil {
			return err
		}
		var ok bool
		cursor, ok = checkedAdd(cursor, uint64(len(bucketData)))
		if !ok || cursor > math.MaxInt64 {
			return errors.New("transaction index: file size overflow")
		}
		nonEmpty++
		if uint64(len(fingerprints)) > maxBucketRows {
			maxBucketRows = uint64(len(fingerprints))
		}
		fingerprints = fingerprints[:0]
		locations = locations[:0]
		return nil
	}
	if err := visitTransactionIndexEntries(opts.Iterate, prefixBits, func(prefix uint32, fingerprint uint64, entry TransactionIndexEntry) error {
		if rows&4095 == 0 {
			if err := opts.Context.Err(); err != nil {
				return err
			}
		}
		block := transactionIndexLocationBlock(entry.Location)
		if block < opts.StartBlock || block >= opts.EndBlock {
			return fmt.Errorf("location block %d is outside run range [%d,%d)", block, opts.StartBlock, opts.EndBlock)
		}
		if haveBucket && prefix == currentPrefix && len(fingerprints) == int(transactionIndexMaxBucketRows) {
			return fmt.Errorf("transaction index: bucket %d exceeds %d rows; increase prefix bits", prefix, transactionIndexMaxBucketRows)
		}
		if !haveBucket || prefix != currentPrefix {
			if err := flushBucket(); err != nil {
				return err
			}
			currentPrefix = prefix
			haveBucket = true
		}
		fingerprints = append(fingerprints, fingerprint)
		locations = append(locations, entry.Location)
		rows++
		return nil
	}); err != nil {
		return result, fmt.Errorf("transaction index build: %w", err)
	}
	if err := opts.Context.Err(); err != nil {
		return result, err
	}
	if err := flushBucket(); err != nil {
		return result, err
	}
	for nextDirectory <= bucketCount {
		directory[nextDirectory] = cursor
		nextDirectory++
	}
	if err := dataWriter.Flush(); err != nil {
		return result, err
	}
	directoryData := make([]byte, len(directory)*8)
	for i, offset := range directory {
		binary.BigEndian.PutUint64(directoryData[i*8:(i+1)*8], offset)
	}
	if _, err := file.WriteAt(directoryData, int64(transactionIndexHeaderSize)); err != nil {
		return result, err
	}

	header := make([]byte, transactionIndexHeaderSize)
	copy(header[:8], transactionIndexMagic)
	binary.BigEndian.PutUint32(header[8:12], transactionIndexVersion)
	binary.BigEndian.PutUint32(header[12:16], transactionIndexHeaderSize)
	binary.BigEndian.PutUint32(header[16:20], prefixBits)
	binary.BigEndian.PutUint32(header[20:24], transactionIndexFingerprintBits)
	binary.BigEndian.PutUint64(header[24:32], opts.StartBlock)
	binary.BigEndian.PutUint64(header[32:40], opts.EndBlock)
	binary.BigEndian.PutUint64(header[40:48], rows)
	binary.BigEndian.PutUint64(header[48:56], uint64(transactionIndexHeaderSize))
	binary.BigEndian.PutUint64(header[56:64], bucketCount+1)
	binary.BigEndian.PutUint64(header[64:72], dataOffset)
	binary.BigEndian.PutUint64(header[72:80], cursor)
	binary.BigEndian.PutUint32(header[80:84], crc32.Checksum(directoryData, transactionIndexCRC))
	binary.BigEndian.PutUint32(header[84:88], crc32.Checksum(header[:84], transactionIndexCRC))
	if _, err := file.WriteAt(header, 0); err != nil {
		return result, err
	}
	if err := opts.Context.Err(); err != nil {
		return result, err
	}
	if err := file.Sync(); err != nil {
		return result, err
	}
	if err := file.Close(); err != nil {
		return result, err
	}
	if err := opts.Context.Err(); err != nil {
		return result, err
	}
	if err := atomicRename(tempName, path); err != nil {
		return result, err
	}
	result = TransactionIndexBuildResult{
		Path:          path,
		Rows:          rows,
		StartBlock:    opts.StartBlock,
		EndBlock:      opts.EndBlock,
		PrefixBits:    prefixBits,
		Buckets:       bucketCount,
		NonEmpty:      nonEmpty,
		MaxBucketRows: maxBucketRows,
		FileBytes:     cursor,
	}
	return result, nil
}

// OpenTransactionIndexRun validates the fixed header and directory. Bucket
// checksums are verified lazily by Candidates and eagerly by Verify.
func OpenTransactionIndexRun(path string) (*TransactionIndexRun, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	fail := func(err error) (*TransactionIndexRun, error) {
		_ = file.Close()
		return nil, err
	}
	header := make([]byte, transactionIndexHeaderSize)
	if _, err := io.ReadFull(file, header); err != nil {
		return fail(fmt.Errorf("transaction index header: %w", err))
	}
	if string(header[:8]) != transactionIndexMagic || binary.BigEndian.Uint32(header[8:12]) != transactionIndexVersion {
		return fail(errors.New("transaction index: unsupported magic or version"))
	}
	if binary.BigEndian.Uint32(header[12:16]) != transactionIndexHeaderSize {
		return fail(errors.New("transaction index: invalid header size"))
	}
	if crc32.Checksum(header[:84], transactionIndexCRC) != binary.BigEndian.Uint32(header[84:88]) {
		return fail(errors.New("transaction index: header checksum mismatch"))
	}
	prefixBits := binary.BigEndian.Uint32(header[16:20])
	if prefixBits < transactionIndexMinPrefixBits || prefixBits > transactionIndexMaxPrefixBits || binary.BigEndian.Uint32(header[20:24]) != transactionIndexFingerprintBits {
		return fail(errors.New("transaction index: unsupported directory or fingerprint width"))
	}
	bucketCount := uint64(1) << prefixBits
	startBlock := binary.BigEndian.Uint64(header[24:32])
	endBlock := binary.BigEndian.Uint64(header[32:40])
	if endBlock < startBlock {
		return fail(errors.New("transaction index: invalid block range"))
	}
	directoryOffset := binary.BigEndian.Uint64(header[48:56])
	directoryEntries := binary.BigEndian.Uint64(header[56:64])
	dataOffset := binary.BigEndian.Uint64(header[64:72])
	fileBytes := binary.BigEndian.Uint64(header[72:80])
	if directoryOffset != uint64(transactionIndexHeaderSize) || directoryEntries != bucketCount+1 {
		return fail(errors.New("transaction index: invalid directory metadata"))
	}
	directoryBytes, ok := checkedMul(directoryEntries, transactionIndexDirectoryEntryBytes)
	if !ok || dataOffset != directoryOffset+directoryBytes || directoryBytes > uint64(^uint(0)>>1) {
		return fail(errors.New("transaction index: invalid directory size"))
	}
	stat, err := file.Stat()
	if err != nil {
		return fail(err)
	}
	if fileBytes != uint64(stat.Size()) || fileBytes < dataOffset {
		return fail(errors.New("transaction index: file size mismatch"))
	}
	directoryData := make([]byte, int(directoryBytes))
	if _, err := file.ReadAt(directoryData, int64(directoryOffset)); err != nil {
		return fail(fmt.Errorf("transaction index directory: %w", err))
	}
	if crc32.Checksum(directoryData, transactionIndexCRC) != binary.BigEndian.Uint32(header[80:84]) {
		return fail(errors.New("transaction index: directory checksum mismatch"))
	}
	directory := make([]uint64, int(directoryEntries))
	for i := range directory {
		directory[i] = binary.BigEndian.Uint64(directoryData[i*8 : (i+1)*8])
		if i == 0 && directory[i] != dataOffset {
			return fail(errors.New("transaction index: invalid first bucket offset"))
		}
		if i > 0 && directory[i] < directory[i-1] {
			return fail(errors.New("transaction index: decreasing bucket offsets"))
		}
	}
	if directory[len(directory)-1] != fileBytes {
		return fail(errors.New("transaction index: invalid final bucket offset"))
	}
	return &TransactionIndexRun{
		path:       path,
		file:       file,
		prefixBits: prefixBits,
		startBlock: startBlock,
		endBlock:   endBlock,
		rows:       binary.BigEndian.Uint64(header[40:48]),
		fileBytes:  fileBytes,
		directory:  directory,
	}, nil
}

func (r *TransactionIndexRun) Close() error {
	if r == nil || r.file == nil {
		return nil
	}
	return r.file.Close()
}

func (r *TransactionIndexRun) Path() string       { return r.path }
func (r *TransactionIndexRun) StartBlock() uint64 { return r.startBlock }
func (r *TransactionIndexRun) EndBlock() uint64   { return r.endBlock }
func (r *TransactionIndexRun) Rows() uint64       { return r.rows }
func (r *TransactionIndexRun) PrefixBits() uint32 { return r.prefixBits }
func (r *TransactionIndexRun) Size() uint64       { return r.fileBytes }

func (r *TransactionIndexRun) CoversBlock(number uint64) bool {
	return r != nil && number >= r.startBlock && number < r.endBlock
}

// Candidates returns every packed location whose routed fingerprint equals the
// requested transaction hash. Callers must verify each candidate against the
// complete hash in the canonical block body before treating it as a hit.
func (r *TransactionIndexRun) Candidates(hash [32]byte) ([]uint64, error) {
	if r == nil || r.file == nil {
		return nil, errors.New("transaction index: closed reader")
	}
	prefix, fingerprint := transactionIndexPrefixFingerprint(hash, r.prefixBits)
	fingerprints, locations, err := r.readBucket(prefix)
	if err != nil || len(fingerprints) == 0 {
		return nil, err
	}
	rows := len(fingerprints) / 8
	first := sort.Search(rows, func(i int) bool {
		return binary.BigEndian.Uint64(fingerprints[i*8:(i+1)*8]) >= fingerprint
	})
	if first == rows || binary.BigEndian.Uint64(fingerprints[first*8:(first+1)*8]) != fingerprint {
		return nil, nil
	}
	last := first + 1
	for last < rows && binary.BigEndian.Uint64(fingerprints[last*8:(last+1)*8]) == fingerprint {
		last++
	}
	result := make([]uint64, last-first)
	for i := range result {
		result[i] = binary.BigEndian.Uint64(locations[(first+i)*8 : (first+i+1)*8])
	}
	return result, nil
}

// Verify reads and checksums every non-empty bucket.
func (r *TransactionIndexRun) Verify() error {
	return r.VerifyContext(context.Background())
}

// VerifyContext reads and checksums every non-empty bucket while allowing
// online maintenance to yield promptly when historical sync starts.
func (r *TransactionIndexRun) VerifyContext(ctx context.Context) error {
	if r == nil || r.file == nil {
		return errors.New("transaction index: closed reader")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	var rows uint64
	for prefix := 0; prefix < len(r.directory)-1; prefix++ {
		if prefix&4095 == 0 {
			if err := ctx.Err(); err != nil {
				return err
			}
		}
		fingerprints, _, err := r.readBucket(uint32(prefix))
		if err != nil {
			return fmt.Errorf("transaction index bucket %d: %w", prefix, err)
		}
		rows += uint64(len(fingerprints) / 8)
	}
	if rows != r.rows {
		return fmt.Errorf("transaction index: verified %d rows, header declares %d", rows, r.rows)
	}
	return nil
}

func (r *TransactionIndexRun) readBucket(prefix uint32) ([]byte, []byte, error) {
	if uint64(prefix)+1 >= uint64(len(r.directory)) {
		return nil, nil, errors.New("transaction index: bucket outside directory")
	}
	start, end := r.directory[prefix], r.directory[prefix+1]
	if start == end {
		return nil, nil, nil
	}
	span := end - start
	if span < transactionIndexBucketHeaderSize || (span-transactionIndexBucketHeaderSize)%16 != 0 {
		return nil, nil, errors.New("transaction index: invalid bucket span")
	}
	rows := (span - transactionIndexBucketHeaderSize) / 16
	if rows > transactionIndexMaxBucketRows || span > uint64(^uint(0)>>1) {
		return nil, nil, errors.New("transaction index: bucket exceeds safety limit")
	}
	data := make([]byte, int(span))
	if _, err := r.file.ReadAt(data, int64(start)); err != nil {
		return nil, nil, err
	}
	header := data[:transactionIndexBucketHeaderSize]
	if crc32.Checksum(header[:12], transactionIndexCRC) != binary.BigEndian.Uint32(header[12:16]) {
		return nil, nil, errors.New("transaction index: bucket header checksum mismatch")
	}
	if uint64(binary.BigEndian.Uint32(header[:4])) != rows {
		return nil, nil, errors.New("transaction index: bucket row count mismatch")
	}
	fingerprintData := data[transactionIndexBucketHeaderSize : transactionIndexBucketHeaderSize+rows*8]
	locationData := data[transactionIndexBucketHeaderSize+rows*8:]
	if crc32.Checksum(fingerprintData, transactionIndexCRC) != binary.BigEndian.Uint32(header[4:8]) {
		return nil, nil, errors.New("transaction index: fingerprint checksum mismatch")
	}
	if crc32.Checksum(locationData, transactionIndexCRC) != binary.BigEndian.Uint32(header[8:12]) {
		return nil, nil, errors.New("transaction index: location checksum mismatch")
	}
	for i := 1; i < int(rows); i++ {
		previous := binary.BigEndian.Uint64(fingerprintData[(i-1)*8 : i*8])
		current := binary.BigEndian.Uint64(fingerprintData[i*8 : (i+1)*8])
		if current < previous {
			return nil, nil, errors.New("transaction index: unsorted fingerprints")
		}
	}
	return fingerprintData, locationData, nil
}

func visitTransactionIndexEntries(iterate TransactionIndexIterator, prefixBits uint32, visit func(uint32, uint64, TransactionIndexEntry) error) error {
	var previous [32]byte
	seen := false
	return iterate(func(entry TransactionIndexEntry) error {
		if seen && bytes.Compare(previous[:], entry.Hash[:]) >= 0 {
			return errors.New("entries are not in strictly increasing hash order")
		}
		previous = entry.Hash
		seen = true
		prefix, fingerprint := transactionIndexPrefixFingerprint(entry.Hash, prefixBits)
		return visit(prefix, fingerprint, entry)
	})
}

func transactionIndexPrefixFingerprint(hash [32]byte, prefixBits uint32) (uint32, uint64) {
	high := binary.BigEndian.Uint64(hash[:8])
	next := binary.BigEndian.Uint64(hash[8:16])
	return uint32(high >> (64 - prefixBits)), high<<prefixBits | next>>(64-prefixBits)
}

func transactionIndexLocationBlock(location uint64) uint64 {
	if location&transactionLocationMarker == 0 {
		return location
	}
	return (location &^ transactionLocationMarker) >> transactionLocationOrdinalBits
}

func checkedAdd(a, b uint64) (uint64, bool) {
	if math.MaxUint64-a < b {
		return 0, false
	}
	return a + b, true
}

func checkedMul(a, b uint64) (uint64, bool) {
	if a != 0 && b > math.MaxUint64/a {
		return 0, false
	}
	return a * b, true
}
