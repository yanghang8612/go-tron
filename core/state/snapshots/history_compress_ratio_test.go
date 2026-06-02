package snapshots

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math/rand"
	"testing"

	"github.com/klauspost/compress/zstd"
	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/state/kvdomains"
)

// valueProfile models the entropy of a 32-byte TRON storage word.
type valueProfile int

const (
	// smallInt: balances/counters/timestamps — big-endian with many leading
	// zero bytes (highly compressible).
	smallInt valueProfile = iota
	// addressLike: a 21-byte TRON address zero-padded to 32 (partly compressible).
	addressLike
	// hashLike: keccak/random 32 bytes (incompressible).
	hashLike
)

func valueBytes(rng *rand.Rand, p valueProfile) []byte {
	v := make([]byte, 32)
	switch p {
	case smallInt:
		// 1..8 significant low bytes, rest zero.
		n := 1 + rng.Intn(8)
		for i := 32 - n; i < 32; i++ {
			v[i] = byte(rng.Intn(256))
		}
	case addressLike:
		v[11] = 0x41 // TRON address prefix lands at offset 32-21
		for i := 12; i < 32; i++ {
			v[i] = byte(rng.Intn(256))
		}
	case hashLike:
		_, _ = rng.Read(v)
	}
	return v
}

// mixedValue draws a storage value from a realistic blend of the three profiles.
func mixedValue(rng *rand.Rand) []byte {
	r := rng.Float64()
	switch {
	case r < 0.55:
		return valueBytes(rng, smallInt)
	case r < 0.80:
		return valueBytes(rng, addressLike)
	default:
		return valueBytes(rng, hashLike)
	}
}

// buildHistoryCorpus returns encoded StateDomainChange records grouped into
// blocks (each block shares BlockNum/BlockHash; records carry sequential
// TxNum/Seq), for the requested dataset profile. valueFn produces Prev/Next.
func buildHistoryCorpus(t *testing.T, blocks, recordsPerBlock int, account bool, valueFn func(*rand.Rand) []byte) [][]byte {
	t.Helper()
	rng := rand.New(rand.NewSource(20260602))
	// A small recurring set of contract/account owners (real history revisits
	// the same hot contracts repeatedly).
	owners := make([]common.Address, 64)
	for i := range owners {
		_, _ = rng.Read(owners[i][:])
		owners[i][0] = 0x41
	}
	out := make([][]byte, 0, blocks*recordsPerBlock)
	for b := 0; b < blocks; b++ {
		var blockHash common.Hash
		_, _ = rng.Read(blockHash[:])
		blockNum := uint64(8_000_000 + b)
		// Within a block, contiguous runs touch the same contract (multi-slot
		// updates), as real contract calls do.
		owner := owners[rng.Intn(len(owners))]
		for s := 0; s < recordsPerBlock; s++ {
			if s%8 == 0 {
				owner = owners[rng.Intn(len(owners))]
			}
			ch := &rawdb.StateDomainChange{
				BlockNum:   blockNum,
				BlockHash:  blockHash,
				TxNum:      blockNum,
				Seq:        uint64(s),
				FlatDomain: rawdb.StateFlatDomainKVLatest,
				Owner:      owner,
				Generation: 0,
				Domain:     kvdomains.ContractStorage,
			}
			if account {
				// Account-envelope change: short domain key, protobuf-ish value
				// (varint-tagged fields with repeating tags across accounts).
				ch.FlatDomain = rawdb.StateFlatDomainAccountLatest
				ch.Domain = 0
				ch.Key = []byte{0x01}
				ch.NextExists = true
				ch.Next = accountEnvelopeBytes(rng)
				ch.PrevExists = s%3 != 0
				if ch.PrevExists {
					ch.Prev = accountEnvelopeBytes(rng)
				}
			} else {
				key := make([]byte, 32)
				_, _ = rng.Read(key)
				ch.Key = key
				ch.NextExists = true
				ch.Next = valueFn(rng)
				ch.PrevExists = s%2 == 0 // half are slot updates, half fresh writes
				if ch.PrevExists {
					ch.Prev = valueFn(rng)
				}
			}
			enc, err := encodeStateDomainChangeRecord(ch)
			if err != nil {
				t.Fatal(err)
			}
			out = append(out, enc)
		}
	}
	return out
}

// accountEnvelopeBytes fakes a protobuf account: a handful of varint fields with
// stable tags (compressible across records) plus a couple of high-entropy bytes.
func accountEnvelopeBytes(rng *rand.Rand) []byte {
	var b bytes.Buffer
	put := func(tag byte, val uint64) {
		b.WriteByte(tag)
		var tmp [10]byte
		n := binary.PutUvarint(tmp[:], val)
		b.Write(tmp[:n])
	}
	put(0x08, uint64(rng.Intn(1_000_000_000)))         // balance
	put(0x10, uint64(rng.Intn(1_000_000)))             // energy usage
	put(0x18, uint64(rng.Intn(1_000_000)))             // bandwidth usage
	put(0x20, uint64(1_600_000_000+rng.Intn(1000000))) // a timestamp
	put(0x28, uint64(rng.Intn(100)))
	return b.Bytes()
}

func zstdBlockCompress(t *testing.T, records [][]byte, blockSize int) (raw, compressed int) {
	t.Helper()
	enc, err := zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.SpeedDefault))
	if err != nil {
		t.Fatal(err)
	}
	defer enc.Close()
	dec, err := zstd.NewReader(nil)
	if err != nil {
		t.Fatal(err)
	}
	defer dec.Close()
	var blockBuf bytes.Buffer
	flush := func() {
		if blockBuf.Len() == 0 {
			return
		}
		raw += blockBuf.Len()
		out := enc.EncodeAll(blockBuf.Bytes(), nil)
		compressed += len(out)
		// Round-trip sanity: decompress must equal the block.
		back, err := dec.DecodeAll(out, nil)
		if err != nil || !bytes.Equal(back, blockBuf.Bytes()) {
			t.Fatalf("zstd block round-trip failed: err=%v equal=%v", err, bytes.Equal(back, blockBuf.Bytes()))
		}
		blockBuf.Reset()
	}
	for i, r := range records {
		blockBuf.Write(r)
		if (i+1)%blockSize == 0 {
			flush()
		}
	}
	flush()
	return raw, compressed
}

// TestKvKeyDuplicationCost measures the recsplit upside for the .kv accessor: how
// much of the (already zstd-compressed) .kv is the duplicated 32-byte keccak key
// per record, which zstd cannot compress (random) but an Erigon-style recsplit
// .kvi would drop entirely (key→offset via a minimal perfect hash, verified by
// reading the key back from the .seg). This is the go/no-go for whether the large
// recsplit/efII port is worth it on top of the 2.56x already achieved.
func TestKvKeyDuplicationCost(t *testing.T) {
	changes := buildHistoryStructs(400, 50)
	from, to := uint64(9_000_000), uint64(9_000_399)
	normalized := normalizeStateDomainChangesForBinary(changes)
	_, _, accessor, err := encodeStateDomainChangeBinarySegment(from, to, normalized)
	if err != nil {
		t.Fatal(err)
	}
	accessorData, err := encodeStateDomainChangeBinaryAccessor(from, to, accessor)
	if err != nil {
		t.Fatal(err)
	}
	// keyless = just the per-entry ints (txNum, seq, offset, recordIndex) a
	// recsplit-backed accessor would still need; the key is gone.
	var keyless bytes.Buffer
	keyBytes := 0
	for _, e := range accessor {
		var b [32]byte
		binary.BigEndian.PutUint64(b[0:8], e.txNum)
		binary.BigEndian.PutUint64(b[8:16], e.seq)
		binary.BigEndian.PutUint64(b[16:24], e.offset)
		binary.BigEndian.PutUint64(b[24:32], e.recordIndex)
		keyless.Write(b[:])
		keyBytes += len(e.key)
	}
	enc, err := zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.SpeedDefault))
	if err != nil {
		t.Fatal(err)
	}
	defer enc.Close()
	fullC := len(enc.EncodeAll(accessorData, nil))
	keylessC := len(enc.EncodeAll(keyless.Bytes(), nil))
	mphfBytes := len(accessor) * 3 / 8 // ~3 bits/key recsplit MPHF
	recsplitEst := keylessC + mphfBytes
	t.Logf("entries=%d  raw-key-bytes=%d  raw-kv=%d", len(accessor), keyBytes, len(accessorData))
	t.Logf("  .kv zstd (with keys)        = %8d", fullC)
	t.Logf("  .kv zstd (keyless ints)     = %8d  (%.0f%% of full)", keylessC, 100*float64(keylessC)/float64(fullC))
	t.Logf("  recsplit est (keyless+MPHF) = %8d  -> .kv %.2fx smaller", recsplitEst, float64(fullC)/float64(recsplitEst))
	t.Logf("  => keys are ~%.0f%% of the compressed .kv", 100*(1-float64(keylessC)/float64(fullC)))
}

// TestHistoryCompressionRatioGate is the go/no-go measurement (not a pass/fail
// test): it reports the zstd block-compression ratio of real-encoded
// StateDomainChange history records across dataset profiles and block sizes, so
// the per-segment compress/skip decision is made on numbers, not hope. Run with:
//
//	go test ./core/state/snapshots -run TestHistoryCompressionRatioGate -v
func TestHistoryCompressionRatioGate(t *testing.T) {
	const blocks, perBlock = 400, 40
	type ds struct {
		name    string
		records [][]byte
	}
	datasets := []ds{
		{"storage_mixed", buildHistoryCorpus(t, blocks, perBlock, false, mixedValue)},
		{"storage_smallint", buildHistoryCorpus(t, blocks, perBlock, false, func(r *rand.Rand) []byte { return valueBytes(r, smallInt) })},
		{"storage_allhash(pessimistic)", buildHistoryCorpus(t, blocks, perBlock, false, func(r *rand.Rand) []byte { return valueBytes(r, hashLike) })},
		{"account_envelope", buildHistoryCorpus(t, blocks, perBlock, true, nil)},
	}
	t.Logf("%-32s %8s %10s %10s %7s", "dataset/blockSize", "records", "raw", "zstd", "ratio")
	for _, d := range datasets {
		for _, bs := range []int{1, 16, 64, 128} {
			raw, comp := zstdBlockCompress(t, d.records, bs)
			ratio := float64(raw) / float64(comp)
			label := fmt.Sprintf("%s/B=%d", d.name, bs)
			t.Logf("%-32s %8d %10d %10d %6.2fx", label, len(d.records), raw, comp, ratio)
		}
	}
}
