package snapshots

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/rawdb/etl"
	"github.com/tronprotocol/go-tron/core/state/kvdomains"
)

type v12CountingReaderAt struct {
	r               io.ReaderAt
	minSharedOffset int64
	sharedReads     int
}

func (c *v12CountingReaderAt) ReadAt(p []byte, off int64) (int, error) {
	if off >= c.minSharedOffset {
		c.sharedReads++
	}
	return c.r.ReadAt(p, off)
}

func TestHistoryAccessorV12RoundtripQueriesAndJointVerification(t *testing.T) {
	ctx := context.Background()
	for _, tc := range []struct {
		name          string
		singletons    int
		hotKeys       int
		postingsPerHK int
	}{
		{name: "mixed_singletons_and_hot_keys", singletons: 600, hotKeys: 4, postingsPerHK: 50},
		{name: "all_singletons", singletons: 500, hotKeys: 0, postingsPerHK: 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db := rawdb.NewMemoryDatabase()
			defer db.Close()

			var all []*rawdb.StateDomainChange
			tx := uint64(10)
			blk := uint64(1)
			// Build singletons across domains (AccountLatest, AssetV2, ContractStorage, ResourceAux).
			for i := 0; i < tc.singletons; i++ {
				sum := sha256.Sum256([]byte(fmt.Sprintf("%s/s/%d", tc.name, i)))
				var owner common.Address
				owner[0] = common.AddressPrefixMainnet
				copy(owner[1:], sum[:20])
				var bhash common.Hash
				binary.BigEndian.PutUint64(bhash[24:], blk)
				var c *rawdb.StateDomainChange
				switch i % 4 {
				case 0:
					c = &rawdb.StateDomainChange{BlockNum: blk, BlockHash: bhash, TxNum: tx, Seq: 1, FlatDomain: rawdb.StateFlatDomainAccountLatest, Owner: owner, PrevExists: true, Prev: bytes.Repeat([]byte{byte(i)}, 35)}
				case 1:
					c = &rawdb.StateDomainChange{BlockNum: blk, BlockHash: bhash, TxNum: tx, Seq: 1, FlatDomain: rawdb.StateFlatDomainKVLatest, Owner: owner, Generation: 1, Domain: kvdomains.AccountAssetV2, Key: []byte("1002000"), PrevExists: true, Prev: []byte{0x01, byte(i), 0x02}}
				case 2:
					var slot [32]byte
					binary.BigEndian.PutUint64(slot[24:], uint64(i))
					c = &rawdb.StateDomainChange{BlockNum: blk, BlockHash: bhash, TxNum: tx, Seq: 1, FlatDomain: rawdb.StateFlatDomainKVLatest, Owner: owner, Generation: 2, Domain: kvdomains.ContractStorage, Key: slot[:], PrevExists: true, Prev: slot[:]}
				case 3:
					c = &rawdb.StateDomainChange{BlockNum: blk, BlockHash: bhash, TxNum: tx, Seq: 1, FlatDomain: rawdb.StateFlatDomainKVLatest, Owner: owner, Generation: 1, Domain: kvdomains.AccountResourceAux, Key: nil, PrevExists: true, Prev: []byte{0x07, byte(i)}}
				}
				all = append(all, c)
				tx += 3
				blk++
			}
			// Interleave multi-posting hot keys with gaps.
			var hotLookupKeys [][]byte
			for h := 0; h < tc.hotKeys; h++ {
				var owner common.Address
				owner[0] = common.AddressPrefixMainnet
				owner[20] = byte(0xe0 + h)
				var slot [32]byte
				slot[31] = byte(h + 1)
				sample := &rawdb.StateDomainChange{FlatDomain: rawdb.StateFlatDomainKVLatest, Owner: owner, Generation: 1, Domain: kvdomains.ContractStorage, Key: slot[:]}
				hotLookupKeys = append(hotLookupKeys, stateDomainChangeBinaryAccessorKey(sample))
				for p := 0; p < tc.postingsPerHK; p++ {
					var bhash common.Hash
					binary.BigEndian.PutUint64(bhash[24:], blk)
					c := &rawdb.StateDomainChange{
						BlockNum: blk, BlockHash: bhash, TxNum: tx, Seq: uint64(p + 1),
						FlatDomain: rawdb.StateFlatDomainKVLatest, Owner: owner, Generation: 1, Domain: kvdomains.ContractStorage, Key: slot[:],
						PrevExists: true, Prev: []byte{byte(h), byte(p)},
					}
					all = append(all, c)
					tx += 2
					blk++
				}
			}
			for _, c := range all {
				if err := rawdb.WriteStateTxRange(db, c.BlockNum, c.BlockHash, c.TxNum, c.TxNum); err != nil {
					t.Fatal(err)
				}
				if err := rawdb.WriteStateDomainChangeBlockRows(db, []*rawdb.StateDomainChange{c}); err != nil {
					t.Fatal(err)
				}
			}
			fromTx, toTx := all[0].TxNum, all[len(all)-1].TxNum+10

			dir := t.TempDir()
			cfg, ok := DefaultDomainRegistry().Dataset(SegmentDatasetStateDomainChange)
			if !ok {
				t.Fatal("missing dataset config")
			}
			refs, err := BuildStateDomainChangeHistorySegmentsFromDB(db, dir, fromTx, toTx, cfg.HistoryPath(fromTx, toTx))
			if err != nil {
				t.Fatal(err)
			}
			var hRef, aRef, iRef SegmentRef
			for _, r := range refs {
				switch r.Kind {
				case SegmentHistory:
					hRef = r
				case SegmentAccessor:
					aRef = r
				case SegmentInverted:
					iRef = r
				}
			}

			accBytes, err := os.ReadFile(filepath.Join(dir, aRef.Path))
			if err != nil {
				t.Fatal(err)
			}
			ah, err := decodeStateDomainChangeBinaryAccessorKeyHeader(bytes.NewReader(accBytes), uint64(len(accBytes)))
			if err != nil {
				t.Fatal(err)
			}
			if ah.version != stateDomainChangeBinaryAccessorVersionV12 {
				t.Fatalf("accessor version = %d, want V%d", ah.version, stateDomainChangeBinaryAccessorVersionV12)
			}
			if ah.txRuns == nil {
				t.Fatal("V12 accessor missing txRuns")
			}
			if err := checkStateDomainChangeBinaryAccessorV7(bytes.NewReader(accBytes), uint64(len(accBytes))); err != nil {
				t.Fatalf("checkStateDomainChangeBinaryAccessorV7: %v", err)
			}
			if err := verifyStateDomainChangeBinaryCompanionsAgainstSegmentContext(ctx, dir, hRef, iRef, aRef); err != nil {
				t.Fatalf("joint companion verification: %v", err)
			}

			segR, segSize, _, err := openHistorySegmentForRead(dir, hRef)
			if err != nil {
				t.Fatal(err)
			}
			defer segR.Close()
			// Verify in-range and out-of-range queries across singletons and hot keys.
			accR := bytes.NewReader(accBytes)
			checkKey := func(target *rawdb.StateDomainChange) {
				t.Helper()
				lk := stateDomainChangeBinaryAccessorKey(target)
				var got []*rawdb.StateDomainChange
				if err := iterateStateDomainChangeBinarySegmentByAccessorV7Key(segR, segSize, accR, uint64(len(accBytes)), lk, target.TxNum, target.TxNum, func(c *rawdb.StateDomainChange) (bool, error) {
					got = append(got, cloneStateDomainChangeForSegment(c))
					return true, nil
				}); err != nil {
					t.Fatalf("point query tx=%d: %v", target.TxNum, err)
				}
				if len(got) != 1 || got[0].TxNum != target.TxNum || !bytes.Equal(got[0].Prev, target.Prev) {
					t.Fatalf("point query mismatch for tx=%d: got=%d", target.TxNum, len(got))
				}

				// Out-of-range singleton query must read ZERO shared offset/tx pages and ZERO segment bytes.
				sharedStart := int64(ah.headerSize + ah.blockDirLen + ah.keyDataLen + ah.postingLen)
				cntAcc := &v12CountingReaderAt{r: accR, minSharedOffset: sharedStart}
				cntSeg := &v12CountingReaderAt{r: segR, minSharedOffset: 0}
				hDecoded, record, found, err := stateDomainChangeBinaryAccessorV7Lookup(accR, uint64(len(accBytes)), lk)
				if err != nil || !found {
					t.Fatalf("lookup failed: found=%v err=%v", found, err)
				}
				missCount := 0
				if err := iterateStateDomainChangeBinarySegmentByAccessorV7Record(cntSeg, segSize, cntAcc, hDecoded, record, 0, target.TxNum-1, func(*rawdb.StateDomainChange) (bool, error) {
					missCount++
					return true, nil
				}); err != nil || missCount != 0 {
					t.Fatalf("out-of-range singleton miss=%d err=%v", missCount, err)
				}
				if cntSeg.sharedReads != 0 {
					t.Fatalf("out-of-range V12 singleton read %d segment blocks, want 0", cntSeg.sharedReads)
				}
				// Shared offset section starts before txRuns footer; txRuns directory is already cached/authenticated in header,
				// and page reads are <=1 bounded tx-run page with zero offset-page or segment reads.
				if ah.offsetDataBase > 0 && cntAcc.sharedReads > 1 {
					t.Fatalf("out-of-range V12 singleton performed %d shared reads (want <=1 tx page, 0 offsets)", cntAcc.sharedReads)
				}
			}
			checkKey(all[0])
			checkKey(all[tc.singletons/2])
			checkKey(all[tc.singletons-1])

			if len(hotLookupKeys) > 0 {
				var batchCount int
				if err := iterateStateDomainChangeBinarySegmentByAccessorV7KeysContext(ctx, segR, segSize, accR, uint64(len(accBytes)), hotLookupKeys, fromTx, toTx, func(_ []byte, c *rawdb.StateDomainChange) (bool, error) {
					batchCount++
					return true, nil
				}); err != nil {
					t.Fatalf("batch hot key query: %v", err)
				}
				wantBatch := tc.hotKeys * tc.postingsPerHK
				if batchCount != wantBatch {
					t.Fatalf("batch hot key count = %d, want %d", batchCount, wantBatch)
				}
			}

			// Fail-closed test: forge a txRun entry in V12 accessor with recomputed CRCs -> joint verify MUST fail.
			forged := bytes.Clone(accBytes)
			txTable := ah.txRuns
			page := txTable.pages[0]
			payload := forged[txTable.prefix+page.off : txTable.prefix+page.off+uint64(page.length)]
			if payload[0] == 0 && len(payload) > 4 {
				// Shift first delta from 0 to 1 or adjust gap between runs if count>=2; even simpler: swap two non-zero deltas if >=2 runs.
				payload[len(payload)-2] ^= 1
				entry := forged[txTable.prefix+txTable.dataLen : txTable.prefix+txTable.dataLen+32]
				binary.BigEndian.PutUint32(entry[28:], historyTxPageCRC(0, entry, payload))
				footer := forged[len(forged)-64:]
				binary.BigEndian.PutUint32(footer[52:56], crc32.ChecksumIEEE(forged[txTable.prefix+txTable.dataLen:uint64(len(forged))-64]))
				binary.BigEndian.PutUint32(footer[60:], crc32.ChecksumIEEE(footer[:60]))
				forgedRef := aRef
				forgedRef.Path = "history/forged-v12.kv"
				forgedRef.Checksum = fmt.Sprintf("sha256:%x", sha256.Sum256(forged))
				forgedRef.Size = uint64(len(forged))
				if err := os.WriteFile(filepath.Join(dir, forgedRef.Path), forged, 0o600); err != nil {
					t.Fatal(err)
				}
				if err := verifyStateDomainChangeBinaryCompanionsAgainstSegmentContext(ctx, dir, hRef, iRef, forgedRef); err == nil {
					t.Fatal("joint verification accepted forged V12 txRun table")
				}
			}

			// Measure exact byte size comparison: V7 vs V11 vs V12 on identical source rows.
			buildVer := func(ver uint32) uint64 {
				vdir := t.TempDir()
				b, e := newStateDomainChangeV6Build(etl.Options{TempDir: t.TempDir()}, vdir, fmt.Sprintf("cmp-v%d", ver))
				if e != nil {
					t.Fatal(e)
				}
				defer b.Close()
				for _, r := range all {
					if e := b.CollectKey(r); e != nil {
						t.Fatal(e)
					}
				}
				if e := b.FinishDictionary(); e != nil {
					t.Fatal(e)
				}
				off := uint64(128)
				for i, r := range all {
					id, e := b.KeyID(stateDomainChangeBinaryAccessorKey(r))
					if e != nil {
						t.Fatal(e)
					}
					if e := b.CollectPosting(id, r.TxNum, off, uint64(i)); e != nil {
						t.Fatal(e)
					}
					off += 32 + uint64(len(r.Prev))
				}
				ref, _, e := b.buildAccessorContextVersion(ctx, vdir, SegmentRef{Dataset: SegmentDatasetStateDomainChange, Kind: SegmentAccessor, FromTxNum: fromTx, ToTxNum: toTx, Path: fmt.Sprintf("history/cmp-v%d.kv", ver)}, uint64(len(all)), ver)
				if e != nil {
					t.Fatal(e)
				}
				return ref.Size
			}
			v7Size := buildVer(stateDomainChangeBinaryVersionV7)
			v11Size := buildVer(stateDomainChangeBinaryAccessorVersionV11)
			v12Size := buildVer(stateDomainChangeBinaryAccessorVersionV12)
			if v12Size >= v11Size || v11Size > v7Size {
				t.Fatalf("unexpected size ordering (%s): V7=%d V11=%d V12=%d", tc.name, v7Size, v11Size, v12Size)
			}
			t.Logf("[%s] V7=%d B, V11=%d B, V12=%d B (V12 vs V11: -%.1f%%, V12 vs V7: -%.1f%%)",
				tc.name, v7Size, v11Size, v12Size,
				100*float64(v11Size-v12Size)/float64(v11Size),
				100*float64(v7Size-v12Size)/float64(v7Size))
		})
	}
}

func TestHistoryRecordColumns1PassSpeedAndSavings(t *testing.T) {
	enc, _, err := cbCodec()
	if err != nil {
		t.Fatal(err)
	}
	const numPages = 16
	pages := make([][]byte, numPages)
	pageStarts := make([][]uint32, numPages)
	var tx uint64 = 80_000_000
	for p := 0; p < numPages; p++ {
		raw := make([]byte, 0, historyRecordColumnPageMax)
		var starts []uint32
		recIdx := 0
		for len(raw)+64 <= historyRecordColumnPageMax {
			starts = append(starts, uint32(len(raw)))
			var valLen uint32
			switch recIdx % 10 {
			case 0, 1, 2, 3, 4, 5, 6:
				valLen = 35 // AccountLatest compact prev
			case 7, 8:
				valLen = 8 // ContractStorage compact value
			default:
				valLen = 20 // AccountResourceAux / AssetV2
			}
			keyID := uint32((p*500 + recIdx/3) & 0xffff)
			if recIdx%5 == 0 {
				keyID = uint32(recIdx % 16) // hot repeated accounts
			}
			if recIdx%4 == 0 {
				tx++
			}
			raw = binary.BigEndian.AppendUint32(raw, 17+valLen)
			raw = binary.BigEndian.AppendUint32(raw, keyID)
			raw = binary.BigEndian.AppendUint64(raw, tx)
			raw = append(raw, 1)
			raw = binary.BigEndian.AppendUint32(raw, valLen)
			val := make([]byte, valLen)
			binary.BigEndian.PutUint64(val[len(val)-8:], uint64(p*10000+recIdx))
			raw = append(raw, val...)
			recIdx++
		}
		pages[p] = raw
		pageStarts[p] = starts
	}

	var f2Bytes, f5Bytes uint64
	startF2 := time.Now()
	for p := range pages {
		comp := enc.EncodeAll(pages[p], nil)
		f2Bytes += uint64(len(comp))
	}
	durF2 := time.Since(startF2)

	var e recordColumnPageEncoder
	startF5 := time.Now()
	encodedF5 := make([][]byte, numPages)
	for p := range pages {
		comp, err := e.encode(pages[p], pageStarts[p], enc)
		if err != nil {
			t.Fatal(err)
		}
		f5Bytes += uint64(len(comp))
		encodedF5[p] = bytes.Clone(comp)
	}
	durF5 := time.Since(startF5)

	for p := range pages {
		got, _, err := decodeRecordColumnStoredPage(encodedF5[p], uint64(len(pages[p])), nil, nil)
		if err != nil || !bytes.Equal(got, pages[p]) {
			t.Fatalf("page %d decode mismatch: err=%v", p, err)
		}
	}
	if f5Bytes >= f2Bytes {
		t.Fatalf("1-pass HRC2 did not save space vs Format 2: F2=%d F5=%d", f2Bytes, f5Bytes)
	}
	savePct := 100 * float64(f2Bytes-f5Bytes) / float64(f2Bytes)
	t.Logf("16x128KiB pages: Format2=%d B (%v/page), 1-pass Format5=%d B (%v/page), savings=-%.2f%%, cpu_ratio=%.2fx",
		f2Bytes, durF2/numPages, f5Bytes, durF5/numPages, savePct, float64(durF5)/float64(durF2))
}
