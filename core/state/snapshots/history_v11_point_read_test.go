package snapshots

import (
	"bytes"
	"errors"
	"math"
	"testing"

	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
)

type historyDependencyRejectReader struct{ calls int }

func (r *historyDependencyRejectReader) ReadAt([]byte, int64) (int, error) {
	r.calls++
	return 0, errors.New("unexpected shared dependency read")
}

func TestHistoryV11InlinePointReadDoesNotReadSharedPages(t *testing.T) {
	owner := common.Address{0: common.AddressPrefixMainnet, 20: 1}
	change := &rawdb.StateDomainChange{BlockNum: 1, BlockHash: common.Hash{31: 1}, TxNum: 10, Seq: 1, FlatDomain: rawdb.StateFlatDomainAccountLatest, Owner: owner, PrevExists: true, Prev: []byte{1, 2, 3}}
	data, _, entries, err := encodeStateDomainChangeBinarySegment(10, 10, []*rawdb.StateDomainChange{change})
	if err != nil {
		t.Fatal(err)
	}
	probe := new(historyDependencyRejectReader)
	h := stateDomainChangeBinaryAccessorV6Header{version: 11, fromTxNum: 10, toTxNum: 10, recordCount: 1, txRuns: &historyTxTable{r: probe, recordCount: 1}}
	record := stateDomainChangeBinaryAccessorV6Record{key: stateDomainChangeBinaryAccessorKey(change), postings: 1, inline: true, inlinePosting: stateDomainChangeBinaryAccessorV6Posting{txNum: 10, offset: entries[0].offset, recordIndex: 0}}
	for _, span := range [][2]uint64{{10, 10}, {0, 9}, {11, math.MaxUint64}} {
		calls := 0
		err := iterateStateDomainChangeBinarySegmentByAccessorV7Record(bytes.NewReader(data), uint64(len(data)), probe, h, record, span[0], span[1], func(got *rawdb.StateDomainChange) (bool, error) {
			calls++
			if got.TxNum != 10 || !got.PrevExists || !bytes.Equal(got.Prev, change.Prev) {
				t.Fatal("changed before-image", got)
			}
			return false, nil
		})
		want := 0
		if span[0] == 10 {
			want = 1
		}
		if err != nil || calls != want || probe.calls != 0 {
			t.Fatal(span, calls, probe.calls, err)
		}
	}
	bad := record
	bad.key = bytes.Clone(record.key)
	bad.key[len(bad.key)-1] ^= 1
	if err := iterateStateDomainChangeBinarySegmentByAccessorV7Record(bytes.NewReader(data), uint64(len(data)), probe, h, bad, 10, 10, func(*rawdb.StateDomainChange) (bool, error) { t.Fatal("forged hit"); return false, nil }); err == nil {
		t.Fatal("accepted wrong inline key")
	}
	if err := iterateStateDomainChangeBinarySegmentByAccessorV7Record(bytes.NewReader(data), uint64(len(data)), probe, h, record, 11, 10, nil); err == nil {
		t.Fatal("accepted inverted range")
	}
	if err := iterateStateDomainChangeBinarySegmentByAccessorV7Record(bytes.NewReader(data[:len(data)-1]), uint64(len(data)-1), probe, h, record, 10, 10, nil); err == nil {
		t.Fatal("accepted truncated record")
	}
}

func TestHistoryV11NegativeAndInlineKeepDirectoryAuthentication(t *testing.T) {
	data, expected := buildOwnerAccessorFixture(t, 11, 4200, true)
	h, err := decodeStateDomainChangeBinaryAccessorKeyHeader(bytes.NewReader(data), uint64(len(data)))
	if err != nil || h.txRuns == nil {
		t.Fatal(err)
	}
	bad := bytes.Clone(data)
	bad[h.txRuns.prefix+h.txRuns.dataLen] ^= 1
	for _, key := range [][]byte{{0xff}, expected[1].key} {
		if _, _, _, err := stateDomainChangeBinaryAccessorV7Lookup(bytes.NewReader(bad), uint64(len(bad)), key); err == nil {
			t.Fatal("corrupt directory accepted", key)
		}
	}
	if h.txRuns.cache != nil {
		t.Fatal("header allocated decoded-page cache before page access")
	}
}

func TestHistoryAccessorDecodedKeySlicesRemainIndependent(t *testing.T) {
	for _, version := range []uint32{7, 10, 11} {
		data, _ := buildOwnerAccessorFixture(t, version, 4200, true)
		r := bytes.NewReader(data)
		h, err := decodeStateDomainChangeBinaryAccessorKeyHeader(r, uint64(len(data)))
		if err != nil {
			t.Fatal(err)
		}
		blocks, err := stateDomainChangeBinaryAccessorV6ReadBlockDirectory(r, h)
		if err != nil {
			t.Fatal(err)
		}
		for i, block := range blocks {
			recs, err := stateDomainChangeBinaryAccessorV6ReadBlock(r, uint64(len(data)), h, block, uint32(i*128))
			if err != nil {
				t.Fatal(err)
			}
			for j := 0; j+1 < len(recs); j++ {
				before := bytes.Clone(recs[j+1].key)
				if cap(recs[j].key) != len(recs[j].key) {
					t.Fatal("key exposes neighbouring arena capacity")
				}
				x := append(recs[j].key, 0xff)
				x[len(x)-1] = 0
				if !bytes.Equal(before, recs[j+1].key) {
					t.Fatal("append changed adjacent key")
				}
			}
		}
	}
}
