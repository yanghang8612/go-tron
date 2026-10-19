package state

import (
	"bytes"
	"fmt"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/state/kvdomains"
)

type diagnosticCountHistory struct {
	readerDB
	values int
	bytes int
}

// The fixture is immutable before constructing this adapter.
func (d *diagnosticCountHistory) IsPinnedKeyValueView() bool { return true }
func (d *diagnosticCountHistory) NewIterator(prefix, start []byte) ethdb.Iterator {
	return &diagnosticCountIterator{Iterator:d.readerDB.NewIterator(prefix,start),owner:d}
}
type diagnosticCountIterator struct { ethdb.Iterator; owner *diagnosticCountHistory }
func (i *diagnosticCountIterator) Value() []byte {
	v:=i.Iterator.Value(); i.owner.values++; i.owner.bytes+=len(v); return v
}

func TestDiagnosticColdLatencyMetadataTailWork(t *testing.T) {
	db:=rawdb.NewMemoryDatabase(); defer db.Close()
	owner,noise:=common.Address{0x41,0x6a},common.Address{0x41,0x6b}
	const target,head,generation=uint64(1),uint64(513),uint64(7)
	key,want:=[]byte("meta"),[]byte("unchanged-contract-metadata")
	for _, n:=range []uint64{target,head} {
		if err:=rawdb.WriteStateTxRange(db,n,common.Hash{byte(n)},n,n);err!=nil {t.Fatal(err)}
	}
	if err:=rawdb.WriteStateKVGeneration(db,owner,generation);err!=nil {t.Fatal(err)}
	if err:=rawdb.WriteStateKVLatest(db,owner,generation,kvdomains.ContractMetadata,key,want);err!=nil {t.Fatal(err)}
	if err:=rawdb.WriteStageProgressWithHash(db,rawdb.StageStateHistoryIndex,target,common.Hash{1});err!=nil {t.Fatal(err)}
	for block:=target+1;block<=head;block++ {
		rows:=make([]*rawdb.StateDomainChange,128)
		for n:=range rows {
			rows[n]=&rawdb.StateDomainChange{BlockNum:block,TxNum:block,Seq:uint64(n+1),
				FlatDomain:rawdb.StateFlatDomainKVLatest,Owner:noise,Generation:1,Domain:kvdomains.ContractStorage,
				Key:[]byte(fmt.Sprintf("unrelated-slot-%03d",n)),PrevExists:true,Prev:bytes.Repeat([]byte{byte(n)},32)}
		}
		if err:=rawdb.WriteStateDomainChangeBlockRows(db,rows);err!=nil {t.Fatal(err)}
	}
	for _,mode:=range []string{"generic-generation-walk","known-target-generation-point"} {
		d:=&diagnosticCountHistory{readerDB:db}
		r:=NewPersistentHistoryReaderWithColdHistory(d,nil,head,&keyedColdHistoryStub{})
		if err:=r.SetHotHistoryBlockRange(target,head);err!=nil {t.Fatal(err)}
		start:=time.Now()
		var got []byte;var ok bool;var err error
		if mode=="generic-generation-walk" {got,ok,err=r.readStateAccountKVAsOf(owner,kvdomains.ContractMetadata,key,target,head)} else {got,ok,err=r.readStateKVAsOf(owner,generation,kvdomains.ContractMetadata,key,target,head)}
		elapsed:=time.Since(start)
		if err!=nil || !ok || !bytes.Equal(got,want) {t.Fatalf("%s got=%q ok=%v err=%v",mode,got,ok,err)}
		wantReads:=1024;if mode!="generic-generation-walk" {wantReads=512}
		if d.values!=wantReads {t.Fatalf("%s packed reads=%d want=%d",mode,d.values,wantReads)}
		t.Logf("DIAGNOSTIC mode=%s tail_blocks=512 rows_per_block=128 packed_reads=%d packed_value_bytes=%d milliseconds=%.3f",mode,d.values,d.bytes,float64(elapsed.Nanoseconds())/1e6)
	}
}
