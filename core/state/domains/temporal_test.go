package domains

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"testing"

	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/state/kvdomains"
)

type benchmarkDomainSink struct{}

func (benchmarkDomainSink) DomainPut(common.Address, kvdomains.KVDomain, []byte, []byte) error {
	return nil
}
func (benchmarkDomainSink) DomainDel(common.Address, kvdomains.KVDomain, []byte) error { return nil }
func (benchmarkDomainSink) DomainDelPrefix(common.Address, kvdomains.KVDomain, []byte) error {
	return nil
}

type benchmarkCommitmentRecorder struct{ count int }

func (*benchmarkCommitmentRecorder) SeekCommitment(context.Context) (uint64, uint64, error) {
	return 0, 0, nil
}
func (*benchmarkCommitmentRecorder) ComputeCommitment(context.Context, uint64, uint64) (common.Hash, error) {
	return common.Hash{}, nil
}
func (r *benchmarkCommitmentRecorder) RecordCommitmentMutations(_ context.Context, mutations []Mutation) error {
	r.count += len(mutations)
	return nil
}

var sharedDomainMutationBenchmarkSink int

func BenchmarkSharedDomainTxMutationBatch(b *testing.B) {
	benchmarkSharedDomainTxMutationBatch(b, false)
}

func BenchmarkSharedDomainTxOwnedMutationBatch(b *testing.B) {
	benchmarkSharedDomainTxMutationBatch(b, true)
}

func benchmarkSharedDomainTxMutationBatch(b *testing.B, owned bool) {
	const mutationsPerBatch = 256
	owner := testAddress(0x7a)
	keys := make([][]byte, mutationsPerBatch)
	values := make([][]byte, mutationsPerBatch)
	for i := range mutationsPerBatch {
		keys[i] = []byte(fmt.Sprintf("storage-slot-%03d", i))
		values[i] = []byte(fmt.Sprintf("value-%03d-value-%03d", i, i))
	}
	recorder := new(benchmarkCommitmentRecorder)
	tx := NewSharedDomainTx(SharedDomainTxConfig{
		Writer:     benchmarkDomainSink{},
		Commitment: recorder,
	})
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		for i := range mutationsPerBatch {
			var err error
			if owned {
				err = tx.DomainPutOwned(owner, kvdomains.ContractStorage, keys[i], values[i])
			} else {
				err = tx.DomainPut(owner, kvdomains.ContractStorage, keys[i], values[i])
			}
			if err != nil {
				b.Fatal(err)
			}
		}
		if err := tx.Flush(context.Background()); err != nil {
			b.Fatal(err)
		}
	}
	sharedDomainMutationBenchmarkSink = recorder.count
}

func TestSharedDomainTxStagesLatestAndFlushes(t *testing.T) {
	owner := testAddress(0x41)
	latest := NewMemoryStore()
	if err := latest.DomainPut(owner, kvdomains.SystemDynamicProperty, []byte("k"), []byte("parent")); err != nil {
		t.Fatal(err)
	}

	tx := NewSharedDomainTx(SharedDomainTxConfig{Latest: latest, Writer: latest})
	tx.SetTxNum(12)
	if got := tx.TxNum(); got != 12 {
		t.Fatalf("TxNum = %d, want 12", got)
	}

	got, ok, err := tx.GetLatest(owner, kvdomains.SystemDynamicProperty, []byte("k"))
	if err != nil || !ok || string(got) != "parent" {
		t.Fatalf("parent read-through = %q ok=%v err=%v", got, ok, err)
	}
	if err := tx.DomainPut(owner, kvdomains.SystemDynamicProperty, []byte("k"), []byte("overlay")); err != nil {
		t.Fatal(err)
	}
	got, ok, err = tx.GetLatest(owner, kvdomains.SystemDynamicProperty, []byte("k"))
	if err != nil || !ok || string(got) != "overlay" {
		t.Fatalf("overlay latest = %q ok=%v err=%v", got, ok, err)
	}
	got, ok, err = latest.GetLatest(owner, kvdomains.SystemDynamicProperty, []byte("k"))
	if err != nil || !ok || string(got) != "parent" {
		t.Fatalf("latest should not change before flush = %q ok=%v err=%v", got, ok, err)
	}

	if err := tx.Flush(context.Background()); err != nil {
		t.Fatal(err)
	}
	got, ok, err = latest.GetLatest(owner, kvdomains.SystemDynamicProperty, []byte("k"))
	if err != nil || !ok || string(got) != "overlay" {
		t.Fatalf("flushed latest = %q ok=%v err=%v", got, ok, err)
	}
	if len(tx.Mutations()) != 0 {
		t.Fatalf("mutations left after flush: %+v", tx.Mutations())
	}
}

func TestSharedDomainTxOwnedMutationKeepsObservableCopiesIsolated(t *testing.T) {
	owner := testAddress(0x47)
	latest := NewMemoryStore()
	key := []byte("owned-key")
	value := []byte("owned-value")
	tx := NewSharedDomainTx(SharedDomainTxConfig{
		Latest: latest,
		Writer: latest,
		Hooks: Hooks{OnMutation: func(m Mutation) {
			m.Key[0] = 'x'
			m.Value[0] = 'x'
		}},
	})
	if err := tx.DomainPutOwned(owner, kvdomains.SystemDynamicProperty, key, value); err != nil {
		t.Fatal(err)
	}
	mutations := tx.Mutations()
	mutations[0].Key[0] = 'y'
	mutations[0].Value[0] = 'y'
	got, ok, err := tx.GetLatest(owner, kvdomains.SystemDynamicProperty, key)
	if err != nil || !ok || string(got) != "owned-value" {
		t.Fatalf("owned overlay read = %q ok=%v err=%v", got, ok, err)
	}
	if err := tx.Flush(context.Background()); err != nil {
		t.Fatal(err)
	}
	got, ok, err = latest.GetLatest(owner, kvdomains.SystemDynamicProperty, key)
	if err != nil || !ok || string(got) != "owned-value" {
		t.Fatalf("owned flushed value = %q ok=%v err=%v", got, ok, err)
	}
}

func TestSharedDomainTxOwnedDeleteFlushes(t *testing.T) {
	owner := testAddress(0x48)
	latest := NewMemoryStore()
	key := []byte("owned-delete")
	if err := latest.DomainPut(owner, kvdomains.SystemReward, key, []byte("value")); err != nil {
		t.Fatal(err)
	}
	tx := NewSharedDomainTx(SharedDomainTxConfig{Latest: latest, Writer: latest})
	if err := tx.DomainDelOwned(owner, kvdomains.SystemReward, key); err != nil {
		t.Fatal(err)
	}
	if _, ok, err := tx.GetLatest(owner, kvdomains.SystemReward, key); err != nil || ok {
		t.Fatalf("owned delete overlay ok=%v err=%v", ok, err)
	}
	if err := tx.Flush(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, ok, err := latest.GetLatest(owner, kvdomains.SystemReward, key); err != nil || ok {
		t.Fatalf("owned delete flush ok=%v err=%v", ok, err)
	}
}

func TestSharedDomainTxDelegatesHistoryAndCommitment(t *testing.T) {
	owner := testAddress(0x42)
	history := fakeAsOfReader{
		key(owner, kvdomains.SystemReward, []byte("cycle"), 7): []byte("historical"),
	}
	commitment := &fakeCommitmentProcessor{
		seekTxNum:    100,
		seekBlockNum: 9,
		root:         common.BytesToHash([]byte("root")),
	}
	tx := NewSharedDomainTx(SharedDomainTxConfig{
		Latest:     NewMemoryStore(),
		Writer:     NewMemoryStore(),
		History:    history,
		Commitment: commitment,
	})

	got, ok, err := tx.GetAsOf(owner, kvdomains.SystemReward, []byte("cycle"), 7)
	if err != nil || !ok || string(got) != "historical" {
		t.Fatalf("history = %q ok=%v err=%v", got, ok, err)
	}
	txNum, blockNum, err := tx.SeekCommitment(context.Background())
	if err != nil || txNum != 100 || blockNum != 9 {
		t.Fatalf("seek commitment tx=%d block=%d err=%v", txNum, blockNum, err)
	}
	root, err := tx.ComputeCommitment(context.Background(), 10, 101)
	if err != nil || root != commitment.root {
		t.Fatalf("compute root = %x err=%v", root, err)
	}
	if commitment.computeBlockNum != 10 || commitment.computeTxNum != 101 {
		t.Fatalf("compute args block=%d tx=%d", commitment.computeBlockNum, commitment.computeTxNum)
	}
}

func TestSharedDomainTxCloseRejectsFutureWork(t *testing.T) {
	tx := NewSharedDomainTx(SharedDomainTxConfig{Latest: NewMemoryStore(), Writer: NewMemoryStore()})
	if err := tx.Close(); err != nil {
		t.Fatal(err)
	}
	if err := tx.DomainPut(testAddress(0x43), kvdomains.SystemReward, []byte("k"), []byte("v")); !errors.Is(err, ErrTemporalTxClosed) {
		t.Fatalf("DomainPut after close err = %v, want %v", err, ErrTemporalTxClosed)
	}
	if err := tx.DomainPutOwned(testAddress(0x43), kvdomains.SystemReward, []byte("k"), []byte("v")); !errors.Is(err, ErrTemporalTxClosed) {
		t.Fatalf("DomainPutOwned after close err = %v, want %v", err, ErrTemporalTxClosed)
	}
	if err := tx.DomainDelOwned(testAddress(0x43), kvdomains.SystemReward, []byte("k")); !errors.Is(err, ErrTemporalTxClosed) {
		t.Fatalf("DomainDelOwned after close err = %v, want %v", err, ErrTemporalTxClosed)
	}
	if err := tx.Flush(context.Background()); !errors.Is(err, ErrTemporalTxClosed) {
		t.Fatalf("Flush after close err = %v, want %v", err, ErrTemporalTxClosed)
	}
}

func TestSharedDomainTxRequiresCommitmentProcessor(t *testing.T) {
	tx := NewSharedDomainTx(SharedDomainTxConfig{Latest: NewMemoryStore(), Writer: NewMemoryStore()})
	if _, _, err := tx.SeekCommitment(context.Background()); !errors.Is(err, ErrNilCommitmentProcessor) {
		t.Fatalf("SeekCommitment err = %v, want %v", err, ErrNilCommitmentProcessor)
	}
	if _, err := tx.ComputeCommitment(context.Background(), 1, 2); !errors.Is(err, ErrNilCommitmentProcessor) {
		t.Fatalf("ComputeCommitment err = %v, want %v", err, ErrNilCommitmentProcessor)
	}
}

type fakeAsOfReader map[string][]byte

func (r fakeAsOfReader) GetAsOf(owner common.Address, domain kvdomains.KVDomain, k []byte, txNum uint64) ([]byte, bool, error) {
	v, ok := r[key(owner, domain, k, txNum)]
	return append([]byte(nil), v...), ok, nil
}

type fakeCommitmentProcessor struct {
	seekTxNum       uint64
	seekBlockNum    uint64
	root            common.Hash
	computeBlockNum uint64
	computeTxNum    uint64
}

func (p *fakeCommitmentProcessor) SeekCommitment(context.Context) (uint64, uint64, error) {
	return p.seekTxNum, p.seekBlockNum, nil
}

func (p *fakeCommitmentProcessor) ComputeCommitment(_ context.Context, blockNum, txNum uint64) (common.Hash, error) {
	p.computeBlockNum = blockNum
	p.computeTxNum = txNum
	return p.root, nil
}

func key(owner common.Address, domain kvdomains.KVDomain, k []byte, txNum uint64) string {
	var suffix [10]byte
	binary.BigEndian.PutUint16(suffix[:2], uint16(domain))
	binary.BigEndian.PutUint64(suffix[2:], txNum)
	return string(owner.Bytes()) + string(suffix[:]) + string(k)
}
