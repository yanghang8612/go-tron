package rawdb

import (
	"bytes"
	"slices"
	"testing"

	"github.com/ethereum/go-ethereum/ethdb"
)

func TestStateCommitmentDomainPrefixIsIndependent(t *testing.T) {
	if bytes.Equal(stateCommitmentDomainPrefix, stateCommitmentPrefix) {
		t.Fatal("commitment domain prefix must not reuse checkpoint prefix")
	}
}

func TestStateCommitmentDomainReadWriteCopies(t *testing.T) {
	db := NewMemoryDatabase()
	key := []byte("branch/account/1")
	value := []byte("node-value")

	if _, ok, err := ReadStateCommitmentDomain(db, key); err != nil || ok {
		t.Fatalf("pre-read = ok:%v err:%v", ok, err)
	}
	if err := WriteStateCommitmentDomain(db, key, value); err != nil {
		t.Fatalf("write commitment domain: %v", err)
	}
	key[0] = 'x'
	value[0] = 'x'

	got, ok, err := ReadStateCommitmentDomain(db, []byte("branch/account/1"))
	if err != nil || !ok || !bytes.Equal(got, []byte("node-value")) {
		t.Fatalf("read = %q ok=%v err=%v, want node-value,true,nil", got, ok, err)
	}
	got[0] = 'x'
	reread, ok, err := ReadStateCommitmentDomain(db, []byte("branch/account/1"))
	if err != nil || !ok || !bytes.Equal(reread, []byte("node-value")) {
		t.Fatalf("reread after mutating result = %q ok=%v err=%v", reread, ok, err)
	}
}

func TestStateCommitmentDomainDelete(t *testing.T) {
	db := NewMemoryDatabase()
	if err := WriteStateCommitmentDomain(db, []byte("node/1"), []byte("value")); err != nil {
		t.Fatal(err)
	}
	if err := DeleteStateCommitmentDomain(db, []byte("node/1")); err != nil {
		t.Fatalf("delete commitment domain: %v", err)
	}
	if got, ok, err := ReadStateCommitmentDomain(db, []byte("node/1")); err != nil || ok || got != nil {
		t.Fatalf("read after delete = %q ok=%v err=%v, want nil,false,nil", got, ok, err)
	}
}

func TestStateCommitmentDomainIterateLogicalPrefix(t *testing.T) {
	db := NewMemoryDatabase()
	mustWriteStateCommitmentDomain(t, db, []byte("acct/a"), []byte("1"))
	mustWriteStateCommitmentDomain(t, db, []byte("acct/b"), []byte("2"))
	mustWriteStateCommitmentDomain(t, db, []byte("storage/a"), []byte("x"))

	var rows []string
	err := IterateStateCommitmentDomain(db, []byte("acct/"), func(logicalKey, value []byte) (bool, error) {
		rows = append(rows, string(logicalKey)+"="+string(value))
		logicalKey[0] = 'x'
		value[0] = 'x'
		return true, nil
	})
	if err != nil {
		t.Fatalf("iterate commitment domain: %v", err)
	}
	want := []string{"acct/a=1", "acct/b=2"}
	if !slices.Equal(rows, want) {
		t.Fatalf("rows = %v, want %v", rows, want)
	}
	got, ok, err := ReadStateCommitmentDomain(db, []byte("acct/a"))
	if err != nil || !ok || !bytes.Equal(got, []byte("1")) {
		t.Fatalf("read after mutating callback value = %q ok=%v err=%v", got, ok, err)
	}
}

func TestStateCommitmentDomainEmptyValue(t *testing.T) {
	db := NewMemoryDatabase()
	if err := WriteStateCommitmentDomain(db, []byte("empty"), nil); err != nil {
		t.Fatalf("write empty commitment domain: %v", err)
	}
	got, ok, err := ReadStateCommitmentDomain(db, []byte("empty"))
	if err != nil || !ok || len(got) != 0 {
		t.Fatalf("empty value = %x ok=%v err=%v, want empty,true,nil", got, ok, err)
	}

	var rows []string
	if err := IterateStateCommitmentDomain(db, []byte("empty"), func(logicalKey, value []byte) (bool, error) {
		rows = append(rows, string(logicalKey)+"="+string(value))
		return true, nil
	}); err != nil {
		t.Fatalf("iterate empty value: %v", err)
	}
	if !slices.Equal(rows, []string{"empty="}) {
		t.Fatalf("empty rows = %v", rows)
	}
}

func TestResetMutableStateDeletesStateCommitmentDomain(t *testing.T) {
	db := NewMemoryDatabase()
	if err := WriteStateCommitmentDomain(db, []byte("node/1"), []byte("value")); err != nil {
		t.Fatal(err)
	}
	if err := ResetMutableState(db); err != nil {
		t.Fatalf("reset mutable state: %v", err)
	}
	if got, ok, err := ReadStateCommitmentDomain(db, []byte("node/1")); err != nil || ok || got != nil {
		t.Fatalf("read after reset = %q ok=%v err=%v, want nil,false,nil", got, ok, err)
	}
}

func mustWriteStateCommitmentDomain(t *testing.T, db ethdb.KeyValueWriter, key, value []byte) {
	t.Helper()
	if err := WriteStateCommitmentDomain(db, key, value); err != nil {
		t.Fatal(err)
	}
}
