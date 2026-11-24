package vm

import (
	"errors"
	"testing"

	tcommon "github.com/tronprotocol/go-tron/common"
)

func TestLegacyStorageCommitUsesJavaOrderForResolvedRows(t *testing.T) {
	// Stub the physical resolver: no relationship between these ordinary
	// logical words and the fixture row is required or used.
	row := tcommon.Hash{0: 42}
	first, second := tcommon.Hash{31: 1}, tcommon.Hash{31: 2}
	value := tcommon.Hash{31: 3}
	for _, tc := range []struct {
		name                          string
		firstValue, secondValue, want tcommon.Hash
	}{
		{"delete last", tcommon.Hash{}, value, tcommon.Hash{}},
		{"put last", value, tcommon.Hash{}, value},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cache := newLegacyStorageCache()
			cache.put(first, legacyStorageRow{key: row, value: tc.firstValue, dirty: true})
			cache.put(second, legacyStorageRow{key: row, value: tc.secondValue, dirty: true})
			var got tcommon.Hash
			var writes int
			err := cache.commit(func(write legacyStorageRow) error {
				if write.key != row {
					t.Fatal("unexpected physical key")
				}
				writes++
				got = write.value
				return nil
			})
			// For these words Java HashMap visits word 2 before word 1.
			if err != nil || writes != 2 || got != tc.want {
				t.Fatalf("writes=%d, final row=%x, want=%x, error=%v", writes, got, tc.want, err)
			}
		})
	}
}

func TestLegacyStorageChildCacheAndOuterSnapshot(t *testing.T) {
	tvm := newTestEVM(t)
	addr := tcommon.Address{0: 0x41, 20: 5}
	slot := tcommon.Hash{31: 1}
	first, second := tcommon.Hash{31: 3}, tcommon.Hash{31: 4}
	s := tvm.StateDB
	s.GetOrCreateAccount(addr)
	physical := s.LegacyTVMStorageRowKey(addr, slot)
	if err := s.WriteLegacyTVMStorageRow(addr, physical, first, nil); err != nil {
		t.Fatal(err)
	}
	checkpoint := s.Snapshot()
	var outerErr error
	finish := tvm.enterLegacyStorageFrame(checkpoint, &outerErr)
	if value, exists, err := tvm.readLegacyStorage(addr, slot); err != nil || !exists || value != first {
		t.Fatalf("root read: %x exists=%v err=%v", value, exists, err)
	}

	childErr := errors.New("child reverted")
	finishChild := tvm.enterLegacyStorageFrame(s.Snapshot(), &childErr)
	tvm.writeLegacyStorage(addr, slot, tcommon.Hash{})
	if value, exists, err := tvm.readLegacyStorage(addr, slot); err != nil || !exists || value != (tcommon.Hash{}) {
		t.Fatalf("child zero must remain present: %x exists=%v err=%v", value, exists, err)
	}
	finishChild()
	if value, _, _ := tvm.readLegacyStorage(addr, slot); value != first {
		t.Fatalf("reverted child changed parent cache: %x", value)
	}

	childErr = nil
	finishChild = tvm.enterLegacyStorageFrame(s.Snapshot(), &childErr)
	tvm.writeLegacyStorage(addr, slot, second)
	finishChild()
	if got := s.GetState(addr, slot); got != first {
		t.Fatalf("child published before outer commit: %x", got)
	}
	finish()
	if got := s.GetState(addr, slot); outerErr != nil || got != second {
		t.Fatalf("outer publication = %x, error=%v", got, outerErr)
	}
	s.RevertToSnapshot(checkpoint)
	if got := s.GetState(addr, slot); got != first {
		t.Fatalf("state snapshot did not restore physical row: %x", got)
	}
}
