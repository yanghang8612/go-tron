package dpos

import (
	"testing"

	"github.com/tronprotocol/go-tron/params"
)

func TestAbsoluteSlot(t *testing.T) {
	genesisTime := int64(0)
	tests := []struct {
		time int64
		want int64
	}{
		{0, 0},
		{3000, 1},
		{6000, 2},
		{9000, 3},
		{2999, 0},
		{3001, 1},
		{90000, 30},
	}
	for _, tt := range tests {
		got := AbsoluteSlot(tt.time, genesisTime)
		if got != tt.want {
			t.Errorf("AbsoluteSlot(%d, %d) = %d, want %d", tt.time, genesisTime, got, tt.want)
		}
	}
}

func TestSlotTime(t *testing.T) {
	genesisTime := int64(0)
	headTime := int64(0)
	got := SlotTime(1, headTime, genesisTime, false, 0)
	if got != 3000 {
		t.Fatalf("expected 3000, got %d", got)
	}
	got = SlotTime(2, headTime, genesisTime, false, 0)
	if got != 6000 {
		t.Fatalf("expected 6000, got %d", got)
	}
}

func TestSlotTimeAligned(t *testing.T) {
	genesisTime := int64(0)
	headTime := int64(10000)
	got := SlotTime(1, headTime, genesisTime, false, 0)
	if got != 12000 {
		t.Fatalf("expected 12000, got %d", got)
	}
}

func TestSlotForTime(t *testing.T) {
	genesisTime := int64(0)
	headTime := int64(0)
	got := SlotForTime(3000, headTime, genesisTime, false, 0)
	if got != 1 {
		t.Fatalf("expected slot 1, got %d", got)
	}
	got = SlotForTime(6000, headTime, genesisTime, false, 0)
	if got != 2 {
		t.Fatalf("expected slot 2, got %d", got)
	}
}

func TestWitnessIndex(t *testing.T) {
	_ = params.MaxActiveWitnessNum
	tests := []struct {
		absSlot      int64
		witnessCount int
		want         int
	}{
		{0, 27, 0},
		{1, 27, 1},
		{26, 27, 26},
		{27, 27, 0},
		{28, 27, 1},
		{54, 27, 0},
	}
	for _, tt := range tests {
		got := WitnessIndex(tt.absSlot, tt.witnessCount)
		if got != tt.want {
			t.Errorf("WitnessIndex(%d, %d) = %d, want %d", tt.absSlot, tt.witnessCount, got, tt.want)
		}
	}
}
