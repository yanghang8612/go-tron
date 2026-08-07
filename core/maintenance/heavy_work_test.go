package maintenance

import "testing"

func TestHeavyWorkGateIsNonBlockingAndReleaseIsIdempotent(t *testing.T) {
	gate := NewHeavyWorkGate()
	release, ok := gate.TryAcquire()
	if !ok {
		t.Fatal("first acquisition failed")
	}
	if _, ok := gate.TryAcquire(); ok {
		t.Fatal("concurrent acquisition succeeded")
	}
	release()
	release()
	if releaseAgain, ok := gate.TryAcquire(); !ok {
		t.Fatal("acquisition after release failed")
	} else {
		releaseAgain()
	}
}

func TestNilHeavyWorkGateIsUnlimited(t *testing.T) {
	var gate *HeavyWorkGate
	release, ok := gate.TryAcquire()
	if !ok {
		t.Fatal("nil gate rejected work")
	}
	release()
}
