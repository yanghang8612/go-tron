package rawdb

import "testing"

// PriceKey is the pure GCD-normalized price encoding shared by the market
// actuators and the rooted SystemMarket KV store. The market records moved out
// of rawdb (into core/state), but this normalization helper stays here.
func TestPriceKey_Normalization(t *testing.T) {
	// 200/100 should normalize to 2/1
	pk200_100 := PriceKey(200, 100)
	pk2_1 := PriceKey(2, 1)
	if pk200_100 != pk2_1 {
		t.Fatalf("PriceKey(200,100) != PriceKey(2,1): %v vs %v", pk200_100, pk2_1)
	}

	// 6/4 should normalize to 3/2
	pk6_4 := PriceKey(6, 4)
	pk3_2 := PriceKey(3, 2)
	if pk6_4 != pk3_2 {
		t.Fatalf("PriceKey(6,4) != PriceKey(3,2): %v vs %v", pk6_4, pk3_2)
	}

	// 2/1 should not equal 3/2
	if pk2_1 == pk3_2 {
		t.Fatalf("PriceKey(2,1) should not equal PriceKey(3,2)")
	}
}
