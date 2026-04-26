package p2p

import (
	"testing"
	"time"
)

func TestRateLimiter_UnregisteredAlwaysAllowed(t *testing.T) {
	rl := &RateLimiter{buckets: make(map[byte]*tokenBucket)}
	for i := 0; i < 100; i++ {
		if !rl.Allow(0x42) {
			t.Fatal("unregistered type should always be allowed")
		}
	}
}

func TestRateLimiter_AllowsUpToRate(t *testing.T) {
	rl := &RateLimiter{buckets: make(map[byte]*tokenBucket)}
	rl.register(0x01, 3.0) // 3 permits/s

	// First 3 permits should succeed (bucket starts full at rate=3)
	allowed := 0
	for i := 0; i < 3; i++ {
		if rl.Allow(0x01) {
			allowed++
		}
	}
	if allowed != 3 {
		t.Fatalf("expected 3 allows, got %d", allowed)
	}
	// 4th should be throttled
	if rl.Allow(0x01) {
		t.Fatal("4th allow should be throttled")
	}
}

func TestRateLimiter_RefillsOverTime(t *testing.T) {
	rl := &RateLimiter{buckets: make(map[byte]*tokenBucket)}
	rl.register(0x01, 3.0)

	// Drain the bucket
	for i := 0; i < 3; i++ {
		rl.Allow(0x01)
	}
	if rl.Allow(0x01) {
		t.Fatal("should be throttled after draining")
	}

	// Wait for ~1 token to refill (1s × 3/s = 3 tokens, but we only need 1)
	time.Sleep(400 * time.Millisecond)
	if !rl.Allow(0x01) {
		t.Fatal("should have refilled at least 1 token after 400ms @ 3/s")
	}
}

func TestRateLimiter_DefaultRatesMatchJavaTron(t *testing.T) {
	rl := NewRateLimiter()

	// MsgSyncBlockChain: 3/s — first 3 pass, 4th fails
	for i := 0; i < 3; i++ {
		if !rl.Allow(MsgSyncBlockChain) {
			t.Fatalf("SyncBlockChain: attempt %d should be allowed", i+1)
		}
	}
	if rl.Allow(MsgSyncBlockChain) {
		t.Fatal("SyncBlockChain: 4th attempt should be throttled")
	}

	// MsgFetchInvData: 3/s
	rl2 := NewRateLimiter()
	for i := 0; i < 3; i++ {
		if !rl2.Allow(MsgFetchInvData) {
			t.Fatalf("FetchInvData: attempt %d should be allowed", i+1)
		}
	}
	if rl2.Allow(MsgFetchInvData) {
		t.Fatal("FetchInvData: 4th attempt should be throttled")
	}

	// MsgDisconnect: 1/s — first 1 passes, 2nd fails
	rl3 := NewRateLimiter()
	if !rl3.Allow(MsgDisconnect) {
		t.Fatal("Disconnect: first attempt should be allowed")
	}
	if rl3.Allow(MsgDisconnect) {
		t.Fatal("Disconnect: 2nd attempt should be throttled")
	}
}
