package blockbuffer

import (
	"bytes"
	"testing"
)

func TestCommitmentParentReadFlightsShareCapturedValueWithLateFollower(t *testing.T) {
	for _, tc := range []struct {
		name  string
		value []byte
	}{
		{name: "present", value: []byte("encoded-branch")},
		{name: "present-empty", value: make([]byte, 0)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var flights commitmentParentReadFlights
			key := []byte("complete-physical-commitment-key")
			hash := layerBloomHashBytes(key)
			leader, isLeader, _, _ := flights.acquire(key, hash, false)
			if !isLeader {
				t.Fatal("first request was not the leader")
			}
			early, earlyLeader, earlyShare, _ := flights.acquire(key, hash, true)
			if earlyLeader || !earlyShare || early != leader {
				t.Fatal("early follower did not join the leader")
			}
			if !flights.capture(leader, tc.value) {
				t.Fatal("leader did not capture for the early follower")
			}
			late, lateLeader, lateShare, _ := flights.acquire(key, hash, false)
			if lateLeader || !lateShare || late != leader {
				t.Fatal("late follower did not reuse the captured value")
			}

			flights.complete(leader, true, nil)
			for name, call := range map[string]*commitmentParentReadFlight{"early": early, "late": late} {
				found, value, err := flights.wait(call)
				if !found || err != nil || !bytes.Equal(value, tc.value) {
					t.Fatalf("%s follower = (%v,%q,%v)", name, found, value, err)
				}
				if len(tc.value) == 0 && value == nil {
					t.Fatalf("%s follower lost present-empty ownership", name)
				}
				flights.release(call)
			}
			flights.release(leader)
		})
	}
}
