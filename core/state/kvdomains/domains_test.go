package kvdomains

import "testing"

func TestRegisteredDomains(t *testing.T) {
	registered := []KVDomain{
		SystemDynamicProperty, SystemWitnessSchedule, SystemProposal,
		SystemForkVote, SystemAsset, SystemExchange, SystemDelegation,
		SystemAccountIndex, ContractStorage, ContractMetadata, ContractABI,
		ContractRuntimeState, AccountLocalIndex, AccountPermissionAux,
		WitnessCapsule, WitnessVoteState,
	}
	for _, d := range registered {
		if !IsRegistered(d) {
			t.Fatalf("domain %#04x should be registered", uint16(d))
		}
		if Name(d) == "" {
			t.Fatalf("domain %#04x should have a name", uint16(d))
		}
	}
}

func TestUnregisteredDomain(t *testing.T) {
	if IsRegistered(KVDomain(0x0099)) {
		t.Fatal("0x0099 must not be registered")
	}
	if IsRegistered(KVDomain(0xABCD)) {
		t.Fatal("0xABCD must not be registered")
	}
}

func TestNoDuplicateIDsOrNames(t *testing.T) {
	seenName := map[string]bool{}
	for d, name := range registry {
		if seenName[name] {
			t.Fatalf("duplicate domain name %q", name)
		}
		seenName[name] = true
		_ = d
	}
}
