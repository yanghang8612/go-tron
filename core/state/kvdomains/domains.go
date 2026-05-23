// Package kvdomains is the central registry of generic account-KV domain IDs.
// Every consensus-relevant mutable record in the rooted state model is addressed
// by (owner AccountID, domain KVDomain, logical key). Domain IDs MUST be
// registered here; no raw domain constants may be scattered through actuators.
package kvdomains

// KVDomain identifies a logical namespace within an account's generic KV space.
type KVDomain uint16

// Domain groups (see design spec "Generic Account KV"):
//
//	0x0001-0x00ff system/global   0x0100-0x01ff contract
//	0x0200-0x02ff account-local   0x0300-0x03ff witness
//	0x0400-0x04ff governance      0x8000-0xffff test/private/reserved
const (
	SystemDynamicProperty KVDomain = 0x0001
	SystemWitnessSchedule KVDomain = 0x0002
	SystemProposal        KVDomain = 0x0003
	SystemForkVote        KVDomain = 0x0004
	SystemAsset           KVDomain = 0x0005
	SystemExchange        KVDomain = 0x0006
	SystemDelegation      KVDomain = 0x0007
	SystemAccountIndex    KVDomain = 0x0008
	SystemMarket          KVDomain = 0x0009
	SystemReward          KVDomain = 0x000a
	SystemShielded        KVDomain = 0x000b
	SystemForkAux         KVDomain = 0x000c
	SystemPBFT            KVDomain = 0x000d
	SystemTapos           KVDomain = 0x000e
	SystemTrace           KVDomain = 0x000f
	SystemBloom           KVDomain = 0x0010
	SystemCheckpoint      KVDomain = 0x0011

	ContractStorage      KVDomain = 0x0100
	ContractMetadata     KVDomain = 0x0101
	ContractABI          KVDomain = 0x0102
	ContractRuntimeState KVDomain = 0x0103

	AccountLocalIndex    KVDomain = 0x0200
	AccountPermissionAux KVDomain = 0x0201

	WitnessCapsule   KVDomain = 0x0300
	WitnessVoteState KVDomain = 0x0301
)

var registry = map[KVDomain]string{
	SystemDynamicProperty: "SystemDynamicProperty",
	SystemWitnessSchedule: "SystemWitnessSchedule",
	SystemProposal:        "SystemProposal",
	SystemForkVote:        "SystemForkVote",
	SystemAsset:           "SystemAsset",
	SystemExchange:        "SystemExchange",
	SystemDelegation:      "SystemDelegation",
	SystemAccountIndex:    "SystemAccountIndex",
	SystemMarket:          "SystemMarket",
	SystemReward:          "SystemReward",
	SystemShielded:        "SystemShielded",
	SystemForkAux:         "SystemForkAux",
	SystemPBFT:            "SystemPBFT",
	SystemTapos:           "SystemTapos",
	SystemTrace:           "SystemTrace",
	SystemBloom:           "SystemBloom",
	SystemCheckpoint:      "SystemCheckpoint",
	ContractStorage:       "ContractStorage",
	ContractMetadata:      "ContractMetadata",
	ContractABI:           "ContractABI",
	ContractRuntimeState:  "ContractRuntimeState",
	AccountLocalIndex:     "AccountLocalIndex",
	AccountPermissionAux:  "AccountPermissionAux",
	WitnessCapsule:        "WitnessCapsule",
	WitnessVoteState:      "WitnessVoteState",
}

// IsRegistered reports whether d is a known domain.
func IsRegistered(d KVDomain) bool {
	_, ok := registry[d]
	return ok
}

// Name returns the registered name for d, or "" if unregistered.
func Name(d KVDomain) string {
	return registry[d]
}
