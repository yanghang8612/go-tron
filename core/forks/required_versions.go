package forks

// requiredVersion maps each AllowFlag to the ForkBlockVersion that must
// have activated (via SR vote quorum) before the flag's execution-path
// effect is valid. Derived from java-tron ProposalUtil.ProposalType case
// bodies and ProposalService.process gating — an authoritative pass is
// built by the fork-gate audit (docs/dev/fork-audit-2026-04-15.md,
// produced in M1.3 Task 5).
//
// Entries not present here mean "soft flag alone suffices" — historically
// the correct behaviour for flags that were introduced alongside their
// governance proposal at the same block version. Adding an entry STRICTENS
// the gate, never loosens it.
var requiredVersion = map[AllowFlag]int32{
	// Entries will be populated as the Task 5 audit walks
	// ProposalUtil.java's forkController.pass(...) sites.
	//
	// Seeded with a representative pair so the machinery is exercised
	// and tests catch regressions in map-lookup semantics:
	AllowAdaptiveEnergy:   9,  // VERSION_3_6_5
	AllowTvmConstantinople: 9, // VERSION_3_6_5
}

// RequiredVersion returns the fork block version that must have passed
// before the flag's feature should be considered active. (0, false) means
// no version gate is required beyond the DP soft flag.
func RequiredVersion(flag AllowFlag) (int32, bool) {
	v, ok := requiredVersion[flag]
	return v, ok
}
