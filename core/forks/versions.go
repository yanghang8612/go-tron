package forks

// VersionParam describes a single SR software fork version.
// Mirrors java-tron's Parameter.ForkBlockVersionEnum entries. Update this
// table (NOT the enum above) when java-tron announces a new fork version.
type VersionParam struct {
	Value        int32 // BlockHeader.raw.version value produced by SRs running this software
	HardForkTime int64 // ms since epoch; block timestamp must reach ceilMaintenance(HardForkTime) before activation
	HardForkRate int   // percentage of active witnesses whose last block must carry Value (0 = strict all-upgrade semantics)
}

// KnownVersions lists the wire versions from java-tron's mainnet and Nile
// ForkBlockVersionEnum (common/.../Parameter.java), ordered by wire value.
// Mainnet ends at version 39. Nile uses distinct rates for versions 33, 34, and 37.
var KnownVersions = []VersionParam{
	{5, 0, 0},               // ENERGY_LIMIT
	{6, 0, 0},               // VERSION_3_2_2
	{7, 0, 0},               // VERSION_3_5
	{8, 0, 0},               // VERSION_3_6
	{9, 0, 0},               // VERSION_3_6_5
	{10, 0, 0},              // VERSION_3_6_6
	{16, 0, 0},              // VERSION_4_0
	{17, 1596780000000, 80}, // VERSION_4_0_1
	{19, 1596780000000, 80}, // VERSION_4_1
	{20, 1596780000000, 80}, // VERSION_4_1_2
	{21, 1596780000000, 80}, // VERSION_4_2
	{22, 1596780000000, 80}, // VERSION_4_3
	{23, 1596780000000, 80}, // VERSION_4_4
	{24, 1596780000000, 80}, // VERSION_4_5
	{25, 1596780000000, 80}, // VERSION_4_6
	{26, 1596780000000, 80}, // VERSION_4_7
	{27, 1596780000000, 80}, // VERSION_4_7_1
	{28, 1596780000000, 80}, // VERSION_4_7_2
	{29, 1596780000000, 80}, // VERSION_4_7_4
	{30, 1596780000000, 80}, // VERSION_4_7_5
	{31, 1596780000000, 80}, // VERSION_4_7_7
	{32, 1596780000000, 80}, // VERSION_4_8_0
	{33, 1596780000000, 70}, // VERSION_4_8_0_1
	{34, 1596780000000, 80}, // VERSION_4_8_1
	{35, 1596780000000, 70}, // VERSION_4_8_1_1
	{36, 1596780000000, 80}, // VERSION_4_8_2
	{37, 1596780000000, 70}, // VERSION_4_8_2_2 (Nile: VERSION_4_8_2_PQ1 at 80%)
	{38, 1596780000000, 70}, // VERSION_4_8_2_3 (Nile: VERSION_4_8_2_2_PQ1)
	{39, 1596780000000, 70}, // VERSION_4_8_2_4 (Nile: VERSION_4_8_2_3_PQ1)
	{40, 1596780000000, 70}, // VERSION_4_8_2_4_PQ1 (Nile only)
}

// Version4822 returns the wire version of the 4.8.2.2 release fork.
func Version4822(nile bool) int32 {
	if nile {
		return 38
	}
	return 37
}

// Version4_0 is the boundary java-tron uses to switch between passOld
// (strict all-upgrade byte check) and passNew (rate + hardForkTime).
// Exported for test clarity.
const Version4_0 int32 = 16

// VoteUpgrade is the byte stored in a slot when the witness's last block
// declared support for the corresponding fork version.
const VoteUpgrade byte = 0x01

// VoteDowngrade is the byte stored when the witness's last block did NOT
// support the fork version.
const VoteDowngrade byte = 0x00

// lookupVersion returns the parameters for a known version or (zero, false).
func lookupVersion(v int32) (VersionParam, bool) {
	return lookupNetworkVersion(v, false)
}

func lookupNetworkVersion(v int32, nile bool) (VersionParam, bool) {
	for _, vp := range networkVersions(nile) {
		if vp.Value == v {
			if nile {
				switch v {
				case 33, 37:
					vp.HardForkRate = 80
				case 34:
					vp.HardForkRate = 70
				}
			}
			return vp, true
		}
	}
	return VersionParam{}, false
}

func networkVersions(nile bool) []VersionParam {
	if nile {
		return KnownVersions
	}
	return KnownVersions[:len(KnownVersions)-1]
}
