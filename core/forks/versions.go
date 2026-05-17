package forks

// VersionParam describes a single SR software fork version.
// Mirrors java-tron's Parameter.ForkBlockVersionEnum entries. Update this
// table (NOT the enum above) when java-tron announces a new fork version.
type VersionParam struct {
	Value        int32 // BlockHeader.raw.version value produced by SRs running this software
	HardForkTime int64 // ms since epoch; block timestamp must reach ceilMaintenance(HardForkTime) before activation
	HardForkRate int   // percentage of active witnesses whose last block must carry Value (0 = strict all-upgrade semantics)
}

// KnownVersions enumerates every ForkBlockVersionEnum entry from
// java-tron's Parameter.ForkBlockVersionEnum (common/.../Parameter.java).
// Order matches upstream for reviewability.
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
	{35, 1596780000000, 80}, // VERSION_4_8_2
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
	for _, vp := range KnownVersions {
		if vp.Value == v {
			return vp, true
		}
	}
	return VersionParam{}, false
}
