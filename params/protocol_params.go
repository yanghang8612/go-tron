package params

const (
	BlockProducedInterval      = 3000
	MaxActiveWitnessNum        = 27
	WitnessStandbyLength       = 127
	SingleRepeat               = 1
	SolidifiedThreshold        = 70
	MaintenanceSkipSlots       = 2
	MaxVoteNumber              = 30
	TRXPrecision               = 1_000_000
	BlockSize                  = 2_000_000
	ClockMaxDelay              = 3_600_000
	BlockProduceTimeoutPercent = 50
	FrozenPeriod               = 86_400_000
	DelegatePeriod             = 3 * 86_400_000
	DefaultMaintenanceInterval = 6 * 3600 * 1000
	// BlockVersion is the value written to BlockHeader.raw.version by
	// producers. It declares the SR's running software fork version;
	// java-tron's ForkController tallies these across recent blocks to
	// activate version-gated features. Bumping this value implies a
	// coordinated network upgrade — see core/forks/versions.go.
	BlockVersion = 35 // VERSION_4_8_2, mirrors java-tron ChainConstant.BLOCK_VERSION
	WindowSizeMs               = 86_400_000
	WindowSizeSlots            = WindowSizeMs / BlockProducedInterval
	// WindowSizePrecision scales the per-account resource recovery window when
	// it is stored in "optimized" (V2) form. Mirrors java-tron
	// ChainConstant.WINDOW_SIZE_PRECISION.
	WindowSizePrecision = 1000
	// MinParticipationRate is the minimum recent block-fill percentage a
	// witness requires to keep producing. When the rolling
	// BLOCK_FILLED_SLOTS rate drops below this, witnesses skip their slot
	// (java-tron State.LOW_PARTICIPATION). Mirrors the default in
	// java-tron framework/src/main/resources/config.conf:179
	// (minParticipationRate = 15) and the comparison at
	// consensus/src/main/java/org/tron/consensus/dpos/StateManager.java:56
	// (strict <).
	MinParticipationRate = 15
)
