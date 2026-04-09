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
	BlockVersion               = 34
	WindowSizeMs               = 86_400_000
	WindowSizeSlots            = WindowSizeMs / BlockProducedInterval
)
