package main

import (
	"fmt"
	"os"
	"strconv"
	"strings"
)

const historyReadersOnlyEnv = "GTRON_HISTORY_ROLLOUT_READERS_ONLY"

// Resolve before opening any store. This holds only background cold publication
// and its dependent pruning; history capture, readers and commitments stay on.
func runtimeHistoryReadersOnly() (bool, error) {
	value := strings.TrimSpace(os.Getenv(historyReadersOnlyEnv))
	if value == "" {
		return false, nil
	}
	enabled, err := strconv.ParseBool(value)
	if err != nil {
		return false, fmt.Errorf("invalid %s %q: %w", historyReadersOnlyEnv, value, err)
	}
	return enabled, nil
}
