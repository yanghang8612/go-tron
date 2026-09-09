//go:build !linux

package main

import "errors"

func readRuntimeHistoryParallel() (historyParallelObservation, error) {
	return historyParallelObservation{}, errors.New("parallel history resource evidence unavailable on this platform")
}
