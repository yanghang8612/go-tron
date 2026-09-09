//go:build linux

package main

func readRuntimeHistoryParallel() (historyParallelObservation, error) {
	return collectHistoryParallel(readHistoryParallelFile)
}
