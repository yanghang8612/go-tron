//go:build !linux

package main

import "errors"

func historyDataDevice(string) (historyDeviceID, error) {
	return historyDeviceID{}, errors.New("history device load is unavailable on this platform")
}
