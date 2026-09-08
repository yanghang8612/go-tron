//go:build linux

package main

import (
	"errors"
	"os"
	"path/filepath"

	"golang.org/x/sys/unix"
)

// A not-yet-created datadir inherits the closest existing parent's filesystem.
// stat follows symlinks, including a datadir linked onto another mounted disk.
func historyDataDevice(path string) (historyDeviceID, error) {
	if path == "" {
		return historyDeviceID{}, errors.New("history data directory is empty")
	}
	absolute, err := filepath.Abs(path)
	if err != nil {
		return historyDeviceID{}, err
	}
	for {
		var stat unix.Stat_t
		if err = unix.Stat(absolute, &stat); err == nil {
			return historyDeviceID{major: unix.Major(uint64(stat.Dev)), minor: unix.Minor(uint64(stat.Dev))}, nil
		}
		if !os.IsNotExist(err) {
			return historyDeviceID{}, err
		}
		// A dangling symlink may target another volume. Its containing
		// directory is not evidence of the destination device.
		if info, linkErr := os.Lstat(absolute); linkErr == nil && info.Mode()&os.ModeSymlink != 0 {
			return historyDeviceID{}, err
		}
		parent := filepath.Dir(absolute)
		if parent == absolute {
			return historyDeviceID{}, err
		}
		absolute = parent
	}
}
