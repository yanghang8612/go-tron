// Copyright 2022 The go-ethereum Authors
// Copyright 2026 The go-tron Authors
//
// Vendored from go-ethereum/core/rawdb/freezer_utils_unix.go.
//
// SPDX-License-Identifier: LGPL-3.0-or-later

//go:build !windows
// +build !windows

package freezer

import (
	"errors"
	"os"
	"syscall"
)

// syncDir ensures that the directory metadata (e.g. newly renamed files)
// is flushed to durable storage.
func syncDir(name string) error {
	f, err := os.Open(name)
	if err != nil {
		return err
	}
	defer f.Close()

	// Some file systems do not support fsyncing directories (e.g. some FUSE
	// mounts). Ignore EINVAL in those cases.
	if err := f.Sync(); err != nil {
		if errors.Is(err, os.ErrInvalid) {
			return nil
		}
		if patherr, ok := err.(*os.PathError); ok && patherr.Err == syscall.EINVAL {
			return nil
		}
		return err
	}
	return nil
}
