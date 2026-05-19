// Copyright 2022 The go-ethereum Authors
// Copyright 2026 The go-tron Authors
//
// Vendored from go-ethereum/core/rawdb/freezer_utils_windows.go.
//
// SPDX-License-Identifier: LGPL-3.0-or-later

//go:build windows
// +build windows

package freezer

// syncDir is a no-op on Windows. Fsyncing a directory handle is not
// supported and returns "Access is denied".
func syncDir(name string) error {
	return nil
}
