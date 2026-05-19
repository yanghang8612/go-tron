// Copyright 2022 The go-ethereum Authors
// Copyright 2026 The go-tron Authors
//
// This file is vendored from go-ethereum/core/rawdb/freezer_utils.go and
// adapted for gtron's package layout. Original go-ethereum source is GNU
// Lesser General Public License v3.0 or later; gtron already depends on
// go-ethereum, so the licence carries over to this copy.
//
// SPDX-License-Identifier: LGPL-3.0-or-later

package freezer

import (
	"io"
	"os"
	"path/filepath"
)

func atomicRename(src, dest string) error {
	if err := os.Rename(src, dest); err != nil {
		return err
	}
	return syncDir(filepath.Dir(src))
}

// copyFrom copies data from 'srcPath' at offset 'offset' into 'destPath'.
// The 'destPath' is created if it doesn't exist, otherwise it is overwritten.
// Before the copy is executed, there is a callback can be registered to
// manipulate the dest file.
// It is perfectly valid to have destPath == srcPath.
func copyFrom(srcPath, destPath string, offset uint64, before func(f *os.File) error) error {
	// Create a temp file in the same dir where we want it to wind up
	f, err := os.CreateTemp(filepath.Dir(destPath), "*")
	if err != nil {
		return err
	}
	fname := f.Name()

	// Clean up the leftover file
	defer func() {
		if f != nil {
			f.Close()
		}
		os.Remove(fname)
	}()
	// Apply the given function if it's not nil before we copy
	// the content from the src.
	if before != nil {
		if err := before(f); err != nil {
			return err
		}
	}
	// Open the source file
	src, err := os.Open(srcPath)
	if err != nil {
		return err
	}
	if _, err = src.Seek(int64(offset), 0); err != nil {
		src.Close()
		return err
	}
	// io.Copy uses 32K buffer internally.
	_, err = io.Copy(f, src)
	if err != nil {
		src.Close()
		return err
	}
	// Rename the temporary file to the specified dest name.
	// src may be same as dest, so needs to be closed before
	// we do the final move.
	src.Close()

	// Permanently persist the content into disk
	if err := f.Sync(); err != nil {
		return err
	}

	if err := f.Close(); err != nil {
		return err
	}
	f = nil

	return atomicRename(fname, destPath)
}

// reset atomically replaces the file at the given path with the provided content.
func reset(path string, content []byte) error {
	// Create a temp file in the same dir where we want it to wind up
	f, err := os.CreateTemp(filepath.Dir(path), "*")
	if err != nil {
		return err
	}
	fname := f.Name()

	// Clean up the leftover file
	defer func() {
		if f != nil {
			f.Close()
		}
		os.Remove(fname)
	}()

	// Write the content into the temp file
	_, err = f.Write(content)
	if err != nil {
		return err
	}
	// Permanently persist the content into disk
	if err := f.Sync(); err != nil {
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	f = nil

	return atomicRename(fname, path)
}

// openFreezerFileForAppend opens a freezer table file and seeks to the end.
func openFreezerFileForAppend(filename string) (*os.File, error) {
	// Open the file without the O_APPEND flag because it has differing
	// behavior during Truncate operations on different OS's.
	file, err := os.OpenFile(filename, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return nil, err
	}
	// Seek to end for append
	if _, err = file.Seek(0, io.SeekEnd); err != nil {
		file.Close()
		return nil, err
	}
	return file, nil
}

// openFreezerFileForReadOnly opens a freezer table file for read only access.
func openFreezerFileForReadOnly(filename string) (*os.File, error) {
	return os.OpenFile(filename, os.O_RDONLY, 0644)
}

// openFreezerFileTruncated opens a freezer table making sure it is truncated.
func openFreezerFileTruncated(filename string) (*os.File, error) {
	return os.OpenFile(filename, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0644)
}

// truncateFreezerFile resizes a freezer table file and seeks to the end.
func truncateFreezerFile(file *os.File, size int64) error {
	if err := file.Truncate(size); err != nil {
		return err
	}
	// Seek to end for append
	if _, err := file.Seek(0, io.SeekEnd); err != nil {
		return err
	}
	return nil
}

// grow prepares the slice space for new item, and doubles the slice capacity
// if space is not enough.
func grow(buf []byte, n int) []byte {
	if cap(buf)-len(buf) < n {
		newcap := 2 * cap(buf)
		if newcap-len(buf) < n {
			newcap = len(buf) + n
		}
		nbuf := make([]byte, len(buf), newcap)
		copy(nbuf, buf)
		buf = nbuf
	}
	buf = buf[:len(buf)+n]
	return buf
}
