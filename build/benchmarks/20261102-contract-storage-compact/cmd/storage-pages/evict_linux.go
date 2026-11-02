//go:build linux

package main

import (
	"os"
	"path/filepath"

	"golang.org/x/sys/unix"
)

// Only experiment SST files are advised out; never uses system drop_caches.
func evictSST(dir string) error {
	return filepath.Walk(dir, func(path string, info os.FileInfo, e error) error {
		if e != nil {
			return e
		}
		if !info.IsDir() && filepath.Ext(path) == ".sst" {
			f, e := os.Open(path)
			if e != nil {
				return e
			}
			e = unix.Fadvise(int(f.Fd()), 0, 0, unix.FADV_DONTNEED)
			c := f.Close()
			if e != nil {
				return e
			}
			return c
		}
		return nil
	})
}
