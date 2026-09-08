//go:build linux

package main

import (
	"os"
	"path/filepath"
	"testing"

	"golang.org/x/sys/unix"
)

func TestHistoryDataDeviceMissingDirectoryAndSymlink(t *testing.T) {
	dir := t.TempDir()
	var stat unix.Stat_t
	if err := unix.Stat(dir, &stat); err != nil {
		t.Fatal(err)
	}
	want := historyDeviceID{unix.Major(uint64(stat.Dev)), unix.Minor(uint64(stat.Dev))}
	missing := filepath.Join(dir, "not-created", "datadir")
	if got, err := historyDataDevice(missing); err != nil || got != want {
		t.Fatalf("missing datadir device: %+v, %v", got, err)
	}
	if _, err := os.Stat(filepath.Dir(missing)); !os.IsNotExist(err) {
		t.Fatalf("read-only probe created a directory: %v", err)
	}
	link := filepath.Join(dir, "linked-data")
	if err := os.Symlink(dir, link); err != nil {
		t.Fatal(err)
	}
	if got, err := historyDataDevice(filepath.Join(link, "missing")); err != nil || got != want {
		t.Fatalf("symlink filesystem device: %+v, %v", got, err)
	}
	dangling := filepath.Join(dir, "unresolved-volume")
	if err := os.Symlink(filepath.Join(dir, "nonexistent-target"), dangling); err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{"", filepath.Join(dangling, "datadir")} {
		if _, err := historyDataDevice(path); err == nil {
			t.Fatalf("inferred destination filesystem for unknown path %q", path)
		}
	}
}
