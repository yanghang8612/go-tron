package snapshots

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"syscall"
	"testing"
)

func TestCompressedFooterBlobCrossFilesystem(t *testing.T) {
	scratch := t.TempDir()
	destination, err := os.MkdirTemp("/dev/shm", "gtron-footer-test-")
	if err != nil {
		t.Skipf("separate tmpfs unavailable: %v", err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(destination) })
	probe := filepath.Join(scratch, "probe")
	if err := os.WriteFile(probe, nil, 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.Link(probe, filepath.Join(destination, "probe")); !errors.Is(err, syscall.EXDEV) {
		t.Skipf("test requires different filesystems: link returned %v", err)
	}
	blob := bytes.Repeat([]byte("history-across-tiers"), 16384)
	for _, format := range []string{"1", "2"} {
		t.Run("format="+format, func(t *testing.T) {
			t.Setenv("GTRON_HISTORY_COMPRESSION_FORMAT", format)
			path := filepath.Join(destination, fmt.Sprintf("history-%s", format))
			if err := compressBlobToFile(scratch, path, blob, 16384); err != nil {
				t.Fatal(err)
			}
			encoded, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			decoded, err := decompressBlockBlob(encoded)
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(decoded, blob) {
				t.Fatal("cross-filesystem output changed history bytes")
			}
		})
	}
}
