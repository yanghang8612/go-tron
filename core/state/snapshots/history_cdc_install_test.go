package snapshots

import (
	"context"
	"os"
	"path/filepath"
	"testing"
)

func TestHistoryCDCCompanionCopyAuthenticatesBeforeInstall(t *testing.T) {
	dir := t.TempDir()
	src, tmp, dst := filepath.Join(dir, "source"), filepath.Join(dir, "copy"), filepath.Join(dir, "installed")
	data := []byte("immutable companion bytes")
	if err := os.WriteFile(src, data, 0600); err != nil {
		t.Fatal(err)
	}
	ref := SegmentRef{Size: uint64(len(data)), Checksum: checksumBytes(data)}
	bad := ref
	bad.Checksum = checksumBytes([]byte("different identity"))
	if err := historyReferenceCopyCompanion(context.Background(), src, tmp, bad); err == nil {
		t.Fatal("accepted wrong copy checksum")
	}
	if err := os.Remove(tmp); err != nil {
		t.Fatal(err)
	}
	if err := historyReferenceCopyCompanion(context.Background(), src, tmp, ref); err != nil {
		t.Fatal(err)
	}
	if err := historyCDCInstallVerifiedFile(context.Background(), tmp, dst, ref); err != nil {
		t.Fatal(err)
	}
	if err := historyCDCInstallVerifiedFile(context.Background(), tmp, dst, ref); err != nil {
		t.Fatalf("identical installed identity: %v", err)
	}
	if err := os.Remove(dst); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(dst, []byte("foreign immutable object"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := historyCDCInstallVerifiedFile(context.Background(), tmp, dst, ref); err == nil {
		t.Fatal("accepted conflicting installed identity")
	}
}
