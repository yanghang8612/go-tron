package snapshots

import "testing"

func TestContentAddressedSnapshotPathReplacesExistingDigest(t *testing.T) {
	checksumA := "sha256:0123456789abcdefaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	checksumB := "sha256:fedcba9876543210bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"

	first := contentAddressedSnapshotPath("history/state-domain-change-1-2.seg", checksumA)
	if first != "history/state-domain-change-1-2-0123456789abcdef.seg" {
		t.Fatalf("first path = %q", first)
	}
	second := contentAddressedSnapshotPath(first, checksumA)
	if second != first {
		t.Fatalf("same checksum path = %q, want %q", second, first)
	}
	replaced := contentAddressedSnapshotPath(first, checksumB)
	if replaced != "history/state-domain-change-1-2-fedcba9876543210.seg" {
		t.Fatalf("replaced path = %q", replaced)
	}
}
