package snapshots

import (
	"path/filepath"
	"strings"
)

const snapshotPathChecksumPrefixLen = 16

func contentAddressedSnapshotPath(path, checksum string) string {
	digest, ok := snapshotChecksumDigest(checksum)
	if !ok {
		return path
	}
	if len(digest) > snapshotPathChecksumPrefixLen {
		digest = digest[:snapshotPathChecksumPrefixLen]
	}
	ext := filepath.Ext(path)
	stem := strings.TrimSuffix(path, ext)
	baseStem, existingDigest, hasExistingDigest := splitSnapshotPathChecksumSuffix(stem)
	if hasExistingDigest && existingDigest == digest {
		return path
	}
	if hasExistingDigest {
		stem = baseStem
	}
	return stem + "-" + digest + ext
}

func snapshotChecksumDigest(checksum string) (string, bool) {
	digest := strings.TrimPrefix(strings.ToLower(checksum), "sha256:")
	if len(digest) < snapshotPathChecksumPrefixLen {
		return "", false
	}
	for _, ch := range digest {
		if (ch < '0' || ch > '9') && (ch < 'a' || ch > 'f') {
			return "", false
		}
	}
	return digest, true
}

func splitSnapshotPathChecksumSuffix(stem string) (string, string, bool) {
	idx := strings.LastIndex(stem, "-")
	if idx < 0 || len(stem)-idx-1 != snapshotPathChecksumPrefixLen {
		return stem, "", false
	}
	suffix := strings.ToLower(stem[idx+1:])
	for _, ch := range suffix {
		if (ch < '0' || ch > '9') && (ch < 'a' || ch > 'f') {
			return stem, "", false
		}
	}
	return stem[:idx], suffix, true
}
