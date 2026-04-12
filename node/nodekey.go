package node

import (
	"crypto/rand"
	"fmt"
	"os"
	"path/filepath"
)

// NodeIDLen is the libp2p node ID length (bytes).
const NodeIDLen = 64

// LoadOrCreateNodeID loads the node's persistent ID from <dataDir>/nodekey, or
// generates a fresh 64-byte random ID and persists it if the file is missing.
// Returns the 64-byte ID.
func LoadOrCreateNodeID(dataDir string) ([]byte, error) {
	if dataDir == "" {
		// Ephemeral — return a fresh ID without persisting.
		id := make([]byte, NodeIDLen)
		if _, err := rand.Read(id); err != nil {
			return nil, err
		}
		return id, nil
	}

	path := filepath.Join(dataDir, "nodekey")
	data, err := os.ReadFile(path)
	if err == nil {
		if len(data) != NodeIDLen {
			return nil, fmt.Errorf("nodekey at %s: wrong length %d (expected %d)", path, len(data), NodeIDLen)
		}
		return data, nil
	}
	if !os.IsNotExist(err) {
		return nil, fmt.Errorf("read nodekey %s: %w", path, err)
	}

	// Generate fresh ID and persist
	id := make([]byte, NodeIDLen)
	if _, err := rand.Read(id); err != nil {
		return nil, err
	}
	if err := os.MkdirAll(dataDir, 0o755); err != nil {
		return nil, fmt.Errorf("mkdir datadir %s: %w", dataDir, err)
	}
	if err := os.WriteFile(path, id, 0o600); err != nil {
		return nil, fmt.Errorf("write nodekey %s: %w", path, err)
	}
	return id, nil
}
