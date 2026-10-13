//go:build (amd64 || arm64) && !purego

package domains

import "testing"

func TestBranchNativeFeatureFallbackDoesNotAllocate(t *testing.T) {
	old := branchOneShotReady
	branchOneShotReady = false
	defer func() { branchOneShotReady = old }()
	TestBranchEncodingAndOneShotHashEquivalence(t)
	TestBranchPreimageHashDoesNotAllocate(t)
}
