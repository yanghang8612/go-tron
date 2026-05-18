package state

import (
	"sort"
	"testing"

	"github.com/tronprotocol/go-tron/internal/testutil/fixture"
)

// fixtureSkipList is the whitelist of java-tron getter names that we
// deliberately skip in the mainnet conformance test. Each entry MUST
// have a rationale in the comment immediately above it — reviewers
// should push back on any unexplained skip.
var fixtureSkipList = map[string]string{
	// (empty for now — everything in the fixture must match once the
	// backfill lands. Task 2–4 of the M1.1 plan is responsible for
	// keeping this list empty.)
}

// fixtureGetterTransform maps a java getter name to a function that
// converts go-tron's raw DP value into the value the java
// `/wallet/getchainparameters` HTTP API returns. java's Wallet.java
// transforms a small set of keys at display time — the fixture captures
// the API output, so the test must replay the same transformation
// before comparing.
//
// Add a new entry whenever a getter is found in Wallet.java's
// `addChainParameter` builder with a non-identity map (e.g. `/ (24 * 60)`).
var fixtureGetterTransform = map[string]func(int64) int64{
	// java Wallet.java:1268-1272 divides by 24*60 = 1440 so the API
	// reports "minutes worth" rather than the raw "slots per day"
	// stored in DP. The raw DP default is 14400 = 24 * 60 * 10 (see
	// DynamicPropertiesStore.java:469); the API surfaces 10.
	"getAdaptiveResourceLimitTargetRatio": func(v int64) int64 { return v / (24 * 60) },
}

// TestDynamicProperties_MatchMainnetFixture is the primary acceptance
// gate for M1.1. It iterates every (key, value) pair in the 00-genesis-
// dp-mainnet fixture and asserts the default DynamicProperties state
// produces the same value under the canonical go-tron key name.
//
// Failure modes surfaced by this test:
//   - "no go-tron mapping"  — javaGetterToGoKeyMap lacks the entry.
//   - "go-tron key missing" — mapping exists but defaultProps lacks it.
//   - "value mismatch"      — default differs from java-tron.
func TestDynamicProperties_MatchMainnetFixture(t *testing.T) {
	fix := fixture.Load(t, "00-genesis-dp-mainnet")
	dp := NewDynamicProperties()

	// Ordered iteration keeps log output stable across runs.
	javaKeys := make([]string, 0, len(fix.DynamicProperties))
	for k := range fix.DynamicProperties {
		javaKeys = append(javaKeys, k)
	}
	sort.Strings(javaKeys)

	var noMapping, missing, mismatch int
	for _, javaKey := range javaKeys {
		if reason, skip := fixtureSkipList[javaKey]; skip {
			t.Logf("SKIP %s (%s)", javaKey, reason)
			continue
		}
		want := fix.DynamicProperties[javaKey]
		goKey := JavaGetterToGoKey(javaKey)
		if goKey == "" {
			t.Errorf("no go-tron mapping for java %q (want %d)", javaKey, want)
			noMapping++
			continue
		}
		got, ok := dp.Get(goKey)
		if !ok {
			t.Errorf("go-tron key %q missing from defaultProps (java %q, want %d)",
				goKey, javaKey, want)
			missing++
			continue
		}
		if xform, ok := fixtureGetterTransform[javaKey]; ok {
			got = xform(got)
		}
		if got != want {
			t.Errorf("DP[%s / %s]: got %d, want %d", javaKey, goKey, got, want)
			mismatch++
		}
	}

	t.Logf("summary: %d no-mapping, %d missing-default, %d value-mismatch (out of %d)",
		noMapping, missing, mismatch, len(javaKeys))
}
