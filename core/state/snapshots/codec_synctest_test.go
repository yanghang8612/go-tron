package snapshots

import (
	"bytes"
	"fmt"
	"os"
	"testing"
)

// The production codec is process-wide and lazily creates internal channels on
// its first nonempty operation. Creating those inside a synctest bubble binds
// them to that bubble and makes another test's EncodeAll/DecodeAll fatal. Keep
// this test-process resource owned by the outer runtime, independent of test
// order; do not reset or replace the production codec between tests.
func TestMain(m *testing.M) {
	enc, dec, err := cbCodec()
	if err != nil {
		fmt.Fprintln(os.Stderr, "initialize shared test codec:", err)
		os.Exit(1)
	}
	raw := []byte("snapshot test codec initialization")
	encoded := enc.EncodeAll(raw, nil)
	decoded, err := dec.DecodeAll(encoded, make([]byte, 0, len(raw)))
	if err != nil || !bytes.Equal(decoded, raw) {
		fmt.Fprintln(os.Stderr, "round-trip shared test codec:", err)
		os.Exit(1)
	}
	os.Exit(m.Run())
}
