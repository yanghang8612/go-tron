package freezer

import (
	"context"
	"errors"
)

// V2CooperativeYield is an opt-in scheduler hook for a direct hot-source build.
// Due must be bounded and side-effect free. Yield runs only with all source,
// preparation and frame compression work quiescent, with no visible partial
// segment. It must honor ctx and must not call migration or Close on this store:
// the build still exclusively owns v2Migrate. The caller must serialize all its
// gate-owning migration/index entrypoints before allowing this hook to yield.
// Neither hook changes the immutable publication unit or publication ordering.
type V2CooperativeYield struct {
	Due   func() bool
	Yield func(context.Context) error
	// Phase reports a fixed internal phase; it must not perform work or yield.
	Phase func(string)
}

const v2CooperativeRecords = uint64(2048)

var errV2LayoutChanged = errors.New("ancient V2: layout changed while direct build yielded")
