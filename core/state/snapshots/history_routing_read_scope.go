package snapshots

import (
	"context"
	"sync"
)

const historyRoutingRequestBytes = 64 << 20
const historyRoutingRequestEntries = 8

type historyRoutingReadScopeKey struct{}
type historyRoutingReadKey struct {
	dir string
	ref SegmentRef
}
type historyRoutingReadImage struct {
	data   []byte
	digest [32]byte
	index  *historyRoutingImageIndex
}
type historyRoutingReadScope struct {
	mu     sync.Mutex
	images map[historyRoutingReadKey]historyRoutingReadImage
	bytes  int
}

// WithHistoryRoutingReadScope starts a fresh auxiliary read image for one
// logical historical query. The caller must not reuse it for another request.
// Each exact reference is read and authenticated before admission; later calls
// in this request use those owned immutable bytes, like a pinned read view.
// New requests check current physical identity including OS ctime before
// reusing an authenticated owned image. No errors, incomplete
// reads enter this cache; exhaustion uses normal reads.
func WithHistoryRoutingReadScope(ctx context.Context) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithValue(ctx, historyRoutingReadScopeKey{}, &historyRoutingReadScope{images: make(map[historyRoutingReadKey]historyRoutingReadImage)})
}

func routingReadScope(ctx context.Context) *historyRoutingReadScope {
	if ctx == nil {
		return nil
	}
	scope, _ := ctx.Value(historyRoutingReadScopeKey{}).(*historyRoutingReadScope)
	return scope
}

func (s *historyRoutingReadScope) get(key historyRoutingReadKey) (historyRoutingReadImage, bool) {
	if s == nil {
		return historyRoutingReadImage{}, false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	image, ok := s.images[key]
	return image, ok
}

func (s *historyRoutingReadScope) put(key historyRoutingReadKey, image historyRoutingReadImage) {
	if s == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.images[key]; exists {
		return
	}
	// A nearly 64 MiB broad route must not be excluded just because a tiny
	// system route arrived first. Prefer the dominant authentication cost,
	// without raising the per-request byte cap or evicting it for small reads.
	if len(image.data) <= historyRoutingRequestBytes && len(image.data) > historyRoutingRequestBytes-s.bytes && len(image.data) > s.bytes {
		s.images = make(map[historyRoutingReadKey]historyRoutingReadImage)
		s.bytes = 0
	}
	if len(s.images) >= historyRoutingRequestEntries || len(image.data) > historyRoutingRequestBytes-s.bytes {
		return
	}
	s.images[key] = image
	s.bytes += len(image.data)
}
