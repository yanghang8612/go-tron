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
// New requests reauthenticate current physical bytes. No errors, incomplete
// reads or cross-request proofs enter this cache; exhaustion uses normal reads.
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
	if len(s.images) >= historyRoutingRequestEntries || len(image.data) > historyRoutingRequestBytes-s.bytes {
		return
	}
	s.images[key] = image
	s.bytes += len(image.data)
}
