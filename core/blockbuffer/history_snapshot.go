package blockbuffer

import (
	"errors"

	"github.com/tronprotocol/go-tron/core/pointread"
	"github.com/tronprotocol/go-tron/core/rawdb"
)

// NewStateHistoryReadSnapshotThrough captures the same committed layer prefix
// and durable handoff as NewReadSnapshotThrough, but permits only history reads.
// A direct-history base pins only its history engine. Canonical hashes and
// current state must be read and verified separately by the caller.
func (b *Buffer) NewStateHistoryReadSnapshotThrough(maxBlock uint64) (*ReadSnapshot, error) {
	return b.captureReadSnapshot(&maxBlock, false, true)
}

type stateHistoryReadBase struct {
	rawdb.StateHistoryReadView
	release func() error
}

func (s *stateHistoryReadBase) Close() error { return s.release() }

func (s *stateHistoryReadBase) SeekPrefix(prefix, start []byte) ([]byte, []byte, bool, error) {
	if seeker, ok := s.StateHistoryReadView.(pointread.PrefixSeeker); ok {
		return seeker.SeekPrefix(prefix, start)
	}
	return seekPrefixWithBase(s.StateHistoryReadView, newOverlayState(), prefix, start)
}

func newStateHistoryReadBase(source any) (pointread.KeyValueSnapshot, error) {
	view, release, err := rawdb.AcquireStateHistoryBuildReadView(source)
	if err != nil {
		return nil, err
	}
	if !view.IsPinnedKeyValueView() {
		if err := release(); err != nil {
			return nil, errors.Join(errors.New("blockbuffer: close unpinned history source"), err)
		}
		return nil, ErrReadSnapshotUnsupported
	}
	return &stateHistoryReadBase{StateHistoryReadView: view, release: release}, nil
}
