package rawdb

import (
	"errors"
	"reflect"
	"testing"

	"github.com/tronprotocol/go-tron/common"
)

type stagePresenceReader struct {
	data   []byte
	exists bool
	err    error
	calls  int
}

func (r *stagePresenceReader) Has([]byte) (bool, error)   { panic("coupled reader used Has") }
func (r *stagePresenceReader) Get([]byte) ([]byte, error) { panic("coupled reader used Get") }
func (r *stagePresenceReader) GetWithPresence([]byte) ([]byte, bool, error) {
	r.calls++
	return append([]byte(nil), r.data...), r.exists, r.err
}

func TestStageProgressCoupledPresence(t *testing.T) {
	storageErr := errors.New("storage failure")
	for _, tt := range []struct {
		name      string
		exists    bool
		data      []byte
		err       error
		wantError bool
	}{
		{name: "missing"}, {name: "present-empty", exists: true, wantError: true},
		{name: "malformed", exists: true, data: []byte("bad"), wantError: true},
		{name: "storage-error", err: storageErr, wantError: true},
		{name: "valid", exists: true, data: encodeStageProgress(17, common.Hash{1}, true)},
	} {
		t.Run(tt.name, func(t *testing.T) {
			r := &stagePresenceReader{data: tt.data, exists: tt.exists, err: tt.err}
			row, ok, err := ReadStageProgressRow(r, StageSyncBodiesReady)
			if r.calls != 1 || (err != nil) != tt.wantError {
				t.Fatalf("calls=%d ok=%v err=%v", r.calls, ok, err)
			}
			if tt.err != nil && !errors.Is(err, storageErr) {
				t.Fatal("storage error identity lost", err)
			}
			if !tt.wantError && (ok != tt.exists || ok && (row.BlockNum != 17 || row.BlockHash != (common.Hash{1}))) {
				t.Fatalf("row=%+v ok=%v", row, ok)
			}
		})
	}
	// One operation never becomes a cached watermark across delete/recreate or
	// a same-height different-hash replacement.
	r := &stagePresenceReader{}
	for _, hash := range []common.Hash{{1}, {2}} {
		r.exists = false
		r.data = nil
		if _, ok, err := ReadStageProgressRow(r, StageSyncBodiesReady); err != nil || ok {
			t.Fatal(ok, err)
		}
		r.exists = true
		r.data = encodeStageProgress(17, hash, true)
		if row, ok, err := ReadStageProgressRow(r, StageSyncBodiesReady); err != nil || !ok || row.BlockHash != hash {
			t.Fatal(row, ok, err)
		}
	}
}

type stagePlainReadTrace struct {
	stageProgressFailingReader
	trace []string
}

func (r *stagePlainReadTrace) Has(k []byte) (bool, error) {
	r.trace = append(r.trace, "Has")
	return r.stageProgressFailingReader.Has(k)
}
func (r *stagePlainReadTrace) Get(k []byte) ([]byte, error) {
	r.trace = append(r.trace, "Get")
	return r.stageProgressFailingReader.Get(k)
}

func TestStageProgressPlainPresenceErrorOrder(t *testing.T) {
	hasErr, getErr := errors.New("Has failed"), errors.New("Get failed")
	for _, tt := range []struct {
		name      string
		reader    stageProgressFailingReader
		wantTrace []string
		wantErr   error
	}{
		{"missing", stageProgressFailingReader{}, []string{"Has"}, nil},
		{"Has-error", stageProgressFailingReader{hasErr: hasErr}, []string{"Has"}, hasErr},
		{"Get-error", stageProgressFailingReader{has: true, getErr: getErr}, []string{"Has", "Get"}, getErr},
		{"valid", stageProgressFailingReader{has: true, data: encodeStageProgress(17, common.Hash{1}, true)}, []string{"Has", "Get"}, nil},
	} {
		t.Run(tt.name, func(t *testing.T) {
			r := &stagePlainReadTrace{stageProgressFailingReader: tt.reader}
			_, _, err := ReadStageProgressRow(r, StageSyncBodiesReady)
			if !reflect.DeepEqual(r.trace, tt.wantTrace) || !errors.Is(err, tt.wantErr) {
				t.Fatalf("trace=%v err=%v", r.trace, err)
			}
		})
	}
}
