package state

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	ethrawdb "github.com/ethereum/go-ethereum/core/rawdb"
	tcommon "github.com/tronprotocol/go-tron/common"
	gtronlog "github.com/tronprotocol/go-tron/common/log"
)

func captureStateCodeDiagnostics(t *testing.T) *bytes.Buffer {
	t.Helper()
	var output bytes.Buffer
	previousLog, previousLimiter := gtronlog.Root(), stateCodeRejectionDiagnostics
	gtronlog.SetDefault(gtronlog.NewLogger(slog.NewJSONHandler(&output, &slog.HandlerOptions{Level: slog.LevelWarn})))
	stateCodeRejectionDiagnostics = new(stateCodeDiagnosticLimiter)
	t.Cleanup(func() {
		gtronlog.SetDefault(previousLog)
		stateCodeRejectionDiagnostics = previousLimiter
	})
	return &output
}

type diagnosticCodeReader struct{ code []byte }

func (r diagnosticCodeReader) Has([]byte) (bool, error) { return true, nil }
func (r diagnosticCodeReader) Get([]byte) ([]byte, error) {
	return bytes.Clone(r.code), nil
}

func TestStateCodeRejectionDiagnosticSourcesPreserveReadBehavior(t *testing.T) {
	code := []byte("diagnostic-private-code-bytes-must-not-be-logged")
	expected := tcommon.Keccak256([]byte("different immutable code"))
	actual := tcommon.Keccak256(code)
	addr := tcommon.Address{0x41, 0x71}
	txHash := tcommon.Hash{0x17, 0x71}
	for _, test := range []struct {
		name, source, errorText string
		strict                  bool
		prepare                 func(*StateDB)
	}{
		{"object", "strict_object", fmt.Sprintf("cached contract runtime code hash mismatch contract=%s codeHash=%s", addr.Hex(), expected.Hex()), true,
			func(s *StateDB) { s.stateObjects[addr].code = bytes.Clone(code) }},
		{"raw_hot", "strict_hot", fmt.Sprintf("state code hash mismatch codeHash=%s", expected.Hex()), true,
			func(s *StateDB) {
				s.codeStore = rawDBStateCodeStore{reader: diagnosticCodeReader{code}, writer: s.db.DiskDB()}
			}},
		{"typed_hot", "strict_hot", fmt.Sprintf("state code hash mismatch codeHash=%s", expected.Hex()), true,
			func(s *StateDB) {
				store := newRecordingStateCodeStore()
				store.codes[expected] = code
				s.codeStore = store
			}},
		{"cold", "strict_cold", fmt.Sprintf("cold contract runtime code hash mismatch contract=%s codeHash=%s", addr.Hex(), expected.Hex()), true,
			func(s *StateDB) { s.SetCodeColdHistory(&countingColdCodeHistory{code: code}, 91) }},
		{"permissive_hot", "cache_admission_hot", "", false,
			func(s *StateDB) {
				s.codeStore = rawDBStateCodeStore{reader: diagnosticCodeReader{code}, writer: s.db.DiskDB()}
			}},
		{"permissive_cold", "cache_admission_cold", "", false,
			func(s *StateDB) { s.SetCodeColdHistory(&countingColdCodeHistory{code: code}, 91) }},
	} {
		t.Run(test.name, func(t *testing.T) {
			output := captureStateCodeDiagnostics(t)
			db := NewDatabaseWithConfig(ethrawdb.NewMemoryDatabase(), DatabaseConfig{CodeCacheSizeBytes: 4096})
			t.Cleanup(func() { _ = db.Close() })
			s := stateDBWithCodeHash(db, addr, expected)
			s.stateObjects[addr].accountKVGeneration = 3
			s.changeSet = domainChangeSetCapture{enabled: true, blockNum: 77, txNum: 88}
			s.BeginBalanceTrace(77, nil, 0)
			s.BeginBalanceTraceTransaction(txHash.Bytes(), "TriggerSmartContract")
			test.prepare(s)
			rejected := stateCodeCacheRejectCounter.Snapshot().Count()
			strictErrors := stateCodeStrictErrorCounter.Snapshot().Count()
			if test.strict {
				got, err := s.GetCodeStrict(addr)
				if got != nil || err == nil || err.Error() != test.errorText {
					t.Fatalf("strict return changed: bytes=%d err=%v", len(got), err)
				}
			} else if got := s.GetCode(addr); !bytes.Equal(got, code) {
				t.Fatal("cache-admission diagnostic changed the permissive read result")
			}
			if delta := stateCodeCacheRejectCounter.Snapshot().Count() - rejected; delta != 1 {
				t.Fatalf("rejection count delta=%d, want 1", delta)
			}
			wantStrict := int64(0)
			if test.strict {
				wantStrict = 1
			}
			if delta := stateCodeStrictErrorCounter.Snapshot().Count() - strictErrors; delta != wantStrict {
				t.Fatalf("strict error delta=%d, want %d", delta, wantStrict)
			}
			if _, ok := db.codeCache.get(expected); ok {
				t.Fatal("rejected code entered the shared cache")
			}
			var record map[string]any
			if err := json.Unmarshal(bytes.TrimSpace(output.Bytes()), &record); err != nil {
				t.Fatalf("expected exactly one structured diagnostic: %v\n%s", err, output.String())
			}
			for key, want := range map[string]any{
				"source": test.source, "expectedHash": expected.Hex(), "actualHash": actual.Hex(),
				"contract": addr.Hex(), "addressKnown": true, "codeBytes": float64(len(code)),
				"captureBlock": float64(77), "captureTxNum": float64(88), "traceBlock": float64(77),
				"traceTxHash": txHash.Hex(), "accountGeneration": float64(3),
			} {
				if record[key] != want {
					t.Errorf("%s=%v, want %v", key, record[key], want)
				}
			}
			if test.source == "strict_cold" || test.source == "cache_admission_cold" {
				if record["coldTxNumCutoff"] != float64(91) {
					t.Error("missing cold history cutoff")
				}
			}
			callers, _ := record["callers"].(string)
			if !strings.Contains(callers, "TestStateCodeRejectionDiagnosticSourcesPreserveReadBehavior") || len(callers) > stateCodeDiagnosticFrames*(stateCodeDiagnosticFrameLen+1) {
				t.Fatalf("missing or unbounded caller context: %s", callers)
			}
			if strings.Contains(output.String(), string(code)) || strings.Contains(output.String(), hex.EncodeToString(code)) {
				t.Fatal("diagnostic exposed bytecode")
			}
		})
	}
}

func TestStateCodeRejectionDiagnosticUnknownAddressAndSuppression(t *testing.T) {
	output := captureStateCodeDiagnostics(t)
	cache := newStateCodeCache(4096)
	t.Cleanup(cache.close)
	code, _ := cacheTestCode(0x62, 64)
	expected := tcommon.Hash{0x81}
	rejected := stateCodeCacheRejectCounter.Snapshot().Count()
	suppressed := stateCodeDiagnosticSuppressed.Snapshot().Count()
	for range stateCodeDiagnosticBurst + 3 {
		if cache.admit(expected, code) {
			t.Fatal("invalid code admitted")
		}
	}
	lines := bytes.Split(bytes.TrimSpace(output.Bytes()), []byte("\n"))
	if len(lines) != stateCodeDiagnosticBurst {
		t.Fatalf("logged %d events, want %d", len(lines), stateCodeDiagnosticBurst)
	}
	for _, line := range lines {
		var record map[string]any
		if err := json.Unmarshal(line, &record); err != nil {
			t.Fatal(err)
		}
		if record["source"] != "cache_admission" || record["addressKnown"] != false || record["contract"] != nil || record["captureBlock"] != nil {
			t.Fatalf("invented address/block context: %s", line)
		}
	}
	if delta := stateCodeCacheRejectCounter.Snapshot().Count() - rejected; delta != stateCodeDiagnosticBurst+3 {
		t.Fatalf("suppression lost rejection events: %d", delta)
	}
	if delta := stateCodeDiagnosticSuppressed.Snapshot().Count() - suppressed; delta != 3 {
		t.Fatalf("suppressed count=%d, want 3", delta)
	}
}

func TestStateCodeDiagnosticLimiterConcurrentAndWindowRecovery(t *testing.T) {
	limiter := new(stateCodeDiagnosticLimiter)
	now := time.Unix(100, 0)
	var accepted atomic.Int64
	var wg sync.WaitGroup
	for range 64 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if ok, _ := limiter.allow(now); ok {
				accepted.Add(1)
			}
		}()
	}
	wg.Wait()
	if accepted.Load() != stateCodeDiagnosticBurst {
		t.Fatalf("concurrent burst=%d", accepted.Load())
	}
	if ok, _ := limiter.allow(now.Add(stateCodeDiagnosticWindow - time.Nanosecond)); ok {
		t.Fatal("allowed event before window expired")
	}
	if ok, suppressed := limiter.allow(now.Add(stateCodeDiagnosticWindow)); !ok || suppressed != 64-stateCodeDiagnosticBurst+1 {
		t.Fatalf("next window admitted=%v suppressed=%d", ok, suppressed)
	}
	if ok, suppressed := limiter.allow(now.Add(stateCodeDiagnosticWindow)); !ok || suppressed != 0 {
		t.Fatal("suppression summary was not consumed")
	}
}

func TestStateCodeHistoricalHashesDoNotRejectAfterNewerCodeRead(t *testing.T) {
	output := captureStateCodeDiagnostics(t)
	db := NewDatabaseWithConfig(ethrawdb.NewMemoryDatabase(), DatabaseConfig{CodeCacheSizeBytes: 4096})
	t.Cleanup(func() { _ = db.Close() })
	addr := tcommon.Address{0x41, 0x32}
	oldCode, oldHash := cacheTestCode(0x31, 128)
	newCode, newHash := cacheTestCode(0x32, 128)
	rejected, strictErrors := stateCodeCacheRejectCounter.Snapshot().Count(), stateCodeStrictErrorCounter.Snapshot().Count()
	for _, view := range []struct {
		code   []byte
		hash   tcommon.Hash
		cutoff uint64
	}{
		{newCode, newHash, 200}, {oldCode, oldHash, 100}, {oldCode, oldHash, 1}, {newCode, newHash, 200},
	} {
		s := stateDBWithCodeHash(db, addr, view.hash)
		s.SetCodeColdHistory(&countingColdCodeHistory{code: view.code}, view.cutoff)
		got, err := s.GetCodeStrict(addr)
		if err != nil || !bytes.Equal(got, view.code) {
			t.Fatalf("content-addressed historical read: %v", err)
		}
	}
	if output.Len() != 0 || stateCodeCacheRejectCounter.Snapshot().Count() != rejected || stateCodeStrictErrorCounter.Snapshot().Count() != strictErrors {
		t.Fatal("valid older content hash was reported as a rejection")
	}
}
