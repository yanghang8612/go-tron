// Package tracers provides the standard TVM execution tracers (the EIP-3155
// struct logger and the call tracer) that back the debug_trace* JSON-RPC
// namespace. They consume the vm.Tracer hook stream and render a
// JSON-serialisable result. Mirrors go-ethereum's eth/tracers, adapted to TVM
// energy semantics (energy fills geth's "gas" slots).
package tracers

import (
	"encoding/json"
	"fmt"

	"github.com/tronprotocol/go-tron/vm"
)

// Tracer is a vm.Tracer that can render its accumulated trace as a
// JSON-serialisable value for a debug_trace* response.
type Tracer interface {
	vm.Tracer
	GetResult() (interface{}, error)
}

// TraceConfig is the geth debug_trace* configuration object: the named-tracer
// selector and its config, plus the struct-log toggles (consumed when no named
// tracer is selected). Matches go-ethereum's TraceConfig JSON shape so existing
// debug tooling interoperates.
type TraceConfig struct {
	Tracer       *string         `json:"tracer,omitempty"`
	TracerConfig json.RawMessage `json:"tracerConfig,omitempty"`
	Timeout      *string         `json:"timeout,omitempty"`
	Reexec       *uint64         `json:"reexec,omitempty"`

	// Struct-logger toggles (used only by the default tracer).
	DisableStack     bool `json:"disableStack"`
	DisableMemory    bool `json:"disableMemory"`
	DisableStorage   bool `json:"disableStorage"`
	EnableReturnData bool `json:"enableReturnData"`
	Limit            int  `json:"limit"`
}

// New builds the tracer named by cfg.Tracer: the default struct logger when the
// name is empty/absent, "callTracer" for the call tree, otherwise an error. A
// nil cfg yields the default struct logger with default toggles.
func New(cfg *TraceConfig) (Tracer, error) {
	if cfg == nil || cfg.Tracer == nil || *cfg.Tracer == "" {
		logCfg := LogConfig{}
		if cfg != nil {
			logCfg = LogConfig{
				DisableStack:     cfg.DisableStack,
				DisableMemory:    cfg.DisableMemory,
				DisableStorage:   cfg.DisableStorage,
				EnableReturnData: cfg.EnableReturnData,
				Limit:            cfg.Limit,
			}
		}
		return NewStructLogger(logCfg), nil
	}
	switch *cfg.Tracer {
	case "callTracer":
		return newCallTracer(), nil
	default:
		return nil, fmt.Errorf("unknown tracer %q", *cfg.Tracer)
	}
}
