package tronapi

import (
	"bytes"
	"encoding/json"
	"net/http"
	"strconv"
	"strings"
)

// tronJSONResponse carries the request's output options through shared response
// helpers, including handlers used by the wallet, solidity and PBFT APIs.
type tronJSONResponse struct {
	http.ResponseWriter
}

func (w tronJSONResponse) Unwrap() http.ResponseWriter { return w.ResponseWriter }

func withTronJSONOptions(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet && strings.EqualFold(r.URL.Query().Get("int64_as_string"), "true") {
			w = tronJSONResponse{ResponseWriter: w}
		}
		next.ServeHTTP(w, r)
	})
}

func responseTronJSON(w http.ResponseWriter) tronJSON {
	_, enabled := w.(tronJSONResponse)
	return tronJSON(enabled)
}

func (opts tronJSON) int64(n int64) any {
	if opts {
		return strconv.FormatInt(n, 10)
	}
	return n
}

// marshalInt64Fields is for hand-built wallet response DTOs whose numeric
// fields all represent int64 values in the upstream protobuf schema.
func (opts tronJSON) marshalInt64Fields(doc any) ([]byte, error) {
	encoded, err := json.Marshal(doc)
	if err != nil || !opts {
		return encoded, err
	}
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.UseNumber()
	var parsed any
	if err := decoder.Decode(&parsed); err != nil {
		return nil, err
	}
	return json.Marshal(quoteInt64Fields(parsed))
}

func quoteInt64Fields(value any) any {
	switch value := value.(type) {
	case json.Number:
		return value.String()
	case map[string]any:
		for key, field := range value {
			value[key] = quoteInt64Fields(field)
		}
	case []any:
		for i, item := range value {
			value[i] = quoteInt64Fields(item)
		}
	}
	return value
}
