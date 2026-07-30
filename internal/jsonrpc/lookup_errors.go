package jsonrpc

import "strings"

func blockLookupNotFound(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	return strings.Contains(msg, "block") && strings.Contains(msg, "not found")
}
