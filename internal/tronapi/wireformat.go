package tronapi

import (
	"encoding/hex"
	"errors"
	"fmt"
	"strings"

	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/crypto"
)

// parseAddress decodes an address string from a HTTP request body. The wire
// form depends on the body's `visible` flag — java-tron's convention is:
//   - visible=true  ⇒ Base58Check string ("TPL66VK2..." / "T...")
//   - visible=false ⇒ hex with optional "0x" prefix (21 bytes ⇒ 42 hex chars)
//
// Returns a non-nil error for malformed inputs instead of silently
// returning the zero address (the old common.FromHex pattern). A bad
// address routing to addr(0) is a real money-loss vector — let the HTTP
// handler surface a 400 instead.
func parseAddress(s string, visible bool) (common.Address, error) {
	if s == "" {
		return common.Address{}, errors.New("empty address")
	}
	if visible {
		// Base58Check is fixed-length (21 raw + 4 checksum) — the helper
		// enforces it. java-tron's Wallet.decodeFromBase58Check throws on
		// length/checksum mismatch.
		return crypto.Base58ToAddress(s)
	}
	raw, err := hex.DecodeString(strings.TrimPrefix(strings.TrimPrefix(s, "0X"), "0x"))
	if err != nil {
		return common.Address{}, fmt.Errorf("decode hex address: %w", err)
	}
	// Hex path is length-lenient: BytesToAddress left-pads short inputs.
	// java-tron's HTTP layer accepts the same (Hex.toBytes followed by
	// ByteArray copy into a 21-byte buffer). The strictness gain over the
	// old common.FromHex is that hex.DecodeString surfaces "not a hex
	// character" — a typo that previously routed silently to addr(0).
	return common.BytesToAddress(raw), nil
}

// parseBytes decodes a bytes field from a HTTP request body. visible=true
// means the input is plain UTF-8 (java-tron sends asset names, URLs,
// memos, ABIs, and contract bytecode this way); visible=false means hex.
//
// Returns (nil, nil) for empty strings — many request fields are
// optional, and the prior common.FromHex helper handled empty by
// returning nil.
//
// Odd-length hex strings are left-padded with a '0' (matching java-tron's
// Hex.toBytes behaviour and the legacy common.FromHex). Non-hex
// characters now surface as an error rather than silently producing nil —
// the silent path was the audit-flagged money-loss vector.
func parseBytes(s string, visible bool) ([]byte, error) {
	if s == "" {
		return nil, nil
	}
	if visible {
		return []byte(s), nil
	}
	s = strings.TrimPrefix(strings.TrimPrefix(s, "0X"), "0x")
	if len(s)%2 == 1 {
		s = "0" + s
	}
	return hex.DecodeString(s)
}
