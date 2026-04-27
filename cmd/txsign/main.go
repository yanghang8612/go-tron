// Command txsign reads an unsigned transaction JSON (tron-format) from stdin,
// signs it with the given private key, and writes the signed transaction JSON
// to stdout (suitable for broadcasttransaction endpoint).
//
// The input JSON must contain a "raw_data_hex" field (hex-encoded marshaled
// TransactionRaw protobuf).
//
// The output JSON contains "raw_data_hex" (passthrough) and "signature"
// (array of hex-encoded signature bytes), matching the broadcasttransaction
// wire format.
//
// Usage: echo '{"txID":"...","raw_data":{...},"raw_data_hex":"..."}' | txsign <hex-private-key>
package main

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"

	"github.com/tronprotocol/go-tron/crypto"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "usage: txsign <hex-private-key>")
		os.Exit(1)
	}

	keyBytes, err := hex.DecodeString(os.Args[1])
	if err != nil {
		fmt.Fprintf(os.Stderr, "invalid hex key: %v\n", err)
		os.Exit(1)
	}
	privKey, err := crypto.BytesToPrivateKey(keyBytes)
	if err != nil {
		fmt.Fprintf(os.Stderr, "invalid private key: %v\n", err)
		os.Exit(1)
	}

	input, err := io.ReadAll(os.Stdin)
	if err != nil {
		fmt.Fprintf(os.Stderr, "read stdin: %v\n", err)
		os.Exit(1)
	}

	// Parse as generic JSON to get raw_data_hex
	var txMap map[string]interface{}
	if err := json.Unmarshal(input, &txMap); err != nil {
		fmt.Fprintf(os.Stderr, "unmarshal JSON: %v\n", err)
		os.Exit(1)
	}

	rawHex, ok := txMap["raw_data_hex"].(string)
	if !ok || rawHex == "" {
		fmt.Fprintln(os.Stderr, "missing raw_data_hex field")
		os.Exit(1)
	}

	rawBytes, err := hex.DecodeString(rawHex)
	if err != nil {
		fmt.Fprintf(os.Stderr, "decode raw_data_hex: %v\n", err)
		os.Exit(1)
	}

	// Sign: SHA256(rawBytes) → ECDSA
	hash := sha256.Sum256(rawBytes)
	sig, err := crypto.Sign(hash[:], privKey)
	if err != nil {
		fmt.Fprintf(os.Stderr, "sign: %v\n", err)
		os.Exit(1)
	}

	// Compute txID
	txID := hex.EncodeToString(hash[:])

	// Output hex-format JSON matching broadcasttransaction wire format
	out := map[string]interface{}{
		"txID":         txID,
		"raw_data_hex": rawHex,
		"signature":    []string{hex.EncodeToString(sig)},
	}
	outBytes, err := json.Marshal(out)
	if err != nil {
		fmt.Fprintf(os.Stderr, "marshal output: %v\n", err)
		os.Exit(1)
	}

	fmt.Println(string(outBytes))
}
