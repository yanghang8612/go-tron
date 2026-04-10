// Command txsign reads an unsigned transaction JSON (tron-format) from stdin,
// signs it with the given private key, and writes the signed transaction as
// protojson to stdout (suitable for broadcasttransaction endpoint).
//
// The input JSON must contain a "raw_data_hex" field (hex-encoded marshaled
// TransactionRaw protobuf).
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
	corepb "github.com/tronprotocol/go-tron/proto/core"
	_ "github.com/tronprotocol/go-tron/proto/core/contract" // register proto types for Any resolution
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
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

	// Unmarshal raw_data from protobuf bytes
	var rawData corepb.TransactionRaw
	if err := proto.Unmarshal(rawBytes, &rawData); err != nil {
		fmt.Fprintf(os.Stderr, "unmarshal raw_data proto: %v\n", err)
		os.Exit(1)
	}

	// Sign: SHA256(rawBytes) → ECDSA
	hash := sha256.Sum256(rawBytes)
	sig, err := crypto.Sign(hash[:], privKey)
	if err != nil {
		fmt.Fprintf(os.Stderr, "sign: %v\n", err)
		os.Exit(1)
	}

	// Build signed Transaction protobuf
	tx := &corepb.Transaction{
		RawData:   &rawData,
		Signature: [][]byte{sig},
	}

	// Output as protojson (broadcasttransaction expects this format)
	out, err := protojson.Marshal(tx)
	if err != nil {
		fmt.Fprintf(os.Stderr, "marshal signed tx: %v\n", err)
		os.Exit(1)
	}

	fmt.Println(string(out))
}
