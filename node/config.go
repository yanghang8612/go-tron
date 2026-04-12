package node

type Config struct {
	DataDir     string
	P2PPort     int
	HTTPPort    int
	JSONRPCPort int
	SeedNodes   []string // "host:port" entries for initial peer discovery
	MaxPeers    int      // max simultaneous peers, default 30
	PrivateKey  []byte   // secp256k1 private key bytes (32 bytes); generated on first start if empty
}
