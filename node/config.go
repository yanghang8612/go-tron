package node

type Config struct {
	DataDir     string
	P2PPort     int
	HTTPPort    int
	JSONRPCPort int
	SeedNodes   []string // "host:port" entries for initial peer discovery
	MaxPeers    int      // max simultaneous peers, default 30
}
