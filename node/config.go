package node

type Config struct {
	DataDir     string
	P2PPort     int
	// DiscoverPort is the UDP port used by Kademlia discovery. 0 → reuse P2PPort
	// (java-tron's default; UDP and TCP share the same port number).
	DiscoverPort int
	HTTPPort     int
	JSONRPCPort  int
	GRPCPort     int      // gRPC Wallet service port; 0 = disabled
	SeedNodes    []string // "host:port" entries for initial peer discovery
	MaxPeers     int      // max simultaneous peers, default 30

	// NetworkID matches the value java-tron peers send in HelloMessage. Defaults
	// to 1 (libp2p default). Mainnet/Nile should override via params.
	NetworkID int32

	// PersistentNodeID is a 64-byte random node ID. If empty, the node will
	// generate one on first start and persist to <DataDir>/nodekey.
	PersistentNodeID []byte

	// ExternalIP is the IPv4 address this node advertises in HelloMessage.from.
	// If empty, "127.0.0.1" is used (acceptable for dev; production should set
	// the actual external IP).
	ExternalIP string
}
