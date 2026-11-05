package main

import (
	"net"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/tronprotocol/go-tron/core/rawdb"
)

func TestGtronRestartOnlyRequiresTarget(t *testing.T) {
	for _, name := range []string{"sync.restart-only", "sync.restart-require-incremental"} {
		if err := gtron(storedReplayTestContext(t, "--"+name)); err == nil || !strings.Contains(err.Error(), "requires --sync.restart-from") {
			t.Fatalf("--%s without target: %v", name, err)
		}
	}
}

func TestGtronRestartOnlyClosesBeforeListeners(t *testing.T) {
	fixture := seedStoredReplayTestDatadir(t, false)
	if err := gtron(storedReplayTestContext(t, append(fixture.args(filepath.Join(t.TempDir(), "replay.jsonl"), "2"), "--history.enabled=true")...)); err != nil {
		t.Fatal(err)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer storedReplayTestClose(t, listener)
	port := strconv.Itoa(listener.Addr().(*net.TCPAddr).Port)
	args := []string{"--datadir", fixture.dir, "--genesis", fixture.genesisPath,
		"--history.enabled=true", "--sync.restart-from", "1", "--sync.restart-only", "--sync.restart-require-incremental",
		"--freezer.disable", "--db.cache", "32", "--db.memtable", "8", "--state.trie.cache", "0"}
	for _, name := range []string{"p2p.port", "http.port", "jsonrpc.port", "grpc.port", "pprof.port"} {
		args = append(args, "--"+name, port)
	}
	if err := gtron(storedReplayTestContext(t, args...)); err != nil {
		t.Fatalf("offline rewind: %v", err)
	}
	assertStoredReplayNoNodeKey(t, fixture.dir)
	db, err := openPebbleDB(makeDBFlagSet(t, nil), chainDataDir(fixture.dir))
	if err != nil {
		t.Fatalf("reopen after offline rewind: %v", err)
	}
	defer storedReplayTestClose(t, db)
	if got := rawdb.ReadHeadBlockHash(db); got != fixture.blocks[0].Hash() {
		t.Fatalf("durable head = %s, want %s", got.Hex(), fixture.blocks[0].Hash().Hex())
	}
}
