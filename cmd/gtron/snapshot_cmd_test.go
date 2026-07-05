package main

import (
	"bytes"
	"crypto/ed25519"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/tronprotocol/go-tron/common"
	corepkg "github.com/tronprotocol/go-tron/core"
	chainfreezer "github.com/tronprotocol/go-tron/core/freezer"
	"github.com/tronprotocol/go-tron/core/rawdb"
	rawdbfreezer "github.com/tronprotocol/go-tron/core/rawdb/freezer"
	corestate "github.com/tronprotocol/go-tron/core/state"
	"github.com/tronprotocol/go-tron/core/state/kvdomains"
	statesnapshots "github.com/tronprotocol/go-tron/core/state/snapshots"
	"github.com/tronprotocol/go-tron/core/txpool"
	coretypes "github.com/tronprotocol/go-tron/core/types"
	jsonrpcapi "github.com/tronprotocol/go-tron/internal/jsonrpc"
	"github.com/tronprotocol/go-tron/internal/tronapi"
	syncdl "github.com/tronprotocol/go-tron/net/sync/downloader"
	"github.com/tronprotocol/go-tron/params"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	contractpb "github.com/tronprotocol/go-tron/proto/core/contract"
	"github.com/urfave/cli/v2"
	"google.golang.org/protobuf/proto"
)

const snapshotTestWitnessKey = "4c0883a69102937d6231471b5dbb6204fe512961708279bfd99babc3f98c7c2d"

func TestParseSnapshotTrustedCatalogKeys(t *testing.T) {
	pub, _, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatalf("GenerateKey: %v", err)
	}
	keys, err := parseSnapshotTrustedCatalogKeys([]string{
		"0x" + hex.EncodeToString(pub),
		"ed25519:" + hex.EncodeToString(pub),
	})
	if err != nil {
		t.Fatalf("parseSnapshotTrustedCatalogKeys: %v", err)
	}
	if len(keys) != 2 {
		t.Fatalf("parsed keys = %d, want 2", len(keys))
	}
	if !strings.EqualFold(hex.EncodeToString(keys[0]), hex.EncodeToString(pub)) {
		t.Fatalf("first key mismatch")
	}
}

func TestParseSnapshotTrustedCatalogKeysRejectsEmpty(t *testing.T) {
	if _, err := parseSnapshotTrustedCatalogKeys(nil); err == nil {
		t.Fatal("expected missing trusted key error")
	}
}

func TestSnapshotTrustedCatalogKeysFromFile(t *testing.T) {
	pub1, _, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatalf("GenerateKey pub1: %v", err)
	}
	pub2, _, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatalf("GenerateKey pub2: %v", err)
	}
	pub3, _, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatalf("GenerateKey pub3: %v", err)
	}
	keyFile := filepath.Join(t.TempDir(), "snapshot-trusted-keys.txt")
	if err := os.WriteFile(keyFile, []byte(fmt.Sprintf(`
# active snapshot catalog keys
0x%s
ed25519:%s, %s # staged rotation overlap
`, hex.EncodeToString(pub1), hex.EncodeToString(pub2), hex.EncodeToString(pub3))), 0o644); err != nil {
		t.Fatalf("write key file: %v", err)
	}

	ctx := makeSnapshotRestoreTestContext(t, []string{
		"--snapshot.trusted-key-file", keyFile,
	})
	keys, err := snapshotTrustedCatalogKeys(ctx)
	if err != nil {
		t.Fatalf("snapshotTrustedCatalogKeys: %v", err)
	}
	if len(keys) != 3 {
		t.Fatalf("keys = %d, want 3", len(keys))
	}
	for i, want := range []ed25519.PublicKey{pub1, pub2, pub3} {
		if !bytes.Equal(keys[i], want) {
			t.Fatalf("key %d = %x, want %x", i, keys[i], want)
		}
	}
}

func TestSnapshotTrustedCatalogKeysFromEnvironment(t *testing.T) {
	pub, _, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatalf("GenerateKey: %v", err)
	}
	ctx := makeSnapshotRestoreTestContextWithEnv(t, nil, map[string]*string{
		"GTRON_SNAPSHOT_TRUSTED_KEY": snapshotTestEnvValue("ed25519:" + hex.EncodeToString(pub)),
	})

	keys, err := snapshotTrustedCatalogKeys(ctx)
	if err != nil {
		t.Fatalf("snapshotTrustedCatalogKeys: %v", err)
	}
	if len(keys) != 1 || !bytes.Equal(keys[0], pub) {
		t.Fatalf("keys = %x, want %x", keys, pub)
	}
}

func TestSnapshotTrustedCatalogKeyFileFromEnvironment(t *testing.T) {
	pub, _, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatalf("GenerateKey: %v", err)
	}
	keyFile := filepath.Join(t.TempDir(), "trusted-keys.txt")
	if err := os.WriteFile(keyFile, []byte(hex.EncodeToString(pub)+"\n"), 0o644); err != nil {
		t.Fatalf("write trusted key file: %v", err)
	}
	ctx := makeSnapshotRestoreTestContextWithEnv(t, nil, map[string]*string{
		"GTRON_SNAPSHOT_TRUSTED_KEY_FILE": snapshotTestEnvValue(keyFile),
	})

	keys, err := snapshotTrustedCatalogKeys(ctx)
	if err != nil {
		t.Fatalf("snapshotTrustedCatalogKeys: %v", err)
	}
	if len(keys) != 1 || !bytes.Equal(keys[0], pub) {
		t.Fatalf("keys = %x, want %x", keys, pub)
	}
}

func TestSnapshotTrustedCatalogKeyFileFlagOverridesEnvironment(t *testing.T) {
	envPub, _, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatalf("GenerateKey env: %v", err)
	}
	flagPub, _, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatalf("GenerateKey flag: %v", err)
	}
	envFile := filepath.Join(t.TempDir(), "env-keys.txt")
	if err := os.WriteFile(envFile, []byte(hex.EncodeToString(envPub)+"\n"), 0o644); err != nil {
		t.Fatalf("write env key file: %v", err)
	}
	flagFile := filepath.Join(t.TempDir(), "flag-keys.txt")
	if err := os.WriteFile(flagFile, []byte(hex.EncodeToString(flagPub)+"\n"), 0o644); err != nil {
		t.Fatalf("write flag key file: %v", err)
	}
	ctx := makeSnapshotRestoreTestContextWithEnv(t, []string{
		"--snapshot.trusted-key-file", flagFile,
	}, map[string]*string{
		"GTRON_SNAPSHOT_TRUSTED_KEY_FILE": snapshotTestEnvValue(envFile),
	})

	keys, err := snapshotTrustedCatalogKeys(ctx)
	if err != nil {
		t.Fatalf("snapshotTrustedCatalogKeys: %v", err)
	}
	if len(keys) != 1 || !bytes.Equal(keys[0], flagPub) {
		t.Fatalf("keys = %x, want flag key %x", keys, flagPub)
	}
}

func TestSnapshotForkConfigHashFromEnvironment(t *testing.T) {
	want := "sha256:" + strings.Repeat("ab", 32)
	ctx := makeSnapshotRestoreTestContextWithEnv(t, nil, map[string]*string{
		"GTRON_SNAPSHOT_FORK_CONFIG_HASH": snapshotTestEnvValue(want),
	})

	got, err := normaliseSnapshotForkConfigHash(ctx.String("snapshot.fork-config-hash"))
	if err != nil {
		t.Fatalf("normaliseSnapshotForkConfigHash: %v", err)
	}
	if got != want {
		t.Fatalf("fork config hash = %q, want %q", got, want)
	}
}

func TestSnapshotRemoteURLUsesEnvironmentDefault(t *testing.T) {
	ctx := makeSnapshotRestoreTestContextWithEnv(t, nil, map[string]*string{
		"GTRON_SNAPSHOT_URL": snapshotTestEnvValue(" https://snapshots.example.invalid/go-tron/mainnet/latest "),
	})

	got, err := snapshotRemoteURL(ctx)
	if err != nil {
		t.Fatalf("snapshotRemoteURL: %v", err)
	}
	if want := "https://snapshots.example.invalid/go-tron/mainnet/latest"; got != want {
		t.Fatalf("snapshotRemoteURL = %q, want %q", got, want)
	}
}

func TestSnapshotRemoteURLFlagOverridesEnvironmentDefault(t *testing.T) {
	ctx := makeSnapshotRestoreTestContextWithEnv(t, []string{
		"--snapshot.url", " https://snapshots.example.invalid/go-tron/nile/latest ",
	}, map[string]*string{
		"GTRON_SNAPSHOT_URL": snapshotTestEnvValue("https://snapshots.example.invalid/go-tron/mainnet/latest"),
	})

	got, err := snapshotRemoteURL(ctx)
	if err != nil {
		t.Fatalf("snapshotRemoteURL: %v", err)
	}
	if want := "https://snapshots.example.invalid/go-tron/nile/latest"; got != want {
		t.Fatalf("snapshotRemoteURL = %q, want %q", got, want)
	}
}

func TestSnapshotRemoteURLRequiresFlagOrEnvironment(t *testing.T) {
	ctx := makeSnapshotRestoreTestContextWithEnv(t, nil, map[string]*string{
		"GTRON_SNAPSHOT_URL": nil,
	})

	if _, err := snapshotRemoteURL(ctx); err == nil || !strings.Contains(err.Error(), "GTRON_SNAPSHOT_URL") {
		t.Fatalf("snapshotRemoteURL error = %v, want missing source hint", err)
	}
}

func TestSnapshotFetchCmdDownloadsSignedRemoteSnapshot(t *testing.T) {
	root := t.TempDir()
	sourceDir := filepath.Join(root, "remote")
	destDir := filepath.Join(root, "downloaded")
	dataDir := filepath.Join(root, "datadir")
	identity, pub, sourceCatalog, refs := writeSnapshotCmdRemoteFetchSource(t, sourceDir)
	server := httptest.NewServer(http.FileServer(http.Dir(sourceDir)))
	defer server.Close()

	ctx := makeSnapshotRestoreTestContext(t, []string{
		"--datadir", dataDir,
		"--snapshot.dir", destDir,
		"--snapshot.url", server.URL,
		"--snapshot.fetch.concurrency", "2",
		"--snapshot.trusted-key", hex.EncodeToString(pub),
	})
	if err := snapshotFetchCmd(ctx); err != nil {
		t.Fatalf("snapshotFetchCmd: %v", err)
	}
	catalog, report, err := statesnapshots.VerifySignedSnapshotCatalog(destDir, identity, []ed25519.PublicKey{pub})
	if err != nil {
		t.Fatalf("VerifySignedSnapshotCatalog(downloaded): %v", err)
	}
	if catalog.VisibleTxStart != 1 || catalog.VisibleTxEnd != 1 || catalog.ManifestChecksum != sourceCatalog.ManifestChecksum {
		t.Fatalf("downloaded catalog = %+v, want range [1,1] checksum %s", catalog, sourceCatalog.ManifestChecksum)
	}
	if report.ActiveSegments != len(refs) {
		t.Fatalf("active segments = %d, want %d", report.ActiveSegments, len(refs))
	}
	manifest, err := statesnapshots.LoadProductionManifest(destDir)
	if err != nil {
		t.Fatalf("LoadProductionManifest(downloaded): %v", err)
	}
	if len(manifest.Segments) != len(refs) {
		t.Fatalf("downloaded manifest segments = %d, want %d", len(manifest.Segments), len(refs))
	}
	for _, name := range []string{statesnapshots.SnapshotCatalogFile, statesnapshots.ManifestFile} {
		snapshotCmdAssertSameFile(t, filepath.Join(sourceDir, name), filepath.Join(destDir, name))
	}
	for _, ref := range refs {
		snapshotCmdAssertSameFile(t, filepath.Join(sourceDir, ref.Path), filepath.Join(destDir, ref.Path))
	}
	mgr, err := statesnapshots.OpenManager(destDir)
	if err != nil {
		t.Fatalf("OpenManager(downloaded): %v", err)
	}
	trace, ok, err := mgr.BlockBalanceTrace(1)
	if err != nil || !ok || trace.GetTimestamp() != 101 {
		t.Fatalf("downloaded balance trace = %+v/%v/%v, want timestamp 101", trace, ok, err)
	}
	traceBlock, balance, ok, err := mgr.AccountTraceAtOrBefore(snapshotCmdRemoteTraceOwner().Bytes(), 1)
	if err != nil || !ok || traceBlock != 1 || balance != 55 {
		t.Fatalf("downloaded account trace = %d/%d/%v/%v, want 1/55/true/nil", traceBlock, balance, ok, err)
	}
}

func TestSnapshotFetchCmdResetRemovesLocalSnapshotDir(t *testing.T) {
	root := t.TempDir()
	sourceDir := filepath.Join(root, "remote")
	destDir := filepath.Join(root, "downloaded")
	dataDir := filepath.Join(root, "datadir")
	identity, pub, _, _ := writeSnapshotCmdRemoteFetchSource(t, sourceDir)
	stalePath := filepath.Join(destDir, "history", "stale.seg")
	if err := os.MkdirAll(filepath.Dir(stalePath), 0o755); err != nil {
		t.Fatalf("mkdir stale dir: %v", err)
	}
	if err := os.WriteFile(stalePath, []byte("stale"), 0o644); err != nil {
		t.Fatalf("write stale file: %v", err)
	}
	server := httptest.NewServer(http.FileServer(http.Dir(sourceDir)))
	defer server.Close()

	ctx := makeSnapshotRestoreTestContext(t, []string{
		"--datadir", dataDir,
		"--snapshot.dir", destDir,
		"--snapshot.url", server.URL,
		"--snapshot.reset",
		"--snapshot.trusted-key", hex.EncodeToString(pub),
	})
	if err := snapshotFetchCmd(ctx); err != nil {
		t.Fatalf("snapshotFetchCmd: %v", err)
	}
	if _, err := os.Stat(stalePath); !os.IsNotExist(err) {
		t.Fatalf("stale snapshot file still exists or stat failed with unexpected error: %v", err)
	}
	if _, _, err := statesnapshots.VerifySignedSnapshotCatalog(destDir, identity, []ed25519.PublicKey{pub}); err != nil {
		t.Fatalf("VerifySignedSnapshotCatalog(downloaded): %v", err)
	}
}

func TestSnapshotFetchCmdPreflightsRemoteURLBeforeReset(t *testing.T) {
	root := t.TempDir()
	destDir := filepath.Join(root, "downloaded")
	dataDir := filepath.Join(root, "datadir")
	stalePath := filepath.Join(destDir, "history", "stale.seg")
	if err := os.MkdirAll(filepath.Dir(stalePath), 0o755); err != nil {
		t.Fatalf("mkdir stale dir: %v", err)
	}
	if err := os.WriteFile(stalePath, []byte("stale"), 0o644); err != nil {
		t.Fatalf("write stale file: %v", err)
	}
	pub, _, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatalf("GenerateKey: %v", err)
	}
	ctx := makeSnapshotRestoreTestContext(t, []string{
		"--datadir", dataDir,
		"--snapshot.dir", destDir,
		"--snapshot.url", "file:///tmp/go-tron-snapshot",
		"--snapshot.reset",
		"--snapshot.trusted-key", hex.EncodeToString(pub),
	})

	err = snapshotFetchCmd(ctx)
	if err == nil || !strings.Contains(err.Error(), "unsupported snapshot URL scheme") {
		t.Fatalf("snapshotFetchCmd err = %v, want unsupported URL scheme", err)
	}
	if _, err := os.Stat(stalePath); err != nil {
		t.Fatalf("stale snapshot file was removed before URL preflight: %v", err)
	}
}

func TestSnapshotFetchCmdPreflightsConcurrencyBeforeReset(t *testing.T) {
	root := t.TempDir()
	destDir := filepath.Join(root, "downloaded")
	dataDir := filepath.Join(root, "datadir")
	stalePath := filepath.Join(destDir, "history", "stale.seg")
	if err := os.MkdirAll(filepath.Dir(stalePath), 0o755); err != nil {
		t.Fatalf("mkdir stale dir: %v", err)
	}
	if err := os.WriteFile(stalePath, []byte("stale"), 0o644); err != nil {
		t.Fatalf("write stale file: %v", err)
	}
	var hits atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		http.Error(w, "unexpected fetch", http.StatusInternalServerError)
	}))
	defer server.Close()
	pub, _, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatalf("GenerateKey: %v", err)
	}
	ctx := makeSnapshotRestoreTestContext(t, []string{
		"--datadir", dataDir,
		"--snapshot.dir", destDir,
		"--snapshot.url", server.URL,
		"--snapshot.reset",
		"--snapshot.fetch.concurrency", "-1",
		"--snapshot.trusted-key", hex.EncodeToString(pub),
	})

	err = snapshotFetchCmd(ctx)
	if err == nil || !strings.Contains(err.Error(), "fetch concurrency -1 must be non-negative") {
		t.Fatalf("snapshotFetchCmd err = %v, want concurrency preflight error", err)
	}
	if _, err := os.Stat(stalePath); err != nil {
		t.Fatalf("stale snapshot file was removed before concurrency preflight: %v", err)
	}
	if got := hits.Load(); got != 0 {
		t.Fatalf("remote snapshot server saw %d requests before concurrency preflight failure, want 0", got)
	}
}

func TestSnapshotVerifyCmdChecksSignedLocalSnapshot(t *testing.T) {
	root := t.TempDir()
	snapshotDir := filepath.Join(root, "snapshot")
	dataDir := filepath.Join(root, "datadir")
	_, pub, _, _ := writeSnapshotCmdRemoteFetchSource(t, snapshotDir)

	ctx := makeSnapshotRestoreTestContext(t, []string{
		"--datadir", dataDir,
		"--snapshot.dir", snapshotDir,
		"--snapshot.trusted-key", hex.EncodeToString(pub),
	})
	if err := snapshotVerifyCmd(ctx); err != nil {
		t.Fatalf("snapshotVerifyCmd: %v", err)
	}
}

func TestSnapshotVerifyCmdAcceptsTrustedKeyFile(t *testing.T) {
	root := t.TempDir()
	snapshotDir := filepath.Join(root, "snapshot")
	dataDir := filepath.Join(root, "datadir")
	_, pub, _, _ := writeSnapshotCmdRemoteFetchSource(t, snapshotDir)
	keyFile := filepath.Join(root, "trusted-keys.txt")
	if err := os.WriteFile(keyFile, []byte("# current catalog signer\n"+hex.EncodeToString(pub)+"\n"), 0o644); err != nil {
		t.Fatalf("write trusted key file: %v", err)
	}

	ctx := makeSnapshotRestoreTestContext(t, []string{
		"--datadir", dataDir,
		"--snapshot.dir", snapshotDir,
		"--snapshot.trusted-key-file", keyFile,
	})
	if err := snapshotVerifyCmd(ctx); err != nil {
		t.Fatalf("snapshotVerifyCmd: %v", err)
	}
}

func TestSnapshotVerifyCmdRejectsUntrustedSigner(t *testing.T) {
	root := t.TempDir()
	snapshotDir := filepath.Join(root, "snapshot")
	dataDir := filepath.Join(root, "datadir")
	writeSnapshotCmdRemoteFetchSource(t, snapshotDir)
	untrustedPub, _, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatalf("GenerateKey untrusted: %v", err)
	}

	ctx := makeSnapshotRestoreTestContext(t, []string{
		"--datadir", dataDir,
		"--snapshot.dir", snapshotDir,
		"--snapshot.trusted-key", hex.EncodeToString(untrustedPub),
	})
	if err := snapshotVerifyCmd(ctx); err == nil || !strings.Contains(err.Error(), "not trusted") {
		t.Fatalf("snapshotVerifyCmd error = %v, want untrusted signer rejection", err)
	}
}

func TestSnapshotBootstrapCmdFetchesAndRestoresCanonicalBoundary(t *testing.T) {
	root := t.TempDir()
	sourceDir := filepath.Join(root, "remote")
	destDir := filepath.Join(root, "downloaded")
	dataDir := filepath.Join(root, "datadir")
	stalePath := filepath.Join(destDir, "stale.seg")
	if err := os.MkdirAll(destDir, 0o755); err != nil {
		t.Fatalf("mkdir dest dir: %v", err)
	}
	if err := os.WriteFile(stalePath, []byte("stale"), 0o644); err != nil {
		t.Fatalf("write stale file: %v", err)
	}

	genesis := params.DefaultMainnetGenesis()
	genesisBlock, err := corepkg.GenesisToBlock(genesis, corestate.NewDatabase(rawdb.WrapKeyValueStore(rawdb.NewMemoryDatabase())))
	if err != nil {
		t.Fatalf("GenesisToBlock: %v", err)
	}
	block1 := snapshotCmdBlock(1)
	stateRoot1 := common.HexToHash("4141414141414141414141414141414141414141414141414141414141414141")
	src := openSnapshotCmdFreezer(t, filepath.Join(root, "src-freezer"))
	defer src.Close()
	appendSnapshotCmdFreezerRows(t, src, []snapshotCmdFreezerRow{
		{block: genesisBlock, txInfosRaw: snapshotCmdTxInfoRawForBlock(t, genesisBlock)},
		{block: block1, stateRoot: stateRoot1.Bytes()},
	})
	freezerRef, err := statesnapshots.BuildChainFreezerSegmentFromAncient(rawdb.NewFreezerReader(src), sourceDir, "", 0, 1)
	if err != nil {
		t.Fatalf("BuildChainFreezerSegmentFromAncient: %v", err)
	}
	indexRef, err := statesnapshots.BuildChainIndexSegmentFromChainFreezerSegment(sourceDir, freezerRef, "")
	if err != nil {
		t.Fatalf("BuildChainIndexSegmentFromChainFreezerSegment: %v", err)
	}

	stateSnapshotDB := rawdb.NewMemoryDatabase()
	owner := common.BytesToAddress(append([]byte{common.AddressPrefixMainnet}, bytes.Repeat([]byte{0x25}, common.AccountIDLength)...))
	if err := rawdb.WriteStateTxRange(stateSnapshotDB, block1.Number(), block1.Hash(), 1, 1); err != nil {
		t.Fatalf("WriteStateTxRange: %v", err)
	}
	if err := rawdb.WriteStateDomainChange(stateSnapshotDB, &rawdb.StateDomainChange{
		BlockNum:   block1.Number(),
		BlockHash:  block1.Hash(),
		TxNum:      1,
		Seq:        1,
		FlatDomain: rawdb.StateFlatDomainAccountLatest,
		Owner:      owner,
		NextExists: true,
		Next:       snapshotCmdAccountEnvelope(t, owner, 33, corepb.AccountType_Normal, common.Hash{}),
	}); err != nil {
		t.Fatalf("WriteStateDomainChange: %v", err)
	}
	historyRefs, err := statesnapshots.BuildStateDomainChangeHistorySegmentsFromDB(stateSnapshotDB, sourceDir, 1, 1, "history/state-domain-change-1-1.seg")
	if err != nil {
		t.Fatalf("BuildStateDomainChangeHistorySegmentsFromDB: %v", err)
	}
	identity, err := snapshotExpectedChainIdentityFromGenesis(genesis, "")
	if err != nil {
		t.Fatalf("snapshotExpectedChainIdentityFromGenesis: %v", err)
	}
	segments := append([]statesnapshots.SegmentRef{}, historyRefs...)
	segments = append(segments, freezerRef, indexRef)
	if err := statesnapshots.PublishManifest(sourceDir, statesnapshots.NewManifestForChain(1, 1, segments, identity)); err != nil {
		t.Fatalf("PublishManifest: %v", err)
	}
	pub, priv, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatalf("GenerateKey: %v", err)
	}
	if _, err := statesnapshots.PublishSignedSnapshotCatalog(sourceDir, priv); err != nil {
		t.Fatalf("PublishSignedSnapshotCatalog: %v", err)
	}
	server := httptest.NewServer(http.FileServer(http.Dir(sourceDir)))
	defer server.Close()

	ctx := makeSnapshotRestoreTestContext(t, []string{
		"--datadir", dataDir,
		"--snapshot.dir", destDir,
		"--snapshot.url", server.URL,
		"--snapshot.reset",
		"--snapshot.trusted-key", hex.EncodeToString(pub),
	})
	if err := snapshotBootstrapCmd(ctx); err != nil {
		t.Fatalf("snapshotBootstrapCmd: %v", err)
	}
	if _, err := os.Stat(stalePath); !os.IsNotExist(err) {
		t.Fatalf("stale snapshot file still exists or stat failed with unexpected error: %v", err)
	}
	if _, _, err := statesnapshots.VerifySignedSnapshotCatalog(destDir, identity, []ed25519.PublicKey{pub}); err != nil {
		t.Fatalf("VerifySignedSnapshotCatalog(downloaded): %v", err)
	}

	db, err := openPebbleDB(ctx, chainDataDir(dataDir))
	if err != nil {
		t.Fatalf("open restored db: %v", err)
	}
	defer db.Close()
	fz, err := rawdbfreezer.NewFreezer(ancientDataDir(dataDir), "", true, freezerTableSize, chainfreezer.FreezerTableSet())
	if err != nil {
		t.Fatalf("open restored freezer: %v", err)
	}
	defer fz.Close()
	if got := rawdb.ReadHeadBlockHash(db); got != block1.Hash() {
		t.Fatalf("head hash = %x, want block1 %x", got, block1.Hash())
	}
	if row, ok, err := rawdb.ReadStageProgressRow(db, rawdb.StageExecution); err != nil || !ok || row.BlockNum != block1.Number() || !row.HasBlockHash || row.BlockHash != block1.Hash() {
		t.Fatalf("Execution stage = %+v ok=%v err=%v, want block1 %x", row, ok, err, block1.Hash())
	}
	if got, ok, err := rawdb.ReadStageProgress(db, rawdb.StageChainFreezer); err != nil || !ok || got != block1.Number() {
		t.Fatalf("ChainFreezer stage = %d ok=%v err=%v, want block1", got, ok, err)
	}
	chainDB := rawdb.NewChainDB(db, rawdb.NewFreezerReader(fz))
	if got := rawdb.ReadBlock(chainDB, block1.Number()); got == nil || got.Hash() != block1.Hash() {
		t.Fatalf("cold block1 = %v, want %x", got, block1.Hash())
	}
	if got := rawdb.ReadBlockStateRoot(chainDB, block1.Hash()); got != stateRoot1 {
		t.Fatalf("block1 state root = %x, want %x", got, stateRoot1)
	}
	bc, err := corepkg.NewBlockChainWithAncient(db, corestate.NewDatabase(rawdb.WrapKeyValueStore(db)), params.MainnetChainConfig, rawdb.NewFreezerReader(fz))
	if err != nil {
		t.Fatalf("NewBlockChainWithAncient(restored): %v", err)
	}
	defer bc.Close()
	summary := syncdl.BuildChainSummary(bc)
	if len(summary) == 0 || summary[len(summary)-1] != block1.ID() {
		t.Fatalf("restored sync summary = %+v, want boundary block1 last", summary)
	}
	if got := syncdl.FindCommonBlock(bc, []coretypes.BlockID{block1.ID()}); got != block1.Number() {
		t.Fatalf("restored common block = %d, want boundary block %d", got, block1.Number())
	}
	block2 := coretypes.NewBlockFromPB(&corepb.Block{
		BlockHeader: &corepb.BlockHeader{
			RawData: &corepb.BlockHeaderRaw{
				Number:     2,
				Timestamp:  int64(60_000),
				ParentHash: block1.Hash().Bytes(),
			},
		},
	})
	if err := bc.InsertBlockWithoutVerify(block2); err != nil {
		t.Fatalf("InsertBlockWithoutVerify(block2 after restored boundary): %v", err)
	}
	if got := bc.CurrentBlock(); got == nil || got.Hash() != block2.Hash() {
		t.Fatalf("head after boundary-tail import = %v, want block2 %x", got, block2.Hash())
	}
}

func TestSnapshotBootstrapCmdPreflightsRestoreTargetBeforeFetch(t *testing.T) {
	root := t.TempDir()
	destDir := filepath.Join(root, "downloaded")
	dataDir := filepath.Join(root, "datadir")
	stalePath := filepath.Join(destDir, "stale.seg")
	if err := os.MkdirAll(destDir, 0o755); err != nil {
		t.Fatalf("mkdir dest dir: %v", err)
	}
	if err := os.WriteFile(stalePath, []byte("stale"), 0o644); err != nil {
		t.Fatalf("write stale file: %v", err)
	}
	db, err := rawdb.NewPebbleDB(chainDataDir(dataDir), 256, 500)
	if err != nil {
		t.Fatalf("NewPebbleDB: %v", err)
	}
	rawdb.WriteHeadBlockHash(db, common.Hash{0xee})
	if err := db.Close(); err != nil {
		t.Fatalf("close db: %v", err)
	}

	var hits atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		http.Error(w, "unexpected fetch", http.StatusInternalServerError)
	}))
	defer server.Close()
	pub, _, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatalf("GenerateKey: %v", err)
	}
	ctx := makeSnapshotRestoreTestContext(t, []string{
		"--datadir", dataDir,
		"--snapshot.dir", destDir,
		"--snapshot.url", server.URL,
		"--snapshot.reset",
		"--snapshot.trusted-key", hex.EncodeToString(pub),
	})

	err = snapshotBootstrapCmd(ctx)
	if err == nil || !strings.Contains(err.Error(), "non-genesis datadir") {
		t.Fatalf("snapshotBootstrapCmd err = %v, want non-genesis preflight failure", err)
	}
	if _, err := os.Stat(stalePath); err != nil {
		t.Fatalf("stale snapshot file was removed before restore preflight: %v", err)
	}
	if got := hits.Load(); got != 0 {
		t.Fatalf("remote snapshot server saw %d requests before restore preflight failure, want 0", got)
	}
}

func TestResetSnapshotFetchDirRejectsUnsafeDirs(t *testing.T) {
	for _, dir := range []string{"", " ", ".", string(filepath.Separator)} {
		if err := resetSnapshotFetchDir(dir); err == nil {
			t.Fatalf("resetSnapshotFetchDir(%q) succeeded, want rejection", dir)
		}
	}
}

func TestParseSnapshotCatalogPrivateKeyAcceptsSeed(t *testing.T) {
	seed := strings.Repeat("11", ed25519.SeedSize)
	key, err := parseSnapshotCatalogPrivateKey(seed)
	if err != nil {
		t.Fatalf("parseSnapshotCatalogPrivateKey: %v", err)
	}
	if len(key) != ed25519.PrivateKeySize {
		t.Fatalf("private key length = %d, want %d", len(key), ed25519.PrivateKeySize)
	}
}

func TestSnapshotCatalogSigningKeyFromEnvironment(t *testing.T) {
	seed := strings.Repeat("22", ed25519.SeedSize)
	ctx := makeSnapshotRestoreTestContextWithEnv(t, nil, map[string]*string{
		"GTRON_SNAPSHOT_SIGNING_KEY": snapshotTestEnvValue("ed25519:" + seed),
	})

	key, err := snapshotCatalogSigningKey(ctx)
	if err != nil {
		t.Fatalf("snapshotCatalogSigningKey: %v", err)
	}
	want := ed25519.NewKeyFromSeed(bytes.Repeat([]byte{0x22}, ed25519.SeedSize))
	if !bytes.Equal(key, want) {
		t.Fatalf("signing key = %x, want %x", key, want)
	}
}

func TestSnapshotCatalogSigningKeyFromFile(t *testing.T) {
	flagSeed := strings.Repeat("33", ed25519.SeedSize)
	fileSeed := strings.Repeat("44", ed25519.SeedSize)
	keyFile := filepath.Join(t.TempDir(), "snapshot-signing-key.txt")
	if err := os.WriteFile(keyFile, []byte("# active catalog key\ned25519:"+fileSeed+"\n"), 0o600); err != nil {
		t.Fatalf("write signing key file: %v", err)
	}
	ctx := makeSnapshotRestoreTestContext(t, []string{
		"--snapshot.signing-key", flagSeed,
		"--snapshot.signing-key-file", keyFile,
	})

	key, err := snapshotCatalogSigningKey(ctx)
	if err != nil {
		t.Fatalf("snapshotCatalogSigningKey: %v", err)
	}
	want := ed25519.NewKeyFromSeed(bytes.Repeat([]byte{0x44}, ed25519.SeedSize))
	if !bytes.Equal(key, want) {
		t.Fatalf("signing key = %x, want file key %x", key, want)
	}
}

func TestSnapshotCatalogSigningKeyFileRejectsMultipleKeys(t *testing.T) {
	keyFile := filepath.Join(t.TempDir(), "snapshot-signing-key.txt")
	if err := os.WriteFile(keyFile, []byte(strings.Repeat("55", ed25519.SeedSize)+"\n"+strings.Repeat("66", ed25519.SeedSize)+"\n"), 0o600); err != nil {
		t.Fatalf("write signing key file: %v", err)
	}
	ctx := makeSnapshotRestoreTestContext(t, []string{
		"--snapshot.signing-key-file", keyFile,
	})

	if _, err := snapshotCatalogSigningKey(ctx); err == nil || !strings.Contains(err.Error(), "multiple keys") {
		t.Fatalf("snapshotCatalogSigningKey multiple keys err = %v, want multiple keys", err)
	}
}

func TestNormaliseSnapshotForkConfigHash(t *testing.T) {
	raw := strings.Repeat("aa", 32)
	got, err := normaliseSnapshotForkConfigHash("SHA256:" + strings.ToUpper(raw))
	if err != nil {
		t.Fatalf("normaliseSnapshotForkConfigHash: %v", err)
	}
	if want := "sha256:" + raw; got != want {
		t.Fatalf("hash = %q, want %q", got, want)
	}
	if _, err := normaliseSnapshotForkConfigHash("bad"); err == nil {
		t.Fatal("expected bad hash error")
	}
}

func TestSnapshotExpectedChainIdentity(t *testing.T) {
	genesis := &params.Genesis{Config: &params.ChainConfig{P2PVersion: 42}}
	hash := common.Hash{0x12}
	identity := snapshotExpectedChainIdentity(&params.ChainConfig{ChainID: 9}, genesis, hash, "sha256:"+strings.Repeat("aa", 32))
	if identity.ChainID != 9 {
		t.Fatalf("chainID = %d, want 9", identity.ChainID)
	}
	if identity.NetworkID != 42 {
		t.Fatalf("networkID = %d, want 42", identity.NetworkID)
	}
	if identity.GenesisHash != hex.EncodeToString(hash[:]) {
		t.Fatalf("genesisHash = %q, want %q", identity.GenesisHash, hex.EncodeToString(hash[:]))
	}
}

func TestSnapshotExpectedChainIdentityFromGenesis(t *testing.T) {
	genesis := params.DefaultNileGenesis()
	identity, err := snapshotExpectedChainIdentityFromGenesis(genesis, "")
	if err != nil {
		t.Fatalf("snapshotExpectedChainIdentityFromGenesis: %v", err)
	}
	if identity.ChainID != params.NileChainConfig.ChainID {
		t.Fatalf("chainID = %d, want %d", identity.ChainID, params.NileChainConfig.ChainID)
	}
	if identity.NetworkID != params.NileNetworkID {
		t.Fatalf("networkID = %d, want %d", identity.NetworkID, params.NileNetworkID)
	}
	if identity.GenesisHash != hex.EncodeToString(params.NileGenesisHash[:]) {
		t.Fatalf("genesisHash = %q, want %q", identity.GenesisHash, hex.EncodeToString(params.NileGenesisHash[:]))
	}
}

func TestSnapshotBuildFreezerCmdWritesDevChainIdentity(t *testing.T) {
	root := t.TempDir()
	dataDir := filepath.Join(root, "datadir")
	snapshotDir := filepath.Join(root, "snapshot")

	ctx := makeSnapshotRestoreTestContext(t, []string{
		"--datadir", dataDir,
		"--snapshot.dir", snapshotDir,
		"--snapshot.from-block", "0",
		"--snapshot.to-block", "1",
		"--dev",
		"--witness.key", snapshotTestWitnessKey,
	})
	genesis, err := snapshotGenesisFromContext(ctx)
	if err != nil {
		t.Fatalf("snapshotGenesisFromContext: %v", err)
	}
	genesisBlock, err := corepkg.GenesisToBlock(genesis, corestate.NewDatabase(rawdb.WrapKeyValueStore(rawdb.NewMemoryDatabase())))
	if err != nil {
		t.Fatalf("GenesisToBlock: %v", err)
	}

	src := openSnapshotCmdFreezer(t, ancientDataDir(dataDir))
	appendSnapshotCmdFreezerRows(t, src, []snapshotCmdFreezerRow{
		{block: genesisBlock, txInfosRaw: snapshotCmdTxInfoRawForBlock(t, genesisBlock)},
		{block: snapshotCmdBlock(1), stateRoot: common.Hash{0x01}.Bytes()},
	})
	if err := src.Close(); err != nil {
		t.Fatalf("close source freezer: %v", err)
	}

	if err := snapshotBuildFreezerCmd(ctx); err != nil {
		t.Fatalf("snapshotBuildFreezerCmd: %v", err)
	}
	identity, err := snapshotExpectedChainIdentityFromContext(ctx, "")
	if err != nil {
		t.Fatalf("snapshotExpectedChainIdentityFromContext: %v", err)
	}
	manifest, err := statesnapshots.LoadProductionManifest(snapshotDir)
	if err != nil {
		t.Fatalf("LoadProductionManifest: %v", err)
	}
	if manifest.Chain == nil {
		t.Fatal("manifest chain identity is nil")
	}
	if err := manifest.ValidateChainIdentity(identity); err != nil {
		t.Fatalf("manifest chain identity mismatch: %v", err)
	}

	priv := ed25519.NewKeyFromSeed(bytes.Repeat([]byte{0x11}, ed25519.SeedSize))
	catalog, err := statesnapshots.PublishSignedSnapshotCatalog(snapshotDir, priv)
	if err != nil {
		t.Fatalf("PublishSignedSnapshotCatalog: %v", err)
	}
	if _, _, err := statesnapshots.VerifySignedSnapshotCatalog(snapshotDir, identity, []ed25519.PublicKey{priv.Public().(ed25519.PublicKey)}); err != nil {
		t.Fatalf("VerifySignedSnapshotCatalog: %v", err)
	}
	if catalog.Chain == nil {
		t.Fatal("signed catalog chain identity is nil")
	}
}

func TestSnapshotBuildBalanceTracesCmdWritesColdSegment(t *testing.T) {
	root := t.TempDir()
	dataDir := filepath.Join(root, "datadir")
	snapshotDir := filepath.Join(root, "snapshot")
	ctx := makeSnapshotRestoreTestContext(t, []string{
		"--datadir", dataDir,
		"--snapshot.dir", snapshotDir,
		"--snapshot.from-block", "12",
		"--snapshot.to-block", "12",
		"--dev",
		"--witness.key", snapshotTestWitnessKey,
	})
	db, err := openPebbleDB(ctx, chainDataDir(dataDir))
	if err != nil {
		t.Fatalf("openPebbleDB: %v", err)
	}
	owner := common.Address{0x41, 0xaa}
	block12 := snapshotCmdBlock(12)
	if err := rawdb.WriteBlock(db, block12); err != nil {
		t.Fatalf("WriteBlock: %v", err)
	}
	if err := rawdb.WriteBlockBalanceTrace(db, 12, &contractpb.BlockBalanceTrace{
		BlockIdentifier: &contractpb.BlockBalanceTrace_BlockIdentifier{
			Hash:   append([]byte(nil), block12.Hash().Bytes()...),
			Number: 12,
		},
		Timestamp: 1200,
	}); err != nil {
		t.Fatalf("WriteBlockBalanceTrace: %v", err)
	}
	if err := rawdb.WriteAccountTrace(db, owner.Bytes(), 12, 900); err != nil {
		t.Fatalf("WriteAccountTrace: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close db: %v", err)
	}

	if err := snapshotBuildBalanceTracesCmd(ctx); err != nil {
		t.Fatalf("snapshotBuildBalanceTracesCmd: %v", err)
	}
	identity, err := snapshotExpectedChainIdentityFromContext(ctx, "")
	if err != nil {
		t.Fatalf("snapshotExpectedChainIdentityFromContext: %v", err)
	}
	report, err := statesnapshots.VerifyManifestFiles(snapshotDir, statesnapshots.VerifyManifestOptions{
		ExpectedChain:     &identity,
		RequireRegistered: true,
		RequireChecksums:  true,
	})
	if err != nil {
		t.Fatalf("VerifyManifestFiles: %v", err)
	}
	if report.ActiveSegments != 1 {
		t.Fatalf("active segments = %d, want 1", report.ActiveSegments)
	}
	manifest, err := statesnapshots.LoadProductionManifest(snapshotDir)
	if err != nil {
		t.Fatalf("LoadProductionManifest: %v", err)
	}
	if len(manifest.Segments) != 1 || manifest.Segments[0].Kind != statesnapshots.SegmentBalanceTrace {
		t.Fatalf("manifest segments = %+v, want one balance trace segment", manifest.Segments)
	}
	mgr, err := statesnapshots.OpenManager(snapshotDir)
	if err != nil {
		t.Fatalf("OpenManager: %v", err)
	}
	trace, ok, err := mgr.BlockBalanceTrace(12)
	if err != nil || !ok || trace.GetTimestamp() != 1200 {
		t.Fatalf("BlockBalanceTrace = %+v/%v/%v, want timestamp 1200", trace, ok, err)
	}
	block, balance, ok, err := mgr.AccountTraceAtOrBefore(owner.Bytes(), 20)
	if err != nil || !ok || block != 12 || balance != 900 {
		t.Fatalf("AccountTraceAtOrBefore = %d/%d/%v/%v, want 12/900/true/nil", block, balance, ok, err)
	}

	priv := ed25519.NewKeyFromSeed(bytes.Repeat([]byte{0x22}, ed25519.SeedSize))
	if _, err := statesnapshots.PublishSignedSnapshotCatalog(snapshotDir, priv); err != nil {
		t.Fatalf("PublishSignedSnapshotCatalog: %v", err)
	}
	pruneCtx := makeSnapshotRestoreTestContext(t, []string{
		"--datadir", dataDir,
		"--snapshot.dir", snapshotDir,
		"--snapshot.trusted-key", hex.EncodeToString(priv.Public().(ed25519.PublicKey)),
		"--dev",
		"--witness.key", snapshotTestWitnessKey,
	})
	if err := snapshotPruneBalanceTracesCmd(pruneCtx); err != nil {
		t.Fatalf("snapshotPruneBalanceTracesCmd: %v", err)
	}
	reopened, err := openPebbleDB(pruneCtx, chainDataDir(dataDir))
	if err != nil {
		t.Fatalf("reopen db: %v", err)
	}
	defer reopened.Close()
	if got := rawdb.ReadBlockBalanceTrace(reopened, 12); got != nil {
		t.Fatalf("hot BlockBalanceTrace after prune = %+v, want nil", got)
	}
	if balance, ok := rawdb.ReadAccountTrace(reopened, owner.Bytes(), 12); ok || balance != 0 {
		t.Fatalf("hot AccountTrace after prune = %d/%v, want 0/false", balance, ok)
	}
	if got, ok, err := rawdb.ReadStageProgress(reopened, rawdb.StageSnapshotBalanceTracePrune); err != nil || !ok || got != 12 {
		t.Fatalf("StageSnapshotBalanceTracePrune = %d ok=%v err=%v, want 12", got, ok, err)
	}
	chainDB := rawdb.NewChainDB(reopened, rawdb.NoopAncient{})
	chainDB.SetBalanceTraceReader(mgr)
	if got := rawdb.ReadBlockBalanceTrace(chainDB, 12); got == nil || got.GetTimestamp() != 1200 {
		t.Fatalf("cold BlockBalanceTrace after prune = %+v, want timestamp 1200", got)
	}
}

func TestSnapshotBuildBalanceTracesCmdAuditsAncientBlocks(t *testing.T) {
	root := t.TempDir()
	dataDir := filepath.Join(root, "datadir")
	snapshotDir := filepath.Join(root, "snapshot")
	ctx := makeSnapshotRestoreTestContext(t, []string{
		"--datadir", dataDir,
		"--snapshot.dir", snapshotDir,
		"--snapshot.from-block", "12",
		"--snapshot.to-block", "12",
		"--dev",
		"--witness.key", snapshotTestWitnessKey,
	})

	fz := openSnapshotCmdFreezer(t, ancientDataDir(dataDir))
	rows := make([]snapshotCmdFreezerRow, 13)
	for blockNum := range rows {
		rows[blockNum] = snapshotCmdFreezerRow{block: snapshotCmdBlock(uint64(blockNum))}
	}
	appendSnapshotCmdFreezerRows(t, fz, rows)
	if err := fz.Close(); err != nil {
		t.Fatalf("close freezer: %v", err)
	}

	db, err := openPebbleDB(ctx, chainDataDir(dataDir))
	if err != nil {
		t.Fatalf("openPebbleDB: %v", err)
	}
	owner := common.Address{0x41, 0xab}
	block12 := snapshotCmdBlock(12)
	if got := rawdb.ReadBlock(rawdb.NewChainDB(db, rawdb.NoopAncient{}), 12); got != nil {
		t.Fatalf("hot block 12 = %+v, want nil so coverage must use ancient", got)
	}
	if err := rawdb.WriteBlockBalanceTrace(db, 12, &contractpb.BlockBalanceTrace{
		BlockIdentifier: &contractpb.BlockBalanceTrace_BlockIdentifier{
			Hash:   append([]byte(nil), block12.Hash().Bytes()...),
			Number: 12,
		},
		Timestamp: 1212,
	}); err != nil {
		t.Fatalf("WriteBlockBalanceTrace: %v", err)
	}
	if err := rawdb.WriteAccountTrace(db, owner.Bytes(), 12, 912); err != nil {
		t.Fatalf("WriteAccountTrace: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close db: %v", err)
	}

	if err := snapshotBuildBalanceTracesCmd(ctx); err != nil {
		t.Fatalf("snapshotBuildBalanceTracesCmd: %v", err)
	}
	mgr, err := statesnapshots.OpenManager(snapshotDir)
	if err != nil {
		t.Fatalf("OpenManager: %v", err)
	}
	trace, ok, err := mgr.BlockBalanceTrace(12)
	if err != nil || !ok || trace.GetTimestamp() != 1212 {
		t.Fatalf("BlockBalanceTrace = %+v/%v/%v, want timestamp 1212", trace, ok, err)
	}
}

func TestSnapshotBuildBalanceTracesCmdRejectsIncompleteCoverage(t *testing.T) {
	root := t.TempDir()
	dataDir := filepath.Join(root, "datadir")
	snapshotDir := filepath.Join(root, "snapshot")
	ctx := makeSnapshotRestoreTestContext(t, []string{
		"--datadir", dataDir,
		"--snapshot.dir", snapshotDir,
		"--snapshot.from-block", "10",
		"--snapshot.to-block", "11",
		"--dev",
		"--witness.key", snapshotTestWitnessKey,
	})
	db, err := openPebbleDB(ctx, chainDataDir(dataDir))
	if err != nil {
		t.Fatalf("openPebbleDB: %v", err)
	}
	block10 := snapshotCmdBlock(10)
	block11 := snapshotCmdBlock(11)
	for _, block := range []*coretypes.Block{block10, block11} {
		if err := rawdb.WriteBlock(db, block); err != nil {
			t.Fatalf("WriteBlock %d: %v", block.Number(), err)
		}
	}
	if err := rawdb.WriteBlockBalanceTrace(db, 10, &contractpb.BlockBalanceTrace{
		BlockIdentifier: &contractpb.BlockBalanceTrace_BlockIdentifier{
			Hash:   append([]byte(nil), block10.Hash().Bytes()...),
			Number: 10,
		},
	}); err != nil {
		t.Fatalf("WriteBlockBalanceTrace: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close db: %v", err)
	}

	err = snapshotBuildBalanceTracesCmd(ctx)
	if err == nil || !strings.Contains(err.Error(), "requires complete coverage") {
		t.Fatalf("snapshotBuildBalanceTracesCmd error = %v, want complete coverage error", err)
	}
}

func TestSnapshotBuildSectionBloomsCmdWritesColdSegment(t *testing.T) {
	root := t.TempDir()
	dataDir := filepath.Join(root, "datadir")
	snapshotDir := filepath.Join(root, "snapshot")
	ctx := makeSnapshotRestoreTestContext(t, []string{
		"--datadir", dataDir,
		"--snapshot.dir", snapshotDir,
		"--snapshot.from-block", "0",
		"--snapshot.to-block", fmt.Sprint(rawdb.SectionBloomBlockPerSection*2 - 1),
		"--dev",
		"--witness.key", snapshotTestWitnessKey,
	})
	db, err := openPebbleDB(ctx, chainDataDir(dataDir))
	if err != nil {
		t.Fatalf("openPebbleDB: %v", err)
	}
	rowA := snapshotCmdSectionBloomEncodedBit(t, 5)
	rowB := snapshotCmdSectionBloomEncodedBit(t, 9)
	if err := rawdb.WriteSectionBloom(db, 0, 42, rowA); err != nil {
		t.Fatalf("WriteSectionBloom 0/42: %v", err)
	}
	if err := rawdb.WriteSectionBloom(db, 1, 99, rowB); err != nil {
		t.Fatalf("WriteSectionBloom 1/99: %v", err)
	}
	sectionEnd := uint64(rawdb.SectionBloomBlockPerSection*2 - 1)
	sectionEndHash := common.Hash{0x42}
	if err := rawdb.WriteStateTxRange(db, sectionEnd, sectionEndHash, sectionEnd, sectionEnd); err != nil {
		t.Fatalf("WriteStateTxRange section end: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close db: %v", err)
	}

	if err := snapshotBuildSectionBloomsCmd(ctx); err != nil {
		t.Fatalf("snapshotBuildSectionBloomsCmd: %v", err)
	}
	identity, err := snapshotExpectedChainIdentityFromContext(ctx, "")
	if err != nil {
		t.Fatalf("snapshotExpectedChainIdentityFromContext: %v", err)
	}
	report, err := statesnapshots.VerifyManifestFiles(snapshotDir, statesnapshots.VerifyManifestOptions{
		ExpectedChain:     &identity,
		RequireRegistered: true,
		RequireChecksums:  true,
	})
	if err != nil {
		t.Fatalf("VerifyManifestFiles: %v", err)
	}
	if report.ActiveSegments != 1 {
		t.Fatalf("active segments = %d, want 1", report.ActiveSegments)
	}
	manifest, err := statesnapshots.LoadProductionManifest(snapshotDir)
	if err != nil {
		t.Fatalf("LoadProductionManifest: %v", err)
	}
	if len(manifest.Segments) != 1 || manifest.Segments[0].Kind != statesnapshots.SegmentSectionBloom {
		t.Fatalf("manifest segments = %+v, want one section bloom segment", manifest.Segments)
	}
	mgr, err := statesnapshots.OpenManager(snapshotDir)
	if err != nil {
		t.Fatalf("OpenManager: %v", err)
	}
	raw, ok, err := mgr.SectionBloom(1, 99)
	if err != nil || !ok || !bytes.Equal(raw, rowB) {
		t.Fatalf("manager SectionBloom = %x/%v/%v, want rowB", raw, ok, err)
	}

	priv := ed25519.NewKeyFromSeed(bytes.Repeat([]byte{0x33}, ed25519.SeedSize))
	if _, err := statesnapshots.PublishSignedSnapshotCatalog(snapshotDir, priv); err != nil {
		t.Fatalf("PublishSignedSnapshotCatalog: %v", err)
	}
	pruneCtx := makeSnapshotRestoreTestContext(t, []string{
		"--datadir", dataDir,
		"--snapshot.dir", snapshotDir,
		"--snapshot.trusted-key", hex.EncodeToString(priv.Public().(ed25519.PublicKey)),
		"--dev",
		"--witness.key", snapshotTestWitnessKey,
	})
	if err := snapshotPruneSectionBloomsCmd(pruneCtx); err != nil {
		t.Fatalf("snapshotPruneSectionBloomsCmd: %v", err)
	}
	reopened, err := openPebbleDB(pruneCtx, chainDataDir(dataDir))
	if err != nil {
		t.Fatalf("reopen db: %v", err)
	}
	defer reopened.Close()
	if got := rawdb.ReadSectionBloom(reopened, 1, 99); got != nil {
		t.Fatalf("hot SectionBloom after prune = %x, want nil", got)
	}
	if got, ok, err := rawdb.ReadStageProgress(reopened, rawdb.StageSnapshotSectionBloomPrune); err != nil || !ok || got != rawdb.SectionBloomBlockPerSection*2-1 {
		t.Fatalf("StageSnapshotSectionBloomPrune = %d ok=%v err=%v, want %d", got, ok, err, rawdb.SectionBloomBlockPerSection*2-1)
	}
	if row, ok, err := rawdb.ReadStageProgressRow(reopened, rawdb.StageSnapshotSectionBloomPrune); err != nil || !ok || !row.HasBlockHash || row.BlockHash != sectionEndHash {
		t.Fatalf("StageSnapshotSectionBloomPrune row = %+v ok=%v err=%v, want hash %x", row, ok, err, sectionEndHash)
	}
	chainDB := rawdb.NewChainDB(reopened, rawdb.NoopAncient{})
	chainDB.SetSectionBloomReader(mgr)
	bitset, ok, err := rawdb.ReadSectionBloomBitSet(chainDB, 1, 99)
	if err != nil || !ok || !rawdb.SectionBloomBitSetHas(bitset, 9) {
		t.Fatalf("cold SectionBloom after prune = %x/%v/%v, want bit 9", bitset, ok, err)
	}
}

func TestSnapshotBuildSectionBloomsCmdRejectsPartialSectionRange(t *testing.T) {
	root := t.TempDir()
	dataDir := filepath.Join(root, "datadir")
	snapshotDir := filepath.Join(root, "snapshot")
	ctx := makeSnapshotRestoreTestContext(t, []string{
		"--datadir", dataDir,
		"--snapshot.dir", snapshotDir,
		"--snapshot.from-block", "0",
		"--snapshot.to-block", fmt.Sprint(rawdb.SectionBloomBlockPerSection - 2),
		"--dev",
		"--witness.key", snapshotTestWitnessKey,
	})
	if err := snapshotBuildSectionBloomsCmd(ctx); err == nil || !strings.Contains(err.Error(), "complete") {
		t.Fatalf("snapshotBuildSectionBloomsCmd partial range error = %v, want complete-section error", err)
	}
}

func TestSnapshotBuildDerivedIndexesCmdWritesColdSegments(t *testing.T) {
	root := t.TempDir()
	dataDir := filepath.Join(root, "datadir")
	snapshotDir := filepath.Join(root, "snapshot")
	etlTemp := filepath.Join(root, "snapshot-etl")
	sectionEnd := uint64(rawdb.SectionBloomBlockPerSection) - 1
	ctx := makeSnapshotRestoreTestContext(t, []string{
		"--datadir", dataDir,
		"--snapshot.dir", snapshotDir,
		"--snapshot.from-block", "0",
		"--snapshot.to-block", fmt.Sprint(sectionEnd),
		"--snapshot.etl.tempdir", etlTemp,
		"--snapshot.etl.buffer", "1",
		"--dev",
		"--witness.key", snapshotTestWitnessKey,
	})
	db, err := openPebbleDB(ctx, chainDataDir(dataDir))
	if err != nil {
		t.Fatalf("openPebbleDB: %v", err)
	}
	owner := common.Address{0x41, 0xbb}
	block12, txHash, _ := snapshotCmdBlockWithTx(t, 12)
	var sectionEndHash common.Hash
	for blockNum := uint64(0); blockNum <= sectionEnd; blockNum++ {
		block := snapshotCmdBlock(blockNum)
		timestamp := int64(30_000 + blockNum)
		if blockNum == 12 {
			block = block12
			timestamp = 1200
		}
		if blockNum == sectionEnd {
			sectionEndHash = block.Hash()
		}
		if err := rawdb.WriteBlock(db, block); err != nil {
			t.Fatalf("WriteBlock %d: %v", blockNum, err)
		}
		if err := rawdb.WriteBlockBalanceTrace(db, int64(blockNum), &contractpb.BlockBalanceTrace{
			BlockIdentifier: &contractpb.BlockBalanceTrace_BlockIdentifier{
				Hash:   append([]byte(nil), block.Hash().Bytes()...),
				Number: int64(blockNum),
			},
			Timestamp: timestamp,
		}); err != nil {
			t.Fatalf("WriteBlockBalanceTrace %d: %v", blockNum, err)
		}
	}
	logAddress := []byte{0x41, 0x12, 0x13, 0x14, 0x15}
	logTopic := common.Hash{0xdd}
	if err := rawdb.WriteTransactionInfosByBlock(db, 12, []*corepb.TransactionInfo{{
		Id:          txHash[:],
		BlockNumber: 12,
		Log: []*corepb.TransactionInfo_Log{{
			Address: logAddress,
			Topics:  [][]byte{logTopic[:]},
			Data:    []byte{0x12},
		}},
	}}); err != nil {
		t.Fatalf("WriteTransactionInfosByBlock: %v", err)
	}
	if err := rawdb.WriteAccountTrace(db, owner.Bytes(), 12, 910); err != nil {
		t.Fatalf("WriteAccountTrace: %v", err)
	}
	bloomRow := snapshotCmdSectionBloomEncodedBit(t, 7)
	if err := rawdb.WriteSectionBloom(db, 0, 42, bloomRow); err != nil {
		t.Fatalf("WriteSectionBloom: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close db: %v", err)
	}

	if err := snapshotBuildDerivedIndexesCmd(ctx); err != nil {
		t.Fatalf("snapshotBuildDerivedIndexesCmd: %v", err)
	}
	if _, err := os.Stat(etlTemp); err != nil {
		t.Fatalf("snapshot ETL temp parent stat: %v", err)
	}
	identity, err := snapshotExpectedChainIdentityFromContext(ctx, "")
	if err != nil {
		t.Fatalf("snapshotExpectedChainIdentityFromContext: %v", err)
	}
	report, err := statesnapshots.VerifyManifestFiles(snapshotDir, statesnapshots.VerifyManifestOptions{
		ExpectedChain:     &identity,
		RequireRegistered: true,
		RequireChecksums:  true,
	})
	if err != nil {
		t.Fatalf("VerifyManifestFiles: %v", err)
	}
	if report.ActiveSegments != 4 {
		t.Fatalf("active segments = %d, want 4", report.ActiveSegments)
	}
	manifest, err := statesnapshots.LoadProductionManifest(snapshotDir)
	if err != nil {
		t.Fatalf("LoadProductionManifest: %v", err)
	}
	var haveBalanceTrace, haveSectionBloom, haveEventLog, haveEventLogIndex bool
	for _, ref := range manifest.Segments {
		switch ref.Kind {
		case statesnapshots.SegmentBalanceTrace:
			haveBalanceTrace = true
		case statesnapshots.SegmentSectionBloom:
			haveSectionBloom = true
		case statesnapshots.SegmentEventLog:
			haveEventLog = true
		case statesnapshots.SegmentEventLogIndex:
			haveEventLogIndex = true
		}
	}
	if !haveBalanceTrace || !haveSectionBloom || !haveEventLog || !haveEventLogIndex {
		t.Fatalf("manifest segments = %+v, want balance trace, section bloom, event log, and event log index segments", manifest.Segments)
	}
	mgr, err := statesnapshots.OpenManager(snapshotDir)
	if err != nil {
		t.Fatalf("OpenManager: %v", err)
	}
	trace, ok, err := mgr.BlockBalanceTrace(12)
	if err != nil || !ok || trace.GetTimestamp() != 1200 {
		t.Fatalf("BlockBalanceTrace = %+v/%v/%v, want timestamp 1200", trace, ok, err)
	}
	block, balance, ok, err := mgr.AccountTraceAtOrBefore(owner.Bytes(), 12)
	if err != nil || !ok || block != 12 || balance != 910 {
		t.Fatalf("AccountTraceAtOrBefore = %d/%d/%v/%v, want 12/910/true/nil", block, balance, ok, err)
	}
	raw, ok, err := mgr.SectionBloom(0, 42)
	if err != nil || !ok || !bytes.Equal(raw, bloomRow) {
		t.Fatalf("SectionBloom = %x/%v/%v, want bloom row", raw, ok, err)
	}
	covered, err := mgr.EventLogRangeCovered(12, 12)
	if err != nil || !covered {
		t.Fatalf("EventLogRangeCovered = %v/%v, want true/nil", covered, err)
	}
	covered, err = mgr.EventLogIndexedRangeCovered(12, 12)
	if err != nil || !covered {
		t.Fatalf("EventLogIndexedRangeCovered = %v/%v, want true/nil", covered, err)
	}
	var eventRows []rawdb.EventLog
	if err := mgr.IterateEventLogs(12, 12, rawdb.EventLogFilter{
		Addresses: []common.Address{common.BytesToAddress(logAddress)},
		Topics:    [][]common.Hash{{logTopic}},
	}, func(row rawdb.EventLog) (bool, error) {
		eventRows = append(eventRows, row)
		return true, nil
	}); err != nil {
		t.Fatalf("IterateEventLogs: %v", err)
	}
	if len(eventRows) != 1 || eventRows[0].BlockNum != 12 || !bytes.Equal(eventRows[0].Log.GetData(), []byte{0x12}) {
		t.Fatalf("event rows = %+v, want one cold event log", eventRows)
	}
	reopened, err := openPebbleDB(ctx, chainDataDir(dataDir))
	if err != nil {
		t.Fatalf("reopen db: %v", err)
	}
	defer reopened.Close()
	if got, ok, err := rawdb.ReadStageProgress(reopened, rawdb.StageSnapshotEventLogBuild); err != nil || !ok || got != sectionEnd {
		t.Fatalf("StageSnapshotEventLogBuild = %d ok=%v err=%v, want %d", got, ok, err, sectionEnd)
	}
	if row, ok, err := rawdb.ReadStageProgressRow(reopened, rawdb.StageSnapshotEventLogBuild); err != nil || !ok || !row.HasBlockHash || row.BlockHash != sectionEndHash {
		t.Fatalf("StageSnapshotEventLogBuild row = %+v ok=%v err=%v, want hash %x", row, ok, err, sectionEndHash)
	}
}

func TestSnapshotBuildEventLogsCmdWritesColdSegment(t *testing.T) {
	root := t.TempDir()
	dataDir := filepath.Join(root, "datadir")
	snapshotDir := filepath.Join(root, "snapshot")
	ctx := makeSnapshotRestoreTestContext(t, []string{
		"--datadir", dataDir,
		"--snapshot.dir", snapshotDir,
		"--snapshot.from-block", "7",
		"--snapshot.to-block", "7",
		"--dev",
		"--witness.key", snapshotTestWitnessKey,
	})
	db, err := openPebbleDB(ctx, chainDataDir(dataDir))
	if err != nil {
		t.Fatalf("openPebbleDB: %v", err)
	}
	block7, txHash, _ := snapshotCmdBlockWithTx(t, 7)
	logAddress := []byte{0x41, 0x77, 0x78, 0x79, 0x7a}
	logTopic := common.Hash{0xee}
	if err := rawdb.WriteBlock(db, block7); err != nil {
		t.Fatalf("WriteBlock: %v", err)
	}
	if err := rawdb.WriteTransactionInfosByBlock(db, 7, []*corepb.TransactionInfo{{
		Id:          txHash[:],
		BlockNumber: 7,
		Log: []*corepb.TransactionInfo_Log{{
			Address: logAddress,
			Topics:  [][]byte{logTopic[:]},
			Data:    []byte{0x07},
		}},
	}}); err != nil {
		t.Fatalf("WriteTransactionInfosByBlock: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close db: %v", err)
	}

	if err := snapshotBuildEventLogsCmd(ctx); err != nil {
		t.Fatalf("snapshotBuildEventLogsCmd: %v", err)
	}
	identity, err := snapshotExpectedChainIdentityFromContext(ctx, "")
	if err != nil {
		t.Fatalf("snapshotExpectedChainIdentityFromContext: %v", err)
	}
	report, err := statesnapshots.VerifyManifestFiles(snapshotDir, statesnapshots.VerifyManifestOptions{
		ExpectedChain:     &identity,
		RequireRegistered: true,
		RequireChecksums:  true,
	})
	if err != nil {
		t.Fatalf("VerifyManifestFiles: %v", err)
	}
	if report.ActiveSegments != 2 {
		t.Fatalf("active segments = %d, want 2", report.ActiveSegments)
	}
	mgr, err := statesnapshots.OpenManager(snapshotDir)
	if err != nil {
		t.Fatalf("OpenManager: %v", err)
	}
	covered, err := mgr.EventLogRangeCovered(7, 7)
	if err != nil || !covered {
		t.Fatalf("EventLogRangeCovered = %v/%v, want true/nil", covered, err)
	}
	covered, err = mgr.EventLogIndexedRangeCovered(7, 7)
	if err != nil || !covered {
		t.Fatalf("EventLogIndexedRangeCovered = %v/%v, want true/nil", covered, err)
	}
	var rows []rawdb.EventLog
	if err := mgr.IterateEventLogs(7, 7, rawdb.EventLogFilter{
		Addresses: []common.Address{common.BytesToAddress(logAddress)},
		Topics:    [][]common.Hash{{logTopic}},
	}, func(row rawdb.EventLog) (bool, error) {
		rows = append(rows, row)
		return true, nil
	}); err != nil {
		t.Fatalf("IterateEventLogs: %v", err)
	}
	if len(rows) != 1 || rows[0].TxHash != txHash || !bytes.Equal(rows[0].Log.GetData(), []byte{0x07}) {
		t.Fatalf("event rows = %+v, want tx %x with data 07", rows, txHash)
	}
	reopened, err := openPebbleDB(ctx, chainDataDir(dataDir))
	if err != nil {
		t.Fatalf("reopen db: %v", err)
	}
	defer reopened.Close()
	if got, ok, err := rawdb.ReadStageProgress(reopened, rawdb.StageSnapshotEventLogBuild); err != nil || ok {
		t.Fatalf("StageSnapshotEventLogBuild = %d ok=%v err=%v, want absent without continuous event-log coverage from block 1", got, ok, err)
	}

	statsOutput, err := captureDBCmdStdout(t, func() error {
		return snapshotEventLogIndexStatsCmd(ctx)
	})
	if err != nil {
		t.Fatalf("snapshotEventLogIndexStatsCmd: %v", err)
	}
	for _, want := range []string{
		"Event log index stats:",
		"segments=1",
		"fromBlock=7",
		"toBlock=7",
		"addressKeys=1",
		"addressPostings=1",
		"addressAvgPostingsMilli=1000",
		"addressSingletonKeys=1",
		"addressMultiPostingKeys=0",
		"topicKeys=1",
		"topicPostings=1",
		"topicAvgPostingsMilli=1000",
		"topicSingletonKeys=1",
		"topicMultiPostingKeys=0",
		"Event log index segment:",
		"range=[7,7]",
	} {
		if !strings.Contains(statsOutput, want) {
			t.Fatalf("event-log-index stats output missing %q:\n%s", want, statsOutput)
		}
	}
}

func TestSnapshotBuildEventLogsCmdFromColdRematerializesSegment(t *testing.T) {
	root := t.TempDir()
	dataDir := filepath.Join(root, "datadir")
	snapshotDir := filepath.Join(root, "snapshot")
	source := rawdb.NewMemoryChainDB()
	block7, txHash, _ := snapshotCmdBlockWithTx(t, 7)
	logAddress := []byte{0x41, 0x87, 0x88, 0x89, 0x8a}
	logTopic := common.Hash{0x87}
	if err := rawdb.WriteBlock(source, block7); err != nil {
		t.Fatalf("WriteBlock: %v", err)
	}
	if err := rawdb.WriteTransactionInfosByBlock(source, 7, []*corepb.TransactionInfo{{
		Id:          txHash[:],
		BlockNumber: 7,
		Log: []*corepb.TransactionInfo_Log{{
			Address: logAddress,
			Topics:  [][]byte{logTopic[:]},
			Data:    []byte{0x87},
		}},
	}}); err != nil {
		t.Fatalf("WriteTransactionInfosByBlock: %v", err)
	}
	ref, err := statesnapshots.BuildEventLogSegmentFromChain(source, snapshotDir, "log/event-log-7-7.seg", 7, 7)
	if err != nil {
		t.Fatalf("BuildEventLogSegmentFromChain: %v", err)
	}
	indexRef, err := statesnapshots.BuildEventLogIndexSegmentFromEventLogSegments(snapshotDir, []statesnapshots.SegmentRef{ref}, "")
	if err != nil {
		t.Fatalf("BuildEventLogIndexSegmentFromEventLogSegments: %v", err)
	}
	if err := statesnapshots.PublishManifest(snapshotDir, statesnapshots.NewManifest(0, 0, []statesnapshots.SegmentRef{ref, indexRef})); err != nil {
		t.Fatalf("PublishManifest: %v", err)
	}

	ctx := makeSnapshotRestoreTestContext(t, []string{
		"--datadir", dataDir,
		"--snapshot.dir", snapshotDir,
		"--snapshot.from-block", "7",
		"--snapshot.to-block", "7",
		"--snapshot.from-cold",
		"--snapshot.etl.buffer", "1",
		"--dev",
		"--witness.key", snapshotTestWitnessKey,
	})
	if err := snapshotBuildEventLogsCmd(ctx); err != nil {
		t.Fatalf("snapshotBuildEventLogsCmd from cold: %v", err)
	}
	identity, err := snapshotExpectedChainIdentityFromContext(ctx, "")
	if err != nil {
		t.Fatalf("snapshotExpectedChainIdentityFromContext: %v", err)
	}
	report, err := statesnapshots.VerifyManifestFiles(snapshotDir, statesnapshots.VerifyManifestOptions{
		ExpectedChain:     &identity,
		RequireRegistered: true,
		RequireChecksums:  true,
	})
	if err != nil {
		t.Fatalf("VerifyManifestFiles: %v", err)
	}
	if report.ActiveSegments != 2 {
		t.Fatalf("active segments = %d, want event-log and event-log-index", report.ActiveSegments)
	}
	manifest, err := statesnapshots.LoadProductionManifest(snapshotDir)
	if err != nil {
		t.Fatalf("LoadProductionManifest: %v", err)
	}
	if manifest.Generation < 2 {
		t.Fatalf("manifest generation = %d, want rematerialized generation >= 2", manifest.Generation)
	}
	mgr, err := statesnapshots.OpenManager(snapshotDir)
	if err != nil {
		t.Fatalf("OpenManager: %v", err)
	}
	if covered, err := mgr.EventLogIndexedRangeCovered(7, 7); err != nil || !covered {
		t.Fatalf("EventLogIndexedRangeCovered = %v/%v, want true/nil", covered, err)
	}
	var rows []rawdb.EventLog
	if err := mgr.IterateEventLogs(7, 7, rawdb.EventLogFilter{
		Addresses: []common.Address{common.BytesToAddress(logAddress)},
		Topics:    [][]common.Hash{{logTopic}},
	}, func(row rawdb.EventLog) (bool, error) {
		rows = append(rows, row)
		return true, nil
	}); err != nil {
		t.Fatalf("IterateEventLogs: %v", err)
	}
	if len(rows) != 1 || rows[0].BlockNum != 7 || rows[0].TxHash != txHash || !bytes.Equal(rows[0].Log.GetData(), []byte{0x87}) {
		t.Fatalf("event rows = %+v, want rematerialized block 7 log", rows)
	}
}

func TestSnapshotBuildDerivedIndexesCmdFromColdRematerializesAllDerivedSegments(t *testing.T) {
	root := t.TempDir()
	dataDir := filepath.Join(root, "datadir")
	snapshotDir := filepath.Join(root, "snapshot")
	source := rawdb.NewMemoryChainDB()
	toBlock := uint64(rawdb.SectionBloomBlockPerSection - 1)

	block1, txHash, _ := snapshotCmdBlockWithTx(t, 1)
	for blockNum := uint64(0); blockNum <= toBlock; blockNum++ {
		block := snapshotCmdBlock(blockNum)
		if blockNum == 1 {
			block = block1
		}
		if err := rawdb.WriteBlock(source, block); err != nil {
			t.Fatalf("WriteBlock %d: %v", blockNum, err)
		}
		if err := rawdb.WriteBlockBalanceTrace(source, int64(blockNum), &contractpb.BlockBalanceTrace{
			BlockIdentifier: &contractpb.BlockBalanceTrace_BlockIdentifier{Number: int64(blockNum)},
			Timestamp:       int64(1000 + blockNum),
		}); err != nil {
			t.Fatalf("WriteBlockBalanceTrace %d: %v", blockNum, err)
		}
	}
	owner := common.BytesToAddress([]byte{0x41, 0xd1})
	trace := &contractpb.BlockBalanceTrace{
		BlockIdentifier: &contractpb.BlockBalanceTrace_BlockIdentifier{Number: 1},
		Timestamp:       1000,
		TransactionBalanceTrace: []*contractpb.TransactionBalanceTrace{{
			Operation: []*contractpb.TransactionBalanceTrace_Operation{{
				Address: owner.Bytes(),
				Amount:  25,
			}},
		}},
	}
	if err := rawdb.WriteBlockBalanceTrace(source, 1, trace); err != nil {
		t.Fatalf("WriteBlockBalanceTrace with operation: %v", err)
	}
	if err := rawdb.WriteAccountTrace(source, owner.Bytes(), 1, 1000); err != nil {
		t.Fatalf("WriteAccountTrace: %v", err)
	}
	balanceRef, err := statesnapshots.BuildBalanceTraceSegmentFromDB(source, snapshotDir, "trace/balance-trace-0-section.seg", 0, toBlock)
	if err != nil {
		t.Fatalf("BuildBalanceTraceSegmentFromDB: %v", err)
	}

	bloomRow := snapshotCmdSectionBloomEncodedBit(t, 17)
	if err := rawdb.WriteSectionBloom(source, 0, 42, bloomRow); err != nil {
		t.Fatalf("WriteSectionBloom: %v", err)
	}
	sectionRef, err := statesnapshots.BuildSectionBloomSegmentFromDB(source, snapshotDir, "log/section-bloom-0.seg", 0, toBlock)
	if err != nil {
		t.Fatalf("BuildSectionBloomSegmentFromDB: %v", err)
	}

	logAddress := []byte{0x41, 0xd2, 0xd3, 0xd4, 0xd5}
	logTopic := common.Hash{0xd2}
	if err := rawdb.WriteTransactionInfosByBlock(source, 1, []*corepb.TransactionInfo{{
		Id:          txHash[:],
		BlockNumber: 1,
		Log: []*corepb.TransactionInfo_Log{{
			Address: logAddress,
			Topics:  [][]byte{logTopic[:]},
			Data:    []byte{0xd2},
		}},
	}}); err != nil {
		t.Fatalf("WriteTransactionInfosByBlock: %v", err)
	}
	eventRef, err := statesnapshots.BuildEventLogSegmentFromChain(source, snapshotDir, "log/event-log-0-section.seg", 0, toBlock)
	if err != nil {
		t.Fatalf("BuildEventLogSegmentFromChain: %v", err)
	}
	eventIndexRef, err := statesnapshots.BuildEventLogIndexSegmentFromEventLogSegments(snapshotDir, []statesnapshots.SegmentRef{eventRef}, "")
	if err != nil {
		t.Fatalf("BuildEventLogIndexSegmentFromEventLogSegments: %v", err)
	}
	if err := statesnapshots.PublishManifest(snapshotDir, statesnapshots.NewManifest(0, 0, []statesnapshots.SegmentRef{
		balanceRef,
		sectionRef,
		eventRef,
		eventIndexRef,
	})); err != nil {
		t.Fatalf("PublishManifest: %v", err)
	}

	ctx := makeSnapshotRestoreTestContext(t, []string{
		"--datadir", dataDir,
		"--snapshot.dir", snapshotDir,
		"--snapshot.from-block", "0",
		"--snapshot.to-block", fmt.Sprintf("%d", toBlock),
		"--snapshot.from-cold",
		"--snapshot.etl.buffer", "1",
		"--dev",
		"--witness.key", snapshotTestWitnessKey,
	})
	if err := snapshotBuildDerivedIndexesCmd(ctx); err != nil {
		t.Fatalf("snapshotBuildDerivedIndexesCmd from cold: %v", err)
	}
	identity, err := snapshotExpectedChainIdentityFromContext(ctx, "")
	if err != nil {
		t.Fatalf("snapshotExpectedChainIdentityFromContext: %v", err)
	}
	report, err := statesnapshots.VerifyManifestFiles(snapshotDir, statesnapshots.VerifyManifestOptions{
		ExpectedChain:     &identity,
		RequireRegistered: true,
		RequireChecksums:  true,
	})
	if err != nil {
		t.Fatalf("VerifyManifestFiles: %v", err)
	}
	if report.ActiveSegments != 4 {
		t.Fatalf("active segments = %d, want balance-trace, section-bloom, event-log, event-log-index", report.ActiveSegments)
	}
	manifest, err := statesnapshots.LoadProductionManifest(snapshotDir)
	if err != nil {
		t.Fatalf("LoadProductionManifest: %v", err)
	}
	if manifest.Generation < 4 {
		t.Fatalf("manifest generation = %d, want rematerialized generation >= 4", manifest.Generation)
	}

	mgr, err := statesnapshots.OpenManager(snapshotDir)
	if err != nil {
		t.Fatalf("OpenManager: %v", err)
	}
	if covered, err := mgr.BalanceTraceRangeCovered(0, toBlock); err != nil || !covered {
		t.Fatalf("BalanceTraceRangeCovered = %v/%v, want true/nil", covered, err)
	}
	if covered, err := mgr.SectionBloomRangeCovered(0, toBlock); err != nil || !covered {
		t.Fatalf("SectionBloomRangeCovered = %v/%v, want true/nil", covered, err)
	}
	if covered, err := mgr.EventLogIndexedRangeCovered(1, 1); err != nil || !covered {
		t.Fatalf("EventLogIndexedRangeCovered = %v/%v, want true/nil", covered, err)
	}
	if got, ok, err := mgr.BlockBalanceTrace(1); err != nil || !ok || got.GetTimestamp() != 1000 {
		t.Fatalf("BlockBalanceTrace = %+v/%v/%v, want timestamp 1000", got, ok, err)
	}
	if got, ok, err := mgr.SectionBloom(0, 42); err != nil || !ok || !bytes.Equal(got, bloomRow) {
		t.Fatalf("SectionBloom = %x/%v/%v, want rematerialized row", got, ok, err)
	}
	var rows []rawdb.EventLog
	if err := mgr.IterateEventLogs(1, 1, rawdb.EventLogFilter{
		Addresses: []common.Address{common.BytesToAddress(logAddress)},
		Topics:    [][]common.Hash{{logTopic}},
	}, func(row rawdb.EventLog) (bool, error) {
		rows = append(rows, row)
		return true, nil
	}); err != nil {
		t.Fatalf("IterateEventLogs: %v", err)
	}
	if len(rows) != 1 || rows[0].BlockNum != 1 || rows[0].TxHash != txHash || !bytes.Equal(rows[0].Log.GetData(), []byte{0xd2}) {
		t.Fatalf("event rows = %+v, want rematerialized block 1 log", rows)
	}
}

func TestSnapshotBuildDerivedIndexesCmdFromColdRequiresColdCoverage(t *testing.T) {
	root := t.TempDir()
	ctx := makeSnapshotRestoreTestContext(t, []string{
		"--datadir", filepath.Join(root, "datadir"),
		"--snapshot.dir", filepath.Join(root, "snapshot"),
		"--snapshot.from-block", "0",
		"--snapshot.to-block", "0",
		"--snapshot.from-cold",
		"--dev",
		"--witness.key", snapshotTestWitnessKey,
	})
	err := snapshotBuildDerivedIndexesCmd(ctx)
	if err == nil || !strings.Contains(err.Error(), "cold balance-trace build requires verified cold coverage") {
		t.Fatalf("snapshotBuildDerivedIndexesCmd from cold error = %v, want cold balance-trace coverage error", err)
	}
}

func TestSnapshotPruneRetiredCmdDeletesRetiredSegmentFiles(t *testing.T) {
	root := t.TempDir()
	dataDir := filepath.Join(root, "datadir")
	snapshotDir := filepath.Join(root, "snapshot")
	source := rawdb.NewMemoryChainDB()
	if err := rawdb.WriteBlockBalanceTrace(source, 1, &contractpb.BlockBalanceTrace{Timestamp: 100}); err != nil {
		t.Fatalf("WriteBlockBalanceTrace retired: %v", err)
	}
	if err := rawdb.WriteBlockBalanceTrace(source, 10, &contractpb.BlockBalanceTrace{Timestamp: 1000}); err != nil {
		t.Fatalf("WriteBlockBalanceTrace active: %v", err)
	}
	retiredRef, err := statesnapshots.BuildBalanceTraceSegmentFromDB(source, snapshotDir, "", 1, 1)
	if err != nil {
		t.Fatalf("BuildBalanceTraceSegmentFromDB retired: %v", err)
	}
	activeRef, err := statesnapshots.BuildBalanceTraceSegmentFromDB(source, snapshotDir, "", 10, 10)
	if err != nil {
		t.Fatalf("BuildBalanceTraceSegmentFromDB active: %v", err)
	}
	manifest := statesnapshots.NewManifest(10, 10, []statesnapshots.SegmentRef{activeRef})
	manifest.Retired = []statesnapshots.SegmentRef{retiredRef}
	if err := statesnapshots.PublishManifest(snapshotDir, manifest); err != nil {
		t.Fatalf("PublishManifest: %v", err)
	}
	ctx := makeSnapshotRestoreTestContext(t, []string{
		"--datadir", dataDir,
		"--snapshot.dir", snapshotDir,
	})

	if err := snapshotPruneRetiredCmd(ctx); err != nil {
		t.Fatalf("snapshotPruneRetiredCmd: %v", err)
	}
	if _, err := os.Stat(filepath.Join(snapshotDir, activeRef.Path)); err != nil {
		t.Fatalf("active segment stat: %v", err)
	}
	if _, err := os.Stat(filepath.Join(snapshotDir, retiredRef.Path)); !os.IsNotExist(err) {
		t.Fatalf("retired segment stat err = %v, want not exist", err)
	}
	if _, err := statesnapshots.VerifyManifestFiles(snapshotDir, statesnapshots.VerifyManifestOptions{}); err != nil {
		t.Fatalf("VerifyManifestFiles active-only: %v", err)
	}
}

func TestSnapshotRestoreVerificationOptionsRebuildsCommitmentRoot(t *testing.T) {
	db := rawdb.NewMemoryDatabase()
	owner := common.BytesToAddress(append([]byte{common.AddressPrefixMainnet}, bytes.Repeat([]byte{0x99}, common.AccountIDLength)...))
	if err := rawdb.WriteStateAccountLatest(db, owner, []byte("account")); err != nil {
		t.Fatalf("WriteStateAccountLatest: %v", err)
	}
	opts := snapshotRestoreVerificationOptions(db)
	if opts.Boundary.RebuildCommitmentRoot == nil || !opts.Boundary.RequireIndependentCommitmentRoot {
		t.Fatalf("strict boundary options not configured: %+v", opts.Boundary)
	}
	root, err := opts.Boundary.RebuildCommitmentRoot()
	if err != nil {
		t.Fatalf("RebuildCommitmentRoot: %v", err)
	}
	if root == (common.Hash{}) {
		t.Fatal("rebuilt root is zero")
	}
	stored, ok, err := rawdb.ReadLatestDomainCommitmentRoot(db)
	if err != nil || !ok || stored != root {
		t.Fatalf("stored root = %x ok=%v err=%v, want %x", stored, ok, err, root)
	}
}

func TestEnsureSnapshotRestoreBootstrapDatadirRejectsNonGenesisHead(t *testing.T) {
	db := rawdb.NewMemoryDatabase()
	genesis := common.Hash{0x01}
	rawdb.WriteHeadBlockHash(db, common.Hash{0x02})
	err := ensureSnapshotRestoreBootstrapDatadir(db, genesis)
	if err == nil || !strings.Contains(err.Error(), "non-genesis datadir") {
		t.Fatalf("expected non-genesis datadir error, got %v", err)
	}
	rawdb.WriteHeadBlockHash(db, genesis)
	if err := ensureSnapshotRestoreBootstrapDatadir(db, genesis); err != nil {
		t.Fatalf("genesis head rejected: %v", err)
	}
}

func TestOpenSnapshotRestoreAncientStoreRejectsNonEmptyFreezer(t *testing.T) {
	dataDir := t.TempDir()
	fz := openSnapshotCmdFreezer(t, ancientDataDir(dataDir))
	appendSnapshotCmdFreezerRows(t, fz, []snapshotCmdFreezerRow{{block: snapshotCmdBlock(0)}})
	if err := fz.Close(); err != nil {
		t.Fatalf("close freezer: %v", err)
	}

	store, reader, closeAncient, err := openSnapshotRestoreAncientStore(dataDir)
	if err == nil {
		closeAncient()
		t.Fatalf("openSnapshotRestoreAncientStore store=%v reader=%v succeeded, want non-empty freezer rejection", store, reader)
	}
	if !strings.Contains(err.Error(), "non-empty freezer") {
		t.Fatalf("openSnapshotRestoreAncientStore error = %v, want non-empty freezer", err)
	}
}

func TestSnapshotRestoreCmdPreflightsChainFreezerBeforeStateRestore(t *testing.T) {
	root := t.TempDir()
	snapshotDir := filepath.Join(root, "snapshot")
	dataDir := filepath.Join(root, "datadir")
	owner := common.BytesToAddress(append([]byte{common.AddressPrefixMainnet}, bytes.Repeat([]byte{0x64}, common.AccountIDLength)...))
	sourceDB := rawdb.NewMemoryDatabase()
	if err := rawdb.WriteStateTxRange(sourceDB, 1, common.Hash{0x44}, 1, 1); err != nil {
		t.Fatalf("WriteStateTxRange: %v", err)
	}
	if err := rawdb.WriteStateDomainChange(sourceDB, &rawdb.StateDomainChange{
		BlockNum:   1,
		BlockHash:  common.Hash{0x44},
		TxNum:      1,
		Seq:        1,
		FlatDomain: rawdb.StateFlatDomainAccountLatest,
		Owner:      owner,
		NextExists: true,
		Next:       snapshotCmdAccountEnvelope(t, owner, 64, corepb.AccountType_Normal, common.Hash{}),
	}); err != nil {
		t.Fatalf("WriteStateDomainChange: %v", err)
	}
	historyRefs, err := statesnapshots.BuildStateDomainChangeHistorySegmentsFromDB(sourceDB, snapshotDir, 1, 1, "history/state-domain-change-1-1.seg")
	if err != nil {
		t.Fatalf("BuildStateDomainChangeHistorySegmentsFromDB: %v", err)
	}
	src := openSnapshotCmdFreezer(t, filepath.Join(root, "src-freezer"))
	defer src.Close()
	appendSnapshotCmdFreezerRows(t, src, []snapshotCmdFreezerRow{
		{block: snapshotCmdBlock(0)},
		{block: snapshotCmdBlock(1)},
	})
	freezerRef, err := statesnapshots.BuildChainFreezerSegmentFromAncient(rawdb.NewFreezerReader(src), snapshotDir, "", 1, 1)
	if err != nil {
		t.Fatalf("BuildChainFreezerSegmentFromAncient: %v", err)
	}
	identity, err := snapshotExpectedChainIdentityFromGenesis(params.DefaultMainnetGenesis(), "")
	if err != nil {
		t.Fatalf("snapshotExpectedChainIdentityFromGenesis: %v", err)
	}
	segments := append([]statesnapshots.SegmentRef{}, historyRefs...)
	segments = append(segments, freezerRef)
	if err := statesnapshots.PublishManifest(snapshotDir, statesnapshots.NewManifestForChain(1, 1, segments, identity)); err != nil {
		t.Fatalf("PublishManifest: %v", err)
	}
	pub, priv, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatalf("GenerateKey: %v", err)
	}
	if _, err := statesnapshots.PublishSignedSnapshotCatalog(snapshotDir, priv); err != nil {
		t.Fatalf("PublishSignedSnapshotCatalog: %v", err)
	}
	ctx := makeSnapshotRestoreTestContext(t, []string{
		"--datadir", dataDir,
		"--snapshot.dir", snapshotDir,
		"--snapshot.trusted-key", hex.EncodeToString(pub),
	})

	err = snapshotRestoreCmd(ctx)
	if err == nil || !strings.Contains(err.Error(), "requires ancient heads all 1 or all 2") {
		t.Fatalf("snapshotRestoreCmd error = %v, want chain-freezer preflight rejection", err)
	}
	db, err := openPebbleDB(ctx, chainDataDir(dataDir))
	if err != nil {
		t.Fatalf("open db after failed restore: %v", err)
	}
	defer db.Close()
	if change, ok, err := rawdb.ReadStateDomainChange(db, 1, 1); err != nil || ok {
		t.Fatalf("state history after failed preflight = %+v ok=%v err=%v, want not restored", change, ok, err)
	}
	if got, ok, err := rawdb.ReadStageProgress(db, rawdb.StageSnapshotInstall); err != nil || ok {
		t.Fatalf("SnapshotInstall progress after failed preflight = %d ok=%v err=%v, want absent", got, ok, err)
	}
}

func TestPruneVerifiedHotChainLookupsRequiresSignedCatalog(t *testing.T) {
	root := t.TempDir()
	src := openSnapshotCmdFreezer(t, filepath.Join(root, "src"))
	defer src.Close()
	block0 := snapshotCmdBlock(0)
	block1, txHash, txInfoRaw := snapshotCmdBlockWithTx(t, 1)
	stateRoot := common.HexToHash("1212121212121212121212121212121212121212121212121212121212121212")
	appendSnapshotCmdFreezerRows(t, src, []snapshotCmdFreezerRow{
		{block: block0},
		{block: block1, txInfosRaw: txInfoRaw, stateRoot: stateRoot.Bytes()},
	})

	snapshotDir := filepath.Join(root, "snapshot")
	freezerRef, err := statesnapshots.BuildChainFreezerSegmentFromAncient(rawdb.NewFreezerReader(src), snapshotDir, "", 0, 1)
	if err != nil {
		t.Fatalf("BuildChainFreezerSegmentFromAncient: %v", err)
	}
	indexRef, err := statesnapshots.BuildChainIndexSegmentFromChainFreezerSegment(snapshotDir, freezerRef, "")
	if err != nil {
		t.Fatalf("BuildChainIndexSegmentFromChainFreezerSegment: %v", err)
	}
	identity := statesnapshots.ChainIdentity{
		ChainID:        1,
		NetworkID:      2,
		GenesisHash:    strings.Repeat("01", 32),
		ForkConfigHash: "sha256:" + strings.Repeat("aa", 32),
	}
	if err := statesnapshots.PublishManifest(snapshotDir, statesnapshots.NewManifestForChain(0, 0, []statesnapshots.SegmentRef{freezerRef, indexRef}, identity)); err != nil {
		t.Fatalf("PublishManifest: %v", err)
	}

	hot := rawdb.NewMemoryDatabase()
	if _, err := statesnapshots.RestoreChainFreezerIndexes(hot, snapshotDir, freezerRef); err != nil {
		t.Fatalf("RestoreChainFreezerIndexes: %v", err)
	}
	if err := rawdb.WriteBlockStateRoot(hot, block1.Hash(), stateRoot); err != nil {
		t.Fatalf("WriteBlockStateRoot: %v", err)
	}
	hotOnly := rawdb.NewChainDB(hot, rawdb.NoopAncient{})
	if num := rawdb.ReadBlockNumber(hotOnly, block1.Hash()); num == nil || *num != 1 {
		t.Fatalf("hot ReadBlockNumber = %v, want 1", num)
	}
	if info := rawdb.ReadTransactionInfo(hotOnly, txHash[:]); info == nil || info.Fee != 777 {
		t.Fatalf("hot ReadTransactionInfo = %+v, want fee 777", info)
	}

	pub, priv, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatalf("GenerateKey: %v", err)
	}
	if _, err := pruneVerifiedHotChainLookups(hot, snapshotDir, identity, []ed25519.PublicKey{pub}); err == nil {
		t.Fatal("expected unsigned catalog rejection")
	}
	if num := rawdb.ReadBlockNumber(hotOnly, block1.Hash()); num == nil || *num != 1 {
		t.Fatalf("failed prune changed hot block lookup = %v, want 1", num)
	}
	if _, err := statesnapshots.PublishSignedSnapshotCatalog(snapshotDir, priv); err != nil {
		t.Fatalf("PublishSignedSnapshotCatalog: %v", err)
	}
	if err := rawdb.WriteStageProgressWithHash(hot, rawdb.StageChainFreezer, block1.Number(), block1.Hash()); err != nil {
		t.Fatalf("WriteStageProgressWithHash ChainFreezer: %v", err)
	}

	result, err := pruneVerifiedHotChainLookups(hot, snapshotDir, identity, []ed25519.PublicKey{pub})
	if err != nil {
		t.Fatalf("pruneVerifiedHotChainLookups: %v", err)
	}
	if result.ColdIndexSegments != 1 || result.MissingIndexSegments != 0 || result.BlockIndexesDeleted != 2 || result.StateRootsDeleted != 2 || result.TxIndexesDeleted != 1 || result.TxInfosDeleted != 1 {
		t.Fatalf("prune result = %+v, want one segment, 2 block/state roots, 1 tx index/info", result)
	}
	if num := rawdb.ReadBlockNumber(hotOnly, block1.Hash()); num != nil {
		t.Fatalf("hot ReadBlockNumber after prune = %v, want nil", num)
	}
	if info := rawdb.ReadTransactionInfo(hotOnly, txHash[:]); info != nil {
		t.Fatalf("hot ReadTransactionInfo after prune = %+v, want nil", info)
	}
	if got := rawdb.ReadBlockStateRoot(hotOnly, block1.Hash()); got != (common.Hash{}) {
		t.Fatalf("hot ReadBlockStateRoot after prune = %x, want zero", got)
	}
}

func TestSnapshotRestoreCmdRestartsWithColdChainIndexLookups(t *testing.T) {
	root := t.TempDir()
	dataDir := filepath.Join(root, "datadir")
	snapshotDir := filepath.Join(root, "snapshot")

	genesis := params.DefaultMainnetGenesis()
	genesisBlock, err := corepkg.GenesisToBlock(genesis, corestate.NewDatabase(rawdb.WrapKeyValueStore(rawdb.NewMemoryDatabase())))
	if err != nil {
		t.Fatalf("GenesisToBlock: %v", err)
	}
	block1, txHash, txInfoRaw := snapshotCmdBlockWithTx(t, 1)
	block2 := snapshotCmdBlock(2)
	logTopic := common.Hash{0x5a}
	logAddress := append([]byte{common.AddressPrefixMainnet}, bytes.Repeat([]byte{0x5b}, common.AccountIDLength)...)
	txInfo := &corepb.TransactionRet{}
	if err := proto.Unmarshal(txInfoRaw, txInfo); err != nil {
		t.Fatalf("unmarshal block1 tx info: %v", err)
	}
	if len(txInfo.Transactioninfo) != 1 {
		t.Fatalf("block1 tx info count = %d, want 1", len(txInfo.Transactioninfo))
	}
	txInfo.Transactioninfo[0].Log = []*corepb.TransactionInfo_Log{{
		Address: logAddress,
		Topics:  [][]byte{logTopic[:]},
		Data:    []byte{0x5c, 0x5d},
	}}
	txInfoRaw, err = proto.Marshal(txInfo)
	if err != nil {
		t.Fatalf("marshal block1 tx info with log: %v", err)
	}
	stateRoot1 := common.HexToHash("3131313131313131313131313131313131313131313131313131313131313131")
	stateRoot2 := common.HexToHash("3232323232323232323232323232323232323232323232323232323232323232")
	archiveAddr := common.BytesToAddress(append([]byte{common.AddressPrefixMainnet}, bytes.Repeat([]byte{0x42}, common.AccountIDLength)...))
	archiveBalance1 := int64(1_000)
	archiveBalance2 := int64(2_000)
	archiveReward1 := int64(111)
	archiveReward2 := int64(222)
	archiveCode1 := []byte{0x60, 0x01, 0x60, 0x02}
	archiveCode2 := []byte{0x60, 0x03, 0x60, 0x04}
	archiveCodeHash1 := common.Keccak256(archiveCode1)
	archiveCodeHash2 := common.Keccak256(archiveCode2)
	archiveSlot := common.Hash{0x99}
	archiveStorage1 := common.HexToHash("0101")
	archiveStorage2 := common.HexToHash("0202")
	archiveStorageKey := snapshotCmdStorageRowKey(archiveAddr, archiveSlot)
	archiveAccount1 := snapshotCmdAccountEnvelope(t, archiveAddr, archiveBalance1, corepb.AccountType_Contract, archiveCodeHash1, archiveReward1)
	archiveAccount2 := snapshotCmdAccountEnvelope(t, archiveAddr, archiveBalance2, corepb.AccountType_Contract, archiveCodeHash2, archiveReward2)
	deletedAddr := common.BytesToAddress(append([]byte{common.AddressPrefixMainnet}, bytes.Repeat([]byte{0x43}, common.AccountIDLength)...))
	deletedBalance1 := int64(3_000)
	deletedAccount1 := snapshotCmdAccountEnvelope(t, deletedAddr, deletedBalance1, corepb.AccountType_Normal, common.Hash{})
	recreatedAddr := common.BytesToAddress(append([]byte{common.AddressPrefixMainnet}, bytes.Repeat([]byte{0x44}, common.AccountIDLength)...))
	recreatedBalance1 := int64(4_000)
	recreatedBalance2 := int64(5_000)
	recreatedSlotA := common.Hash{31: 0xa1}
	recreatedSlotB := common.Hash{31: 0xb1}
	recreatedOldA := common.HexToHash("0a")
	recreatedOldB := common.HexToHash("0b")
	recreatedNewA := common.HexToHash("1a")
	recreatedStorageKeyA := snapshotCmdStorageRowKey(recreatedAddr, recreatedSlotA)
	recreatedStorageKeyB := snapshotCmdStorageRowKey(recreatedAddr, recreatedSlotB)
	recreatedAccount1 := snapshotCmdAccountEnvelopeWithGeneration(t, recreatedAddr, recreatedBalance1, corepb.AccountType_Contract, archiveCodeHash1, 0)
	recreatedAccount2 := snapshotCmdAccountEnvelopeWithGeneration(t, recreatedAddr, recreatedBalance2, corepb.AccountType_Contract, archiveCodeHash2, 1)
	recreatedMeta1 := snapshotCmdContractMetadata(t, recreatedAddr, "snapshot-old-contract", archiveCode1)
	recreatedMeta2 := snapshotCmdContractMetadata(t, recreatedAddr, "snapshot-new-contract", archiveCode2)
	delegationFrom := common.BytesToAddress(append([]byte{common.AddressPrefixMainnet}, bytes.Repeat([]byte{0x45}, common.AccountIDLength)...))
	delegationTo := common.BytesToAddress(append([]byte{common.AddressPrefixMainnet}, bytes.Repeat([]byte{0x46}, common.AccountIDLength)...))
	delegationBandwidth := int64(6_000)
	delegationBandwidthExpire := int64(60_000)
	delegationEnergy := int64(7_000)
	delegationEnergyExpire := int64(70_000)

	src := openSnapshotCmdFreezer(t, filepath.Join(root, "src-freezer"))
	defer src.Close()
	appendSnapshotCmdFreezerRows(t, src, []snapshotCmdFreezerRow{
		{block: genesisBlock, txInfosRaw: snapshotCmdTxInfoRawForBlock(t, genesisBlock)},
		{block: block1, txInfosRaw: txInfoRaw, stateRoot: stateRoot1.Bytes()},
		{block: block2, stateRoot: stateRoot2.Bytes()},
	})
	freezerRef, err := statesnapshots.BuildChainFreezerSegmentFromAncient(rawdb.NewFreezerReader(src), snapshotDir, "", 0, 2)
	if err != nil {
		t.Fatalf("BuildChainFreezerSegmentFromAncient: %v", err)
	}
	indexRef, err := statesnapshots.BuildChainIndexSegmentFromChainFreezerSegment(snapshotDir, freezerRef, "")
	if err != nil {
		t.Fatalf("BuildChainIndexSegmentFromChainFreezerSegment: %v", err)
	}
	sourceChain := rawdb.NewChainDB(rawdb.NewMemoryDatabase(), rawdb.NewFreezerReader(src))
	eventLogRef, err := statesnapshots.BuildEventLogSegmentFromChain(sourceChain, snapshotDir, "log/event-log-1-1.seg", 1, 1)
	if err != nil {
		t.Fatalf("BuildEventLogSegmentFromChain: %v", err)
	}
	eventLogIndexRef, err := statesnapshots.BuildEventLogIndexSegmentFromEventLogSegments(snapshotDir, []statesnapshots.SegmentRef{eventLogRef}, "")
	if err != nil {
		t.Fatalf("BuildEventLogIndexSegmentFromEventLogSegments: %v", err)
	}

	stateSnapshotDB := rawdb.NewMemoryDatabase()
	if err := rawdb.WriteStateAccountLatest(stateSnapshotDB, archiveAddr, archiveAccount2); err != nil {
		t.Fatalf("WriteStateAccountLatest: %v", err)
	}
	if err := rawdb.WriteStateAccountLatest(stateSnapshotDB, recreatedAddr, recreatedAccount2); err != nil {
		t.Fatalf("WriteStateAccountLatest recreated: %v", err)
	}
	if err := rawdb.WriteStateKVGeneration(stateSnapshotDB, recreatedAddr, 1); err != nil {
		t.Fatalf("WriteStateKVGeneration recreated: %v", err)
	}
	if err := rawdb.WriteStateKVLatest(stateSnapshotDB, recreatedAddr, 0, kvdomains.ContractMetadata, snapshotCmdContractMetaKey(), recreatedMeta1); err != nil {
		t.Fatalf("WriteStateKVLatest recreated old metadata: %v", err)
	}
	if err := rawdb.WriteStateKVLatest(stateSnapshotDB, recreatedAddr, 1, kvdomains.ContractMetadata, snapshotCmdContractMetaKey(), recreatedMeta2); err != nil {
		t.Fatalf("WriteStateKVLatest recreated new metadata: %v", err)
	}
	if err := rawdb.WriteStateCode(stateSnapshotDB, archiveCodeHash1, archiveCode1); err != nil {
		t.Fatalf("WriteStateCode code1: %v", err)
	}
	if err := rawdb.WriteStateCode(stateSnapshotDB, archiveCodeHash2, archiveCode2); err != nil {
		t.Fatalf("WriteStateCode code2: %v", err)
	}
	if err := rawdb.WriteStateKVLatest(stateSnapshotDB, archiveAddr, 0, kvdomains.ContractStorage, archiveStorageKey.Bytes(), archiveStorage2.Bytes()); err != nil {
		t.Fatalf("WriteStateKVLatest contract storage: %v", err)
	}
	if err := rawdb.WriteStateKVLatest(stateSnapshotDB, recreatedAddr, 0, kvdomains.ContractStorage, recreatedStorageKeyA.Bytes(), recreatedOldA.Bytes()); err != nil {
		t.Fatalf("WriteStateKVLatest recreated old slotA: %v", err)
	}
	if err := rawdb.WriteStateKVLatest(stateSnapshotDB, recreatedAddr, 0, kvdomains.ContractStorage, recreatedStorageKeyB.Bytes(), recreatedOldB.Bytes()); err != nil {
		t.Fatalf("WriteStateKVLatest recreated old slotB: %v", err)
	}
	if err := rawdb.WriteStateKVLatest(stateSnapshotDB, recreatedAddr, 1, kvdomains.ContractStorage, recreatedStorageKeyA.Bytes(), recreatedNewA.Bytes()); err != nil {
		t.Fatalf("WriteStateKVLatest recreated new slotA: %v", err)
	}
	delegationUnlocked := mustSnapshotCmdDelegationResource(t, &rawdb.DelegatedResource{
		From:                      delegationFrom,
		To:                        delegationTo,
		FrozenBalanceForBandwidth: delegationBandwidth,
		ExpireTimeForBandwidth:    delegationBandwidthExpire,
	})
	if err := rawdb.WriteStateKVLatest(stateSnapshotDB, common.SystemAccountAddress, 0, kvdomains.SystemDelegation, rawdb.DelegatedResourceV2StateKey(delegationFrom, delegationTo, false), delegationUnlocked); err != nil {
		t.Fatalf("WriteStateKVLatest delegation unlocked: %v", err)
	}
	delegationLocked := mustSnapshotCmdDelegationResource(t, &rawdb.DelegatedResource{
		From:                   delegationFrom,
		To:                     delegationTo,
		FrozenBalanceForEnergy: delegationEnergy,
		ExpireTimeForEnergy:    delegationEnergyExpire,
	})
	if err := rawdb.WriteStateKVLatest(stateSnapshotDB, common.SystemAccountAddress, 0, kvdomains.SystemDelegation, rawdb.DelegatedResourceV2StateKey(delegationFrom, delegationTo, true), delegationLocked); err != nil {
		t.Fatalf("WriteStateKVLatest delegation locked: %v", err)
	}
	if err := rawdb.WriteStateKVLatest(stateSnapshotDB, common.SystemAccountAddress, 0, kvdomains.SystemDelegation, rawdb.DelegationIndexStateKey(delegationFrom), delegationTo.Bytes()); err != nil {
		t.Fatalf("WriteStateKVLatest delegation index: %v", err)
	}
	if err := rawdb.WriteStateTxRange(stateSnapshotDB, block1.Number(), block1.Hash(), 1, 1); err != nil {
		t.Fatalf("WriteStateTxRange block1: %v", err)
	}
	if err := rawdb.WriteStateTxRange(stateSnapshotDB, block2.Number(), block2.Hash(), 2, 2); err != nil {
		t.Fatalf("WriteStateTxRange block2: %v", err)
	}
	if err := rawdb.WriteStateDomainChange(stateSnapshotDB, &rawdb.StateDomainChange{
		BlockNum:   block2.Number(),
		BlockHash:  block2.Hash(),
		TxNum:      2,
		Seq:        1,
		FlatDomain: rawdb.StateFlatDomainAccountLatest,
		Owner:      archiveAddr,
		PrevExists: true,
		Prev:       archiveAccount1,
		NextExists: true,
		Next:       archiveAccount2,
	}); err != nil {
		t.Fatalf("WriteStateDomainChange: %v", err)
	}
	if err := rawdb.WriteStateDomainChange(stateSnapshotDB, &rawdb.StateDomainChange{
		BlockNum:   block2.Number(),
		BlockHash:  block2.Hash(),
		TxNum:      2,
		Seq:        2,
		FlatDomain: rawdb.StateFlatDomainKVLatest,
		Owner:      archiveAddr,
		Generation: 0,
		Domain:     kvdomains.ContractStorage,
		Key:        archiveStorageKey.Bytes(),
		PrevExists: true,
		Prev:       archiveStorage1.Bytes(),
		NextExists: true,
		Next:       archiveStorage2.Bytes(),
	}); err != nil {
		t.Fatalf("WriteStateDomainChange storage: %v", err)
	}
	if err := rawdb.WriteStateDomainChange(stateSnapshotDB, &rawdb.StateDomainChange{
		BlockNum:   block2.Number(),
		BlockHash:  block2.Hash(),
		TxNum:      2,
		Seq:        3,
		FlatDomain: rawdb.StateFlatDomainAccountLatest,
		Owner:      deletedAddr,
		PrevExists: true,
		Prev:       deletedAccount1,
		NextExists: false,
	}); err != nil {
		t.Fatalf("WriteStateDomainChange deleted account: %v", err)
	}
	if err := rawdb.WriteStateDomainChange(stateSnapshotDB, &rawdb.StateDomainChange{
		BlockNum:   block2.Number(),
		BlockHash:  block2.Hash(),
		TxNum:      2,
		Seq:        4,
		FlatDomain: rawdb.StateFlatDomainAccountLatest,
		Owner:      recreatedAddr,
		PrevExists: true,
		Prev:       recreatedAccount1,
		NextExists: true,
		Next:       recreatedAccount2,
	}); err != nil {
		t.Fatalf("WriteStateDomainChange recreated account: %v", err)
	}
	if err := rawdb.WriteStateDomainChange(stateSnapshotDB, &rawdb.StateDomainChange{
		BlockNum:   block2.Number(),
		BlockHash:  block2.Hash(),
		TxNum:      2,
		Seq:        5,
		FlatDomain: rawdb.StateFlatDomainKVGeneration,
		Owner:      recreatedAddr,
		PrevExists: false,
		NextExists: true,
		Next:       rawdb.EncodeStateKVGenerationValue(1),
	}); err != nil {
		t.Fatalf("WriteStateDomainChange recreated generation: %v", err)
	}
	if err := rawdb.WriteStateDomainChange(stateSnapshotDB, &rawdb.StateDomainChange{
		BlockNum:   block2.Number(),
		BlockHash:  block2.Hash(),
		TxNum:      2,
		Seq:        6,
		FlatDomain: rawdb.StateFlatDomainKVLatest,
		Owner:      recreatedAddr,
		Generation: 0,
		Domain:     kvdomains.ContractStorage,
		Key:        recreatedStorageKeyA.Bytes(),
		PrevExists: true,
		Prev:       recreatedOldA.Bytes(),
		NextExists: false,
	}); err != nil {
		t.Fatalf("WriteStateDomainChange recreated old slotA: %v", err)
	}
	if err := rawdb.WriteStateDomainChange(stateSnapshotDB, &rawdb.StateDomainChange{
		BlockNum:   block2.Number(),
		BlockHash:  block2.Hash(),
		TxNum:      2,
		Seq:        7,
		FlatDomain: rawdb.StateFlatDomainKVLatest,
		Owner:      recreatedAddr,
		Generation: 0,
		Domain:     kvdomains.ContractStorage,
		Key:        recreatedStorageKeyB.Bytes(),
		PrevExists: true,
		Prev:       recreatedOldB.Bytes(),
		NextExists: false,
	}); err != nil {
		t.Fatalf("WriteStateDomainChange recreated old slotB: %v", err)
	}
	if err := rawdb.WriteStateDomainChange(stateSnapshotDB, &rawdb.StateDomainChange{
		BlockNum:   block2.Number(),
		BlockHash:  block2.Hash(),
		TxNum:      2,
		Seq:        8,
		FlatDomain: rawdb.StateFlatDomainKVLatest,
		Owner:      recreatedAddr,
		Generation: 1,
		Domain:     kvdomains.ContractStorage,
		Key:        recreatedStorageKeyA.Bytes(),
		PrevExists: false,
		NextExists: true,
		Next:       recreatedNewA.Bytes(),
	}); err != nil {
		t.Fatalf("WriteStateDomainChange recreated new slotA: %v", err)
	}
	if err := rawdb.WriteStateDomainChange(stateSnapshotDB, &rawdb.StateDomainChange{
		BlockNum:   block2.Number(),
		BlockHash:  block2.Hash(),
		TxNum:      2,
		Seq:        9,
		FlatDomain: rawdb.StateFlatDomainKVLatest,
		Owner:      recreatedAddr,
		Generation: 0,
		Domain:     kvdomains.ContractMetadata,
		Key:        snapshotCmdContractMetaKey(),
		PrevExists: true,
		Prev:       recreatedMeta1,
		NextExists: false,
	}); err != nil {
		t.Fatalf("WriteStateDomainChange recreated old metadata: %v", err)
	}
	if err := rawdb.WriteStateDomainChange(stateSnapshotDB, &rawdb.StateDomainChange{
		BlockNum:   block2.Number(),
		BlockHash:  block2.Hash(),
		TxNum:      2,
		Seq:        10,
		FlatDomain: rawdb.StateFlatDomainKVLatest,
		Owner:      recreatedAddr,
		Generation: 1,
		Domain:     kvdomains.ContractMetadata,
		Key:        snapshotCmdContractMetaKey(),
		PrevExists: false,
		NextExists: true,
		Next:       recreatedMeta2,
	}); err != nil {
		t.Fatalf("WriteStateDomainChange recreated new metadata: %v", err)
	}
	accountRef, accountAccessorRef, accountBTreeRef, err := statesnapshots.BuildAccountLatestSegmentFilesFromDB(stateSnapshotDB, snapshotDir, 1, 2, "latest/accounts-1-2.seg")
	if err != nil {
		t.Fatalf("BuildAccountLatestSegmentFilesFromDB: %v", err)
	}
	codeRef, codeAccessorRef, codeBTreeRef, err := statesnapshots.BuildCodeSegmentFilesFromDB(stateSnapshotDB, snapshotDir, 1, 2, "latest/code-1-2.seg")
	if err != nil {
		t.Fatalf("BuildCodeSegmentFilesFromDB: %v", err)
	}
	storageRef, storageAccessorRef, storageBTreeRef, err := statesnapshots.BuildLatestDomainSegmentFilesFromDB(stateSnapshotDB, snapshotDir, kvdomains.ContractStorage, 1, 2, "latest/contract-storage-1-2.seg")
	if err != nil {
		t.Fatalf("BuildLatestDomainSegmentFilesFromDB(contract storage): %v", err)
	}
	metadataRef, metadataAccessorRef, metadataBTreeRef, err := statesnapshots.BuildLatestDomainSegmentFilesFromDB(stateSnapshotDB, snapshotDir, kvdomains.ContractMetadata, 1, 2, "latest/contract-metadata-1-2.seg")
	if err != nil {
		t.Fatalf("BuildLatestDomainSegmentFilesFromDB(contract metadata): %v", err)
	}
	delegationRef, delegationAccessorRef, delegationBTreeRef, err := statesnapshots.BuildLatestDomainSegmentFilesFromDB(stateSnapshotDB, snapshotDir, kvdomains.SystemDelegation, 1, 2, "latest/system-delegation-1-2.seg")
	if err != nil {
		t.Fatalf("BuildLatestDomainSegmentFilesFromDB(system delegation): %v", err)
	}
	historyRefs, err := statesnapshots.BuildStateDomainChangeHistorySegmentsFromDB(stateSnapshotDB, snapshotDir, 1, 2, "history/state-domain-change-1-2.seg")
	if err != nil {
		t.Fatalf("BuildStateDomainChangeHistorySegmentsFromDB: %v", err)
	}

	identity, err := snapshotExpectedChainIdentityFromGenesis(genesis, "")
	if err != nil {
		t.Fatalf("snapshotExpectedChainIdentityFromGenesis: %v", err)
	}
	segments := []statesnapshots.SegmentRef{
		accountRef, accountAccessorRef, accountBTreeRef,
		codeRef, codeAccessorRef, codeBTreeRef,
		storageRef, storageAccessorRef, storageBTreeRef,
		metadataRef, metadataAccessorRef, metadataBTreeRef,
		delegationRef, delegationAccessorRef, delegationBTreeRef,
	}
	segments = append(segments, historyRefs...)
	segments = append(segments, freezerRef, indexRef, eventLogRef, eventLogIndexRef)
	if err := statesnapshots.PublishManifest(snapshotDir, statesnapshots.NewManifestForChain(1, 2, segments, identity)); err != nil {
		t.Fatalf("PublishManifest: %v", err)
	}
	pub, priv, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatalf("GenerateKey: %v", err)
	}
	if _, err := statesnapshots.PublishSignedSnapshotCatalog(snapshotDir, priv); err != nil {
		t.Fatalf("PublishSignedSnapshotCatalog: %v", err)
	}

	ctx := makeSnapshotRestoreTestContext(t, []string{
		"--datadir", dataDir,
		"--snapshot.dir", snapshotDir,
		"--snapshot.trusted-key", hex.EncodeToString(pub),
	})
	if err := snapshotRestoreCmd(ctx); err != nil {
		t.Fatalf("snapshotRestoreCmd: %v", err)
	}

	db, err := openPebbleDB(ctx, chainDataDir(dataDir))
	if err != nil {
		t.Fatalf("open restored db: %v", err)
	}
	defer db.Close()
	fz, err := rawdbfreezer.NewFreezer(ancientDataDir(dataDir), "", true, freezerTableSize, chainfreezer.FreezerTableSet())
	if err != nil {
		t.Fatalf("open restored freezer: %v", err)
	}
	defer fz.Close()

	hotOnly := rawdb.NewChainDB(db, rawdb.NoopAncient{})
	if got := rawdb.ReadBlock(hotOnly, block1.Number()); got != nil {
		t.Fatalf("hot-only block1 = %x, want nil", got.Hash())
	}
	if got := rawdb.ReadBlockNumber(hotOnly, block1.Hash()); got != nil {
		t.Fatalf("hot-only block1 lookup = %v, want nil", got)
	}
	if got := rawdb.ReadTransactionIndex(hotOnly, txHash[:]); got != nil {
		t.Fatalf("hot-only tx lookup = %v, want nil", got)
	}
	if got := rawdb.ReadTransactionInfo(hotOnly, txHash[:]); got != nil {
		t.Fatalf("hot-only tx info = %+v, want nil", got)
	}
	if got := rawdb.ReadBlockStateRoot(hotOnly, block1.Hash()); got != (common.Hash{}) {
		t.Fatalf("hot-only state root = %x, want zero", got)
	}
	if got, ok, err := rawdb.ReadStageProgressRow(db, rawdb.StageExecution); err != nil || !ok || got.BlockNum != block2.Number() || got.BlockHash != block2.Hash() || !got.HasBlockHash {
		t.Fatalf("Execution stage = %+v ok=%v err=%v, want block2", got, ok, err)
	}
	if got, ok, err := rawdb.ReadStageProgress(db, rawdb.StageChainFreezer); err != nil || !ok || got != block2.Number() {
		t.Fatalf("ChainFreezer stage = %d ok=%v err=%v, want block2", got, ok, err)
	}
	if got, ok, err := rawdb.ReadStageProgress(db, rawdb.StageSnapshotEventLogBuild); err != nil || !ok || got != block1.Number() {
		t.Fatalf("SnapshotEventLogBuild stage = %d ok=%v err=%v, want block1", got, ok, err)
	}

	mgr, err := statesnapshots.OpenManager(snapshotDir)
	if err != nil {
		t.Fatalf("OpenManager: %v", err)
	}
	chainConfig := *params.MainnetChainConfig
	chainConfig.HistoryEnabled = true
	chainConfig.HistoryMode = params.HistoryModeSnap
	bc, err := corepkg.NewBlockChainWithAncient(db, corestate.NewDatabase(rawdb.WrapKeyValueStore(db)), &chainConfig, rawdb.NewFreezerReader(fz))
	if err != nil {
		t.Fatalf("NewBlockChainWithAncient: %v", err)
	}
	defer bc.Close()
	bc.SetStateCodeColdHistory(mgr)
	bc.SetStateCommitmentColdHistory(mgr)
	bc.ChainDB().SetChainIndexReader(mgr)
	bc.ChainDB().SetEventLogReader(mgr)
	backend := corepkg.NewTronBackend(bc, txpool.New())
	backend.SetStateColdHistory(mgr)
	if err := rawdb.DeleteStateDomainChanges(db, block2.Number()); err != nil {
		t.Fatalf("DeleteStateDomainChanges(block2): %v", err)
	}
	if err := rawdb.DeleteStateTxRange(db, block1.Number()); err != nil {
		t.Fatalf("DeleteStateTxRange(block1): %v", err)
	}
	if err := rawdb.DeleteStateTxRange(db, block2.Number()); err != nil {
		t.Fatalf("DeleteStateTxRange(block2): %v", err)
	}
	dynProps := bc.DynProps()
	dynProps.SetLatestSolidifiedBlockNum(int64(block1.Number()))
	bc.SetDynPropsCacheForTest(dynProps)
	rawdb.WriteLatestPbftBlockNum(db, int64(block1.Number()))

	if got := bc.CurrentBlock(); got == nil || got.Number() != block2.Number() || got.Hash() != block2.Hash() {
		t.Fatalf("current block = %v, want block2 %x", got, block2.Hash())
	}
	gotBlock, err := backend.GetBlockByHash(block1.Hash())
	if err != nil || gotBlock == nil || gotBlock.Hash() != block1.Hash() {
		t.Fatalf("GetBlockByHash(block1) = %v/%v, want %x", gotBlock, err, block1.Hash())
	}
	gotTx, err := backend.GetTransactionByID(txHash)
	if err != nil || gotTx == nil || coretypes.NewTransactionFromPB(gotTx).Hash() != txHash {
		t.Fatalf("GetTransactionByID = %v/%v, want %x", gotTx, err, txHash)
	}
	info, err := backend.GetTransactionInfoByID(txHash)
	if err != nil || info == nil || info.Fee != 777 || uint64(info.BlockNumber) != block1.Number() {
		t.Fatalf("GetTransactionInfoByID = %+v/%v, want fee 777 block1", info, err)
	}
	gotTx, gotBlock, idx, err := backend.GetTransactionByHash(txHash)
	if err != nil || gotTx == nil || gotBlock == nil || gotBlock.Hash() != block1.Hash() || idx != 0 {
		t.Fatalf("GetTransactionByHash = tx:%v block:%v idx:%d err:%v, want tx/block1/0", gotTx, gotBlock, idx, err)
	}
	if got := bc.StateRootAtBlock(block1.Number()); got != stateRoot1 {
		t.Fatalf("StateRootAtBlock(block1) = %x, want %x", got, stateRoot1)
	}
	gotBalance, err := backend.GetBalanceAt(archiveAddr, block1.Number())
	if err != nil || gotBalance != archiveBalance1 {
		t.Fatalf("GetBalanceAt(block1) = %d/%v, want %d", gotBalance, err, archiveBalance1)
	}
	gotBalance, err = backend.GetBalanceAt(archiveAddr, block2.Number())
	if err != nil || gotBalance != archiveBalance2 {
		t.Fatalf("GetBalanceAt(block2) = %d/%v, want %d", gotBalance, err, archiveBalance2)
	}
	gotCode, err := backend.GetCodeAt(archiveAddr, block1.Number())
	if err != nil || !bytes.Equal(gotCode, archiveCode1) {
		t.Fatalf("GetCodeAt(block1) = %x/%v, want %x", gotCode, err, archiveCode1)
	}
	gotStorage, err := backend.GetStorageAtBlock(archiveAddr, archiveSlot, block1.Number())
	if err != nil || gotStorage != archiveStorage1 {
		t.Fatalf("GetStorageAtBlock(block1) = %x/%v, want %x", gotStorage, err, archiveStorage1)
	}
	gotReward, err := backend.GetRewardAt(archiveAddr, block1.Number())
	if err != nil || gotReward.Reward != archiveReward1 {
		t.Fatalf("GetRewardAt(block1) = %+v/%v, want %d", gotReward, err, archiveReward1)
	}
	deletedBalance, err := backend.GetBalanceAt(deletedAddr, block1.Number())
	if err != nil || deletedBalance != deletedBalance1 {
		t.Fatalf("GetBalanceAt(deleted block1) = %d/%v, want %d", deletedBalance, err, deletedBalance1)
	}
	deletedBalance, err = backend.GetBalanceAt(deletedAddr, block2.Number())
	if err != nil || deletedBalance != 0 {
		t.Fatalf("GetBalanceAt(deleted block2) = %d/%v, want 0", deletedBalance, err)
	}
	if deletedAccount, err := backend.GetAccountAt(deletedAddr, block1.Number()); err != nil || deletedAccount == nil || deletedAccount.Balance() != deletedBalance1 {
		t.Fatalf("GetAccountAt(deleted block1) = %+v/%v, want balance %d", deletedAccount, err, deletedBalance1)
	}
	if deletedAccount, err := backend.GetAccountAt(deletedAddr, block2.Number()); err == nil || deletedAccount != nil {
		t.Fatalf("GetAccountAt(deleted block2) = %+v/%v, want nil/error", deletedAccount, err)
	}
	if recreatedCode, err := backend.GetCodeAt(recreatedAddr, block1.Number()); err != nil || !bytes.Equal(recreatedCode, archiveCode1) {
		t.Fatalf("GetCodeAt(recreated block1) = %x/%v, want %x", recreatedCode, err, archiveCode1)
	}
	if recreatedCode, err := backend.GetCodeAt(recreatedAddr, block2.Number()); err != nil || !bytes.Equal(recreatedCode, archiveCode2) {
		t.Fatalf("GetCodeAt(recreated block2) = %x/%v, want %x", recreatedCode, err, archiveCode2)
	}
	if recreatedStorage, err := backend.GetStorageAtBlock(recreatedAddr, recreatedSlotA, block1.Number()); err != nil || recreatedStorage != recreatedOldA {
		t.Fatalf("GetStorageAtBlock(recreated slotA block1) = %x/%v, want %x", recreatedStorage, err, recreatedOldA)
	}
	if recreatedStorage, err := backend.GetStorageAtBlock(recreatedAddr, recreatedSlotB, block1.Number()); err != nil || recreatedStorage != recreatedOldB {
		t.Fatalf("GetStorageAtBlock(recreated slotB block1) = %x/%v, want %x", recreatedStorage, err, recreatedOldB)
	}
	if recreatedStorage, err := backend.GetStorageAtBlock(recreatedAddr, recreatedSlotA, block2.Number()); err != nil || recreatedStorage != recreatedNewA {
		t.Fatalf("GetStorageAtBlock(recreated slotA block2) = %x/%v, want %x", recreatedStorage, err, recreatedNewA)
	}
	if recreatedStorage, err := backend.GetStorageAtBlock(recreatedAddr, recreatedSlotB, block2.Number()); err != nil || recreatedStorage != (common.Hash{}) {
		t.Fatalf("GetStorageAtBlock(recreated slotB block2) = %x/%v, want zero", recreatedStorage, err)
	}
	if recreatedContract, err := backend.GetContractAt(recreatedAddr, block1.Number()); err != nil || recreatedContract == nil || recreatedContract.Name != "snapshot-old-contract" || !bytes.Equal(recreatedContract.Bytecode, archiveCode1) {
		t.Fatalf("GetContractAt(recreated block1) = %+v/%v, want old contract metadata", recreatedContract, err)
	}
	if recreatedContract, err := backend.GetContractAt(recreatedAddr, block2.Number()); err != nil || recreatedContract == nil || recreatedContract.Name != "snapshot-new-contract" || !bytes.Equal(recreatedContract.Bytecode, archiveCode2) {
		t.Fatalf("GetContractAt(recreated block2) = %+v/%v, want new contract metadata", recreatedContract, err)
	}
	delegated, err := backend.GetDelegatedResourceV2(delegationFrom, delegationTo)
	if err != nil {
		t.Fatalf("GetDelegatedResourceV2: %v", err)
	}
	if len(delegated) != 2 {
		t.Fatalf("GetDelegatedResourceV2 len = %d, want 2: %+v", len(delegated), delegated)
	}
	if delegated[0].FrozenBalanceForBandwidth != delegationBandwidth || delegated[0].ExpireTimeForBandwidth != delegationBandwidthExpire {
		t.Fatalf("GetDelegatedResourceV2 bandwidth = %+v, want amount=%d expire=%d", delegated[0], delegationBandwidth, delegationBandwidthExpire)
	}
	if delegated[1].FrozenBalanceForEnergy != delegationEnergy || delegated[1].ExpireTimeForEnergy != delegationEnergyExpire {
		t.Fatalf("GetDelegatedResourceV2 energy = %+v, want amount=%d expire=%d", delegated[1], delegationEnergy, delegationEnergyExpire)
	}
	delegationIndex, err := backend.GetDelegatedResourceAccountIndexV2(delegationFrom)
	if err != nil {
		t.Fatalf("GetDelegatedResourceAccountIndexV2: %v", err)
	}
	if delegationIndex == nil || len(delegationIndex.ToAddresses) != 1 || delegationIndex.ToAddresses[0] != hex.EncodeToString(delegationTo.Bytes()) {
		t.Fatalf("GetDelegatedResourceAccountIndexV2 = %+v, want %x", delegationIndex, delegationTo.Bytes())
	}

	tronMux := http.NewServeMux()
	tronapi.NewAPI(backend).RegisterRoutes(tronMux)
	tronServer := httptest.NewServer(tronMux)
	defer tronServer.Close()

	blockHashHex := hex.EncodeToString(block1.Hash().Bytes())
	txHashHex := hex.EncodeToString(txHash.Bytes())
	for _, prefix := range []string{"/wallet", "/walletsolidity", "/walletpbft"} {
		blockJSON := postSnapshotTestJSON(t, tronServer.URL+prefix+"/getblockbyid", fmt.Sprintf(`{"value":"%s"}`, blockHashHex))
		if got := blockJSON["blockID"]; got == nil || got == "" {
			t.Fatalf("%s/getblockbyid response missing blockID: %v", prefix, blockJSON)
		}
		txJSON := postSnapshotTestJSON(t, tronServer.URL+prefix+"/gettransactionbyid", fmt.Sprintf(`{"value":"%s"}`, txHashHex))
		if got := txJSON["txID"]; got != txHashHex {
			t.Fatalf("%s/gettransactionbyid txID = %v, want %s: %v", prefix, got, txHashHex, txJSON)
		}
		infoJSON := postSnapshotTestJSON(t, tronServer.URL+prefix+"/gettransactioninfobyid", fmt.Sprintf(`{"value":"%s"}`, txHashHex))
		if got := asFloat64(infoJSON["fee"]); got != 777 {
			t.Fatalf("%s/gettransactioninfobyid fee = %v, want 777: %v", prefix, infoJSON["fee"], infoJSON)
		}
		if got := asFloat64(infoJSON["blockNumber"]); got != float64(block1.Number()) {
			t.Fatalf("%s/gettransactioninfobyid blockNumber = %v, want %d: %v", prefix, infoJSON["blockNumber"], block1.Number(), infoJSON)
		}
		delegatedJSON := postSnapshotTestJSON(t, tronServer.URL+prefix+"/getdelegatedresourcev2", fmt.Sprintf(`{"fromAddress":"%s","toAddress":"%s"}`, hex.EncodeToString(delegationFrom.Bytes()), hex.EncodeToString(delegationTo.Bytes())))
		delegatedList, ok := delegatedJSON["delegatedResource"].([]any)
		if !ok || len(delegatedList) != 2 {
			t.Fatalf("%s/getdelegatedresourcev2 list = %v, want 2 entries", prefix, delegatedJSON["delegatedResource"])
		}
		delegatedBandwidthJSON, _ := delegatedList[0].(map[string]any)
		if got := asFloat64(delegatedBandwidthJSON["frozen_balance_for_bandwidth"]); got != float64(delegationBandwidth) {
			t.Fatalf("%s/getdelegatedresourcev2 bandwidth = %v, want %d: %v", prefix, delegatedBandwidthJSON["frozen_balance_for_bandwidth"], delegationBandwidth, delegatedJSON)
		}
		delegatedEnergyJSON, _ := delegatedList[1].(map[string]any)
		if got := asFloat64(delegatedEnergyJSON["frozen_balance_for_energy"]); got != float64(delegationEnergy) {
			t.Fatalf("%s/getdelegatedresourcev2 energy = %v, want %d: %v", prefix, delegatedEnergyJSON["frozen_balance_for_energy"], delegationEnergy, delegatedJSON)
		}
		delegationIndexJSON := postSnapshotTestJSON(t, tronServer.URL+prefix+"/getdelegatedresourceaccountindexv2", fmt.Sprintf(`{"value":"%s"}`, hex.EncodeToString(delegationFrom.Bytes())))
		toAddresses, ok := delegationIndexJSON["toAddresses"].([]any)
		if !ok || len(toAddresses) != 1 || toAddresses[0] != hex.EncodeToString(delegationTo.Bytes()) {
			t.Fatalf("%s/getdelegatedresourceaccountindexv2 = %v, want %x", prefix, delegationIndexJSON, delegationTo.Bytes())
		}
	}
	for _, prefix := range []string{"/walletsolidity", "/walletpbft"} {
		blockNumURL := fmt.Sprintf("%s%s/getblockbynum?num=%d", tronServer.URL, prefix, block1.Number())
		blockJSON := postSnapshotTestJSON(t, blockNumURL, `{}`)
		if got := blockJSON["blockID"]; got == nil || got == "" {
			t.Fatalf("%s/getblockbynum response missing blockID: %v", prefix, blockJSON)
		}
		txInfoURL := fmt.Sprintf("%s%s/gettransactioninfobyblocknum?num=%d", tronServer.URL, prefix, block1.Number())
		infosJSON := postSnapshotTestJSONArray(t, txInfoURL, `{}`)
		if len(infosJSON) != 1 {
			t.Fatalf("%s/gettransactioninfobyblocknum len = %d, want 1: %v", prefix, len(infosJSON), infosJSON)
		}
		if got := asFloat64(infosJSON[0]["fee"]); got != 777 {
			t.Fatalf("%s/gettransactioninfobyblocknum fee = %v, want 777: %v", prefix, infosJSON[0]["fee"], infosJSON)
		}
		if got := asFloat64(infosJSON[0]["blockNumber"]); got != float64(block1.Number()) {
			t.Fatalf("%s/gettransactioninfobyblocknum blockNumber = %v, want %d: %v", prefix, infosJSON[0]["blockNumber"], block1.Number(), infosJSON)
		}
		accountJSON := postSnapshotTestJSON(t, tronServer.URL+prefix+"/getaccount", fmt.Sprintf(`{"address":"%s"}`, hex.EncodeToString(archiveAddr.Bytes())))
		if got := asFloat64(accountJSON["balance"]); got != float64(archiveBalance1) {
			t.Fatalf("%s/getaccount balance = %v, want %d: %v", prefix, accountJSON["balance"], archiveBalance1, accountJSON)
		}
		resourceJSON := postSnapshotTestJSON(t, tronServer.URL+prefix+"/getaccountresource", fmt.Sprintf(`{"address":"%s"}`, hex.EncodeToString(archiveAddr.Bytes())))
		if _, ok := resourceJSON["freeNetUsed"]; !ok {
			t.Fatalf("%s/getaccountresource response missing freeNetUsed: %v", prefix, resourceJSON)
		}
		rewardJSON := postSnapshotTestJSON(t, tronServer.URL+prefix+"/getreward", fmt.Sprintf(`{"address":"%s"}`, hex.EncodeToString(archiveAddr.Bytes())))
		if got := asFloat64(rewardJSON["reward"]); got != float64(archiveReward1) {
			t.Fatalf("%s/getreward reward = %v, want %d: %v", prefix, rewardJSON["reward"], archiveReward1, rewardJSON)
		}
		deletedAccountJSON := postSnapshotTestJSON(t, tronServer.URL+prefix+"/getaccount", fmt.Sprintf(`{"address":"%s"}`, hex.EncodeToString(deletedAddr.Bytes())))
		if got := asFloat64(deletedAccountJSON["balance"]); got != float64(deletedBalance1) {
			t.Fatalf("%s/getaccount deleted-account balance = %v, want %d: %v", prefix, deletedAccountJSON["balance"], deletedBalance1, deletedAccountJSON)
		}
		recreatedAccountJSON := postSnapshotTestJSON(t, tronServer.URL+prefix+"/getaccount", fmt.Sprintf(`{"address":"%s"}`, hex.EncodeToString(recreatedAddr.Bytes())))
		if got := asFloat64(recreatedAccountJSON["balance"]); got != float64(recreatedBalance1) {
			t.Fatalf("%s/getaccount recreated-account balance = %v, want %d: %v", prefix, recreatedAccountJSON["balance"], recreatedBalance1, recreatedAccountJSON)
		}
		recreatedContractJSON := postSnapshotTestJSON(t, tronServer.URL+prefix+"/getcontract", fmt.Sprintf(`{"value":"%s"}`, hex.EncodeToString(recreatedAddr.Bytes())))
		if got := recreatedContractJSON["name"]; got != "snapshot-old-contract" {
			t.Fatalf("%s/getcontract recreated-contract name = %v, want snapshot-old-contract: %v", prefix, got, recreatedContractJSON)
		}
		if got := recreatedContractJSON["bytecode"]; got != hex.EncodeToString(archiveCode1) {
			t.Fatalf("%s/getcontract recreated-contract bytecode = %v, want %x: %v", prefix, got, archiveCode1, recreatedContractJSON)
		}
	}
	deletedHeadAccountJSON := postSnapshotTestJSON(t, tronServer.URL+"/wallet/getaccount", fmt.Sprintf(`{"address":"%s"}`, hex.EncodeToString(deletedAddr.Bytes())))
	if len(deletedHeadAccountJSON) != 0 {
		t.Fatalf("/wallet/getaccount deleted head account = %v, want empty object", deletedHeadAccountJSON)
	}
	recreatedHeadAccountJSON := postSnapshotTestJSON(t, tronServer.URL+"/wallet/getaccount", fmt.Sprintf(`{"address":"%s"}`, hex.EncodeToString(recreatedAddr.Bytes())))
	if got := asFloat64(recreatedHeadAccountJSON["balance"]); got != float64(recreatedBalance2) {
		t.Fatalf("/wallet/getaccount recreated head balance = %v, want %d: %v", recreatedHeadAccountJSON["balance"], recreatedBalance2, recreatedHeadAccountJSON)
	}
	recreatedHeadContractJSON := postSnapshotTestJSON(t, tronServer.URL+"/wallet/getcontract", fmt.Sprintf(`{"value":"%s"}`, hex.EncodeToString(recreatedAddr.Bytes())))
	if got := recreatedHeadContractJSON["name"]; got != "snapshot-new-contract" {
		t.Fatalf("/wallet/getcontract recreated head name = %v, want snapshot-new-contract: %v", got, recreatedHeadContractJSON)
	}
	if got := recreatedHeadContractJSON["bytecode"]; got != hex.EncodeToString(archiveCode2) {
		t.Fatalf("/wallet/getcontract recreated head bytecode = %v, want %x: %v", got, archiveCode2, recreatedHeadContractJSON)
	}
	tronLifecycleServer := tronapi.NewServer(backend, 0)
	if err := tronLifecycleServer.Start(); err != nil {
		t.Fatalf("start TRON API lifecycle server: %v", err)
	}
	defer tronLifecycleServer.Stop()
	tronLifecycleURL := snapshotTestServerURL(t, tronLifecycleServer.ListenAddr())
	lifecycleAccountJSON := postSnapshotTestJSON(t, tronLifecycleURL+"/walletsolidity/getaccount", fmt.Sprintf(`{"address":"%s"}`, hex.EncodeToString(recreatedAddr.Bytes())))
	if got := asFloat64(lifecycleAccountJSON["balance"]); got != float64(recreatedBalance1) {
		t.Fatalf("lifecycle /walletsolidity/getaccount recreated balance = %v, want %d: %v", lifecycleAccountJSON["balance"], recreatedBalance1, lifecycleAccountJSON)
	}
	lifecycleContractJSON := postSnapshotTestJSON(t, tronLifecycleURL+"/walletsolidity/getcontract", fmt.Sprintf(`{"value":"%s"}`, hex.EncodeToString(recreatedAddr.Bytes())))
	if got := lifecycleContractJSON["name"]; got != "snapshot-old-contract" {
		t.Fatalf("lifecycle /walletsolidity/getcontract recreated name = %v, want snapshot-old-contract: %v", got, lifecycleContractJSON)
	}

	rpcServer := httptest.NewServer(jsonrpcapi.NewAPI(backend))
	defer rpcServer.Close()
	blockRPC := postSnapshotTestRPC(t, rpcServer.URL, "eth_getBlockByHash", []any{"0x" + blockHashHex, false})
	blockRPCResult, ok := blockRPC["result"].(map[string]any)
	if !ok || blockRPCResult["hash"] != "0x"+blockHashHex {
		t.Fatalf("eth_getBlockByHash result = %v, want block1 hash", blockRPC["result"])
	}
	blockByNumberRPC := postSnapshotTestRPC(t, rpcServer.URL, "eth_getBlockByNumber", []any{"0x1", true})
	blockByNumber, ok := blockByNumberRPC["result"].(map[string]any)
	if !ok || blockByNumber["hash"] != "0x"+blockHashHex || blockByNumber["number"] != "0x1" {
		t.Fatalf("eth_getBlockByNumber result = %v, want block1", blockByNumberRPC["result"])
	}
	blockTxs, ok := blockByNumber["transactions"].([]any)
	if !ok || len(blockTxs) != 1 {
		t.Fatalf("eth_getBlockByNumber transactions = %v, want one full tx", blockByNumber["transactions"])
	}
	blockTx, ok := blockTxs[0].(map[string]any)
	if !ok || blockTx["hash"] != "0x"+txHashHex || blockTx["blockHash"] != "0x"+blockHashHex || blockTx["blockNumber"] != "0x1" {
		t.Fatalf("eth_getBlockByNumber tx = %v, want tx/block1 hashes", blockTxs[0])
	}
	blockHashesOnlyRPC := postSnapshotTestRPC(t, rpcServer.URL, "eth_getBlockByNumber", []any{"0x1", false})
	blockHashesOnly, ok := blockHashesOnlyRPC["result"].(map[string]any)
	if !ok || blockHashesOnly["hash"] != "0x"+blockHashHex || blockHashesOnly["number"] != "0x1" {
		t.Fatalf("eth_getBlockByNumber hashes-only result = %v, want block1", blockHashesOnlyRPC["result"])
	}
	blockTxHashes, ok := blockHashesOnly["transactions"].([]any)
	if !ok || len(blockTxHashes) != 1 || blockTxHashes[0] != "0x"+txHashHex {
		t.Fatalf("eth_getBlockByNumber hashes-only transactions = %v, want tx hash", blockHashesOnly["transactions"])
	}
	txRPC := postSnapshotTestRPC(t, rpcServer.URL, "eth_getTransactionByHash", []any{"0x" + txHashHex})
	txResult, ok := txRPC["result"].(map[string]any)
	if !ok || txResult["hash"] != "0x"+txHashHex || txResult["blockHash"] != "0x"+blockHashHex || txResult["blockNumber"] != "0x1" {
		t.Fatalf("eth_getTransactionByHash result = %v, want tx/block1 hashes", txRPC["result"])
	}
	receiptRPC := postSnapshotTestRPC(t, rpcServer.URL, "eth_getTransactionReceipt", []any{"0x" + txHashHex})
	receipt, ok := receiptRPC["result"].(map[string]any)
	if !ok || receipt["transactionHash"] != "0x"+txHashHex || receipt["blockHash"] != "0x"+blockHashHex {
		t.Fatalf("eth_getTransactionReceipt result = %v, want tx/block1 hashes", receiptRPC["result"])
	}
	balanceRPC := postSnapshotTestRPC(t, rpcServer.URL, "eth_getBalance", []any{"0x" + hex.EncodeToString(archiveAddr.Bytes()), "0x1"})
	if got := balanceRPC["result"]; got != snapshotTestSunToWeiHex(archiveBalance1) {
		t.Fatalf("eth_getBalance archive result = %v, want %s", got, snapshotTestSunToWeiHex(archiveBalance1))
	}
	codeRPC := postSnapshotTestRPC(t, rpcServer.URL, "eth_getCode", []any{"0x" + hex.EncodeToString(archiveAddr.Bytes()), "0x1"})
	if got := codeRPC["result"]; got != "0x"+hex.EncodeToString(archiveCode1) {
		t.Fatalf("eth_getCode archive result = %v, want 0x%s", got, hex.EncodeToString(archiveCode1))
	}
	storageRPC := postSnapshotTestRPC(t, rpcServer.URL, "eth_getStorageAt", []any{"0x" + hex.EncodeToString(archiveAddr.Bytes()), "0x" + hex.EncodeToString(archiveSlot.Bytes()), "0x1"})
	if got := storageRPC["result"]; got != "0x"+hex.EncodeToString(archiveStorage1.Bytes()) {
		t.Fatalf("eth_getStorageAt archive result = %v, want 0x%s", got, hex.EncodeToString(archiveStorage1.Bytes()))
	}
	logsRPC := postSnapshotTestRPC(t, rpcServer.URL, "eth_getLogs", []any{map[string]any{
		"fromBlock": "0x1",
		"toBlock":   "0x1",
		"address":   "0x" + hex.EncodeToString(logAddress),
		"topics":    []any{"0x" + hex.EncodeToString(logTopic.Bytes())},
	}})
	logsResult, ok := logsRPC["result"].([]any)
	if !ok || len(logsResult) != 1 {
		t.Fatalf("eth_getLogs restored cold result = %v, want one log", logsRPC["result"])
	}
	logObj, ok := logsResult[0].(map[string]any)
	if !ok {
		t.Fatalf("eth_getLogs restored cold log = %T %v, want object", logsResult[0], logsResult[0])
	}
	if logObj["data"] != "0x5c5d" ||
		logObj["blockHash"] != fmt.Sprintf("0x%x", block1.Hash()) ||
		logObj["transactionHash"] != fmt.Sprintf("0x%x", txHash) {
		t.Fatalf("eth_getLogs restored cold log = %v, want block1 tx log", logObj)
	}
	wantLogAddress := "0x" + hex.EncodeToString(logAddress[len(logAddress)-common.AccountIDLength:])
	if logObj["address"] != wantLogAddress {
		t.Fatalf("eth_getLogs restored cold address = %v, want %s", logObj["address"], wantLogAddress)
	}
	logsByHashRPC := postSnapshotTestRPC(t, rpcServer.URL, "eth_getLogs", []any{map[string]any{
		"blockHash": "0x" + blockHashHex,
		"address":   "0x" + hex.EncodeToString(logAddress),
		"topics":    []any{"0x" + hex.EncodeToString(logTopic.Bytes())},
	}})
	logsByHash, ok := logsByHashRPC["result"].([]any)
	if !ok || len(logsByHash) != 1 {
		t.Fatalf("eth_getLogs restored cold blockHash result = %v, want one log", logsByHashRPC["result"])
	}
	logByHash, ok := logsByHash[0].(map[string]any)
	if !ok {
		t.Fatalf("eth_getLogs restored cold blockHash log = %T %v, want object", logsByHash[0], logsByHash[0])
	}
	if logByHash["data"] != "0x5c5d" ||
		logByHash["blockHash"] != fmt.Sprintf("0x%x", block1.Hash()) ||
		logByHash["transactionHash"] != fmt.Sprintf("0x%x", txHash) ||
		logByHash["address"] != wantLogAddress {
		t.Fatalf("eth_getLogs restored cold blockHash log = %v, want block1 tx log", logByHash)
	}
	deletedBalanceRPC := postSnapshotTestRPC(t, rpcServer.URL, "eth_getBalance", []any{"0x" + hex.EncodeToString(deletedAddr.Bytes()), "0x1"})
	if got := deletedBalanceRPC["result"]; got != snapshotTestSunToWeiHex(deletedBalance1) {
		t.Fatalf("eth_getBalance deleted-account block1 result = %v, want %s", got, snapshotTestSunToWeiHex(deletedBalance1))
	}
	deletedHeadBalanceRPC := postSnapshotTestRPC(t, rpcServer.URL, "eth_getBalance", []any{"0x" + hex.EncodeToString(deletedAddr.Bytes()), "0x2"})
	if got := deletedHeadBalanceRPC["result"]; got != snapshotTestSunToWeiHex(0) {
		t.Fatalf("eth_getBalance deleted-account block2 result = %v, want %s", got, snapshotTestSunToWeiHex(0))
	}
	recreatedBalanceRPC := postSnapshotTestRPC(t, rpcServer.URL, "eth_getBalance", []any{"0x" + hex.EncodeToString(recreatedAddr.Bytes()), "0x1"})
	if got := recreatedBalanceRPC["result"]; got != snapshotTestSunToWeiHex(recreatedBalance1) {
		t.Fatalf("eth_getBalance recreated-account block1 result = %v, want %s", got, snapshotTestSunToWeiHex(recreatedBalance1))
	}
	recreatedHeadBalanceRPC := postSnapshotTestRPC(t, rpcServer.URL, "eth_getBalance", []any{"0x" + hex.EncodeToString(recreatedAddr.Bytes()), "0x2"})
	if got := recreatedHeadBalanceRPC["result"]; got != snapshotTestSunToWeiHex(recreatedBalance2) {
		t.Fatalf("eth_getBalance recreated-account block2 result = %v, want %s", got, snapshotTestSunToWeiHex(recreatedBalance2))
	}
	recreatedOldCodeRPC := postSnapshotTestRPC(t, rpcServer.URL, "eth_getCode", []any{"0x" + hex.EncodeToString(recreatedAddr.Bytes()), "0x1"})
	if got := recreatedOldCodeRPC["result"]; got != "0x"+hex.EncodeToString(archiveCode1) {
		t.Fatalf("eth_getCode recreated block1 result = %v, want 0x%s", got, hex.EncodeToString(archiveCode1))
	}
	recreatedNewCodeRPC := postSnapshotTestRPC(t, rpcServer.URL, "eth_getCode", []any{"0x" + hex.EncodeToString(recreatedAddr.Bytes()), "0x2"})
	if got := recreatedNewCodeRPC["result"]; got != "0x"+hex.EncodeToString(archiveCode2) {
		t.Fatalf("eth_getCode recreated block2 result = %v, want 0x%s", got, hex.EncodeToString(archiveCode2))
	}
	recreatedOldSlotRPC := postSnapshotTestRPC(t, rpcServer.URL, "eth_getStorageAt", []any{"0x" + hex.EncodeToString(recreatedAddr.Bytes()), "0x" + hex.EncodeToString(recreatedSlotB.Bytes()), "0x1"})
	if got := recreatedOldSlotRPC["result"]; got != "0x"+hex.EncodeToString(recreatedOldB.Bytes()) {
		t.Fatalf("eth_getStorageAt recreated old slotB block1 result = %v, want 0x%s", got, hex.EncodeToString(recreatedOldB.Bytes()))
	}
	recreatedNewSlotRPC := postSnapshotTestRPC(t, rpcServer.URL, "eth_getStorageAt", []any{"0x" + hex.EncodeToString(recreatedAddr.Bytes()), "0x" + hex.EncodeToString(recreatedSlotA.Bytes()), "0x2"})
	if got := recreatedNewSlotRPC["result"]; got != "0x"+hex.EncodeToString(recreatedNewA.Bytes()) {
		t.Fatalf("eth_getStorageAt recreated new slotA block2 result = %v, want 0x%s", got, hex.EncodeToString(recreatedNewA.Bytes()))
	}
	recreatedLeakedSlotRPC := postSnapshotTestRPC(t, rpcServer.URL, "eth_getStorageAt", []any{"0x" + hex.EncodeToString(recreatedAddr.Bytes()), "0x" + hex.EncodeToString(recreatedSlotB.Bytes()), "0x2"})
	if got := recreatedLeakedSlotRPC["result"]; got != "0x"+hex.EncodeToString(common.Hash{}.Bytes()) {
		t.Fatalf("eth_getStorageAt recreated slotB block2 result = %v, want zero", got)
	}
	rpcLifecycleServer := jsonrpcapi.NewServer(backend, 0)
	if err := rpcLifecycleServer.Start(); err != nil {
		t.Fatalf("start JSON-RPC lifecycle server: %v", err)
	}
	defer rpcLifecycleServer.Stop()
	rpcLifecycleURL := snapshotTestServerURL(t, rpcLifecycleServer.ListenAddr())
	lifecycleBalanceRPC := postSnapshotTestRPC(t, rpcLifecycleURL, "eth_getBalance", []any{"0x" + hex.EncodeToString(archiveAddr.Bytes()), "0x1"})
	if got := lifecycleBalanceRPC["result"]; got != snapshotTestSunToWeiHex(archiveBalance1) {
		t.Fatalf("lifecycle eth_getBalance archive result = %v, want %s", got, snapshotTestSunToWeiHex(archiveBalance1))
	}
	lifecycleTxRPC := postSnapshotTestRPC(t, rpcLifecycleURL, "eth_getTransactionByHash", []any{"0x" + txHashHex})
	lifecycleTx, ok := lifecycleTxRPC["result"].(map[string]any)
	if !ok || lifecycleTx["hash"] != "0x"+txHashHex || lifecycleTx["blockHash"] != "0x"+blockHashHex || lifecycleTx["blockNumber"] != "0x1" {
		t.Fatalf("lifecycle eth_getTransactionByHash result = %v, want tx/block1 hashes", lifecycleTxRPC["result"])
	}
	lifecycleReceiptRPC := postSnapshotTestRPC(t, rpcLifecycleURL, "eth_getTransactionReceipt", []any{"0x" + txHashHex})
	lifecycleReceipt, ok := lifecycleReceiptRPC["result"].(map[string]any)
	if !ok || lifecycleReceipt["transactionHash"] != "0x"+txHashHex || lifecycleReceipt["blockHash"] != "0x"+blockHashHex {
		t.Fatalf("lifecycle eth_getTransactionReceipt result = %v, want tx/block1 hashes", lifecycleReceiptRPC["result"])
	}
}

func makeSnapshotRestoreTestContext(t *testing.T, argv []string) *cli.Context {
	t.Helper()
	restoreFlags := restoreSnapshotTestCLIFlagState()
	defer restoreFlags()
	app := cli.NewApp()
	app.Flags = []cli.Flag{
		dataDirFlag,
		testnetFlag,
		genesisFileFlag,
		devFlag,
		witnessKeyFlag,
		devFullFeaturesFlag,
		devMaintenanceIntervalFlag,
		dbCacheFlag,
		dbHandlesFlag,
		dbMemtableFlag,
		dbL0CompactionFlag,
		dbL0StopFlag,
		snapshotDirFlag,
		snapshotURLFlag,
		snapshotResetFlag,
		snapshotFetchConcurrencyFlag,
		snapshotTrustedCatalogKeyFlag,
		snapshotTrustedCatalogKeyFileFlag,
		snapshotForkConfigHashFlag,
		snapshotCatalogSigningKeyFlag,
		snapshotCatalogSigningKeyFileFlag,
		snapshotFromBlockFlag,
		snapshotToBlockFlag,
		snapshotFromColdFlag,
		snapshotETLTempDirFlag,
		snapshotETLBufferMiBFlag,
		snapshotETLBatchMiBFlag,
	}
	set := flag.NewFlagSet("snapshot-restore-test", flag.ContinueOnError)
	for _, f := range app.Flags {
		if err := f.Apply(set); err != nil {
			t.Fatalf("apply flag: %v", err)
		}
	}
	if err := set.Parse(argv); err != nil {
		t.Fatalf("parse flags: %v", err)
	}
	return cli.NewContext(app, set, nil)
}

func restoreSnapshotTestCLIFlagState() func() {
	snapshotURLValue, snapshotURLHasBeenSet := snapshotURLFlag.Value, snapshotURLFlag.HasBeenSet
	snapshotTrustedCatalogKeyHasBeenSet := snapshotTrustedCatalogKeyFlag.HasBeenSet
	snapshotTrustedCatalogKeyFileValue, snapshotTrustedCatalogKeyFileHasBeenSet := snapshotTrustedCatalogKeyFileFlag.Value, snapshotTrustedCatalogKeyFileFlag.HasBeenSet
	snapshotForkConfigHashValue, snapshotForkConfigHashHasBeenSet := snapshotForkConfigHashFlag.Value, snapshotForkConfigHashFlag.HasBeenSet
	snapshotCatalogSigningKeyValue, snapshotCatalogSigningKeyHasBeenSet := snapshotCatalogSigningKeyFlag.Value, snapshotCatalogSigningKeyFlag.HasBeenSet
	snapshotCatalogSigningKeyFileValue, snapshotCatalogSigningKeyFileHasBeenSet := snapshotCatalogSigningKeyFileFlag.Value, snapshotCatalogSigningKeyFileFlag.HasBeenSet
	return func() {
		snapshotURLFlag.Value, snapshotURLFlag.HasBeenSet = snapshotURLValue, snapshotURLHasBeenSet
		snapshotTrustedCatalogKeyFlag.HasBeenSet = snapshotTrustedCatalogKeyHasBeenSet
		snapshotTrustedCatalogKeyFileFlag.Value, snapshotTrustedCatalogKeyFileFlag.HasBeenSet = snapshotTrustedCatalogKeyFileValue, snapshotTrustedCatalogKeyFileHasBeenSet
		snapshotForkConfigHashFlag.Value, snapshotForkConfigHashFlag.HasBeenSet = snapshotForkConfigHashValue, snapshotForkConfigHashHasBeenSet
		snapshotCatalogSigningKeyFlag.Value, snapshotCatalogSigningKeyFlag.HasBeenSet = snapshotCatalogSigningKeyValue, snapshotCatalogSigningKeyHasBeenSet
		snapshotCatalogSigningKeyFileFlag.Value, snapshotCatalogSigningKeyFileFlag.HasBeenSet = snapshotCatalogSigningKeyFileValue, snapshotCatalogSigningKeyFileHasBeenSet
	}
}

func makeSnapshotRestoreTestContextWithEnv(t *testing.T, argv []string, env map[string]*string) *cli.Context {
	t.Helper()
	restore := setSnapshotTestEnv(t, env)
	defer restore()
	return makeSnapshotRestoreTestContext(t, argv)
}

func snapshotTestEnvValue(value string) *string {
	return &value
}

func setSnapshotTestEnv(t *testing.T, env map[string]*string) func() {
	t.Helper()
	saved := make(map[string]*string, len(env))
	for key, value := range env {
		if old, ok := os.LookupEnv(key); ok {
			saved[key] = snapshotTestEnvValue(old)
		} else {
			saved[key] = nil
		}
		if value == nil {
			if err := os.Unsetenv(key); err != nil {
				t.Fatalf("unset env %s: %v", key, err)
			}
			continue
		}
		if err := os.Setenv(key, *value); err != nil {
			t.Fatalf("set env %s: %v", key, err)
		}
	}
	return func() {
		for key, value := range saved {
			var err error
			if value == nil {
				err = os.Unsetenv(key)
			} else {
				err = os.Setenv(key, *value)
			}
			if err != nil {
				t.Fatalf("restore env %s: %v", key, err)
			}
		}
	}
}

func postSnapshotTestJSON(t *testing.T, url, body string) map[string]any {
	t.Helper()
	resp, err := http.Post(url, "application/json", strings.NewReader(body))
	if err != nil {
		t.Fatalf("POST %s: %v", url, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("POST %s status = %d, want 200", url, resp.StatusCode)
	}
	var out map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatalf("decode %s: %v", url, err)
	}
	return out
}

func postSnapshotTestJSONArray(t *testing.T, url, body string) []map[string]any {
	t.Helper()
	resp, err := http.Post(url, "application/json", strings.NewReader(body))
	if err != nil {
		t.Fatalf("POST %s: %v", url, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("POST %s status = %d, want 200", url, resp.StatusCode)
	}
	var out []map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatalf("decode %s: %v", url, err)
	}
	return out
}

func postSnapshotTestRPC(t *testing.T, url, method string, params any) map[string]any {
	t.Helper()
	body, err := json.Marshal(map[string]any{
		"jsonrpc": "2.0",
		"method":  method,
		"params":  params,
		"id":      1,
	})
	if err != nil {
		t.Fatalf("marshal rpc body: %v", err)
	}
	resp, err := http.Post(url, "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("POST rpc %s: %v", method, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("POST rpc %s status = %d, want 200", method, resp.StatusCode)
	}
	var out map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatalf("decode rpc %s: %v", method, err)
	}
	if errValue, ok := out["error"]; ok {
		t.Fatalf("rpc %s error: %v", method, errValue)
	}
	return out
}

func asFloat64(v any) float64 {
	got, _ := v.(float64)
	return got
}

func snapshotCmdAccountEnvelope(t *testing.T, addr common.Address, balance int64, accountType corepb.AccountType, codeHash common.Hash, allowance ...int64) []byte {
	t.Helper()
	return snapshotCmdAccountEnvelopeWithGeneration(t, addr, balance, accountType, codeHash, 0, allowance...)
}

func snapshotCmdAccountEnvelopeWithGeneration(t *testing.T, addr common.Address, balance int64, accountType corepb.AccountType, codeHash common.Hash, generation uint64, allowance ...int64) []byte {
	t.Helper()
	if len(allowance) > 1 {
		t.Fatalf("snapshotCmdAccountEnvelope: expected at most one allowance, got %d", len(allowance))
	}
	account := coretypes.NewAccount(addr, accountType)
	account.SetBalance(balance)
	if len(allowance) == 1 {
		account.SetAllowance(allowance[0])
	}
	accountRaw, err := proto.Marshal(account.Proto())
	if err != nil {
		t.Fatalf("marshal account: %v", err)
	}
	envelope := &corestate.StateAccountV2{
		Version:             corestate.StateAccountVersion,
		AccountProto:        accountRaw,
		AccountKVRoot:       corestate.EmptyKVRoot,
		AccountKVGeneration: generation,
		CodeHash:            codeHash,
	}
	raw, err := envelope.Encode()
	if err != nil {
		t.Fatalf("encode account envelope: %v", err)
	}
	return raw
}

func snapshotCmdStorageRowKey(addr common.Address, key common.Hash) common.Hash {
	addrHash := common.Keccak256(addr.Bytes())
	var rowKey common.Hash
	copy(rowKey[:16], addrHash[:16])
	copy(rowKey[16:], key[16:])
	return rowKey
}

func snapshotCmdContractMetaKey() []byte {
	return []byte("meta")
}

func snapshotCmdContractMetadata(t *testing.T, addr common.Address, name string, bytecode []byte) []byte {
	t.Helper()
	raw, err := proto.Marshal(&contractpb.SmartContract{
		ContractAddress: addr.Bytes(),
		Name:            name,
		Bytecode:        bytecode,
		OriginAddress:   addr.Bytes(),
	})
	if err != nil {
		t.Fatalf("marshal contract metadata: %v", err)
	}
	return raw
}

func snapshotTestSunToWeiHex(sun int64) string {
	wei := new(big.Int).Mul(big.NewInt(sun), big.NewInt(1_000_000_000_000))
	return fmt.Sprintf("0x%x", wei)
}

func snapshotTestServerURL(t *testing.T, listenAddr string) string {
	t.Helper()
	host, port, err := net.SplitHostPort(listenAddr)
	if err != nil {
		t.Fatalf("split listen addr %q: %v", listenAddr, err)
	}
	if host == "" || host == "::" || host == "0.0.0.0" {
		host = "127.0.0.1"
	}
	return "http://" + net.JoinHostPort(host, port)
}

func mustSnapshotCmdDelegationResource(t *testing.T, dr *rawdb.DelegatedResource) []byte {
	t.Helper()
	raw, err := json.Marshal(dr)
	if err != nil {
		t.Fatalf("marshal delegated resource: %v", err)
	}
	return raw
}

func writeSnapshotCmdRemoteFetchSource(t *testing.T, sourceDir string) (statesnapshots.ChainIdentity, ed25519.PublicKey, *statesnapshots.SnapshotCatalog, []statesnapshots.SegmentRef) {
	t.Helper()
	owner := common.BytesToAddress(append([]byte{common.AddressPrefixMainnet}, bytes.Repeat([]byte{0x24}, common.AccountIDLength)...))
	traceOwner := snapshotCmdRemoteTraceOwner()
	sourceDB := rawdb.NewMemoryDatabase()
	if err := rawdb.WriteStateTxRange(sourceDB, 1, common.Hash{0x01}, 1, 1); err != nil {
		t.Fatalf("WriteStateTxRange: %v", err)
	}
	if err := rawdb.WriteStateDomainChange(sourceDB, &rawdb.StateDomainChange{
		BlockNum:   1,
		BlockHash:  common.Hash{0x01},
		TxNum:      1,
		Seq:        1,
		FlatDomain: rawdb.StateFlatDomainAccountLatest,
		Owner:      owner,
		PrevExists: true,
		Prev:       snapshotCmdAccountEnvelope(t, owner, 10, corepb.AccountType_Normal, common.Hash{}),
		NextExists: true,
		Next:       snapshotCmdAccountEnvelope(t, owner, 20, corepb.AccountType_Normal, common.Hash{}),
	}); err != nil {
		t.Fatalf("WriteStateDomainChange: %v", err)
	}
	refs, err := statesnapshots.BuildStateDomainChangeHistorySegmentsFromDB(sourceDB, sourceDir, 1, 1, "history/state-domain-change-1-1.seg")
	if err != nil {
		t.Fatalf("BuildStateDomainChangeHistorySegmentsFromDB: %v", err)
	}
	if len(refs) != 3 {
		t.Fatalf("history refs = %+v, want segment+index+accessor", refs)
	}
	if err := rawdb.WriteBlockBalanceTrace(sourceDB, 1, &contractpb.BlockBalanceTrace{
		BlockIdentifier: &contractpb.BlockBalanceTrace_BlockIdentifier{
			Hash:   bytes.Repeat([]byte{0x01}, common.HashLength),
			Number: 1,
		},
		Timestamp: 101,
	}); err != nil {
		t.Fatalf("WriteBlockBalanceTrace: %v", err)
	}
	if err := rawdb.WriteAccountTrace(sourceDB, traceOwner.Bytes(), 1, 55); err != nil {
		t.Fatalf("WriteAccountTrace: %v", err)
	}
	traceRef, err := statesnapshots.BuildBalanceTraceSegmentFromDB(sourceDB, sourceDir, "", 1, 1)
	if err != nil {
		t.Fatalf("BuildBalanceTraceSegmentFromDB: %v", err)
	}
	refs = append(refs, traceRef)
	identity, err := snapshotExpectedChainIdentityFromGenesis(params.DefaultMainnetGenesis(), "")
	if err != nil {
		t.Fatalf("snapshotExpectedChainIdentityFromGenesis: %v", err)
	}
	if err := statesnapshots.PublishManifest(sourceDir, statesnapshots.NewManifestForChain(1, 1, refs, identity)); err != nil {
		t.Fatalf("PublishManifest: %v", err)
	}
	pub, priv, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatalf("GenerateKey: %v", err)
	}
	catalog, err := statesnapshots.PublishSignedSnapshotCatalog(sourceDir, priv)
	if err != nil {
		t.Fatalf("PublishSignedSnapshotCatalog: %v", err)
	}
	return identity, pub, catalog, refs
}

func snapshotCmdRemoteTraceOwner() common.Address {
	return common.BytesToAddress(append([]byte{common.AddressPrefixMainnet}, bytes.Repeat([]byte{0x25}, common.AccountIDLength)...))
}

func snapshotCmdAssertSameFile(t *testing.T, wantPath, gotPath string) {
	t.Helper()
	want, err := os.ReadFile(wantPath)
	if err != nil {
		t.Fatalf("read source file %s: %v", wantPath, err)
	}
	got, err := os.ReadFile(gotPath)
	if err != nil {
		t.Fatalf("read downloaded file %s: %v", gotPath, err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("downloaded file %s differs from source %s", gotPath, wantPath)
	}
}

type snapshotCmdFreezerRow struct {
	block      *coretypes.Block
	txInfosRaw []byte
	stateRoot  []byte
}

func openSnapshotCmdFreezer(t *testing.T, dir string) *rawdbfreezer.Freezer {
	t.Helper()
	f, err := rawdbfreezer.NewFreezer(dir, "", false, 2049, chainfreezer.FreezerTableSet())
	if err != nil {
		t.Fatalf("NewFreezer: %v", err)
	}
	return f
}

func appendSnapshotCmdFreezerRows(t *testing.T, f *rawdbfreezer.Freezer, rows []snapshotCmdFreezerRow) {
	t.Helper()
	if _, err := f.ModifyAncients(func(op rawdb.AncientWriteOp) error {
		for i, row := range rows {
			if row.block == nil {
				return fmt.Errorf("nil block at row %d", i)
			}
			n := row.block.Number()
			if n != uint64(i) {
				return fmt.Errorf("row %d has block number %d", i, n)
			}
			blockRaw, err := row.block.Marshal()
			if err != nil {
				return err
			}
			if err := op.AppendRaw(rawdb.AncientBlocksTable, n, blockRaw); err != nil {
				return err
			}
			if err := op.AppendRaw(rawdb.AncientTxInfosTable, n, row.txInfosRaw); err != nil {
				return err
			}
			if err := op.AppendRaw(rawdb.AncientStateRootsTable, n, row.stateRoot); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatalf("ModifyAncients: %v", err)
	}
	if err := f.Sync(); err != nil {
		t.Fatalf("Sync: %v", err)
	}
}

func snapshotCmdBlock(number uint64) *coretypes.Block {
	return coretypes.NewBlockFromPB(&corepb.Block{
		BlockHeader: &corepb.BlockHeader{
			RawData: &corepb.BlockHeaderRaw{
				Number:    int64(number),
				Timestamp: int64(30_000 + number),
			},
		},
	})
}

func snapshotCmdBlockWithTx(t *testing.T, number uint64) (*coretypes.Block, common.Hash, []byte) {
	t.Helper()
	txPB := &corepb.Transaction{
		RawData: &corepb.TransactionRaw{
			Timestamp:  int64(10_000 + number),
			Expiration: int64(20_000 + number),
		},
	}
	tx := coretypes.NewTransactionFromPB(txPB)
	txHash := tx.Hash()
	block := coretypes.NewBlockFromPB(&corepb.Block{
		BlockHeader: &corepb.BlockHeader{
			RawData: &corepb.BlockHeaderRaw{
				Number:    int64(number),
				Timestamp: int64(30_000 + number),
			},
		},
		Transactions: []*corepb.Transaction{txPB},
	})
	ret := &corepb.TransactionRet{
		BlockNumber: int64(number),
		Transactioninfo: []*corepb.TransactionInfo{
			{
				Id:          txHash[:],
				Fee:         777,
				BlockNumber: int64(number),
			},
		},
	}
	txInfoRaw, err := proto.Marshal(ret)
	if err != nil {
		t.Fatalf("marshal tx info: %v", err)
	}
	return block, txHash, txInfoRaw
}

func snapshotCmdTxInfoRawForBlock(t *testing.T, block *coretypes.Block) []byte {
	t.Helper()
	txs := block.Transactions()
	if len(txs) == 0 {
		return nil
	}
	ret := &corepb.TransactionRet{
		BlockNumber:     int64(block.Number()),
		BlockTimeStamp:  block.Timestamp(),
		Transactioninfo: make([]*corepb.TransactionInfo, 0, len(txs)),
	}
	for _, tx := range txs {
		txHash := tx.Hash()
		ret.Transactioninfo = append(ret.Transactioninfo, &corepb.TransactionInfo{
			Id:             txHash[:],
			BlockNumber:    int64(block.Number()),
			BlockTimeStamp: block.Timestamp(),
		})
	}
	raw, err := proto.Marshal(ret)
	if err != nil {
		t.Fatalf("marshal tx info for block %d: %v", block.Number(), err)
	}
	return raw
}

func snapshotCmdSectionBloomEncodedBit(t *testing.T, bit uint64) []byte {
	t.Helper()
	encoded, err := rawdb.EncodeSectionBloomBitSet(snapshotCmdSectionBloomSetBit(nil, bit))
	if err != nil {
		t.Fatalf("EncodeSectionBloomBitSet: %v", err)
	}
	return encoded
}

func snapshotCmdSectionBloomSetBit(bitset []byte, bit uint64) []byte {
	byteIndex := bit / 8
	if byteIndex >= uint64(len(bitset)) {
		grown := make([]byte, byteIndex+1)
		copy(grown, bitset)
		bitset = grown
	}
	bitset[byteIndex] |= 1 << (bit % 8)
	return bitset
}
