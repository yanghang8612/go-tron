package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/tronprotocol/go-tron/core/conformance"
	corepb "github.com/tronprotocol/go-tron/proto/core"
)

func run(cfg captureConfig) error {
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	cli, err := newHTTPClient(cfg.httpURL, cfg.socks5)
	if err != nil {
		return fmt.Errorf("http client: %w", err)
	}

	// 1. Probe head + resolve start.
	nowBlk, err := cli.getNowBlock(ctx)
	if err != nil {
		return fmt.Errorf("probe head: %w", err)
	}
	head := uint64(nowBlk.BlockHeader.RawData.Number)
	log.Printf("current head=%d witness=%x", head, nowBlk.BlockHeader.RawData.WitnessAddress)

	var start uint64
	if cfg.startExplicit > 0 {
		start = cfg.startExplicit
	} else {
		start = head + uint64(cfg.startAutoBuffer)
	}
	end := start + uint64(cfg.count) - 1
	log.Printf("range %s: capturing [%d..%d] (%d blocks); seed at %d", cfg.rangeName, start, end, cfg.count, start-1)

	// 2. Resolve closure (active or full-candidate set).
	closure, err := resolveClosure(ctx, cli, cfg)
	if err != nil {
		return fmt.Errorf("closure: %w", err)
	}
	log.Printf("closure size: %d address(es)", len(closure))

	rangeDir := filepath.Join(cfg.outRoot, cfg.rangeName)
	if err := os.MkdirAll(rangeDir, 0o755); err != nil {
		return fmt.Errorf("mkdir %s: %w", rangeDir, err)
	}

	// 3. Capture genesis time (for fixture.json). For TRON mainnet this is
	// 0 (the on-disk genesis block has timestamp=0, and SR latestSlotNum
	// values confirm — see DposSlot.getAbSlot). Lite nodes that pruned
	// block 0 still respond, just with the zero default. Nile uses
	// 1529891469000; provide via --genesis-time-override when needed.
	genesisTime, err := readGenesisTime(ctx, cli)
	if err != nil {
		log.Printf("WARN: genesis time unavailable (%v); defaulting to 0 (mainnet)", err)
		genesisTime = 0
	}

	// 4. Wait for head to reach start-1 and snapshot seed.
	if err := waitForHead(ctx, cli, start-1); err != nil {
		return fmt.Errorf("wait for head=%d: %w", start-1, err)
	}
	log.Printf("head reached start-1 (%d), capturing seed", start-1)
	seed, activeWitnessesAtStart, err := captureSeed(ctx, cli, start, closure, cfg.parallel)
	if err != nil {
		return fmt.Errorf("captureSeed: %w", err)
	}
	if err := writeJSONFile(filepath.Join(rangeDir, "seed.json"), seed); err != nil {
		return fmt.Errorf("write seed.json: %w", err)
	}

	// 5. Open blocks.bin + oracle.ndjson; truncate for fresh capture.
	blocksFile, err := os.Create(filepath.Join(rangeDir, "blocks.bin"))
	if err != nil {
		return fmt.Errorf("create blocks.bin: %w", err)
	}
	defer blocksFile.Close()
	blocksBuf := bufio.NewWriter(blocksFile)

	oracleFile, err := os.Create(filepath.Join(rangeDir, "oracle.ndjson"))
	if err != nil {
		return fmt.Errorf("create oracle.ndjson: %w", err)
	}
	defer oracleFile.Close()
	oracleBuf := bufio.NewWriter(oracleFile)

	// 6. Per-block loop.
	for h := start; h <= end; h++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := waitForHead(ctx, cli, h); err != nil {
			return fmt.Errorf("wait for h=%d: %w", h, err)
		}
		log.Printf("h=%d reached", h)

		block, err := cli.getBlockByNum(ctx, h)
		if err != nil {
			return fmt.Errorf("h=%d block fetch: %w", h, err)
		}
		blockBytes, err := proto.Marshal(block)
		if err != nil {
			return fmt.Errorf("h=%d block marshal: %w", h, err)
		}
		if err := writeUvarintFrame(blocksBuf, blockBytes); err != nil {
			return fmt.Errorf("h=%d blocks.bin write: %w", h, err)
		}

		snap, err := captureSnapshot(ctx, cli, h, closure, cfg.parallel)
		if err != nil {
			return fmt.Errorf("h=%d snapshot: %w", h, err)
		}

		// Verify head is still exactly h (or h+1 — block may have rolled
		// to next slot during snapshot). If head jumped past h+1 the
		// snapshot is unreliable.
		nowBlk, err := cli.getNowBlock(ctx)
		if err == nil {
			postHead := uint64(nowBlk.BlockHeader.RawData.Number)
			if postHead > h+1 {
				log.Printf("WARN: head jumped past h+1 during h=%d snapshot (post=%d); state may be inconsistent", h, postHead)
			}
		}

		entry, err := computeOracleEntry(snap)
		if err != nil {
			return fmt.Errorf("h=%d digest: %w", h, err)
		}
		entryBytes, _ := json.Marshal(entry)
		if _, err := oracleBuf.Write(append(entryBytes, '\n')); err != nil {
			return fmt.Errorf("h=%d oracle.ndjson write: %w", h, err)
		}
	}

	if err := blocksBuf.Flush(); err != nil {
		return err
	}
	if err := oracleBuf.Flush(); err != nil {
		return err
	}

	// 7. fixture.json + divergence-allowlist.json (empty).
	jtVer := nowBlk.BlockHeader.RawData.Version
	meta := conformance.FixtureMeta{
		Schema:          conformance.SchemaVersion,
		Scenario:        cfg.rangeName,
		JavaTronVersion: fmt.Sprintf("blockVersion=%d", jtVer),
		JarSha256:       "remote-http-capture",
		CapturedAt:      time.Now().UTC().Format(time.RFC3339),
		StartBlock:      start,
		EndBlock:        end,
		GenesisTime:     genesisTime,
		ActiveWitnesses: activeWitnessesAtStart,
	}
	if err := writeJSONFile(filepath.Join(rangeDir, "fixture.json"), meta); err != nil {
		return fmt.Errorf("write fixture.json: %w", err)
	}
	if err := writeJSONFile(filepath.Join(rangeDir, "divergence-allowlist.json"), []conformance.AllowlistEntry{}); err != nil {
		return fmt.Errorf("write divergence-allowlist.json: %w", err)
	}
	log.Printf("range %s captured: %s/", cfg.rangeName, rangeDir)
	return nil
}

// waitForHead polls getnowblock until the chain head is >= target. Returns
// when reached, errors on context cancellation or repeated probe failures.
func waitForHead(ctx context.Context, cli *httpClient, target uint64) error {
	tick := time.NewTicker(500 * time.Millisecond)
	defer tick.Stop()
	var consecutiveErr int
	for {
		blk, err := cli.getNowBlock(ctx)
		if err != nil {
			consecutiveErr++
			if consecutiveErr > 10 {
				return fmt.Errorf("getnowblock failed 10x consecutively: %w", err)
			}
		} else {
			consecutiveErr = 0
			h := uint64(blk.BlockHeader.RawData.Number)
			if h >= target {
				return nil
			}
			// Log every ~6s while waiting.
			if time.Now().Unix()%6 == 0 {
				log.Printf("waiting head=%d → target=%d (Δ=%d)", h, target, target-h)
			}
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-tick.C:
		}
	}
}

// resolveClosure assembles the 41-hex closure list from --closure file (one
// per line) plus optional --closure-witnesses (active-only or full-candidate).
// Deduplicates; addresses are normalized to lowercase 41-hex.
func resolveClosure(ctx context.Context, cli *httpClient, cfg captureConfig) ([]string, error) {
	seen := map[string]struct{}{}
	var out []string
	add := func(a string) {
		a = strings.ToLower(strings.TrimSpace(a))
		if a == "" {
			return
		}
		if !strings.HasPrefix(a, "41") || len(a) != 42 {
			return
		}
		if _, ok := seen[a]; ok {
			return
		}
		seen[a] = struct{}{}
		out = append(out, a)
	}

	if cfg.closureFile != "" {
		data, err := os.ReadFile(cfg.closureFile)
		if err != nil {
			return nil, err
		}
		for _, line := range strings.Split(string(data), "\n") {
			add(line)
		}
	}

	if cfg.closureWitnesses {
		ws, err := cli.listWitnesses(ctx)
		if err != nil {
			return nil, fmt.Errorf("listwitnesses: %w", err)
		}
		for _, w := range ws {
			if cfg.closureActiveOnly && !w.IsJobs {
				continue
			}
			add(hex.EncodeToString(w.Address))
		}
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("empty closure: pass --closure FILE or --closure-witnesses")
	}
	return out, nil
}

// captureSeed snapshots state at start-1 for the closure. Returns the Seed
// and the list of active-witness 41-hex addresses (for fixture.json).
func captureSeed(ctx context.Context, cli *httpClient, startHeight uint64, closure []string, parallel int) (*conformance.Seed, []string, error) {
	dp, err := cli.getChainParameters(ctx)
	if err != nil {
		return nil, nil, err
	}

	witnesses, err := cli.listWitnesses(ctx)
	if err != nil {
		return nil, nil, err
	}
	witnessByAddr := make(map[string]*corepb.Witness, len(witnesses))
	var activeHex []string
	for _, w := range witnesses {
		ah := hex.EncodeToString(w.Address)
		witnessByAddr[ah] = w
		if w.IsJobs {
			activeHex = append(activeHex, ah)
		}
	}

	accounts, contracts, err := fetchAccountsAndContracts(ctx, cli, closure, parallel)
	if err != nil {
		return nil, nil, err
	}

	var seedAccounts []conformance.SeedAccount
	for addrHex, acc := range accounts {
		bs, mErr := proto.Marshal(acc)
		if mErr != nil {
			return nil, nil, fmt.Errorf("marshal account %s: %w", addrHex, mErr)
		}
		raw, _ := json.Marshal(b64(bs))
		seedAccounts = append(seedAccounts, conformance.SeedAccount{
			Address: addrHex,
			Raw:     raw,
		})
	}

	var seedContracts []conformance.SeedContract
	for addrHex, sc := range contracts {
		seedContracts = append(seedContracts, conformance.SeedContract{
			Address: addrHex,
			CodeHex: hex.EncodeToString(sc.Bytecode),
		})
	}

	var seedWitnesses []conformance.SeedWitness
	for _, addrHex := range closure {
		w, ok := witnessByAddr[addrHex]
		if !ok {
			continue
		}
		bs, mErr := proto.Marshal(w)
		if mErr != nil {
			return nil, nil, fmt.Errorf("marshal witness %s: %w", addrHex, mErr)
		}
		seedWitnesses = append(seedWitnesses, conformance.SeedWitness{
			Address:      addrHex,
			WitnessProto: b64(bs),
		})
	}

	return &conformance.Seed{
		Schema:           conformance.SchemaVersion,
		StartHeight:      startHeight,
		DynamicProps:     dp,
		Accounts:         seedAccounts,
		Contracts:        seedContracts,
		Witnesses:        seedWitnesses,
		ClosureAddresses: closure,
	}, activeHex, nil
}

// captureSnapshot snapshots post-state for block h. Same shape as captureSeed
// but emits the conformance.Snapshot wire form.
func captureSnapshot(ctx context.Context, cli *httpClient, blockNum uint64, closure []string, parallel int) (*conformance.Snapshot, error) {
	dp, err := cli.getChainParameters(ctx)
	if err != nil {
		return nil, err
	}
	witnesses, err := cli.listWitnesses(ctx)
	if err != nil {
		return nil, err
	}
	witnessByAddr := make(map[string]*corepb.Witness, len(witnesses))
	for _, w := range witnesses {
		witnessByAddr[hex.EncodeToString(w.Address)] = w
	}

	accounts, contracts, err := fetchAccountsAndContracts(ctx, cli, closure, parallel)
	if err != nil {
		return nil, err
	}

	snap := &conformance.Snapshot{
		BlockNum: blockNum,
		DP:       dp,
		Closure:  closure,
	}
	for addrHex, acc := range accounts {
		bs, mErr := proto.Marshal(acc)
		if mErr != nil {
			return nil, fmt.Errorf("marshal account %s: %w", addrHex, mErr)
		}
		snap.Accounts = append(snap.Accounts, conformance.SnapshotAccount{
			Address:      addrHex,
			AccountProto: b64(bs),
		})
	}
	for addrHex, sc := range contracts {
		snap.Code = append(snap.Code, conformance.SnapshotCode{
			Address: addrHex,
			CodeHex: hex.EncodeToString(sc.Bytecode),
		})
	}
	for _, addrHex := range closure {
		w, ok := witnessByAddr[addrHex]
		if !ok {
			continue
		}
		bs, mErr := proto.Marshal(w)
		if mErr != nil {
			return nil, fmt.Errorf("marshal witness %s: %w", addrHex, mErr)
		}
		snap.Witnesses = append(snap.Witnesses, conformance.SeedWitness{
			Address:      addrHex,
			WitnessProto: b64(bs),
		})
	}
	return snap, nil
}

// fetchAccountsAndContracts queries getaccount + getcontract concurrently for
// every closure entry. Returns maps keyed by 41-hex.
func fetchAccountsAndContracts(ctx context.Context, cli *httpClient, closure []string, parallel int) (map[string]*corepb.Account, map[string]*contractBundle, error) {
	if parallel < 1 {
		parallel = 1
	}
	type result struct {
		addr string
		acc  *corepb.Account
		err  error
	}
	in := make(chan string, len(closure))
	out := make(chan result, len(closure))
	var wg sync.WaitGroup
	for i := 0; i < parallel; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for addr := range in {
				a, err := cli.getAccount(ctx, addr)
				out <- result{addr: addr, acc: a, err: err}
			}
		}()
	}
	for _, a := range closure {
		in <- a
	}
	close(in)
	wg.Wait()
	close(out)

	accounts := map[string]*corepb.Account{}
	for r := range out {
		if r.err != nil {
			return nil, nil, fmt.Errorf("getaccount %s: %w", r.addr, r.err)
		}
		if r.acc != nil {
			accounts[r.addr] = r.acc
		}
	}

	// getcontract only for contract-typed accounts.
	contracts := map[string]*contractBundle{}
	for addr, acc := range accounts {
		if acc.Type != corepb.AccountType_Contract {
			continue
		}
		sc, err := cli.getContract(ctx, addr)
		if err != nil {
			return nil, nil, err
		}
		if sc == nil {
			continue
		}
		contracts[addr] = &contractBundle{Bytecode: sc.Bytecode}
	}
	return accounts, contracts, nil
}

// contractBundle pulls only the bytecode field from contractpb.SmartContract;
// ContractState is not exposed by /wallet/getcontract on the older java-tron
// HTTP — it lives in /wallet/getcontractinfo on more recent versions. For
// the maintenance-range POC, bytecode is enough.
type contractBundle struct {
	Bytecode []byte
}

// computeOracleEntry runs the captured snapshot through the same digest
// pipeline the replay engine uses, producing an OracleEntry with both
// DigestB and the diagC for diagnostic value.
func computeOracleEntry(snap *conformance.Snapshot) (conformance.OracleEntry, error) {
	data, err := json.Marshal(snap)
	if err != nil {
		return conformance.OracleEntry{}, err
	}
	loaded, parsed, err := conformance.LoadSnapshot(bytes.NewReader(data))
	if err != nil {
		return conformance.OracleEntry{}, fmt.Errorf("load snapshot: %w", err)
	}
	d := conformance.DigestB(loaded.StateDB, loaded.DiskDB, loaded.Closure, loaded.DynProps)
	return conformance.OracleEntry{
		BlockNum: parsed.BlockNum,
		DigestB:  hex.EncodeToString(d[:]),
		DiagC:    conformance.DigestC(loaded.StateDB, loaded.DiskDB, loaded.Closure, loaded.DynProps),
	}, nil
}

// readGenesisTime tries to read block 0's timestamp. Lite nodes usually have
// pruned this; caller must fall back.
func readGenesisTime(ctx context.Context, cli *httpClient) (int64, error) {
	blk, err := cli.getBlockByNum(ctx, 0)
	if err != nil {
		return 0, err
	}
	if blk.BlockHeader == nil || blk.BlockHeader.RawData == nil {
		return 0, errors.New("block 0 empty")
	}
	return blk.BlockHeader.RawData.Timestamp, nil
}

// writeUvarintFrame writes uvarint(len) || data.
func writeUvarintFrame(w *bufio.Writer, data []byte) error {
	var lenBuf [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(lenBuf[:], uint64(len(data)))
	if _, err := w.Write(lenBuf[:n]); err != nil {
		return err
	}
	_, err := w.Write(data)
	return err
}

func writeJSONFile(path string, v any) error {
	data, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, append(data, '\n'), 0o644)
}

func b64(b []byte) string { return base64.StdEncoding.EncodeToString(b) }
