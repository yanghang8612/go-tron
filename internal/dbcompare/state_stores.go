package dbcompare

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"

	"github.com/ethereum/go-ethereum/ethdb"
	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/state"
	"github.com/tronprotocol/go-tron/core/state/kvdomains"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	contractpb "github.com/tronprotocol/go-tron/proto/core/contract"
	"google.golang.org/protobuf/proto"
)

type byteLookup func(key []byte) (value []byte, present bool, err error)
type protoLookup func(key []byte, java proto.Message) (gtron proto.Message, present bool, err error)

type byteComparisonJob struct {
	slot  int
	key   []byte
	value []byte
}

type byteComparisonResult struct {
	slot    int
	key     []byte
	want    []byte
	got     []byte
	present bool
	err     error
}

func (c *comparer) compareByteStore(name, scope string, java ethdb.KeyValueStore, lookup byteLookup) error {
	r := StoreResult{Name: name, Scope: scope, Present: java != nil}
	defer c.trackStore(&r)()
	if java == nil {
		return nil
	}
	progress := c.newProgressCounter(&r, "comparing java rows")
	it := java.NewIterator(nil, nil)
	defer it.Release()
	for it.Next() {
		progress.Add(1)
		key := append([]byte(nil), it.Key()...)
		got, ok, err := lookup(key)
		if err != nil {
			r.Invalid++
			c.addDiff(name, printableKey(key), "invalid_java_key", err.Error())
			continue
		}
		if !ok {
			r.MissingGtron++
			c.addDiff(name, printableKey(key), "missing_gtron", "corresponding gtron state row not found")
			continue
		}
		r.Compared++
		if bytes.Equal(it.Value(), got) {
			r.Equal++
		} else {
			r.Different++
			c.addByteDiff(name, printableKey(key), it.Value(), got)
		}
	}
	return it.Error()
}

// compareByteStoreParallel retains Java iterator order while running the
// independent gtron point lookups concurrently. This is intended for very
// large byte stores such as java-tron's delegation/reward history, where tens
// of millions of serial Pebble Gets otherwise dominate the audit.
func (c *comparer) compareByteStoreParallel(name, scope string, java ethdb.KeyValueStore, lookup byteLookup) error {
	r := StoreResult{Name: name, Scope: scope, Present: java != nil}
	defer c.trackStore(&r)()
	if java == nil {
		return nil
	}

	workers := c.workerCount()
	batchSize := workers * 256
	stage := fmt.Sprintf("comparing java rows (workers=%d)", workers)
	c.emitProgress(ProgressEvent{Phase: "info", Store: name, Detail: fmt.Sprintf("byte-store parallel workers=%d batch_size=%d", workers, batchSize)})
	progress := c.newProgressCounter(&r, stage)

	jobs := make(chan byteComparisonJob, batchSize)
	results := make(chan byteComparisonResult, batchSize)
	var workersWG sync.WaitGroup
	workersWG.Add(workers)
	for range workers {
		go func() {
			defer workersWG.Done()
			for job := range jobs {
				got, present, err := lookup(job.key)
				results <- byteComparisonResult{
					slot: job.slot, key: job.key, want: job.value,
					got: got, present: present, err: err,
				}
			}
		}()
	}

	processBatch := func(batch []byteComparisonJob) {
		for _, job := range batch {
			jobs <- job
		}
		ordered := make([]byteComparisonResult, len(batch))
		for range batch {
			result := <-results
			ordered[result.slot] = result
		}
		for _, result := range ordered {
			progress.Add(1)
			key := printableKey(result.key)
			if result.err != nil {
				r.Invalid++
				c.addDiff(name, key, "invalid_java_key", result.err.Error())
				continue
			}
			if !result.present {
				r.MissingGtron++
				c.addDiff(name, key, "missing_gtron", "corresponding gtron state row not found")
				continue
			}
			r.Compared++
			if bytes.Equal(result.want, result.got) {
				r.Equal++
			} else {
				r.Different++
				c.addByteDiff(name, key, result.want, result.got)
			}
		}
	}

	it := java.NewIterator(nil, nil)
	defer it.Release()
	batch := make([]byteComparisonJob, 0, batchSize)
	for it.Next() {
		batch = append(batch, byteComparisonJob{
			slot: len(batch), key: append([]byte(nil), it.Key()...), value: append([]byte(nil), it.Value()...),
		})
		if len(batch) == batchSize {
			processBatch(batch)
			batch = batch[:0]
		}
	}
	processBatch(batch)
	close(jobs)
	workersWG.Wait()
	return it.Error()
}

func (c *comparer) compareProtoStore(name, scope string, java ethdb.KeyValueStore, newMessage func() proto.Message, lookup protoLookup) error {
	r := StoreResult{Name: name, Scope: scope, Present: java != nil}
	defer c.trackStore(&r)()
	if java == nil {
		return nil
	}
	progress := c.newProgressCounter(&r, "comparing java rows")
	it := java.NewIterator(nil, nil)
	defer it.Release()
	for it.Next() {
		progress.Add(1)
		key := append([]byte(nil), it.Key()...)
		want := newMessage()
		if err := proto.Unmarshal(it.Value(), want); err != nil {
			r.Invalid++
			c.addDiff(name, printableKey(key), "invalid_java", err.Error())
			continue
		}
		got, ok, err := lookup(key, want)
		if err != nil {
			r.Invalid++
			c.addDiff(name, printableKey(key), "invalid_java_key", err.Error())
			continue
		}
		if !ok || got == nil {
			r.MissingGtron++
			c.addDiff(name, printableKey(key), "missing_gtron", "corresponding gtron state row not found")
			continue
		}
		r.Compared++
		if proto.Equal(want, got) {
			r.Equal++
		} else {
			r.Different++
			c.addProtoDiff(name, printableKey(key), want, got)
		}
	}
	return it.Error()
}

func (c *comparer) compareAdditionalStateStores(gtron ethdb.KeyValueStore, sdb *state.StateDB, java *JavaStores) error {
	steps := []func() error{
		func() error { return c.compareAccountIndex(sdb, java.Store("account-index"), false) },
		func() error { return c.compareAccountIndex(sdb, java.Store("accountid-index"), true) },
		func() error { return c.compareAccountAssets(sdb, java.Store("account-asset")) },
		func() error { return c.compareAssetIssues(sdb, java.Store("asset-issue"), false) },
		func() error { return c.compareAssetIssues(sdb, java.Store("asset-issue-v2"), true) },
		func() error { return c.compareContractStates(sdb, java.Store("contract-state")) },
		func() error { return c.compareDelegatedResources(sdb, java.Store("DelegatedResource")) },
		func() error {
			return c.compareDelegatedResourceIndexes(sdb, java.Store("DelegatedResourceAccountIndex"))
		},
		func() error { return c.compareDelegation(sdb, java.Store("delegation")) },
		func() error { return c.compareExchanges(sdb, java.Store("exchange"), false) },
		func() error { return c.compareExchanges(sdb, java.Store("exchange-v2"), true) },
		func() error { return c.compareMarket(sdb, java) },
		func() error { return c.compareNullifiers(sdb, java.Store("nullifier")) },
		func() error { return c.compareMerkleTrees(sdb, java.Store("IncrementalMerkleTree")) },
		func() error { return c.compareProposals(sdb, java.Store("proposal")) },
		func() error { return c.compareRecentBlocks(gtron, java.Store("recent-block")) },
		func() error { return c.compareRewardVI(sdb, java.Store("reward-vi")) },
		func() error { return c.compareStorageRows(gtron, java.Store("storage-row")) },
		func() error { return c.compareTreeBlockIndex(sdb, java.Store("tree-block-index")) },
		func() error { return c.compareVotes(sdb, java.Store("votes")) },
		func() error { return c.compareWitnessSchedule(gtron, sdb, java.Store("witness_schedule")) },
		func() error { return c.compareZKProofs(sdb, java.Store("zkProof")) },
	}
	for _, step := range steps {
		if err := step(); err != nil {
			return err
		}
	}
	return nil
}

func (c *comparer) compareAccountIndex(sdb *state.StateDB, java ethdb.KeyValueStore, id bool) error {
	name := "account-index"
	if id {
		name = "accountid-index"
	}
	return c.compareByteStore(name, "state", java, func(key []byte) ([]byte, bool, error) {
		var value []byte
		if id {
			value = sdb.ReadAccountIdIndex(key)
		} else {
			value = sdb.ReadAccountNameIndex(key)
		}
		return value, len(value) != 0, nil
	})
}

func (c *comparer) compareAccountAssets(sdb *state.StateDB, java ethdb.KeyValueStore) error {
	return c.compareByteStore("account-asset", "state", java, func(key []byte) ([]byte, bool, error) {
		if len(key) <= tcommon.AddressLength {
			return nil, false, fmt.Errorf("key is %d bytes; want address(21)+token-id", len(key))
		}
		owner := tcommon.BytesToAddress(key[:tcommon.AddressLength])
		account := sdb.GetAccount(owner)
		if account == nil {
			return nil, false, nil
		}
		token := string(key[tcommon.AddressLength:])
		balance, ok := account.Proto().AssetV2[token]
		if !ok {
			return nil, false, nil
		}
		var out [8]byte
		binary.BigEndian.PutUint64(out[:], uint64(balance))
		return out[:], true, nil
	})
}

func (c *comparer) compareAssetIssues(sdb *state.StateDB, java ethdb.KeyValueStore, v2 bool) error {
	name := "asset-issue"
	if v2 {
		name = "asset-issue-v2"
	}
	return c.compareProtoStore(name, "state", java,
		func() proto.Message { return new(contractpb.AssetIssueContract) },
		func(key []byte, _ proto.Message) (proto.Message, bool, error) {
			if !v2 {
				got := sdb.ReadAssetIssueByName(key)
				return got, got != nil, nil
			}
			id, err := strconv.ParseInt(string(key), 10, 64)
			if err != nil {
				return nil, false, fmt.Errorf("token id %q is not decimal: %w", key, err)
			}
			got := sdb.ReadAssetIssue(id)
			return got, got != nil, nil
		})
}

func (c *comparer) compareContractStates(sdb *state.StateDB, java ethdb.KeyValueStore) error {
	return c.compareProtoStore("contract-state", "state", java,
		func() proto.Message { return new(contractpb.ContractState) },
		func(key []byte, _ proto.Message) (proto.Message, bool, error) {
			if len(key) != tcommon.AddressLength {
				return nil, false, fmt.Errorf("contract address is %d bytes, want 21", len(key))
			}
			got := sdb.ReadContractState(tcommon.BytesToAddress(key))
			if got == nil {
				return nil, false, nil
			}
			return got.Proto(), true, nil
		})
}

func (c *comparer) compareDelegatedResources(sdb *state.StateDB, java ethdb.KeyValueStore) error {
	return c.compareProtoStore("DelegatedResource", "state", java,
		func() proto.Message { return new(corepb.DelegatedResource) },
		func(key []byte, _ proto.Message) (proto.Message, bool, error) {
			var fromBytes, toBytes []byte
			var locked, v2 bool
			switch len(key) {
			case 2 * tcommon.AddressLength:
				fromBytes, toBytes = key[:tcommon.AddressLength], key[tcommon.AddressLength:]
			case 1 + 2*tcommon.AddressLength:
				v2 = true
				locked = key[0] == 0x02
				if key[0] != 0x01 && key[0] != 0x02 {
					return nil, false, fmt.Errorf("unknown V2 key prefix %#x", key[0])
				}
				fromBytes, toBytes = key[1:1+tcommon.AddressLength], key[1+tcommon.AddressLength:]
			default:
				return nil, false, fmt.Errorf("key is %d bytes, want 42 or 43", len(key))
			}
			from, to := tcommon.BytesToAddress(fromBytes), tcommon.BytesToAddress(toBytes)
			var row *rawdb.DelegatedResource
			if v2 {
				row = sdb.ReadDelegatedResourceV2(from, to, locked)
			} else {
				row = sdb.ReadDelegatedResourceLegacy(from, to)
			}
			if row == nil {
				return nil, false, nil
			}
			return &corepb.DelegatedResource{
				From: row.From.Bytes(), To: row.To.Bytes(),
				FrozenBalanceForBandwidth: row.FrozenBalanceForBandwidth,
				FrozenBalanceForEnergy:    row.FrozenBalanceForEnergy,
				ExpireTimeForBandwidth:    row.ExpireTimeForBandwidth,
				ExpireTimeForEnergy:       row.ExpireTimeForEnergy,
			}, true, nil
		})
}

func (c *comparer) compareDelegatedResourceIndexes(sdb *state.StateDB, java ethdb.KeyValueStore) error {
	return c.compareProtoStore("DelegatedResourceAccountIndex", "state", java,
		func() proto.Message { return new(corepb.DelegatedResourceAccountIndex) },
		func(key []byte, _ proto.Message) (proto.Message, bool, error) {
			if len(key) == tcommon.AddressLength {
				got := sdb.ReadDrAccountIndexLegacy(key)
				return got, got != nil, nil
			}
			if len(key) != 1+2*tcommon.AddressLength || key[0] < 1 || key[0] > 4 {
				return nil, false, fmt.Errorf("key is not legacy address or direction+anchor+counterparty")
			}
			got := sdb.ReadDrAccountIndexEntry(rawdb.DrAccIdxDirection(key[0]), key[1:1+tcommon.AddressLength], key[1+tcommon.AddressLength:])
			return got, got != nil, nil
		})
}

func (c *comparer) compareDelegation(sdb *state.StateDB, java ethdb.KeyValueStore) error {
	// Hydrate the shared system account before workers begin. Subsequent
	// GetAccountKV calls only read the cached envelope and immutable latest
	// state, avoiding concurrent cache population inside StateDB.
	_ = sdb.GetAccount(tcommon.SystemAccountAddress)
	return c.compareByteStoreParallel("delegation", "state", java, func(key []byte) ([]byte, bool, error) {
		logical, err := delegationLogicalKey(key)
		if err != nil {
			return nil, false, err
		}
		return sdb.GetAccountKV(tcommon.SystemAccountAddress, kvdomains.SystemReward, logical)
	})
}

func delegationLogicalKey(key []byte) ([]byte, error) {
	if len(key) == tcommon.AddressLength && key[0] == tcommon.AddressPrefixMainnet {
		return rawdb.BeginCycleStateKey(key), nil
	}
	text := string(key)
	if strings.HasPrefix(text, "end-") {
		addr, err := hex.DecodeString(strings.TrimPrefix(text, "end-"))
		if err != nil || len(addr) != tcommon.AddressLength {
			return nil, fmt.Errorf("invalid end-cycle address in %q", text)
		}
		return rawdb.EndCycleStateKey(addr), nil
	}
	parts := strings.Split(text, "-")
	if len(parts) < 3 {
		return nil, fmt.Errorf("unknown delegation key %q", text)
	}
	cycle, err := strconv.ParseInt(parts[0], 10, 64)
	if err != nil {
		return nil, fmt.Errorf("invalid delegation cycle: %w", err)
	}
	addr, err := hex.DecodeString(parts[1])
	if err != nil || len(addr) != tcommon.AddressLength {
		return nil, fmt.Errorf("invalid delegation address %q", parts[1])
	}
	suffix := strings.Join(parts[2:], "-")
	switch suffix {
	case "reward":
		return rawdb.CycleRewardStateKey(cycle, addr), nil
	case "vote":
		return rawdb.CycleVoteStateKey(cycle, addr), nil
	case "account-vote":
		return rawdb.CycleAccountVoteStateKey(cycle, addr), nil
	case "brokerage":
		return rawdb.CycleBrokerageStateKey(cycle, addr), nil
	case "vi":
		return rawdb.WitnessVIStateKey(cycle, addr), nil
	default:
		return nil, fmt.Errorf("unknown delegation suffix %q", suffix)
	}
}

func (c *comparer) compareExchanges(sdb *state.StateDB, java ethdb.KeyValueStore, v2 bool) error {
	name := "exchange"
	if v2 {
		name = "exchange-v2"
	}
	return c.compareProtoStore(name, "state", java,
		func() proto.Message { return new(corepb.Exchange) },
		func(key []byte, _ proto.Message) (proto.Message, bool, error) {
			if len(key) != 8 {
				return nil, false, fmt.Errorf("exchange id key is %d bytes, want 8", len(key))
			}
			id := int64(binary.BigEndian.Uint64(key))
			var got *corepb.Exchange
			if v2 {
				got = sdb.ReadExchangeV2(id)
			} else {
				got = sdb.ReadExchange(id)
			}
			return got, got != nil, nil
		})
}

func (c *comparer) compareMarket(sdb *state.StateDB, java *JavaStores) error {
	if err := c.compareProtoStore("market_order", "state", java.Store("market_order"),
		func() proto.Message { return new(corepb.MarketOrder) },
		func(key []byte, _ proto.Message) (proto.Message, bool, error) {
			got := sdb.ReadMarketOrder(key)
			return got, got != nil, nil
		}); err != nil {
		return err
	}
	if err := c.compareProtoStore("market_account", "state", java.Store("market_account"),
		func() proto.Message { return new(corepb.MarketAccountOrder) },
		func(key []byte, _ proto.Message) (proto.Message, bool, error) {
			got := sdb.ReadMarketAccountOrder(key)
			return got, got != nil, nil
		}); err != nil {
		return err
	}
	if err := c.compareByteStore("market_pair_to_price", "state", java.Store("market_pair_to_price"), func(key []byte) ([]byte, bool, error) {
		sell, buy, _, err := decodeJavaMarketKey(key, false)
		if err != nil {
			return nil, false, err
		}
		count := sdb.ReadMarketPairPriceCount(sell, buy)
		var out [8]byte
		binary.BigEndian.PutUint64(out[:], uint64(count))
		return out[:], true, nil
	}); err != nil {
		return err
	}
	return c.compareProtoStore("market_pair_price_to_order", "state", java.Store("market_pair_price_to_order"),
		func() proto.Message { return new(corepb.MarketOrderIdList) },
		func(key []byte, _ proto.Message) (proto.Message, bool, error) {
			sell, buy, price, err := decodeJavaMarketKey(key, true)
			if err != nil {
				return nil, false, err
			}
			got := sdb.ReadMarketOrderBook(sell, buy, price)
			return got, got != nil, nil
		})
}

func decodeJavaMarketKey(key []byte, withPrice bool) ([]byte, []byte, [16]byte, error) {
	const tokenLen = 19
	want := 2 * tokenLen
	if withPrice {
		want += 16
	}
	if len(key) != want {
		return nil, nil, [16]byte{}, fmt.Errorf("market key is %d bytes, want %d", len(key), want)
	}
	trim := func(v []byte) []byte { return bytes.TrimRight(v, "\x00") }
	sell := append([]byte(nil), trim(key[:tokenLen])...)
	buy := append([]byte(nil), trim(key[tokenLen:2*tokenLen])...)
	var price [16]byte
	if withPrice {
		copy(price[:], key[2*tokenLen:])
	}
	return sell, buy, price, nil
}

func (c *comparer) compareNullifiers(sdb *state.StateDB, java ethdb.KeyValueStore) error {
	r := StoreResult{Name: "nullifier", Scope: "state", Present: java != nil}
	defer c.trackStore(&r)()
	if java == nil {
		return nil
	}
	progress := c.newProgressCounter(&r, "comparing java nullifiers")
	it := java.NewIterator(nil, nil)
	defer it.Release()
	for it.Next() {
		progress.Add(1)
		if !sdb.HasNullifier(it.Key()) {
			r.MissingGtron++
			c.addDiff("nullifier", printableKey(it.Key()), "missing_gtron", "spent nullifier not found")
			continue
		}
		r.Compared++
		r.Equal++ // Java stores the nullifier as its value; gtron stores a marker.
	}
	return it.Error()
}

func (c *comparer) compareMerkleTrees(sdb *state.StateDB, java ethdb.KeyValueStore) error {
	return c.compareProtoStore("IncrementalMerkleTree", "state", java,
		func() proto.Message { return new(contractpb.IncrementalMerkleTree) },
		func(key []byte, _ proto.Message) (proto.Message, bool, error) {
			var got *contractpb.IncrementalMerkleTree
			switch string(key) {
			case "LAST_TREE":
				got = sdb.ReadLastMerkleTree()
			case "CURRENT_TREE":
				got = sdb.ReadCurrentMerkleTree()
			default:
				got = sdb.ReadIncrMerkleTree(key)
			}
			return got, got != nil, nil
		})
}

func (c *comparer) compareProposals(sdb *state.StateDB, java ethdb.KeyValueStore) error {
	return c.compareProtoStore("proposal", "state", java,
		func() proto.Message { return new(corepb.Proposal) },
		func(key []byte, _ proto.Message) (proto.Message, bool, error) {
			if len(key) != 8 {
				return nil, false, fmt.Errorf("proposal id key is %d bytes, want 8", len(key))
			}
			row := sdb.ReadProposal(int64(binary.BigEndian.Uint64(key)))
			if row == nil {
				return nil, false, nil
			}
			stateValue := corepb.Proposal_PENDING
			switch row.State {
			case rawdb.ProposalStateApproved:
				stateValue = corepb.Proposal_APPROVED
			case rawdb.ProposalStateCanceled:
				stateValue = corepb.Proposal_CANCELED
			}
			approvals := make([][]byte, len(row.Approvals))
			for i := range row.Approvals {
				approvals[i] = row.Approvals[i].Bytes()
			}
			return &corepb.Proposal{
				ProposalId: row.ID, ProposerAddress: row.Proposer.Bytes(), Parameters: row.Parameters,
				ExpirationTime: row.ExpirationTime, CreateTime: row.CreateTime,
				Approvals: approvals, State: stateValue,
			}, true, nil
		})
}

func (c *comparer) compareRecentBlocks(gtron ethdb.KeyValueStore, java ethdb.KeyValueStore) error {
	return c.compareByteStore("recent-block", "state-index", java, func(key []byte) ([]byte, bool, error) {
		got := rawdb.ReadTaposRef(gtron, key)
		return got, len(got) != 0, nil
	})
}

func (c *comparer) compareRewardVI(sdb *state.StateDB, java ethdb.KeyValueStore) error {
	return c.compareByteStore("reward-vi", "state-cache", java, func(key []byte) ([]byte, bool, error) {
		if len(key) == 1 && key[0] == 0 {
			return sdb.GetAccountKV(tcommon.SystemAccountAddress, kvdomains.SystemReward, rawdb.RewardViIsDoneStateKey())
		}
		parts := strings.Split(string(key), "-")
		if len(parts) != 3 || parts[2] != "vi" {
			return nil, false, fmt.Errorf("invalid reward-vi key %q", key)
		}
		cycle, err := strconv.ParseInt(parts[0], 10, 64)
		if err != nil {
			return nil, false, err
		}
		addr, err := hex.DecodeString(parts[1])
		if err != nil || len(addr) != tcommon.AddressLength {
			return nil, false, fmt.Errorf("invalid reward-vi address %q", parts[1])
		}
		value, ok, err := sdb.GetAccountKV(tcommon.SystemAccountAddress, kvdomains.SystemReward, rawdb.RewardViStateKey(cycle, addr))
		if err != nil || !ok {
			return value, ok, err
		}
		// Java BigInteger.toByteArray adds a leading sign byte when needed;
		// normalize positive values before byte comparison.
		if len(value) > 0 && value[0]&0x80 != 0 {
			value = append([]byte{0}, value...)
		}
		return value, true, nil
	})
}

func (c *comparer) compareStorageRows(gtron ethdb.KeyValueStore, java ethdb.KeyValueStore) error {
	r := StoreResult{Name: "storage-row", Scope: "state", Present: java != nil}
	defer c.trackStore(&r)()
	if java == nil {
		return nil
	}
	tmpRoot, err := os.MkdirTemp("", "gtron-db-compare-storage-")
	if err != nil {
		return fmt.Errorf("create storage-row comparison index: %w", err)
	}
	defer os.RemoveAll(tmpRoot)
	index, err := rawdb.NewPebbleDB(filepath.Join(tmpRoot, "rows"), 64, 64)
	if err != nil {
		return fmt.Errorf("open storage-row comparison index: %w", err)
	}
	defer index.Close()

	batch := index.NewBatch()
	var lastOwner tcommon.Address
	var lastGeneration uint64
	haveOwner := false
	indexProgress := c.newProgressCounter(&r, "building gtron storage index")
	err = rawdb.IterateStateKVLatestDomainRows(gtron, kvdomains.ContractStorage, func(row rawdb.StateKVLatestRow) (bool, error) {
		indexProgress.Add(1)
		if !haveOwner || row.Owner != lastOwner {
			lastOwner, haveOwner = row.Owner, true
			generation, ok, err := rawdb.ReadStateKVGeneration(gtron, row.Owner)
			if err != nil {
				return false, err
			}
			if ok {
				lastGeneration = generation
			} else {
				lastGeneration = 0
			}
		}
		if row.Generation != lastGeneration {
			return true, nil
		}
		if err := batch.Put(row.Key, row.Value); err != nil {
			return false, err
		}
		if batch.ValueSize() >= 16<<20 {
			if err := batch.Write(); err != nil {
				return false, err
			}
			batch.Reset()
		}
		return true, nil
	})
	if err == nil && batch.ValueSize() > 0 {
		err = batch.Write()
	}
	batch.Close()
	if err != nil {
		return fmt.Errorf("build gtron storage-row index: %w", err)
	}

	deletes := index.NewBatch()
	javaProgress := c.newProgressCounter(&r, "comparing java storage rows")
	it := java.NewIterator(nil, nil)
	for it.Next() {
		javaProgress.Add(1)
		key := append([]byte(nil), it.Key()...)
		got, getErr := index.Get(key)
		if getErr != nil {
			r.MissingGtron++
			c.addDiff("storage-row", printableKey(key), "missing_gtron", "row key not found in current ContractStorage generation")
			continue
		}
		r.Compared++
		if bytes.Equal(it.Value(), got) {
			r.Equal++
		} else {
			r.Different++
			c.addByteDiff("storage-row", printableKey(key), it.Value(), got)
		}
		if err := deletes.Delete(key); err != nil {
			it.Release()
			return err
		}
		if deletes.ValueSize() >= 16<<20 {
			if err := deletes.Write(); err != nil {
				it.Release()
				return err
			}
			deletes.Reset()
		}
	}
	iterErr := it.Error()
	it.Release()
	if iterErr != nil {
		return iterErr
	}
	if deletes.ValueSize() > 0 {
		if err := deletes.Write(); err != nil {
			return err
		}
	}
	deletes.Close()
	reverseProgress := c.newProgressCounter(&r, "checking gtron-only storage rows")
	remaining := index.NewIterator(nil, nil)
	defer remaining.Release()
	for remaining.Next() {
		reverseProgress.Add(1)
		r.MissingJava++
		c.addDiff("storage-row", printableKey(remaining.Key()), "missing_java", "gtron current ContractStorage row has no java storage-row entry")
	}
	return remaining.Error()
}

func (c *comparer) compareTreeBlockIndex(sdb *state.StateDB, java ethdb.KeyValueStore) error {
	return c.compareByteStore("tree-block-index", "state-index", java, func(key []byte) ([]byte, bool, error) {
		if len(key) != 8 {
			return nil, false, fmt.Errorf("block number key is %d bytes, want 8", len(key))
		}
		got := sdb.ReadMerkleTreeRootByBlock(int64(binary.BigEndian.Uint64(key)))
		return got, len(got) != 0, nil
	})
}

func (c *comparer) compareVotes(sdb *state.StateDB, java ethdb.KeyValueStore) error {
	return c.compareProtoStore("votes", "state", java,
		func() proto.Message { return new(corepb.Votes) },
		func(key []byte, _ proto.Message) (proto.Message, bool, error) {
			if len(key) != tcommon.AddressLength {
				return nil, false, fmt.Errorf("voter address is %d bytes, want 21", len(key))
			}
			got := sdb.ReadVotes(tcommon.BytesToAddress(key))
			return got, got != nil, nil
		})
}

func (c *comparer) compareWitnessSchedule(gtron ethdb.KeyValueStore, sdb *state.StateDB, java ethdb.KeyValueStore) error {
	return c.compareByteStore("witness_schedule", "state", java, func(key []byte) ([]byte, bool, error) {
		var addresses []tcommon.Address
		switch string(key) {
		case "active_witnesses":
			addresses = sdb.ReadActiveWitnesses()
		case "current_shuffled_witnesses":
			addresses = rawdb.ReadShuffledWitnesses(gtron)
		default:
			return nil, false, fmt.Errorf("unknown witness schedule key %q", key)
		}
		if len(addresses) == 0 {
			return nil, false, nil
		}
		out := make([]byte, 0, len(addresses)*tcommon.AddressLength)
		for _, addr := range addresses {
			out = append(out, addr.Bytes()...)
		}
		return out, true, nil
	})
}

func (c *comparer) compareZKProofs(sdb *state.StateDB, java ethdb.KeyValueStore) error {
	return c.compareByteStore("zkProof", "state-cache", java, func(key []byte) ([]byte, bool, error) {
		value, ok := sdb.ReadZKProofResult(key)
		if !ok {
			return nil, false, nil
		}
		if value {
			return []byte{1}, true, nil
		}
		return []byte{0}, true, nil
	})
}
