package core

import (
	"crypto/ecdsa"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	ethrawdb "github.com/ethereum/go-ethereum/core/rawdb"
	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/consensus/dpos"
	"github.com/tronprotocol/go-tron/core/state"
	"github.com/tronprotocol/go-tron/core/txpool"
	"github.com/tronprotocol/go-tron/core/types"
	"github.com/tronprotocol/go-tron/crypto"
	"github.com/tronprotocol/go-tron/params"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	contractpb "github.com/tronprotocol/go-tron/proto/core/contract"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
)

// BenchmarkPrewarmBlockSignatures measures the parallel signature pre-pass on a
// batch of signed transfer txs (the workload the provided InsertBlocks bench
// can't show, since its txs are unsigned). It rebuilds fresh, cache-cold blocks
// each iteration so every run does real ECDSA work.
//
//	go test ./core -run '^$' -bench BenchmarkPrewarmBlockSignatures -benchmem
func BenchmarkPrewarmBlockSignatures(b *testing.B) {
	b.Run("parallel", func(b *testing.B) { benchPrewarm(b, defaultParallelSigVerifyMinTxs) })
	b.Run("serial_killswitch", func(b *testing.B) { benchPrewarm(b, 0) })
}

func benchPrewarm(b *testing.B, minTxs int) {
	prev := ParallelSigVerifyMinTxs
	ParallelSigVerifyMinTxs = minTxs
	defer func() { ParallelSigVerifyMinTxs = prev }()

	// Build raw signed-tx blocks once; clone wrappers per-iteration so the
	// signers memo is cold each time (mirrors fresh-from-wire sync blocks).
	const nBlocks, txPerBlock = 16, 64
	key, err := crypto.GenerateKey()
	if err != nil {
		b.Fatal(err)
	}
	from := crypto.PubkeyToAddress(&key.PublicKey)
	var to tcommon.Address
	to[0] = 0x41
	to[20] = 0x99
	type rawTx struct {
		raw, sig []byte
	}
	blocksRaw := make([][]rawTx, nBlocks)
	for blk := 0; blk < nBlocks; blk++ {
		txs := make([]rawTx, txPerBlock)
		for j := 0; j < txPerBlock; j++ {
			tc := &contractpb.TransferContract{OwnerAddress: from.Bytes(), ToAddress: to.Bytes(), Amount: int64(blk*txPerBlock + j + 1)}
			param, _ := anypb.New(tc)
			rawPB := &corepb.TransactionRaw{
				Expiration: 60_000,
				Contract:   []*corepb.Transaction_Contract{{Type: corepb.Transaction_Contract_TransferContract, Parameter: param}},
			}
			tx := types.NewTransactionFromPB(&corepb.Transaction{RawData: rawPB})
			h := tx.Hash()
			sig, err := crypto.Sign(h[:], key)
			if err != nil {
				b.Fatal(err)
			}
			rawData, _ := proto.Marshal(rawPB)
			txs[j] = rawTx{raw: rawData, sig: sig}
		}
		blocksRaw[blk] = txs
	}
	mkBlocks := func() []*types.Block {
		out := make([]*types.Block, nBlocks)
		for blk := 0; blk < nBlocks; blk++ {
			pbTxs := make([]*corepb.Transaction, txPerBlock)
			for j, rt := range blocksRaw[blk] {
				rawPB := &corepb.TransactionRaw{}
				_ = proto.Unmarshal(rt.raw, rawPB)
				pbTxs[j] = &corepb.Transaction{RawData: rawPB, Signature: [][]byte{rt.sig}}
			}
			out[blk] = types.NewBlockFromPB(&corepb.Block{
				BlockHeader:  &corepb.BlockHeader{RawData: &corepb.BlockHeaderRaw{Number: int64(blk + 1)}},
				Transactions: pbTxs,
			})
		}
		return out
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		blocks := mkBlocks()
		b.StartTimer()
		prewarmBlockSignatures(blocks, nil)
		// Force the serial-equivalent recovery when the kill switch is on so
		// both arms do the same total ECDSA work (apples-to-apples).
		if minTxs == 0 {
			for _, blk := range blocks {
				for _, tx := range blk.Transactions() {
					_, _ = tx.RecoverSigners()
				}
			}
		}
	}
}

// fixedVerifyGenesis returns a genesis with a single SR (witnessKey) plus the
// supplied funded accounts, deferred maintenance. Both the producer chain and
// the fresh verifier chain are built from the SAME genesis so signed blocks
// validate identically across them.
func fixedVerifyGenesis(witnessAddr tcommon.Address, funded ...tcommon.Address) *params.Genesis {
	accounts := []params.GenesisAccount{{Address: witnessAddr, Balance: 1_000_000_000_000}}
	for _, a := range funded {
		accounts = append(accounts, params.GenesisAccount{Address: a, Balance: 1_000_000_000_000})
	}
	return &params.Genesis{
		Config:    params.MainnetChainConfig,
		Timestamp: 0,
		Accounts:  accounts,
		Witnesses: []params.GenesisWitness{{Address: witnessAddr, VoteCount: 1000, URL: "http://sr1"}},
		DynamicProperties: map[string]int64{
			"next_maintenance_time": 9_000_000_000,
		},
	}
}

// newVerifierChain stands up a fresh DPoS-wired chain from the given genesis.
func newVerifierChain(t testing.TB, genesis *params.Genesis) *BlockChain {
	t.Helper()
	diskdb := ethrawdb.NewMemoryDatabase()
	if _, _, err := SetupGenesisBlock(diskdb, genesis); err != nil {
		t.Fatal(err)
	}
	bc, err := NewBlockChain(diskdb, state.NewDatabase(diskdb), params.MainnetChainConfig)
	if err != nil {
		t.Fatal(err)
	}
	bc.SetEngine(dpos.New(bc))
	return bc
}

// produceSignedBlocks builds `n` sequential, witness-signed blocks on a private
// producer chain (each carrying the txs returned by txsFor for that height,
// already pool-added) and returns the wire bytes. Re-unmarshalling these into
// fresh *Block instances gives the verifier a cold cache, exactly like a peer
// delivering blocks during sync.
func produceSignedBlocks(t testing.TB, genesis *params.Genesis, witnessKey *ecdsa.PrivateKey, n int, txsFor func(height uint64) []*types.Transaction) [][]byte {
	t.Helper()
	bc := newVerifierChain(t, genesis)
	defer func() { _ = bc.Close() }()
	witnessAddr := bc.ActiveWitnesses()[0]
	out := make([][]byte, 0, n)
	for h := uint64(1); h <= uint64(n); h++ {
		pool := txpool.New()
		for _, tx := range txsFor(h) {
			pool.Add(tx)
		}
		res, err := BuildBlock(bc, pool, witnessAddr, int64(h)*int64(params.BlockProducedInterval))
		if err != nil {
			t.Fatalf("BuildBlock height %d: %v", h, err)
		}
		if err := SignBlock(res.Block, witnessKey); err != nil {
			t.Fatalf("SignBlock height %d: %v", h, err)
		}
		if err := bc.InsertBlock(res.Block); err != nil {
			t.Fatalf("producer InsertBlock height %d: %v", h, err)
		}
		b, err := res.Block.Marshal()
		if err != nil {
			t.Fatalf("marshal height %d: %v", h, err)
		}
		out = append(out, b)
	}
	return out
}

func unmarshalBatch(t testing.TB, raw [][]byte) []*types.Block {
	t.Helper()
	blocks := make([]*types.Block, len(raw))
	for i, b := range raw {
		blk, err := types.UnmarshalBlock(b)
		if err != nil {
			t.Fatalf("unmarshal block %d: %v", i, err)
		}
		blocks[i] = blk
	}
	return blocks
}

// withMinTxs runs fn with ParallelSigVerifyMinTxs set to v, restoring after.
func withMinTxs(v int, fn func()) {
	prev := ParallelSigVerifyMinTxs
	ParallelSigVerifyMinTxs = v
	defer func() { ParallelSigVerifyMinTxs = prev }()
	fn()
}

// BenchmarkInsertBlocksSignaturePipeline compares the old batch barrier with
// the production pipeline. Chain construction and wire decoding stay outside the
// timed region; both arms perform the same signature recovery and block/state
// validation work on fresh memos and a fresh verifier DB.
func BenchmarkInsertBlocksSignaturePipeline(b *testing.B) {
	witnessKey, witnessAddr := keyAndAddr(b)
	senderKey, sender := keyAndAddr(b)
	_, recipient := keyAndAddr(b)
	genesis := fixedVerifyGenesis(witnessAddr, sender)
	refChain := newVerifierChain(b, genesis)
	refBytes, refHash := genesisTaposRef(b, refChain)
	_ = refChain.Close()

	const blocksPerBatch, txsPerBlock = 8, 32
	raw := produceSignedBlocks(b, genesis, witnessKey, blocksPerBatch, func(height uint64) []*types.Transaction {
		txs := make([]*types.Transaction, txsPerBlock)
		for i := range txs {
			amount := int32(height*txsPerBlock + uint64(i) + 1)
			txs[i] = buildTransferTxWithRef(b, sender, recipient, amount, 0, refBytes, refHash, senderKey)
		}
		return txs
	})

	for _, tc := range []struct {
		name        string
		synchronous bool
	}{
		{name: "batch_barrier", synchronous: true},
		{name: "overlap"},
	} {
		b.Run(tc.name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				b.StopTimer()
				bc := newVerifierChain(b, genesis)
				blocks := unmarshalBatch(b, raw)
				prevMin := ParallelSigVerifyMinTxs
				ParallelSigVerifyMinTxs = 1
				b.StartTimer()
				if tc.synchronous {
					prewarmBlockSignatures(blocks, bc.headerSigPrewarmer())
					ParallelSigVerifyMinTxs = 0
				}
				err := bc.InsertBlocks(blocks)
				b.StopTimer()
				ParallelSigVerifyMinTxs = prevMin
				if closeErr := bc.Close(); err == nil {
					err = closeErr
				}
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func TestStartBlockSignaturePrewarmReturnsBeforeWorkersFinish(t *testing.T) {
	blocks := make([]*types.Block, 2)
	for i := range blocks {
		pbTxs := make([]*corepb.Transaction, 32)
		for j := range pbTxs {
			pbTxs[j] = &corepb.Transaction{
				RawData:   &corepb.TransactionRaw{Timestamp: int64(i*32 + j + 1)},
				Signature: [][]byte{make([]byte, 65)},
			}
		}
		blocks[i] = types.NewBlockFromPB(&corepb.Block{Transactions: pbTxs})
	}

	prevMin := ParallelSigVerifyMinTxs
	ParallelSigVerifyMinTxs = 1
	defer func() { ParallelSigVerifyMinTxs = prevMin }()
	prevHook := sigPrewarmJobHook
	defer func() { sigPrewarmJobHook = prevHook }()

	entered := make(chan struct{}, 1)
	release := make(chan struct{})
	var releaseOnce sync.Once
	var jobs atomic.Int64
	sigPrewarmJobHook = func() {
		jobs.Add(1)
		select {
		case entered <- struct{}{}:
		default:
		}
		<-release
	}

	returned := make(chan *signaturePrewarmRun, 1)
	go func() { returned <- startBlockSignaturePrewarm(blocks, nil) }()
	var run *signaturePrewarmRun
	defer func() {
		releaseOnce.Do(func() { close(release) })
		run.Wait()
	}()
	select {
	case run = <-returned:
	case <-time.After(2 * time.Second):
		releaseOnce.Do(func() { close(release) })
		select {
		case run = <-returned:
			run.Wait()
		case <-time.After(2 * time.Second):
		}
		t.Fatal("startBlockSignaturePrewarm waited for blocked workers")
	}
	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		releaseOnce.Do(func() { close(release) })
		t.Fatal("signature prewarm workers did not start")
	}

	waitDone := make(chan struct{})
	go func() {
		run.Wait()
		close(waitDone)
	}()
	select {
	case <-waitDone:
		releaseOnce.Do(func() { close(release) })
		t.Fatal("prewarm Wait returned before workers were released")
	case <-time.After(25 * time.Millisecond):
	}
	releaseOnce.Do(func() { close(release) })
	select {
	case <-waitDone:
	case <-time.After(2 * time.Second):
		t.Fatal("prewarm Wait did not join released workers")
	}
	if got, want := jobs.Load(), int64(64); got != want {
		t.Fatalf("prewarm jobs = %d, want %d", got, want)
	}
}

// TestPrewarm_IdenticalAccept_OnVsOff: a batch of validly-signed blocks (header
// signatures + signed transfer txs) must insert identically whether the parallel
// pre-pass is ON (low threshold) or OFF (0). Both reach the same head; ON warms
// the cache (job counter > 0), OFF runs zero pre-pass jobs.
func TestPrewarm_IdenticalAccept_OnVsOff(t *testing.T) {
	witnessKey, err := crypto.GenerateKey()
	if err != nil {
		t.Fatal(err)
	}
	witnessAddr := crypto.PubkeyToAddress(&witnessKey.PublicKey)
	senderKey, sender := keyAndAddr(t)
	_, recipient := keyAndAddr(t)
	genesis := fixedVerifyGenesis(witnessAddr, sender)

	// Each block carries one signed transfer from `sender`, referencing genesis
	// for TAPOS (the ring's only available block for these low heights).
	bcForRef := newVerifierChain(t, genesis)
	refBytes, refHash := genesisTaposRef(t, bcForRef)
	const nBlocks = 4
	txsFor := func(h uint64) []*types.Transaction {
		// Distinct amount per height keeps tx hashes (and thus sigs) unique.
		return []*types.Transaction{
			buildTransferTxWithRef(t, sender, recipient, int32(10+h), 0, refBytes, refHash, senderKey),
		}
	}
	raw := produceSignedBlocks(t, genesis, witnessKey, nBlocks, txsFor)

	run := func(minTxs int) (uint64, int64) {
		var head uint64
		var jobs atomic.Int64
		prevHook := sigPrewarmJobHook
		sigPrewarmJobHook = func() { jobs.Add(1) }
		defer func() { sigPrewarmJobHook = prevHook }()
		withMinTxs(minTxs, func() {
			bc := newVerifierChain(t, genesis)
			if err := bc.InsertBlocks(unmarshalBatch(t, raw)); err != nil {
				t.Fatalf("InsertBlocks (minTxs=%d): %v", minTxs, err)
			}
			head = bc.CurrentBlock().Number()
		})
		return head, jobs.Load()
	}

	headOn, jobsOn := run(1)   // pre-pass ON
	headOff, jobsOff := run(0) // pre-pass OFF (kill switch)

	if headOn != nBlocks || headOff != nBlocks {
		t.Fatalf("head mismatch: on=%d off=%d, want %d", headOn, headOff, nBlocks)
	}
	// ON must have warmed: nBlocks header jobs + nBlocks tx jobs = 8.
	if jobsOn == 0 {
		t.Fatalf("pre-pass ON ran 0 recovery jobs; cache was not warmed")
	}
	if want := int64(2 * nBlocks); jobsOn != want {
		t.Fatalf("pre-pass ON ran %d jobs, want %d (header+tx per block)", jobsOn, want)
	}
	// OFF must be a true kill switch: zero pre-pass jobs.
	if jobsOff != 0 {
		t.Fatalf("pre-pass OFF ran %d jobs, want 0 (kill switch)", jobsOff)
	}
}

// TestPrewarm_IdenticalReject_BadTxSig: a batch whose 3rd block contains a tx
// signed by the wrong key must be rejected at that exact block — identically
// with the pre-pass ON and OFF. This is the load-bearing guarantee: the parallel
// pass warms the (failing) recovery but the serial envelope check still owns the
// reject, at the same index, with the same error.
func TestPrewarm_IdenticalReject_BadTxSig(t *testing.T) {
	witnessKey, err := crypto.GenerateKey()
	if err != nil {
		t.Fatal(err)
	}
	witnessAddr := crypto.PubkeyToAddress(&witnessKey.PublicKey)
	senderKey, sender := keyAndAddr(t)
	wrongKey, _ := keyAndAddr(t)
	_, recipient := keyAndAddr(t)
	genesis := fixedVerifyGenesis(witnessAddr, sender)

	bcForRef := newVerifierChain(t, genesis)
	refBytes, refHash := genesisTaposRef(t, bcForRef)
	const badHeight = 3
	// A tx signed by wrongKey: a valid 65-byte signature that recovers fine but
	// whose signer is NOT in sender's permission set → ErrUnauthorizedSigner.
	badTx := buildTransferTxWithRef(t, sender, recipient, int32(10+badHeight), 0, refBytes, refHash, wrongKey)
	pbBlocks := produceBatchWithBadTx(t, genesis, witnessKey, badHeight, refBytes, refHash, sender, recipient, senderKey, badTx)

	run := func(minTxs int) (uint64, error) {
		var head uint64
		var insErr error
		withMinTxs(minTxs, func() {
			bc := newVerifierChain(t, genesis)
			insErr = bc.InsertBlocks(unmarshalBatch(t, pbBlocks))
			head = bc.CurrentBlock().Number()
		})
		return head, insErr
	}

	headOn, errOn := run(1)
	headOff, errOff := run(0)

	for _, c := range []struct {
		name string
		head uint64
		err  error
	}{{"on", headOn, errOn}, {"off", headOff, errOff}} {
		if c.err == nil {
			t.Fatalf("%s: expected rejection of the bad-sig block, got nil", c.name)
		}
		var rangeErr *InsertBlocksError
		if !errors.As(c.err, &rangeErr) {
			t.Fatalf("%s: error = %v, want InsertBlocksError", c.name, c.err)
		}
		if rangeErr.Index != badHeight-1 {
			t.Fatalf("%s: failed at index %d, want %d (the bad block)", c.name, rangeErr.Index, badHeight-1)
		}
		if !errors.Is(c.err, ErrUnauthorizedSigner) {
			t.Fatalf("%s: error = %v, want ErrUnauthorizedSigner", c.name, c.err)
		}
		// Only the two good blocks before the bad one committed.
		if c.head != badHeight-1 {
			t.Fatalf("%s: head = %d, want %d (committed up to the block before the bad one)", c.name, c.head, badHeight-1)
		}
	}
}

// TestPrewarm_IdenticalReject_MalformedTxSig is the recovery-FAILURE counterpart
// to the bad-signer test: the bad block's tx carries a 65-byte signature that
// passes the length/v-header gate but fails ECDSA recovery (zero r/s). The
// pre-pass memoizes the recovery error; the serial ValidateTxEnvelope reads the
// same memo and collapses it to ErrInvalidTxSignature — identically ON and OFF.
// This is the load-bearing "bad signature must still be rejected" guarantee.
func TestPrewarm_IdenticalReject_MalformedTxSig(t *testing.T) {
	witnessKey, err := crypto.GenerateKey()
	if err != nil {
		t.Fatal(err)
	}
	witnessAddr := crypto.PubkeyToAddress(&witnessKey.PublicKey)
	senderKey, sender := keyAndAddr(t)
	_, recipient := keyAndAddr(t)
	genesis := fixedVerifyGenesis(witnessAddr, sender)

	bcForRef := newVerifierChain(t, genesis)
	refBytes, refHash := genesisTaposRef(t, bcForRef)
	const badHeight = 3
	// Build a well-formed tx, then replace its signature with a malformed but
	// right-length one (valid v=27 header, zero r/s) so SigToPub fails. The
	// envelope's len(sigs) (1) <= len(perm.Keys) (1) guard passes, so recovery
	// is reached and is the failing step.
	badTx := buildTransferTxWithRef(t, sender, recipient, int32(10+badHeight), 0, refBytes, refHash, senderKey)
	malformed := make([]byte, 65)
	malformed[64] = 27
	badTx.Proto().Signature = [][]byte{malformed}
	pbBlocks := produceBatchWithBadTx(t, genesis, witnessKey, badHeight, refBytes, refHash, sender, recipient, senderKey, badTx)

	run := func(minTxs int) (uint64, error) {
		var head uint64
		var insErr error
		withMinTxs(minTxs, func() {
			bc := newVerifierChain(t, genesis)
			insErr = bc.InsertBlocks(unmarshalBatch(t, pbBlocks))
			head = bc.CurrentBlock().Number()
		})
		return head, insErr
	}

	headOn, errOn := run(1)
	headOff, errOff := run(0)
	for _, c := range []struct {
		name string
		head uint64
		err  error
	}{{"on", headOn, errOn}, {"off", headOff, errOff}} {
		if !errors.Is(c.err, ErrInvalidTxSignature) {
			t.Fatalf("%s: error = %v, want ErrInvalidTxSignature", c.name, c.err)
		}
		var rangeErr *InsertBlocksError
		if errors.As(c.err, &rangeErr) && rangeErr.Index != badHeight-1 {
			t.Fatalf("%s: failed at index %d, want %d", c.name, rangeErr.Index, badHeight-1)
		}
		if c.head != badHeight-1 {
			t.Fatalf("%s: head = %d, want %d", c.name, c.head, badHeight-1)
		}
	}
}

// TestPrewarm_IdenticalReject_BadHeaderSig exercises the header-signature memo
// path: a batch whose 3rd block's HEADER is signed by the wrong key (so the
// recovered witness ≠ block.WitnessAddress) must be rejected at that block with
// dpos.ErrInvalidSignature — identically with the pre-pass ON and OFF. The
// pre-pass warms the (mismatching) witness recovery; the serial header check
// still owns the reject.
func TestPrewarm_IdenticalReject_BadHeaderSig(t *testing.T) {
	witnessKey, err := crypto.GenerateKey()
	if err != nil {
		t.Fatal(err)
	}
	witnessAddr := crypto.PubkeyToAddress(&witnessKey.PublicKey)
	wrongKey, err := crypto.GenerateKey()
	if err != nil {
		t.Fatal(err)
	}
	genesis := fixedVerifyGenesis(witnessAddr)
	const badHeight = 3

	// Good empty blocks 1..2 produced + applied; bad block 3 hand-built on top
	// and signed by the wrong key (header signature recovers to a non-witness).
	bc := newVerifierChain(t, genesis)
	pbBlocks := make([][]byte, 0, badHeight)
	for h := 1; h <= badHeight; h++ {
		ts := int64(h) * int64(params.BlockProducedInterval)
		if h == badHeight {
			parent := bc.CurrentBlock()
			pb := &corepb.Block{
				BlockHeader: &corepb.BlockHeader{RawData: &corepb.BlockHeaderRaw{
					Number:         int64(h),
					Timestamp:      ts,
					ParentHash:     parent.Hash().Bytes(),
					WitnessAddress: witnessAddr.Bytes(),
				}},
			}
			setTestBlockTransactionMerkleRoot(t, pb)
			blk := types.NewBlockFromPB(pb)
			if err := SignBlock(blk, wrongKey); err != nil { // wrong header signer
				t.Fatalf("sign bad-header block: %v", err)
			}
			b, _ := blk.Marshal()
			pbBlocks = append(pbBlocks, b)
			break
		}
		pool := txpool.New()
		res, err := BuildBlock(bc, pool, witnessAddr, ts)
		if err != nil {
			t.Fatalf("BuildBlock height %d: %v", h, err)
		}
		if err := SignBlock(res.Block, witnessKey); err != nil {
			t.Fatalf("SignBlock height %d: %v", h, err)
		}
		if err := bc.InsertBlock(res.Block); err != nil {
			t.Fatalf("producer InsertBlock height %d: %v", h, err)
		}
		b, _ := res.Block.Marshal()
		pbBlocks = append(pbBlocks, b)
	}

	run := func(minTxs int) (uint64, error) {
		var head uint64
		var insErr error
		withMinTxs(minTxs, func() {
			vc := newVerifierChain(t, genesis)
			insErr = vc.InsertBlocks(unmarshalBatch(t, pbBlocks))
			head = vc.CurrentBlock().Number()
		})
		return head, insErr
	}

	headOn, errOn := run(1)
	headOff, errOff := run(0)
	for _, c := range []struct {
		name string
		head uint64
		err  error
	}{{"on", headOn, errOn}, {"off", headOff, errOff}} {
		if !errors.Is(c.err, dpos.ErrInvalidSignature) {
			t.Fatalf("%s: error = %v, want dpos.ErrInvalidSignature", c.name, c.err)
		}
		var rangeErr *InsertBlocksError
		if errors.As(c.err, &rangeErr) && rangeErr.Index != badHeight-1 {
			t.Fatalf("%s: failed at index %d, want %d", c.name, rangeErr.Index, badHeight-1)
		}
		if c.head != badHeight-1 {
			t.Fatalf("%s: head = %d, want %d", c.name, c.head, badHeight-1)
		}
	}
}

// produceBatchWithBadTx builds a wire batch [good_1 .. good_{badHeight-1}, bad_{badHeight}].
// Good blocks each carry one signed transfer from senderKey, produced + applied on
// the running producer head so the bad block links to a real parent and carries a
// valid header signature. The bad block (height badHeight) carries the
// caller-supplied badTx and is hand-assembled — BuildBlock would silently drop a
// tx its validation rejects — and is NOT applied to the producer. The verifier
// must reject precisely at index badHeight-1, identically with the pre-pass on and
// off; the caller asserts which error.
func produceBatchWithBadTx(t *testing.T, genesis *params.Genesis, witnessKey *ecdsa.PrivateKey, badHeight int, refBytes, refHash []byte, sender, recipient tcommon.Address, senderKey *ecdsa.PrivateKey, badTx *types.Transaction) [][]byte {
	t.Helper()
	bc := newVerifierChain(t, genesis)
	witnessAddr := bc.ActiveWitnesses()[0]
	out := make([][]byte, 0, badHeight)
	for h := 1; h <= badHeight; h++ {
		ts := int64(h) * int64(params.BlockProducedInterval)
		if h == badHeight {
			// Hand-build the bad block on the producer's current head.
			parent := bc.CurrentBlock()
			pb := &corepb.Block{
				BlockHeader: &corepb.BlockHeader{RawData: &corepb.BlockHeaderRaw{
					Number:         int64(h),
					Timestamp:      ts,
					ParentHash:     parent.Hash().Bytes(),
					WitnessAddress: witnessAddr.Bytes(),
				}},
				Transactions: []*corepb.Transaction{badTx.Proto()},
			}
			setTestBlockTransactionMerkleRoot(t, pb)
			blk := types.NewBlockFromPB(pb)
			if err := SignBlock(blk, witnessKey); err != nil {
				t.Fatalf("sign bad block: %v", err)
			}
			b, err := blk.Marshal()
			if err != nil {
				t.Fatalf("marshal bad block: %v", err)
			}
			out = append(out, b)
			break
		}
		pool := txpool.New()
		pool.Add(buildTransferTxWithRef(t, sender, recipient, int32(10+h), 0, refBytes, refHash, senderKey))
		res, err := BuildBlock(bc, pool, witnessAddr, ts)
		if err != nil {
			t.Fatalf("BuildBlock height %d: %v", h, err)
		}
		if err := SignBlock(res.Block, witnessKey); err != nil {
			t.Fatalf("SignBlock height %d: %v", h, err)
		}
		if err := bc.InsertBlock(res.Block); err != nil {
			t.Fatalf("producer InsertBlock height %d: %v", h, err)
		}
		b, err := res.Block.Marshal()
		if err != nil {
			t.Fatalf("marshal height %d: %v", h, err)
		}
		out = append(out, b)
	}
	return out
}

func setTestBlockTransactionMerkleRoot(t *testing.T, block *corepb.Block) {
	t.Helper()
	root, err := types.TransactionMerkleRoot(block.Transactions)
	if err != nil {
		t.Fatalf("transaction merkle root: %v", err)
	}
	block.BlockHeader.RawData.TxTrieRoot = root.Bytes()
}
