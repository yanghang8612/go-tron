package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/state"
)

// A companion verification can check account pointers on an already fully
// authenticated offline image without repeating millions of MuHash operations.
func runPointerArgs(args []string) error {
	f := flag.NewFlagSet("verify-account-pointers", flag.ContinueOnError)
	dir := f.String("db-dir", "", "immutable offline chaindata copy")
	out := f.String("out", "", "new companion report outside DB")
	if err := f.Parse(args); err != nil {
		return err
	}
	if *dir == "" || *out == "" || f.NArg() != 0 {
		return errors.New("db-dir and out required")
	}
	path, err := filepath.EvalSymlinks(*dir)
	if err != nil {
		return err
	}
	path, err = filepath.Abs(path)
	if err != nil {
		return err
	}
	if _, err = os.Stat(filepath.Join(path, "CURRENT")); err != nil {
		return err
	}
	output, err := outsideOutput(path, *out)
	if err != nil {
		return err
	}
	file, err := os.OpenFile(output, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return err
	}
	var r struct {
		Complete bool                      `json:"complete"`
		Error    string                    `json:"error,omitempty"`
		Source   string                    `json:"source"`
		Finished time.Time                 `json:"finished_utc"`
		Head     rawdb.StateCommitmentHead `json:"head"`
		Pointers accountPointerCensus      `json:"account_generation_pointers"`
	}
	r.Source = path
	db, err := rawdb.NewPebbleDBReadOnly(path, 64, 128)
	if err == nil {
		var present bool
		r.Head, present, err = rawdb.ReadStateCommitmentHead(db)
		if err == nil && !present {
			err = rawdb.ErrStateCommitmentHead
		}
		if err == nil {
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
			r.Pointers, err = inspectAccountPointers(ctx, db)
			cancel()
		}
		err = errors.Join(err, db.Close())
	}
	r.Complete = err == nil
	r.Finished = time.Now().UTC()
	if err != nil {
		r.Error = err.Error()
	}
	enc := json.NewEncoder(file)
	enc.SetIndent("", "  ")
	return errors.Join(err, enc.Encode(r), file.Sync(), file.Close())
}

type accountPointerCensus struct {
	Accounts     uint64   `json:"accounts"`
	Explicit     uint64   `json:"explicit_generation_accounts"`
	ImplicitZero uint64   `json:"implicit_zero_accounts"`
	Mismatches   uint64   `json:"mismatches"`
	Examples     []string `json:"mismatch_examples,omitempty"`
}

// Execution reads the generation from the account envelope; the separate
// generation row also supports recreate and historical readers. Verify every
// live envelope agrees with that row/default before trusting GC classification.
func inspectAccountPointers(ctx context.Context, db interface {
	ethdb.KeyValueReader
	ethdb.Iteratee
}) (r accountPointerCensus, retErr error) {
	it := db.NewIterator(rawdb.StateCommitmentSourcePrefixes()[1], nil)
	defer func() {
		retErr = errors.Join(retErr, it.Error())
		it.Release()
		retErr = errors.Join(retErr, it.Error())
	}()
	var owner common.Address
	var generation uint64
	ok := false
	advance := func() error {
		ok = it.Next()
		if !ok {
			return it.Error()
		}
		var valid bool
		owner, valid = rawdb.DecodeStateKVGenerationKey(it.Key())
		if !valid {
			return errors.New("malformed generation key")
		}
		var err error
		generation, err = rawdb.DecodeStateKVGenerationValue(it.Value())
		return err
	}
	if err := advance(); err != nil {
		return r, err
	}
	err := rawdb.IterateStateAccountLatest(db, nil, func(row rawdb.StateAccountLatestRow) (bool, error) {
		if err := ctx.Err(); err != nil {
			return false, err
		}
		r.Accounts++
		for ok && bytes.Compare(owner[:], row.Owner[:]) < 0 {
			if err := advance(); err != nil {
				return false, err
			}
		}
		current := uint64(0)
		if ok && owner == row.Owner {
			current = generation
			r.Explicit++
		} else {
			r.ImplicitZero++
		}
		envelope, err := state.DecodeStateAccountV3(row.Value)
		if err != nil {
			return false, err
		}
		if envelope.AccountKVGeneration != current {
			r.Mismatches++
			if len(r.Examples) < 32 {
				r.Examples = append(r.Examples, fmt.Sprintf("owner=%s envelope=%d counter=%d", row.Owner.Hex(), envelope.AccountKVGeneration, current))
			}
		}
		return true, nil
	})
	if err != nil {
		return r, err
	}
	if r.Mismatches > 0 {
		return r, errors.New("execution account generation differs from generation table/default; classification is not deletion authority")
	}
	return r, nil
}
