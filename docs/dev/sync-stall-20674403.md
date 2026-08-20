# Mainnet sync stall at block 20,674,403

## Summary

On 2026-08-20 the mainnet gtron service stopped while importing block
20,674,403:

```text
process block: tx 0: transaction vm result mismatch: tx 59feaaa85d... expected SUCCESS actual OUT_OF_ENERGY
```

The failing transaction called `betreward()` on the COST contract with a
5,000,000-sun call value. Its local pre-block balance was 5,000,000 sun, while
the canonical balance was 10,000,000 sun. After reserving the call value, the
legacy local state had no balance left to buy energy.

Restarting cannot fix this state because the divergence was committed 58,147
blocks earlier.

## Root cause

The first divergent transaction was mainnet block 20,616,256 transaction
`0def93583b6b879aa744d87a7a76c02285d97902fa2a050fc59d512c8641c677`.
It called `reward(recipient, 5000000, 5000000)` on contract
`41159d096ab12d5d35ebf8357960324f598fd54d16`. Java-tron:

- transferred 5,000,000 sun from the contract to recipient
  `41e2e8867dc625426a62224abd1f407eb83b12732e`;
- charged payer `417890bf5b427064e755d882b01ca66cfc2b862363`
  140,870 sun for 14,087 energy and credited the blackhole account; and
- incremented contract storage slot 10 by 5,000,000.

The legacy gtron database contains a SUCCESS receipt with zero energy and none
of those state changes. This is the same pre-fix speculative VM publication
failure documented in `sync-stall-18414381.md`: an empty-code speculative result
was accepted instead of executing the contract.

The missing transfer first becomes fatal at block 20,674,403 transaction
`59feaaa85dc3b264cf2a5207f8ddb4d20ca5074a9a71aa3b98fd0938928d60aa`.
Java-tron executes it successfully with 20,470 energy.

## Follow-up mismatch at transaction 55

After correcting transaction 0, replay advanced to transaction 55
`e6fc2876547d39db5d736e008166439ec4899104de33770035b5e5a4805c7c45`
and stopped with expected REVERT, actual SUCCESS.

This transaction sends empty calldata to the WINK token contract
`4174472e7d35395a6b5add427eecb7f4b62ad2b071`. Canonical execution reaches the
fallback, consumes 43 energy, and executes REVERT. The legacy database account
still references canonical runtime hash
`b8b0efb7d4ff5ce4567d234b4bed40278f1955619f7e496fcff99aa20b6e1c08`,
but the corresponding immutable state-code blob is missing, so gtron treats the
call as empty code and returns SUCCESS.

The contract metadata remains intact. Its 7,969-byte creation bytecode has hash
`6d6573f4e5bca2b3c691e783e3fb63a619d993602220158cf84996c4f166b834`.
Extracting 6,887 bytes at offset `0x35a` exactly reproduces both the runtime
returned in the canonical creation receipt and the account's runtime hash.

## Correction

At canonical block 20,674,403, exact-state guarded repairs run before
transaction execution. The first restores the five omitted COST state deltas
and checks:

- the canonical block ID;
- recipient, contract, payer, and blackhole balances; and
- the exact pre-repair value of contract storage slot 10.

The second restores the missing WINK immutable code blob from existing contract
metadata. It requires the canonical block ID, a missing runtime row, the exact
account runtime hash, the exact creation-code hash, and a matching hash for the
extracted runtime. Restoring the blob leaves the account's consensus-visible
code hash unchanged.

Both repairs are inside the block snapshot, so a failed block rolls them back.
A clean replay, a fork, a retry after successful repair, or any independently
modified state does not match all guards and remains unchanged.

## Recovery

Deploy a binary containing the existing speculative VM fixes and both guarded
repairs, then restart `gtron.service`. It retries block 20,674,403 and should
resume sync without a clean resync.

If the guard does not match, do not edit the database manually. Restore a
trusted snapshot before block 20,616,256 or clean-resync with the corrected
binary. Disabling `--exec.parallel-transfers` is conservative during a replay,
but cannot repair divergence already committed by an older binary.

## Verification

- Confirm block 20,674,403 imports and the head advances.
- Confirm transaction `59feaaa85d...` records SUCCESS with 20,470 energy.
- Confirm transaction `e6fc287654...` records REVERT with 43 energy.
- Run `go test ./core -count=1 -timeout 300s`.
