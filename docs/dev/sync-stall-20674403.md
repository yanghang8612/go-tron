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

## Correction

At canonical block 20,674,403, an exact-state guarded repair restores the five
omitted state deltas before transaction execution. It checks:

- the canonical block ID;
- recipient, contract, payer, and blackhole balances; and
- the exact pre-repair value of contract storage slot 10.

The repair is inside the block snapshot, so a failed block rolls it back. A
clean replay, a fork, a retry after successful repair, or any independently
modified state does not match all guards and remains unchanged.

## Recovery

Deploy a binary containing the existing speculative VM fixes and this guarded
repair, then restart `gtron.service`. It retries block 20,674,403 and should
resume sync without a clean resync.

If the guard does not match, do not edit the database manually. Restore a
trusted snapshot before block 20,616,256 or clean-resync with the corrected
binary. Disabling `--exec.parallel-transfers` is conservative during a replay,
but cannot repair divergence already committed by an older binary.

## Verification

- Confirm block 20,674,403 imports and the head advances.
- Confirm transaction `59feaaa85d...` records SUCCESS with 20,470 energy.
- Run `go test ./core -count=1 -timeout 300s`.
