# Mainnet sync stall at block 18,414,381

## Summary

On 2026-08-05 the mainnet gtron service stopped while importing block
18,414,381:

```text
insert block range index 0 block 18414381: process block: tx 5: validate: balance is not sufficient
```

The failing transaction was
`6186f5fe1b5ba706e5b17b608df422cb2143b369bfbba73fd0af4366cffe799e`.
Its owner attempted a 77,000,000 sun contract call with a local pre-block
balance of 67,007,277 sun. The canonical balance was 78,834,028 sun.

Restarting could not help because the balance divergence had already been
committed 12,077 blocks earlier.

## Root cause

The first divergent transaction was mainnet block 18,402,304 transaction
`1f63de3942671be7824c8bd2f98106002253e641152aa8fcdfb240d5f291a748`.
The `groupPayment` call should have:

- transferred 11,826,751 sun from contract
  `41a061137c9fcba2ff9658660d49c12b5c091fe585` to account
  `419f9e5ec2ccfbf8ca532550c8394312c86a5b7087`; and
- charged payer `4198562b3871533e853d6cbcfc511197b5943047c0` 85,360 sun
  for 8,536 energy.

The block number is on the 1,024-block parallel-publication boundary. A
speculative VM worker saw the entry contract with empty code and published a
zero-energy SUCCESS result. Typed read-version validation showed that no prior
transaction in the block had changed the code cell, but it did not prove that
the worker's block-start base value matched the canonical StateDB. The result
therefore omitted the transfer, energy charge, and blackhole credit.

Serial `debug_traceTransaction` replay executed 515 opcodes, consumed 8,536
energy, returned true, and reproduced the canonical state transition.

## Corrections

The correction has three layers:

1. `fddd3762 fix(exec): retain cached code in VM workers` preserves cached
   contract code when creating execution copies, removing the underlying empty
   code view.
2. Speculative TriggerSmartContract results carry an entry-code fingerprint.
   Publication compares it with the authoritative StateDB and falls back to
   serial execution on any mismatch.
3. At canonical block 18,414,381, an exact-state guarded repair corrects the
   four state deltas omitted at block 18,402,304. The guard checks the canonical
   block ID and all four legacy balances, making it idempotent and inert for
   clean replays, forks, or independently modified databases.

## Recovery

Deploy a binary containing all three corrections and restart `gtron.service`.
The service retries block 18,414,381, applies the guarded correction before
transaction execution, and resumes sync without a clean resync.

If the exact-state guard does not match, do not alter balances manually. Restore
a trusted snapshot before block 18,402,304, or clean-resync with a corrected
binary. Disabling `--exec.parallel-vm` is an additional conservative option
during that replay, but does not repair an already-diverged database. Builds
before the VM flag was separated used `--exec.parallel-transfers` for both
publishers.

## Verification

- Compare the four affected balances with a java-tron historical state query at
  block 18,414,380.
- Confirm block 18,414,381 imports and the head advances.
- Run `go test ./core -count=1 -timeout 300s`.
- Keep the entry-code mismatch test and guarded-repair idempotence/rollback test
  enabled in normal CI.
