# VM exception-gate audit (2026-07-23)

Scope: java-tron exception boundaries that can change `contractRet`, remaining
energy, or state rollback across proposal eras. This audit was started after
mainnet sync stalls at 3,422,904, 4,904,919, 4,997,510, 5,196,383, and
5,780,856.

## Consensus-result matrix

| Java path | Before gate | After gate | gtron status |
|---|---|---|---|
| `Program.callToAddress`: accountless/self TRX transfer | `BytecodeExecutionException` → UNKNOWN, spend all | Constantinople: `TransferException` → TRANSFER_FAILED, refund | covered |
| `Program.callToAddress`: recipient TRX balance overflow | same as above | same as above | covered by overflow tests |
| `Program.callToAddress`: accountless/self TRC10 transfer | `BytecodeExecutionException` → UNKNOWN, spend all | Constantinople: `TransferException` → TRANSFER_FAILED, refund | covered |
| `Program.callToAddress`: recipient TRC10 balance overflow | same as above | same as above | covered by overflow tests |
| CALL/CALLCODE/CALLTOKEN endowment `longValueExact` | raw `ArithmeticException` → UNKNOWN, spend all | Constantinople ordinary-address path: `TransferException` → TRANSFER_FAILED, refund | fixed in `432f6429` |
| Precompile-call endowment `longValueExact` | raw `ArithmeticException` | remains raw (no Constantinople catch) | covered |
| CREATE/CREATE2 endowment `longValueExact` | raw `ArithmeticException` | remains raw (no Constantinople catch) | covered |
| `Program.suicide` beneficiary validation | `BytecodeExecutionException("transfer failure")` → UNKNOWN | Constantinople: `TransferException` → TRANSFER_FAILED | fixed in `d81e73f4` |
| Internal CREATE empty runtime-code cache | pre-MultiSign message-less NPE → UNKNOWN | MultiSign initializes the empty cache value | covered |

## Reviewed non-throwing conversions

Freeze/unfreeze/delegate/vote native operations catch their own
`ArithmeticException`/`ContractValidateException`, reject the internal
transaction, and return an opcode success flag. They must not be promoted to a
top-level VM exception.

## Follow-up risks

1. Internal CREATE into an existing non-contract account can preserve an old
   balance. Its endowment validation is a CREATE-wrapper
   `BytecodeExecutionException`, not an ordinary child-create failure. Add a
   collision fixture before changing propagation because java's pre- and
   post-Constantinople account replacement rules differ.
2. CALLTOKEN token-ID overflow and low-ID errors already have the correct
   UNKNOWN-versus-TRANSFER_FAILED class split, but their historical
   `resMessage` strings need byte-level fixture comparison across MultiSign and
   Constantinople.
3. Precompile-targeted TRC10 transfer overflow follows
   `callToPrecompiledAddress`, not ordinary `callToAddress`; add a java fixture
   before changing its message sentinel.
4. Keep scanning upcoming mainnet UNKNOWN receipts by message family
   (`BigInteger out of long range`, `validateForSmartContract failure`,
   `transfer failure`, `Unknown Exception`) and add one replay fixture per
   distinct Java catch boundary rather than per transaction.

## Forward scan

Use the batched scanner to inventory UNKNOWN receipts before gtron reaches
them:

```bash
scripts/dev/scan_vm_unknown.sh 5780857 5880857 > /tmp/mainnet-unknown.tsv
tail -n +2 /tmp/mainnet-unknown.tsv | cut -f4 | sort | uniq -c | sort -nr
```

`TRON_HTTP_API` can point at another full-history HTTP endpoint. A lite
fullnode usually disables `getblockbylimitnext`; the public default is
TronGrid. `ALL_PROXY=socks5h://127.0.0.1:1088` may be used when required.

The initial public-endpoint scan covered `[5,780,857, 5,783,957)` (3,100
blocks) and found no `UNKNOWN` receipts. Continue from block 5,783,957; this is
an inventory boundary, not evidence that the remaining range is clean.
