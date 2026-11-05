# Java VM frame-boundary oracle

`java_golden.json` was emitted by the real java-tron VM at
`4e8389f159d0782f1ebcd9dab2fd88388c0e6fe9`. `TestJavaFrameBoundaryParity`
runs every vector through Go with and without tracing. Normal Go tests consume
the checked-in data and do not need Java, Mockito, a node or network access.

The 40 vectors cover failed/successful static calls and successful descendants,
dynamic-energy catch-up and penalties, CALLCODE endowment failures, ordinary and
precompiled targets, DELEGATECALL with an inherited value exceeding the current
balance, CREATE/CREATE2 EF RETURN/REVERT behavior with London enabled/disabled,
and prior CREATE2 return data before/after Osaka. Comparisons include exact
return bytes, remaining energy, penalties, nonce, internal transaction
notes/values/rejected flags, balances, ContractState and parent storage.

To regenerate from an existing clean java-tron source/build:

```sh
python3 scripts/dev/run_vm_frame_oracle.py /path/to/java-tron /new/scratch \
  --java-home /path/to/jdk17
```

The script recompiles java-tron's VM package, VMConfig and ProgramResult from
source into the scratch directory. Existing module classes and FullNode.jar
supply the remaining runtime dependencies. It uses local Maven artifacts:
Lombok 1.18.34, Mockito 4.11.0, Byte Buddy 1.12.19 and Objenesis 3.3.
It never changes the Java checkout or starts a node.

`JavaFrameOracle.java` executes the original VM, Program, OperationActions and
RepositoryImpl, including real child repository publication and discard. Only
empty backing stores and fixed dynamic property inputs are mocked; the VM and
repository algorithms are not mocked or reimplemented. Each case has a fresh
root repository, never commits that root to disk, and executes normally rather
than Java's `byTestingSuite` shortcut.

The regenerated JSON is written to `/new/scratch/java_golden.json`. Review its
source identity and diff before replacing the checked-in fixture. The Go test
pins both the Java source commit and the vector-family inventory.

Stack targets use 20-byte ABI addresses. Persistent account keys retain their
21-byte TRON network prefix. A separate 21-byte stack/precompile alias
discrepancy discovered during probe setup is recorded in the audit report and
is outside these three fixes.
