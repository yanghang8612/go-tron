# One independent production-platform evaluation: B versus B+A2

This is a new linux/amd64, Go1.25.5, CGO1/Sapling experiment. The approved B-only
Git commit is the baseline; the candidate is that identical B plus the sealed
A2 production patch. Darwin A2 remains NO-GO (cold parent spread0.370148), and
none of its timings are mixed, replaced or reclassified. This is not a retry of
that platform or a claim that sharing a production host removes uncertainty.

The packet is inert text in Git. All Go harnesses/correctness helpers end in
.go.txt (the JSON fixture ends in .json.txt), outside package source. It contains
no binary, source export, native archive, credential, database or Git bundle.
It is delivered only with the reviewed B commit through GitHub. No loose source
upload, temporary service, node setting change or database access is permitted.

The baseline is tested B commit572dcc377584be867542d0a56e0faf164cb1aa41,
Git tree98d06df1dd1799d1ed59a9e60344ec4e7e7fd96a. This packet travels in a later
docs-only commit; its non-doc source/test bytes must equal that tested commit.
The baseline checkout stays pinned to572dcc, not a moving branch.
source-identity.json additionally freezes exact production projection trees:
complete actual9030 tree plus B's three production changes, and that projection
plus A2. B's new tests/docs and the packet are excluded only from this projection,
not from the later actual Git commit/tree identity. The preparer checks all old
9030 Git entries, the three exact B production files, newly added runtime files,
and all12 expected A2 production files. Applying the sealed A2 patch to exact B
has already matched the combined worktree byte for byte, including shared
buffer.go. Both measured arms have identical B and five common fixture/harness
files. Six A2 correctness files are installed only in the candidate checkout.

## Environment fixed before any Linux result

Read-only preflight09:11:31Z found CPUs0..15, eight physical cores; CPU15 has SMT
sibling7. Freeze test/build/count/benchmark affinity to logical CPU15 and Python
resource observer/orchestration to CPU0. This does not reserve the physical core
or move the node/Java/observer, which may run on either sibling. Do not switch
CPU, host, affinity, runtime, input or resource limit after seeing results.
Python is pinned to /usr/bin/python3 version3.6.8 (actual taskset[15] proof);
stdlib calls use Python3.6-supported APIs, not merely newer syntax.
GOMAXPROCS=1, GOGC=100, GOMEMLIMIT=4GiB, CGO_ENABLED=1, GOOS=linux, GOARCH=amd64,
GOFLAGS empty, GOWORK=off, GOPROXY=off. The4GiB limit is only the Go benchmark's
soft heap target, not a hard RSS cap; it never changes node20GiB/GOGC100/cache
or service40G limits. Existing Go/native/header SHA pins must match exactly.
Native code is copied unchanged into isolated checkouts; no Cargo rebuild.

After Root authorizes quiescence/publication, pause/drain auto deploy and own
the audited /data/gtron/start.lock descriptor before GitHub push/fetch. Check
other chat still idle without messaging it and obtain actual mutual exclusion;
old no-holder observations are not ownership. Scripts require the inherited
start-lock FD and verify/lock that same open file description. They do not take
the helper's second release lock or operate services. The9030 process remains
running throughout the evaluation. Its PID/exe are recorded and changes fail
this evaluation; do not stop background cold-query observer or Java.

Scratch output must be a unique /data/gtron/go-tron/build/linux-a2.NAME folder.
TMPDIR is inside that folder; never use any node datadir or root temp filesystem.
Record actual Git commit/tree, toolchain/native hashes, CPU topology/affinity,
/proc state, cgroup limits, df, services and guard/observer state before/after.
Sample host/perCPU/sibling CPU, memory, diskstats and node RSS/IO every5 seconds
on CPU0 throughout correctness/counts/formal processes. Preserve raw JSONL and
per-command times/status/stderr. These observations explain limitations; they
are not grounds to discard a sample. Before work require hostavailable>=12GiB,
rootfree>=8GiB/datafree>=32GiB. If while running available<8GiB, rootfree<8GiB,
datafree<32GiB, or node identity changes, terminate only the benchmark child,
mark the evaluation FAIL and retain all output. Do not change services or retry.

## Exact single experiment and release rule

Prepare four isolated real Git checkouts from the exact reviewed B commit:
parent, candidate, parent-counts, candidate-counts. Apply A2 only to candidates;
install identical common files in all. Instrument successful SST iterator
construction only in counts copies. Run affected normal/race correctness with
Sapling on candidate; any failure stops evaluation. Build eight independent
Sapling test binaries and freeze complete source/binary/env identities before
counts. One counts proof, parent then candidate,9shapes64ops, verifies same
physical/logical fixture and activation. Its ns/op is never used for a gate.
Review its counters/identities before the single formal window; no second proof.

Formal order is exactly ABBAABBAAB, five samples/arm. Each position runs four
independent processes: component9shapes64ops; canonical2shapes64ops;
GoldenTron1024ops; TUSD8192ops. Total40commands/13shapes, same exact selectors
and common input/order/budgets as sealed A2. No adaptive b.N, warmup sample,
extra calibration, removed sample, rerun, replacement CPU or second window.

All original thresholds remain: cold and steady median time>=10% better,
actual SST iterator constructions>=50% lower, same-work durable calls<=5% higher,
both primary spreads(max-min)/median<.30. Hot/overlay/mixed128/scatter/canonical128/
Golden/TUSD median regression<=5%. Single/empty/canonicalempty incremental<=25us.
No-cache is diagnostic; busy is functional no-queue only. Quantized live SST
counts remain labeled; scatter requires>=16liveSST and reports low reuse.
Actual Get counts and table construction are separate; neither is device IO.

Only Linux all-gate PASS + correctness + independent final combined audit can
admit A2 to the next production commit. Then push the audited A2 addition under
the same exclusive protocol, fetch exact Git source, run complete Linux Sapling
whole-repo tests/build, review exact binary identity and perform one final helper
deploy. Any Linux gate/correctness/environment failure chooses Bv2 only, with its
own exact Linux full-repo/build validation. Do not iterate candidate/platform
validation until it passes. Neither branch deploys before explicit Root GO.

## Raw evidence recovery

After the sole attempt finishes (PASS or FAIL), explicitly run
`/usr/bin/python3 archive-evidence.py /data/gtron/go-tron/build/linux-a2.NAME`.
It preserves every root-level .log/.json/.jsonl, including failure status,
correctness/build outputs, all40 formal raw logs if reached, both counts raw
logs, complete source/binary identities and resource observations, into
`linux-a2-evidence.tar.gz` with a per-file SHA manifest. It excludes only source
checkouts, native archives and test binary bytes (their complete hashes remain).
Print the server archive SHA256 before transferring. Use this chat's independent
JumpServer SFTP connection to download that exact archive once into Downloads;
verify local archive SHA, safely extract into a new local evidence directory,
and verify every manifest member SHA before any audit conclusion. Keep the
server originals and any failed/partial download. No result is accepted from
manual transcription of benchmark rows; if the complete archive cannot be
retrieved, A2 remains unproved/NO-GO. No retry of the evaluation is permitted.
