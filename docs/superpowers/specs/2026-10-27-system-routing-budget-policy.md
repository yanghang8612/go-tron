# Adaptive broad history routing maintenance

Keep GTSYSR01 V1/V2, canonical source identities, strict import verification,
MuHash inputs and all historical values unchanged. Retain the independent
small-system reader already implemented in this worktree.

Read/authenticate the old broad route and each attempted source dictionary once
per build. Assemble a bounded candidate, then immediately probe one new source
on aggregate budget rejection. At most two assemblies avoid repeated scans
of the large old key set, including identical candidates with fewer than 64
pending sources. Remember the successful smaller limit for later passes.
Do not retry source I/O on every smaller assembly. Preserve all active reusable
old columns; uncovered sources always use authoritative readers. Errors and
cancellation remain errors rather than becoming budget deferrals.

Maintenance policy belongs to one Runner under passMu. Remember a successful
smaller batch. On genuine no-progress budget rejection, apply bounded backoff
and rotate the attempted input cursor; no cross-request absence proof comes
from this state. Bind scheduling hints to the broad route metadata, every
active reusable old source identity and the attempted source prefix. Tail
appends beyond that prefix must not repeatedly defeat backoff; replacements,
retirement and relevant new inputs must invalidate it. Bound state and retain
no large decoded dictionary beyond the build.

Missing/changed source files and semantic corruption are not budget failures.
Physical authentication on every query/cache hit and strict snapshot import
remain unchanged. Publish only via the existing auxiliary Integrate transaction.

Verify adaptive selection, no duplicate scans, minimal/no capacity, backoff and
its invalidation, directory separation, cursor rotation, source replacement,
legacy V1/V2, corruption/cancellation and historical results. Compare the
pre-policy worktree against the candidate on the same frozen inputs for actual
cold construction plus maintenance, CPU and complete HTTP eth_call with warm
and verified private-file cold caches. Recompute fixed-copy MuHash and exercise
history/strict/signed restoration. No production deployment, restart, reset or
manual database modification is authorized.
