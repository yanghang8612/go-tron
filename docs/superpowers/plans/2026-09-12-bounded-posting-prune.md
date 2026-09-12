# Implementation plan

1. Add a bounded posting-only rawdb chunk with exclusive in-memory cursor and
   failure/budget/read-equivalence tests.
2. Add a chain wrapper binding cold permission, durable stages and canonical
   hashes, with opportunistic index admission and queued chain-lock handoff;
   test rewind, writer exclusion and cancellation after a queued wait.
3. Publish successful snap hot-prune permission atomically. Wire an opt-in
   lifecycle with pressure admission, bounded metrics, cancellation and no
   competing full sweep. Test progress, invalidation and stop behavior.
4. Run affected tests, race tests, build and incremental lint. Review independent
   findings before committing directly to master and pushing GitHub.
5. Pin deployment helper, build/test natively, activate with rollback available,
   and compare live sampling and actual filesystem observations with the prior
   version. Record throughput and remaining limitations without treating
   logical deleted bytes as recovered disk capacity.
