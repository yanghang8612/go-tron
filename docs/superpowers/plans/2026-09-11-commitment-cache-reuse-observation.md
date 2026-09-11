# Commitment cache observation implementation

1. Preserve fixed-input comparisons for all five admission candidates, including
   adverse delayed-reuse cases; do not deploy an unproven policy change.
2. Add the real-Pebble snapshot-after-flush regression and retain the global fill
   guard; document the negative-control failures.
3. Implement the optional bounded owner/shard cohort observer and writer hooks.
   Validate transparency, conservation, collisions, lifecycle and memory limits.
4. Measure observer off/on overhead and run relevant full/race/native checks.
5. Push source and a pinned deployment helper directly to master. Prepare the
   native binary, activate with the explicit observer environment, retain exact
   rollback bytes and verify runtime observer identity.
6. Collect and analyze production cohorts, cache/backlog stability and errors;
   record evidence and limitations before selecting another admission policy.
