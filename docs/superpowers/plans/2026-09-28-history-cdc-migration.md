# CDC inventory migration implementation and rollout

1. Add bounded CDC reencoding with exact virtual-byte comparison and measured gain.
2. Reuse the durable trio migration protocol with isolated target journals; retain
   existing R1 behavior and block metadata cleanup for pending CDC journals.
3. Add offline CLI selection/budget/progress flags and regression/fault tests.
4. Build a separate maintenance binary from the pinned deployed source plus the
   reviewed patch. Do not replace the service binary or restart the node.
5. Save manifest, binary/source identities, stopped unit states and disk baseline.
6. Plan and execute a small newest-history pilot, verify unchanged coverage/progress,
   actual freed space, complete trio validation and old-binary query compatibility.
7. Continue serial batches with a live free-space floor, preserving logs and journal.
   Report active byte savings separately from filesystem free-space fluctuations.
