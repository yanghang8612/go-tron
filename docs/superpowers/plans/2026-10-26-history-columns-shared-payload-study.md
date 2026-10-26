# Next compression study after complete ordered-page acceptance

1. Keep product Go inputs frozen and the complete 60-row publication and
   704-member correctness evidence immutable. Use the existing worktree and
   separate overlay-only diagnostic source and bounded native unit.
2. Implement a small separated-key/common-tail prototype outside product code.
   Test recovery and bounds locally without claiming file or HTTP acceptance.
3. Guard native execution on successful exit of the previous acceptance unit;
   never restart it because a browser connection or observation expired.
4. Inspect every page of the three fixed V5 outputs, verify original file SHA
   and existing exact encoder bytes, then retain all per-page sizes/digests.
   Do not drop pages where the prototype grows or ordinary fallback wins.
5. Independently recompute complete page totals and diagnostic timing scope.
   Decide from measured loss/gain whether a registered representation merits
   implementation; reject an unhelpful prototype and keep its evidence.
6. A promising prototype still requires actual file/space accounting, full
   compactor performance, query/memory checks and final-binary correctness
   gates before recommending use. The broader compression goal remains active.

Steps 1–5 are complete: 1,817/1,817 canonical page checks passed, 555,250 page
bytes saved, resident encode −39.09% and decode +10.12%. Complete raw evidence
is locally verified. Step 6 is still pending; the immediate follow-up is the
bounded scratch decoder study, with no product format or deployment change.
