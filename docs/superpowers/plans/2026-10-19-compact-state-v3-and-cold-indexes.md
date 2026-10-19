# Implementation and validation order

- Implement rooted VotesIndex pages and behavioral regressions.
- Add eight-byte hot/cold latest codec and exact presence/malformed tests.
- Add explicit owner-elided account core and update all hydration paths.
- Upgrade scheme/version guards and extend full-state lifecycle oracle.
- Implement relative transaction-index writer/reader/merge and corruption tests.
- Implement bounded compressed history-index reader/writer and query tests.
- Run relevant packages/race/vet, full Go 1.25 suite and Sapling build/tests.
- Measure actual output bytes and query CPU/allocations/latency on fixed corpora;
  retain raw evidence and document limits before preparing any release.
