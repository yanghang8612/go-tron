# Independent event archive frontier

With direct Ancient V2 and external receipt logs, production prepares the next
complete ancient segment's events independently of state-history SnapshotBuild.
The source remains capped by the latest solid block and hash-verified Finish.
Event refs carry an exact terminal canonical hash, including log-free terminal
blocks. Runtime coverage verifies this source authority separately from file
checksums and lookup companion coverage.

The normal V4 event builder publishes its main/index pair together. A damaged or
manually assembled catalog can instead contain a wider main or lookup ref with
an incomplete companion. Repair must preserve that entire connected component;
replacing just its prefix would retire the wider ref and discard coverage.

If this complete repair exceeds the next freezer segment's requested end, the
runner returns `ErrEventLogCompanionRepairRequired`, including the requested and
repair ranges and an instruction to rebuild the complete companion offline. It
starts no event source build, advances no stage, and publishes no new manifest.
It does not defer forever waiting for a freezer demand that the missing companion
might itself prevent from advancing. This uncommon catalog shape requires
operator maintenance; automatic unbounded repair is outside this scheduler.

Generic from-cold rewrites reject any crossing active main or lookup ref before
construction, and recheck before publication. The dedicated V4 migrator may
preserve a crossing lookup only after verifying unchanged candidate keys. Both
paths require and retain authenticated exact-end source anchors; they never infer
the terminal source hash from the last emitted log.
